#!/usr/bin/env python3
"""Run one CI command under an explicit hard deadline on Linux/POSIX.

This helper is intentionally small and independent of Threadlane runtime or
model timeout policy. It executes argv directly without a shell, streams both
output pipes, emits a fixed heartbeat, and owns one process session. On the
deadline it sends SIGTERM and then a bounded SIGKILL only while the original
child is still live; it never signals a group after the child has been reaped.
Inherited output pipes are drained for a bounded interval. If they stay open,
the wrapper reports incomplete cleanup and returns nonzero without claiming
that escaped descendants were killed.
"""

from __future__ import annotations

import argparse
import contextlib
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Sequence

DEFAULT_HEARTBEAT_SECONDS = 15.0
MAX_TIMEOUT_SECONDS = 7200.0
TERMINATE_GRACE_SECONDS = 2.0
KILL_GRACE_SECONDS = 1.0
POST_EXIT_DRAIN_SECONDS = 2.0
READ_CHUNK_BYTES = 64 * 1024
MAX_READ_CHUNKS_PER_EVENT = 8
NO_PIPE_POLL_SECONDS = 0.05
MAX_LINE_BYTES = 4 * 1024
MAX_HINT_CHARS = 200
EXIT_TIMEOUT = 124
EXIT_CLEANUP_INCOMPLETE = 125

_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
_HINT_RE = re.compile(
    r"^(?:"
    r"test result:|"
    r"running \d+ tests?\b|"
    r"test .+? \.\.\. ?(?:ok|FAILED|ignored)\b|"
    r"(?:compiling|building|checking|finished|running|documenting|fresh|"
    r"locking|updating|downloading|downloaded|removing)\b|"
    r"(?:error|warning)(?:\[[^\]]+\])?:"
    r")",
    re.IGNORECASE,
)


class _UsageError(RuntimeError):
    """Invalid wrapper invocation."""


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise _UsageError(message)


class _LineBuffer:
    """Split a byte stream into lines while retaining at most ``max_bytes``."""

    def __init__(self, max_bytes: int = MAX_LINE_BYTES) -> None:
        self.max_bytes = max_bytes
        self._pending = bytearray()

    @property
    def pending_bytes(self) -> int:
        return len(self._pending)

    def feed(self, data: bytes) -> list[bytes]:
        lines: list[bytes] = []
        start = 0
        while True:
            newline = data.find(b"\n", start)
            if newline < 0:
                self._append(data[start:])
                return lines
            segment = data[start:newline]
            if self._pending:
                self._append(segment)
                lines.append(bytes(self._pending))
                self._pending.clear()
            else:
                lines.append(segment[: self.max_bytes])
            start = newline + 1

    def flush(self) -> bytes | None:
        if not self._pending:
            return None
        line = bytes(self._pending)
        self._pending.clear()
        return line

    def _append(self, segment: bytes) -> None:
        if not segment:
            return
        remaining = self.max_bytes - len(self._pending)
        if remaining > 0:
            self._pending.extend(segment[:remaining])


def _clean_line(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    text = _ANSI_RE.sub("", text)
    text = _CONTROL_RE.sub("", text)
    return re.sub(r"\s+", " ", text).strip()


class _HintTracker:
    """Keep the last relevant libtest or Cargo build phase line."""

    def __init__(self) -> None:
        self._last = ""

    def feed_line(self, raw: bytes) -> None:
        text = _clean_line(raw)
        if text and _HINT_RE.match(text):
            self._last = text[:MAX_HINT_CHARS]

    @property
    def hint(self) -> str:
        return self._last or "waiting for libtest/build output"


@dataclass
class _StreamState:
    pipe: object
    sink: object
    fd: int
    lines: _LineBuffer = field(default_factory=_LineBuffer)


def _write_bytes(stream, data: bytes) -> None:
    if not data:
        return
    try:
        buffer = getattr(stream, "buffer", None)
        if buffer is not None:
            buffer.write(data)
            buffer.flush()
        else:
            stream.write(data.decode("utf-8", errors="replace"))
            stream.flush()
    except (BrokenPipeError, OSError, TypeError, ValueError):
        # A closed CI log sink must not stop output draining or deadline work.
        pass


def _write_text(stream, text: str) -> None:
    _write_bytes(stream, text.encode("utf-8", errors="replace"))


def _heartbeat_text(elapsed: float, timeout_seconds: float, hint: str) -> str:
    return (
        f"[run-ci-check] heartbeat elapsed={elapsed:.1f}s "
        f"timeout={timeout_seconds:g}s hint={hint}"
    )


def _child_exit_code(returncode: int) -> int:
    return 128 - returncode if returncode < 0 else returncode


def _drain_state(state: _StreamState, hint: _HintTracker) -> bool:
    """Read a bounded slice of one pipe. Return True at EOF or a read error."""
    for _ in range(MAX_READ_CHUNKS_PER_EVENT):
        try:
            chunk = os.read(state.fd, READ_CHUNK_BYTES)
        except BlockingIOError:
            return False
        except OSError:
            chunk = b""
        if not chunk:
            partial = state.lines.flush()
            if partial is not None:
                hint.feed_line(partial)
            return True
        _write_bytes(state.sink, chunk)
        for line in state.lines.feed(chunk):
            hint.feed_line(line)
    return False


def _signal_owned_group(proc: subprocess.Popen[bytes], sig: int) -> bool:
    """Signal the child's owned group only while the original child is live."""
    if proc.poll() is not None:
        return False
    try:
        if os.getpgid(proc.pid) != proc.pid:
            # Never risk signalling a group we do not own. The child is live,
            # so signalling only that pid is still safe.
            proc.send_signal(sig)
            return True
        os.killpg(proc.pid, sig)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        try:
            if proc.poll() is None:
                proc.send_signal(sig)
                return True
        except OSError:
            pass
        return False


def _terminate_for_interrupt(
    proc: subprocess.Popen[bytes],
    terminate_grace_seconds: float,
    kill_grace_seconds: float,
    stderr,
) -> None:
    if _signal_owned_group(proc, signal.SIGTERM):
        _write_text(stderr, "[run-ci-check] interrupt: sent SIGTERM to owned process group\n")
    try:
        proc.wait(timeout=terminate_grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    if _signal_owned_group(proc, signal.SIGKILL):
        _write_text(stderr, "[run-ci-check] interrupt: sent SIGKILL to owned process group\n")
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=kill_grace_seconds)


def _run_command(
    command: Sequence[str],
    timeout_seconds: float,
    heartbeat_seconds: float,
    terminate_grace_seconds: float,
    kill_grace_seconds: float,
    post_exit_drain_seconds: float,
    stdout,
    stderr,
) -> int:
    start = time.monotonic()
    deadline = start + timeout_seconds
    next_heartbeat = start + heartbeat_seconds
    hint = _HintTracker()

    try:
        proc = subprocess.Popen(
            list(command),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
            bufsize=0,
        )
    except FileNotFoundError:
        _write_text(stderr, f"[run-ci-check] executable not found: {command[0]!r}\n")
        return 127
    except OSError as exc:
        _write_text(stderr, f"[run-ci-check] could not start command: {exc}\n")
        return 126

    selector = selectors.DefaultSelector()
    states: dict[int, _StreamState] = {}
    for pipe, sink in (
        (proc.stdout, stdout),
        (proc.stderr, stderr),
    ):
        assert pipe is not None
        fd = pipe.fileno()
        os.set_blocking(fd, False)
        state = _StreamState(pipe=pipe, sink=sink, fd=fd)
        states[fd] = state
        selector.register(fd, selectors.EVENT_READ, state)

    timed_out = False
    term_sent_at: float | None = None
    kill_sent_at: float | None = None
    drain_deadline: float | None = None
    cleanup_incomplete = False
    child_exited = False
    child_returncode: int | None = None

    try:
        while True:
            now = time.monotonic()
            if not child_exited:
                returncode = proc.poll()
                if returncode is not None:
                    child_exited = True
                    child_returncode = returncode
                    drain_deadline = now + post_exit_drain_seconds

            if not child_exited and not timed_out and now >= deadline:
                # Re-check immediately before declaring a timeout: the child may
                # have exited while no output pipes remained to wake the selector.
                returncode = proc.poll()
                if returncode is not None:
                    child_exited = True
                    child_returncode = returncode
                    drain_deadline = now + post_exit_drain_seconds
                else:
                    timed_out = True
                    term_sent_at = now
                    if _signal_owned_group(proc, signal.SIGTERM):
                        _write_text(
                            stderr,
                            f"[run-ci-check] deadline {timeout_seconds:g}s reached; "
                            "sent SIGTERM to owned process group\n",
                        )

            if timed_out and not child_exited:
                now = time.monotonic()
                if (
                    kill_sent_at is None
                    and term_sent_at is not None
                    and now >= term_sent_at + terminate_grace_seconds
                ):
                    returncode = proc.poll()
                    if returncode is not None:
                        child_exited = True
                        child_returncode = returncode
                        drain_deadline = now + post_exit_drain_seconds
                    else:
                        sent = _signal_owned_group(proc, signal.SIGKILL)
                        kill_sent_at = now
                        if sent:
                            _write_text(
                                stderr,
                                "[run-ci-check] SIGTERM grace elapsed; "
                                "sent SIGKILL to owned process group\n",
                            )
                if (
                    kill_sent_at is not None
                    and not child_exited
                    and time.monotonic() >= kill_sent_at + kill_grace_seconds
                ):
                    returncode = proc.poll()
                    if returncode is not None:
                        child_exited = True
                        child_returncode = returncode
                        drain_deadline = time.monotonic() + post_exit_drain_seconds
                    else:
                        cleanup_incomplete = True
                        _write_text(
                            stderr,
                            "[run-ci-check] cleanup incomplete: child remained live "
                            "after SIGKILL; stopped waiting\n",
                        )
                        break

            if (
                child_exited
                and states
                and drain_deadline is not None
                and time.monotonic() >= drain_deadline
            ):
                cleanup_incomplete = True
                _write_text(
                    stderr,
                    "[run-ci-check] cleanup incomplete: inherited output pipe(s) "
                    "stayed open after child exit; closed them without signalling "
                    "descendants\n",
                )
                break

            if child_exited and not states:
                break

            now = time.monotonic()
            if not child_exited and now >= next_heartbeat:
                _write_text(
                    stderr,
                    _heartbeat_text(now - start, timeout_seconds, hint.hint) + "\n",
                )
                next_heartbeat += heartbeat_seconds
                if next_heartbeat <= now:
                    next_heartbeat = now + heartbeat_seconds

            now = time.monotonic()
            waits: list[float] = []
            if not child_exited:
                if not states:
                    # No output pipes remain to wake the selector; keep polling
                    # the child so completion is noticed before the deadline.
                    waits.append(NO_PIPE_POLL_SECONDS)
                if not timed_out:
                    waits.append(max(0.0, deadline - now))
                waits.append(max(0.0, next_heartbeat - now))
                if timed_out:
                    if kill_sent_at is None and term_sent_at is not None:
                        waits.append(
                            max(0.0, term_sent_at + terminate_grace_seconds - now)
                        )
                    elif kill_sent_at is not None:
                        waits.append(max(0.0, kill_sent_at + kill_grace_seconds - now))
            elif drain_deadline is not None:
                waits.append(max(0.0, drain_deadline - now))

            for key, _ in selector.select(min(waits) if waits else 0.1):
                state = key.data
                if _drain_state(state, hint):
                    selector.unregister(state.fd)
                    states.pop(state.fd, None)
                    with contextlib.suppress(OSError):
                        state.pipe.close()
    except KeyboardInterrupt:
        _terminate_for_interrupt(
            proc,
            terminate_grace_seconds,
            kill_grace_seconds,
            stderr,
        )
        _write_text(stderr, "[run-ci-check] interrupted\n")
        return 130
    finally:
        for state in list(states.values()):
            with contextlib.suppress(KeyError, ValueError, OSError):
                selector.unregister(state.fd)
            with contextlib.suppress(OSError):
                state.pipe.close()
        states.clear()
        selector.close()

    elapsed = time.monotonic() - start
    if timed_out:
        _write_text(
            stderr,
            f"[run-ci-check] command exceeded {timeout_seconds:g}s after "
            f"{elapsed:.1f}s; returning {EXIT_TIMEOUT}\n",
        )
        return EXIT_TIMEOUT
    if cleanup_incomplete:
        if child_returncode in (None, 0):
            code = EXIT_CLEANUP_INCOMPLETE
        else:
            code = _child_exit_code(child_returncode)
        _write_text(
            stderr,
            f"[run-ci-check] cleanup incomplete after {elapsed:.1f}s; "
            f"returning {code}\n",
        )
        return code
    if child_returncode is None:
        _write_text(stderr, "[run-ci-check] command ended without an exit status\n")
        return 1
    return _child_exit_code(child_returncode)


def _build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="run-ci-check.py",
        description=(
            "Run one argv directly on Linux/POSIX with a hard deadline, "
            "streamed output, a fixed heartbeat, and bounded cleanup."
        ),
        epilog="Usage: run-ci-check.py --timeout-seconds SECONDS -- COMMAND [ARG ...]",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        required=True,
        metavar="SECONDS",
        help=f"positive hard deadline, at most {MAX_TIMEOUT_SECONDS:g} seconds",
    )
    return parser


def _validated_timeout(value: float) -> float:
    if not math.isfinite(value) or value <= 0 or value > MAX_TIMEOUT_SECONDS:
        raise _UsageError(
            f"--timeout-seconds must be greater than 0 and at most "
            f"{MAX_TIMEOUT_SECONDS:g}."
        )
    return float(value)


def _parse_args(argv: Sequence[str]) -> tuple[float, list[str]]:
    args = list(argv)
    separator = args.index("--") if "--" in args else len(args)
    if any(item in ("-h", "--help") for item in args[:separator]):
        _build_parser().print_help()
        raise SystemExit(0)
    if separator == len(args):
        raise _UsageError("missing '--' before the command argv")
    command = args[separator + 1 :]
    if not command:
        raise _UsageError("missing command after '--'")
    parsed = _build_parser().parse_args(args[:separator])
    return _validated_timeout(parsed.timeout_seconds), command


def _require_posix() -> None:
    if os.name != "posix":
        raise _UsageError("run-ci-check.py requires a Linux/POSIX host")


def _require_positive(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise _UsageError(f"{name} must be positive")


def main(
    argv: Sequence[str] | None = None,
    *,
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS,
    terminate_grace_seconds: float = TERMINATE_GRACE_SECONDS,
    kill_grace_seconds: float = KILL_GRACE_SECONDS,
    post_exit_drain_seconds: float = POST_EXIT_DRAIN_SECONDS,
) -> int:
    try:
        _require_posix()
        _require_positive("heartbeat interval", heartbeat_seconds)
        _require_positive("SIGTERM grace", terminate_grace_seconds)
        _require_positive("SIGKILL grace", kill_grace_seconds)
        _require_positive("post-exit drain", post_exit_drain_seconds)
        timeout_seconds, command = _parse_args(
            sys.argv[1:] if argv is None else argv
        )
        return _run_command(
            command,
            timeout_seconds,
            heartbeat_seconds,
            terminate_grace_seconds,
            kill_grace_seconds,
            post_exit_drain_seconds,
            sys.stdout,
            sys.stderr,
        )
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 2
    except _UsageError as exc:
        _write_text(sys.stderr, f"run-ci-check.py: error: {exc}\n")
        return 2
    except KeyboardInterrupt:
        _write_text(sys.stderr, "[run-ci-check] interrupted\n")
        return 130
    except Exception as exc:  # Keep the CLI boundary free of tracebacks.
        _write_text(
            sys.stderr,
            f"[run-ci-check] internal error: {type(exc).__name__}\n",
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
