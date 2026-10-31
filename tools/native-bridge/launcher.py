#!/usr/bin/env python3
"""Unactivated private diagnostic launcher prototype for bundled Codex app-server.

Intercepts only the native stdio app-server launch forms and replaces the backend
invocation with a single bundled backend listening on a private per-run Unix
WebSocket (`--listen unix://...`), bridging native stdin/stdout JSONL to
WebSocket text frames without inspecting or modifying protocol payloads.

All other CLI invocations are exec'd directly into the real binary unchanged.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Sequence

REAL_CODEX_ENV = "THREADLANE_REAL_CODEX"
RUNTIME_DIR_ENV = "XDG_RUNTIME_DIR"
SESSION_DIR_PREFIX = "tl-codex-"
SOCKET_FILENAME = "s.sock"
DESCRIPTOR_FILENAME = "descriptor.json"

MAX_SOCKET_PATH_BYTES = 100
MAX_FRAME_BYTES = 8 * 1024 * 1024
READ_CHUNK_BYTES = 64 * 1024
WS_MAX_QUEUE = 16
READY_TIMEOUT_SECONDS = 10.0
READY_POLL_INTERVAL_SECONDS = 0.02
OPEN_TIMEOUT_SECONDS = 5.0
CLOSE_TIMEOUT_SECONDS = 2.0
TERM_TIMEOUT_SECONDS = 2.0
KILL_TIMEOUT_SECONDS = 2.0

INTERCEPTED_ARGVS: tuple[ tuple[str, ...], ... ] = (
    ("app-server",),
    ("app-server", "--listen", "stdio://"),
    ("app-server", "--listen=stdio://"),
    ("app-server", "--stdio"),
)

OPAQUE_PAIR_FLAGS: frozenset[str] = frozenset(
    {"-c", "--config", "--enable", "--disable"}
)
PREFIX_BOOL_FLAGS: frozenset[str] = frozenset({"--strict-config"})
SUFFIX_BOOL_FLAGS: frozenset[str] = frozenset(
    {"--analytics-default-enabled", "--strict-config"}
)

FIXED_ERROR_DIAGNOSTIC = "threadlane-prototype: backend launch or bridge failed\n"


class LauncherError(Exception):
    """Raised when a safety precondition, handshake, or bridge invariant fails."""

    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class SignalTermination(Exception):
    """Raised when SIGTERM, SIGINT, or SIGHUP stops the bridge."""

    def __init__(self, signum: int) -> None:
        super().__init__(f"signal {signum}")
        self.signum = signum


def plan_backend_argv(
    args: Sequence[str],
    socket_path: str,
) -> list[str] | None:
    """Plan backend argv for known safe stdio app-server CLI shapes, or return None.

    Recognizes leading global opaque flags (`-c VALUE`, `--config VALUE`,
    `--config=VALUE`, `--enable VALUE`, `--disable VALUE`, `--strict-config`)
    before `app-server`, and whitelisted `app-server` suffix flags
    (`--analytics-default-enabled`, `--strict-config`, the same `-c`/`--config`/
    `--config=..`/`--enable`/`--disable` forms, and at most one explicit stdio
    listener `--stdio`, `--listen stdio://`, or `--listen=stdio://`).

    Preserves all opaque tokens and flags in order, strips any explicit stdio
    listener flag/pair, and appends `['--listen', 'unix://' + socket_path]`.
    """
    n = len(args)
    if n == 0:
        return None

    planned: list[str] = []
    i = 0
    found_command = False

    while i < n:
        token = args[i]
        if token == "app-server":
            planned.append(token)
            i += 1
            found_command = True
            break
        if token in OPAQUE_PAIR_FLAGS:
            if i + 1 >= n:
                return None
            planned.append(token)
            planned.append(args[i + 1])
            i += 2
            continue
        if token.startswith("--config="):
            if len(token) <= len("--config="):
                return None
            planned.append(token)
            i += 1
            continue
        if token in PREFIX_BOOL_FLAGS:
            planned.append(token)
            i += 1
            continue
        return None

    if not found_command:
        return None

    stdio_listener_count = 0
    while i < n:
        token = args[i]
        if token in SUFFIX_BOOL_FLAGS:
            planned.append(token)
            i += 1
            continue
        if token in OPAQUE_PAIR_FLAGS:
            if i + 1 >= n:
                return None
            planned.append(token)
            planned.append(args[i + 1])
            i += 2
            continue
        if token.startswith("--config="):
            if len(token) <= len("--config="):
                return None
            planned.append(token)
            i += 1
            continue
        if token == "--stdio":
            stdio_listener_count += 1
            if stdio_listener_count > 1:
                return None
            i += 1
            continue
        if token == "--listen":
            if i + 1 >= n or args[i + 1] != "stdio://":
                return None
            stdio_listener_count += 1
            if stdio_listener_count > 1:
                return None
            i += 2
            continue
        if token.startswith("--listen="):
            if token != "--listen=stdio://":
                return None
            stdio_listener_count += 1
            if stdio_listener_count > 1:
                return None
            i += 1
            continue
        return None

    planned.extend(["--listen", f"unix://{socket_path}"])
    return planned


def is_intercepted_argv(args: Sequence[str]) -> bool:
    """Return True only for supported stdio app-server launch forms."""
    return plan_backend_argv(args, SOCKET_FILENAME) is not None


def validate_real_codex(
    real_codex: str | None,
    launcher_path: str | None = None,
) -> str:
    """Validate that THREADLANE_REAL_CODEX is an executable absolute file distinct from launcher."""
    if not real_codex or not os.path.isabs(real_codex):
        raise LauncherError("invalid real codex path")

    try:
        st = os.stat(real_codex)
    except OSError as exc:
        raise LauncherError("real codex missing") from exc

    if not stat.S_ISREG(st.st_mode):
        raise LauncherError("real codex is not a regular file")

    if not os.access(real_codex, os.X_OK):
        raise LauncherError("real codex is not executable")

    self_candidates = [__file__]
    if launcher_path:
        self_candidates.append(launcher_path)

    real_resolved = os.path.realpath(real_codex)
    for candidate in self_candidates:
        try:
            if os.path.realpath(candidate) == real_resolved or os.path.samefile(
                real_codex, candidate
            ):
                raise LauncherError("real codex resolves to launcher")
        except FileNotFoundError:
            if os.path.realpath(candidate) == real_resolved:
                raise LauncherError("real codex resolves to launcher")

    return real_codex


def exec_passthrough(real_codex: str, args: Sequence[str]) -> None:
    """Exec the real binary directly with argv preserved byte-for-byte."""
    argv_bytes = [os.fsencode(real_codex), *(os.fsencode(a) for a in args)]
    os.execv( os.fsencode(real_codex), argv_bytes )


def validate_runtime_root(runtime_root: str | None, uid: int | None = None) -> str:
    """Require XDG_RUNTIME_DIR to be an existing, non-symlink 0700 directory owned by uid."""
    if uid is None:
        uid = os.getuid()

    if not runtime_root or not os.path.isabs(runtime_root):
        raise LauncherError("missing or relative runtime root")

    normalized = os.path.normpath(runtime_root)
    try:
        st = os.lstat(normalized)
    except OSError as exc:
        raise LauncherError("runtime root unavailable") from exc

    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise LauncherError("runtime root must be a real directory")

    if os.path.realpath(normalized) != normalized:
        raise LauncherError("runtime root must not traverse symlinks")

    if st.st_uid != uid:
        raise LauncherError("runtime root uid mismatch")

    if stat.S_IMODE(st.st_mode) != 0o700:
        raise LauncherError("runtime root permissions must be 0700")

    return normalized


class SessionCleanupTracker:
    """Tracks owned per-run artifacts by path and (dev, ino) for safe non-recursive cleanup."""

    def __init__(
        self,
        session_dir: str,
        session_dir_id: tuple[int, int],
        advertised_socket_path: str,
        owner_uid: int,
    ) -> None:
        self.session_dir = session_dir
        self.session_dir_id = session_dir_id
        self.advertised_socket_path = advertised_socket_path
        self.owner_uid = owner_uid
        self.descriptor_path: str | None = None
        self.descriptor_id: tuple[int, int] | None = None

    def record_descriptor(self, path: str, file_id: tuple[int, int]) -> None:
        self.descriptor_path = path
        self.descriptor_id = file_id

    def _session_dir_still_owned(self) -> bool:
        try:
            st = os.lstat(self.session_dir)
        except OSError:
            return False
        return (
            stat.S_ISDIR(st.st_mode)
            and not stat.S_ISLNK(st.st_mode)
            and (st.st_dev, st.st_ino) == self.session_dir_id
            and st.st_uid == self.owner_uid
        )

    def cleanup(self) -> None:
        """Remove only owned descriptor, advertised socket/symlink, and empty session dir."""
        if not self._session_dir_still_owned():
            return

        if self.descriptor_path and self.descriptor_id is not None:
            try:
                dst = os.lstat(self.descriptor_path)
                if (
                    (dst.st_dev, dst.st_ino) == self.descriptor_id
                    and dst.st_uid == self.owner_uid
                    and stat.S_ISREG(dst.st_mode)
                ):
                    os.unlink(self.descriptor_path)
            except FileNotFoundError:
                pass
            except OSError:
                pass

        if (
            self.advertised_socket_path
            and os.path.dirname(self.advertised_socket_path) == self.session_dir
        ):
            try:
                sst = os.lstat(self.advertised_socket_path)
                if sst.st_uid == self.owner_uid and (
                    stat.S_ISSOCK(sst.st_mode) or stat.S_ISLNK(sst.st_mode)
                ):
                    os.unlink(self.advertised_socket_path)
            except FileNotFoundError:
                pass
            except OSError:
                pass

        if self._session_dir_still_owned():
            try:
                os.rmdir(self.session_dir)
            except OSError:
                # Preserve directory if non-empty or already removed.
                pass


def create_private_session_dir(
    runtime_root: str, uid: int | None = None
) -> SessionCleanupTracker:
    """Create a fresh 0700 per-run directory and verify socket path length."""
    if uid is None:
        uid = os.getuid()

    validated_root = validate_runtime_root(runtime_root, uid=uid)
    os.umask(0o077)
    session_dir = tempfile.mkdtemp(prefix=SESSION_DIR_PREFIX, dir=validated_root)
    os.chmod(session_dir, 0o700)

    st = os.lstat(session_dir)
    if (
        not stat.S_ISDIR(st.st_mode)
        or stat.S_ISLNK(st.st_mode)
        or st.st_uid != uid
        or stat.S_IMODE(st.st_mode) != 0o700
    ):
        try:
            os.rmdir(session_dir)
        except OSError:
            pass
        raise LauncherError("failed to secure private session dir")

    socket_path = os.path.join(session_dir, SOCKET_FILENAME)
    tracker = SessionCleanupTracker(
        session_dir=session_dir,
        session_dir_id=(st.st_dev, st.st_ino),
        advertised_socket_path=socket_path,
        owner_uid=uid,
    )

    if len(os.fsencode(socket_path)) >= MAX_SOCKET_PATH_BYTES:
        tracker.cleanup()
        raise LauncherError("socket path exceeds unix domain socket length limit")

    return tracker


def validate_ready_socket(tracker: SessionCleanupTracker) -> bool:
    """Check whether the advertised socket is present and satisfies all ownership/mode rules."""
    if not tracker._session_dir_still_owned():
        raise LauncherError("private session directory replaced or modified")

    socket_path = tracker.advertised_socket_path
    if os.path.dirname(os.path.normpath(socket_path)) != os.path.normpath(
        tracker.session_dir
    ):
        raise LauncherError("advertised socket outside private session directory")

    try:
        adv_st = os.lstat(socket_path)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise LauncherError("unable to stat advertised socket") from exc

    if adv_st.st_uid != tracker.owner_uid:
        raise LauncherError("advertised socket owner mismatch")

    if stat.S_ISLNK(adv_st.st_mode):
        resolved_path = os.path.realpath(socket_path)
        try:
            target_st = os.lstat(resolved_path)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise LauncherError("unable to stat resolved socket target") from exc
    elif stat.S_ISSOCK(adv_st.st_mode):
        resolved_path = os.path.realpath(socket_path)
        if resolved_path != os.path.abspath(socket_path):
            raise LauncherError("unexpected path resolution for direct socket")
        target_st = adv_st
    else:
        raise LauncherError("advertised path is neither socket nor symlink")

    if not stat.S_ISSOCK(target_st.st_mode):
        raise LauncherError("resolved socket target is not a unix socket")

    if target_st.st_uid != tracker.owner_uid:
        raise LauncherError("resolved socket target owner mismatch")

    if (stat.S_IMODE(target_st.st_mode) & 0o077) != 0:
        raise LauncherError("resolved socket target has group or world permissions")

    target_parent = os.path.dirname(resolved_path)
    try:
        parent_st = os.lstat(target_parent)
    except OSError as exc:
        raise LauncherError("unable to stat resolved socket parent") from exc

    if not stat.S_ISDIR(parent_st.st_mode) or stat.S_ISLNK(parent_st.st_mode):
        raise LauncherError("resolved socket parent is not a real directory")

    if os.path.realpath(target_parent) != os.path.abspath(target_parent):
        raise LauncherError("resolved socket parent traverses symlinks")

    if parent_st.st_uid != tracker.owner_uid:
        raise LauncherError("resolved socket parent owner mismatch")

    if stat.S_IMODE(parent_st.st_mode) != 0o700:
        raise LauncherError("resolved socket parent permissions must be 0700")

    return True


def write_descriptor(
    tracker: SessionCleanupTracker,
    backend_pid: int,
    real_codex: str,
) -> str:
    """Write mode 0600 descriptor.json inside the private per-run directory."""
    if not tracker._session_dir_still_owned():
        raise LauncherError("session dir invalid before writing descriptor")

    descriptor_path = os.path.join(tracker.session_dir, DESCRIPTOR_FILENAME)
    payload = {
        "schema_version": 1,
        "wrapper_pid": os.getpid(),
        "backend_pid": backend_pid,
        "socket_path": tracker.advertised_socket_path,
        "real_codex": real_codex,
        "transport": "unix_websocket",
    }

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(descriptor_path, flags, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    os.chmod(descriptor_path, 0o600)
    dst = os.lstat(descriptor_path)
    tracker.record_descriptor(descriptor_path, (dst.st_dev, dst.st_ino))
    return descriptor_path


def _exit_code_from_returncode(returncode: int | None) -> int:
    if returncode is None or returncode == 0:
        return 1
    if returncode > 0:
        return returncode
    return 128 + abs(returncode)


def terminate_backend_process_group(
    proc: subprocess.Popen[bytes] | None,
    term_timeout: float = TERM_TIMEOUT_SECONDS,
    kill_timeout: float = KILL_TIMEOUT_SECONDS,
) -> None:
    """Terminate and reap only the backend process group created by this launcher."""
    if proc is None:
        return

    pgid = proc.pid
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except PermissionError:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass

    try:
        proc.wait(timeout=term_timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired:
            pass

    # Ensure no lingering processes remain in the dedicated process group.
    try:
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


async def read_bounded_line(
    reader: asyncio.StreamReader,
    buf: bytearray,
    max_line_bytes: int = MAX_FRAME_BYTES,
    chunk_size: int = READ_CHUNK_BYTES,
) -> bytes | None:
    """Read a single newline-delimited line from reader while bounding memory to max_line_bytes."""
    while True:
        nl_pos = buf.find(b"\n")
        if nl_pos != -1:
            raw_line = bytes(buf[:nl_pos])
            del buf[: nl_pos + 1]
            if len(raw_line) > max_line_bytes:
                raise LauncherError("stdin line exceeds maximum size")
            return raw_line

        if len(buf) > max_line_bytes:
            raise LauncherError("stdin line exceeds maximum size")

        chunk = await reader.read(chunk_size)
        if not chunk:
            if not buf:
                return None
            raw_line = bytes(buf)
            buf.clear()
            if len(raw_line) > max_line_bytes:
                raise LauncherError("stdin line exceeds maximum size")
            return raw_line

        buf.extend(chunk)
        if buf.find(b"\n") == -1 and len(buf) > max_line_bytes:
            raise LauncherError("stdin line exceeds maximum size")


async def _wait_for_socket_ready(
    proc: subprocess.Popen[bytes],
    tracker: SessionCleanupTracker,
    ready_timeout: float = READY_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + ready_timeout
    while True:
        rc = proc.poll()
        if rc is not None:
            raise LauncherError(
                "backend exited before socket ready",
                exit_code=_exit_code_from_returncode(rc),
            )

        if validate_ready_socket(tracker):
            return

        if time.monotonic() >= deadline:
            raise LauncherError("timed out waiting for backend socket")

        await asyncio.sleep(READY_POLL_INTERVAL_SECONDS)


async def _connect_stdio_pipes(
    loop: asyncio.AbstractEventLoop,
) -> tuple[
    asyncio.StreamReader,
    asyncio.BaseTransport,
    asyncio.StreamWriter,
    asyncio.BaseTransport,
]:
    stdin_fd = os.dup(sys.stdin.fileno())
    stdin_file = os.fdopen(stdin_fd, "rb", buffering=0)
    stdin_reader = asyncio.StreamReader(limit=READ_CHUNK_BYTES)
    stdin_protocol = asyncio.StreamReaderProtocol(stdin_reader)
    read_transport, _ = await loop.connect_read_pipe(
        lambda: stdin_protocol, stdin_file
    )

    stdout_fd = os.dup(sys.stdout.fileno())
    stdout_file = os.fdopen(stdout_fd, "wb", buffering=0)
    stdout_reader = asyncio.StreamReader(limit=READ_CHUNK_BYTES)
    stdout_protocol = asyncio.StreamReaderProtocol(stdout_reader)
    write_transport, _ = await loop.connect_write_pipe(
        lambda: stdout_protocol, stdout_file
    )
    stdout_writer = asyncio.StreamWriter(
        write_transport, stdout_protocol, None, loop
    )

    return stdin_reader, read_transport, stdout_writer, write_transport


async def _forward_stdin_to_ws(
    stdin_reader: asyncio.StreamReader,
    ws: object,
    stdin_eof_event: asyncio.Event,
    max_line_bytes: int = MAX_FRAME_BYTES,
) -> str:
    from websockets.exceptions import ConnectionClosed

    buf = bytearray()
    send_fn = getattr(ws, "send")
    close_fn = getattr(ws, "close")

    while True:
        raw_line = await read_bounded_line(
            stdin_reader, buf, max_line_bytes=max_line_bytes
        )
        if raw_line is None:
            stdin_eof_event.set()
            try:
                await asyncio.wait_for(close_fn(), timeout=CLOSE_TIMEOUT_SECONDS)
            except (ConnectionClosed, asyncio.TimeoutError, OSError):
                pass
            return "stdin_eof"

        try:
            text_frame = raw_line.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise LauncherError("malformed utf-8 on stdin") from exc

        try:
            await send_fn(text_frame)
        except ConnectionClosed as exc:
            raise LauncherError("websocket closed while sending") from exc


async def _forward_ws_to_stdout(
    ws: object,
    stdout_writer: asyncio.StreamWriter,
    max_line_bytes: int = MAX_FRAME_BYTES,
) -> str:
    from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

    try:
        async for message in ws:  # type: ignore[union-attr]
            if not isinstance(message, str):
                raise LauncherError("binary websocket frame rejected")

            if "\n" in message or "\r" in message:
                raise LauncherError("newline inside websocket frame rejected")

            try:
                encoded = message.encode("utf-8", errors="strict")
            except UnicodeEncodeError as exc:
                raise LauncherError("invalid utf-8 in websocket frame") from exc

            if len(encoded) > max_line_bytes:
                raise LauncherError("websocket frame exceeds maximum size")

            stdout_writer.write(encoded + b"\n")
            await stdout_writer.drain()
    except ConnectionClosedOK:
        return "ws_closed"
    except ConnectionClosedError as exc:
        raise LauncherError("websocket connection closed abnormally") from exc

    return "ws_closed"


async def _watch_backend_process(proc: subprocess.Popen[bytes]) -> str:
    while proc.poll() is None:
        await asyncio.sleep(READY_POLL_INTERVAL_SECONDS)
    return "proc_exited"


async def _run_intercepted_async(
    real_codex: str,
    runtime_root: str | None,
    original_argv: Sequence[str] = ("app-server",),
    ready_timeout: float = READY_TIMEOUT_SECONDS,
    max_line_bytes: int = MAX_FRAME_BYTES,
) -> int:
    if os.name != "posix" or sys.platform == "win32":
        raise LauncherError("unix platform required")

    from websockets.asyncio.client import unix_connect

    tracker = create_private_session_dir(runtime_root)
    backend_argv = plan_backend_argv(
        original_argv, tracker.advertised_socket_path
    )
    if backend_argv is None:
        tracker.cleanup()
        raise LauncherError("unsupported app-server argv")

    proc: subprocess.Popen[bytes] | None = None
    read_transport: asyncio.BaseTransport | None = None
    write_transport: asyncio.BaseTransport | None = None
    received_signal: int | None = None

    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()

    def _on_signal(signum: int) -> None:
        nonlocal received_signal
        if received_signal is None:
            received_signal = signum
        if main_task is not None and not main_task.done():
            main_task.cancel()

    registered_signals: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        try:
            loop.add_signal_handler(sig, _on_signal, int(sig))
            registered_signals.append(sig)
        except (NotImplementedError, RuntimeError, ValueError):
            pass

    try:
        try:
            proc = subprocess.Popen(
                [real_codex, *backend_argv],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=None,
                start_new_session=True,
                close_fds=True,
            )
        except OSError as exc:
            raise LauncherError("failed to spawn backend") from exc

        await _wait_for_socket_ready(proc, tracker, ready_timeout=ready_timeout)

        stdin_reader, read_transport, stdout_writer, write_transport = (
            await _connect_stdio_pipes(loop)
        )

        try:
            ws_conn = unix_connect(
                path=tracker.advertised_socket_path,
                uri="ws://localhost/",
                open_timeout=OPEN_TIMEOUT_SECONDS,
                close_timeout=CLOSE_TIMEOUT_SECONDS,
                max_size=max_line_bytes,
                max_queue=WS_MAX_QUEUE,
                compression=None,
            )
            async with ws_conn as ws:
                if not validate_ready_socket(tracker):
                    raise LauncherError("socket invalidated during handshake")

                descriptor_path = write_descriptor(
                    tracker=tracker,
                    backend_pid=proc.pid,
                    real_codex=real_codex,
                )
                sys.stderr.write(
                    f"threadlane-prototype-descriptor: {descriptor_path}\n"
                )
                sys.stderr.flush()

                stdin_eof_event = asyncio.Event()
                stdin_task = asyncio.create_task(
                    _forward_stdin_to_ws(
                        stdin_reader,
                        ws,
                        stdin_eof_event=stdin_eof_event,
                        max_line_bytes=max_line_bytes,
                    )
                )
                ws_task = asyncio.create_task(
                    _forward_ws_to_stdout(
                        ws, stdout_writer, max_line_bytes=max_line_bytes
                    )
                )
                proc_task = asyncio.create_task(_watch_backend_process(proc))
                tasks = {stdin_task, ws_task, proc_task}

                try:
                    done, _ = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )

                    for finished in done:
                        exc = finished.exception()
                        if exc is not None:
                            raise exc

                    if stdin_eof_event.is_set():
                        await asyncio.wait(
                            {stdin_task, ws_task}, timeout=CLOSE_TIMEOUT_SECONDS
                        )
                        for io_task in (stdin_task, ws_task):
                            if io_task.done():
                                io_exc = io_task.exception()
                                if io_exc is not None:
                                    raise io_exc
                        observed_exit = proc.poll()
                        if observed_exit not in (None, 0):
                            raise LauncherError(
                                "backend failed while stdin closed",
                                exit_code=_exit_code_from_returncode(observed_exit),
                            )
                        return 0

                    # Backend exited or closed WebSocket before stdin EOF.
                    if proc.poll() is None:
                        try:
                            await asyncio.wait_for(proc_task, timeout=0.2)
                        except asyncio.TimeoutError:
                            pass

                    raise LauncherError(
                        "backend closed before stdin eof",
                        exit_code=_exit_code_from_returncode(proc.poll()),
                    )
                finally:
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except LauncherError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            rc = proc.poll() if proc is not None else None
            exit_code = _exit_code_from_returncode(rc) if rc not in (None, 0) else 1
            raise LauncherError(
                "websocket handshake or bridge error", exit_code=exit_code
            ) from exc
    except asyncio.CancelledError as exc:
        if received_signal is not None:
            raise SignalTermination(received_signal) from exc
        raise LauncherError("bridge cancelled") from exc
    finally:
        for sig in registered_signals:
            try:
                loop.remove_signal_handler(sig)
            except Exception:
                pass
        if read_transport is not None:
            read_transport.close()
        if write_transport is not None:
            write_transport.close()
        terminate_backend_process_group(proc)
        tracker.cleanup()


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the prototype Codex wrapper."""
    args = list(sys.argv[1:] if argv is None else argv)

    try:
        real_codex = validate_real_codex(
            os.environ.get(REAL_CODEX_ENV),
            launcher_path=sys.argv[0] if sys.argv else None,
        )
    except LauncherError:
        sys.stderr.write(FIXED_ERROR_DIAGNOSTIC)
        sys.stderr.flush()
        return 1

    if not is_intercepted_argv(args):
        try:
            exec_passthrough(real_codex, args)
        except OSError:
            sys.stderr.write(FIXED_ERROR_DIAGNOSTIC)
            sys.stderr.flush()
            return 1

    try:
        return asyncio.run(
            _run_intercepted_async(
                real_codex=real_codex,
                runtime_root=os.environ.get(RUNTIME_DIR_ENV),
                original_argv=args,
            )
        )
    except SignalTermination as sig_exc:
        return 128 + sig_exc.signum
    except LauncherError as launch_exc:
        sys.stderr.write(FIXED_ERROR_DIAGNOSTIC)
        sys.stderr.flush()
        return launch_exc.exit_code
    except KeyboardInterrupt:
        return 130
    except Exception:
        sys.stderr.write(FIXED_ERROR_DIAGNOSTIC)
        sys.stderr.flush()
        return 1


if __name__ == "__main__":
    sys.exit(main())
