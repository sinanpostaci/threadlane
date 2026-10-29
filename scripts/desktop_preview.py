#!/usr/bin/env python3
"""Local launcher for the Milestone 3A Threadlane desktop preview and owned demo runtime.

Starts an isolated owned demo supervisor (`threadlane serve --state-dir <preview>/state`,
with Codex wake-ups and worker profiles disabled), waits for readiness via the trusted
CLI (`threadlane list-bindings`), and launches the standalone `threadlane-desktop` binary
pointed at `--demo-state-dir` (plus an optional read-only observer `--state-dir`).
When the GUI exits, only the owned demo supervisor is stopped and reaped; any attached
external runtime and all private diagnostic logs are preserved.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Iterator, Mapping, NamedTuple, NoReturn

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from live_worker_pilot import build_sanitized_child_env

REPO_ROOT = _SCRIPTS_DIR.parent
_EXE_SUFFIX = ".exe" if os.name == "nt" else ""
DEFAULT_THREADLANE_BIN = REPO_ROOT / "target" / "debug" / f"threadlane{_EXE_SUFFIX}"
DEFAULT_DESKTOP_BIN = REPO_ROOT / "target" / "desktop" / "debug" / f"threadlane-desktop{_EXE_SUFFIX}"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / ".local" / "desktop-previews"

SUPERVISOR_READY_TIMEOUT_SECONDS = 10.0
CLI_CALL_TIMEOUT_SECONDS = 10.0
POLL_INTERVAL_SECONDS = 0.05
MAX_CLI_OUTPUT_BYTES = 131_072
MAX_LOG_SNIPPET_BYTES = 16_384

_HEX_TOKEN_RE = re.compile(r"\b[0-9a-fA-F]{64,}\b")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+\S+")
_JSON_TOKEN_RE = re.compile(r'("token"\s*:\s*)"[^"]*"')


class PreviewError(RuntimeError):
    def __init__(self, error_class: str) -> None:
        super().__init__(error_class)
        self.error_class = error_class


class PreviewConfig(NamedTuple):
    threadlane: Path
    desktop: Path
    state_dir: Path | None
    output_root: Path
    dry_run: bool


def redact_sensitive_text(raw: str) -> str:
    redacted = _HEX_TOKEN_RE.sub("[REDACTED_TOKEN]", raw)
    redacted = _BEARER_RE.sub("Bearer [REDACTED_TOKEN]", redacted)
    redacted = _JSON_TOKEN_RE.sub(r'\1"[REDACTED_TOKEN]"', redacted)
    return redacted[:MAX_LOG_SNIPPET_BYTES]


def validate_clean_absolute_path(raw: str, error_class: str = "invalid_path_argument") -> Path:
    if (
        not isinstance(raw, str)
        or not raw
        or raw.strip() != raw
        or raw.startswith("-")
        or any(ord(ch) < 32 or ord(ch) == 127 for ch in raw)
    ):
        raise PreviewError(error_class)
    if os.name != "nt":
        if (
            not raw.startswith("/")
            or raw == "/"
            or raw.endswith("/")
            or "//" in raw
            or any(seg in ("", ".", "..") for seg in raw.split("/")[1:])
        ):
            raise PreviewError(error_class)
    else:
        candidate = Path(raw)
        if (
            not candidate.is_absolute()
            or raw.endswith(("/", "\\"))
            or any(seg in (".", "..") for seg in re.split(r"[\\/]+", raw) if seg)
        ):
            raise PreviewError(error_class)
    return Path(raw)


def _paths_overlap(first: Path, second: Path) -> bool:
    if first == second:
        return True
    first_prefix = str(first).rstrip("/\\") + os.sep
    second_prefix = str(second).rstrip("/\\") + os.sep
    return str(first).startswith(second_prefix) or str(second).startswith(first_prefix)


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise PreviewError("invalid_cli_arguments")


def parse_and_validate_args(argv: list[str] | None = None) -> PreviewConfig:
    parser = _SafeArgumentParser(prog="desktop_preview.py", add_help=True)
    parser.add_argument("--threadlane", default=str(DEFAULT_THREADLANE_BIN))
    parser.add_argument("--desktop", default=str(DEFAULT_DESKTOP_BIN))
    parser.add_argument("--state-dir", default=None)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--dry-run", action="store_true")
    try:
        ns = parser.parse_args(argv)
    except SystemExit as exc:
        if exc.code in (0, None):
            raise
        raise PreviewError("invalid_cli_arguments") from exc

    threadlane_path = validate_clean_absolute_path(ns.threadlane, "invalid_threadlane_path")
    desktop_path = validate_clean_absolute_path(ns.desktop, "invalid_desktop_path")
    output_root = validate_clean_absolute_path(ns.output_root, "invalid_output_root_path")
    state_dir = (
        validate_clean_absolute_path(ns.state_dir, "invalid_state_dir_path")
        if ns.state_dir is not None
        else None
    )
    if state_dir is not None and _paths_overlap(state_dir, output_root):
        raise PreviewError("state_dir_overlaps_output_root")

    return PreviewConfig(
        threadlane=threadlane_path,
        desktop=desktop_path,
        state_dir=state_dir,
        output_root=output_root,
        dry_run=bool(ns.dry_run),
    )


def build_supervisor_argv(threadlane_bin: Path, demo_state_dir: Path) -> list[str]:
    return [str(threadlane_bin), "serve", "--state-dir", str(demo_state_dir)]


def build_readiness_argv(threadlane_bin: Path, demo_state_dir: Path) -> list[str]:
    return [str(threadlane_bin), "list-bindings", "--state-dir", str(demo_state_dir)]


def build_desktop_argv(desktop_bin: Path, demo_state_dir: Path, state_dir: Path | None) -> list[str]:
    argv = [str(desktop_bin), "--demo-state-dir", str(demo_state_dir)]
    if state_dir is not None:
        argv.extend(["--state-dir", str(state_dir)])
    return argv


def build_session_layout(output_root: Path, session_id: str) -> dict[str, str]:
    session_dir = output_root / session_id
    demo_state_dir = session_dir / "state"
    logs_dir = session_dir / "logs"
    return {
        "output_root": str(output_root),
        "session_dir": str(session_dir),
        "demo_state_dir": str(demo_state_dir),
        "logs_dir": str(logs_dir),
        "supervisor_stdout_log": str(logs_dir / "demo_supervisor.stdout.log"),
        "supervisor_stderr_log": str(logs_dir / "demo_supervisor.stderr.log"),
        "desktop_stdout_log": str(logs_dir / "desktop.stdout.log"),
        "desktop_stderr_log": str(logs_dir / "desktop.stderr.log"),
        "cli_diagnostics_log": str(logs_dir / "cli_diagnostics.log"),
        "summary_path": str(session_dir / "preview_summary.json"),
    }


def build_dry_run_plan(config: PreviewConfig, session_id: str | None = None) -> dict[str, Any]:
    sid = session_id if session_id is not None else str(uuid.uuid4())
    layout = build_session_layout(config.output_root, sid)
    demo_state_dir = Path(layout["demo_state_dir"])
    return {
        "mode": "dry_run",
        "live_execution": False,
        "paid_workers_enabled": False,
        "codex_wakeup_enabled": False,
        "threadlane_executable": str(config.threadlane),
        "desktop_executable": str(config.desktop),
        "attached_state_dir": str(config.state_dir) if config.state_dir is not None else None,
        "output_root": str(config.output_root),
        "session_id": sid,
        "planned_layout": {k: v for k, v in layout.items() if k != "output_root"},
        "planned_commands": {
            "demo_supervisor_argv": build_supervisor_argv(config.threadlane, demo_state_dir),
            "readiness_probe_argv": build_readiness_argv(config.threadlane, demo_state_dir),
            "desktop_argv": build_desktop_argv(config.desktop, demo_state_dir, config.state_dir),
        },
    }


def validate_no_symlink_ancestors(path: Path) -> None:
    anchor = path.anchor or "/"
    current = Path(anchor)
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for idx, part in enumerate(parts):
        current = current / part
        with contextlib.suppress(FileNotFoundError):
            st = os.lstat(current)
            if stat.S_ISLNK(st.st_mode) or (idx < len(parts) - 1 and not stat.S_ISDIR(st.st_mode)):
                raise PreviewError("symlink_or_invalid_ancestor_refused")


def validate_executable_path(path: Path, *, missing_error: str = "executable_not_found") -> None:
    validate_no_symlink_ancestors(path)
    try:
        st = os.lstat(path)
    except FileNotFoundError as exc:
        raise PreviewError(missing_error) from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise PreviewError("invalid_executable")
    if os.name != "nt" and (stat.S_IMODE(st.st_mode) & 0o111) == 0:
        raise PreviewError("invalid_executable")


def validate_optional_state_dir(state_dir: Path | None) -> None:
    if state_dir is None:
        return
    validate_no_symlink_ancestors(state_dir)
    with contextlib.suppress(FileNotFoundError):
        st = os.lstat(state_dir)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise PreviewError("invalid_state_dir")


def _verify_owned_dir(path: Path, *, require_private_mode: bool = True) -> None:
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise PreviewError("private_directory_verification_failed")
    if hasattr(os, "getuid") and st.st_uid != os.getuid():
        raise PreviewError("private_directory_verification_failed")
    if os.name != "nt" and require_private_mode and stat.S_IMODE(st.st_mode) != 0o700:
        raise PreviewError("private_directory_verification_failed")


def create_private_dir(path: Path, *, require_existing_parent: bool = False) -> Path:
    if os.path.lexists(path):
        raise PreviewError("preexisting_output_refused")
    validate_no_symlink_ancestors(path)
    if require_existing_parent:
        try:
            pst = os.lstat(path.parent)
        except FileNotFoundError as exc:
            raise PreviewError("output_parent_missing") from exc
        if stat.S_ISLNK(pst.st_mode) or not stat.S_ISDIR(pst.st_mode):
            raise PreviewError("invalid_output_parent")
    try:
        os.mkdir(path, 0o700)
    except FileExistsError as exc:
        raise PreviewError("preexisting_output_refused") from exc
    if os.name != "nt":
        os.chmod(path, 0o700)
    _verify_owned_dir(path, require_private_mode=True)
    return path


def ensure_private_output_root(output_root: Path) -> Path:
    validate_no_symlink_ancestors(output_root)
    if os.path.lexists(output_root):
        try:
            st = os.lstat(output_root)
        except FileNotFoundError as exc:
            raise PreviewError("invalid_output_root") from exc
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise PreviewError("invalid_output_root")
        if hasattr(os, "getuid") and st.st_uid != os.getuid():
            raise PreviewError("invalid_output_root")
        return output_root

    if output_root == DEFAULT_OUTPUT_ROOT and not os.path.lexists(output_root.parent):
        create_private_dir(output_root.parent, require_existing_parent=True)

    return create_private_dir(output_root, require_existing_parent=True)


def write_private_file(path: Path, content: bytes, *, allow_overwrite: bool = False) -> None:
    validate_no_symlink_ancestors(path)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_BINARY", 0)
        | (os.O_TRUNC if allow_overwrite else os.O_EXCL)
    )
    fd = os.open(path, flags, 0o600)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        elif os.name != "nt":
            os.chmod(path, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise PreviewError("private_file_write_failed")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def open_private_log(path: Path):
    validate_no_symlink_ancestors(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    if hasattr(os, "fchmod"):
        os.fchmod(fd, 0o600)
    elif os.name != "nt":
        os.chmod(path, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")


def probe_supervisor_ready(
    threadlane_bin: Path,
    demo_state_dir: Path,
    log_path: Path,
    *,
    env: Mapping[str, str] | None = None,
    timeout: float = CLI_CALL_TIMEOUT_SECONDS,
) -> list[Any]:
    argv = build_readiness_argv(threadlane_bin, demo_state_dir)
    child_env = build_sanitized_child_env(env)
    try:
        proc = subprocess.run(
            argv,
            cwd=str(demo_state_dir.parent),
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (subprocess.TimeoutExpired, OSError) as exc:
        with open_private_log(log_path) as log_f:
            log_f.write(f"[threadlane_list_bindings_failed] subprocess exception: {type(exc).__name__}\n")
        raise PreviewError("threadlane_list_bindings_failed") from exc

    if len(proc.stdout.encode("utf-8")) > MAX_CLI_OUTPUT_BYTES:
        raise PreviewError("subprocess_output_overflow")

    if proc.returncode != 0:
        with open_private_log(log_path) as log_f:
            safe_err = redact_sensitive_text(proc.stderr or "")
            safe_out = redact_sensitive_text(proc.stdout or "")
            log_f.write(
                f"[threadlane_list_bindings_failed] exit={proc.returncode}\n"
                f"stderr:\n{safe_err}\nstdout:\n{safe_out}\n"
            )
        raise PreviewError("threadlane_list_bindings_failed")

    try:
        parsed = json.loads(proc.stdout)
    except ValueError as exc:
        with open_private_log(log_path) as log_f:
            log_f.write("[threadlane_list_bindings_invalid_json] invalid JSON from list-bindings\n")
        raise PreviewError("threadlane_list_bindings_invalid_json") from exc

    if not isinstance(parsed, list):
        with open_private_log(log_path) as log_f:
            log_f.write("[threadlane_list_bindings_invalid_json] expected JSON list\n")
        raise PreviewError("threadlane_list_bindings_invalid_json")

    return parsed


def wait_for_supervisor_ready(
    proc: subprocess.Popen[Any],
    threadlane_bin: Path,
    demo_state_dir: Path,
    log_path: Path,
    *,
    timeout_seconds: float = SUPERVISOR_READY_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise PreviewError("supervisor_exited_early")
        with contextlib.suppress(PreviewError):
            probe_supervisor_ready(threadlane_bin, demo_state_dir, log_path)
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise PreviewError("supervisor_ready_timeout")


def terminate_and_reap_process(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is None:
        with contextlib.suppress(ProcessLookupError, OSError):
            if os.name != "nt":
                proc.send_signal(signal.SIGTERM)
            else:
                proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, OSError):
                proc.kill()
            proc.wait(timeout=3.0)
    else:
        proc.wait(timeout=1.0)


@contextlib.contextmanager
def supervised_demo_server(
    threadlane_bin: Path,
    demo_state_dir: Path,
    logs_dir: Path,
    cwd: Path,
) -> Iterator[subprocess.Popen[Any]]:
    child_env = build_sanitized_child_env()
    argv = build_supervisor_argv(threadlane_bin, demo_state_dir)
    stdout_path = logs_dir / "demo_supervisor.stdout.log"
    stderr_path = logs_dir / "demo_supervisor.stderr.log"
    with open_private_log(stdout_path) as out_f, open_private_log(stderr_path) as err_f:
        proc = subprocess.Popen(
            argv,
            cwd=str(cwd),
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=out_f,
            stderr=err_f,
            text=True,
        )
        try:
            yield proc
        finally:
            terminate_and_reap_process(proc)


def run_preview(config: PreviewConfig) -> int:
    validate_executable_path(config.threadlane, missing_error="threadlane_executable_not_found")
    validate_executable_path(config.desktop, missing_error="desktop_executable_not_found")
    validate_optional_state_dir(config.state_dir)
    ensure_private_output_root(config.output_root)

    session_id = str(uuid.uuid4())
    layout = build_session_layout(config.output_root, session_id)
    session_dir = create_private_dir(Path(layout["session_dir"]), require_existing_parent=True)
    demo_state_dir = create_private_dir(Path(layout["demo_state_dir"]), require_existing_parent=True)
    logs_dir = create_private_dir(Path(layout["logs_dir"]), require_existing_parent=True)
    cli_log_path = Path(layout["cli_diagnostics_log"])
    summary_path = Path(layout["summary_path"])

    # Create private log files upfront so all preserved paths exist with 0600 permissions.
    for log_key in (
        "supervisor_stdout_log",
        "supervisor_stderr_log",
        "desktop_stdout_log",
        "desktop_stderr_log",
        "cli_diagnostics_log",
    ):
        with open_private_log(Path(layout[log_key])):
            pass

    desktop_exit_code: int | None = None
    error_class: str | None = None
    demo_supervisor_stopped = False

    try:
        with supervised_demo_server(config.threadlane, demo_state_dir, logs_dir, session_dir) as sup_proc:
            wait_for_supervisor_ready(sup_proc, config.threadlane, demo_state_dir, cli_log_path)
            if sup_proc.poll() is not None:
                raise PreviewError("supervisor_exited_early")

            desktop_argv = build_desktop_argv(config.desktop, demo_state_dir, config.state_dir)
            child_env = build_sanitized_child_env()
            with (
                open_private_log(Path(layout["desktop_stdout_log"])) as desk_out_f,
                open_private_log(Path(layout["desktop_stderr_log"])) as desk_err_f,
            ):
                desktop_proc = subprocess.Popen(
                    desktop_argv,
                    cwd=str(session_dir),
                    env=child_env,
                    stdin=subprocess.DEVNULL,
                    stdout=desk_out_f,
                    stderr=desk_err_f,
                    text=True,
                )
                try:
                    desktop_exit_code = desktop_proc.wait()
                finally:
                    if desktop_proc.poll() is None:
                        terminate_and_reap_process(desktop_proc)
        demo_supervisor_stopped = True
        if desktop_exit_code != 0:
            error_class = "desktop_exited_nonzero"
    except PreviewError as exc:
        demo_supervisor_stopped = True
        error_class = exc.error_class
    except KeyboardInterrupt:
        demo_supervisor_stopped = True
        error_class = "interrupted"
    except Exception:
        demo_supervisor_stopped = True
        error_class = "unexpected_execution_error"

    passed = error_class is None and desktop_exit_code == 0
    summary_payload = {
        "mode": "live",
        "live_execution": True,
        "passed": passed,
        "error_class": error_class,
        "paid_workers_enabled": False,
        "codex_wakeup_enabled": False,
        "session_id": session_id,
        "output_root": str(config.output_root),
        "session_dir": str(session_dir),
        "demo_state_dir": str(demo_state_dir),
        "attached_state_dir": str(config.state_dir) if config.state_dir is not None else None,
        "logs_dir": str(logs_dir),
        "preserved_paths": layout,
        "desktop_exit_code": desktop_exit_code,
        "demo_supervisor_stopped": demo_supervisor_stopped,
    }
    encoded = json.dumps(summary_payload, indent=2, sort_keys=True)
    print(encoded, flush=True)
    try:
        write_private_file(summary_path, (encoded + "\n").encode("utf-8"), allow_overwrite=True)
    except Exception:
        return 1
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    try:
        config = parse_and_validate_args(argv)
        if config.dry_run:
            print(json.dumps(build_dry_run_plan(config), indent=2, sort_keys=True), flush=True)
            return 0
        return run_preview(config)
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 2
    except PreviewError as exc:
        print(
            json.dumps({"error_class": exc.error_class, "passed": False}, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
