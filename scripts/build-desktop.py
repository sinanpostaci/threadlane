#!/usr/bin/env python3
"""Build the standalone Threadlane desktop binary without a shell.

The script always resolves paths from this checkout, installs the locked
frontend dependencies, builds the Vite bundle, and then compiles the standalone
Tauri Cargo workspace with ``CARGO_TARGET_DIR=<repo>/target/desktop``. It never
runs a Tauri bundler, signs artifacts, or installs anything on the host.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from live_worker_pilot import build_sanitized_child_env

REPO_ROOT = _SCRIPTS_DIR.parent
DESKTOP_DIR = REPO_ROOT / "apps" / "desktop"
MANIFEST_PATH = DESKTOP_DIR / "src-tauri" / "Cargo.toml"
LOCKFILE_PATH = DESKTOP_DIR / "src-tauri" / "Cargo.lock"
PACKAGE_LOCK_PATH = DESKTOP_DIR / "package-lock.json"
TARGET_DIR = REPO_ROOT / "target" / "desktop"

DEFAULT_TIMEOUT_SECONDS = 1800.0
MAX_TIMEOUT_SECONDS = 7200.0
MAX_DIAGNOSTIC_STREAM_BYTES = 8192
MAX_DIAGNOSTIC_MESSAGE_CHARS = 24_000
TERMINATE_GRACE_SECONDS = 2.0
KILL_GRACE_SECONDS = 1.0
_EXE_SUFFIX = ".exe" if os.name == "nt" else ""

_HEX_TOKEN_RE = re.compile(r"\b[0-9a-fA-F]{64,}\b")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+\S+")
_JSON_TOKEN_RE = re.compile(r'("token"\s*:\s*)"[^"]*"')


class BuildError(RuntimeError):
    """A safe, classified build error with no raw command environment."""

    def __init__(self, error_class: str, message: str = "", exit_code: int = 1) -> None:
        super().__init__(message or error_class)
        self.error_class = error_class
        self.message = message or error_class
        self.exit_code = exit_code


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: tuple[str, ...]
    cwd: Path
    env: Mapping[str, str] | None = None

    def as_json(self) -> dict[str, object]:
        return {
            "label": self.label,
            "argv": list(self.argv),
            "cwd": str(self.cwd),
            "env": dict(self.env or {}),
        }


@dataclass(frozen=True)
class BuildConfig:
    repo_root: Path
    desktop_dir: Path
    manifest_path: Path
    lockfile_path: Path
    package_lock_path: Path
    target_dir: Path
    release: bool
    timeout_seconds: float

    @property
    def profile(self) -> str:
        return "release" if self.release else "debug"

    @property
    def artifact_path(self) -> Path:
        return self.target_dir / self.profile / f"threadlane-desktop{_EXE_SUFFIX}"


def _redact_text(raw: str) -> str:
    redacted = _HEX_TOKEN_RE.sub("[REDACTED_TOKEN]", raw)
    redacted = _BEARER_RE.sub("Bearer [REDACTED_TOKEN]", redacted)
    redacted = _JSON_TOKEN_RE.sub(r'\1"[REDACTED_TOKEN]"', redacted)
    return redacted[:MAX_DIAGNOSTIC_MESSAGE_CHARS]


def _require_file(path: Path, error_class: str) -> None:
    try:
        present = path.is_file()
    except OSError as exc:
        raise BuildError(error_class, f"Required file could not be inspected: {path}", 2) from exc
    if not present:
        raise BuildError(error_class, f"Required file is missing: {path}", 2)


def resolve_config(repo_root: Path, release: bool, timeout_seconds: float) -> BuildConfig:
    if not math.isfinite(timeout_seconds) or not (0 < timeout_seconds <= MAX_TIMEOUT_SECONDS):
        raise BuildError(
            "invalid_timeout_seconds",
            f"--timeout-seconds must be greater than 0 and at most {MAX_TIMEOUT_SECONDS:g}.",
            2,
        )

    try:
        root = Path(repo_root).resolve()
    except OSError as exc:
        raise BuildError("repo_root_unreadable", "The repository root could not be resolved.", 2) from exc
    if not root.is_dir():
        raise BuildError("repo_root_missing", f"Repository root is not a directory: {root}", 2)

    desktop_dir = root / "apps" / "desktop"
    manifest_path = desktop_dir / "src-tauri" / "Cargo.toml"
    lockfile_path = desktop_dir / "src-tauri" / "Cargo.lock"
    package_lock_path = desktop_dir / "package-lock.json"

    _require_file(root / "Cargo.toml", "root_cargo_manifest_missing")
    _require_file(desktop_dir / "package.json", "desktop_package_manifest_missing")
    _require_file(package_lock_path, "desktop_package_lock_missing")
    _require_file(manifest_path, "desktop_cargo_manifest_missing")
    _require_file(lockfile_path, "desktop_cargo_lock_missing")

    return BuildConfig(
        repo_root=root,
        desktop_dir=desktop_dir,
        manifest_path=manifest_path,
        lockfile_path=lockfile_path,
        package_lock_path=package_lock_path,
        target_dir=root / "target" / "desktop",
        release=bool(release),
        timeout_seconds=float(timeout_seconds),
    )


def _desktop_build_args(config: BuildConfig) -> list[str]:
    argv = ["build", "--manifest-path", str(config.manifest_path), "--locked"]
    if config.release:
        argv.append("--release")
    return argv


def _dry_run_commands(config: BuildConfig) -> list[CommandSpec]:
    return [
        CommandSpec("frontend_install", ("npm", "ci"), config.desktop_dir),
        CommandSpec("frontend_build", ("npm", "run", "build"), config.desktop_dir),
        CommandSpec(
            "desktop_build",
            ("cargo", *_desktop_build_args(config)),
            config.repo_root,
            {"CARGO_TARGET_DIR": str(config.target_dir)},
        ),
    ]


def build_dry_run_plan(config: BuildConfig) -> dict[str, object]:
    return {
        "mode": "dry_run",
        "live_execution": False,
        "uses_shell": False,
        "repo_root": str(config.repo_root),
        "desktop_dir": str(config.desktop_dir),
        "manifest_path": str(config.manifest_path),
        "lockfile_path": str(config.lockfile_path),
        "package_lock_path": str(config.package_lock_path),
        "cargo_target_dir": str(config.target_dir),
        "profile": config.profile,
        "artifact": str(config.artifact_path),
        "timeout_seconds": config.timeout_seconds,
        "commands": [spec.as_json() for spec in _dry_run_commands(config)],
    }


def _npm_argv(args: Sequence[str]) -> list[str]:
    npm = shutil.which("npm")
    if npm is None:
        raise BuildError("npm_not_found", "npm was not found on PATH.", 2)

    if os.name == "nt":
        # npm on Windows is a .cmd shim, so execute npm-cli.js through node to
        # keep the no-shell invariant while still using the official npm CLI.
        node = shutil.which("node")
        npm_cli = Path(npm).resolve().parent / "node_modules" / "npm" / "bin" / "npm-cli.js"
        if node is None or not npm_cli.is_file():
            raise BuildError(
                "npm_cli_not_resolvable",
                "npm was found, but node and npm-cli.js could not be resolved on Windows.",
                2,
            )
        return [node, str(npm_cli), *args]

    return [npm, *args]


def _cargo_argv(args: Sequence[str]) -> list[str]:
    cargo = shutil.which("cargo")
    if cargo is None:
        raise BuildError("cargo_not_found", "cargo was not found on PATH.", 2)
    return [cargo, *args]


def _resolved_commands(config: BuildConfig) -> list[CommandSpec]:
    return [
        CommandSpec(
            "frontend_install",
            tuple(_npm_argv(["ci"])),
            config.desktop_dir,
        ),
        CommandSpec(
            "frontend_build",
            tuple(_npm_argv(["run", "build"])),
            config.desktop_dir,
        ),
        CommandSpec(
            "desktop_build",
            tuple(_cargo_argv(_desktop_build_args(config))),
            config.repo_root,
            {"CARGO_TARGET_DIR": str(config.target_dir)},
        ),
    ]


def _read_tail(stream, limit: int = MAX_DIAGNOSTIC_STREAM_BYTES) -> tuple[str, bool]:
    stream.flush()
    size = stream.seek(0, os.SEEK_END)
    start = max(0, size - limit)
    stream.seek(start)
    raw = stream.read(limit)
    return _redact_text(raw.decode("utf-8", errors="replace")), size > limit


def _terminate_process(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return

    if os.name != "nt":
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            with contextlib.suppress(OSError):
                proc.terminate()
    else:
        ctrl_break = getattr(signal, "CTRL_BREAK_EVENT", None)
        try:
            if ctrl_break is not None:
                proc.send_signal(ctrl_break)
            else:
                proc.terminate()
        except (OSError, ValueError):
            with contextlib.suppress(OSError):
                proc.kill()

    try:
        proc.wait(timeout=TERMINATE_GRACE_SECONDS)
        return
    except subprocess.TimeoutExpired:
        pass

    if os.name != "nt":
        with contextlib.suppress(ProcessLookupError, OSError):
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    else:
        with contextlib.suppress(OSError):
            proc.kill()
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=KILL_GRACE_SECONDS)


def _command_environment(base_env: Mapping[str, str], spec: CommandSpec) -> dict[str, str]:
    env = dict(base_env)
    if spec.env:
        env.update(spec.env)
    return env


def _run_command(spec: CommandSpec, env: Mapping[str, str], timeout_seconds: float) -> None:
    stdout_file = tempfile.TemporaryFile()
    stderr_file = tempfile.TemporaryFile()
    popen_kwargs: dict[str, object] = {}
    if os.name != "nt":
        popen_kwargs["start_new_session"] = True
    else:
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)

    try:
        try:
            proc = subprocess.Popen(
                list(spec.argv),
                cwd=str(spec.cwd),
                env=dict(env),
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                **popen_kwargs,
            )
        except FileNotFoundError as exc:
            raise BuildError(f"{spec.label}_not_found", f"Executable not found for {spec.label}.", 2) from exc
        except OSError as exc:
            raise BuildError(f"{spec.label}_spawn_failed", f"Could not start {spec.label}.", 2) from exc

        try:
            returncode = proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            _terminate_process(proc)
            stdout_tail, stdout_truncated = _read_tail(stdout_file)
            stderr_tail, stderr_truncated = _read_tail(stderr_file)
            raise BuildError(
                f"{spec.label}_timeout",
                _redact_text(
                    f"{spec.label} exceeded {timeout_seconds:g} seconds.\n"
                    f"stdout{' (tail)' if stdout_truncated else ''}:\n{stdout_tail}\n"
                    f"stderr{' (tail)' if stderr_truncated else ''}:\n{stderr_tail}"
                ),
                1,
            ) from exc
        except KeyboardInterrupt:
            _terminate_process(proc)
            raise

        if returncode != 0:
            stdout_tail, stdout_truncated = _read_tail(stdout_file)
            stderr_tail, stderr_truncated = _read_tail(stderr_file)
            raise BuildError(
                f"{spec.label}_failed",
                _redact_text(
                    f"{spec.label} exited with status {returncode}.\n"
                    f"stdout{' (tail)' if stdout_truncated else ''}:\n{stdout_tail}\n"
                    f"stderr{' (tail)' if stderr_truncated else ''}:\n{stderr_tail}"
                ),
                1,
            )
    finally:
        stdout_file.close()
        stderr_file.close()


def _validate_artifact(config: BuildConfig) -> None:
    artifact = config.artifact_path
    try:
        present = artifact.is_file()
    except OSError as exc:
        raise BuildError("desktop_artifact_unreadable", f"Could not inspect artifact: {artifact}", 1) from exc
    if not present:
        raise BuildError(
            "desktop_artifact_missing",
            f"Desktop build finished without the expected artifact: {artifact}",
            1,
        )
    if os.name != "nt":
        try:
            mode = artifact.stat().st_mode
        except OSError as exc:
            raise BuildError("desktop_artifact_unreadable", f"Could not inspect artifact: {artifact}", 1) from exc
        if not stat.S_ISREG(mode) or (stat.S_IMODE(mode) & 0o111) == 0:
            raise BuildError("desktop_artifact_not_executable", f"Artifact is not executable: {artifact}", 1)


def run_build(config: BuildConfig) -> dict[str, object]:
    commands = _resolved_commands(config)
    base_env = build_sanitized_child_env()
    base_env["CARGO_TARGET_DIR"] = str(config.target_dir)
    base_env.pop("CARGO_BUILD_TARGET", None)
    base_env.pop("TAURI_SIGNING_PRIVATE_KEY", None)
    base_env.pop("TAURI_SIGNING_PRIVATE_KEY_PASSWORD", None)
    base_env.pop("TAURI_KEY_PASSWORD", None)

    for spec in commands:
        _run_command(spec, _command_environment(base_env, spec), config.timeout_seconds)

    _validate_artifact(config)
    return {
        "mode": "build",
        "passed": True,
        "uses_shell": False,
        "repo_root": str(config.repo_root),
        "profile": config.profile,
        "artifact": str(config.artifact_path),
        "cargo_target_dir": str(config.target_dir),
        "commands": [spec.as_json() for spec in commands],
    }


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise BuildError("invalid_cli_arguments", message, 2)


def _build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="build-desktop.py",
        description=(
            "Build the standalone Threadlane desktop binary with argv-only npm and cargo steps."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved build plan as JSON without creating files or running commands.",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Build with cargo --release instead of the default debug profile.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-step timeout in seconds (default: {DEFAULT_TIMEOUT_SECONDS:g}).",
    )
    return parser


def main(argv: list[str] | None = None, *, repo_root: Path | None = None) -> int:
    try:
        args = _build_parser().parse_args(argv)
        config = resolve_config(
            REPO_ROOT if repo_root is None else repo_root,
            args.release,
            args.timeout_seconds,
        )
        if args.dry_run:
            print(json.dumps(build_dry_run_plan(config), indent=2, sort_keys=True), flush=True)
            return 0
        summary = run_build(config)
        print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
        return 0
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 2
    except BuildError as exc:
        print(
            json.dumps(
                {
                    "passed": False,
                    "uses_shell": False,
                    "error_class": exc.error_class,
                    "message": _redact_text(exc.message),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return exc.exit_code
    except KeyboardInterrupt:
        print(
            json.dumps(
                {"passed": False, "uses_shell": False, "error_class": "interrupted"},
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return 1
    except Exception as exc:  # Keep the CLI boundary free of tracebacks and raw env.
        print(
            json.dumps(
                {
                    "passed": False,
                    "uses_shell": False,
                    "error_class": "internal_error",
                    "message": type(exc).__name__,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
