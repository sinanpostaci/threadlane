#!/usr/bin/env python3
"""Isolated desktop acceptance runner for Threadlane browser and NixOS Wayland/tray checks."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import datetime as dt
import json
import math
import os
from pathlib import Path
import re
import select
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Mapping, Sequence


SAFE_PARENT_ENV_KEYS = frozenset(
    {
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "NIX_PATH",
        "NIX_PROFILES",
        "NIX_SSL_CERT_FILE",
        "PATH",
        "PLAYWRIGHT_BROWSERS_PATH",
        "SSL_CERT_FILE",
        "TERM",
        "THREADLANE_CHROMIUM_EXECUTABLE",
        "TZ",
    }
)

FORBIDDEN_COMMAND_BASENAMES = frozenset(
    {
        "killall",
        "nixos-rebuild",
        "nrs",
        "pkill",
        "reboot",
        "shutdown",
        "systemctl",
    }
)

FORBIDDEN_SHARED_ARTIFACT_ROOTS = frozenset(
    Path(p)
    for p in (
        "/",
        "/bin",
        "/boot",
        "/dev",
        "/etc",
        "/home",
        "/lib",
        "/lib64",
        "/nix",
        "/nix/store",
        "/opt",
        "/proc",
        "/root",
        "/run",
        "/sbin",
        "/srv",
        "/sys",
        "/tmp",
        "/usr",
        "/var",
        "/var/tmp",
    )
)

REQUIRED_VM_CHECKS = (
    "wayland_native_shell",
    "no_forced_tray_compositor_rules",
    "main_window_geometry",
    "min_window_geometry",
    "panel_tray_pointer_open",
    "rendered_popup_geometry",
    "target_monitor_and_scale",
    "pointer_transit_dwell_survival",
    "click_outside_dismiss",
    "escape_dismiss",
    "reopen_after_dismiss",
    "hide_main_to_tray_and_open_control",
    "quit_control",
    "no_codex_startup_setup_preserved",
)

EXPECTED_POPUP_LOGICAL_WIDTH = 360.0
EXPECTED_POPUP_LOGICAL_HEIGHT = 320.0
POPUP_LOGICAL_TOLERANCE = 4.0
MIN_POPUP_FILL_RATIO = 0.70
MIN_POINTER_TRANSIT_WAYPOINTS = 3
UI_REFRESH_INTERVAL_MS = 4000.0
MIN_POINTER_DWELL_MS = 9500.0
MIN_H2_REFRESH_DWELL_SECONDS = MIN_POINTER_DWELL_MS / 1000.0
REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES = 10
MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS = 0.90
MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS = 2.5
MIN_H1_REFRESH_SAMPLING_WINDOW_SECONDS = 9.0
MIN_H1_REFRESH_TOTAL_DURATION_SECONDS = 9.0
MAX_REFRESH_SURVIVAL_DURATION_SECONDS = 120.0

WL_SEAT_CAPABILITY_POINTER = 1
WL_SEAT_CAPABILITY_KEYBOARD = 2
REQUIRED_SEAT0_CAPABILITIES = (
    WL_SEAT_CAPABILITY_POINTER | WL_SEAT_CAPABILITY_KEYBOARD
)

MAIN_RENDERED_LOGICAL_TOLERANCE = 4.0
MIN_MAIN_FILL_RATIO = 0.85
MIN_MAIN_INTERIOR_CHANNEL_STDDEV = 4.0
MIN_MAIN_INTERIOR_COLOR_BUCKETS = 3
MIN_MAIN_BAND_CONTRAST_DELTA = 42
MIN_MAIN_BAND_CONTRAST_PIXELS = 24
REQUIRED_MAIN_PAINT_BANDS = ("titlebar", "navigation", "workspace_body")
REQUIRED_SCENE_CLEANUP_OWNERS = (
    "threadlane_desktop",
    "waybar",
    "persistent_input",
    "sway",
)

MAX_HELPER_READY_LINE_BYTES = 8_192
MAX_DIAGNOSTIC_LOG_TAIL_BYTES = 16_384
MAX_DIAGNOSTIC_JSON_BYTES = 131_072

SECRET_REDACTION_PATTERNS = (
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-+/=]+"),
    re.compile(r"(?i)((?:api[_-]?key|token|secret|password)\s*[=:]\s*)\S+"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}\b"),
)


class AcceptanceError(RuntimeError):
    """Fail-closed error raised when isolation, prerequisites, or assertions fail."""


class SceneCleanupError(AcceptanceError):
    """Fail-closed error raised when owned scene process or log cleanup fails."""

    def __init__(self, message: str, *, scene_cleanup: dict[str, Any]) -> None:
        super().__init__(message)
        self.scene_cleanup = scene_cleanup


@dataclass(frozen=True)
class ChildIsolationLayout:
    root_dir: Path
    home_dir: Path
    xdg_config_dir: Path
    xdg_data_dir: Path
    xdg_cache_dir: Path
    xdg_state_dir: Path
    xdg_runtime_dir: Path


@dataclass(frozen=True)
class ValidatedArtifactTarget:
    artifact_dir: Path
    inside_repo: bool


@dataclass(frozen=True)
class PlaywrightSuiteCounts:
    total: int
    passed: int
    failed: int
    skipped: int


def redact_sensitive_text(text: str) -> str:
    redacted = text
    redacted = SECRET_REDACTION_PATTERNS[0].sub(r"\1[REDACTED]", redacted)
    redacted = SECRET_REDACTION_PATTERNS[1].sub(r"\1[REDACTED]", redacted)
    redacted = SECRET_REDACTION_PATTERNS[2].sub("[REDACTED]", redacted)
    return redacted


def validate_safe_absolute_path(raw: str | Path, label: str) -> Path:
    text = str(raw)
    if not text or "\x00" in text:
        raise AcceptanceError(f"{label} must be a non-empty path without NUL bytes.")
    if text != text.strip():
        raise AcceptanceError(f"{label} must not have leading or trailing whitespace.")
    if text.startswith("-"):
        raise AcceptanceError(f"{label} must not start with '-'.")
    candidate = Path(text)
    if not candidate.is_absolute():
        raise AcceptanceError(f"{label} must be an absolute path: {text}")
    if ".." in candidate.parts:
        raise AcceptanceError(f"{label} must not contain '..' traversal segments: {text}")
    return candidate


def ensure_no_symlink_ancestors(path: Path, label: str) -> None:
    current = Path(path.anchor)
    parts = path.parts[1:]
    for idx, part in enumerate(parts):
        current = current / part
        try:
            st = os.lstat(current)
        except FileNotFoundError:
            break
        except OSError as exc:
            raise AcceptanceError(f"Unable to inspect {label} component {current}: {exc}") from exc
        if stat.S_ISLNK(st.st_mode):
            raise AcceptanceError(f"Refusing symlink component in {label}: {current}")
        if idx < len(parts) - 1 and not stat.S_ISDIR(st.st_mode):
            raise AcceptanceError(
                f"Refusing non-directory ancestor component in {label}: {current}"
            )


def ensure_directory_preserving_existing(path: Path, label: str) -> Path:
    ensure_no_symlink_ancestors(path, label)
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        try:
            st = os.lstat(current)
        except FileNotFoundError:
            try:
                os.mkdir(current, 0o700)
            except OSError as exc:
                raise AcceptanceError(
                    f"Unable to create {label} component {current}: {exc}"
                ) from exc
            os.chmod(current, 0o700)
        else:
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
                raise AcceptanceError(f"Expected regular directory for {label} at {current}")
    return path


def ensure_private_dir(path: Path) -> Path:
    ensure_no_symlink_ancestors(path, "private directory")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise AcceptanceError(f"Expected regular directory at {path}")
    os.chmod(path, 0o700)
    return path


def write_private_text(path: Path, content: str) -> None:
    ensure_private_dir(path.parent)
    if path.exists() or path.is_symlink():
        st = os.lstat(path)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise AcceptanceError(f"Refusing to overwrite non-regular file: {path}")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, content.encode("utf-8"))
    finally:
        os.close(fd)
    os.chmod(path, 0o600)


def build_sanitized_git_env(base_env: Mapping[str, str] | None = None) -> dict[str, str]:
    source = os.environ if base_env is None else base_env
    clean: dict[str, str] = {}
    for key in ("PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "SYSTEMROOT"):
        val = source.get(key)
        if val:
            clean[key] = val
    clean["GIT_TERMINAL_PROMPT"] = "0"
    return clean


def validate_repo_root(repo_root: Path) -> Path:
    checked = validate_safe_absolute_path(repo_root, "repo_root")
    ensure_no_symlink_ancestors(checked, "repo_root")
    if not checked.is_dir():
        raise AcceptanceError(f"Repository root does not exist: {checked}")
    required = (
        checked / "apps" / "desktop" / "index.html",
        checked / "apps" / "desktop" / "playwright.config.ts",
        checked / "nix" / "desktop-acceptance.nix",
        checked / "nix" / "packages.nix",
    )
    for req in required:
        if not req.is_file():
            raise AcceptanceError(f"Repository root is missing required file: {req}")
    return checked


def check_git_ignored(repo_root: Path, target_path: Path) -> bool:
    git_bin = shutil.which("git")
    if not git_bin:
        raise AcceptanceError("git executable is required to verify artifact git-ignore status.")
    proc = subprocess.run(
        [git_bin, "-C", str(repo_root), "check-ignore", "-q", "--", str(target_path)],
        env=build_sanitized_git_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    detail = redact_sensitive_text(proc.stderr.strip() or proc.stdout.strip())
    raise AcceptanceError(f"git check-ignore failed for {target_path}: {detail}")


def find_enclosing_git_toplevel(target_path: Path) -> Path | None:
    git_bin = shutil.which("git")
    if not git_bin:
        raise AcceptanceError("git executable is required to verify artifact git-ignore status.")
    probe = target_path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    if not probe.exists():
        return None
    probe_dir = probe if probe.is_dir() else probe.parent
    proc = subprocess.run(
        [git_bin, "-C", str(probe_dir), "rev-parse", "--show-toplevel"],
        env=build_sanitized_git_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    top = proc.stdout.strip()
    return Path(top) if top else None


def _ensure_not_existing_non_directory(target: Path, label: str) -> None:
    try:
        st = os.lstat(target)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise AcceptanceError(f"Unable to inspect {label} {target}: {exc}") from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise AcceptanceError(f"{label} must be a regular directory, got non-directory: {target}")


def _validate_in_repo_dot_local(checked_repo: Path, target: Path) -> ValidatedArtifactTarget:
    dot_local = checked_repo / ".local"
    if target != dot_local and not target.is_relative_to(dot_local):
        raise AcceptanceError(
            f"In-repo artifacts are strictly restricted to {dot_local}; refused {target}"
        )
    ensure_no_symlink_ancestors(dot_local, ".local directory")
    try:
        st = os.lstat(dot_local)
    except FileNotFoundError as exc:
        raise AcceptanceError(
            f"In-repo {dot_local} must pre-exist before running desktop acceptance. "
            "Create it with 0700 permissions or pass an external --artifact-dir outside the repository."
        ) from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
        raise AcceptanceError(f"{dot_local} must be an existing non-symlink directory.")
    if not check_git_ignored(checked_repo, dot_local) or not check_git_ignored(
        checked_repo, target
    ):
        raise AcceptanceError(
            f"Refusing in-repo artifact directory because {target} is not git-ignored."
        )
    return ValidatedArtifactTarget(artifact_dir=target, inside_repo=True)


def validate_artifact_directory(
    repo_root: Path,
    artifact_dir: Path | None,
) -> ValidatedArtifactTarget:
    checked_repo = validate_repo_root(repo_root)
    target = (
        checked_repo / ".local" / "desktop-acceptance"
        if artifact_dir is None
        else validate_safe_absolute_path(artifact_dir, "artifact_dir")
    )
    if target in FORBIDDEN_SHARED_ARTIFACT_ROOTS:
        raise AcceptanceError(
            f"artifact_dir must be a dedicated directory, not shared system root: {target}"
        )
    ensure_no_symlink_ancestors(target, "artifact_dir")
    _ensure_not_existing_non_directory(target, "artifact_dir")

    if target == checked_repo or checked_repo.is_relative_to(target):
        raise AcceptanceError(
            f"artifact_dir must not be the repository root or an ancestor of it: {target}"
        )

    if target.is_relative_to(checked_repo):
        return _validate_in_repo_dot_local(checked_repo, target)

    other_repo = find_enclosing_git_toplevel(target)
    if other_repo is not None:
        if not check_git_ignored(other_repo, target):
            raise AcceptanceError(
                f"Refusing external artifact_dir inside another non-ignored Git worktree ({other_repo}): {target}"
            )
        raise AcceptanceError(
            f"Refusing external artifact_dir inside another Git worktree ({other_repo}): {target}"
        )
    return ValidatedArtifactTarget(artifact_dir=target, inside_repo=False)


def validate_desktop_source_dir(desktop_source_dir: str | Path) -> Path:
    checked = validate_safe_absolute_path(desktop_source_dir, "THREADLANE_DESKTOP_SOURCE_DIR")
    ensure_no_symlink_ancestors(checked, "THREADLANE_DESKTOP_SOURCE_DIR")
    if not checked.is_dir():
        raise AcceptanceError(f"Desktop source directory does not exist: {checked}")
    for required_name in ("index.html", "playwright.config.ts"):
        req = checked / required_name
        if not req.is_file():
            raise AcceptanceError(
                f"Invalid desktop source directory {checked}: missing {required_name}"
            )
    return checked


def _is_threadlane_repo_root(candidate: Path) -> bool:
    return (
        candidate.is_dir()
        and (candidate / "apps" / "desktop" / "index.html").is_file()
        and (candidate / "apps" / "desktop" / "playwright.config.ts").is_file()
        and (candidate / "nix" / "desktop-acceptance.nix").is_file()
        and (candidate / "nix" / "packages.nix").is_file()
    )


def prepare_playwright_staging_dir(
    *,
    artifact_dir: str | Path | None,
    staging_dir: str | Path | None = None,
    repo_root: str | Path | None = None,
    desktop_source_dir: str | Path | None = None,
    cwd: Path | None = None,
) -> Path:
    checked_desktop_src: Path | None = None
    if desktop_source_dir is not None and str(desktop_source_dir) != "":
        checked_desktop_src = validate_desktop_source_dir(desktop_source_dir)

    if artifact_dir is None or str(artifact_dir) == "":
        raise AcceptanceError(
            "THREADLANE_ACCEPTANCE_ARTIFACT_DIR must point to a validated non-tracked artifact directory."
        )
    target = validate_safe_absolute_path(artifact_dir, "THREADLANE_ACCEPTANCE_ARTIFACT_DIR")
    if target in FORBIDDEN_SHARED_ARTIFACT_ROOTS:
        raise AcceptanceError(
            f"THREADLANE_ACCEPTANCE_ARTIFACT_DIR must be a dedicated directory, not shared system root: {target}"
        )
    ensure_no_symlink_ancestors(target, "THREADLANE_ACCEPTANCE_ARTIFACT_DIR")
    _ensure_not_existing_non_directory(target, "THREADLANE_ACCEPTANCE_ARTIFACT_DIR")

    explicit_repo: Path | None = None
    if repo_root is not None and str(repo_root) != "":
        explicit_repo = validate_repo_root(Path(repo_root))

    contextual_repos: list[Path] = []
    if checked_desktop_src is not None:
        src_git_root = find_enclosing_git_toplevel(checked_desktop_src)
        if src_git_root is not None and src_git_root not in contextual_repos:
            contextual_repos.append(src_git_root)

    effective_cwd = Path.cwd() if cwd is None else cwd
    if effective_cwd.exists():
        cwd_git_root = find_enclosing_git_toplevel(effective_cwd)
        if cwd_git_root is not None and cwd_git_root not in contextual_repos:
            contextual_repos.append(cwd_git_root)

    for ctx_repo in contextual_repos:
        if target == ctx_repo or ctx_repo.is_relative_to(target):
            raise AcceptanceError(
                f"THREADLANE_ACCEPTANCE_ARTIFACT_DIR must not be a repository root or an ancestor of it: {target}"
            )
        if explicit_repo is None and _is_threadlane_repo_root(ctx_repo):
            explicit_repo = ctx_repo

    if explicit_repo is not None:
        validate_artifact_directory(explicit_repo, target)
    else:
        enclosing_repo = find_enclosing_git_toplevel(target)
        if enclosing_repo is not None:
            if target == enclosing_repo or enclosing_repo.is_relative_to(target):
                raise AcceptanceError(
                    f"THREADLANE_ACCEPTANCE_ARTIFACT_DIR must not be a repository root or an ancestor of it: {target}"
                )
            _validate_in_repo_dot_local(enclosing_repo, target)

    stage_candidate: Path | None = None
    if staging_dir is not None and str(staging_dir) != "":
        stage_candidate = validate_safe_absolute_path(
            staging_dir,
            "THREADLANE_ACCEPTANCE_STAGING_DIR",
        )
        ensure_no_symlink_ancestors(stage_candidate, "THREADLANE_ACCEPTANCE_STAGING_DIR")
        if stage_candidate == target or not stage_candidate.is_relative_to(target):
            raise AcceptanceError(
                f"THREADLANE_ACCEPTANCE_STAGING_DIR must be a child path inside "
                f"THREADLANE_ACCEPTANCE_ARTIFACT_DIR ({target}); refused {stage_candidate}"
            )
        try:
            os.lstat(stage_candidate)
        except FileNotFoundError:
            pass
        else:
            raise AcceptanceError(
                f"Refusing preexisting THREADLANE_ACCEPTANCE_STAGING_DIR to preserve existing files: "
                f"{stage_candidate}"
            )

    ensure_directory_preserving_existing(target, "THREADLANE_ACCEPTANCE_ARTIFACT_DIR")

    if stage_candidate is None:
        created_stage = Path(tempfile.mkdtemp(prefix="staging-desktop-", dir=str(target)))
        os.chmod(created_stage, 0o700)
    else:
        if stage_candidate.parent != target:
            ensure_directory_preserving_existing(
                stage_candidate.parent,
                "THREADLANE_ACCEPTANCE_STAGING_DIR parent",
            )
        try:
            os.mkdir(stage_candidate, 0o700)
        except FileExistsError as exc:
            raise AcceptanceError(
                f"Refusing preexisting THREADLANE_ACCEPTANCE_STAGING_DIR to preserve existing files: "
                f"{stage_candidate}"
            ) from exc
        except OSError as exc:
            raise AcceptanceError(
                f"Unable to create THREADLANE_ACCEPTANCE_STAGING_DIR {stage_candidate}: {exc}"
            ) from exc
        os.chmod(stage_candidate, 0o700)
        created_stage = stage_candidate

    node_modules_dir = created_stage / "node_modules"
    os.mkdir(node_modules_dir, 0o700)
    os.chmod(node_modules_dir, 0o700)
    return created_stage


def prepare_child_isolation(run_dir: Path) -> ChildIsolationLayout:
    iso_root = ensure_private_dir(run_dir / "isolation")
    return ChildIsolationLayout(
        root_dir=iso_root,
        home_dir=ensure_private_dir(iso_root / "home"),
        xdg_config_dir=ensure_private_dir(iso_root / "xdg-config"),
        xdg_data_dir=ensure_private_dir(iso_root / "xdg-data"),
        xdg_cache_dir=ensure_private_dir(iso_root / "xdg-cache"),
        xdg_state_dir=ensure_private_dir(iso_root / "xdg-state"),
        xdg_runtime_dir=ensure_private_dir(iso_root / "xdg-runtime"),
    )


def build_isolated_child_env(
    layout: ChildIsolationLayout,
    *,
    parent_env: Mapping[str, str] | None = None,
    extra_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source = os.environ if parent_env is None else parent_env
    child_env: dict[str, str] = {}
    for key in SAFE_PARENT_ENV_KEYS:
        val = source.get(key)
        if val:
            child_env[key] = val

    child_env["HOME"] = str(layout.home_dir)
    child_env["XDG_CONFIG_HOME"] = str(layout.xdg_config_dir)
    child_env["XDG_DATA_HOME"] = str(layout.xdg_data_dir)
    child_env["XDG_CACHE_HOME"] = str(layout.xdg_cache_dir)
    child_env["XDG_STATE_HOME"] = str(layout.xdg_state_dir)
    child_env["XDG_RUNTIME_DIR"] = str(layout.xdg_runtime_dir)
    child_env["GIT_TERMINAL_PROMPT"] = "0"
    child_env["PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD"] = "1"
    child_env["NO_COLOR"] = "1"

    if extra_env:
        for key, val in extra_env.items():
            child_env[key] = val

    # Child environment must never inherit host session D-Bus or host Codex overrides.
    child_env.pop("DBUS_SESSION_BUS_ADDRESS", None)
    child_env.pop("CODEX_HOME", None)
    return child_env


def assert_command_allowed(argv: Sequence[str]) -> None:
    if not argv:
        raise AcceptanceError("Refusing to execute empty command argv.")
    exe_name = Path(argv[0]).name
    if exe_name in FORBIDDEN_COMMAND_BASENAMES:
        raise AcceptanceError(
            f"Refusing forbidden host-mutating command in desktop acceptance: {exe_name}"
        )
    for token in argv:
        if "\x00" in token:
            raise AcceptanceError("Command argument contains forbidden NUL byte.")


def build_desktop_launch_argv(
    desktop_executable: str | Path,
    extra_args: Sequence[str] = (),
) -> list[str]:
    exe_path = validate_safe_absolute_path(desktop_executable, "desktop_executable")
    argv = [str(exe_path), "--no-codex-startup-setup"]
    for arg in extra_args:
        if arg == "--no-codex-startup-setup":
            continue
        argv.append(arg)
    assert_command_allowed(argv)
    if "--no-codex-startup-setup" not in argv:
        raise AcceptanceError("threadlane-desktop launch must include --no-codex-startup-setup.")
    return argv


def wrap_with_private_dbus(
    inner_argv: Sequence[str],
    *,
    dbus_run_session_bin: str | None = None,
) -> list[str]:
    assert_command_allowed(inner_argv)
    resolved_dbus = dbus_run_session_bin or shutil.which("dbus-run-session")
    if not resolved_dbus:
        raise AcceptanceError(
            "dbus-run-session is required to isolate test child processes on a private D-Bus session."
        )
    dbus_path = validate_safe_absolute_path(resolved_dbus, "dbus-run-session")
    wrapped = [str(dbus_path), "--", *inner_argv]
    assert_command_allowed(wrapped)
    return wrapped


def capture_repo_status_porcelain(repo_root: Path) -> str:
    git_bin = shutil.which("git")
    if not git_bin:
        raise AcceptanceError("git executable is required to verify repository cleanliness.")
    proc = subprocess.run(
        [git_bin, "-C", str(repo_root), "status", "--porcelain=v1", "--untracked-files=all"],
        env=build_sanitized_git_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        detail = redact_sensitive_text(proc.stderr.strip() or proc.stdout.strip())
        raise AcceptanceError(f"git status failed: {detail}")
    return proc.stdout


def pick_ephemeral_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def verify_playwright_json_report(report_path: Path) -> PlaywrightSuiteCounts:
    if not report_path.is_file():
        raise AcceptanceError(
            f"Playwright JSON report was not produced at {report_path}; refusing silent success."
        )
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise AcceptanceError(f"Invalid Playwright JSON report at {report_path}: {exc}") from exc

    if not isinstance(payload, dict):
        raise AcceptanceError("Playwright JSON report root must be an object.")

    top_errors = payload.get("errors")
    if isinstance(top_errors, list) and len(top_errors) > 0:
        first_msg = redact_sensitive_text(str(top_errors[0]))
        raise AcceptanceError(f"Playwright reported suite-level errors: {first_msg}")

    total = 0
    passed = 0
    failed = 0
    skipped = 0

    def _walk_suite(suite: dict[str, Any]) -> None:
        nonlocal total, passed, failed, skipped
        for spec in suite.get("specs") or []:
            if not isinstance(spec, dict):
                continue
            tests = spec.get("tests") or []
            for test_entry in tests:
                if not isinstance(test_entry, dict):
                    continue
                total += 1
                status = str(test_entry.get("status") or "")
                results = test_entry.get("results") or []
                has_failed_result = any(
                    isinstance(r, dict)
                    and str(r.get("status") or "") not in ("passed", "skipped")
                    for r in results
                )
                if status == "skipped" or (
                    results
                    and all(isinstance(r, dict) and r.get("status") == "skipped" for r in results)
                ):
                    skipped += 1
                elif status == "expected" and not has_failed_result and bool(results):
                    passed += 1
                else:
                    failed += 1
        for child_suite in suite.get("suites") or []:
            if isinstance(child_suite, dict):
                _walk_suite(child_suite)

    for root_suite in payload.get("suites") or []:
        if isinstance(root_suite, dict):
            _walk_suite(root_suite)

    if total == 0:
        raise AcceptanceError("Zero Playwright tests were discovered; refusing silent success.")
    if failed > 0:
        raise AcceptanceError(f"Playwright acceptance failed ({failed} of {total} tests failed).")
    if skipped > 0:
        raise AcceptanceError(
            f"Playwright acceptance had {skipped} skipped test(s); partial runs cannot pass."
        )
    if passed != total:
        raise AcceptanceError(
            f"Playwright pass count mismatch (passed={passed}, total={total})."
        )
    return PlaywrightSuiteCounts(total=total, passed=passed, failed=failed, skipped=skipped)


def _require_non_bool_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AcceptanceError(f"Expected numeric value for {label} in VM report, got {value!r}")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise AcceptanceError(
            f"Expected finite numeric value for {label} in VM report, got {value!r}"
        )
    return numeric


def _require_non_bool_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AcceptanceError(f"Expected integer value for {label} in VM report, got {value!r}")
    return value


def _verify_rect_dimensions(
    rect: Any,
    *,
    label: str,
    expected_width: float,
    expected_height: float,
    tolerance: float,
) -> None:
    if not isinstance(rect, dict):
        raise AcceptanceError(f"VM report {label} must be an object, got {rect!r}")
    for coord_key in ("x", "y", "width", "height"):
        _require_non_bool_number(rect.get(coord_key), f"{label}.{coord_key}")
    width = _require_non_bool_number(rect.get("width"), f"{label}.width")
    height = _require_non_bool_number(rect.get("height"), f"{label}.height")
    if abs(width - expected_width) > tolerance or abs(height - expected_height) > tolerance:
        raise AcceptanceError(
            f"VM report {label} dimensions {width}x{height} outside expected "
            f"{expected_width}x{expected_height} +/- {tolerance}"
        )


def _verify_rendered_popup_measurement(
    entry: Any,
    *,
    label: str,
    expected_scale: float,
) -> None:
    if not isinstance(entry, dict):
        raise AcceptanceError(f"VM report {label} must be an object, got {entry!r}")
    if entry.get("measurement_method") != "grim_pixel_diff":
        raise AcceptanceError(
            f"VM report {label} must use grim_pixel_diff rendered measurement, "
            f"got {entry.get('measurement_method')!r}"
        )
    output_name = entry.get("output_name")
    if not isinstance(output_name, str) or not output_name.strip():
        raise AcceptanceError(
            f"VM report {label}.output_name must be a non-empty string, got {output_name!r}"
        )
    scale = _require_non_bool_number(entry.get("output_scale"), f"{label}.output_scale")
    if abs(scale - expected_scale) > 1e-6:
        raise AcceptanceError(
            f"VM report {label}.output_scale must be {expected_scale}, got {scale}"
        )
    _verify_rect_dimensions(
        entry.get("logical_rect"),
        label=f"{label}.logical_rect",
        expected_width=EXPECTED_POPUP_LOGICAL_WIDTH,
        expected_height=EXPECTED_POPUP_LOGICAL_HEIGHT,
        tolerance=POPUP_LOGICAL_TOLERANCE,
    )
    _verify_rect_dimensions(
        entry.get("physical_rect"),
        label=f"{label}.physical_rect",
        expected_width=EXPECTED_POPUP_LOGICAL_WIDTH * expected_scale,
        expected_height=EXPECTED_POPUP_LOGICAL_HEIGHT * expected_scale,
        tolerance=POPUP_LOGICAL_TOLERANCE * expected_scale,
    )
    fill_ratio = _require_non_bool_number(entry.get("fill_ratio"), f"{label}.fill_ratio")
    if fill_ratio < MIN_POPUP_FILL_RATIO or fill_ratio > 1.0:
        raise AcceptanceError(
            f"VM report {label}.fill_ratio {fill_ratio} outside valid range "
            f"[{MIN_POPUP_FILL_RATIO}, 1.0]"
        )
    changed_pixels = _require_non_bool_int(
        entry.get("changed_pixels"),
        f"{label}.changed_pixels",
    )
    phys_rect = entry["physical_rect"]
    phys_area = _require_non_bool_number(
        phys_rect.get("width"),
        f"{label}.physical_rect.width",
    ) * _require_non_bool_number(
        phys_rect.get("height"),
        f"{label}.physical_rect.height",
    )
    if (
        changed_pixels <= 0
        or changed_pixels > int(math.ceil(phys_area))
        or (changed_pixels / phys_area) < (MIN_POPUP_FILL_RATIO - 1e-3)
    ):
        raise AcceptanceError(
            f"VM report {label}.changed_pixels ({changed_pixels}) inconsistent with "
            f"minimum fill_ratio {MIN_POPUP_FILL_RATIO} for physical area {phys_area:.1f}"
        )
    if entry.get("within_output_bounds") is not True:
        raise AcceptanceError(f"VM report {label}.within_output_bounds must be True.")
    if entry.get("proximate_to_clicked_icon") is not True:
        raise AcceptanceError(f"VM report {label}.proximate_to_clicked_icon must be True.")
    other_diff = _require_non_bool_int(
        entry.get("other_output_diff_pixels"),
        f"{label}.other_output_diff_pixels",
    )
    if other_diff != 0:
        raise AcceptanceError(
            f"VM report {label}.other_output_diff_pixels must be 0, got {other_diff}"
        )


def validate_isolated_guest_wayland_env(
    env: Mapping[str, str],
    *,
    require_socket: bool = True,
) -> tuple[Path, str]:
    raw_runtime = str(env.get("XDG_RUNTIME_DIR") or "")
    runtime_dir = validate_safe_absolute_path(raw_runtime, "XDG_RUNTIME_DIR")
    ensure_no_symlink_ancestors(runtime_dir, "XDG_RUNTIME_DIR")

    host_user_run = Path("/run/user")
    if runtime_dir == host_user_run or runtime_dir.is_relative_to(host_user_run):
        raise AcceptanceError(
            f"Refusing host user runtime directory for guest Wayland fixture: {runtime_dir}"
        )

    if str(env.get("DISPLAY") or "").strip():
        raise AcceptanceError(
            "Refusing X11 DISPLAY in isolated Wayland guest fixture environment."
        )

    wayland_display = str(env.get("WAYLAND_DISPLAY") or "")
    if (
        not wayland_display
        or wayland_display != wayland_display.strip()
        or "\x00" in wayland_display
        or "/" in wayland_display
        or wayland_display in (".", "..")
        or wayland_display.startswith("-")
    ):
        raise AcceptanceError(
            f"WAYLAND_DISPLAY must be a bare isolated socket name, got {wayland_display!r}"
        )

    if require_socket:
        sock_path = runtime_dir / wayland_display
        try:
            st = os.lstat(sock_path)
        except FileNotFoundError as exc:
            raise AcceptanceError(
                f"Isolated Wayland socket does not exist at {sock_path}"
            ) from exc
        except OSError as exc:
            raise AcceptanceError(
                f"Unable to stat isolated Wayland socket {sock_path}: {exc}"
            ) from exc
        if not stat.S_ISSOCK(st.st_mode):
            raise AcceptanceError(
                f"Expected Wayland Unix socket at {sock_path}, got mode {oct(st.st_mode)}"
            )

    return (runtime_dir, wayland_display)


def read_bounded_redacted_log_tail(
    log_path: Path,
    *,
    allowed_root: Path | None = None,
    max_bytes: int = MAX_DIAGNOSTIC_LOG_TAIL_BYTES,
) -> dict[str, Any]:
    checked_path = validate_safe_absolute_path(log_path, "owned_log_path")
    ensure_no_symlink_ancestors(checked_path, "owned_log_path")
    if allowed_root is not None:
        checked_root = validate_safe_absolute_path(allowed_root, "allowed_log_root")
        ensure_no_symlink_ancestors(checked_root, "allowed_log_root")
        if checked_path == checked_root or not checked_path.is_relative_to(checked_root):
            raise AcceptanceError(
                f"Refusing diagnostic log read outside owned artifact root {checked_root}: {checked_path}"
            )

    try:
        st = os.lstat(checked_path)
    except FileNotFoundError:
        return {
            "present": False,
            "size_bytes": 0,
            "truncated": False,
            "tail": "",
        }
    except OSError as exc:
        raise AcceptanceError(f"Unable to inspect owned log {checked_path}: {exc}") from exc

    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
        raise AcceptanceError(f"Refusing non-regular diagnostic log file: {checked_path}")

    open_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(checked_path, open_flags)
    try:
        total_size = int(os.fstat(fd).st_size)
        bounded_max = max(1, int(max_bytes))
        offset = max(0, total_size - bounded_max)
        if offset > 0:
            os.lseek(fd, offset, os.SEEK_SET)
        chunks: list[bytes] = []
        remaining = min(total_size - offset, bounded_max)
        while remaining > 0:
            chunk = os.read(fd, min(4096, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    finally:
        os.close(fd)

    raw_text = b"".join(chunks).decode("utf-8", errors="replace")
    return {
        "present": True,
        "size_bytes": total_size,
        "truncated": total_size > max(1, int(max_bytes)),
        "tail": redact_sensitive_text(raw_text),
    }


class PersistentGuestInputFixture:
    """Owned lifecycle controller for the isolated guest persistent Wayland input helper."""

    def __init__(
        self,
        helper_argv: Sequence[str],
        *,
        env: Mapping[str, str],
        stderr_log_path: Path,
        require_wayland_socket: bool = True,
    ) -> None:
        assert_command_allowed(helper_argv)
        validate_isolated_guest_wayland_env(env, require_socket=require_wayland_socket)
        checked_log = validate_safe_absolute_path(stderr_log_path, "stderr_log_path")
        ensure_no_symlink_ancestors(checked_log, "stderr_log_path")
        self._helper_argv = [str(token) for token in helper_argv]
        self._env = dict(env)
        self._stderr_log_path = checked_log
        self._proc: subprocess.Popen[bytes] | None = None
        self._stderr_file: Any = None
        self._ready_info: dict[str, Any] | None = None

    @property
    def pid(self) -> int | None:
        return None if self._proc is None else self._proc.pid

    @property
    def ready_info(self) -> dict[str, Any] | None:
        return None if self._ready_info is None else dict(self._ready_info)

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def _stderr_excerpt(self) -> str:
        try:
            tail_info = read_bounded_redacted_log_tail(
                self._stderr_log_path,
                max_bytes=4096,
            )
        except Exception:  # noqa: BLE001
            return ""
        text = str(tail_info.get("tail") or "").strip()
        return f" (stderr: {text})" if text else ""

    def assert_running(self, context: str = "persistent guest input helper") -> None:
        if self._proc is None:
            raise AcceptanceError(f"{context} was never started.")
        rc = self._proc.poll()
        if rc is not None:
            raise AcceptanceError(
                f"{context} exited unexpectedly with code {rc}{self._stderr_excerpt()}"
            )

    def start(
        self,
        *,
        ready_timeout_sec: float = 10.0,
        term_timeout_sec: float = 3.0,
        kill_timeout_sec: float = 2.0,
    ) -> dict[str, Any]:
        if self._proc is not None:
            raise AcceptanceError("PersistentGuestInputFixture has already been started.")
        if ready_timeout_sec <= 0:
            raise AcceptanceError("ready_timeout_sec must be positive.")

        ensure_directory_preserving_existing(
            self._stderr_log_path.parent,
            "stderr_log_path parent",
        )
        if self._stderr_log_path.exists() or self._stderr_log_path.is_symlink():
            st = os.lstat(self._stderr_log_path)
            if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
                raise AcceptanceError(
                    f"Refusing non-regular stderr_log_path: {self._stderr_log_path}"
                )

        stderr_fd = os.open(
            self._stderr_log_path,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        self._stderr_file = os.fdopen(stderr_fd, "wb", buffering=0)
        try:
            self._proc = subprocess.Popen(
                list(self._helper_argv),
                env=dict(self._env),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=self._stderr_file,
                text=False,
                close_fds=True,
            )
            assert self._proc.stdout is not None
            stdout_fd = self._proc.stdout.fileno()
            deadline = time.monotonic() + ready_timeout_sec
            buf = bytearray()
            eof_reached = False

            while b"\n" not in buf:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AcceptanceError(
                        f"Timed out after {ready_timeout_sec:.2f}s waiting for persistent guest input helper readiness"
                    )
                readable, _, _ = select.select(
                    [stdout_fd],
                    [],
                    [],
                    min(0.1, remaining),
                )
                if not readable:
                    if self._proc.poll() is not None:
                        eof_reached = True
                        break
                    continue
                chunk = os.read(stdout_fd, 4096)
                if not chunk:
                    eof_reached = True
                    break
                buf.extend(chunk)
                if len(buf) > MAX_HELPER_READY_LINE_BYTES:
                    raise AcceptanceError(
                        "Persistent guest input helper exceeded bounded readiness line limit."
                    )

            if b"\n" not in buf:
                rc = self._proc.poll()
                state = f"exited with code {rc}" if rc is not None or eof_reached else "closed stdout"
                raise AcceptanceError(
                    f"Persistent guest input helper {state} before emitting readiness JSON"
                )

            raw_line = bytes(buf.split(b"\n", 1)[0]).decode("utf-8", errors="strict").strip()
            try:
                payload = json.loads(raw_line)
            except Exception as exc:  # noqa: BLE001
                raise AcceptanceError(
                    f"Invalid readiness JSON from persistent guest input helper: {raw_line!r}"
                ) from exc

            if not isinstance(payload, dict) or payload.get("event") != "ready":
                raise AcceptanceError(
                    f"Unexpected readiness payload from persistent guest input helper: {payload!r}"
                )
            if payload.get("seat") != "seat0":
                raise AcceptanceError(
                    f"Persistent guest input helper bound seat {payload.get('seat')!r}, expected 'seat0'"
                )
            caps = _require_non_bool_int(payload.get("capabilities"), "helper.capabilities")
            if caps != REQUIRED_SEAT0_CAPABILITIES:
                raise AcceptanceError(
                    f"Persistent guest input helper reported capabilities={caps}, "
                    f"expected pointer|keyboard ({REQUIRED_SEAT0_CAPABILITIES})"
                )
            if payload.get("pointer") is not True or payload.get("keyboard") is not True:
                raise AcceptanceError(
                    f"Persistent guest input helper must retain both pointer and keyboard: {payload!r}"
                )
            if payload.get("keymap_format") != "xkb_v1":
                raise AcceptanceError(
                    f"Persistent guest input helper keymap_format must be 'xkb_v1', got {payload.get('keymap_format')!r}"
                )
            keymap_bytes = _require_non_bool_int(
                payload.get("keymap_bytes"),
                "helper.keymap_bytes",
            )
            if keymap_bytes <= 0:
                raise AcceptanceError(
                    f"Persistent guest input helper keymap_bytes must be > 0, got {keymap_bytes}"
                )
            keypresses_sent = _require_non_bool_int(
                payload.get("setup_keypresses_sent"),
                "helper.setup_keypresses_sent",
            )
            if keypresses_sent != 0:
                raise AcceptanceError(
                    f"Persistent guest input helper must send 0 setup keypresses, got {keypresses_sent}"
                )
            reported_pid = _require_non_bool_int(payload.get("pid"), "helper.pid")
            if reported_pid <= 0:
                raise AcceptanceError(f"Invalid helper pid in readiness payload: {reported_pid}")

            self.assert_running("persistent guest input helper")
            self._ready_info = dict(payload)
            return dict(payload)
        except Exception as exc:
            stderr_note = self._stderr_excerpt()
            self.stop(
                term_timeout_sec=term_timeout_sec,
                kill_timeout_sec=kill_timeout_sec,
            )
            if isinstance(exc, AcceptanceError):
                msg = str(exc)
                if stderr_note and stderr_note not in msg:
                    raise AcceptanceError(f"{msg}{stderr_note}") from exc
                raise
            raise AcceptanceError(
                f"Failed to start persistent guest input helper: {exc}{stderr_note}"
            ) from exc

    def stop(
        self,
        *,
        term_timeout_sec: float = 3.0,
        kill_timeout_sec: float = 2.0,
    ) -> dict[str, Any]:
        proc = self._proc
        stderr_close_error: str | None = None
        try:
            outcome = stop_owned_process(
                proc,
                owner_name="persistent_input",
                term_timeout_sec=term_timeout_sec,
                kill_timeout_sec=kill_timeout_sec,
            )
        finally:
            self._proc = None
            if self._stderr_file is not None:
                try:
                    self._stderr_file.close()
                except OSError as exc:
                    stderr_close_error = (
                        "persistent_input: stderr log close() failed: "
                        f"{redact_sensitive_text(str(exc))}"
                    )
                self._stderr_file = None

        combined_errors = [
            msg
            for msg in (outcome.get("error"), stderr_close_error)
            if isinstance(msg, str) and msg
        ]
        ok = bool(outcome["ok"] and not combined_errors)
        return {
            "owner": "persistent_input",
            "present": bool(outcome["present"]),
            "pid": outcome["pid"],
            "already_exited": bool(outcome["already_exited"]),
            "exit_code": outcome["exit_code"],
            "terminated_cleanly": bool(ok and not outcome["killed_with_sigkill"]),
            "killed_with_sigkill": bool(outcome["killed_with_sigkill"]),
            "reaped": bool(outcome["reaped"]),
            "ok": ok,
            "error": "; ".join(combined_errors) if combined_errors else None,
        }


def stop_owned_process(
    proc: Any,
    *,
    owner_name: str,
    term_timeout_sec: float = 5.0,
    kill_timeout_sec: float = 2.0,
) -> dict[str, Any]:
    if proc is None:
        return {
            "owner": owner_name,
            "present": False,
            "pid": None,
            "already_exited": True,
            "exit_code": None,
            "terminated_cleanly": True,
            "killed_with_sigkill": False,
            "reaped": True,
            "ok": True,
            "error": None,
        }

    pid: int | None = getattr(proc, "pid", None)
    already_exited = False
    killed_with_sigkill = False
    reaped = False
    exit_code: int | None = None
    errors: list[str] = []

    try:
        try:
            polled = proc.poll()
        except Exception as poll_exc:  # noqa: BLE001
            polled = None
            errors.append(
                f"{owner_name}: poll() failed: {redact_sensitive_text(str(poll_exc))}"
            )

        if polled is not None and not errors:
            already_exited = True
            reaped = True
            exit_code = polled
        else:
            need_sigkill = False
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            except Exception as term_exc:  # noqa: BLE001
                errors.append(
                    f"{owner_name}: terminate() failed: {redact_sensitive_text(str(term_exc))}"
                )
                need_sigkill = True

            if not need_sigkill:
                try:
                    exit_code = proc.wait(timeout=max(0.05, term_timeout_sec))
                    reaped = True
                except subprocess.TimeoutExpired:
                    need_sigkill = True
                except Exception as wait_term_exc:  # noqa: BLE001
                    errors.append(
                        f"{owner_name}: wait() after SIGTERM failed: "
                        f"{redact_sensitive_text(str(wait_term_exc))}"
                    )
                    try:
                        if proc.poll() is not None:
                            exit_code = proc.returncode
                            reaped = True
                        else:
                            need_sigkill = True
                    except Exception:  # noqa: BLE001
                        need_sigkill = True

            if need_sigkill and not reaped:
                killed_with_sigkill = True
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                except Exception as kill_exc:  # noqa: BLE001
                    errors.append(
                        f"{owner_name}: kill() failed: {redact_sensitive_text(str(kill_exc))}"
                    )
                try:
                    exit_code = proc.wait(timeout=max(0.05, kill_timeout_sec))
                    reaped = True
                except subprocess.TimeoutExpired as timeout_exc:
                    errors.append(
                        f"{owner_name}: process pid={pid!r} did not exit within "
                        f"{max(0.05, kill_timeout_sec):.2f}s after SIGKILL "
                        f"({redact_sensitive_text(str(timeout_exc))})"
                    )
                except Exception as wait_kill_exc:  # noqa: BLE001
                    errors.append(
                        f"{owner_name}: wait() after SIGKILL failed: "
                        f"{redact_sensitive_text(str(wait_kill_exc))}"
                    )
                    try:
                        if proc.poll() is not None:
                            exit_code = proc.returncode
                            reaped = True
                    except Exception:  # noqa: BLE001
                        pass

            if reaped and exit_code is None:
                exit_code = getattr(proc, "returncode", None)
    finally:
        stdout_pipe = getattr(proc, "stdout", None)
        if stdout_pipe is not None:
            try:
                stdout_pipe.close()
            except Exception:  # noqa: BLE001
                pass

    error_summary = "; ".join(errors) if errors else None
    ok = bool(reaped and not errors)
    return {
        "owner": owner_name,
        "present": True,
        "pid": pid,
        "already_exited": already_exited,
        "exit_code": exit_code,
        "terminated_cleanly": bool(ok and not killed_with_sigkill),
        "killed_with_sigkill": killed_with_sigkill,
        "reaped": reaped,
        "ok": ok,
        "error": error_summary,
    }


def cleanup_owned_scene_processes(
    owners: Sequence[tuple[str, Any, float, float]],
    *,
    log_handles: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    owner_results: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    for owner_name, target, term_timeout_sec, kill_timeout_sec in owners:
        try:
            if target is None:
                res = stop_owned_process(
                    None,
                    owner_name=owner_name,
                    term_timeout_sec=term_timeout_sec,
                    kill_timeout_sec=kill_timeout_sec,
                )
            elif isinstance(target, PersistentGuestInputFixture) or (
                callable(getattr(target, "stop", None))
                and not callable(getattr(target, "poll", None))
            ):
                raw_stop = target.stop(
                    term_timeout_sec=term_timeout_sec,
                    kill_timeout_sec=kill_timeout_sec,
                )
                if not isinstance(raw_stop, dict):
                    raise AcceptanceError(
                        f"{owner_name}: stop() returned non-dict result {raw_stop!r}"
                    )
                stop_err = raw_stop.get("error")
                reaped = raw_stop.get("reaped", True) is True
                ok_flag = raw_stop.get("ok", True) is True and reaped and not stop_err
                res = {
                    "owner": owner_name,
                    "present": bool(
                        raw_stop.get("present", raw_stop.get("pid") is not None)
                    ),
                    "pid": raw_stop.get("pid"),
                    "already_exited": bool(raw_stop.get("already_exited", False)),
                    "exit_code": raw_stop.get("exit_code"),
                    "terminated_cleanly": bool(
                        ok_flag and raw_stop.get("terminated_cleanly", False)
                    ),
                    "killed_with_sigkill": bool(
                        raw_stop.get("killed_with_sigkill", False)
                    ),
                    "reaped": reaped,
                    "ok": ok_flag,
                    "error": (
                        redact_sensitive_text(str(stop_err))
                        if stop_err
                        else (
                            None
                            if ok_flag
                            else f"{owner_name}: fixture stop reported unsuccessful state"
                        )
                    ),
                }
            else:
                res = stop_owned_process(
                    target,
                    owner_name=owner_name,
                    term_timeout_sec=term_timeout_sec,
                    kill_timeout_sec=kill_timeout_sec,
                )
        except Exception as exc:  # noqa: BLE001
            err_msg = (
                f"{owner_name}: unexpected cleanup exception: "
                f"{redact_sensitive_text(str(exc))}"
            )
            res = {
                "owner": owner_name,
                "present": True,
                "pid": getattr(target, "pid", None),
                "already_exited": False,
                "exit_code": None,
                "terminated_cleanly": False,
                "killed_with_sigkill": False,
                "reaped": False,
                "ok": False,
                "error": err_msg,
            }

        owner_results[owner_name] = res
        if not res["ok"]:
            errors.append(res["error"] or f"{owner_name}: owned process cleanup failed")

    log_results: dict[str, dict[str, Any]] = {}
    if log_handles:
        for log_name, handle in log_handles.items():
            if handle is None:
                log_results[log_name] = {"closed": True, "ok": True, "error": None}
                continue
            handle_errors: list[str] = []
            try:
                if not getattr(handle, "closed", False):
                    try:
                        handle.flush()
                    except Exception as flush_exc:  # noqa: BLE001
                        handle_errors.append(
                            f"{log_name}: flush() failed: {redact_sensitive_text(str(flush_exc))}"
                        )
                    handle.close()
            except Exception as close_exc:  # noqa: BLE001
                handle_errors.append(
                    f"{log_name}: close() failed: {redact_sensitive_text(str(close_exc))}"
                )
            is_closed = bool(getattr(handle, "closed", False))
            handle_ok = is_closed and not handle_errors
            err_text = "; ".join(handle_errors) if handle_errors else None
            log_results[log_name] = {
                "closed": is_closed,
                "ok": handle_ok,
                "error": err_text,
            }
            if not handle_ok:
                errors.append(err_text or f"{log_name}: log handle failed to close")

    return {
        "ok": len(errors) == 0,
        "owners": owner_results,
        "log_handles": log_results,
        "errors": errors,
    }


def _extract_keyboard_layout_name(device: Mapping[str, Any]) -> str:
    active = device.get("xkb_active_layout_name")
    if isinstance(active, str) and active.strip():
        return active.strip()
    layouts = device.get("xkb_layout_names")
    if isinstance(layouts, list):
        for item in layouts:
            if isinstance(item, str) and item.strip():
                return item.strip()
    return ""


def verify_seat0_input_state(
    seats_payload: Any,
    inputs_payload: Any,
) -> dict[str, Any]:
    if not isinstance(seats_payload, list):
        raise AcceptanceError(
            f"Expected list from sway get_seats, got {type(seats_payload).__name__}"
        )
    if not isinstance(inputs_payload, list):
        raise AcceptanceError(
            f"Expected list from sway get_inputs, got {type(inputs_payload).__name__}"
        )

    seat0_entries: list[dict[str, Any]] = []
    foreign_seat_by_id: dict[str, str] = {}
    for entry in seats_payload:
        if not isinstance(entry, dict):
            raise AcceptanceError(
                f"Expected object entries in sway get_seats, got {entry!r}"
            )
        seat_name = entry.get("name")
        if not isinstance(seat_name, str) or not seat_name.strip():
            raise AcceptanceError(
                f"Expected non-empty string 'name' in sway get_seats entry, got {entry!r}"
            )
        if seat_name == "seat0":
            seat0_entries.append(entry)
        else:
            other_devices = entry.get("devices")
            if other_devices is not None and not isinstance(other_devices, list):
                raise AcceptanceError(
                    f"Expected list in {seat_name}.devices from sway get_seats, got {other_devices!r}"
                )
            if isinstance(other_devices, list):
                for foreign_dev in other_devices:
                    if isinstance(foreign_dev, dict):
                        fid = foreign_dev.get("identifier")
                        if isinstance(fid, str) and fid.strip():
                            foreign_seat_by_id[fid.strip()] = seat_name

    if not seat0_entries:
        raise AcceptanceError("Compositor get_seats does not contain required 'seat0'.")
    if len(seat0_entries) > 1:
        raise AcceptanceError(
            f"Compositor get_seats returned duplicate 'seat0' entries ({len(seat0_entries)})."
        )
    seat0 = seat0_entries[0]

    caps = _require_non_bool_int(seat0.get("capabilities"), "seat0.capabilities")
    if caps != REQUIRED_SEAT0_CAPABILITIES:
        raise AcceptanceError(
            f"Expected seat0 capabilities pointer|keyboard ({REQUIRED_SEAT0_CAPABILITIES}), got {caps}"
        )

    seat_devices_field = seat0.get("devices")
    if not isinstance(seat_devices_field, list):
        raise AcceptanceError(
            "Compositor seat0.devices must be a valid membership list; "
            f"got {type(seat_devices_field).__name__} ({seat_devices_field!r})"
        )
    if len(seat_devices_field) < 2:
        raise AcceptanceError(
            f"Expected pointer and keyboard attached to seat0.devices, got {seat_devices_field!r}"
        )

    seat0_members: dict[str, tuple[str, str | None]] = {}
    for raw_member in seat_devices_field:
        if not isinstance(raw_member, dict):
            raise AcceptanceError(
                f"Invalid non-object device entry in seat0.devices: {raw_member!r}"
            )
        member_seat = raw_member.get("seat")
        if member_seat is not None and member_seat != "seat0":
            raise AcceptanceError(
                f"Foreign seat tag {member_seat!r} inside seat0.devices entry {raw_member!r}"
            )
        raw_id = raw_member.get("identifier")
        raw_name = raw_member.get("name")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise AcceptanceError(
                f"Device entry in seat0.devices missing valid identifier: {raw_member!r}"
            )
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise AcceptanceError(
                f"Device entry in seat0.devices missing valid name: {raw_member!r}"
            )
        member_id = raw_id.strip()
        member_name = raw_name.strip()
        member_type: str | None = None
        if "type" in raw_member:
            raw_type = raw_member.get("type")
            if not isinstance(raw_type, str) or not raw_type.strip():
                raise AcceptanceError(
                    f"Device entry in seat0.devices has invalid type: {raw_member!r}"
                )
            member_type = raw_type.strip()
        if member_id in foreign_seat_by_id:
            raise AcceptanceError(
                f"Device {member_id!r} in seat0.devices is also claimed by foreign seat "
                f"{foreign_seat_by_id[member_id]!r}."
            )
        if member_id in seat0_members:
            raise AcceptanceError(
                f"Duplicate device identifier {member_id!r} in seat0.devices."
            )
        seat0_members[member_id] = (member_name, member_type)

    matched_seat0_ids: set[str] = set()
    pointer_devices: list[dict[str, str]] = []
    keyboard_devices: list[dict[str, str]] = []
    identities: list[dict[str, str]] = []

    for dev in inputs_payload:
        if not isinstance(dev, dict):
            raise AcceptanceError(
                f"Expected object entries in sway get_inputs, got {dev!r}"
            )
        raw_id = dev.get("identifier")
        if not isinstance(raw_id, str) or not raw_id.strip():
            continue
        identifier = raw_id.strip()
        if identifier not in seat0_members:
            continue
        if identifier in matched_seat0_ids:
            raise AcceptanceError(
                f"Duplicate seat0 device identifier {identifier!r} in sway get_inputs."
            )
        dev_seat = dev.get("seat")
        if dev_seat is not None and dev_seat != "seat0":
            raise AcceptanceError(
                f"Device {identifier!r} matched seat0.devices but get_inputs reports foreign seat {dev_seat!r}."
            )
        raw_name = dev.get("name")
        raw_type = dev.get("type")
        if not isinstance(raw_name, str) or not raw_name.strip():
            raise AcceptanceError(
                f"seat0 device {identifier!r} in sway get_inputs has invalid name: {dev!r}"
            )
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise AcceptanceError(
                f"seat0 device {identifier!r} in sway get_inputs has invalid type: {dev!r}"
            )
        name = raw_name.strip()
        dev_type = raw_type.strip()
        expected_name, expected_type = seat0_members[identifier]
        if name != expected_name:
            raise AcceptanceError(
                f"seat0 device {identifier!r} name mismatch between seat0.devices ({expected_name!r}) "
                f"and get_inputs ({name!r})."
            )
        if expected_type is not None and dev_type != expected_type:
            raise AcceptanceError(
                f"seat0 device {identifier!r} type mismatch between seat0.devices ({expected_type!r}) "
                f"and get_inputs ({dev_type!r})."
            )
        matched_seat0_ids.add(identifier)

        if dev_type == "pointer":
            pointer_devices.append(
                {
                    "identifier": identifier,
                    "name": name,
                    "type": "pointer",
                }
            )
            identities.append(
                {
                    "type": "pointer",
                    "identifier": identifier,
                    "name": name,
                }
            )
        elif dev_type == "keyboard":
            layout_name = _extract_keyboard_layout_name(dev)
            if not layout_name:
                raise AcceptanceError(
                    f"Keyboard device {identifier!r} ({name!r}) on seat0 has no active XKB keymap layout."
                )
            keyboard_devices.append(
                {
                    "identifier": identifier,
                    "name": name,
                    "type": "keyboard",
                    "xkb_active_layout_name": layout_name,
                }
            )
            identities.append(
                {
                    "type": "keyboard",
                    "identifier": identifier,
                    "name": name,
                }
            )

    missing_from_inputs = sorted(set(seat0_members) - matched_seat0_ids)
    if missing_from_inputs:
        raise AcceptanceError(
            f"seat0.devices member(s) missing from sway get_inputs: {missing_from_inputs!r}"
        )
    if not pointer_devices:
        raise AcceptanceError("No virtual pointer device attached to seat0 in sway get_inputs.")
    if not keyboard_devices:
        raise AcceptanceError("No virtual keyboard device attached to seat0 in sway get_inputs.")

    pointer_devices.sort(key=lambda item: (item["identifier"], item["name"]))
    keyboard_devices.sort(key=lambda item: (item["identifier"], item["name"]))
    identities.sort(key=lambda item: (item["type"], item["identifier"], item["name"]))

    return {
        "seat": "seat0",
        "capabilities": caps,
        "pointer_devices": pointer_devices,
        "keyboard_devices": keyboard_devices,
        "device_identities": identities,
    }


def verify_input_device_identities_preserved(
    before_state: Mapping[str, Any],
    after_state: Mapping[str, Any],
    *,
    helper_pid_before: int,
    helper_pid_after: int | None,
    helper_running_after: bool,
    setup_keypresses_sent: int = 0,
) -> dict[str, Any]:
    if before_state.get("seat") != "seat0" or after_state.get("seat") != "seat0":
        raise AcceptanceError("Input state seat mismatch; expected 'seat0' before and after.")
    caps_before = _require_non_bool_int(
        before_state.get("capabilities"),
        "before_state.capabilities",
    )
    caps_after = _require_non_bool_int(
        after_state.get("capabilities"),
        "after_state.capabilities",
    )
    if caps_before != REQUIRED_SEAT0_CAPABILITIES or caps_after != REQUIRED_SEAT0_CAPABILITIES:
        raise AcceptanceError(
            f"Expected seat0 capabilities {REQUIRED_SEAT0_CAPABILITIES} before and after, "
            f"got before={caps_before}, after={caps_after}"
        )

    checked_pid_before = _require_non_bool_int(helper_pid_before, "helper_pid_before")
    if checked_pid_before <= 0:
        raise AcceptanceError(f"Invalid helper_pid_before: {checked_pid_before}")
    if helper_running_after is not True:
        raise AcceptanceError(
            "Persistent guest input helper was not running continuously until scene cleanup."
        )
    if isinstance(helper_pid_after, bool) or helper_pid_after != checked_pid_before:
        raise AcceptanceError(
            f"Persistent guest input helper PID changed during run "
            f"(before={checked_pid_before}, after={helper_pid_after!r})"
        )

    checked_keypresses = _require_non_bool_int(
        setup_keypresses_sent,
        "setup_keypresses_sent",
    )
    if checked_keypresses != 0:
        raise AcceptanceError(
            f"Persistent guest input setup must send 0 keypresses, got {checked_keypresses}"
        )

    before_ids = before_state.get("device_identities")
    after_ids = after_state.get("device_identities")
    if not isinstance(before_ids, list) or not before_ids:
        raise AcceptanceError("before_state.device_identities must be a non-empty list.")
    if before_ids != after_ids:
        raise AcceptanceError(
            f"seat0 input device identities changed across acceptance run: "
            f"before={before_ids!r}, after={after_ids!r}"
        )

    return {
        "seat": "seat0",
        "capabilities_before": caps_before,
        "capabilities_after": caps_after,
        "setup_keypresses_sent": checked_keypresses,
        "helper_pid": checked_pid_before,
        "helper_running_before_cleanup": True,
        "identities_preserved": True,
        "pointer_devices": list(after_state.get("pointer_devices") or []),
        "keyboard_devices": list(after_state.get("keyboard_devices") or []),
        "device_identities": list(after_ids),
    }


def _count_band_foreground_contrast_pixels(
    window_rgb_bytes: bytes,
    *,
    phys_w: int,
    phys_h: int,
    bx0: int,
    by0: int,
    bx1: int,
    by1: int,
) -> int:
    bucket_stats: dict[tuple[int, int, int], list[int]] = {}
    for py in range(by0, by1, 2):
        row_base = py * phys_w * 3
        for px in range(bx0, bx1):
            idx = row_base + px * 3
            r = window_rgb_bytes[idx]
            g = window_rgb_bytes[idx + 1]
            b = window_rgb_bytes[idx + 2]
            key = (r >> 3, g >> 3, b >> 3)
            acc = bucket_stats.get(key)
            if acc is None:
                bucket_stats[key] = [1, r, g, b]
            else:
                acc[0] += 1
                acc[1] += r
                acc[2] += g
                acc[3] += b

    if not bucket_stats:
        return 0

    dominant = max(bucket_stats.values(), key=lambda item: item[0])
    dom_count = dominant[0]
    bg_r = dominant[1] / float(dom_count)
    bg_g = dominant[2] / float(dom_count)
    bg_b = dominant[3] / float(dom_count)

    contrast_pixels = 0
    for py in range(by0, by1, 2):
        row_base = py * phys_w * 3
        for px in range(bx0, bx1):
            idx = row_base + px * 3
            r = window_rgb_bytes[idx]
            g = window_rgb_bytes[idx + 1]
            b = window_rgb_bytes[idx + 2]
            if (
                max(abs(r - bg_r), abs(g - bg_g), abs(b - bg_b))
                >= MIN_MAIN_BAND_CONTRAST_DELTA
            ):
                contrast_pixels += 1
    return contrast_pixels


def evaluate_main_window_paint_readiness(
    *,
    tree_node: Mapping[str, Any] | None,
    expected_width: int,
    expected_height: int,
    output_rect: Mapping[str, Any],
    output_scale: float,
    bar_px_h: int,
    changed_pixels: int,
    bbox_ws: tuple[int, int, int, int] | None,
    window_rgb_bytes: bytes,
    screenshot_name: str,
) -> dict[str, Any]:
    if not isinstance(tree_node, Mapping) or tree_node.get("visible") is not True:
        raise AcceptanceError("Main window is not visible in sway get_tree yet.")
    if tree_node.get("shell") != "xdg_shell":
        raise AcceptanceError(
            f"Expected native Wayland xdg_shell for main window, got {tree_node.get('shell')!r}"
        )

    native_rect = tree_node.get("rect")
    if not isinstance(native_rect, Mapping):
        raise AcceptanceError("Main window node in sway get_tree is missing 'rect'.")
    native_x = _require_non_bool_number(native_rect.get("x"), "main_node.rect.x")
    native_y = _require_non_bool_number(native_rect.get("y"), "main_node.rect.y")
    native_w = _require_non_bool_number(native_rect.get("width"), "main_node.rect.width")
    native_h = _require_non_bool_number(native_rect.get("height"), "main_node.rect.height")
    if int(round(native_w)) != expected_width or int(round(native_h)) != expected_height:
        raise AcceptanceError(
            f"Main window native rect {native_w}x{native_h} does not match expected "
            f"{expected_width}x{expected_height}"
        )

    if output_scale <= 0:
        raise AcceptanceError(f"output_scale must be > 0, got {output_scale}")
    if changed_pixels <= 0 or bbox_ws is None:
        raise AcceptanceError(
            f"Main window ({expected_width}x{expected_height}) has 0 rendered workspace diff pixels."
        )

    left, upper, right, lower = bbox_ws
    phys_x = int(left)
    phys_y = int(bar_px_h + upper)
    phys_w = int(right - left)
    phys_h = int(lower - upper)
    if phys_w <= 0 or phys_h <= 0:
        raise AcceptanceError(
            f"Invalid main window rendered physical bbox: {bbox_ws!r}"
        )
    if len(window_rgb_bytes) != phys_w * phys_h * 3:
        raise AcceptanceError(
            f"Expected {phys_w * phys_h * 3} RGB bytes for {phys_w}x{phys_h} window crop, "
            f"got {len(window_rgb_bytes)}"
        )

    fill_ratio = changed_pixels / float(phys_w * phys_h)
    if fill_ratio < MIN_MAIN_FILL_RATIO:
        raise AcceptanceError(
            f"Main window rendered diff bbox is too sparse (fill_ratio={fill_ratio:.4f} < {MIN_MAIN_FILL_RATIO})"
        )

    out_x = _require_non_bool_number(output_rect.get("x"), "output_rect.x")
    out_y = _require_non_bool_number(output_rect.get("y"), "output_rect.y")
    logical_x = out_x + (phys_x / output_scale)
    logical_y = out_y + (phys_y / output_scale)
    logical_w = phys_w / output_scale
    logical_h = phys_h / output_scale

    if (
        abs(logical_w - expected_width) > MAIN_RENDERED_LOGICAL_TOLERANCE
        or abs(logical_h - expected_height) > MAIN_RENDERED_LOGICAL_TOLERANCE
    ):
        raise AcceptanceError(
            f"Rendered main window logical size {logical_w:.2f}x{logical_h:.2f} does not match "
            f"expected {expected_width}x{expected_height} +/- {MAIN_RENDERED_LOGICAL_TOLERANCE}"
        )
    if (
        abs(logical_x - native_x) > MAIN_RENDERED_LOGICAL_TOLERANCE
        or abs(logical_y - native_y) > MAIN_RENDERED_LOGICAL_TOLERANCE
    ):
        raise AcceptanceError(
            f"Rendered main window origin ({logical_x:.2f}, {logical_y:.2f}) disagrees with "
            f"native sway rect origin ({native_x:.2f}, {native_y:.2f})"
        )

    inset_x = min(max(2, phys_w // 12), max(4, int(round(16.0 * output_scale))))
    inset_y_top = min(max(2, phys_h // 16), max(4, int(round(8.0 * output_scale))))
    inset_y_bot = min(max(2, phys_h // 12), max(4, int(round(16.0 * output_scale))))
    if phys_w <= inset_x * 2 + 4 or phys_h <= inset_y_top + inset_y_bot + 4:
        raise AcceptanceError(
            f"Rendered main window {phys_w}x{phys_h} too small for interior paint verification"
        )

    sample_count = 0
    sum_r = 0.0
    sum_g = 0.0
    sum_b = 0.0
    sum_r2 = 0.0
    sum_g2 = 0.0
    sum_b2 = 0.0
    color_buckets: set[tuple[int, int, int]] = set()

    for py in range(inset_y_top, phys_h - inset_y_bot, 2):
        row_base = py * phys_w * 3
        for px in range(inset_x, phys_w - inset_x, 2):
            idx = row_base + px * 3
            r = window_rgb_bytes[idx]
            g = window_rgb_bytes[idx + 1]
            b = window_rgb_bytes[idx + 2]
            sample_count += 1
            sum_r += r
            sum_g += g
            sum_b += b
            sum_r2 += r * r
            sum_g2 += g * g
            sum_b2 += b * b
            color_buckets.add((r >> 4, g >> 4, b >> 4))

    if sample_count == 0:
        raise AcceptanceError("Zero interior samples collected for main window paint check.")

    std_r = math.sqrt(max(0.0, (sum_r2 / sample_count) - (sum_r / sample_count) ** 2))
    std_g = math.sqrt(max(0.0, (sum_g2 / sample_count) - (sum_g / sample_count) ** 2))
    std_b = math.sqrt(max(0.0, (sum_b2 / sample_count) - (sum_b / sample_count) ** 2))
    max_stddev = max(std_r, std_g, std_b)

    if (
        max_stddev < MIN_MAIN_INTERIOR_CHANNEL_STDDEV
        or len(color_buckets) < MIN_MAIN_INTERIOR_COLOR_BUCKETS
    ):
        raise AcceptanceError(
            f"Main window ({expected_width}x{expected_height}) appears unpainted/fallback rectangle "
            f"(interior_max_stddev={max_stddev:.2f}, color_buckets={len(color_buckets)})"
        )

    if expected_width >= 761:
        logical_bands = {
            "titlebar": (16.0, 6.0, float(expected_width - 16), 38.0),
            "navigation": (
                16.0,
                48.0,
                float(min(260, expected_width - 16)),
                float(min(expected_height - 16, 420)),
            ),
            "workspace_body": (
                288.0,
                48.0,
                float(expected_width - 16),
                float(min(expected_height - 16, 480)),
            ),
        }
    else:
        logical_bands = {
            "titlebar": (16.0, 6.0, float(expected_width - 16), 38.0),
            "navigation": (16.0, 46.0, float(expected_width - 16), 92.0),
            "workspace_body": (
                16.0,
                102.0,
                float(expected_width - 16),
                float(min(expected_height - 16, 484)),
            ),
        }

    scale_x = phys_w / float(expected_width)
    scale_y = phys_h / float(expected_height)
    band_contrast_pixels: dict[str, int] = {}

    for band_name, (lx0, ly0, lx1, ly1) in logical_bands.items():
        bx0 = max(0, min(phys_w - 1, int(round(lx0 * scale_x))))
        bx1 = max(bx0 + 1, min(phys_w, int(round(lx1 * scale_x))))
        by0 = max(0, min(phys_h - 1, int(round(ly0 * scale_y))))
        by1 = max(by0 + 1, min(phys_h, int(round(ly1 * scale_y))))
        band_px = _count_band_foreground_contrast_pixels(
            window_rgb_bytes,
            phys_w=phys_w,
            phys_h=phys_h,
            bx0=bx0,
            by0=by0,
            bx1=bx1,
            by1=by1,
        )
        band_contrast_pixels[band_name] = band_px
        if band_px < MIN_MAIN_BAND_CONTRAST_PIXELS:
            raise AcceptanceError(
                f"Main window ({expected_width}x{expected_height}) band {band_name!r} has "
                f"insufficient painted content ({band_px} contrast pixels < {MIN_MAIN_BAND_CONTRAST_PIXELS})"
            )

    return {
        "painted": True,
        "measurement_method": "grim_pixel_diff_and_interior_contrast",
        "expected_size": {"width": expected_width, "height": expected_height},
        "native_rect": {
            "x": int(round(native_x)),
            "y": int(round(native_y)),
            "width": int(round(native_w)),
            "height": int(round(native_h)),
        },
        "physical_rect": {
            "x": phys_x,
            "y": phys_y,
            "width": phys_w,
            "height": phys_h,
        },
        "logical_rect": {
            "x": round(logical_x, 2),
            "y": round(logical_y, 2),
            "width": round(logical_w, 2),
            "height": round(logical_h, 2),
        },
        "changed_pixels": int(changed_pixels),
        "fill_ratio": round(fill_ratio, 4),
        "interior_max_channel_stddev": round(max_stddev, 3),
        "interior_color_buckets": len(color_buckets),
        "band_contrast_pixels": band_contrast_pixels,
        "screenshot": str(screenshot_name),
    }


def _sanitize_diagnostic_name(raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", str(raw).strip()).strip("-")
    return cleaned or "item"


def _write_bounded_diagnostic_text(artifact_dir: Path, filename: str, content: str) -> str:
    safe_name = _sanitize_diagnostic_name(filename)
    target = artifact_dir / safe_name
    if target.exists() or target.is_symlink():
        st = os.lstat(target)
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode):
            raise AcceptanceError(f"Refusing non-regular diagnostic file: {target}")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, content.encode("utf-8", errors="replace"))
    finally:
        os.close(fd)
    return safe_name


def collect_bounded_failure_diagnostics(
    *,
    artifact_dir: Path,
    client_env: Mapping[str, str] | None,
    owned_log_paths: Mapping[str, Path],
    output_names: Sequence[str] = ("HEADLESS-1", "HEADLESS-2"),
    command_runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    command_timeout_sec: float = 5.0,
) -> dict[str, Any]:
    """Capture bounded failure evidence without masking the original error or reading user state."""
    runner = subprocess.run if command_runner is None else command_runner
    summary: dict[str, Any] = {
        "outputs": {},
        "sway_queries": {},
        "owned_logs": {},
    }

    try:
        checked_dir = validate_safe_absolute_path(artifact_dir, "artifact_dir")
        ensure_directory_preserving_existing(checked_dir, "artifact_dir")
    except Exception as exc:  # noqa: BLE001
        summary["artifact_dir_error"] = redact_sensitive_text(str(exc))
        return summary

    isolated_env_valid = False
    if client_env is not None:
        try:
            validate_isolated_guest_wayland_env(client_env, require_socket=False)
            isolated_env_valid = True
        except Exception as exc:  # noqa: BLE001
            summary["wayland_capture_skipped"] = redact_sensitive_text(str(exc))
    else:
        summary["wayland_capture_skipped"] = "Wayland client_env not initialized yet."

    if isolated_env_valid and client_env is not None:
        env_copy = dict(client_env)
        for out_name in output_names:
            safe_out = _sanitize_diagnostic_name(out_name)
            shot_name = f"failure-output-{safe_out}.png"
            shot_path = checked_dir / shot_name
            try:
                assert_command_allowed(["grim", "-o", str(out_name), str(shot_path)])
                res = runner(
                    ["grim", "-o", str(out_name), str(shot_path)],
                    env=env_copy,
                    check=False,
                    text=True,
                    capture_output=True,
                    timeout=command_timeout_sec,
                )
                if res.returncode == 0 and shot_path.is_file():
                    summary["outputs"][str(out_name)] = {
                        "captured": True,
                        "file": shot_name,
                        "size_bytes": int(shot_path.stat().st_size),
                    }
                else:
                    detail = redact_sensitive_text(
                        (res.stderr or res.stdout or "").strip()
                    )[:512]
                    summary["outputs"][str(out_name)] = {
                        "captured": False,
                        "exit_code": res.returncode,
                        "error": detail,
                    }
            except Exception as exc:  # noqa: BLE001
                summary["outputs"][str(out_name)] = {
                    "captured": False,
                    "error": redact_sensitive_text(str(exc))[:512],
                }

        sway_sock = str(env_copy.get("SWAYSOCK") or "").strip()
        if sway_sock:
            for label, query_type in (
                ("outputs", "get_outputs"),
                ("seats", "get_seats"),
                ("inputs", "get_inputs"),
                ("tree", "get_tree"),
            ):
                out_file = f"failure-sway-{label}.json"
                try:
                    assert_command_allowed(["swaymsg", "-t", query_type, "-r"])
                    res = runner(
                        ["swaymsg", "-t", query_type, "-r"],
                        env=env_copy,
                        check=False,
                        text=True,
                        capture_output=True,
                        timeout=command_timeout_sec,
                    )
                    raw_out = res.stdout or ""
                    encoded = raw_out.encode("utf-8", errors="replace")
                    truncated = len(encoded) > MAX_DIAGNOSTIC_JSON_BYTES
                    bounded_text = redact_sensitive_text(
                        encoded[:MAX_DIAGNOSTIC_JSON_BYTES].decode("utf-8", errors="replace")
                    )
                    if res.returncode == 0 and bounded_text.strip():
                        saved = _write_bounded_diagnostic_text(
                            checked_dir,
                            out_file,
                            bounded_text + ("\n" if not bounded_text.endswith("\n") else ""),
                        )
                        summary["sway_queries"][label] = {
                            "captured": True,
                            "file": saved,
                            "size_bytes": len(encoded),
                            "truncated": truncated,
                        }
                    else:
                        detail = redact_sensitive_text(
                            (res.stderr or res.stdout or "").strip()
                        )[:512]
                        summary["sway_queries"][label] = {
                            "captured": False,
                            "exit_code": res.returncode,
                            "error": detail,
                        }
                except Exception as exc:  # noqa: BLE001
                    summary["sway_queries"][label] = {
                        "captured": False,
                        "error": redact_sensitive_text(str(exc))[:512],
                    }

    for log_label, log_path in owned_log_paths.items():
        safe_label = _sanitize_diagnostic_name(log_label)
        try:
            tail_info = read_bounded_redacted_log_tail(
                Path(log_path),
                allowed_root=checked_dir,
                max_bytes=MAX_DIAGNOSTIC_LOG_TAIL_BYTES,
            )
            tail_text = str(tail_info.get("tail") or "")
            tail_file: str | None = None
            if tail_info.get("present") is True:
                tail_file = _write_bounded_diagnostic_text(
                    checked_dir,
                    f"failure-log-tail-{safe_label}.txt",
                    tail_text,
                )
            summary["owned_logs"][str(log_label)] = {
                "present": bool(tail_info.get("present")),
                "size_bytes": int(tail_info.get("size_bytes") or 0),
                "truncated": bool(tail_info.get("truncated")),
                "tail_file": tail_file,
                "tail_excerpt": tail_text[-2048:],
            }
        except Exception as exc:  # noqa: BLE001
            summary["owned_logs"][str(log_label)] = {
                "present": False,
                "error": redact_sensitive_text(str(exc))[:512],
            }

    try:
        _write_bounded_diagnostic_text(
            checked_dir,
            "failure-diagnostics.json",
            json.dumps(summary, indent=2) + "\n",
        )
    except Exception as exc:  # noqa: BLE001
        summary["write_error"] = redact_sensitive_text(str(exc))[:512]

    return summary


def complete_owned_scene_and_write_passed_report(
    *,
    report_path: Path,
    report_payload: Mapping[str, Any],
    owners: Sequence[tuple[str, Any, float, float]],
    log_handles: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    scene_cleanup = cleanup_owned_scene_processes(
        owners,
        log_handles=log_handles,
    )
    report_doc = dict(report_payload)
    guest_input_raw = report_doc.get("guest_input")
    if isinstance(guest_input_raw, Mapping):
        guest_input_copy = dict(guest_input_raw)
        guest_input_copy["cleanup_shutdown"] = scene_cleanup["owners"].get(
            "persistent_input"
        )
        report_doc["guest_input"] = guest_input_copy
    report_doc["scene_cleanup"] = scene_cleanup

    if not scene_cleanup["ok"]:
        err_msg = (
            "Owned scene cleanup failed after acceptance checks: "
            + "; ".join(scene_cleanup["errors"])
        )
        failed_doc = {
            "schema_version": report_doc.get("schema_version", 2),
            "status": "failed",
            "error": redact_sensitive_text(err_msg),
            "compositor_invariants": report_doc.get("compositor_invariants"),
            "checks": report_doc.get("checks"),
            "scene_cleanup": scene_cleanup,
        }
        try:
            report_path.write_text(
                json.dumps(failed_doc, indent=2) + "\n",
                encoding="utf-8",
            )
        except OSError:
            pass
        raise SceneCleanupError(err_msg, scene_cleanup=scene_cleanup)

    if report_doc.get("status") != "passed":
        raise AcceptanceError(
            "Expected passed status in complete_owned_scene_and_write_passed_report, "
            f"got {report_doc.get('status')!r}"
        )
    report_path.write_text(
        json.dumps(report_doc, indent=2) + "\n",
        encoding="utf-8",
    )
    return scene_cleanup


def record_owned_scene_failure(
    *,
    report_path: Path,
    exc: BaseException,
    compositor_invariants: Mapping[str, Any] | None,
    checks: Mapping[str, bool],
    artifact_dir: Path,
    client_env: Mapping[str, str] | None,
    owned_log_paths: Mapping[str, Path],
    owners: Sequence[tuple[str, Any, float, float]],
    log_handles: Mapping[str, Any] | None = None,
    prior_cleanup: dict[str, Any] | None = None,
    command_runner: Any = None,
) -> dict[str, Any]:
    if log_handles:
        for handle in log_handles.values():
            if handle is not None and not getattr(handle, "closed", False):
                try:
                    handle.flush()
                except Exception:  # noqa: BLE001
                    pass

    failure_diagnostics: dict[str, Any] = {}
    try:
        failure_diagnostics = collect_bounded_failure_diagnostics(
            artifact_dir=artifact_dir,
            client_env=client_env,
            owned_log_paths=owned_log_paths,
            command_runner=command_runner,
        )
    except Exception as diag_exc:  # noqa: BLE001
        failure_diagnostics = {
            "collection_error": redact_sensitive_text(str(diag_exc))
        }

    scene_cleanup = (
        prior_cleanup
        if prior_cleanup is not None
        else cleanup_owned_scene_processes(
            owners,
            log_handles=log_handles,
        )
    )

    failed_report: dict[str, Any] = {
        "schema_version": 2,
        "status": "failed",
        "error": redact_sensitive_text(str(exc)),
        "compositor_invariants": (
            dict(compositor_invariants)
            if isinstance(compositor_invariants, Mapping)
            else compositor_invariants
        ),
        "checks": dict(checks),
        "failure_diagnostics": failure_diagnostics,
        "scene_cleanup": scene_cleanup,
    }
    try:
        report_path.write_text(
            json.dumps(failed_report, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError:
        pass
    return failed_report


def _verify_guest_input_section(guest_input: Any) -> None:
    if not isinstance(guest_input, dict):
        raise AcceptanceError("VM acceptance report is missing 'guest_input' object.")
    if guest_input.get("seat") != "seat0":
        raise AcceptanceError(
            f"VM acceptance report requires guest_input.seat == 'seat0', got {guest_input.get('seat')!r}"
        )
    caps_before = _require_non_bool_int(
        guest_input.get("capabilities_before"),
        "guest_input.capabilities_before",
    )
    caps_after = _require_non_bool_int(
        guest_input.get("capabilities_after"),
        "guest_input.capabilities_after",
    )
    if (
        caps_before != REQUIRED_SEAT0_CAPABILITIES
        or caps_after != REQUIRED_SEAT0_CAPABILITIES
    ):
        raise AcceptanceError(
            f"VM acceptance report requires guest_input capabilities {REQUIRED_SEAT0_CAPABILITIES} "
            f"before and after, got before={caps_before}, after={caps_after}"
        )
    setup_keys = _require_non_bool_int(
        guest_input.get("setup_keypresses_sent"),
        "guest_input.setup_keypresses_sent",
    )
    if setup_keys != 0:
        raise AcceptanceError(
            f"VM acceptance report requires guest_input.setup_keypresses_sent == 0, got {setup_keys}"
        )
    if guest_input.get("helper_running_before_cleanup") is not True:
        raise AcceptanceError(
            "VM acceptance report requires guest_input.helper_running_before_cleanup == True."
        )
    if guest_input.get("identities_preserved") is not True:
        raise AcceptanceError(
            "VM acceptance report requires guest_input.identities_preserved == True."
        )

    pointer_devs = guest_input.get("pointer_devices")
    if not isinstance(pointer_devs, list) or not pointer_devs:
        raise AcceptanceError(
            "VM acceptance report requires non-empty guest_input.pointer_devices."
        )
    for dev in pointer_devs:
        if not isinstance(dev, dict) or not str(dev.get("identifier") or "").strip():
            raise AcceptanceError(
                f"Invalid pointer device entry in guest_input.pointer_devices: {dev!r}"
            )

    keyboard_devs = guest_input.get("keyboard_devices")
    if not isinstance(keyboard_devs, list) or not keyboard_devs:
        raise AcceptanceError(
            "VM acceptance report requires non-empty guest_input.keyboard_devices."
        )
    for dev in keyboard_devs:
        if (
            not isinstance(dev, dict)
            or not str(dev.get("identifier") or "").strip()
            or not str(dev.get("xkb_active_layout_name") or "").strip()
        ):
            raise AcceptanceError(
                f"Invalid keyboard device entry in guest_input.keyboard_devices: {dev!r}"
            )


def _verify_rendering_environment_section(rendering_env: Any) -> None:
    if not isinstance(rendering_env, dict):
        raise AcceptanceError(
            "VM acceptance report is missing 'rendering_environment' object."
        )
    if rendering_env.get("isolated_vm_only") is not True:
        raise AcceptanceError(
            "VM acceptance report requires rendering_environment.isolated_vm_only == True."
        )
    if rendering_env.get("wlr_renderer") != "pixman":
        raise AcceptanceError(
            "VM acceptance report requires rendering_environment.wlr_renderer == 'pixman'."
        )
    if rendering_env.get("webkit_disable_dmabuf_renderer") != "1":
        raise AcceptanceError(
            "VM acceptance report requires rendering_environment.webkit_disable_dmabuf_renderer == '1'."
        )
    if rendering_env.get("webkit_disable_compositing_mode") != "1":
        raise AcceptanceError(
            "VM acceptance report requires rendering_environment.webkit_disable_compositing_mode == '1'."
        )
    if rendering_env.get("libgl_always_software") != "1":
        raise AcceptanceError(
            "VM acceptance report requires rendering_environment.libgl_always_software == '1'."
        )


def _verify_main_paint_readiness_entry(
    entry: Any,
    *,
    label: str,
    expected_width: float,
    expected_height: float,
) -> None:
    if not isinstance(entry, dict):
        raise AcceptanceError(f"VM report {label} must be an object, got {entry!r}")
    if entry.get("painted") is not True:
        raise AcceptanceError(f"VM report {label}.painted must be True.")
    if entry.get("measurement_method") != "grim_pixel_diff_and_interior_contrast":
        raise AcceptanceError(
            f"VM report {label}.measurement_method must be "
            f"'grim_pixel_diff_and_interior_contrast', got {entry.get('measurement_method')!r}"
        )
    _verify_rect_dimensions(
        entry.get("native_rect"),
        label=f"{label}.native_rect",
        expected_width=expected_width,
        expected_height=expected_height,
        tolerance=0.0,
    )
    _verify_rect_dimensions(
        entry.get("logical_rect"),
        label=f"{label}.logical_rect",
        expected_width=expected_width,
        expected_height=expected_height,
        tolerance=MAIN_RENDERED_LOGICAL_TOLERANCE,
    )
    fill_ratio = _require_non_bool_number(entry.get("fill_ratio"), f"{label}.fill_ratio")
    if fill_ratio < MIN_MAIN_FILL_RATIO:
        raise AcceptanceError(
            f"VM report {label}.fill_ratio {fill_ratio} below minimum {MIN_MAIN_FILL_RATIO}"
        )
    stddev = _require_non_bool_number(
        entry.get("interior_max_channel_stddev"),
        f"{label}.interior_max_channel_stddev",
    )
    if stddev < MIN_MAIN_INTERIOR_CHANNEL_STDDEV:
        raise AcceptanceError(
            f"VM report {label}.interior_max_channel_stddev {stddev} below minimum "
            f"{MIN_MAIN_INTERIOR_CHANNEL_STDDEV}"
        )
    buckets = _require_non_bool_int(
        entry.get("interior_color_buckets"),
        f"{label}.interior_color_buckets",
    )
    if buckets < MIN_MAIN_INTERIOR_COLOR_BUCKETS:
        raise AcceptanceError(
            f"VM report {label}.interior_color_buckets {buckets} below minimum "
            f"{MIN_MAIN_INTERIOR_COLOR_BUCKETS}"
        )
    bands = entry.get("band_contrast_pixels")
    if not isinstance(bands, dict):
        raise AcceptanceError(f"VM report {label}.band_contrast_pixels must be an object.")
    for band_key in REQUIRED_MAIN_PAINT_BANDS:
        band_px = _require_non_bool_int(
            bands.get(band_key),
            f"{label}.band_contrast_pixels.{band_key}",
        )
        if band_px < MIN_MAIN_BAND_CONTRAST_PIXELS:
            raise AcceptanceError(
                f"VM report {label}.band_contrast_pixels.{band_key} ({band_px}) "
                f"below minimum {MIN_MAIN_BAND_CONTRAST_PIXELS}"
            )


def _verify_main_paint_readiness_section(paint_section: Any) -> None:
    if not isinstance(paint_section, dict):
        raise AcceptanceError(
            "VM acceptance report is missing 'main_window_paint_readiness' object."
        )
    _verify_main_paint_readiness_entry(
        paint_section.get("default_1040x760"),
        label="main_window_paint_readiness.default_1040x760",
        expected_width=1040.0,
        expected_height=760.0,
    )
    _verify_main_paint_readiness_entry(
        paint_section.get("resized_360x500"),
        label="main_window_paint_readiness.resized_360x500",
        expected_width=360.0,
        expected_height=500.0,
    )
    _verify_main_paint_readiness_entry(
        paint_section.get("restored_wide_1040x760"),
        label="main_window_paint_readiness.restored_wide_1040x760",
        expected_width=1040.0,
        expected_height=760.0,
    )


def _verify_scene_cleanup_section(scene_cleanup: Any) -> None:
    if not isinstance(scene_cleanup, dict):
        raise AcceptanceError(
            "VM acceptance report is missing 'scene_cleanup' object."
        )
    if scene_cleanup.get("ok") is not True:
        raise AcceptanceError(
            f"VM acceptance report requires scene_cleanup.ok == True, got {scene_cleanup!r}"
        )
    errors = scene_cleanup.get("errors")
    if not isinstance(errors, list) or errors:
        raise AcceptanceError(
            f"VM acceptance report requires empty scene_cleanup.errors, got {errors!r}"
        )
    owners = scene_cleanup.get("owners")
    if not isinstance(owners, dict):
        raise AcceptanceError(
            "VM acceptance report requires scene_cleanup.owners object."
        )
    for owner_key in REQUIRED_SCENE_CLEANUP_OWNERS:
        owner_entry = owners.get(owner_key)
        if not isinstance(owner_entry, dict):
            raise AcceptanceError(
                f"VM acceptance report missing scene_cleanup.owners.{owner_key}."
            )
        if owner_entry.get("present") is not True:
            raise AcceptanceError(
                f"VM acceptance report requires scene_cleanup.owners.{owner_key}.present == True."
            )
        if owner_entry.get("reaped") is not True or owner_entry.get("ok") is not True:
            raise AcceptanceError(
                f"VM acceptance report requires scene_cleanup.owners.{owner_key} reaped and ok, "
                f"got {owner_entry!r}"
            )
        if owner_entry.get("error") is not None:
            raise AcceptanceError(
                f"VM acceptance report requires scene_cleanup.owners.{owner_key}.error == None, "
                f"got {owner_entry.get('error')!r}"
            )


def verify_vm_acceptance_report(report_path: Path) -> dict[str, Any]:
    if not report_path.is_file():
        raise AcceptanceError(
            f"NixOS VM acceptance report missing at {report_path}; refusing silent success."
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise AcceptanceError(f"Invalid VM acceptance report JSON at {report_path}: {exc}") from exc

    if not isinstance(report, dict) or report.get("status") != "passed":
        raise AcceptanceError(f"NixOS VM acceptance did not pass: {report}")
    exit_code = report.get("desktop_exit_code")
    if isinstance(exit_code, bool) or exit_code != 0:
        raise AcceptanceError(
            f"Expected desktop_exit_code 0 in VM report, got {exit_code!r}"
        )
    checks = report.get("checks")
    if not isinstance(checks, dict):
        raise AcceptanceError("VM acceptance report is missing 'checks' object.")

    missing_or_failed = [key for key in REQUIRED_VM_CHECKS if checks.get(key) is not True]
    if missing_or_failed:
        raise AcceptanceError(
            f"VM acceptance report failed required check(s): {', '.join(missing_or_failed)}"
        )

    compositor = report.get("compositor_invariants")
    if not isinstance(compositor, dict):
        raise AcceptanceError("VM acceptance report is missing 'compositor_invariants' object.")
    if compositor.get("focus_follows_mouse") != "yes":
        raise AcceptanceError(
            "VM acceptance report requires compositor_invariants.focus_follows_mouse == 'yes'."
        )
    if compositor.get("broad_floating_rules_present") is not False:
        raise AcceptanceError(
            "VM acceptance report requires compositor_invariants.broad_floating_rules_present == False."
        )
    if compositor.get("tray_compositor_rules_present") is not False:
        raise AcceptanceError(
            "VM acceptance report requires compositor_invariants.tray_compositor_rules_present == False."
        )
    if compositor.get("cursor_positioning_command") != "set":
        raise AcceptanceError(
            "VM acceptance report requires compositor_invariants.cursor_positioning_command == 'set'."
        )
    if compositor.get("main_default_size_unforced") is not True:
        raise AcceptanceError(
            "VM acceptance report requires compositor_invariants.main_default_size_unforced == True."
        )

    _verify_rect_dimensions(
        report.get("initial_default_main_rect"),
        label="initial_default_main_rect",
        expected_width=1040.0,
        expected_height=760.0,
        tolerance=0.0,
    )
    _verify_rect_dimensions(
        report.get("explicit_resized_min_main_rect"),
        label="explicit_resized_min_main_rect",
        expected_width=360.0,
        expected_height=500.0,
        tolerance=0.0,
    )

    measurements = report.get("rendered_popup_measurements")
    if not isinstance(measurements, dict):
        raise AcceptanceError(
            "VM acceptance report is missing 'rendered_popup_measurements' object."
        )
    _verify_rendered_popup_measurement(
        measurements.get("scale_1"),
        label="rendered_popup_measurements.scale_1",
        expected_scale=1.0,
    )
    _verify_rendered_popup_measurement(
        measurements.get("scale_2"),
        label="rendered_popup_measurements.scale_2",
        expected_scale=2.0,
    )

    transit = report.get("pointer_transit")
    if not isinstance(transit, dict):
        raise AcceptanceError("VM acceptance report is missing 'pointer_transit' object.")
    if transit.get("clicked_during_transit") is not False:
        raise AcceptanceError(
            "VM acceptance report requires pointer_transit.clicked_during_transit == False."
        )
    waypoints_count = _require_non_bool_int(
        transit.get("waypoints_count"),
        "pointer_transit.waypoints_count",
    )
    if waypoints_count < MIN_POINTER_TRANSIT_WAYPOINTS:
        raise AcceptanceError(
            f"VM acceptance report requires >= {MIN_POINTER_TRANSIT_WAYPOINTS} pointer transit "
            f"waypoints, got {waypoints_count}"
        )
    dwell_ms = _require_non_bool_number(transit.get("dwell_ms"), "pointer_transit.dwell_ms")
    if (
        dwell_ms < MIN_POINTER_DWELL_MS
        or dwell_ms > MAX_REFRESH_SURVIVAL_DURATION_SECONDS * 1000.0
    ):
        raise AcceptanceError(
            f"VM acceptance report requires pointer_transit.dwell_ms in "
            f"[{MIN_POINTER_DWELL_MS}, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS * 1000.0}], "
            f"got {dwell_ms}"
        )
    if transit.get("visible_after_dwell") is not True:
        raise AcceptanceError(
            "VM acceptance report requires pointer_transit.visible_after_dwell == True."
        )

    escape_info = report.get("escape_dismissal")
    if not isinstance(escape_info, dict):
        raise AcceptanceError("VM acceptance report is missing 'escape_dismissal' object.")
    if escape_info.get("interior_click_before_escape") is not False:
        raise AcceptanceError(
            "VM acceptance report requires escape_dismissal.interior_click_before_escape == False."
        )
    if escape_info.get("dismissed") is not True:
        raise AcceptanceError("VM acceptance report requires escape_dismissal.dismissed == True.")

    _verify_refresh_paint_survival_section(
        report.get("refresh_paint_survival"),
        pointer_transit=transit,
    )
    _verify_guest_input_section(report.get("guest_input"))
    _verify_rendering_environment_section(report.get("rendering_environment"))
    _verify_main_paint_readiness_section(report.get("main_window_paint_readiness"))
    _verify_scene_cleanup_section(report.get("scene_cleanup"))

    return report


def _verify_refresh_paint_survival_section(
    survival: Any,
    *,
    pointer_transit: Mapping[str, Any],
) -> None:
    if not isinstance(survival, dict):
        raise AcceptanceError(
            "VM acceptance report is missing 'refresh_paint_survival' object."
        )

    ui_interval_ms = _require_non_bool_number(
        survival.get("ui_refresh_interval_ms"),
        "refresh_paint_survival.ui_refresh_interval_ms",
    )
    if abs(ui_interval_ms - UI_REFRESH_INTERVAL_MS) > 1e-6:
        raise AcceptanceError(
            "VM acceptance report requires "
            f"refresh_paint_survival.ui_refresh_interval_ms == {UI_REFRESH_INTERVAL_MS}, "
            f"got {ui_interval_ms}"
        )

    h2_dwell = survival.get("headless2_scale2_dwell")
    if not isinstance(h2_dwell, dict):
        raise AcceptanceError(
            "VM acceptance report is missing "
            "'refresh_paint_survival.headless2_scale2_dwell' object."
        )
    h2_dwell_sec = _require_non_bool_number(
        h2_dwell.get("dwell_seconds"),
        "refresh_paint_survival.headless2_scale2_dwell.dwell_seconds",
    )
    if (
        h2_dwell_sec < MIN_H2_REFRESH_DWELL_SECONDS
        or h2_dwell_sec > MAX_REFRESH_SURVIVAL_DURATION_SECONDS
    ):
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless2_scale2_dwell.dwell_seconds in "
            f"[{MIN_H2_REFRESH_DWELL_SECONDS}, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS}], "
            f"got {h2_dwell_sec}"
        )
    h2_dwell_ms = _require_non_bool_number(
        h2_dwell.get("dwell_ms"),
        "refresh_paint_survival.headless2_scale2_dwell.dwell_ms",
    )
    if (
        h2_dwell_ms < MIN_POINTER_DWELL_MS
        or h2_dwell_ms > MAX_REFRESH_SURVIVAL_DURATION_SECONDS * 1000.0
    ):
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless2_scale2_dwell.dwell_ms in "
            f"[{MIN_POINTER_DWELL_MS}, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS * 1000.0}], "
            f"got {h2_dwell_ms}"
        )
    if abs(h2_dwell_ms - (h2_dwell_sec * 1000.0)) > 50.0:
        raise AcceptanceError(
            "VM acceptance report "
            f"refresh_paint_survival.headless2_scale2_dwell.dwell_ms ({h2_dwell_ms}) "
            f"disagrees with dwell_seconds ({h2_dwell_sec})"
        )
    transit_dwell_ms = _require_non_bool_number(
        pointer_transit.get("dwell_ms"),
        "pointer_transit.dwell_ms",
    )
    if abs(h2_dwell_ms - transit_dwell_ms) > 50.0:
        raise AcceptanceError(
            "VM acceptance report "
            f"refresh_paint_survival.headless2_scale2_dwell.dwell_ms ({h2_dwell_ms}) "
            f"disagrees with pointer_transit.dwell_ms ({transit_dwell_ms})"
        )
    _verify_rendered_popup_measurement(
        h2_dwell.get("measurement"),
        label="refresh_paint_survival.headless2_scale2_dwell.measurement",
        expected_scale=2.0,
    )

    h1_reopen = survival.get("headless1_scale1_reopen")
    if not isinstance(h1_reopen, dict):
        raise AcceptanceError(
            "VM acceptance report is missing "
            "'refresh_paint_survival.headless1_scale1_reopen' object."
        )
    _verify_rendered_popup_measurement(
        h1_reopen.get("initial_measurement"),
        label="refresh_paint_survival.headless1_scale1_reopen.initial_measurement",
        expected_scale=1.0,
    )
    target_interval = _require_non_bool_number(
        h1_reopen.get("target_interval_seconds"),
        "refresh_paint_survival.headless1_scale1_reopen.target_interval_seconds",
    )
    if abs(target_interval - 1.0) > 1e-6:
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless1_scale1_reopen.target_interval_seconds == 1.0, "
            f"got {target_interval}"
        )

    sample_count = _require_non_bool_int(
        h1_reopen.get("sample_count"),
        "refresh_paint_survival.headless1_scale1_reopen.sample_count",
    )
    if sample_count != REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES:
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless1_scale1_reopen.sample_count == "
            f"{REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES}, got {sample_count}"
        )

    samples = h1_reopen.get("samples")
    if not isinstance(samples, list):
        raise AcceptanceError(
            "VM acceptance report requires list at "
            "refresh_paint_survival.headless1_scale1_reopen.samples."
        )
    if len(samples) != REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES:
        raise AcceptanceError(
            "VM acceptance report requires exactly "
            f"{REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES} bounded samples in "
            f"refresh_paint_survival.headless1_scale1_reopen.samples, got {len(samples)}"
        )

    sampling_window = _require_non_bool_number(
        h1_reopen.get("sampling_window_seconds"),
        "refresh_paint_survival.headless1_scale1_reopen.sampling_window_seconds",
    )
    if (
        sampling_window < MIN_H1_REFRESH_SAMPLING_WINDOW_SECONDS
        or sampling_window > MAX_REFRESH_SURVIVAL_DURATION_SECONDS
    ):
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless1_scale1_reopen.sampling_window_seconds in "
            f"[{MIN_H1_REFRESH_SAMPLING_WINDOW_SECONDS}, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS}], "
            f"got {sampling_window}"
        )

    total_duration = _require_non_bool_number(
        h1_reopen.get("total_duration_seconds"),
        "refresh_paint_survival.headless1_scale1_reopen.total_duration_seconds",
    )
    if (
        total_duration < MIN_H1_REFRESH_TOTAL_DURATION_SECONDS
        or total_duration > MAX_REFRESH_SURVIVAL_DURATION_SECONDS
    ):
        raise AcceptanceError(
            "VM acceptance report requires "
            "refresh_paint_survival.headless1_scale1_reopen.total_duration_seconds in "
            f"[{MIN_H1_REFRESH_TOTAL_DURATION_SECONDS}, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS}], "
            f"got {total_duration}"
        )

    prev_elapsed = 0.0
    elapsed_values: list[float] = []
    seen_screenshots: set[str] = set()
    initial_shot = h1_reopen["initial_measurement"].get("screenshot")
    if isinstance(initial_shot, str) and initial_shot.strip():
        seen_screenshots.add(initial_shot.strip())

    for idx, sample in enumerate(samples):
        sample_label = f"refresh_paint_survival.headless1_scale1_reopen.samples[{idx}]"
        if not isinstance(sample, dict):
            raise AcceptanceError(f"VM report {sample_label} must be an object, got {sample!r}")
        sample_index = _require_non_bool_int(
            sample.get("sample_index"),
            f"{sample_label}.sample_index",
        )
        if sample_index != idx + 1:
            raise AcceptanceError(
                f"VM report {sample_label}.sample_index must be {idx + 1}, got {sample_index}"
            )
        elapsed = _require_non_bool_number(
            sample.get("elapsed_seconds"),
            f"{sample_label}.elapsed_seconds",
        )
        interval = _require_non_bool_number(
            sample.get("interval_seconds"),
            f"{sample_label}.interval_seconds",
        )
        if elapsed <= 0.0 or elapsed > MAX_REFRESH_SURVIVAL_DURATION_SECONDS:
            raise AcceptanceError(
                f"VM report {sample_label}.elapsed_seconds must be in "
                f"(0, {MAX_REFRESH_SURVIVAL_DURATION_SECONDS}], got {elapsed}"
            )
        delta = elapsed - prev_elapsed
        if delta <= 0.0:
            raise AcceptanceError(
                f"VM report {sample_label}.elapsed_seconds ({elapsed}) must strictly increase "
                f"beyond previous timestamp ({prev_elapsed})"
            )
        if (
            delta < MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS
            or delta > MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS
        ):
            raise AcceptanceError(
                f"VM report {sample_label} monotonic step delta ({delta:.4f}s) outside "
                f"[{MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS}, {MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS}]"
            )
        if (
            interval < MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS
            or interval > MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS
        ):
            raise AcceptanceError(
                f"VM report {sample_label}.interval_seconds ({interval}) outside "
                f"[{MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS}, {MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS}]"
            )
        if abs(interval - delta) > 0.05:
            raise AcceptanceError(
                f"VM report {sample_label}.interval_seconds ({interval}) disagrees with "
                f"monotonic timestamp delta ({delta:.4f})"
            )
        _verify_rendered_popup_measurement(
            sample,
            label=sample_label,
            expected_scale=1.0,
        )
        shot_name = sample.get("screenshot")
        if not isinstance(shot_name, str) or not shot_name.strip():
            raise AcceptanceError(
                f"VM report {sample_label}.screenshot must be a non-empty filename."
            )
        clean_shot = shot_name.strip()
        if clean_shot in seen_screenshots:
            raise AcceptanceError(
                f"VM report {sample_label}.screenshot reuses duplicate capture {clean_shot!r}"
            )
        seen_screenshots.add(clean_shot)
        prev_elapsed = elapsed
        elapsed_values.append(elapsed)

    actual_window = elapsed_values[-1] - elapsed_values[0]
    if actual_window < MIN_H1_REFRESH_SAMPLING_WINDOW_SECONDS:
        raise AcceptanceError(
            f"VM report refresh_paint_survival monotonic sampling window ({actual_window:.4f}s) "
            f"below minimum {MIN_H1_REFRESH_SAMPLING_WINDOW_SECONDS}s"
        )
    if abs(sampling_window - actual_window) > 0.05:
        raise AcceptanceError(
            f"VM report sampling_window_seconds ({sampling_window}) disagrees with "
            f"sample timestamp span ({actual_window:.4f})"
        )
    if elapsed_values[-1] < MIN_H1_REFRESH_TOTAL_DURATION_SECONDS:
        raise AcceptanceError(
            f"VM report final sample timestamp ({elapsed_values[-1]:.4f}s) below minimum "
            f"total duration {MIN_H1_REFRESH_TOTAL_DURATION_SECONDS}s"
        )
    if total_duration + 0.05 < elapsed_values[-1] or abs(total_duration - elapsed_values[-1]) > 2.0:
        raise AcceptanceError(
            f"VM report total_duration_seconds ({total_duration}) disagrees with "
            f"final sample elapsed_seconds ({elapsed_values[-1]:.4f})"
        )


def run_logged_child(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> subprocess.CompletedProcess[str]:
    assert_command_allowed(argv)
    proc = subprocess.run(
        list(argv),
        cwd=str(cwd),
        env=dict(env),
        text=True,
        capture_output=True,
        check=False,
    )
    combined = (
        f"$ {' '.join(argv)}\n"
        f"exit_code={proc.returncode}\n"
        f"--- stdout ---\n{redact_sensitive_text(proc.stdout)}\n"
        f"--- stderr ---\n{redact_sensitive_text(proc.stderr)}\n"
    )
    write_private_text(log_path, combined)
    return proc


def resolve_or_build_playwright_launcher(
    repo_root: Path,
    *,
    explicit_launcher: Path | None,
    child_env: Mapping[str, str],
    logs_dir: Path,
) -> Path:
    if explicit_launcher is not None:
        checked = validate_safe_absolute_path(explicit_launcher, "playwright_launcher")
        ensure_no_symlink_ancestors(checked, "playwright_launcher")
        if not os.access(checked, os.X_OK):
            raise AcceptanceError(f"Playwright launcher is not executable: {checked}")
        return checked

    nix_bin = shutil.which("nix")
    if not nix_bin:
        raise AcceptanceError(
            "nix executable is required to build reproducible threadlane-desktop-playwright."
        )
    nix_path = validate_safe_absolute_path(nix_bin, "nix")
    proc = run_logged_child(
        [
            str(nix_path),
            "build",
            "--no-link",
            "--print-out-paths",
            f"{repo_root}#threadlane-desktop-playwright",
        ],
        cwd=repo_root,
        env=child_env,
        log_path=logs_dir / "nix-build-playwright.log",
    )
    if proc.returncode != 0:
        detail = redact_sensitive_text(proc.stderr.strip() or proc.stdout.strip())
        raise AcceptanceError(
            f"Failed to build reproducible threadlane-desktop-playwright package: {detail}"
        )
    out_lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not out_lines:
        raise AcceptanceError("nix build returned no output path for threadlane-desktop-playwright.")
    launcher = Path(out_lines[-1]) / "bin" / "threadlane-desktop-playwright"
    if not os.access(launcher, os.X_OK):
        raise AcceptanceError(f"Built Playwright launcher binary missing at {launcher}")
    return launcher


def execute_browser_stage(
    repo_root: Path,
    run_dir: Path,
    layout: ChildIsolationLayout,
    *,
    explicit_launcher: Path | None,
) -> PlaywrightSuiteCounts:
    browser_dir = ensure_private_dir(run_dir / "browser")
    logs_dir = ensure_private_dir(run_dir / "logs")
    port = pick_ephemeral_loopback_port()

    child_env = build_isolated_child_env(
        layout,
        extra_env={
            "THREADLANE_ACCEPTANCE_ARTIFACT_DIR": str(browser_dir),
            "THREADLANE_ACCEPTANCE_REPO_ROOT": str(repo_root),
            "THREADLANE_DESKTOP_SOURCE_DIR": str(repo_root / "apps" / "desktop"),
            "THREADLANE_PLAYWRIGHT_PORT": str(port),
        },
    )

    launcher = resolve_or_build_playwright_launcher(
        repo_root,
        explicit_launcher=explicit_launcher,
        child_env=child_env,
        logs_dir=logs_dir,
    )
    wrapped_argv = wrap_with_private_dbus([str(launcher)])
    proc = run_logged_child(
        wrapped_argv,
        cwd=browser_dir,
        env=child_env,
        log_path=logs_dir / "browser-playwright.log",
    )
    report_path = browser_dir / "playwright-results.json"
    if proc.returncode != 0:
        detail = redact_sensitive_text(proc.stderr.strip() or proc.stdout.strip())
        raise AcceptanceError(
            f"Playwright browser acceptance exited with code {proc.returncode}: {detail}"
        )
    return verify_playwright_json_report(report_path)


def resolve_vm_store_artifacts_dir(store_out: Path) -> Path:
    checked_store = validate_safe_absolute_path(store_out, "vm_store_output")
    ensure_no_symlink_ancestors(checked_store, "vm_store_output")
    if not checked_store.is_dir():
        raise AcceptanceError(f"VM Nix store output directory does not exist: {checked_store}")

    candidates = (
        checked_store / "vm-artifacts",
        checked_store / "vm-artifacts" / "vm-acceptance-artifacts",
        checked_store / "vm-acceptance-artifacts",
    )
    for candidate in candidates:
        try:
            st = os.lstat(candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise AcceptanceError(
                f"Unable to inspect VM artifacts candidate {candidate}: {exc}"
            ) from exc
        if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode):
            raise AcceptanceError(
                f"Refusing non-regular VM artifacts directory candidate: {candidate}"
            )
        report_candidate = candidate / "vm-acceptance-report.json"
        try:
            rep_st = os.lstat(report_candidate)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise AcceptanceError(
                f"Unable to inspect VM acceptance report candidate {report_candidate}: {exc}"
            ) from exc
        if stat.S_ISLNK(rep_st.st_mode) or not stat.S_ISREG(rep_st.st_mode):
            raise AcceptanceError(
                f"Refusing non-regular VM acceptance report file: {report_candidate}"
            )
        return candidate

    checked_list = ", ".join(str(c) for c in candidates)
    raise AcceptanceError(
        f"Expected VM artifacts directory containing vm-acceptance-report.json in {checked_store} "
        f"(checked: {checked_list})"
    )


def copy_store_artifacts_privately(src_dir: Path, dest_dir: Path) -> None:
    if not src_dir.is_dir():
        raise AcceptanceError(f"Expected VM artifacts directory in Nix output: {src_dir}")
    ensure_private_dir(dest_dir)
    for item in sorted(src_dir.iterdir()):
        if item.is_symlink():
            continue
        target = dest_dir / item.name
        if item.is_dir():
            copy_store_artifacts_privately(item, target)
        elif item.is_file():
            shutil.copyfile(item, target)
            os.chmod(target, 0o600)


def execute_vm_stage(
    repo_root: Path,
    run_dir: Path,
    layout: ChildIsolationLayout,
) -> dict[str, Any]:
    if sys.platform != "linux":
        raise AcceptanceError(
            f"NixOS Wayland/tray VM acceptance requires Linux; current platform is {sys.platform}."
        )
    nix_bin = shutil.which("nix")
    if not nix_bin:
        raise AcceptanceError("nix executable is required to run desktop-acceptance-vm.")
    nix_path = validate_safe_absolute_path(nix_bin, "nix")

    vm_dir = ensure_private_dir(run_dir / "vm")
    logs_dir = ensure_private_dir(run_dir / "logs")
    child_env = build_isolated_child_env(layout)
    wrapped_argv = wrap_with_private_dbus(
        [
            str(nix_path),
            "build",
            "--no-link",
            "--print-out-paths",
            "--rebuild",
            f"{repo_root}#desktop-acceptance-vm",
        ]
    )
    proc = run_logged_child(
        wrapped_argv,
        cwd=vm_dir,
        env=child_env,
        log_path=logs_dir / "vm-nixos-acceptance.log",
    )
    if proc.returncode != 0:
        first_try_combined = f"{proc.stdout}\n{proc.stderr}".lower()
        needs_initial_build = (
            "not built" in first_try_combined
            or "does not exist" in first_try_combined
            or "cannot rebuild" in first_try_combined
            or "is not valid" in first_try_combined
        )
        if needs_initial_build:
            wrapped_initial = wrap_with_private_dbus(
                [
                    str(nix_path),
                    "build",
                    "--no-link",
                    "--print-out-paths",
                    f"{repo_root}#desktop-acceptance-vm",
                ]
            )
            proc = run_logged_child(
                wrapped_initial,
                cwd=vm_dir,
                env=child_env,
                log_path=logs_dir / "vm-nixos-acceptance.log",
            )
    if proc.returncode != 0:
        detail = redact_sensitive_text(proc.stderr.strip() or proc.stdout.strip())
        raise AcceptanceError(
            f"NixOS Wayland/tray VM acceptance failed with code {proc.returncode}: {detail}"
        )

    out_lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    if not out_lines:
        raise AcceptanceError("nix build returned no store output path for desktop-acceptance-vm.")
    store_out = Path(out_lines[-1])
    artifacts_src = resolve_vm_store_artifacts_dir(store_out)
    copy_store_artifacts_privately(artifacts_src, vm_dir)
    return verify_vm_acceptance_report(vm_dir / "vm-acceptance-report.json")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run isolated Threadlane desktop acceptance checks (Playwright browser suite "
            "and/or NixOS Wayland + StatusNotifier tray VM suite)."
        )
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Absolute path to the Threadlane checkout root.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help=(
            "Absolute path for logs, traces, screenshots, and VM outputs. "
            "Defaults to <repo>/.local/desktop-acceptance (requires pre-existing git-ignored .local)."
        ),
    )
    parser.add_argument(
        "--stage",
        choices=("all", "browser", "vm"),
        default="all",
        help="Acceptance stage to run. Full desktop acceptance requires 'all' (the default).",
    )
    parser.add_argument(
        "--require-full-acceptance",
        action="store_true",
        help="Fail closed unless both browser and native NixOS VM stages run and pass.",
    )
    parser.add_argument(
        "--playwright-launcher",
        default=None,
        help="Optional absolute path to a prebuilt threadlane-desktop-playwright executable.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate paths, git-ignore gates, and child isolation layout without starting processes.",
    )
    parser.add_argument(
        "--prepare-playwright-stage",
        action="store_true",
        help=(
            "Validate THREADLANE_ACCEPTANCE_ARTIFACT_DIR, optional THREADLANE_ACCEPTANCE_STAGING_DIR, "
            "and THREADLANE_DESKTOP_SOURCE_DIR, then create and print a fresh 0700 staging child directory."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    parent_env_snapshot = dict(os.environ)
    try:
        args = parse_args(argv)
        if args.prepare_playwright_stage:
            if args.dry_run or args.require_full_acceptance:
                raise AcceptanceError(
                    "--prepare-playwright-stage cannot be combined with --dry-run or --require-full-acceptance."
                )
            stage_repo_root = (
                args.repo_root
                if args.repo_root is not None
                else os.environ.get("THREADLANE_ACCEPTANCE_REPO_ROOT")
            )
            stage_artifact_dir = (
                args.artifact_dir
                if args.artifact_dir is not None
                else os.environ.get("THREADLANE_ACCEPTANCE_ARTIFACT_DIR")
            )
            stage_dir = prepare_playwright_staging_dir(
                artifact_dir=stage_artifact_dir,
                staging_dir=os.environ.get("THREADLANE_ACCEPTANCE_STAGING_DIR"),
                repo_root=stage_repo_root,
                desktop_source_dir=os.environ.get("THREADLANE_DESKTOP_SOURCE_DIR"),
            )
            print(str(stage_dir))
            return 0

        if args.require_full_acceptance and args.stage != "all":
            raise AcceptanceError(
                f"--require-full-acceptance requires --stage all; refused partial stage '{args.stage}'."
            )

        raw_repo_root = (
            args.repo_root
            if args.repo_root is not None
            else str(Path(__file__).resolve().parent.parent)
        )
        repo_root = validate_repo_root(Path(raw_repo_root))
        explicit_artifact = Path(args.artifact_dir) if args.artifact_dir is not None else None
        validated_target = validate_artifact_directory(repo_root, explicit_artifact)
        explicit_launcher = (
            validate_safe_absolute_path(args.playwright_launcher, "playwright_launcher")
            if args.playwright_launcher is not None
            else None
        )
        sample_desktop_cmd = build_desktop_launch_argv(
            Path("/run/current-system/sw/bin/threadlane-desktop")
        )

        if args.dry_run:
            plan = {
                "mode": "dry_run",
                "stage": args.stage,
                "full_desktop_acceptance_targeted": args.stage == "all",
                "repo_root": str(repo_root),
                "artifact_dir": str(validated_target.artifact_dir),
                "artifact_inside_repo": validated_target.inside_repo,
                "desktop_launch_flags_enforced": sample_desktop_cmd[1:],
                "child_isolation": {
                    "private_home": True,
                    "private_xdg_dirs": True,
                    "private_dbus_session": True,
                    "parent_environment_mutated": dict(os.environ) != parent_env_snapshot,
                    "touches_host_codex": False,
                    "invokes_host_nrs": False,
                },
            }
            print(json.dumps(plan, indent=2))
            return 0

        pre_status = capture_repo_status_porcelain(repo_root)
        base_artifact_dir = ensure_directory_preserving_existing(
            validated_target.artifact_dir,
            "artifact_dir",
        )
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = ensure_private_dir(base_artifact_dir / "runs" / f"{timestamp}-{os.getpid()}")
        layout = prepare_child_isolation(run_dir)

        browser_counts: PlaywrightSuiteCounts | None = None
        vm_report: dict[str, Any] | None = None

        if args.stage in ("all", "browser"):
            browser_counts = execute_browser_stage(
                repo_root,
                run_dir,
                layout,
                explicit_launcher=explicit_launcher,
            )

        if args.stage in ("all", "vm"):
            vm_report = execute_vm_stage(repo_root, run_dir, layout)

        post_status = capture_repo_status_porcelain(repo_root)
        if post_status != pre_status:
            raise AcceptanceError(
                "Repository working tree changed during acceptance run; tracked/untracked checkout must stay clean."
            )

        if dict(os.environ) != parent_env_snapshot:
            raise AcceptanceError("Parent environment was unexpectedly mutated during acceptance run.")

        full_passed = (
            args.stage == "all"
            and browser_counts is not None
            and browser_counts.passed > 0
            and vm_report is not None
            and vm_report.get("status") == "passed"
        )
        summary = {
            "schema_version": 1,
            "stage": args.stage,
            "status": "passed" if full_passed else f"partial_{args.stage}_passed",
            "full_desktop_acceptance": full_passed,
            "run_dir": str(run_dir),
            "browser": (
                {
                    "executed": True,
                    "total": browser_counts.total,
                    "passed": browser_counts.passed,
                    "failed": browser_counts.failed,
                    "skipped": browser_counts.skipped,
                }
                if browser_counts is not None
                else {"executed": False}
            ),
            "vm": (
                {
                    "executed": True,
                    "status": vm_report.get("status"),
                    "checks": vm_report.get("checks"),
                    "compositor_invariants": vm_report.get("compositor_invariants"),
                    "guest_input": vm_report.get("guest_input"),
                    "rendering_environment": vm_report.get("rendering_environment"),
                    "main_window_paint_readiness": vm_report.get(
                        "main_window_paint_readiness"
                    ),
                    "rendered_popup_measurements": vm_report.get("rendered_popup_measurements"),
                    "pointer_transit": vm_report.get("pointer_transit"),
                    "refresh_paint_survival": vm_report.get("refresh_paint_survival"),
                }
                if vm_report is not None
                else {"executed": False}
            ),
        }
        write_private_text(run_dir / "acceptance-summary.json", json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))
        return 0
    except AcceptanceError as exc:
        sys.stderr.write(f"desktop-acceptance error: {redact_sensitive_text(str(exc))}\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
