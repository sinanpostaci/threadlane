#!/usr/bin/env python3
"""Reproducible local operator harness for the narrow Milestone 2C live AGY worker fixture.

Exercises an isolated paid-provider Antigravity (`agy`) worker fixture against a disposable Git
repository and local `threadlane` supervisor via the trusted CLI (`bind`, `status`, `call`).
Not a proof of native Codex chat integration, MCP conversation acceptance, or automatic patch
acceptance (`patch_accepted` is always `false`; max state is `ready_for_review`).
"""

from __future__ import annotations

import argparse, contextlib, json, os, signal, stat, subprocess, sys, time, uuid
from pathlib import Path
from typing import Any, Iterator, Mapping, NamedTuple, NoReturn

PROVIDER_ID, PROFILE_ID, FILE_PERMISSIONS_MODE = "antigravity", "agy_live_pilot", "task_files"
TRACKED_REL_PATH, INITIAL_GREETING_CONTENT, EXPECTED_GREETING_CONTENT = "greeting.txt", "Hello\n", "Hello from Threadlane\n"
BOUNDED_TASK_PROMPT = (
    "Use file tools only to read greeting.txt in the current working directory and "
    "update greeting.txt so its exact contents are:\nHello from Threadlane\n"
    "(a single line reading 'Hello from Threadlane' followed by a single trailing newline). "
    "Do not use shell commands, MCP tools, child agents, or modify any other files."
)
MIN_TIMEOUT_SECONDS, MAX_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_SECONDS = 1, 3600, 300
DEFAULT_MAX_LINE_BYTES, DEFAULT_MAX_STDOUT_BYTES, DEFAULT_MAX_STDERR_BYTES, DEFAULT_MAX_EVENTS = 262_144, 4_194_304, 262_144, 256
MAX_IDENTIFIER_BYTES, MAX_BOUNDED_READ_BYTES, MAX_CLI_OUTPUT_BYTES, MAX_LOG_SNIPPET_BYTES = 128, 65_536, 131_072, 16_384
SUPERVISOR_READY_TIMEOUT_SECONDS, CLI_CALL_TIMEOUT_SECONDS, GIT_CMD_TIMEOUT_SECONDS, POLL_INTERVAL_SECONDS = 10.0, 15.0, 10.0, 0.2

TERMINAL_WORKER_STATUSES = frozenset({"completed", "failed", "cancelled", "timed_out", "unknown"})
KNOWN_WORKER_STATUSES = TERMINAL_WORKER_STATUSES | frozenset({"queued", "claimed", "running", "cancel_requested"})
KNOWN_ACCEPTANCE_STATES = frozenset({"pending_review", "ready_for_review", "rejected_by_diff_gate", "execution_failed", "unknown"})
KNOWN_EVENT_KINDS = frozenset({"system_init", "assistant_message", "tool_use", "tool_result", "blocked", "error", "result", "other"})


class PilotError(RuntimeError):
    def __init__(self, error_class: str) -> None:
        super().__init__(error_class)
        self.error_class = error_class


class PilotConfig(NamedTuple):
    threadlane: Path
    provider_executable: Path
    model: str
    effort: str
    output: Path
    timeout_seconds: int
    allow_live: bool


def is_safe_identifier(v: Any, max_bytes: int = MAX_IDENTIFIER_BYTES) -> bool:
    return (
        isinstance(v, str) and bool(v) and v.strip() == v and len(v.encode("utf-8")) <= max_bytes
        and not v.startswith(("-", ".")) and all((ch.isascii() and ch.isalnum()) or ch in "._-" for ch in v)
    )

def safe_id_or_none(v: Any) -> str | None:
    return v if is_safe_identifier(v) else None

def safe_enum_or_none(v: Any, allowed: frozenset[str]) -> str | None:
    return v if isinstance(v, str) and v in allowed else None

def safe_hex40_or_none(v: Any) -> str | None:
    return v if isinstance(v, str) and len(v) == 40 and all(c in "0123456789abcdef" for c in v) else None

def safe_int_or_none(v: Any) -> int | None:
    return v if isinstance(v, int) and not isinstance(v, bool) else None

def validate_absolute_clean_path(raw: str, error_class: str = "invalid_path_argument") -> Path:
    if (
        not isinstance(raw, str) or not raw or raw.strip() != raw or any(ord(ch) < 32 or ord(ch) == 127 for ch in raw)
        or not raw.startswith("/") or raw == "/" or raw.endswith("/") or "//" in raw or any(seg in ("", ".", "..") for seg in raw.split("/")[1:])
    ):
        raise PilotError(error_class)
    return Path(raw)


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise PilotError("invalid_cli_arguments")


def parse_and_validate_args(argv: list[str] | None = None) -> PilotConfig:
    parser = _SafeArgumentParser(prog="live_worker_pilot.py", add_help=True)
    for flag in ("--threadlane", "--provider-executable", "--model", "--effort", "--output"):
        parser.add_argument(flag, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--allow-live", action="store_true")
    try:
        ns = parser.parse_args(argv)
    except SystemExit as exc:
        if exc.code in (0, None):
            raise
        raise PilotError("invalid_cli_arguments") from exc
    tl, prov, out = (validate_absolute_clean_path(p) for p in (ns.threadlane, ns.provider_executable, ns.output))
    if not is_safe_identifier(ns.model) or not is_safe_identifier(ns.effort):
        raise PilotError("invalid_identifier_argument")
    if not (MIN_TIMEOUT_SECONDS <= ns.timeout_seconds <= MAX_TIMEOUT_SECONDS):
        raise PilotError("invalid_timeout_argument")
    return PilotConfig(tl, prov, ns.model, ns.effort, out, ns.timeout_seconds, bool(ns.allow_live))


def build_execution_budgets(timeout_seconds: int) -> dict[str, int]:
    return {"timeout_ms": timeout_seconds * 1000, "max_line_bytes": DEFAULT_MAX_LINE_BYTES, "max_stdout_bytes": DEFAULT_MAX_STDOUT_BYTES, "max_stderr_bytes": DEFAULT_MAX_STDERR_BYTES, "max_events": DEFAULT_MAX_EVENTS}

def build_layout_paths(output: Path) -> dict[str, str]:
    return {"output_root": str(output), "source_repo": str(output / "source_repo"), "work_area": str(output / "work_area"), "state_dir": str(output / "state"), "workers_config": str(output / "workers_config.json"), "logs_dir": str(output / "logs"), "summary_path": str(output / "pilot_summary.json")}

def build_dry_run_plan(config: PilotConfig) -> dict[str, Any]:
    layout = build_layout_paths(config.output)
    return {
        "mode": "dry_run", "live_execution": False, "paid_provider_fixture": True, "native_chat_acceptance_proof": False, "patch_accepted": False,
        "provider": PROVIDER_ID, "threadlane_executable": str(config.threadlane), "provider_executable": str(config.provider_executable),
        "requested_model": config.model, "requested_effort": config.effort, "timeout_seconds": config.timeout_seconds, "output_root": str(config.output),
        "planned_layout": {k: v for k, v in layout.items() if k != "output_root"},
        "profile_plan": {
            "profile_id": PROFILE_ID, "max_parallel": 1, "provider": PROVIDER_ID, "model": config.model, "effort": config.effort,
            "allowed_models": [config.model], "standalone": False, "file_permissions": FILE_PERMISSIONS_MODE,
            "allowed_write_paths": [TRACKED_REL_PATH], "budgets": build_execution_budgets(config.timeout_seconds),
        },
        "fixture_plan": {"tracked_file": TRACKED_REL_PATH, "initial_content": INITIAL_GREETING_CONTENT, "expected_content": EXPECTED_GREETING_CONTENT, "prompt": BOUNDED_TASK_PROMPT},
    }

def build_workers_config_payload(binding_id: str, config: PilotConfig, source_repo: Path, base_commit: str, work_area: Path) -> dict[str, Any]:
    return {
        "max_parallel": 1,
        "profiles": [{
            "profile_id": PROFILE_ID, "allowed_binding_ids": [binding_id],
            "spec": {
                "provider": PROVIDER_ID, "executable": str(config.provider_executable), "model": config.model, "effort": config.effort,
                "allowed_models": [config.model], "standalone": False, "file_permissions": FILE_PERMISSIONS_MODE,
                "repository_path": str(source_repo), "base_commit": base_commit,
                "work_area_root": str(work_area), "allowed_write_paths": [TRACKED_REL_PATH], "budgets": build_execution_budgets(config.timeout_seconds),
            },
        }],
    }


def validate_no_symlink_ancestors(path: Path) -> None:
    current, parts = Path("/"), path.parts[1:]
    for idx, part in enumerate(parts):
        current = current / part
        with contextlib.suppress(FileNotFoundError):
            st = os.lstat(current)
            if stat.S_ISLNK(st.st_mode) or (idx < len(parts) - 1 and not stat.S_ISDIR(st.st_mode)):
                raise PilotError("symlink_or_invalid_ancestor_refused")

def validate_executable_path(path: Path) -> None:
    validate_no_symlink_ancestors(path)
    try:
        st = os.lstat(path)
    except FileNotFoundError as exc:
        raise PilotError("executable_not_found") from exc
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode) or (stat.S_IMODE(st.st_mode) & 0o111) == 0:
        raise PilotError("invalid_executable")

def create_private_dir(path: Path, *, require_existing_parent: bool = False) -> Path:
    if os.path.lexists(path):
        raise PilotError("preexisting_output_refused")
    validate_no_symlink_ancestors(path)
    if require_existing_parent:
        try:
            pst = os.lstat(path.parent)
        except FileNotFoundError as exc:
            raise PilotError("output_parent_missing") from exc
        if stat.S_ISLNK(pst.st_mode) or not stat.S_ISDIR(pst.st_mode):
            raise PilotError("invalid_output_parent")
    try:
        os.mkdir(path, 0o700)
    except FileExistsError as exc:
        raise PilotError("preexisting_output_refused") from exc
    os.chmod(path, 0o700)
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise PilotError("private_directory_verification_failed")
    return path

def write_private_file(path: Path, content: bytes, *, allow_overwrite: bool = False) -> None:
    validate_no_symlink_ancestors(path)
    flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | (os.O_TRUNC if allow_overwrite else os.O_EXCL)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        view = memoryview(content)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise PilotError("private_file_write_failed")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)

def open_private_log(path: Path):
    validate_no_symlink_ancestors(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0), 0o600)
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")

def read_bounded_regular_file(path: Path, max_bytes: int = MAX_BOUNDED_READ_BYTES) -> str:
    validate_no_symlink_ancestors(path)
    st = os.lstat(path)
    if stat.S_ISLNK(st.st_mode) or not stat.S_ISREG(st.st_mode) or st.st_size > max_bytes:
        raise PilotError("bounded_file_read_failed")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        raw = os.read(fd, max_bytes + 1)
    finally:
        os.close(fd)
    if len(raw) > max_bytes:
        raise PilotError("bounded_file_read_failed")
    return raw.decode("utf-8")


def build_sanitized_child_env(env: Mapping[str, str] | None = None) -> dict[str, str]:
    merged = os.environ if env is None else {**os.environ, **env}
    sanitized = {k: str(v) for k, v in merged.items() if not k.upper().startswith("GIT_")}
    sanitized.update({
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_TERMINAL_PROMPT": "0",
    })
    return sanitized


def run_command_logged(
    argv: list[str], *, log_path: Path, cwd: Path | None = None, env: Mapping[str, str] | None = None,
    timeout: float = CLI_CALL_TIMEOUT_SECONDS, error_class: str = "subprocess_failed",
) -> str:
    child_env = build_sanitized_child_env(env)
    try:
        proc = subprocess.run(argv, cwd=str(cwd) if cwd else None, env=child_env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout, check=False)
    except (subprocess.TimeoutExpired, OSError) as exc:
        with open_private_log(log_path) as log_f:
            log_f.write(f"[{error_class}] subprocess exception: {type(exc).__name__}\n")
        raise PilotError(error_class) from exc
    if len(proc.stdout.encode("utf-8")) > MAX_CLI_OUTPUT_BYTES:
        raise PilotError("subprocess_output_overflow")
    if proc.returncode != 0:
        with open_private_log(log_path) as log_f:
            log_f.write(f"[{error_class}] exit={proc.returncode}\nstderr:\n{(proc.stderr or '')[:MAX_LOG_SNIPPET_BYTES]}\nstdout:\n{(proc.stdout or '')[:MAX_LOG_SNIPPET_BYTES]}\n")
        raise PilotError(error_class)
    return proc.stdout

def run_threadlane_cli(threadlane_bin: Path, args: list[str], log_path: Path) -> dict[str, Any]:
    stdout = run_command_logged([str(threadlane_bin), *args], log_path=log_path, error_class=f"threadlane_{args[0]}_failed")
    try:
        parsed = json.loads(stdout)
    except ValueError as exc:
        raise PilotError(f"threadlane_{args[0]}_invalid_json") from exc
    if not isinstance(parsed, dict):
        raise PilotError(f"threadlane_{args[0]}_invalid_json")
    return parsed

def call_threadlane_when_ready(proc: subprocess.Popen[Any], threadlane_bin: Path, args: list[str], log_path: Path) -> dict[str, Any]:
    deadline = time.monotonic() + SUPERVISOR_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise PilotError("supervisor_exited_early")
        with contextlib.suppress(PilotError):
            return run_threadlane_cli(threadlane_bin, args, log_path)
        time.sleep(0.05)
    raise PilotError("supervisor_ready_timeout")

@contextlib.contextmanager
def supervised_threadlane(threadlane_bin: Path, args: list[str], logs_dir: Path, prefix: str, cwd: Path) -> Iterator[subprocess.Popen[Any]]:
    child_env = build_sanitized_child_env()
    with open_private_log(logs_dir / f"{prefix}.stdout.log") as out_f, open_private_log(logs_dir / f"{prefix}.stderr.log") as err_f:
        proc = subprocess.Popen([str(threadlane_bin), *args], cwd=str(cwd), env=child_env, stdin=subprocess.DEVNULL, stdout=out_f, stderr=err_f, text=True)
        try:
            yield proc
        finally:
            if proc.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):
                        proc.kill()
                    proc.wait(timeout=3.0)
            else:
                proc.wait(timeout=1.0)


def run_git_isolated(repo_dir: Path, args: list[str], log_path: Path) -> str:
    return run_command_logged(["git", *args], log_path=log_path, cwd=repo_dir, timeout=GIT_CMD_TIMEOUT_SECONDS, error_class="git_command_failed")

def init_disposable_git_fixture(source_repo: Path, log_path: Path) -> str:
    run_git_isolated(source_repo, ["init", "-q"], log_path)
    write_private_file(source_repo / TRACKED_REL_PATH, INITIAL_GREETING_CONTENT.encode("utf-8"))
    run_git_isolated(source_repo, ["add", "--", TRACKED_REL_PATH], log_path)
    run_git_isolated(source_repo, ["-c", "user.name=Threadlane Fixture", "-c", "user.email=fixture@threadlane.local", "-c", "commit.gpgsign=false", "commit", "-q", "-m", "Initial fixture commit"], log_path)
    head_sha = safe_hex40_or_none(run_git_isolated(source_repo, ["rev-parse", "--verify", "HEAD"], log_path).strip().lower())
    if head_sha is None or run_git_isolated(source_repo, ["status", "--porcelain", "--untracked-files=all"], log_path).strip():
        raise PilotError("git_fixture_invalid")
    return head_sha

def verify_source_checkout_unchanged(source_repo: Path, expected_base_commit: str, log_path: Path) -> dict[str, Any]:
    try:
        head_after = safe_hex40_or_none(run_git_isolated(source_repo, ["rev-parse", "--verify", "HEAD"], log_path).strip().lower())
        status_clean = run_git_isolated(source_repo, ["status", "--porcelain", "--untracked-files=all"], log_path).strip() == ""
        greeting_unchanged = read_bounded_regular_file(source_repo / TRACKED_REL_PATH) == INITIAL_GREETING_CONTENT
    except Exception:
        return {"preserved": False, "head_matches_base": False, "status_clean": False, "greeting_unchanged": False, "head_commit": None}
    head_matches = head_after == expected_base_commit
    return {"preserved": bool(head_matches and status_clean and greeting_unchanged), "head_matches_base": head_matches, "status_clean": status_clean, "greeting_unchanged": greeting_unchanged, "head_commit": head_after}

def inspect_owned_task_worktree(work_area: Path, task_worktree_path_raw: Any) -> dict[str, Any]:
    if not isinstance(task_worktree_path_raw, str) or not task_worktree_path_raw:
        return {"task_worktree_path": None, "expected_edit_verified": False, "observed_greeting_bytes": None}
    try:
        task_path = validate_absolute_clean_path(task_worktree_path_raw)
        validate_no_symlink_ancestors(task_path)
        if not str(task_path).startswith(str(work_area) + "/"):
            raise PilotError("unowned_task_worktree")
        content = read_bounded_regular_file(task_path / TRACKED_REL_PATH)
    except Exception:
        return {"task_worktree_path": None, "expected_edit_verified": False, "observed_greeting_bytes": None}
    return {"task_worktree_path": str(task_path), "expected_edit_verified": content == EXPECTED_GREETING_CONTENT, "observed_greeting_bytes": len(content.encode("utf-8"))}


def evaluate_pilot_outcome(
    config: PilotConfig, base_commit: str, snapshot: dict[str, Any] | None,
    poll_timed_out: bool, source_check: dict[str, Any], task_check: dict[str, Any],
) -> tuple[bool, dict[str, Any]]:
    snap = snapshot if isinstance(snapshot, dict) else {}
    lifecycle_status = safe_enum_or_none(snap.get("status"), KNOWN_WORKER_STATUSES)
    observed_model, observed_effort = safe_id_or_none(snap.get("observed_model")), safe_id_or_none(snap.get("observed_effort"))
    progress = snap.get("progress") if isinstance(snap.get("progress"), dict) else {}
    outcome = snap.get("outcome") if isinstance(snap.get("outcome"), dict) else {}
    worktree_info = snap.get("worktree") if isinstance(snap.get("worktree"), dict) else {}
    acceptance_state = safe_enum_or_none(outcome.get("acceptance_state"), KNOWN_ACCEPTANCE_STATES)
    missing_model_evidence = bool(outcome.get("missing_model_evidence", observed_model is None))
    missing_effort_evidence = bool(outcome.get("missing_effort_evidence", observed_effort is None))
    rejected_by_diff_gate = bool(outcome.get("rejected_by_diff_gate", False))
    violation_count, changed_paths_count = safe_int_or_none(outcome.get("violation_count")) or 0, safe_int_or_none(outcome.get("changed_paths_count")) or 0
    expected_path_only = outcome.get("changed_paths_preview") == [TRACKED_REL_PATH]
    timed_out_flag = bool(outcome.get("timed_out", False)) or lifecycle_status == "timed_out"
    cancelled_flag = bool(outcome.get("cancelled", False)) or lifecycle_status == "cancelled"
    provider_error_flag, incomplete_cleanup_flag = bool(outcome.get("provider_error", False)), bool(outcome.get("incomplete_cleanup", False))
    diff_gate_passed = bool(outcome) and not rejected_by_diff_gate and violation_count == 0 and changed_paths_count == 1 and expected_path_only
    model_matched, effort_matched = (observed_model == config.model and not missing_model_evidence), (observed_effort == config.effort and not missing_effort_evidence)
    passed = (
        snapshot is not None and not poll_timed_out and lifecycle_status == "completed" and acceptance_state == "ready_for_review"
        and bool(task_check.get("expected_edit_verified")) and bool(source_check.get("preserved")) and model_matched and effort_matched
        and diff_gate_passed and not (timed_out_flag or cancelled_flag or provider_error_flag or incomplete_cleanup_flag)
    )
    if snapshot is None:
        classification = "timed_out" if poll_timed_out else "execution_error"
    elif poll_timed_out or timed_out_flag:
        classification = "timed_out"
    elif lifecycle_status == "unknown" or acceptance_state == "unknown":
        classification = "unknown"
    elif lifecycle_status == "cancelled" or cancelled_flag:
        classification = "cancelled"
    elif rejected_by_diff_gate or violation_count > 0:
        classification = "rejected_by_diff_gate"
    elif not effort_matched or not model_matched:
        classification = "missing_or_mismatched_telemetry"
    elif not source_check.get("preserved"):
        classification = "source_checkout_modified"
    elif not task_check.get("expected_edit_verified"):
        classification = "expected_edit_missing"
    else:
        classification = "ready_for_review" if passed else "failed"

    return passed, {
        "passed": passed, "outcome_classification": classification, "patch_accepted": False, "review_required": True, "poll_timed_out": poll_timed_out,
        "worker_id": safe_id_or_none(snap.get("worker_id")), "run_id": safe_id_or_none(snap.get("run_id")),
        "binding_id": safe_id_or_none(snap.get("binding_id")), "profile_id": safe_id_or_none(snap.get("profile_id")),
        "provider": PROVIDER_ID, "lifecycle_status": lifecycle_status, "acceptance_state": acceptance_state,
        "requested_model": config.model, "observed_model": observed_model, "requested_effort": config.effort, "observed_effort": observed_effort,
        "missing_model_evidence": missing_model_evidence, "missing_effort_evidence": missing_effort_evidence,
        "progress_counts": {"durable_progress_events": safe_int_or_none(progress.get("durable_progress_events")) or 0, "coalesced_progress_events": safe_int_or_none(progress.get("coalesced_progress_events")) or 0, "dropped_live_events": safe_int_or_none(progress.get("dropped_live_events")) or 0, "last_event_kind": safe_enum_or_none(progress.get("last_event_kind"), KNOWN_EVENT_KINDS)},
        "diff_result": {"passed": diff_gate_passed, "rejected_by_diff_gate": rejected_by_diff_gate, "changed_paths_count": changed_paths_count, "expected_path_only": expected_path_only, "violation_count": violation_count, "base_commit": safe_hex40_or_none(worktree_info.get("base_commit")) or base_commit, "head_commit": safe_hex40_or_none(worktree_info.get("head_commit"))},
        "execution_outcome": {"exit_code": safe_int_or_none(outcome.get("exit_code")), "signal": safe_int_or_none(outcome.get("signal")), "duration_ms": safe_int_or_none(outcome.get("duration_ms")), "timed_out": timed_out_flag, "cancelled": cancelled_flag, "provider_error": provider_error_flag, "incomplete_cleanup": incomplete_cleanup_flag, "stdout_truncated": bool(outcome.get("stdout_truncated", False)), "stderr_truncated": bool(outcome.get("stderr_truncated", False)), "report_artifact_id": safe_id_or_none(outcome.get("report_artifact_id"))},
        "source_checkout": source_check, "task_worktree": task_check,
    }


def run_live_pilot(config: PilotConfig) -> int:
    if os.path.lexists(config.output):
        raise PilotError("preexisting_output_refused")
    validate_no_symlink_ancestors(config.output)
    validate_executable_path(config.threadlane)
    validate_executable_path(config.provider_executable)
    create_private_dir(config.output, require_existing_parent=True)
    source_repo, work_area = create_private_dir(config.output / "source_repo"), create_private_dir(config.output / "work_area")
    state_dir, logs_dir = create_private_dir(config.output / "state"), create_private_dir(config.output / "logs")
    cli_log_path, workers_config_path, summary_path = logs_dir / "cli_diagnostics.log", config.output / "workers_config.json", config.output / "pilot_summary.json"
    base_commit = init_disposable_git_fixture(source_repo, cli_log_path)
    snapshot, poll_timed_out, error_class = None, False, None
    try:
        serve_base = ["serve", "--state-dir", str(state_dir), "--tick-interval-ms", "50"]
        with supervised_threadlane(config.threadlane, serve_base, logs_dir, "supervisor_bootstrap", config.output) as boot_proc:
            bind_args = ["bind", "--state-dir", str(state_dir), "--thread", str(uuid.uuid4()), "--label", "Isolated AGY Live Worker Fixture (Not a Native Chat)"]
            binding_id = safe_id_or_none(call_threadlane_when_ready(boot_proc, config.threadlane, bind_args, cli_log_path).get("binding_id"))
            if not binding_id or not binding_id.startswith("bnd_"):
                raise PilotError("invalid_binding_id")
        workers_cfg = build_workers_config_payload(binding_id, config, source_repo, base_commit, work_area)
        write_private_file(workers_config_path, (json.dumps(workers_cfg, indent=2, sort_keys=True) + "\n").encode("utf-8"))
        with supervised_threadlane(config.threadlane, [*serve_base, "--workers-config", str(workers_config_path)], logs_dir, "supervisor_workers", config.output) as live_proc:
            call_threadlane_when_ready(live_proc, config.threadlane, ["status", "--state-dir", str(state_dir), "--binding", binding_id], cli_log_path)
            start_req = {"op": "start_worker", "command_key": "live-pilot-start-1", "profile_id": PROFILE_ID, "instruction": BOUNDED_TASK_PROMPT}
            start_resp = run_threadlane_cli(config.threadlane, ["call", "--state-dir", str(state_dir), "--binding", binding_id, "--request", json.dumps(start_req)], cli_log_path)
            if start_resp.get("kind") != "worker" or not isinstance(start_resp.get("data"), dict):
                raise PilotError("invalid_start_worker_response")
            snapshot = start_resp["data"]
            run_id = safe_id_or_none(snapshot.get("run_id"))
            if not run_id:
                raise PilotError("invalid_worker_run_id")
            deadline = time.monotonic() + float(config.timeout_seconds) + 15.0
            while snapshot.get("status") not in TERMINAL_WORKER_STATUSES:
                if time.monotonic() >= deadline:
                    poll_timed_out = True
                    break
                if live_proc.poll() is not None:
                    raise PilotError("supervisor_exited_during_worker_run")
                time.sleep(POLL_INTERVAL_SECONDS)
                poll_args = ["call", "--state-dir", str(state_dir), "--binding", binding_id, "--request", json.dumps({"op": "get_worker", "run_id": run_id})]
                poll_resp = run_threadlane_cli(config.threadlane, poll_args, cli_log_path)
                if poll_resp.get("kind") != "worker" or not isinstance(poll_resp.get("data"), dict):
                    raise PilotError("invalid_get_worker_response")
                snapshot = poll_resp["data"]
    except PilotError as exc:
        error_class = exc.error_class
    except Exception:
        error_class = "unexpected_execution_error"

    source_check = verify_source_checkout_unchanged(source_repo, base_commit, cli_log_path)
    worktree_dict = snapshot.get("worktree") if isinstance(snapshot, dict) and isinstance(snapshot.get("worktree"), dict) else {}
    task_check = inspect_owned_task_worktree(work_area, worktree_dict.get("task_worktree_path"))
    passed, evaluation = evaluate_pilot_outcome(config, base_commit, snapshot, poll_timed_out, source_check, task_check)
    if error_class is not None:
        passed, evaluation["passed"], evaluation["error_class"] = False, False, error_class
    summary_payload = {
        "mode": "live", "live_execution": True, "paid_provider_fixture": True,
        "native_chat_acceptance_proof": False, "output_root": str(config.output),
        "diagnostics_log_dir": str(logs_dir), "preserved_paths": build_layout_paths(config.output), **evaluation,
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
        if not config.allow_live:
            print(json.dumps(build_dry_run_plan(config), indent=2, sort_keys=True), flush=True)
            return 0
        return run_live_pilot(config)
    except SystemExit as exc:
        return 0 if exc.code in (0, None) else 2
    except PilotError as exc:
        print(json.dumps({"error_class": exc.error_class, "passed": False, "patch_accepted": False}, sort_keys=True), file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
