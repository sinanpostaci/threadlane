#!/usr/bin/env python3
"""Self-contained CLI smoke test for Threadlane Milestone 1.

Runs a purely simulated, deterministic demo loop against an isolated temporary
state directory:
  1. Starts `threadlane serve` on 127.0.0.1 with a fresh private descriptor.
  2. Creates a trusted binding (`threadlane bind`).
  3. Starts a deterministic demo run (`start_demo`).
  4. Waits for the run to reach `waiting_input` and pose a question.
  5. Pulls and acknowledges the emitted event batch (`pull_events` + `ack_events`).
  6. Answers the pending question (`answer`) at revision 1.
  7. Waits for the run to reach `completed`, acknowledges completion events, and
     shuts down the supervisor cleanly.

Does NOT access real user chats, configuration, or paid external models.
"""

from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
DEMO_THREAD_UUID = "11111111-2222-4333-8444-555555555555"


def resolve_binary() -> Path:
    env_bin = os.environ.get("THREADLANE_BIN")
    if env_bin:
        path = Path(env_bin).resolve()
        if path.is_file():
            return path
        raise RuntimeError(f"THREADLANE_BIN does not exist: {path}")

    debug_bin = REPO_ROOT / "target" / "debug" / "threadlane"
    if debug_bin.is_file():
        return debug_bin

    release_bin = REPO_ROOT / "target" / "release" / "threadlane"
    if release_bin.is_file():
        return release_bin

    print("[smoke] Building `threadlane` binary via cargo...", flush=True)
    subprocess.run(
        ["cargo", "build", "-p", "threadlane", "--bin", "threadlane"],
        cwd=REPO_ROOT,
        check=True,
    )
    if not debug_bin.is_file():
        raise RuntimeError(f"Expected compiled binary at {debug_bin}")
    return debug_bin


def run_cli_json(bin_path: Path, args: list[str]) -> Any:
    proc = subprocess.run(
        [str(bin_path), *args],
        cwd=REPO_ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10.0,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"CLI command {args} failed (exit {proc.returncode}):\n{proc.stderr}"
        )
    return json.loads(proc.stdout)


def main() -> int:
    bin_path = resolve_binary()
    print(f"[smoke] Using binary: {bin_path} (DETERMINISTIC SIMULATION ONLY)", flush=True)

    with tempfile.TemporaryDirectory(prefix="threadlane-smoke-") as tmp_root:
        state_dir = Path(tmp_root) / "state"
        descriptor_path = state_dir / "supervisor.json"

        serve_proc = subprocess.Popen(
            [
                str(bin_path),
                "serve",
                "--state-dir",
                str(state_dir),
                "--tick-interval-ms",
                "20",
            ],
            cwd=REPO_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            deadline = time.monotonic() + 5.0
            while not descriptor_path.is_file():
                if serve_proc.poll() is not None:
                    stderr_out = serve_proc.stderr.read() if serve_proc.stderr else ""
                    raise RuntimeError(
                        f"Supervisor exited early with code {serve_proc.returncode}: {stderr_out}"
                    )
                if time.monotonic() >= deadline:
                    raise TimeoutError("Timed out waiting for supervisor.json")
                time.sleep(0.02)

            dir_mode = stat.S_IMODE(state_dir.lstat().st_mode)
            desc_mode = stat.S_IMODE(descriptor_path.lstat().st_mode)
            if dir_mode != 0o700:
                raise AssertionError(f"Expected state_dir mode 0700, got {oct(dir_mode)}")
            if desc_mode != 0o600:
                raise AssertionError(f"Expected descriptor mode 0600, got {oct(desc_mode)}")

            # 1. Create a trusted chat binding.
            binding = run_cli_json(
                bin_path,
                [
                    "bind",
                    "--state-dir",
                    str(state_dir),
                    "--thread",
                    DEMO_THREAD_UUID,
                    "--label",
                    "Simulated Smoke Demo Chat",
                ],
            )
            binding_id = binding["binding_id"]
            print(f"[smoke] Created binding: {binding_id} for thread {DEMO_THREAD_UUID}", flush=True)

            # 2. Start a deterministic demo run.
            start_resp = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps(
                        {
                            "op": "start_demo",
                            "command_key": "smoke-start-1",
                            "instruction": "Simulated smoke test task",
                            "step_delay_ms": 40,
                        }
                    ),
                ],
            )
            run_id = start_resp["data"]["run_id"]
            print(f"[smoke] Started simulated demo run: {run_id}", flush=True)

            # 3. Wait for run to reach `waiting_input` and pose a question.
            question_id = None
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                status = run_cli_json(
                    bin_path,
                    ["status", "--state-dir", str(state_dir), "--binding", binding_id],
                )
                recent = status["recent_runs"][0]
                if recent["status"] == "waiting_input" and recent["active_question_id"]:
                    question_id = recent["active_question_id"]
                    break
                time.sleep(0.02)

            if not question_id:
                raise TimeoutError(f"Run {run_id} did not reach waiting_input")

            run_detail = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps({"op": "get_run", "run_id": run_id}),
                ],
            )["data"]
            prompt_text = run_detail["questions"][0]["prompt"]
            print(
                f"[smoke] Run waiting for input on question {question_id} (untrusted prompt: {prompt_text!r})",
                flush=True,
            )

            # 4. Pull and acknowledge initial events.
            batch_1 = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps({"op": "pull_events", "limit": 20}),
                ],
            )["data"]
            ack_1 = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps(
                        {
                            "op": "ack_events",
                            "command_key": "smoke-ack-1",
                            "batch_id": batch_1["batch_id"],
                        }
                    ),
                ],
            )["data"]
            print(
                f"[smoke] Pulled and acknowledged batch {batch_1['batch_id']} (acked_seq={ack_1['acked_seq']})",
                flush=True,
            )

            # 5. Answer the question on revision 1.
            run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps(
                        {
                            "op": "answer",
                            "command_key": "smoke-answer-1",
                            "run_id": run_id,
                            "question_id": question_id,
                            "expected_revision": 1,
                            "answer": "Confirmed simulated step execution",
                        }
                    ),
                ],
            )
            print(f"[smoke] Submitted answer for question {question_id}", flush=True)

            # 6. Wait for the run to complete.
            completed = False
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                status = run_cli_json(
                    bin_path,
                    ["status", "--state-dir", str(state_dir), "--binding", binding_id],
                )
                if status["recent_runs"][0]["status"] == "completed":
                    completed = True
                    break
                time.sleep(0.02)

            if not completed:
                raise TimeoutError(f"Run {run_id} did not reach completed status")

            # 7. Pull and acknowledge completion events.
            batch_2 = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps({"op": "pull_events", "limit": 20}),
                ],
            )["data"]
            ack_2 = run_cli_json(
                bin_path,
                [
                    "call",
                    "--state-dir",
                    str(state_dir),
                    "--binding",
                    binding_id,
                    "--request",
                    json.dumps(
                        {
                            "op": "ack_events",
                            "command_key": "smoke-ack-2",
                            "batch_id": batch_2["batch_id"],
                        }
                    ),
                ],
            )["data"]
            assert ack_2["wakeup_status"] == "idle", f"Expected idle wakeup, got {ack_2}"
            print(
                f"[smoke] Run {run_id} completed and all events acknowledged (acked_seq={ack_2['acked_seq']}, wakeup=idle).",
                flush=True,
            )

        finally:
            if serve_proc.poll() is None:
                serve_proc.send_signal(signal.SIGTERM)
                try:
                    serve_proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    serve_proc.kill()
                    serve_proc.wait(timeout=2.0)

    print("[smoke] SUCCESS (simulated Milestone 1 loop verified).", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
