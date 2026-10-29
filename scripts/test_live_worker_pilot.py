#!/usr/bin/env python3
"""Unit and local fake-supervisor integration tests for `scripts/live_worker_pilot.py`.

All tests run offline against temporary directories and local fake executables using
`sys.executable`; no paid provider or external network is ever invoked.
"""

from __future__ import annotations

import contextlib, io, json, os, stat, sys, tempfile, textwrap, unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import live_worker_pilot as pilot


def _write_exec(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
    path.chmod(0o700)
    return path


def _run_main(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = pilot.main(argv)
    return rc, out.getvalue(), err.getvalue()


class LiveWorkerPilotTests(unittest.TestCase):
    def test_dry_run_emits_bounded_plan_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_dir = root / "nonexistent_parent" / "pilot_out"
            rc, stdout, stderr = _run_main([
                "--threadlane", str(root / "missing_threadlane"), "--provider-executable", str(root / "missing_agy"),
                "--model", "gemini-4-argon-high", "--effort", "high", "--output", str(out_dir), "--timeout-seconds", "180",
            ])
            self.assertEqual((rc, stderr, out_dir.exists()), (0, "", False))
            plan = json.loads(stdout)
            self.assertEqual((plan["mode"], plan["live_execution"], plan["paid_provider_fixture"]), ("dry_run", False, True))
            self.assertEqual((plan["native_chat_acceptance_proof"], plan["patch_accepted"]), (False, False))
            self.assertEqual((plan["requested_model"], plan["requested_effort"]), ("gemini-4-argon-high", "high"))
            self.assertEqual(plan["profile_plan"]["file_permissions"], "task_files")
            self.assertEqual(plan["profile_plan"]["allowed_write_paths"], ["greeting.txt"])
            self.assertEqual(plan["profile_plan"]["budgets"]["timeout_ms"], 180_000)
            cfg = pilot.parse_and_validate_args([
                "--threadlane", str(root / "missing_threadlane"), "--provider-executable", str(root / "missing_agy"),
                "--model", "gemini-4-argon-high", "--effort", "high", "--output", str(out_dir), "--timeout-seconds", "180",
            ])
            wcfg = pilot.build_workers_config_payload("bnd_01", cfg, out_dir / "source_repo", "a" * 40, out_dir / "work_area")
            self.assertEqual(wcfg["profiles"][0]["spec"]["file_permissions"], "task_files")

    def test_argument_validation_and_flag_injection_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for help_flag in ("--help", "-h"):
                rc_h, stdout_h, stderr_h = _run_main([help_flag])
                self.assertEqual((rc_h, stderr_h, (root / "out").exists()), (0, "", False))
                self.assertIn("usage:", stdout_h)

            base = ["--threadlane", str(root / "tl"), "--provider-executable", str(root / "ag"), "--model", "gemini-4-argon-high", "--effort", "high", "--output", str(root / "out")]
            bad_cases = [
                [],
                [*base, "--unknown-flag"],
                ["--threadlane", "relative/threadlane", *base[2:]],
                ["--threadlane", f"{root}/../escape", *base[2:]],
                [*base[:8], "--output", "/"],
                [*base[:8], "--output", f"{root}/trailing/"],
                [*base[:4], "--model", "", *base[6:]],
                [*base[:4], "--model", "bad model!", *base[6:]],
                [*base[:4], "--model", "-flag-injection", *base[6:]],
                [*base[:4], "--model=-flag-injection", *base[6:]],
                [*base[:6], "--effort", "-high", *base[8:]],
                [*base[:6], "--effort=-high", *base[8:]],
                [*base, "--timeout-seconds", "0"],
                [*base, "--timeout-seconds", "-5"],
                [*base, "--timeout-seconds", "3601"],
            ]
            for argv in bad_cases:
                with self.subTest(argv=argv):
                    rc, stdout, _stderr = _run_main(argv)
                    self.assertNotEqual(rc, 0)
                    self.assertEqual(stdout, "")
                    self.assertFalse((root / "out").exists())

            for eq_flag in (["--model=-flag-injection"], ["--effort=-high"]):
                merged = [*base[:4], *eq_flag, *base[6:]] if eq_flag[0].startswith("--model=") else [*base[:6], *eq_flag, *base[8:]]
                rc, _, stderr = _run_main(merged)
                self.assertEqual(rc, 2)
                self.assertEqual(json.loads(stderr)["error_class"], "invalid_identifier_argument")

    def test_live_mode_refuses_and_preserves_preexisting_or_symlink_targets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tl = _write_exec(root / "tl", "raise SystemExit(0)\n")
            ag = _write_exec(root / "ag", "raise SystemExit(0)\n")

            def invoke(out_path: Path) -> tuple[int, str, str]:
                return _run_main(["--threadlane", str(tl), "--provider-executable", str(ag), "--model", "gemini-4-argon-high", "--effort", "high", "--output", str(out_path), "--allow-live"])

            existing_dir = root / "existing_dir"
            existing_dir.mkdir(mode=0o700)
            sentinel = existing_dir / "sentinel.txt"
            sentinel.write_text("preserve-me\n", encoding="utf-8")
            rc, out, _ = invoke(existing_dir)
            self.assertNotEqual(rc, 0)
            self.assertEqual((out, sentinel.read_text(encoding="utf-8")), ("", "preserve-me\n"))

            existing_file = root / "existing_file"
            existing_file.write_text("do-not-overwrite\n", encoding="utf-8")
            self.assertNotEqual(invoke(existing_file)[0], 0)
            self.assertEqual(existing_file.read_text(encoding="utf-8"), "do-not-overwrite\n")

            real_dir = root / "real_target_dir"
            real_dir.mkdir(mode=0o700)
            (real_dir / "target.txt").write_text("untouched\n", encoding="utf-8")
            sym_dir = root / "symlink_output"
            sym_dir.symlink_to(real_dir)
            self.assertNotEqual(invoke(sym_dir)[0], 0)
            self.assertTrue(sym_dir.is_symlink())
            self.assertEqual((real_dir / "target.txt").read_text(encoding="utf-8"), "untouched\n")

            dangling = root / "dangling_symlink"
            dangling.symlink_to(root / "nonexistent_target")
            self.assertNotEqual(invoke(dangling)[0], 0)
            self.assertFalse((root / "nonexistent_target").exists())

            sym_parent = root / "symlink_parent"
            sym_parent.symlink_to(real_dir)
            self.assertNotEqual(invoke(sym_parent / "child_out")[0], 0)
            self.assertFalse((real_dir / "child_out").exists())

    def test_evaluate_pilot_outcome_classifications_and_whitelist(self) -> None:
        cfg = pilot.PilotConfig(Path("/bin/tl"), Path("/bin/ag"), "gemini-4-argon-high", "high", Path("/tmp/out"), 120, True)
        base_sha, head_sha = "a" * 40, "b" * 40
        src_ok = {"preserved": True, "head_matches_base": True, "status_clean": True, "greeting_unchanged": True, "head_commit": base_sha}
        task_ok = {"task_worktree_path": "/tmp/out/work_area/wt_1", "expected_edit_verified": True, "observed_greeting_bytes": 22}
        secret_marker = "UNTRUSTED_PROVIDER_SUMMARY_DO_NOT_ECHO_sk-live-999999999999999999"
        good_snap = {
            "worker_id": "wrk_01", "run_id": "run_01", "binding_id": "bnd_01", "profile_id": "agy_live_pilot",
            "status": "completed", "observed_model": "gemini-4-argon-high", "observed_effort": "high",
            "worktree": {"base_commit": base_sha, "head_commit": head_sha, "task_worktree_path": "/tmp/out/work_area/wt_1"},
            "progress": {"durable_progress_events": 2, "coalesced_progress_events": 1, "dropped_live_events": 0, "last_event_kind": "result", "last_summary": secret_marker},
            "outcome": {
                "acceptance_state": "ready_for_review", "exit_code": 0, "signal": None, "duration_ms": 500,
                "timed_out": False, "cancelled": False, "provider_error": False, "incomplete_cleanup": False,
                "missing_model_evidence": False, "missing_effort_evidence": False, "rejected_by_diff_gate": False,
                "changed_paths_count": 1, "changed_paths_preview": ["greeting.txt"], "violation_count": 0,
                "violation_summaries": [secret_marker], "final_summary": secret_marker, "failure_reason": secret_marker,
                "stdout_truncated": False, "stderr_truncated": False, "report_artifact_id": "art_01",
            },
        }
        passed, ev = pilot.evaluate_pilot_outcome(cfg, base_sha, good_snap, False, src_ok, task_ok)
        self.assertEqual((passed, ev["outcome_classification"], ev["patch_accepted"]), (True, "ready_for_review", False))
        self.assertNotIn(secret_marker, json.dumps(ev))

        no_effort = {**good_snap, "status": "failed", "observed_effort": None, "outcome": {**good_snap["outcome"], "acceptance_state": "execution_failed", "missing_effort_evidence": True}}
        passed_ne, ev_ne = pilot.evaluate_pilot_outcome(cfg, base_sha, no_effort, False, src_ok, task_ok)
        self.assertEqual((passed_ne, ev_ne["outcome_classification"], ev_ne["acceptance_state"]), (False, "missing_or_mismatched_telemetry", "execution_failed"))

        for status_val, state_val, expected_cls in (
            ("timed_out", "execution_failed", "timed_out"),
            ("unknown", "unknown", "unknown"),
            ("failed", "rejected_by_diff_gate", "rejected_by_diff_gate"),
            ("completed", "pending_review", "failed"),
        ):
            snap = {**good_snap, "status": status_val, "outcome": {**good_snap["outcome"], "acceptance_state": state_val, "rejected_by_diff_gate": state_val == "rejected_by_diff_gate"}}
            ok, res = pilot.evaluate_pilot_outcome(cfg, base_sha, snap, False, src_ok, task_ok)
            self.assertEqual((ok, res["outcome_classification"], res["acceptance_state"], res["patch_accepted"]), (False, expected_cls, state_val, False))

        for active_status in ("queued", "claimed", "running", "cancel_requested"):
            ok, res = pilot.evaluate_pilot_outcome(cfg, base_sha, {**good_snap, "status": active_status}, False, src_ok, task_ok)
            self.assertEqual((ok, res["lifecycle_status"]), (False, active_status))
        _, res_bad_status = pilot.evaluate_pilot_outcome(cfg, base_sha, {**good_snap, "status": "cancelling"}, False, src_ok, task_ok)
        self.assertIsNone(res_bad_status["lifecycle_status"])

        for kind in ("system_init", "assistant_message", "tool_use", "tool_result", "blocked", "error", "result", "other"):
            _, res_kind = pilot.evaluate_pilot_outcome(cfg, base_sha, {**good_snap, "progress": {**good_snap["progress"], "last_event_kind": kind}}, False, src_ok, task_ok)
            self.assertEqual(res_kind["progress_counts"]["last_event_kind"], kind)
        _, res_bad_kind = pilot.evaluate_pilot_outcome(cfg, base_sha, {**good_snap, "progress": {**good_snap["progress"], "last_event_kind": "progress"}}, False, src_ok, task_ok)
        self.assertIsNone(res_bad_kind["progress_counts"]["last_event_kind"])

    def test_live_mode_end_to_end_with_fake_local_supervisor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake_ag = _write_exec(root / "fake_agy", "raise SystemExit(0)\n")
            fake_tl = _write_exec(
                root / "fake_threadlane",
                """
                import json, signal, sys, time
                from pathlib import Path
                args = sys.argv[1:]
                cmd, state_dir = args[0], Path(args[args.index("--state-dir") + 1])
                if cmd == "serve":
                    wcfg = args[args.index("--workers-config") + 1] if "--workers-config" in args else None
                    (state_dir / "ready.tmp").write_text(json.dumps({"workers_config": wcfg}), encoding="utf-8")
                    (state_dir / "ready.tmp").replace(state_dir / "ready.json")
                    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
                    while True:
                        time.sleep(0.05)
                ready = json.loads((state_dir / "ready.json").read_text(encoding="utf-8"))
                if cmd == "bind":
                    print(json.dumps({"binding_id": "bnd_fake0001"}))
                elif cmd == "status":
                    sys.exit(0 if ready.get("workers_config") and print(json.dumps({"status": "ok"})) is None else 1)
                elif cmd == "call":
                    req, wfile = json.loads(args[args.index("--request") + 1]), state_dir / "worker.json"
                    if req["op"] == "start_worker":
                        spec = json.loads(Path(ready["workers_config"]).read_text(encoding="utf-8"))["profiles"][0]["spec"]
                        if spec.get("file_permissions") != "task_files":
                            sys.exit(2)
                        wt = Path(spec["work_area_root"]) / "wt_fake0001"
                        wt.mkdir(mode=0o700, exist_ok=True)
                        (wt / "greeting.txt").write_text("Hello from Threadlane\\n", encoding="utf-8")
                        snap = {
                            "worker_id": "wrk_fake0001", "run_id": "run_fake0001", "binding_id": "bnd_fake0001",
                            "profile_id": req["profile_id"], "status": "running", "observed_model": spec["model"], "observed_effort": spec["effort"],
                            "worktree": {"base_commit": spec["base_commit"], "head_commit": "c" * 40, "task_worktree_path": str(wt)},
                            "progress": {"durable_progress_events": 1, "coalesced_progress_events": 0, "dropped_live_events": 0, "last_event_kind": "tool_use", "last_summary": "SECRET_SUMMARY_NEVER_PRINT"},
                            "outcome": None,
                        }
                        wfile.write_text(json.dumps(snap), encoding="utf-8")
                        print(json.dumps({"kind": "worker", "data": snap}))
                    elif req["op"] == "get_worker":
                        snap = json.loads(wfile.read_text(encoding="utf-8"))
                        snap["status"], snap["progress"]["last_event_kind"] = "completed", "result"
                        snap["outcome"] = {
                            "acceptance_state": "ready_for_review", "exit_code": 0, "signal": None, "duration_ms": 42,
                            "timed_out": False, "cancelled": False, "provider_error": False, "incomplete_cleanup": False,
                            "missing_model_evidence": False, "missing_effort_evidence": False, "rejected_by_diff_gate": False,
                            "changed_paths_count": 1, "changed_paths_preview": ["greeting.txt"], "violation_count": 0,
                            "violation_summaries": ["SECRET_VIOLATION_NEVER_PRINT"], "final_summary": "SECRET_FINAL_NEVER_PRINT",
                            "failure_reason": None, "stdout_truncated": False, "stderr_truncated": False, "report_artifact_id": "art_fake0001",
                        }
                        print(json.dumps({"kind": "worker", "data": snap}))
                """,
            )
            out_dir = root / "pilot_run"
            rc, stdout, stderr = _run_main(["--threadlane", str(fake_tl), "--provider-executable", str(fake_ag), "--model", "gemini-4-argon-high", "--effort", "high", "--output", str(out_dir), "--timeout-seconds", "30", "--allow-live"])
            self.assertEqual((rc, stderr), (0, ""), msg=f"stdout={stdout} stderr={stderr}")
            for secret in ("SECRET_SUMMARY_NEVER_PRINT", "SECRET_VIOLATION_NEVER_PRINT", "SECRET_FINAL_NEVER_PRINT"):
                self.assertNotIn(secret, stdout)
            summary = json.loads(stdout)
            self.assertEqual((summary["passed"], summary["outcome_classification"], summary["patch_accepted"]), (True, "ready_for_review", False))
            self.assertEqual(summary["progress_counts"]["last_event_kind"], "result")
            wcfg_disk = json.loads((out_dir / "workers_config.json").read_text(encoding="utf-8"))
            self.assertEqual(wcfg_disk["profiles"][0]["spec"]["file_permissions"], "task_files")
            self.assertTrue(summary["source_checkout"]["preserved"] and summary["task_worktree"]["expected_edit_verified"])
            for d in (out_dir, out_dir / "source_repo", out_dir / "work_area", out_dir / "state", out_dir / "logs"):
                self.assertEqual(stat.S_IMODE(os.lstat(d).st_mode), 0o700)
            for f in (out_dir / "workers_config.json", out_dir / "pilot_summary.json"):
                self.assertEqual(stat.S_IMODE(os.lstat(f).st_mode), 0o600)


if __name__ == "__main__":
    unittest.main()
