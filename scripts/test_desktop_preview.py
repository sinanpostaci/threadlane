#!/usr/bin/env python3
"""Unit and local fake-subprocess lifecycle tests for `scripts/desktop_preview.py`.

All tests run offline against temporary directories and local dummy executables built with
`sys.executable`; no real Tauri window, paid provider, or external service is invoked.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import desktop_preview as preview
import live_worker_pilot as pilot


def _write_exec(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
    path.chmod(0o700)
    return path


def _run_main(argv: list[str]) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        rc = preview.main(argv)
    return rc, out.getvalue(), err.getvalue()


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class DesktopPreviewTests(unittest.TestCase):
    def test_dry_run_emits_plan_without_filesystem_or_process_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "nonexistent_parent" / "previews"
            ext_state = root / "external_runtime" / "state"

            rc_default, out_default, err_default = _run_main(["--dry-run"])
            self.assertEqual((rc_default, err_default), (0, ""))
            plan_default = json.loads(out_default)
            self.assertEqual(
                (
                    plan_default["mode"],
                    plan_default["live_execution"],
                    plan_default["paid_workers_enabled"],
                    plan_default["codex_wakeup_enabled"],
                ),
                ("dry_run", False, False, False),
            )
            self.assertEqual(plan_default["threadlane_executable"], str(preview.DEFAULT_THREADLANE_BIN))
            self.assertEqual(plan_default["desktop_executable"], str(preview.DEFAULT_DESKTOP_BIN))
            self.assertEqual(plan_default["output_root"], str(preview.DEFAULT_OUTPUT_ROOT))
            self.assertIsNone(plan_default["attached_state_dir"])

            rc, stdout, stderr = _run_main([
                "--dry-run",
                "--threadlane",
                str(root / "missing_threadlane"),
                "--desktop",
                str(root / "missing_desktop"),
                "--state-dir",
                str(ext_state),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual((rc, stderr, out_root.exists(), ext_state.exists()), (0, "", False, False))
            plan = json.loads(stdout)
            demo_state = plan["planned_layout"]["demo_state_dir"]
            self.assertEqual(plan["attached_state_dir"], str(ext_state))
            self.assertEqual(
                plan["planned_commands"]["demo_supervisor_argv"],
                [str(root / "missing_threadlane"), "serve", "--state-dir", demo_state],
            )
            self.assertEqual(
                plan["planned_commands"]["readiness_probe_argv"],
                [str(root / "missing_threadlane"), "list-bindings", "--state-dir", demo_state],
            )
            self.assertEqual(
                plan["planned_commands"]["desktop_argv"],
                [
                    str(root / "missing_desktop"),
                    "--demo-state-dir",
                    demo_state,
                    "--state-dir",
                    str(ext_state),
                ],
            )

    def test_argument_validation_and_flag_injection_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for help_flag in ("--help", "-h"):
                rc_h, out_h, err_h = _run_main([help_flag])
                self.assertEqual((rc_h, err_h), (0, ""))
                self.assertIn("usage:", out_h)

            bad_cases = [
                ["--unknown-flag"],
                ["--threadlane", "relative/threadlane", "--dry-run"],
                ["--desktop", "relative/desktop", "--dry-run"],
                ["--state-dir", "relative/state", "--dry-run"],
                ["--output-root", "relative/out", "--dry-run"],
                ["--threadlane", f"{root}/../escape", "--dry-run"],
                ["--output-root", "/", "--dry-run"],
                ["--output-root", f"{root}/trailing/", "--dry-run"],
                ["--threadlane=-flag-injection", "--dry-run"],
                ["--desktop=-flag-injection", "--dry-run"],
                ["--state-dir=-flag-injection", "--dry-run"],
                ["--output-root=-flag-injection", "--dry-run"],
                ["--output-root", str(root / "shared"), "--state-dir", str(root / "shared"), "--dry-run"],
                [
                    "--output-root",
                    str(root / "shared"),
                    "--state-dir",
                    str(root / "shared" / "nested"),
                    "--dry-run",
                ],
                [
                    "--output-root",
                    str(root / "shared" / "nested"),
                    "--state-dir",
                    str(root / "shared"),
                    "--dry-run",
                ],
            ]
            for argv in bad_cases:
                with self.subTest(argv=argv):
                    rc, stdout, stderr = _run_main(argv)
                    self.assertEqual(rc, 2)
                    self.assertEqual(stdout, "")
                    self.assertFalse(json.loads(stderr)["passed"])

    def test_refuses_symlinks_and_invalid_executables_or_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tl = _write_exec(root / "tl", "raise SystemExit(0)\n")
            desk = _write_exec(root / "desk", "raise SystemExit(0)\n")
            out_root = root / "previews"

            rc_missing_tl, _, err_missing_tl = _run_main([
                "--threadlane",
                str(root / "missing_tl"),
                "--desktop",
                str(desk),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual(rc_missing_tl, 2)
            self.assertEqual(json.loads(err_missing_tl)["error_class"], "threadlane_executable_not_found")
            self.assertFalse(out_root.exists())

            rc_missing_desk, _, err_missing_desk = _run_main([
                "--threadlane",
                str(tl),
                "--desktop",
                str(root / "missing_desk"),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual(rc_missing_desk, 2)
            self.assertEqual(json.loads(err_missing_desk)["error_class"], "desktop_executable_not_found")
            self.assertFalse(out_root.exists())

            non_exec = root / "non_exec"
            non_exec.write_text("not executable\n", encoding="utf-8")
            non_exec.chmod(0o600)
            rc_non_exec, _, err_non_exec = _run_main([
                "--threadlane",
                str(non_exec),
                "--desktop",
                str(desk),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual(rc_non_exec, 2)
            self.assertEqual(json.loads(err_non_exec)["error_class"], "invalid_executable")

            sym_tl = root / "sym_tl"
            sym_tl.symlink_to(tl)
            rc_sym_tl, _, _ = _run_main([
                "--threadlane",
                str(sym_tl),
                "--desktop",
                str(desk),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual(rc_sym_tl, 2)

            real_dir = root / "real_dir"
            real_dir.mkdir(mode=0o700)
            sentinel = real_dir / "sentinel.txt"
            sentinel.write_text("keep-me\n", encoding="utf-8")
            sym_out = root / "sym_out"
            sym_out.symlink_to(real_dir)
            rc_sym_out, _, _ = _run_main([
                "--threadlane",
                str(tl),
                "--desktop",
                str(desk),
                "--output-root",
                str(sym_out),
            ])
            self.assertEqual(rc_sym_out, 2)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep-me\n")

            file_state = root / "file_state"
            file_state.write_text("not-a-dir\n", encoding="utf-8")
            rc_file_state, _, err_file_state = _run_main([
                "--threadlane",
                str(tl),
                "--desktop",
                str(desk),
                "--state-dir",
                str(file_state),
                "--output-root",
                str(out_root),
            ])
            self.assertEqual(rc_file_state, 2)
            self.assertEqual(json.loads(err_file_state)["error_class"], "invalid_state_dir")
            self.assertFalse(out_root.exists())

    def test_live_lifecycle_stops_only_owned_demo_supervisor_and_preserves_attached_runtime_and_logs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            external_state_dir = root / "external_runtime"
            external_state_dir.mkdir(mode=0o700)
            external_secret = "a" * 64
            demo_secret = "b" * 64
            probe_err_secret = "c" * 64

            external_proc = subprocess.Popen(
                [sys.executable, "-c", "import time\nwhile True:\n    time.sleep(0.1)\n"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                external_desc = external_state_dir / "supervisor.json"
                external_desc.write_text(
                    json.dumps({
                        "version": 1,
                        "host": "127.0.0.1",
                        "port": 41234,
                        "pid": external_proc.pid,
                        "token": external_secret,
                    }),
                    encoding="utf-8",
                )
                external_desc.chmod(0o600)
                external_desc_before = external_desc.read_bytes()

                fake_tl = _write_exec(
                    root / "fake_threadlane",
                    f"""
                    import json, os, signal, sys, time
                    from pathlib import Path

                    args = sys.argv[1:]
                    cmd = args[0]
                    state_dir = Path(args[args.index("--state-dir") + 1])

                    if cmd == "serve":
                        for forbidden in ("--codex", "--notify-binding", "--workers-config"):
                            if forbidden in args:
                                sys.exit(9)
                        (state_dir / "demo_serve.pid").write_text(str(os.getpid()), encoding="utf-8")
                        desc = {{
                            "version": 1,
                            "host": "127.0.0.1",
                            "port": 49876,
                            "pid": os.getpid(),
                            "token": {demo_secret!r},
                        }}
                        (state_dir / "supervisor.json").write_text(json.dumps(desc), encoding="utf-8")
                        os.chmod(state_dir / "supervisor.json", 0o600)

                        def _on_term(*_):
                            (state_dir / "demo_stopped.marker").write_text("stopped\\n", encoding="utf-8")
                            sys.exit(0)

                        signal.signal(signal.SIGTERM, _on_term)
                        while True:
                            time.sleep(0.02)

                    if cmd == "list-bindings":
                        attempt_file = state_dir / "probe_attempts.txt"
                        count = int(attempt_file.read_text(encoding="utf-8")) if attempt_file.exists() else 0
                        attempt_file.write_text(str(count + 1), encoding="utf-8")
                        if count == 0 or not (state_dir / "supervisor.json").exists():
                            sys.stderr.write("not ready yet Bearer " + {probe_err_secret!r} + "\\n")
                            sys.exit(1)
                        print("[]")
                        sys.exit(0)

                    sys.exit(2)
                    """,
                )

                fake_desk = _write_exec(
                    root / "fake_desktop",
                    """
                    import json, os, sys
                    from pathlib import Path

                    args = sys.argv[1:]
                    demo_dir = Path(args[args.index("--demo-state-dir") + 1])
                    ext_dir = Path(args[args.index("--state-dir") + 1])
                    demo_pid = int((demo_dir / "demo_serve.pid").read_text(encoding="utf-8").strip())
                    os.kill(demo_pid, 0)
                    (demo_dir / "desktop_seen.json").write_text(
                        json.dumps({"demo_state_dir": str(demo_dir), "state_dir": str(ext_dir), "demo_pid": demo_pid}),
                        encoding="utf-8",
                    )
                    print("desktop preview ran cleanly")
                    sys.exit(0)
                    """,
                )

                out_root = root / "desktop_previews"
                rc, stdout, stderr = _run_main([
                    "--threadlane",
                    str(fake_tl),
                    "--desktop",
                    str(fake_desk),
                    "--state-dir",
                    str(external_state_dir),
                    "--output-root",
                    str(out_root),
                ])
                self.assertEqual((rc, stderr), (0, ""), msg=f"stdout={stdout} stderr={stderr}")
                summary = json.loads(stdout)
                self.assertTrue(summary["passed"])
                self.assertIsNone(summary["error_class"])
                self.assertEqual(summary["desktop_exit_code"], 0)
                self.assertTrue(summary["demo_supervisor_stopped"])
                self.assertEqual(summary["attached_state_dir"], str(external_state_dir))

                demo_state_dir = Path(summary["demo_state_dir"])
                session_dir = Path(summary["session_dir"])
                logs_dir = Path(summary["logs_dir"])
                self.assertTrue((demo_state_dir / "demo_stopped.marker").is_file())
                demo_pid = int((demo_state_dir / "demo_serve.pid").read_text(encoding="utf-8").strip())
                self.assertFalse(_pid_is_alive(demo_pid))

                seen = json.loads((demo_state_dir / "desktop_seen.json").read_text(encoding="utf-8"))
                self.assertEqual(seen["demo_state_dir"], str(demo_state_dir))
                self.assertEqual(seen["state_dir"], str(external_state_dir))

                # Attached external runtime must remain running and untouched.
                self.assertIsNone(external_proc.poll())
                self.assertEqual(external_desc.read_bytes(), external_desc_before)

                for d in (out_root, session_dir, demo_state_dir, logs_dir):
                    self.assertEqual(stat.S_IMODE(os.lstat(d).st_mode), 0o700)
                for key in (
                    "supervisor_stdout_log",
                    "supervisor_stderr_log",
                    "desktop_stdout_log",
                    "desktop_stderr_log",
                    "cli_diagnostics_log",
                    "summary_path",
                ):
                    fpath = Path(summary["preserved_paths"][key])
                    self.assertTrue(fpath.is_file())
                    self.assertEqual(stat.S_IMODE(os.lstat(fpath).st_mode), 0o600)

                cli_diag = Path(summary["preserved_paths"]["cli_diagnostics_log"]).read_text(encoding="utf-8")
                self.assertIn("[REDACTED_TOKEN]", cli_diag)
                for secret in (external_secret, demo_secret, probe_err_secret):
                    self.assertNotIn(secret, stdout)
                    self.assertNotIn(secret, stderr)
                    self.assertNotIn(secret, cli_diag)
            finally:
                if external_proc.poll() is None:
                    external_proc.kill()
                    external_proc.wait(timeout=3.0)

    def test_supervisor_reaped_on_readiness_timeout_and_nonzero_desktop_exit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            # Case 1: Supervisor never becomes ready on list-bindings; owned server is still reaped.
            unready_tl = _write_exec(
                root / "unready_tl",
                """
                import os, signal, sys, time
                from pathlib import Path

                args = sys.argv[1:]
                cmd = args[0]
                state_dir = Path(args[args.index("--state-dir") + 1])
                if cmd == "serve":
                    (state_dir / "serve.pid").write_text(str(os.getpid()), encoding="utf-8")
                    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
                    while True:
                        time.sleep(0.02)
                sys.stderr.write('{"token": "' + ("d" * 64) + '"}\\n')
                sys.exit(1)
                """,
            )
            noop_desk = _write_exec(root / "noop_desk", "raise SystemExit(0)\n")
            out_timeout = root / "previews_timeout"

            with mock.patch.object(preview, "SUPERVISOR_READY_TIMEOUT_SECONDS", 0.25):
                rc_t, out_t, err_t = _run_main([
                    "--threadlane",
                    str(unready_tl),
                    "--desktop",
                    str(noop_desk),
                    "--output-root",
                    str(out_timeout),
                ])
            self.assertEqual((rc_t, err_t), (1, ""))
            summary_t = json.loads(out_t)
            self.assertEqual((summary_t["passed"], summary_t["error_class"]), (False, "supervisor_ready_timeout"))
            pid_t = int((Path(summary_t["demo_state_dir"]) / "serve.pid").read_text(encoding="utf-8").strip())
            self.assertFalse(_pid_is_alive(pid_t))
            cli_log_t = Path(summary_t["preserved_paths"]["cli_diagnostics_log"]).read_text(encoding="utf-8")
            self.assertNotIn("d" * 64, cli_log_t)
            self.assertIn("[REDACTED_TOKEN]", cli_log_t)

            # Case 2: Desktop exits non-zero; owned server is reaped and exit code is recorded.
            ready_tl = _write_exec(
                root / "ready_tl",
                """
                import os, signal, sys, time
                from pathlib import Path

                args = sys.argv[1:]
                cmd = args[0]
                state_dir = Path(args[args.index("--state-dir") + 1])
                if cmd == "serve":
                    (state_dir / "serve.pid").write_text(str(os.getpid()), encoding="utf-8")
                    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
                    while True:
                        time.sleep(0.02)
                if cmd == "list-bindings":
                    print("[]")
                    sys.exit(0)
                sys.exit(1)
                """,
            )
            failing_desk = _write_exec(root / "failing_desk", "raise SystemExit(3)\n")
            out_fail = root / "previews_fail"

            rc_f, out_f, err_f = _run_main([
                "--threadlane",
                str(ready_tl),
                "--desktop",
                str(failing_desk),
                "--output-root",
                str(out_fail),
            ])
            self.assertEqual((rc_f, err_f), (1, ""))
            summary_f = json.loads(out_f)
            self.assertEqual(
                (summary_f["passed"], summary_f["error_class"], summary_f["desktop_exit_code"]),
                (False, "desktop_exited_nonzero", 3),
            )
            pid_f = int((Path(summary_f["demo_state_dir"]) / "serve.pid").read_text(encoding="utf-8").strip())
            self.assertFalse(_pid_is_alive(pid_f))

    def test_reuses_live_worker_pilot_build_sanitized_child_env(self) -> None:
        self.assertIs(preview.build_sanitized_child_env, pilot.build_sanitized_child_env)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session_dir = preview.create_private_dir(root / "session")
            demo_state_dir = preview.create_private_dir(session_dir / "state")
            logs_dir = preview.create_private_dir(session_dir / "logs")
            poisoned_env = {
                "GIT_DIR": "/poison/git_dir",
                "GIT_WORK_TREE": "/poison/work_tree",
                "GIT_CONFIG_PARAMETERS": "'core.hooksPath=/poison/hooks'",
                "GIO_EXTRA_MODULES": "/nix/store/fake-glib-networking/lib/gio/modules",
                "XDG_DATA_DIRS": "/nix/store/fake-schemas/share",
            }
            expected_git_env = {
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_CONFIG_SYSTEM": os.devnull,
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_TERMINAL_PROMPT": "0",
            }

            with mock.patch.dict(os.environ, poisoned_env, clear=False):
                with mock.patch("desktop_preview.subprocess.run") as mock_run:
                    mock_run.return_value = subprocess.CompletedProcess(
                        args=["tl"], returncode=0, stdout="[]", stderr=""
                    )
                    res = preview.probe_supervisor_ready(
                        Path("/bin/threadlane"),
                        demo_state_dir,
                        logs_dir / "cli_diagnostics.log",
                    )
                    self.assertEqual(res, [])
                    run_env = mock_run.call_args.kwargs["env"]

                with mock.patch("desktop_preview.subprocess.Popen") as mock_popen:
                    fake_proc = mock.MagicMock()
                    fake_proc.poll.return_value = 0
                    mock_popen.return_value = fake_proc
                    with preview.supervised_demo_server(
                        Path("/bin/threadlane"), demo_state_dir, logs_dir, session_dir
                    ):
                        pass
                    popen_env = mock_popen.call_args.kwargs["env"]

            for env_map in (run_env, popen_env):
                git_subset = {k: v for k, v in env_map.items() if k.upper().startswith("GIT_")}
                self.assertEqual(git_subset, expected_git_env)
                self.assertEqual(
                    env_map.get("GIO_EXTRA_MODULES"),
                    "/nix/store/fake-glib-networking/lib/gio/modules",
                )
                self.assertEqual(env_map.get("XDG_DATA_DIRS"), "/nix/store/fake-schemas/share")


if __name__ == "__main__":
    unittest.main()
