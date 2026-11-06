#!/usr/bin/env python3
"""Fast offline regressions for ``scripts/run-ci-check.py``.

The fixtures are local Python processes only. They do not use a shell, a
provider, the network, or a system service. Any fixture descendant is recorded
and terminated during cleanup.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import signal
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

_SCRIPT = Path(__file__).resolve().with_name("run-ci-check.py")
_SPEC = importlib.util.spec_from_file_location("threadlane_run_ci_check", _SCRIPT)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load the CI wrapper from {_SCRIPT}")
run_ci_check = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = run_ci_check
_SPEC.loader.exec_module(run_ci_check)


def _run_raw(argv: list[str], **kwargs) -> tuple[int, str, str]:
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        rc = run_ci_check.main(argv, **kwargs)
    return rc, stdout.getvalue(), stderr.getvalue()


def _run(
    argv: list[str],
    *,
    timeout: float = 5.0,
    heartbeat: float = 0.1,
    terminate_grace: float = 0.2,
    kill_grace: float = 0.2,
    drain: float = 0.2,
) -> tuple[int, str, str]:
    return _run_raw(
        ["--timeout-seconds", str(timeout), "--", *argv],
        heartbeat_seconds=heartbeat,
        terminate_grace_seconds=terminate_grace,
        kill_grace_seconds=kill_grace,
        post_exit_drain_seconds=drain,
    )


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_pid_gone(pid: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.02)
    return not _pid_alive(pid)


def _terminate_pid(pid: int) -> None:
    if not _pid_alive(pid):
        return
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGTERM)
    if _wait_pid_gone(pid, 0.5):
        return
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)
    _wait_pid_gone(pid, 0.5)


@unittest.skipUnless(os.name == "posix", "run-ci-check.py is POSIX-only")
class RunCiCheckTests(unittest.TestCase):
    def test_argv_with_spaces_is_passed_without_shell_splitting(self) -> None:
        code = (
            "import sys\n"
            "print(repr(sys.argv[1]))\n"
            "print(repr(sys.argv[2]), file=sys.stderr)\n"
        )
        rc, stdout, stderr = _run(
            [sys.executable, "-c", code, "left right", "err value"]
        )

        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "'left right'\n")
        self.assertIn("'err value'", stderr)

        with tempfile.TemporaryDirectory(prefix="run-ci-check ") as tmp:
            script = Path(tmp) / "fixture tool.py"
            script.write_text(
                "import sys\nprint(sys.argv[1])\n",
                encoding="utf-8",
            )
            rc, stdout, _ = _run([sys.executable, str(script), "path value"])

        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "path value\n")

    def test_streams_output_and_preserves_normal_and_failing_exit_codes(self) -> None:
        success = "import sys; print('normal out'); print('normal err', file=sys.stderr)"
        rc, stdout, stderr = _run([sys.executable, "-c", success])
        self.assertEqual(rc, 0)
        self.assertIn("normal out\n", stdout)
        self.assertIn("normal err\n", stderr)

        failure = "import sys; print('failure out'); sys.exit(7)"
        rc, stdout, _ = _run([sys.executable, "-c", failure])
        self.assertEqual(rc, 7)
        self.assertIn("failure out\n", stdout)

    def test_early_pipe_close_preserves_completion_exit_codes(self) -> None:
        success = "import os, time; os.close(1); os.close(2); time.sleep(0.05)"
        failure = (
            "import os, time; os.close(1); os.close(2); "
            "time.sleep(0.05); os._exit(7)"
        )
        for label, fixture, expected in (
            ("success", success, 0),
            ("failure", failure, 7),
        ):
            with self.subTest(label=label):
                start = time.monotonic()
                rc, stdout, stderr = _run(
                    [sys.executable, "-c", fixture],
                    timeout=0.3,
                    heartbeat=1.0,
                    terminate_grace=0.1,
                    kill_grace=0.1,
                    drain=0.1,
                )
                elapsed = time.monotonic() - start

                self.assertEqual(rc, expected, msg=stderr)
                self.assertEqual(stdout, "")
                self.assertNotIn("exceeded", stderr)
                self.assertLess(elapsed, 2.0, msg=stderr)

    def test_long_running_child_after_early_pipe_close_still_times_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            pid_file = Path(tmp) / "child.pid"
            code = textwrap.dedent(
                f"""
                import os, time
                from pathlib import Path

                Path({str(pid_file)!r}).write_text(str(os.getpid()))
                os.close(1)
                os.close(2)
                time.sleep(5)
                """
            )
            start = time.monotonic()
            rc, stdout, stderr = _run(
                [sys.executable, "-c", code],
                timeout=0.3,
                heartbeat=1.0,
                terminate_grace=0.1,
                kill_grace=0.1,
                drain=0.1,
            )
            elapsed = time.monotonic() - start

            child_pid: int | None = None
            try:
                self.assertEqual(rc, run_ci_check.EXIT_TIMEOUT, msg=stderr)
                self.assertEqual(stdout, "")
                self.assertIn("exceeded", stderr)
                self.assertLess(elapsed, 3.0, msg=stderr)
                child_pid = int(pid_file.read_text(encoding="utf-8"))
                self.assertTrue(_wait_pid_gone(child_pid, 1.0), msg=stderr)
            finally:
                if child_pid is None and pid_file.is_file():
                    child_pid = int(pid_file.read_text(encoding="utf-8"))
                if child_pid is not None:
                    _terminate_pid(child_pid)

    def test_quiet_success_emits_heartbeats_without_cancelling(self) -> None:
        code = "import time; time.sleep(0.6)"
        rc, stdout, stderr = _run([sys.executable, "-c", code], heartbeat=0.15)

        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "")
        beats = [
            line
            for line in stderr.splitlines()
            if "[run-ci-check] heartbeat" in line
        ]
        self.assertGreaterEqual(len(beats), 2, msg=stderr)
        for line in beats:
            self.assertIn("elapsed=", line)
            self.assertIn("hint=", line)
        self.assertIn("waiting for libtest/build output", beats[0])

    def test_heartbeat_reports_the_last_libtest_hint(self) -> None:
        code = (
            "import time\n"
            "print('running 3 tests', flush=True)\n"
            "print('test demo ... ok', flush=True)\n"
            "time.sleep(0.5)\n"
        )
        rc, _, stderr = _run([sys.executable, "-c", code], heartbeat=0.1)

        self.assertEqual(rc, 0)
        beats = [
            line
            for line in stderr.splitlines()
            if "[run-ci-check] heartbeat" in line
        ]
        self.assertTrue(
            any("hint=test demo ... ok" in line for line in beats),
            msg=stderr,
        )

    def test_hint_tracker_keeps_libtest_and_build_phase_lines(self) -> None:
        tracker = run_ci_check._HintTracker()
        tracker.feed_line(b"Compiling demo v0.1.0")
        self.assertIn("Compiling demo", tracker.hint)
        tracker.feed_line(b"unrelated chatter")
        self.assertIn("Compiling demo", tracker.hint)
        tracker.feed_line(b"running 2 tests")
        self.assertIn("running 2 tests", tracker.hint)

    def test_partial_and_large_output_are_streamed_with_bounded_line_memory(self) -> None:
        partial = "import sys; sys.stdout.write('partial-without-newline'); sys.stdout.flush()"
        rc, stdout, _ = _run([sys.executable, "-c", partial])
        self.assertEqual(rc, 0)
        self.assertEqual(stdout, "partial-without-newline")

        large = "import sys; sys.stdout.write('x' * 262144); sys.stdout.flush()"
        rc, stdout, _ = _run([sys.executable, "-c", large])
        self.assertEqual(rc, 0)
        self.assertEqual(len(stdout), 262144)

        buffer = run_ci_check._LineBuffer(max_bytes=32)
        self.assertEqual(buffer.feed(b"y" * 4096), [])
        self.assertLessEqual(buffer.pending_bytes, 32)
        lines = buffer.feed(b"\n")
        self.assertEqual(len(lines), 1)
        self.assertLessEqual(len(lines[0]), 32)

    def test_deadline_ignores_quiet_output_and_escalates_to_kill(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "term-seen.txt"
            pid_file = root / "child.pid"
            code = textwrap.dedent(
                f"""
                import os, signal, time
                from pathlib import Path

                Path({str(pid_file)!r}).write_text(str(os.getpid()))

                def handle(signum, frame):
                    Path({str(marker)!r}).write_text("term\\n")

                signal.signal(signal.SIGTERM, handle)
                print("ready", flush=True)
                while True:
                    time.sleep(0.05)
                """
            )
            start = time.monotonic()
            rc, _, stderr = _run(
                [sys.executable, "-c", code],
                timeout=1.0,
                heartbeat=0.1,
                terminate_grace=0.2,
                kill_grace=0.5,
            )
            elapsed = time.monotonic() - start

            child_pid: int | None = None
            try:
                self.assertEqual(rc, run_ci_check.EXIT_TIMEOUT)
                self.assertLess(elapsed, 3.0, msg=stderr)
                self.assertTrue(marker.is_file(), msg=stderr)
                self.assertIn("sent SIGTERM", stderr)
                self.assertIn("sent SIGKILL", stderr)
                self.assertLess(
                    stderr.index("sent SIGTERM"),
                    stderr.index("sent SIGKILL"),
                )
                child_pid = int(pid_file.read_text(encoding="utf-8"))
                self.assertTrue(_wait_pid_gone(child_pid, 1.0), msg=stderr)
            finally:
                if child_pid is None and pid_file.is_file():
                    child_pid = int(pid_file.read_text(encoding="utf-8"))
                if child_pid is not None:
                    _terminate_pid(child_pid)

    def test_inherited_pipe_drain_is_bounded_and_reports_incomplete_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pid_file = root / "holder.pid"
            code = textwrap.dedent(
                f"""
                import subprocess, sys
                from pathlib import Path

                holder = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(2)"]
                )
                Path({str(pid_file)!r}).write_text(str(holder.pid))
                sys.exit(0)
                """
            )
            start = time.monotonic()
            rc, _, stderr = _run(
                [sys.executable, "-c", code],
                timeout=10.0,
                heartbeat=0.1,
                drain=0.25,
            )
            elapsed = time.monotonic() - start

            holder_pid: int | None = None
            try:
                self.assertEqual(rc, run_ci_check.EXIT_CLEANUP_INCOMPLETE)
                self.assertLess(elapsed, 2.0, msg=stderr)
                self.assertIn("cleanup incomplete", stderr)
                self.assertIn("without signalling descendants", stderr)
                holder_pid = int(pid_file.read_text(encoding="utf-8"))
                self.assertTrue(_pid_alive(holder_pid), msg=stderr)
            finally:
                if holder_pid is None and pid_file.is_file():
                    holder_pid = int(pid_file.read_text(encoding="utf-8"))
                if holder_pid is not None:
                    _terminate_pid(holder_pid)

    def test_refuses_non_posix_hosts(self) -> None:
        with mock.patch.object(run_ci_check.os, "name", "nt"):
            rc, _, stderr = _run_raw(
                ["--timeout-seconds", "1", "--", sys.executable, "-c", "pass"]
            )

        self.assertEqual(rc, 2)
        self.assertIn("POSIX", stderr)

    def test_rejects_invalid_timeouts_and_missing_separator(self) -> None:
        for value in ("0", "-1", "nan", "inf", "7201"):
            with self.subTest(timeout=value):
                rc, _, stderr = _run_raw(
                    [
                        "--timeout-seconds",
                        value,
                        "--",
                        sys.executable,
                        "-c",
                        "pass",
                    ]
                )
                self.assertEqual(rc, 2)
                self.assertIn("--timeout-seconds", stderr)

        rc, _, stderr = _run_raw(["--timeout-seconds", "1", sys.executable])
        self.assertEqual(rc, 2)
        self.assertIn("--", stderr)

        rc, _, stderr = _run_raw(["--timeout-seconds", "1", "--"])
        self.assertEqual(rc, 2)
        self.assertIn("missing command", stderr)


if __name__ == "__main__":
    unittest.main()
