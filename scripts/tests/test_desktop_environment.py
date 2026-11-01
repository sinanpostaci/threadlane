#!/usr/bin/env python3
"""Offline tests for `scripts/desktop-environment.sh`.

Each case sources the real helper in a controlled POSIX shell and passes a
temporary driver fixture path. No real GPU, desktop session, or network is used.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

_ENV_HELPER = Path(__file__).resolve().parents[1] / "desktop-environment.sh"
_SHELL = shutil.which("sh")
_UNSET = "__THREADLANE_TEST_UNSET__"

_RUN_HELPER = """
set -eu
. "$THREADLANE_TEST_ENV_HELPER"
threadlane_configure_desktop_environment "$THREADLANE_TEST_DRIVER_FILE"
printf '%s\\n' "${__NV_DISABLE_EXPLICIT_SYNC-__THREADLANE_TEST_UNSET__}"
printf '%s\\n' "${GDK_BACKEND-__THREADLANE_TEST_UNSET__}"
printf '%s\\n' "${WEBKIT_DISABLE_DMABUF_RENDERER-__THREADLANE_TEST_UNSET__}"
"""

_OUTPUT_KEYS = (
    "__NV_DISABLE_EXPLICIT_SYNC",
    "GDK_BACKEND",
    "WEBKIT_DISABLE_DMABUF_RENDERER",
)


@unittest.skipUnless(_SHELL, "a POSIX sh is required")
class DesktopEnvironmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.driver = self.root / "nvidia-version"
        self.driver.write_text("NVRM version: fixture\n", encoding="utf-8")
        self.runner = self.root / "run-helper.sh"
        self.runner.write_text(textwrap.dedent(_RUN_HELPER), encoding="utf-8")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, driver: Path | None = None, **environment: str) -> dict[str, str | None]:
        env = {
            "PATH": "/usr/bin:/bin",
            "LC_ALL": "C",
            "THREADLANE_TEST_ENV_HELPER": str(_ENV_HELPER),
            "THREADLANE_TEST_DRIVER_FILE": str(driver or self.driver),
        }
        env.update(environment)

        completed = subprocess.run(
            [_SHELL, str(self.runner)],
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(
            completed.returncode,
            0,
            msg=f"stdout={completed.stdout!r} stderr={completed.stderr!r}",
        )
        lines = completed.stdout.splitlines()
        self.assertEqual(len(lines), len(_OUTPUT_KEYS), msg=completed.stdout)
        return {
            key: None if value == _UNSET else value
            for key, value in zip(_OUTPUT_KEYS, lines)
        }

    def test_nvidia_wayland_sets_sync_default(self) -> None:
        result = self._run(WAYLAND_DISPLAY="wayland-0")

        self.assertEqual(result["__NV_DISABLE_EXPLICIT_SYNC"], "1")
        self.assertIsNone(result["GDK_BACKEND"])
        self.assertIsNone(result["WEBKIT_DISABLE_DMABUF_RENDERER"])

    def test_xdg_wayland_session_without_wayland_display_sets_default(self) -> None:
        result = self._run(XDG_SESSION_TYPE="wayland")

        self.assertEqual(result["__NV_DISABLE_EXPLICIT_SYNC"], "1")

    def test_non_nvidia_or_non_wayland_sessions_are_unchanged(self) -> None:
        cases = {
            "missing driver": (self.root / "missing-nvidia-version", {"WAYLAND_DISPLAY": "wayland-0"}),
            "x11 session": (self.driver, {"XDG_SESSION_TYPE": "x11"}),
            "no session": (self.driver, {}),
            "empty wayland display": (
                self.driver,
                {"WAYLAND_DISPLAY": "", "XDG_SESSION_TYPE": "x11"},
            ),
        }

        for label, (driver, environment) in cases.items():
            with self.subTest(label=label):
                result = self._run(driver=driver, **environment)
                self.assertIsNone(result["__NV_DISABLE_EXPLICIT_SYNC"])

    def test_explicit_x11_backend_is_unchanged(self) -> None:
        result = self._run(WAYLAND_DISPLAY="wayland-0", GDK_BACKEND="x11")

        self.assertIsNone(result["__NV_DISABLE_EXPLICIT_SYNC"])
        self.assertEqual(result["GDK_BACKEND"], "x11")
        self.assertIsNone(result["WEBKIT_DISABLE_DMABUF_RENDERER"])

    def test_non_x11_backend_still_receives_default(self) -> None:
        result = self._run(WAYLAND_DISPLAY="wayland-0", GDK_BACKEND="wayland")

        self.assertEqual(result["__NV_DISABLE_EXPLICIT_SYNC"], "1")
        self.assertEqual(result["GDK_BACKEND"], "wayland")

    def test_operator_values_are_preserved(self) -> None:
        for value in ("0", "", "1", "custom"):
            with self.subTest(value=value):
                result = self._run(
                    WAYLAND_DISPLAY="wayland-0",
                    __NV_DISABLE_EXPLICIT_SYNC=value,
                )
                self.assertEqual(result["__NV_DISABLE_EXPLICIT_SYNC"], value)

    def test_unreadable_driver_is_ignored(self) -> None:
        if os.name == "posix" and hasattr(os, "geteuid") and os.geteuid() == 0:
            self.skipTest("root can read a mode-000 fixture")
        self.driver.chmod(0o000)
        try:
            if os.access(self.driver, os.R_OK):
                self.skipTest("the filesystem does not enforce mode-000 fixtures")
            result = self._run(WAYLAND_DISPLAY="wayland-0")
        finally:
            self.driver.chmod(0o600)

        self.assertIsNone(result["__NV_DISABLE_EXPLICIT_SYNC"])

    def test_non_file_driver_path_is_ignored(self) -> None:
        directory = self.root / "driver-directory"
        directory.mkdir()

        result = self._run(driver=directory, WAYLAND_DISPLAY="wayland-0")

        self.assertIsNone(result["__NV_DISABLE_EXPLICIT_SYNC"])


if __name__ == "__main__":
    unittest.main()
