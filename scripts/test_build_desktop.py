#!/usr/bin/env python3
"""Offline regression tests for `scripts/build-desktop.py`.

The tests use tiny Python fixture executables and temporary checkout layouts.
They do not invoke npm, cargo, Tauri, a window, or any network service.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

_BUILD_SCRIPT = Path(__file__).resolve().with_name("build-desktop.py")
_BUILD_SPEC = importlib.util.spec_from_file_location("threadlane_build_desktop", _BUILD_SCRIPT)
if _BUILD_SPEC is None or _BUILD_SPEC.loader is None:
    raise ImportError(f"Unable to load the desktop build script from {_BUILD_SCRIPT}")
build = importlib.util.module_from_spec(_BUILD_SPEC)
sys.modules[_BUILD_SPEC.name] = build
_BUILD_SPEC.loader.exec_module(build)


def _write_tool(path: Path, body: str) -> Path:
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
    path.chmod(0o700)
    return path


def _run_main(argv: list[str], repo_root: Path) -> tuple[int, str, str]:
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        rc = build.main(argv, repo_root=repo_root)
    return rc, stdout.getvalue(), stderr.getvalue()


def _read_records(path: Path) -> list[dict[str, object]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _artifact(root: Path, profile: str) -> Path:
    return root / "target" / "desktop" / profile / f"threadlane-desktop{build._EXE_SUFFIX}"


def _make_checkout(root: Path) -> None:
    desktop = root / "apps" / "desktop"
    src_tauri = desktop / "src-tauri"
    src_tauri.mkdir(parents=True, exist_ok=True)
    (root / "Cargo.toml").write_text("[workspace]\nmembers = []\n", encoding="utf-8")
    (root / "Cargo.lock").write_text("# fixture lock\n", encoding="utf-8")
    (desktop / "package.json").write_text('{"name":"fixture"}\n', encoding="utf-8")
    (desktop / "package-lock.json").write_text('{"lockfileVersion":3}\n', encoding="utf-8")
    (src_tauri / "Cargo.toml").write_text(
        '[workspace]\n\n[package]\nname = "fixture-desktop"\nversion = "0.1.0"\n',
        encoding="utf-8",
    )
    (src_tauri / "Cargo.lock").write_text("# fixture desktop lock\n", encoding="utf-8")


_NPM_SUCCESS = """
import json, os, sys
from pathlib import Path

args = sys.argv[1:]
log = Path(os.environ["FAKE_TOOL_LOG"])
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "npm", "argv": args, "cwd": os.getcwd()}) + "\\n")

cwd = Path.cwd()
if args == ["ci"]:
    (cwd / "node_modules").mkdir(exist_ok=True)
    (cwd / "node_modules" / "ready").write_text("ready\\n", encoding="utf-8")
    sys.exit(0)
if args == ["run", "build"]:
    dist = cwd / "dist"
    dist.mkdir(exist_ok=True)
    (dist / "index.html").write_text("<html></html>\\n", encoding="utf-8")
    sys.exit(0)
sys.exit(7)
"""

_NPM_FAILURE = """
import sys

sys.stderr.write("x" * 100000 + "\\nBearer " + "a" * 64 + "\\n")
sys.exit(9)
"""

_NPM_TIMEOUT = """
import time

time.sleep(30)
"""

_CARGO_SUCCESS = """
import json, os, sys
from pathlib import Path

args = sys.argv[1:]
keys = [
    "CARGO_TARGET_DIR",
    "CARGO_BUILD_TARGET",
    "GIT_DIR",
    "GIT_CONFIG_NOSYSTEM",
    "TAURI_SIGNING_PRIVATE_KEY",
    "TAURI_SIGNING_PRIVATE_KEY_PASSWORD",
]
log = Path(os.environ["FAKE_TOOL_LOG"])
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({
        "tool": "cargo",
        "argv": args,
        "cwd": os.getcwd(),
        "env": {key: os.environ.get(key) for key in keys},
    }) + "\\n")

target = Path(os.environ["CARGO_TARGET_DIR"])
profile = "release" if "--release" in args else "debug"
suffix = os.environ.get("FAKE_EXE_SUFFIX", "")
artifact = target / profile / f"threadlane-desktop{suffix}"
artifact.parent.mkdir(parents=True, exist_ok=True)
artifact.write_text("fixture binary\\n", encoding="utf-8")
if os.name != "nt":
    artifact.chmod(0o700)
sys.exit(0)
"""

_CARGO_MISSING_ARTIFACT = """
import json, os, sys
from pathlib import Path

args = sys.argv[1:]
log = Path(os.environ["FAKE_TOOL_LOG"])
with log.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps({"tool": "cargo", "argv": args, "cwd": os.getcwd()}) + "\\n")
sys.exit(0)
"""


class DesktopBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        _make_checkout(self.root)
        self.log = self.root / "tool-log.jsonl"
        self.npm = _write_tool(self.root / "fake_npm.py", _NPM_SUCCESS)
        self.cargo = _write_tool(self.root / "fake_cargo.py", _CARGO_SUCCESS)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    @contextlib.contextmanager
    def _tools(self, npm: Path | None = None, cargo: Path | None = None):
        npm_path = npm or self.npm
        cargo_path = cargo or self.cargo
        with mock.patch.dict(
            os.environ,
            {"FAKE_TOOL_LOG": str(self.log), "FAKE_EXE_SUFFIX": build._EXE_SUFFIX},
            clear=False,
        ):
            with mock.patch.object(
                build,
                "_npm_argv",
                lambda args: [sys.executable, str(npm_path), *args],
            ):
                with mock.patch.object(
                    build,
                    "_cargo_argv",
                    lambda args: [sys.executable, str(cargo_path), *args],
                ):
                    yield

    def test_dry_run_has_no_side_effects_and_resolves_repo_paths(self) -> None:
        rc, stdout, stderr = _run_main(["--dry-run", "--release"], self.root)

        self.assertEqual((rc, stderr), (0, ""))
        self.assertFalse(self.log.exists())
        self.assertFalse((self.root / "target").exists())
        self.assertFalse((self.root / "apps" / "desktop" / "node_modules").exists())
        self.assertFalse((self.root / "apps" / "desktop" / "dist").exists())

        plan = json.loads(stdout)
        self.assertEqual(plan["mode"], "dry_run")
        self.assertFalse(plan["live_execution"])
        self.assertFalse(plan["uses_shell"])
        self.assertEqual(plan["repo_root"], str(self.root))
        self.assertEqual(
            plan["manifest_path"],
            str(self.root / "apps" / "desktop" / "src-tauri" / "Cargo.toml"),
        )
        self.assertEqual(plan["cargo_target_dir"], str(self.root / "target" / "desktop"))
        self.assertEqual(plan["profile"], "release")
        self.assertEqual(plan["artifact"], str(_artifact(self.root, "release")))

        commands = plan["commands"]
        self.assertEqual([item["label"] for item in commands], [
            "frontend_install",
            "frontend_build",
            "desktop_build",
        ])
        self.assertEqual(commands[0]["argv"], ["npm", "ci"])
        self.assertEqual(commands[0]["cwd"], str(self.root / "apps" / "desktop"))
        self.assertEqual(commands[1]["argv"], ["npm", "run", "build"])
        self.assertEqual(commands[2]["argv"][0], "cargo")
        self.assertIn("--release", commands[2]["argv"])
        self.assertEqual(commands[2]["env"], {"CARGO_TARGET_DIR": str(self.root / "target" / "desktop")})

    def test_release_build_runs_argv_only_steps_and_creates_expected_artifact(self) -> None:
        with self._tools():
            rc, stdout, stderr = _run_main(["--release", "--timeout-seconds", "30"], self.root)

        self.assertEqual((rc, stderr), (0, ""), msg=stdout)
        summary = json.loads(stdout)
        self.assertTrue(summary["passed"])
        self.assertFalse(summary["uses_shell"])
        artifact = _artifact(self.root, "release")
        self.assertEqual(Path(summary["artifact"]), artifact)
        self.assertTrue(artifact.is_file())
        self.assertTrue((self.root / "apps" / "desktop" / "dist" / "index.html").is_file())

        records = _read_records(self.log)
        self.assertEqual([record["tool"] for record in records], ["npm", "npm", "cargo"])
        npm_records = [record for record in records if record["tool"] == "npm"]
        cargo_record = next(record for record in records if record["tool"] == "cargo")

        self.assertEqual([record["argv"] for record in npm_records], [["ci"], ["run", "build"]])
        for record in npm_records:
            self.assertEqual(record["cwd"], str(self.root / "apps" / "desktop"))
        self.assertEqual(cargo_record["cwd"], str(self.root))
        self.assertEqual(cargo_record["argv"][0], "build")
        self.assertIn("--release", cargo_record["argv"])
        self.assertIn(str(self.root / "apps" / "desktop" / "src-tauri" / "Cargo.toml"), cargo_record["argv"])
        self.assertEqual(
            cargo_record["env"]["CARGO_TARGET_DIR"],
            str(self.root / "target" / "desktop"),
        )

    def test_default_profile_is_debug(self) -> None:
        with self._tools():
            rc, stdout, stderr = _run_main([], self.root)

        self.assertEqual((rc, stderr), (0, ""), msg=stdout)
        self.assertEqual(json.loads(stdout)["profile"], "debug")
        self.assertTrue(_artifact(self.root, "debug").is_file())

    def test_sanitizes_git_and_signing_environment_for_build_children(self) -> None:
        poisoned = {
            "GIT_DIR": "/poison/git_dir",
            "GIT_CONFIG_PARAMETERS": "'core.hooksPath=/poison/hooks'",
            "CARGO_BUILD_TARGET": "wasm32-unknown-unknown",
            "TAURI_SIGNING_PRIVATE_KEY": "do-not-pass",
            "TAURI_SIGNING_PRIVATE_KEY_PASSWORD": "do-not-pass",
        }
        with mock.patch.dict(os.environ, poisoned, clear=False):
            with self._tools():
                rc, stdout, stderr = _run_main([], self.root)

        self.assertEqual((rc, stderr), (0, ""), msg=stdout)
        records = _read_records(self.log)
        cargo_record = next(record for record in records if record["tool"] == "cargo")
        env_record = cargo_record["env"]
        self.assertIsNone(env_record["GIT_DIR"])
        self.assertIsNone(env_record["CARGO_BUILD_TARGET"])
        self.assertIsNone(env_record["TAURI_SIGNING_PRIVATE_KEY"])
        self.assertIsNone(env_record["TAURI_SIGNING_PRIVATE_KEY_PASSWORD"])
        self.assertEqual(env_record["GIT_CONFIG_NOSYSTEM"], "1")
        self.assertEqual(env_record["CARGO_TARGET_DIR"], str(self.root / "target" / "desktop"))

    def test_failed_frontend_install_is_redacted_and_bounded(self) -> None:
        failing_npm = _write_tool(self.root / "fake_npm_failure.py", _NPM_FAILURE)
        with self._tools(npm=failing_npm):
            rc, stdout, stderr = _run_main([], self.root)

        self.assertEqual(rc, 1)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(payload["error_class"], "frontend_install_failed")
        self.assertFalse(payload["passed"])
        self.assertIn("[REDACTED_TOKEN]", payload["message"])
        self.assertNotIn("a" * 64, payload["message"])
        self.assertLessEqual(len(payload["message"]), build.MAX_DIAGNOSTIC_MESSAGE_CHARS)
        self.assertFalse(_artifact(self.root, "debug").exists())

    def test_timeout_terminates_the_owned_build_process(self) -> None:
        slow_npm = _write_tool(self.root / "fake_npm_timeout.py", _NPM_TIMEOUT)
        started = time.monotonic()
        with self._tools(npm=slow_npm):
            rc, stdout, stderr = _run_main(["--timeout-seconds", "0.2"], self.root)
        elapsed = time.monotonic() - started

        self.assertEqual(rc, 1)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(payload["error_class"], "frontend_install_timeout")
        self.assertLess(elapsed, 10.0)
        self.assertFalse(_artifact(self.root, "debug").exists())

    def test_successful_cargo_without_artifact_is_an_error(self) -> None:
        cargo_missing = _write_tool(self.root / "fake_cargo_missing.py", _CARGO_MISSING_ARTIFACT)
        with self._tools(cargo=cargo_missing):
            rc, stdout, stderr = _run_main([], self.root)

        self.assertEqual(rc, 1)
        self.assertEqual(stdout, "")
        payload = json.loads(stderr)
        self.assertEqual(payload["error_class"], "desktop_artifact_missing")
        self.assertIn("threadlane-desktop", payload["message"])

    def test_missing_lockfile_and_invalid_arguments_are_classified(self) -> None:
        (self.root / "apps" / "desktop" / "src-tauri" / "Cargo.lock").unlink()
        rc, stdout, stderr = _run_main(["--dry-run"], self.root)
        self.assertEqual((rc, stdout), (2, ""))
        self.assertEqual(json.loads(stderr)["error_class"], "desktop_cargo_lock_missing")

        _make_checkout(self.root)
        rc, stdout, stderr = _run_main(["--unknown"], self.root)
        self.assertEqual((rc, stdout), (2, ""))
        self.assertEqual(json.loads(stderr)["error_class"], "invalid_cli_arguments")

        for timeout in ("0", "999999"):
            with self.subTest(timeout=timeout):
                rc, stdout, stderr = _run_main(["--timeout-seconds", timeout], self.root)
                self.assertEqual((rc, stdout), (2, ""))
                self.assertEqual(json.loads(stderr)["error_class"], "invalid_timeout_seconds")

    def test_missing_tool_is_reported_without_running_build_steps(self) -> None:
        with mock.patch.object(build.shutil, "which", return_value=None):
            rc, stdout, stderr = _run_main([], self.root)

        self.assertEqual((rc, stdout), (2, ""))
        self.assertEqual(json.loads(stderr)["error_class"], "npm_not_found")
        self.assertFalse(self.log.exists())

    def test_help_is_side_effect_free(self) -> None:
        rc, stdout, stderr = _run_main(["--help"], self.root)
        self.assertEqual((rc, stderr), (0, ""))
        self.assertIn("usage:", stdout)
        self.assertFalse(self.log.exists())
        self.assertFalse((self.root / "target").exists())


if __name__ == "__main__":
    unittest.main()
