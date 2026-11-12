#!/usr/bin/env python3
"""Offline unit tests for scripts/desktop-acceptance.py."""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


def _load_desktop_acceptance_module():
    script_path = Path(__file__).resolve().with_name("desktop-acceptance.py")
    spec = importlib.util.spec_from_file_location("desktop_acceptance", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


da = _load_desktop_acceptance_module()


def _init_fake_repo(root: Path, *, gitignore_dot_local: bool = True, create_dot_local: bool = True) -> Path:
    (root / "apps" / "desktop").mkdir(parents=True, exist_ok=True)
    (root / "nix").mkdir(parents=True, exist_ok=True)
    (root / "apps" / "desktop" / "index.html").write_text("<!doctype html>\n", encoding="utf-8")
    (root / "apps" / "desktop" / "playwright.config.ts").write_text("export default {};\n", encoding="utf-8")
    (root / "nix" / "desktop-acceptance.nix").write_text("{ }:\n{}\n", encoding="utf-8")
    (root / "nix" / "packages.nix").write_text("{ }:\n{}\n", encoding="utf-8")

    gitignore_lines = ["/target/\n"]
    if gitignore_dot_local:
        gitignore_lines.append("/.local/\n")
    (root / ".gitignore").write_text("".join(gitignore_lines), encoding="utf-8")

    subprocess.run(["git", "init", "-q"], cwd=str(root), check=True)
    subprocess.run(["git", "add", "."], cwd=str(root), check=True)

    if create_dot_local:
        dot_local = root / ".local"
        dot_local.mkdir(mode=0o700)
        dot_local.chmod(0o700)
    return root


class DesktopAcceptanceArtifactGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name).resolve()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_accepts_preexisting_git_ignored_dot_local(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=True)
        validated = da.validate_artifact_directory(repo, None)
        self.assertTrue(validated.inside_repo)
        self.assertEqual(validated.artifact_dir, repo / ".local" / "desktop-acceptance")

    def test_rejects_missing_dot_local_for_in_repo_artifacts(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=False)
        with self.assertRaisesRegex(da.AcceptanceError, "must pre-exist"):
            da.validate_artifact_directory(repo, None)

    def test_rejects_unignored_dot_local(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=False, create_dot_local=True)
        with self.assertRaisesRegex(da.AcceptanceError, "not git-ignored"):
            da.validate_artifact_directory(repo, None)

    def test_rejects_tracked_in_repo_paths_outside_dot_local(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=True)
        for bad in (
            repo / "apps" / "desktop" / "test-results",
            repo / "apps" / "desktop" / "playwright-report",
            repo / "docs" / "artifacts",
            repo,
        ):
            with self.assertRaises(da.AcceptanceError):
                da.validate_artifact_directory(repo, bad)

    def test_rejects_symlinked_dot_local_and_relative_paths(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=False)
        outside = self.base / "outside"
        outside.mkdir()
        (repo / ".local").symlink_to(outside)

        with self.assertRaisesRegex(da.AcceptanceError, "symlink"):
            da.validate_artifact_directory(repo, None)

        with self.assertRaisesRegex(da.AcceptanceError, "absolute"):
            da.validate_safe_absolute_path("relative/artifacts", "artifact_dir")

        with self.assertRaisesRegex(da.AcceptanceError, "start with '-'"):
            da.validate_safe_absolute_path("-bad-flag", "artifact_dir")

    def test_accepts_dedicated_external_dir_and_rejects_other_unignored_git_repo(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=False)
        external = self.base / "external-artifacts" / "run-1"
        validated = da.validate_artifact_directory(repo, external)
        self.assertFalse(validated.inside_repo)
        self.assertEqual(validated.artifact_dir, external)

        other_repo = _init_fake_repo(
            self.base / "other-repo",
            gitignore_dot_local=True,
            create_dot_local=False,
        )
        with self.assertRaisesRegex(da.AcceptanceError, "another non-ignored Git worktree"):
            da.validate_artifact_directory(repo, other_repo / "tracked-subdir")

    def _run_prepare_stage_cli(
        self,
        env_overrides: dict[str, str],
        *,
        cwd: Path | None = None,
    ) -> tuple[int, str, str]:
        merged_env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            **env_overrides,
        }
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        cwd_ctx = (
            mock.patch.object(Path, "cwd", return_value=cwd)
            if cwd is not None
            else contextlib.nullcontext()
        )
        with (
            mock.patch.dict(os.environ, merged_env, clear=True),
            cwd_ctx,
            mock.patch("sys.stdout", stdout_buf),
            mock.patch("sys.stderr", stderr_buf),
        ):
            rc = da.main(["--prepare-playwright-stage"])
        return rc, stdout_buf.getvalue().strip(), stderr_buf.getvalue().strip()

    def test_playwright_staging_allocates_fresh_private_child_and_preserves_preexisting_entries(
        self,
    ) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=True)
        outside_targets = self.base / "outside-targets"
        (outside_targets / "linked-dir").mkdir(parents=True)
        dir_target_file = outside_targets / "linked-dir" / "precious-dir-file.txt"
        dir_target_file.write_text("do-not-delete-dir-target\n", encoding="utf-8")
        file_target = outside_targets / "precious-file.txt"
        file_target.write_text("do-not-delete-file-target\n", encoding="utf-8")

        artifact_root = self.base / "external-artifacts" / "browser"
        legacy_stage = artifact_root / "staging-desktop"
        (legacy_stage / "nested").mkdir(parents=True)
        (legacy_stage / "keep.txt").write_text("preexisting-staging-file\n", encoding="utf-8")
        (legacy_stage / "nested" / "state.json").write_text('{"keep": true}\n', encoding="utf-8")
        (legacy_stage / "sym-dir").symlink_to(outside_targets / "linked-dir")
        (legacy_stage / "sym-file").symlink_to(file_target)
        (artifact_root / "existing-note.txt").write_text("keep-root-file\n", encoding="utf-8")
        (artifact_root / "root-link").symlink_to(file_target)

        base_env = {
            "THREADLANE_ACCEPTANCE_ARTIFACT_DIR": str(artifact_root),
            "THREADLANE_ACCEPTANCE_REPO_ROOT": str(repo),
            "THREADLANE_DESKTOP_SOURCE_DIR": str(repo / "apps" / "desktop"),
        }
        rc1, out1, err1 = self._run_prepare_stage_cli(base_env, cwd=repo)
        self.assertEqual(rc1, 0, err1)
        rc2, out2, err2 = self._run_prepare_stage_cli(base_env, cwd=repo)
        self.assertEqual(rc2, 0, err2)

        stage_one = Path(out1)
        stage_two = Path(out2)
        self.assertNotEqual(stage_one, stage_two)
        self.assertEqual(stage_one.parent, artifact_root)
        self.assertEqual(stage_two.parent, artifact_root)

        for created_dir in (
            stage_one,
            stage_one / "node_modules",
            stage_two,
            stage_two / "node_modules",
        ):
            st = os.lstat(created_dir)
            self.assertTrue(stat.S_ISDIR(st.st_mode))
            self.assertFalse(stat.S_ISLNK(st.st_mode))
            self.assertEqual(stat.S_IMODE(st.st_mode), 0o700)

        custom_child = artifact_root / "owned" / "stage-explicit"
        rc3, out3, err3 = self._run_prepare_stage_cli(
            {**base_env, "THREADLANE_ACCEPTANCE_STAGING_DIR": str(custom_child)},
            cwd=repo,
        )
        self.assertEqual(rc3, 0, err3)
        self.assertEqual(Path(out3), custom_child)
        self.assertEqual(stat.S_IMODE(os.lstat(custom_child).st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(os.lstat(custom_child / "node_modules").st_mode), 0o700)

        self.assertEqual(
            (legacy_stage / "keep.txt").read_text(encoding="utf-8"),
            "preexisting-staging-file\n",
        )
        self.assertEqual(
            (legacy_stage / "nested" / "state.json").read_text(encoding="utf-8"),
            '{"keep": true}\n',
        )
        self.assertTrue((legacy_stage / "sym-dir").is_symlink())
        self.assertEqual(os.readlink(legacy_stage / "sym-dir"), str(outside_targets / "linked-dir"))
        self.assertTrue((legacy_stage / "sym-file").is_symlink())
        self.assertEqual(os.readlink(legacy_stage / "sym-file"), str(file_target))
        self.assertEqual(
            (artifact_root / "existing-note.txt").read_text(encoding="utf-8"),
            "keep-root-file\n",
        )
        self.assertTrue((artifact_root / "root-link").is_symlink())
        self.assertEqual(os.readlink(artifact_root / "root-link"), str(file_target))
        self.assertEqual(
            dir_target_file.read_text(encoding="utf-8"),
            "do-not-delete-dir-target\n",
        )
        self.assertEqual(
            file_target.read_text(encoding="utf-8"),
            "do-not-delete-file-target\n",
        )

    def test_playwright_staging_rejects_unsafe_or_preexisting_overrides_without_deleting_anything(
        self,
    ) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=True)
        caller_user_dir = self.base / "caller-user-files"
        (caller_user_dir / "subdir").mkdir(parents=True)
        user_doc = caller_user_dir / "subdir" / "important.txt"
        user_doc.write_text("user-owned-data\n", encoding="utf-8")

        external_target_dir = self.base / "external-target"
        external_target_dir.mkdir()
        external_sym_target = external_target_dir / "secret.txt"
        external_sym_target.write_text("symlink-target-data\n", encoding="utf-8")
        (caller_user_dir / "target-link").symlink_to(external_sym_target)

        artifact_root = repo / ".local" / "desktop-acceptance" / "browser"
        preexisting_stage = artifact_root / "staging-desktop"
        preexisting_stage.mkdir(parents=True, mode=0o700)
        (preexisting_stage / "inside.txt").write_text("keep-inside\n", encoding="utf-8")
        (preexisting_stage / "ext-link").symlink_to(external_sym_target)

        preexisting_file = artifact_root / "occupied-file"
        preexisting_file.write_text("keep-file\n", encoding="utf-8")
        preexisting_symlink = artifact_root / "occupied-symlink"
        preexisting_symlink.symlink_to(caller_user_dir)
        broken_symlink = artifact_root / "broken-symlink"
        broken_symlink.symlink_to(self.base / "nonexistent-target")

        store_like_desktop = self.base / "nix-store-desktop"
        store_like_desktop.mkdir()
        (store_like_desktop / "index.html").write_text("<!doctype html>\n", encoding="utf-8")
        (store_like_desktop / "playwright.config.ts").write_text(
            "export default {};\n",
            encoding="utf-8",
        )

        base_env = {
            "THREADLANE_ACCEPTANCE_ARTIFACT_DIR": str(artifact_root),
            "THREADLANE_DESKTOP_SOURCE_DIR": str(store_like_desktop),
        }

        for unsafe_staging in (
            str(caller_user_dir),
            str(preexisting_stage),
            str(artifact_root),
            str(preexisting_file),
            str(preexisting_symlink),
            str(preexisting_symlink / "nested-stage"),
            str(broken_symlink),
            f"{artifact_root}/../caller-user-files",
            "relative-stage",
        ):
            rc, _out, err = self._run_prepare_stage_cli(
                {**base_env, "THREADLANE_ACCEPTANCE_STAGING_DIR": unsafe_staging},
                cwd=self.base,
            )
            self.assertEqual(rc, 1, f"Expected rejection for staging={unsafe_staging!r}: {err}")

        repo_no_local = _init_fake_repo(
            self.base / "repo-no-local",
            gitignore_dot_local=True,
            create_dot_local=False,
        )
        repo_unignored = _init_fake_repo(
            self.base / "repo-unignored",
            gitignore_dot_local=False,
            create_dot_local=True,
        )

        for unsafe_artifact in (
            "",
            str(repo / "apps" / "desktop" / "test-results"),
            str(repo / "target" / "pw"),
            str(repo),
            str(repo_no_local / ".local" / "pw"),
            str(repo_unignored / ".local" / "pw"),
            str(user_doc),
            str(preexisting_symlink),
            "/tmp",
        ):
            rc, _out, err = self._run_prepare_stage_cli(
                {
                    "THREADLANE_ACCEPTANCE_ARTIFACT_DIR": unsafe_artifact,
                    "THREADLANE_DESKTOP_SOURCE_DIR": str(store_like_desktop),
                },
                cwd=self.base,
            )
            self.assertEqual(rc, 1, f"Expected rejection for artifact={unsafe_artifact!r}: {err}")

        self.assertFalse((repo / "apps" / "desktop" / "test-results").exists())
        self.assertFalse((repo / "target" / "pw").exists())
        self.assertFalse((repo_no_local / ".local").exists())
        self.assertFalse((repo_unignored / ".local" / "pw").exists())

        rc_ok, out_ok, err_ok = self._run_prepare_stage_cli(base_env, cwd=self.base)
        self.assertEqual(rc_ok, 0, err_ok)
        self.assertEqual(Path(out_ok).parent, artifact_root)

        self.assertEqual(user_doc.read_text(encoding="utf-8"), "user-owned-data\n")
        self.assertEqual(external_sym_target.read_text(encoding="utf-8"), "symlink-target-data\n")
        self.assertTrue((caller_user_dir / "target-link").is_symlink())
        self.assertEqual(
            (preexisting_stage / "inside.txt").read_text(encoding="utf-8"),
            "keep-inside\n",
        )
        self.assertTrue((preexisting_stage / "ext-link").is_symlink())
        self.assertEqual(preexisting_file.read_text(encoding="utf-8"), "keep-file\n")
        self.assertTrue(preexisting_symlink.is_symlink())
        self.assertEqual(os.readlink(preexisting_symlink), str(caller_user_dir))
        self.assertTrue(broken_symlink.is_symlink())


class DesktopAcceptanceIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name).resolve()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_child_isolation_creates_private_0700_dirs_without_mutating_parent_env(self) -> None:
        before_parent = dict(os.environ)
        layout = da.prepare_child_isolation(self.base / "run-01")

        for directory in (
            layout.root_dir,
            layout.home_dir,
            layout.xdg_config_dir,
            layout.xdg_data_dir,
            layout.xdg_cache_dir,
            layout.xdg_state_dir,
            layout.xdg_runtime_dir,
        ):
            mode = stat.S_IMODE(os.lstat(directory).st_mode)
            self.assertEqual(mode, 0o700)

        fake_parent = {
            "PATH": "/usr/bin:/bin",
            "HOME": "/home/host-operator",
            "XDG_CONFIG_HOME": "/home/host-operator/.config",
            "DBUS_SESSION_BUS_ADDRESS": "unix:path=/run/user/1000/bus",
            "CODEX_HOME": "/home/host-operator/.codex",
            "OPENAI_API_KEY": "sk-secret-value-1234567890",
        }
        child_env = da.build_isolated_child_env(layout, parent_env=fake_parent)

        self.assertEqual(child_env["HOME"], str(layout.home_dir))
        self.assertEqual(child_env["XDG_CONFIG_HOME"], str(layout.xdg_config_dir))
        self.assertEqual(child_env["XDG_DATA_HOME"], str(layout.xdg_data_dir))
        self.assertEqual(child_env["XDG_CACHE_HOME"], str(layout.xdg_cache_dir))
        self.assertEqual(child_env["XDG_STATE_HOME"], str(layout.xdg_state_dir))
        self.assertEqual(child_env["XDG_RUNTIME_DIR"], str(layout.xdg_runtime_dir))
        self.assertNotIn("DBUS_SESSION_BUS_ADDRESS", child_env)
        self.assertNotIn("CODEX_HOME", child_env)
        self.assertNotIn("OPENAI_API_KEY", child_env)
        self.assertEqual(dict(os.environ), before_parent)

    def test_enforces_no_codex_startup_setup_and_blocks_host_commands(self) -> None:
        argv = da.build_desktop_launch_argv(
            "/nix/store/demo-threadlane-desktop/bin/threadlane-desktop",
            extra_args=["--state-dir", "/tmp/isolated-state"],
        )
        self.assertEqual(
            argv,
            [
                "/nix/store/demo-threadlane-desktop/bin/threadlane-desktop",
                "--no-codex-startup-setup",
                "--state-dir",
                "/tmp/isolated-state",
            ],
        )

        deduped = da.build_desktop_launch_argv(
            "/nix/store/demo-threadlane-desktop/bin/threadlane-desktop",
            extra_args=["--no-codex-startup-setup"],
        )
        self.assertEqual(deduped.count("--no-codex-startup-setup"), 1)

        for forbidden in ("nrs", "/run/current-system/sw/bin/nixos-rebuild", "systemctl", "pkill", "killall"):
            with self.assertRaisesRegex(da.AcceptanceError, "forbidden"):
                da.assert_command_allowed([forbidden, "switch"])

    def test_private_dbus_wrapper_requires_dbus_run_session(self) -> None:
        wrapped = da.wrap_with_private_dbus(
            ["/nix/store/pw/bin/threadlane-desktop-playwright"],
            dbus_run_session_bin="/usr/bin/dbus-run-session",
        )
        self.assertEqual(
            wrapped,
            [
                "/usr/bin/dbus-run-session",
                "--",
                "/nix/store/pw/bin/threadlane-desktop-playwright",
            ],
        )

        with mock.patch("shutil.which", return_value=None):
            with self.assertRaisesRegex(da.AcceptanceError, "dbus-run-session"):
                da.wrap_with_private_dbus(["/nix/store/pw/bin/threadlane-desktop-playwright"])


class DesktopAcceptanceFailClosedVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name).resolve()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_playwright_report_fails_closed_on_zero_failed_or_skipped_tests(self) -> None:
        report_file = self.base / "playwright-results.json"

        # Missing report
        with self.assertRaisesRegex(da.AcceptanceError, "not produced"):
            da.verify_playwright_json_report(report_file)

        # Zero discovered tests
        report_file.write_text(json.dumps({"suites": [], "errors": []}), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "Zero Playwright tests"):
            da.verify_playwright_json_report(report_file)

        # Suite-level syntax/config error
        report_file.write_text(
            json.dumps({"suites": [], "errors": [{"message": "SyntaxError in spec"}]}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(da.AcceptanceError, "suite-level errors"):
            da.verify_playwright_json_report(report_file)

        # Skipped test rejected
        report_file.write_text(
            json.dumps(
                {
                    "suites": [
                        {
                            "specs": [
                                {
                                    "tests": [
                                        {
                                            "status": "skipped",
                                            "results": [{"status": "skipped"}],
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(da.AcceptanceError, "skipped"):
            da.verify_playwright_json_report(report_file)

        # Assertion failure rejected
        report_file.write_text(
            json.dumps(
                {
                    "suites": [
                        {
                            "specs": [
                                {
                                    "tests": [
                                        {
                                            "status": "unexpected",
                                            "results": [{"status": "failed"}],
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(da.AcceptanceError, "failed"):
            da.verify_playwright_json_report(report_file)

        # Passing nested suites accepted
        report_file.write_text(
            json.dumps(
                {
                    "suites": [
                        {
                            "suites": [
                                {
                                    "specs": [
                                        {
                                            "tests": [
                                                {
                                                    "status": "expected",
                                                    "results": [{"status": "passed"}],
                                                }
                                            ]
                                        },
                                        {
                                            "tests": [
                                                {
                                                    "status": "expected",
                                                    "results": [{"status": "passed"}],
                                                }
                                            ]
                                        },
                                    ]
                                }
                            ]
                        }
                    ],
                    "errors": [],
                }
            ),
            encoding="utf-8",
        )
        counts = da.verify_playwright_json_report(report_file)
        self.assertEqual(counts.total, 2)
        self.assertEqual(counts.passed, 2)
        self.assertEqual(counts.failed, 0)
        self.assertEqual(counts.skipped, 0)

    def _make_valid_vm_report_payload(self) -> dict[str, object]:
        scale1_base = {
            "measurement_method": "grim_pixel_diff",
            "output_name": "HEADLESS-1",
            "output_scale": 1.0,
            "in_sway_get_tree": False,
            "physical_rect": {"x": 8, "y": 32, "width": 360, "height": 320},
            "logical_rect": {"x": 8.0, "y": 32.0, "width": 360.0, "height": 320.0},
            "changed_pixels": 114800,
            "fill_ratio": 0.9965,
            "within_output_bounds": True,
            "proximate_to_clicked_icon": True,
            "other_output_diff_pixels": 0,
        }
        scale2_base = {
            "measurement_method": "grim_pixel_diff",
            "output_name": "HEADLESS-2",
            "output_scale": 2.0,
            "in_sway_get_tree": False,
            "physical_rect": {"x": 16, "y": 64, "width": 720, "height": 640},
            "logical_rect": {"x": 1928.0, "y": 32.0, "width": 360.0, "height": 320.0},
            "changed_pixels": 459200,
            "fill_ratio": 0.9965,
            "within_output_bounds": True,
            "proximate_to_clicked_icon": True,
            "other_output_diff_pixels": 0,
        }
        return {
            "schema_version": 2,
            "status": "passed",
            "desktop_exit_code": 0,
            "compositor_invariants": {
                "focus_follows_mouse": "yes",
                "broad_floating_rules_present": False,
                "tray_compositor_rules_present": False,
                "cursor_positioning_command": "set",
                "main_only_floating_rule": 'for_window [title="^Threadlane$"] floating enable',
                "main_default_size_unforced": True,
            },
            "guest_input": {
                "seat": "seat0",
                "capabilities_before": 3,
                "capabilities_after": 3,
                "setup_keypresses_sent": 0,
                "helper_pid": 4242,
                "helper_running_before_cleanup": True,
                "identities_preserved": True,
                "pointer_devices": [
                    {
                        "identifier": "0:0:virtual_pointer",
                        "name": "virtual_pointer",
                        "type": "pointer",
                    }
                ],
                "keyboard_devices": [
                    {
                        "identifier": "0:0:virtual_keyboard",
                        "name": "virtual_keyboard",
                        "type": "keyboard",
                        "xkb_active_layout_name": "English (US)",
                    }
                ],
                "device_identities": [
                    {
                        "type": "keyboard",
                        "identifier": "0:0:virtual_keyboard",
                        "name": "virtual_keyboard",
                    },
                    {
                        "type": "pointer",
                        "identifier": "0:0:virtual_pointer",
                        "name": "virtual_pointer",
                    },
                ],
            },
            "rendering_environment": {
                "isolated_vm_only": True,
                "wlr_backends": "headless",
                "wlr_renderer": "pixman",
                "webkit_disable_dmabuf_renderer": "1",
                "webkit_disable_compositing_mode": "1",
                "libgl_always_software": "1",
            },
            "main_window_paint_readiness": {
                "default_1040x760": {
                    "painted": True,
                    "measurement_method": "grim_pixel_diff_and_interior_contrast",
                    "expected_size": {"width": 1040, "height": 760},
                    "native_rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
                    "physical_rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
                    "logical_rect": {"x": 440.0, "y": 176.0, "width": 1040.0, "height": 760.0},
                    "changed_pixels": 789200,
                    "fill_ratio": 0.9985,
                    "interior_max_channel_stddev": 26.4,
                    "interior_color_buckets": 9,
                    "band_contrast_pixels": {
                        "titlebar": 420,
                        "navigation": 1680,
                        "workspace_body": 2340,
                    },
                    "screenshot": "01-main-default-1040x760.png",
                },
                "resized_360x500": {
                    "painted": True,
                    "measurement_method": "grim_pixel_diff_and_interior_contrast",
                    "expected_size": {"width": 360, "height": 500},
                    "native_rect": {"x": 780, "y": 306, "width": 360, "height": 500},
                    "physical_rect": {"x": 780, "y": 306, "width": 360, "height": 500},
                    "logical_rect": {"x": 780.0, "y": 306.0, "width": 360.0, "height": 500.0},
                    "changed_pixels": 179600,
                    "fill_ratio": 0.9978,
                    "interior_max_channel_stddev": 28.1,
                    "interior_color_buckets": 8,
                    "band_contrast_pixels": {
                        "titlebar": 280,
                        "navigation": 640,
                        "workspace_body": 1120,
                    },
                    "screenshot": "02-main-explicit-resize-360x500.png",
                },
                "restored_wide_1040x760": {
                    "painted": True,
                    "measurement_method": "grim_pixel_diff_and_interior_contrast",
                    "expected_size": {"width": 1040, "height": 760},
                    "native_rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
                    "physical_rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
                    "logical_rect": {"x": 440.0, "y": 176.0, "width": 1040.0, "height": 760.0},
                    "changed_pixels": 789400,
                    "fill_ratio": 0.9987,
                    "interior_max_channel_stddev": 26.6,
                    "interior_color_buckets": 9,
                    "band_contrast_pixels": {
                        "titlebar": 425,
                        "navigation": 1690,
                        "workspace_body": 2350,
                    },
                    "screenshot": "02b-main-restored-wide-1040x760.png",
                },
            },
            "initial_default_main_rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
            "explicit_resized_min_main_rect": {"x": 780, "y": 306, "width": 360, "height": 500},
            "rendered_popup_measurements": {
                "scale_1": copy.deepcopy(scale1_base),
                "scale_2": copy.deepcopy(scale2_base),
            },
            "pointer_transit": {
                "clicked_during_transit": False,
                "waypoints_count": 5,
                "dwell_ms": 9520.0,
                "visible_after_dwell": True,
            },
            "refresh_paint_survival": {
                "ui_refresh_interval_ms": 4000,
                "headless2_scale2_dwell": {
                    "dwell_seconds": 9.52,
                    "dwell_ms": 9520.0,
                    "measurement": {
                        **copy.deepcopy(scale2_base),
                        "screenshot": "05-after-pointer-transit-dwell-headless2.png",
                    },
                },
                "headless1_scale1_reopen": {
                    "initial_measurement": {
                        **copy.deepcopy(scale1_base),
                        "screenshot": "09b-open-for-restore-headless1.png",
                    },
                    "target_interval_seconds": 1.0,
                    "sample_count": 10,
                    "sampling_window_seconds": 9.45,
                    "total_duration_seconds": 10.5,
                    "samples": [
                        {
                            "sample_index": idx,
                            "elapsed_seconds": round(1.05 * idx, 4),
                            "interval_seconds": 1.05,
                            **copy.deepcopy(scale1_base),
                            "screenshot": f"09b-refresh-survival-{idx:02d}-headless1.png",
                        }
                        for idx in range(1, 11)
                    ],
                },
            },
            "escape_dismissal": {
                "interior_click_before_escape": False,
                "dismissed": True,
            },
            "scene_cleanup": {
                "ok": True,
                "errors": [],
                "owners": {
                    "threadlane_desktop": {
                        "owner": "threadlane_desktop",
                        "present": True,
                        "pid": 4301,
                        "already_exited": True,
                        "exit_code": 0,
                        "terminated_cleanly": True,
                        "killed_with_sigkill": False,
                        "reaped": True,
                        "ok": True,
                        "error": None,
                    },
                    "waybar": {
                        "owner": "waybar",
                        "present": True,
                        "pid": 4280,
                        "already_exited": False,
                        "exit_code": 0,
                        "terminated_cleanly": True,
                        "killed_with_sigkill": False,
                        "reaped": True,
                        "ok": True,
                        "error": None,
                    },
                    "persistent_input": {
                        "owner": "persistent_input",
                        "present": True,
                        "pid": 4242,
                        "already_exited": False,
                        "exit_code": 0,
                        "terminated_cleanly": True,
                        "killed_with_sigkill": False,
                        "reaped": True,
                        "ok": True,
                        "error": None,
                    },
                    "sway": {
                        "owner": "sway",
                        "present": True,
                        "pid": 4210,
                        "already_exited": False,
                        "exit_code": 0,
                        "terminated_cleanly": True,
                        "killed_with_sigkill": False,
                        "reaped": True,
                        "ok": True,
                        "error": None,
                    },
                },
                "log_handles": {
                    "sway": {"closed": True, "ok": True, "error": None},
                    "waybar": {"closed": True, "ok": True, "error": None},
                    "threadlane_desktop": {"closed": True, "ok": True, "error": None},
                },
            },
            "checks": {key: True for key in da.REQUIRED_VM_CHECKS},
        }

    def test_vm_report_requires_every_wayland_and_tray_check(self) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        for required_new in (
            "no_forced_tray_compositor_rules",
            "rendered_popup_geometry",
            "pointer_transit_dwell_survival",
        ):
            self.assertIn(required_new, da.REQUIRED_VM_CHECKS)

        valid_payload = self._make_valid_vm_report_payload()
        vm_report.write_text(json.dumps(valid_payload), encoding="utf-8")
        verified = da.verify_vm_acceptance_report(vm_report)
        self.assertEqual(verified["status"], "passed")

        # Flipping any required check to False or omitting it must fail closed.
        for key in da.REQUIRED_VM_CHECKS:
            false_payload = copy.deepcopy(valid_payload)
            assert isinstance(false_payload["checks"], dict)
            false_payload["checks"][key] = False
            vm_report.write_text(json.dumps(false_payload), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, key):
                da.verify_vm_acceptance_report(vm_report)

            missing_payload = copy.deepcopy(valid_payload)
            assert isinstance(missing_payload["checks"], dict)
            del missing_payload["checks"][key]
            vm_report.write_text(json.dumps(missing_payload), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, key):
                da.verify_vm_acceptance_report(vm_report)

        # Boolean False must never be accepted as desktop_exit_code == 0.
        bool_exit = copy.deepcopy(valid_payload)
        bool_exit["desktop_exit_code"] = False
        vm_report.write_text(json.dumps(bool_exit), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "desktop_exit_code"):
            da.verify_vm_acceptance_report(vm_report)

    def test_vm_report_rejects_compositor_masking_or_forced_tray_rules(self) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        base = self._make_valid_vm_report_payload()

        mutations: list[tuple[str, object, str]] = [
            ("focus_follows_mouse", "no", "focus_follows_mouse"),
            ("broad_floating_rules_present", True, "broad_floating_rules_present"),
            ("tray_compositor_rules_present", True, "tray_compositor_rules_present"),
            ("cursor_positioning_command", "move", "cursor_positioning_command"),
            ("main_default_size_unforced", False, "main_default_size_unforced"),
        ]
        for field, bad_val, pattern in mutations:
            broken = copy.deepcopy(base)
            assert isinstance(broken["compositor_invariants"], dict)
            broken["compositor_invariants"][field] = bad_val
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        tiled_main = copy.deepcopy(base)
        tiled_main["initial_default_main_rect"] = {
            "x": 0,
            "y": 32,
            "width": 1920,
            "height": 1048,
        }
        vm_report.write_text(json.dumps(tiled_main), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "initial_default_main_rect"):
            da.verify_vm_acceptance_report(vm_report)

    def test_vm_report_rejects_fake_oversized_or_leaking_popup_geometry(self) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        base = self._make_valid_vm_report_payload()

        # Non-pixel measurement source (e.g. sway_get_tree or app telemetry) must be rejected.
        fake_method = copy.deepcopy(base)
        fake_method["rendered_popup_measurements"]["scale_2"]["measurement_method"] = (  # type: ignore[index]
            "sway_get_tree"
        )
        vm_report.write_text(json.dumps(fake_method), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "grim_pixel_diff"):
            da.verify_vm_acceptance_report(vm_report)

        # Oversized tiled tray popup (e.g. 1280x688 logical on Sway) must be rejected.
        oversized = copy.deepcopy(base)
        oversized["rendered_popup_measurements"]["scale_2"]["logical_rect"] = {  # type: ignore[index]
            "x": 1920.0,
            "y": 32.0,
            "width": 1280.0,
            "height": 688.0,
        }
        oversized["rendered_popup_measurements"]["scale_2"]["physical_rect"] = {  # type: ignore[index]
            "x": 0,
            "y": 64,
            "width": 2560,
            "height": 1376,
        }
        vm_report.write_text(json.dumps(oversized), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "scale_2.logical_rect"):
            da.verify_vm_acceptance_report(vm_report)

        # Wrong physical HiDPI size at scale 2.0 (360x320 instead of ~720x640) must be rejected.
        bad_hidpi = copy.deepcopy(base)
        bad_hidpi["rendered_popup_measurements"]["scale_2"]["physical_rect"] = {  # type: ignore[index]
            "x": 16,
            "y": 64,
            "width": 360,
            "height": 320,
        }
        vm_report.write_text(json.dumps(bad_hidpi), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "scale_2.physical_rect"):
            da.verify_vm_acceptance_report(vm_report)

        # Sparse fill, out-of-bounds, non-proximate icon placement, or cross-output leak must fail.
        for scale_key, field, bad_val, pattern in (
            ("scale_1", "fill_ratio", 0.25, "fill_ratio"),
            ("scale_1", "within_output_bounds", False, "within_output_bounds"),
            ("scale_2", "proximate_to_clicked_icon", False, "proximate_to_clicked_icon"),
            ("scale_2", "other_output_diff_pixels", 18, "other_output_diff_pixels"),
            ("scale_1", "other_output_diff_pixels", False, "other_output_diff_pixels"),
        ):
            broken = copy.deepcopy(base)
            broken["rendered_popup_measurements"][scale_key][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

    def test_vm_report_rejects_invalid_pointer_transit_or_escape_shortcuts(self) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        base = self._make_valid_vm_report_payload()

        for field, bad_val, pattern in (
            ("clicked_during_transit", True, "clicked_during_transit"),
            ("waypoints_count", 2, "waypoints"),
            ("dwell_ms", 150, "dwell_ms"),
            ("dwell_ms", 550, "dwell_ms"),
            ("dwell_ms", 8500, "dwell_ms"),
            ("visible_after_dwell", False, "visible_after_dwell"),
        ):
            broken = copy.deepcopy(base)
            broken["pointer_transit"][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        for field, bad_val, pattern in (
            ("interior_click_before_escape", True, "interior_click_before_escape"),
            ("dismissed", False, "dismissed"),
        ):
            broken = copy.deepcopy(base)
            broken["escape_dismissal"][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        # Offline static guard (not a substitute for real QEMU VM execution): ensure the VM harness
        # source initializes check flags to False, preserves software-rendering flags, embeds
        # refresh_paint_survival in the main report, and reuses verify_vm_acceptance_report.
        nix_path = Path(__file__).resolve().parent.parent / "nix" / "desktop-acceptance.nix"
        nix_text = nix_path.read_text(encoding="utf-8")
        self.assertIn("checks: dict[str, bool] = {key: False for key in REQUIRED_CHECK_KEYS}", nix_text)
        self.assertNotIn("for_window [all] floating enable", nix_text)
        self.assertNotIn("focus_follows_mouse no", nix_text)
        self.assertIn("focus_follows_mouse yes", nix_text)
        self.assertNotIn('"cursor", "move"', nix_text)
        self.assertIn('"cursor", "set"', nix_text)
        self.assertIn('sway_env["WLR_RENDERER"] = "pixman"', nix_text)
        self.assertIn('client_env["WEBKIT_DISABLE_DMABUF_RENDERER"] = "1"', nix_text)
        self.assertIn('client_env["WEBKIT_DISABLE_COMPOSITING_MODE"] = "1"', nix_text)
        self.assertIn('client_env["LIBGL_ALWAYS_SOFTWARE"] = "1"', nix_text)
        self.assertIn('"refresh_paint_survival": refresh_paint_survival', nix_text)
        self.assertIn("verify_vm_acceptance_report", nix_text)

    def test_dry_run_and_partial_vs_full_acceptance_honesty(self) -> None:
        repo = _init_fake_repo(self.base / "repo", gitignore_dot_local=True, create_dot_local=True)

        # Partial stage with --require-full-acceptance must fail closed.
        stderr_buf = io.StringIO()
        with mock.patch("sys.stderr", stderr_buf):
            rc = da.main(
                [
                    "--repo-root",
                    str(repo),
                    "--stage",
                    "browser",
                    "--require-full-acceptance",
                ]
            )
        self.assertEqual(rc, 1)
        self.assertIn("requires --stage all", stderr_buf.getvalue())

        # Dry run succeeds without creating run directories.
        stdout_buf = io.StringIO()
        with mock.patch("sys.stdout", stdout_buf):
            rc_dry = da.main(["--repo-root", str(repo), "--dry-run"])
        self.assertEqual(rc_dry, 0)
        plan = json.loads(stdout_buf.getvalue())
        self.assertEqual(plan["mode"], "dry_run")
        self.assertTrue(plan["full_desktop_acceptance_targeted"])
        self.assertFalse(plan["child_isolation"]["parent_environment_mutated"])
        self.assertFalse(plan["child_isolation"]["touches_host_codex"])
        self.assertFalse(plan["child_isolation"]["invokes_host_nrs"])
        self.assertFalse((repo / ".local" / "desktop-acceptance").exists())

        # Partial browser-only run explicitly records full_desktop_acceptance=False.
        with mock.patch.object(
            da,
            "execute_browser_stage",
            return_value=da.PlaywrightSuiteCounts(total=11, passed=11, failed=0, skipped=0),
        ):
            partial_out = io.StringIO()
            with mock.patch("sys.stdout", partial_out):
                rc_partial = da.main(["--repo-root", str(repo), "--stage", "browser"])
        self.assertEqual(rc_partial, 0)
        partial_summary = json.loads(partial_out.getvalue())
        self.assertFalse(partial_summary["full_desktop_acceptance"])
        self.assertEqual(partial_summary["status"], "partial_browser_passed")
        self.assertFalse(partial_summary["vm"]["executed"])

    def test_vm_report_rejects_missing_or_invalid_input_rendering_and_paint_sections(
        self,
    ) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        base = self._make_valid_vm_report_payload()

        guest_mutations: list[tuple[str, object, str]] = [
            ("seat", "seat1", "guest_input.seat"),
            ("capabilities_before", 1, "guest_input capabilities"),
            ("capabilities_after", 2, "guest_input capabilities"),
            ("capabilities_before", True, "guest_input.capabilities_before"),
            ("setup_keypresses_sent", 1, "setup_keypresses_sent"),
            ("helper_running_before_cleanup", False, "helper_running_before_cleanup"),
            ("identities_preserved", False, "identities_preserved"),
            ("pointer_devices", [], "pointer_devices"),
            ("keyboard_devices", [{"identifier": "0:0:vkbd", "name": "vkbd"}], "keyboard_devices"),
        ]
        for field, bad_val, pattern in guest_mutations:
            broken = copy.deepcopy(base)
            broken["guest_input"][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        render_mutations: list[tuple[str, object, str]] = [
            ("isolated_vm_only", False, "isolated_vm_only"),
            ("wlr_renderer", "vulkan", "wlr_renderer"),
            ("webkit_disable_dmabuf_renderer", "0", "webkit_disable_dmabuf_renderer"),
            ("webkit_disable_compositing_mode", "0", "webkit_disable_compositing_mode"),
            ("libgl_always_software", "0", "libgl_always_software"),
        ]
        for field, bad_val, pattern in render_mutations:
            broken = copy.deepcopy(base)
            broken["rendering_environment"][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        for target_key in (
            "default_1040x760",
            "resized_360x500",
            "restored_wide_1040x760",
        ):
            missing_stage = copy.deepcopy(base)
            del missing_stage["main_window_paint_readiness"][target_key]  # type: ignore[attr-defined]
            vm_report.write_text(json.dumps(missing_stage), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, target_key):
                da.verify_vm_acceptance_report(vm_report)

            unpainted = copy.deepcopy(base)
            unpainted["main_window_paint_readiness"][target_key]["painted"] = False  # type: ignore[index]
            vm_report.write_text(json.dumps(unpainted), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, f"{target_key}.painted"):
                da.verify_vm_acceptance_report(vm_report)

            blank_body = copy.deepcopy(base)
            blank_body["main_window_paint_readiness"][target_key]["band_contrast_pixels"][  # type: ignore[index]
                "workspace_body"
            ] = 0
            vm_report.write_text(json.dumps(blank_body), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, "workspace_body"):
                da.verify_vm_acceptance_report(vm_report)

        # Corrupting restored_wide_1040x760 geometry, method, or interior contrast must fail closed.
        restored_corruptions: list[tuple[str, object, str]] = [
            ("measurement_method", "sway_get_tree", "restored_wide_1040x760.measurement_method"),
            (
                "native_rect",
                {"x": 780, "y": 306, "width": 360, "height": 500},
                "restored_wide_1040x760.native_rect",
            ),
            (
                "logical_rect",
                {"x": 780.0, "y": 306.0, "width": 360.0, "height": 500.0},
                "restored_wide_1040x760.logical_rect",
            ),
            ("fill_ratio", 0.50, "restored_wide_1040x760.fill_ratio"),
            (
                "interior_max_channel_stddev",
                1.2,
                "restored_wide_1040x760.interior_max_channel_stddev",
            ),
            ("interior_color_buckets", 1, "restored_wide_1040x760.interior_color_buckets"),
        ]
        for field, bad_val, pattern in restored_corruptions:
            broken = copy.deepcopy(base)
            broken["main_window_paint_readiness"]["restored_wide_1040x760"][field] = bad_val  # type: ignore[index]
            vm_report.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        # Missing or failed scene_cleanup in a claimed passed report must fail closed.
        missing_cleanup = copy.deepcopy(base)
        del missing_cleanup["scene_cleanup"]
        vm_report.write_text(json.dumps(missing_cleanup), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "scene_cleanup"):
            da.verify_vm_acceptance_report(vm_report)

        failed_cleanup = copy.deepcopy(base)
        failed_cleanup["scene_cleanup"]["ok"] = False  # type: ignore[index]
        vm_report.write_text(json.dumps(failed_cleanup), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "scene_cleanup.ok"):
            da.verify_vm_acceptance_report(vm_report)

        unreaped_owner = copy.deepcopy(base)
        unreaped_owner["scene_cleanup"]["owners"]["sway"]["reaped"] = False  # type: ignore[index]
        vm_report.write_text(json.dumps(unreaped_owner), encoding="utf-8")
        with self.assertRaisesRegex(da.AcceptanceError, "scene_cleanup.owners.sway"):
            da.verify_vm_acceptance_report(vm_report)

    def test_vm_report_rejects_missing_or_invalid_refresh_paint_survival_proof(
        self,
    ) -> None:
        vm_report = self.base / "vm-acceptance-report.json"
        base = self._make_valid_vm_report_payload()
        self.assertGreaterEqual(da.MIN_POINTER_DWELL_MS, 9500.0)
        self.assertEqual(da.MIN_H1_REFRESH_SAMPLE_INTERVAL_SECONDS, 0.90)
        self.assertEqual(da.MAX_H1_REFRESH_SAMPLE_INTERVAL_SECONDS, 2.5)

        def _assert_rejected(payload: dict[str, object], pattern: str) -> None:
            vm_report.write_text(json.dumps(payload, allow_nan=True), encoding="utf-8")
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.verify_vm_acceptance_report(vm_report)

        # 1. Missing section, non-object section, or missing nested blocks must fail closed.
        missing_section = copy.deepcopy(base)
        del missing_section["refresh_paint_survival"]
        _assert_rejected(missing_section, "refresh_paint_survival")

        for bad_section in (None, False, True, [], "passed"):
            non_obj = copy.deepcopy(base)
            non_obj["refresh_paint_survival"] = bad_section
            _assert_rejected(non_obj, "refresh_paint_survival")

        for nested_key in ("headless2_scale2_dwell", "headless1_scale1_reopen"):
            missing_nested = copy.deepcopy(base)
            del missing_nested["refresh_paint_survival"][nested_key]  # type: ignore[attr-defined]
            _assert_rejected(missing_nested, nested_key)

        bad_ui_interval = copy.deepcopy(base)
        bad_ui_interval["refresh_paint_survival"]["ui_refresh_interval_ms"] = 8000  # type: ignore[index]
        _assert_rejected(bad_ui_interval, "ui_refresh_interval_ms")

        # 2. Short dwell, short sampling window/duration, or <10 / unbounded sample counts must fail.
        for short_sec, short_ms in ((0.55, 550.0), (8.9, 8900.0), (-9.5, -9500.0), (180.0, 180000.0)):
            bad_dwell = copy.deepcopy(base)
            bad_dwell["refresh_paint_survival"]["headless2_scale2_dwell"]["dwell_seconds"] = short_sec  # type: ignore[index]
            bad_dwell["refresh_paint_survival"]["headless2_scale2_dwell"]["dwell_ms"] = short_ms  # type: ignore[index]
            _assert_rejected(bad_dwell, "headless2_scale2_dwell.dwell_seconds")

        unbounded_transit_dwell = copy.deepcopy(base)
        unbounded_transit_dwell["pointer_transit"]["dwell_ms"] = 180000.0  # type: ignore[index]
        _assert_rejected(unbounded_transit_dwell, "pointer_transit.dwell_ms")

        mismatched_dwell = copy.deepcopy(base)
        mismatched_dwell["pointer_transit"]["dwell_ms"] = 9900.0  # type: ignore[index]
        _assert_rejected(mismatched_dwell, "pointer_transit.dwell_ms")

        fewer_samples = copy.deepcopy(base)
        fewer_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["sample_count"] = 9  # type: ignore[index]
        fewer_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"] = (  # type: ignore[index]
            fewer_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][:9]  # type: ignore[index]
        )
        _assert_rejected(fewer_samples, "sample_count")

        count_mismatch = copy.deepcopy(base)
        count_mismatch["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"] = (  # type: ignore[index]
            count_mismatch["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][:9]  # type: ignore[index]
        )
        _assert_rejected(count_mismatch, "bounded samples")

        unbounded_samples = copy.deepcopy(base)
        extra_entry = copy.deepcopy(
            unbounded_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][-1]  # type: ignore[index]
        )
        extra_entry["sample_index"] = 11
        extra_entry["elapsed_seconds"] = 11.55
        extra_entry["screenshot"] = "09b-refresh-survival-11-headless1.png"
        unbounded_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["sample_count"] = 11  # type: ignore[index]
        unbounded_samples["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"].append(  # type: ignore[index]
            extra_entry
        )
        _assert_rejected(unbounded_samples, "sample_count")

        unbounded_list_only = copy.deepcopy(unbounded_samples)
        unbounded_list_only["refresh_paint_survival"]["headless1_scale1_reopen"]["sample_count"] = 10  # type: ignore[index]
        _assert_rejected(unbounded_list_only, "bounded samples")

        short_window = copy.deepcopy(base)
        h1_short = short_window["refresh_paint_survival"]["headless1_scale1_reopen"]  # type: ignore[index]
        for idx, sample in enumerate(h1_short["samples"], start=1):
            sample["elapsed_seconds"] = round(0.5 * idx, 4)
            sample["interval_seconds"] = 0.5
        h1_short["sampling_window_seconds"] = 4.5
        h1_short["total_duration_seconds"] = 5.0
        _assert_rejected(short_window, "sampling_window_seconds")

        lied_window = copy.deepcopy(short_window)
        lied_window["refresh_paint_survival"]["headless1_scale1_reopen"][  # type: ignore[index]
            "sampling_window_seconds"
        ] = 9.45
        lied_window["refresh_paint_survival"]["headless1_scale1_reopen"][  # type: ignore[index]
            "total_duration_seconds"
        ] = 10.5
        _assert_rejected(lied_window, "monotonic step delta")

        tight_nine_under_window = copy.deepcopy(base)
        h1_under = tight_nine_under_window["refresh_paint_survival"]["headless1_scale1_reopen"]  # type: ignore[index]
        for idx, sample in enumerate(h1_under["samples"], start=1):
            sample["elapsed_seconds"] = round(0.95 * idx, 4)
            sample["interval_seconds"] = 0.95
        h1_under["sampling_window_seconds"] = 9.5
        h1_under["total_duration_seconds"] = 9.5
        _assert_rejected(tight_nine_under_window, "monotonic sampling window")

        for slow_step, slow_window, slow_total in (
            (2.6, 23.4, 26.0),
            (10.0, 90.0, 100.0),
        ):
            over_max_interval = copy.deepcopy(base)
            h1_slow = over_max_interval["refresh_paint_survival"]["headless1_scale1_reopen"]  # type: ignore[index]
            for idx, sample in enumerate(h1_slow["samples"], start=1):
                sample["elapsed_seconds"] = round(slow_step * idx, 4)
                sample["interval_seconds"] = slow_step
            h1_slow["sampling_window_seconds"] = slow_window
            h1_slow["total_duration_seconds"] = slow_total
            _assert_rejected(
                over_max_interval,
                r"monotonic step delta .* outside \[0\.9, 2\.5\]",
            )

        over_reported_interval_only = copy.deepcopy(base)
        h1_rep = over_reported_interval_only["refresh_paint_survival"]["headless1_scale1_reopen"]  # type: ignore[index]
        for idx, sample in enumerate(h1_rep["samples"], start=1):
            sample["elapsed_seconds"] = round(2.48 + 1.05 * (idx - 1), 4)
            sample["interval_seconds"] = 2.52 if idx == 1 else 1.05
        h1_rep["sampling_window_seconds"] = 9.45
        h1_rep["total_duration_seconds"] = 11.93
        _assert_rejected(
            over_reported_interval_only,
            r"samples\[0\]\.interval_seconds \(2\.52\) outside \[0\.9, 2\.5\]",
        )

        # 3. Sparse 13x13 CPU-damage blank frame or low-fill frame inside an otherwise valid 10-sample sequence.
        sparse_13x13_mid = copy.deepcopy(base)
        mid_13 = sparse_13x13_mid["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][5]  # type: ignore[index]
        mid_13["physical_rect"] = {"x": 8, "y": 32, "width": 13, "height": 13}
        mid_13["logical_rect"] = {"x": 8.0, "y": 32.0, "width": 13.0, "height": 13.0}
        mid_13["changed_pixels"] = 169
        mid_13["fill_ratio"] = 1.0
        _assert_rejected(sparse_13x13_mid, r"samples\[5\]\.logical_rect")

        low_fill_mid = copy.deepcopy(base)
        mid_low = low_fill_mid["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][6]  # type: ignore[index]
        mid_low["fill_ratio"] = 0.08
        mid_low["changed_pixels"] = 9216
        _assert_rejected(low_fill_mid, r"samples\[6\]\.fill_ratio")

        forged_fill_13x13_pixels = copy.deepcopy(base)
        mid_forged = forged_fill_13x13_pixels["refresh_paint_survival"]["headless1_scale1_reopen"][  # type: ignore[index]
            "samples"
        ][7]
        mid_forged["changed_pixels"] = 169
        mid_forged["fill_ratio"] = 0.9965
        _assert_rejected(forged_fill_13x13_pixels, r"samples\[7\]\.changed_pixels")

        for bad_field, bad_val, pattern in (
            ("measurement_method", "window_geometry", r"samples\[3\].*grim_pixel_diff"),
            ("within_output_bounds", False, r"samples\[3\]\.within_output_bounds"),
            ("within_output_bounds", None, r"samples\[3\]\.within_output_bounds"),
            ("proximate_to_clicked_icon", False, r"samples\[3\]\.proximate_to_clicked_icon"),
            ("proximate_to_clicked_icon", None, r"samples\[3\]\.proximate_to_clicked_icon"),
        ):
            bad_sample_indicator = copy.deepcopy(base)
            bad_sample_indicator["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][3][  # type: ignore[index]
                bad_field
            ] = bad_val
            _assert_rejected(bad_sample_indicator, pattern)

        # 4. Wrong scale or leaked / unmeasured (None / False) non-target monitor on H2 or H1 samples.
        bad_h2_scale = copy.deepcopy(base)
        bad_h2_scale["refresh_paint_survival"]["headless2_scale2_dwell"]["measurement"][  # type: ignore[index]
            "output_scale"
        ] = 1.0
        _assert_rejected(bad_h2_scale, r"headless2_scale2_dwell\.measurement\.output_scale")

        for bad_other in (14, None, False, True, -1):
            bad_h2_other = copy.deepcopy(base)
            bad_h2_other["refresh_paint_survival"]["headless2_scale2_dwell"]["measurement"][  # type: ignore[index]
                "other_output_diff_pixels"
            ] = bad_other
            _assert_rejected(
                bad_h2_other,
                r"headless2_scale2_dwell\.measurement\.other_output_diff_pixels",
            )

            bad_h1_other = copy.deepcopy(base)
            bad_h1_other["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][4][  # type: ignore[index]
                "other_output_diff_pixels"
            ] = bad_other
            _assert_rejected(bad_h1_other, r"samples\[4\]\.other_output_diff_pixels")

            bad_init_other = copy.deepcopy(base)
            bad_init_other["refresh_paint_survival"]["headless1_scale1_reopen"][  # type: ignore[index]
                "initial_measurement"
            ]["other_output_diff_pixels"] = bad_other
            _assert_rejected(
                bad_init_other,
                r"initial_measurement\.other_output_diff_pixels",
            )

        bad_h1_scale = copy.deepcopy(base)
        bad_h1_scale["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][4][  # type: ignore[index]
            "output_scale"
        ] = 2.0
        _assert_rejected(bad_h1_scale, r"samples\[4\]\.output_scale")

        # 5. Duplicate/backward timestamps, inconsistent interval, wrong sample_index, or reused screenshot.
        dup_ts = copy.deepcopy(base)
        dup_samples = dup_ts["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"]  # type: ignore[index]
        dup_samples[4]["elapsed_seconds"] = dup_samples[3]["elapsed_seconds"]
        _assert_rejected(dup_ts, "strictly increase")

        backward_ts = copy.deepcopy(base)
        back_samples = backward_ts["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"]  # type: ignore[index]
        back_samples[5]["elapsed_seconds"] = round(back_samples[4]["elapsed_seconds"] - 0.3, 4)
        _assert_rejected(backward_ts, "strictly increase")

        lied_interval = copy.deepcopy(base)
        lied_interval["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][4][  # type: ignore[index]
            "interval_seconds"
        ] = 2.4
        _assert_rejected(lied_interval, "disagrees with monotonic timestamp delta")

        wrong_idx = copy.deepcopy(base)
        wrong_idx["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][2][  # type: ignore[index]
            "sample_index"
        ] = 9
        _assert_rejected(wrong_idx, r"samples\[2\]\.sample_index")

        dup_shot = copy.deepcopy(base)
        dup_shot_samples = dup_shot["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"]  # type: ignore[index]
        dup_shot_samples[8]["screenshot"] = dup_shot_samples[7]["screenshot"]
        _assert_rejected(dup_shot, "duplicate capture")

        # 6. Boolean, NaN, +inf, and -inf timing or numeric values must all fail closed.
        for bool_val in (True, False):
            bool_dwell = copy.deepcopy(base)
            bool_dwell["refresh_paint_survival"]["headless2_scale2_dwell"]["dwell_seconds"] = bool_val  # type: ignore[index]
            _assert_rejected(bool_dwell, "Expected numeric value")

            bool_sample_ts = copy.deepcopy(base)
            bool_sample_ts["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][2][  # type: ignore[index]
                "elapsed_seconds"
            ] = bool_val
            _assert_rejected(bool_sample_ts, "Expected numeric value")

            bool_count = copy.deepcopy(base)
            bool_count["refresh_paint_survival"]["headless1_scale1_reopen"]["sample_count"] = bool_val  # type: ignore[index]
            _assert_rejected(bool_count, "Expected integer value")

        for non_finite in (float("nan"), float("inf"), float("-inf")):
            nf_transit = copy.deepcopy(base)
            nf_transit["pointer_transit"]["dwell_ms"] = non_finite  # type: ignore[index]
            _assert_rejected(nf_transit, "Expected finite numeric value")

            nf_dwell = copy.deepcopy(base)
            nf_dwell["refresh_paint_survival"]["headless2_scale2_dwell"]["dwell_seconds"] = non_finite  # type: ignore[index]
            _assert_rejected(nf_dwell, "Expected finite numeric value")

            nf_elapsed = copy.deepcopy(base)
            nf_elapsed["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][2][  # type: ignore[index]
                "elapsed_seconds"
            ] = non_finite
            _assert_rejected(nf_elapsed, "Expected finite numeric value")

            nf_interval = copy.deepcopy(base)
            nf_interval["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][2][  # type: ignore[index]
                "interval_seconds"
            ] = non_finite
            _assert_rejected(nf_interval, "Expected finite numeric value")

            nf_window = copy.deepcopy(base)
            nf_window["refresh_paint_survival"]["headless1_scale1_reopen"][  # type: ignore[index]
                "sampling_window_seconds"
            ] = non_finite
            _assert_rejected(nf_window, "Expected finite numeric value")

            nf_fill = copy.deepcopy(base)
            nf_fill["refresh_paint_survival"]["headless1_scale1_reopen"]["samples"][2][  # type: ignore[index]
                "fill_ratio"
            ] = non_finite
            _assert_rejected(nf_fill, "Expected finite numeric value")


class DesktopAcceptanceGuestInputFixtureTests(unittest.TestCase):
    def setUp(self) -> None:
        short_parent = (
            "/tmp"
            if Path("/tmp").is_dir() and not Path("/tmp").is_symlink()
            else None
        )
        self._tmp = tempfile.TemporaryDirectory(dir=short_parent)
        self.base = Path(self._tmp.name).resolve()
        self.runtime_dir = self.base / "rt"
        self.runtime_dir.mkdir(mode=0o700)
        self.runtime_dir.chmod(0o700)
        self._wayland_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._wayland_sock.bind(str(self.runtime_dir / "wayland-1"))
        self._wayland_sock.listen(1)

    def tearDown(self) -> None:
        self._wayland_sock.close()
        self._tmp.cleanup()

    def _isolated_guest_env(self) -> dict[str, str]:
        return {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "XDG_RUNTIME_DIR": str(self.runtime_dir),
            "WAYLAND_DISPLAY": "wayland-1",
        }

    def test_rejects_host_display_or_unisolated_wayland_environment(self) -> None:
        for bad_env, pattern in (
            (
                {"XDG_RUNTIME_DIR": "/run/user/1000", "WAYLAND_DISPLAY": "wayland-0"},
                "host user runtime directory",
            ),
            (
                {
                    "XDG_RUNTIME_DIR": str(self.runtime_dir),
                    "WAYLAND_DISPLAY": "wayland-1",
                    "DISPLAY": ":0",
                },
                "DISPLAY",
            ),
            (
                {
                    "XDG_RUNTIME_DIR": str(self.runtime_dir),
                    "WAYLAND_DISPLAY": "../wayland-0",
                },
                "bare isolated socket name",
            ),
            (
                {
                    "XDG_RUNTIME_DIR": str(self.runtime_dir),
                    "WAYLAND_DISPLAY": "wayland-missing",
                },
                "does not exist",
            ),
        ):
            with self.assertRaisesRegex(da.AcceptanceError, pattern):
                da.PersistentGuestInputFixture(
                    [sys.executable, "-c", "pass"],
                    env=bad_env,
                    stderr_log_path=self.base / "persistent-input.log",
                )

    def test_persistent_input_fixture_nonblocking_stdin_ready_and_clean_stop(self) -> None:
        helper_code = "\n".join(
            [
                "import json, os, signal, sys, time",
                "stdin_eof = sys.stdin.read()",
                "assert stdin_eof == '', f'expected EOF stdin, got {stdin_eof!r}'",
                "stopped = False",
                "def _on_term(_sig, _frm):",
                "    global stopped",
                "    stopped = True",
                "signal.signal(signal.SIGTERM, _on_term)",
                "print(json.dumps({",
                "    'event': 'ready',",
                "    'pid': os.getpid(),",
                "    'seat': 'seat0',",
                "    'capabilities': 3,",
                "    'pointer': True,",
                "    'keyboard': True,",
                "    'keymap_format': 'xkb_v1',",
                "    'keymap_bytes': 48120,",
                "    'setup_keypresses_sent': 0,",
                "}), flush=True)",
                "while not stopped:",
                "    time.sleep(0.02)",
                "sys.stderr.write('clean-sigterm-shutdown token=secret123\\n')",
                "sys.stderr.flush()",
            ]
        )
        log_path = self.base / "persistent-input.log"
        fixture = da.PersistentGuestInputFixture(
            [sys.executable, "-u", "-c", helper_code],
            env=self._isolated_guest_env(),
            stderr_log_path=log_path,
        )
        ready = fixture.start(ready_timeout_sec=3.0)
        try:
            self.assertEqual(ready["seat"], "seat0")
            self.assertEqual(ready["capabilities"], 3)
            self.assertEqual(ready["setup_keypresses_sent"], 0)
            self.assertEqual(ready["pid"], fixture.pid)
            self.assertTrue(fixture.is_running())
            fixture.assert_running("test helper")
        finally:
            stop_info = fixture.stop(term_timeout_sec=2.0, kill_timeout_sec=1.0)

        self.assertFalse(fixture.is_running())
        self.assertIsNone(fixture.pid)
        self.assertEqual(stop_info["exit_code"], 0)
        self.assertTrue(stop_info["terminated_cleanly"])
        self.assertFalse(stop_info["killed_with_sigkill"])
        tail = da.read_bounded_redacted_log_tail(log_path, allowed_root=self.base)
        self.assertIn("clean-sigterm-shutdown", tail["tail"])
        self.assertNotIn("secret123", tail["tail"])
        self.assertIn("[REDACTED]", tail["tail"])

    def test_persistent_input_fixture_select_timeout_and_owned_sigkill_fallback(self) -> None:
        stalled_code = "\n".join(
            [
                "import signal, sys, time",
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
                "sys.stderr.write('stalled-before-ready\\n')",
                "sys.stderr.flush()",
                "for _ in range(150):",
                "    time.sleep(0.2)",
            ]
        )
        fixture = da.PersistentGuestInputFixture(
            [sys.executable, "-u", "-c", stalled_code],
            env=self._isolated_guest_env(),
            stderr_log_path=self.base / "stalled.log",
        )
        with self.assertRaisesRegex(
            da.AcceptanceError,
            r"Timed out.*stalled-before-ready",
        ):
            fixture.start(
                ready_timeout_sec=0.25,
                term_timeout_sec=0.15,
                kill_timeout_sec=1.5,
            )
        self.assertFalse(fixture.is_running())
        self.assertIsNone(fixture.pid)

        # Also verify SIGKILL fallback when stop() runs on a ready process ignoring SIGTERM.
        ignore_term_after_ready = "\n".join(
            [
                "import json, os, signal, time",
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)",
                "print(json.dumps({",
                "    'event': 'ready',",
                "    'pid': os.getpid(),",
                "    'seat': 'seat0',",
                "    'capabilities': 3,",
                "    'pointer': True,",
                "    'keyboard': True,",
                "    'keymap_format': 'xkb_v1',",
                "    'keymap_bytes': 4096,",
                "    'setup_keypresses_sent': 0,",
                "}), flush=True)",
                "for _ in range(150):",
                "    time.sleep(0.2)",
            ]
        )
        ready_fixture = da.PersistentGuestInputFixture(
            [sys.executable, "-u", "-c", ignore_term_after_ready],
            env=self._isolated_guest_env(),
            stderr_log_path=self.base / "ignore-term.log",
        )
        ready_fixture.start(ready_timeout_sec=3.0)
        stop_res = ready_fixture.stop(term_timeout_sec=0.15, kill_timeout_sec=1.5)
        self.assertTrue(stop_res["killed_with_sigkill"])
        self.assertFalse(stop_res["terminated_cleanly"])
        self.assertFalse(ready_fixture.is_running())

    def test_persistent_input_fixture_fails_clearly_on_bad_caps_keypress_or_early_exit(
        self,
    ) -> None:
        early_exit_code = "\n".join(
            [
                "import sys",
                "sys.stderr.write('missing required protocol zwp_virtual_keyboard_manager_v1\\n')",
                "sys.exit(3)",
            ]
        )
        f_exit = da.PersistentGuestInputFixture(
            [sys.executable, "-u", "-c", early_exit_code],
            env=self._isolated_guest_env(),
            stderr_log_path=self.base / "early-exit.log",
        )
        with self.assertRaisesRegex(
            da.AcceptanceError,
            "zwp_virtual_keyboard_manager_v1",
        ):
            f_exit.start(ready_timeout_sec=3.0)
        self.assertFalse(f_exit.is_running())

        for bad_caps, bad_keys, expected_err in (
            (1, 0, "capabilities=1"),
            (3, 1, "0 setup keypresses"),
        ):
            bad_ready_code = "\n".join(
                [
                    "import json, os, time",
                    f"print(json.dumps({{'event': 'ready', 'pid': os.getpid(), 'seat': 'seat0', "
                    f"'capabilities': {bad_caps}, 'pointer': True, 'keyboard': True, "
                    f"'keymap_format': 'xkb_v1', 'keymap_bytes': 4096, "
                    f"'setup_keypresses_sent': {bad_keys}}}), flush=True)",
                    "time.sleep(5.0)",
                ]
            )
            f_bad = da.PersistentGuestInputFixture(
                [sys.executable, "-u", "-c", bad_ready_code],
                env=self._isolated_guest_env(),
                stderr_log_path=self.base / f"bad-{bad_caps}-{bad_keys}.log",
            )
            with self.assertRaisesRegex(da.AcceptanceError, expected_err):
                f_bad.start(ready_timeout_sec=3.0, term_timeout_sec=1.0)
            self.assertFalse(f_bad.is_running())

    def test_seat0_capabilities_and_device_identity_preservation(self) -> None:
        valid_seats = [
            {
                "name": "seat0",
                "capabilities": 3,
                "devices": [
                    {"identifier": "0:0:virtual_pointer", "name": "virtual_pointer", "type": "pointer"},
                    {"identifier": "0:0:virtual_keyboard", "name": "virtual_keyboard", "type": "keyboard"},
                ],
            }
        ]
        valid_inputs = [
            {
                "identifier": "0:0:virtual_pointer",
                "name": "virtual_pointer",
                "type": "pointer",
            },
            {
                "identifier": "0:0:virtual_keyboard",
                "name": "virtual_keyboard",
                "type": "keyboard",
                "xkb_layout_names": ["English (US)"],
                "xkb_active_layout_name": "English (US)",
            },
        ]
        before = da.verify_seat0_input_state(valid_seats, valid_inputs)
        after = da.verify_seat0_input_state(
            copy.deepcopy(valid_seats),
            copy.deepcopy(valid_inputs),
        )
        preserved = da.verify_input_device_identities_preserved(
            before,
            after,
            helper_pid_before=9001,
            helper_pid_after=9001,
            helper_running_after=True,
        )
        self.assertTrue(preserved["identities_preserved"])
        self.assertEqual(preserved["capabilities_before"], 3)
        self.assertEqual(preserved["capabilities_after"], 3)

        # Bare Sway (caps=0), pointer-only (caps=1), keyboard-only (caps=2) must all fail.
        for bad_caps in (0, 1, 2):
            seats_bad = copy.deepcopy(valid_seats)
            seats_bad[0]["capabilities"] = bad_caps
            with self.assertRaisesRegex(da.AcceptanceError, "capabilities"):
                da.verify_seat0_input_state(seats_bad, valid_inputs)

        # Keyboard without compiled XKB keymap layout must fail.
        no_keymap_inputs = copy.deepcopy(valid_inputs)
        no_keymap_inputs[1]["xkb_active_layout_name"] = None  # type: ignore[assignment]
        no_keymap_inputs[1]["xkb_layout_names"] = []
        with self.assertRaisesRegex(da.AcceptanceError, "no active XKB keymap"):
            da.verify_seat0_input_state(valid_seats, no_keymap_inputs)

        # Mutated device identifier, dead helper, or restarted PID must fail preservation check.
        changed_after = copy.deepcopy(after)
        changed_after["device_identities"][0]["identifier"] = "0:0:recreated_keyboard"
        with self.assertRaisesRegex(da.AcceptanceError, "identities changed"):
            da.verify_input_device_identities_preserved(
                before,
                changed_after,
                helper_pid_before=9001,
                helper_pid_after=9001,
                helper_running_after=True,
            )

        with self.assertRaisesRegex(da.AcceptanceError, "not running continuously"):
            da.verify_input_device_identities_preserved(
                before,
                after,
                helper_pid_before=9001,
                helper_pid_after=9001,
                helper_running_after=False,
            )

        with self.assertRaisesRegex(da.AcceptanceError, "PID changed"):
            da.verify_input_device_identities_preserved(
                before,
                after,
                helper_pid_before=9001,
                helper_pid_after=9002,
                helper_running_after=True,
            )

    def test_seat0_verifier_fails_closed_on_missing_nonlist_or_foreign_seat_devices(
        self,
    ) -> None:
        rich_sway_seats = [
            {
                "name": "seat0",
                "capabilities": 3,
                "focus": 0,
                "devices": [
                    {
                        "identifier": "0:0:wlr_virtual_pointer_threadlane-pe",
                        "name": "wlr_virtual_pointer_threadlane-pe",
                        "type": "pointer",
                        "vendor": 0,
                        "product": 0,
                        "scroll_factor": 1.0,
                    },
                    {
                        "identifier": "0:0:wlr_virtual_keyboard_threadlane-pe",
                        "name": "wlr_virtual_keyboard_threadlane-pe",
                        "type": "keyboard",
                        "vendor": 0,
                        "product": 0,
                        "repeat_delay": 600,
                        "repeat_rate": 25,
                        "xkb_layout_names": ["English (US)"],
                        "xkb_active_layout_index": 0,
                        "xkb_active_layout_name": "English (US)",
                    },
                ],
            },
            {
                "name": "seat1",
                "capabilities": 3,
                "focus": 0,
                "devices": [
                    {
                        "identifier": "0:0:foreign_seat1_pointer",
                        "name": "foreign_seat1_pointer",
                        "type": "pointer",
                    },
                    {
                        "identifier": "0:0:foreign_seat1_keyboard",
                        "name": "foreign_seat1_keyboard",
                        "type": "keyboard",
                        "xkb_active_layout_name": "English (US)",
                    },
                ],
            },
        ]
        rich_sway_inputs = [
            copy.deepcopy(rich_sway_seats[0]["devices"][0]),  # type: ignore[index]
            copy.deepcopy(rich_sway_seats[0]["devices"][1]),  # type: ignore[index]
            copy.deepcopy(rich_sway_seats[1]["devices"][0]),  # type: ignore[index]
            copy.deepcopy(rich_sway_seats[1]["devices"][1]),  # type: ignore[index]
        ]

        # Valid Sway metadata on seat0 passes and excludes foreign seat1 devices from seat0 identities.
        verified = da.verify_seat0_input_state(rich_sway_seats, rich_sway_inputs)
        self.assertEqual(verified["seat"], "seat0")
        self.assertEqual(
            verified["device_identities"],
            [
                {
                    "type": "keyboard",
                    "identifier": "0:0:wlr_virtual_keyboard_threadlane-pe",
                    "name": "wlr_virtual_keyboard_threadlane-pe",
                },
                {
                    "type": "pointer",
                    "identifier": "0:0:wlr_virtual_pointer_threadlane-pe",
                    "name": "wlr_virtual_pointer_threadlane-pe",
                },
            ],
        )

        # Missing or non-list seat0.devices must fail closed (never accept get_inputs globally).
        missing_devices_seats = [
            {"name": "seat0", "capabilities": 3},
        ]
        with self.assertRaisesRegex(da.AcceptanceError, "seat0.devices must be a valid membership list"):
            da.verify_seat0_input_state(missing_devices_seats, rich_sway_inputs)

        for bad_devices in (None, {}, "0:0:virtual_pointer", 3, True):
            nonlist_seats = [
                {"name": "seat0", "capabilities": 3, "devices": bad_devices},
            ]
            with self.assertRaisesRegex(
                da.AcceptanceError,
                "seat0.devices must be a valid membership list",
            ):
                da.verify_seat0_input_state(nonlist_seats, rich_sway_inputs)

        # Empty or single-device seat0.devices must fail closed.
        for short_devices in ([], [rich_sway_seats[0]["devices"][0]]):  # type: ignore[index]
            short_seats = [
                {"name": "seat0", "capabilities": 3, "devices": short_devices},
            ]
            with self.assertRaisesRegex(da.AcceptanceError, "seat0.devices"):
                da.verify_seat0_input_state(short_seats, rich_sway_inputs)

        # Foreign-seat keyboard on seat1 must never satisfy seat0 when seat0 only has pointers.
        pointer_only_seat0_with_foreign_kbd = [
            {
                "name": "seat0",
                "capabilities": 3,
                "devices": [
                    {
                        "identifier": "0:0:wlr_virtual_pointer_threadlane-pe",
                        "name": "wlr_virtual_pointer_threadlane-pe",
                        "type": "pointer",
                    },
                    {
                        "identifier": "0:0:second_pointer_on_seat0",
                        "name": "second_pointer_on_seat0",
                        "type": "pointer",
                    },
                ],
            },
            copy.deepcopy(rich_sway_seats[1]),
        ]
        inputs_with_second_ptr = [
            *copy.deepcopy(rich_sway_inputs),
            {
                "identifier": "0:0:second_pointer_on_seat0",
                "name": "second_pointer_on_seat0",
                "type": "pointer",
            },
        ]
        with self.assertRaisesRegex(da.AcceptanceError, "No virtual keyboard device attached to seat0"):
            da.verify_seat0_input_state(
                pointer_only_seat0_with_foreign_kbd,
                inputs_with_second_ptr,
            )

        # Missing seat0 entirely (only seat1 present) must fail closed.
        with self.assertRaisesRegex(da.AcceptanceError, "does not contain required 'seat0'"):
            da.verify_seat0_input_state([copy.deepcopy(rich_sway_seats[1])], rich_sway_inputs)

        # Cross-seat ambiguity (same device identifier claimed by both seat0 and seat1) must fail closed.
        cross_seat_collision = copy.deepcopy(rich_sway_seats)
        cross_seat_collision[1]["devices"].append(  # type: ignore[union-attr]
            copy.deepcopy(rich_sway_seats[0]["devices"][1])  # type: ignore[index]
        )
        with self.assertRaisesRegex(da.AcceptanceError, "claimed by foreign seat 'seat1'"):
            da.verify_seat0_input_state(cross_seat_collision, rich_sway_inputs)

        # Mismatched name, mismatched type, or missing member in get_inputs must fail closed.
        name_mismatch_inputs = copy.deepcopy(rich_sway_inputs)
        name_mismatch_inputs[1]["name"] = "tampered_keyboard_name"
        with self.assertRaisesRegex(da.AcceptanceError, "name mismatch"):
            da.verify_seat0_input_state(rich_sway_seats, name_mismatch_inputs)

        type_mismatch_inputs = copy.deepcopy(rich_sway_inputs)
        type_mismatch_inputs[1]["type"] = "pointer"
        with self.assertRaisesRegex(da.AcceptanceError, "type mismatch"):
            da.verify_seat0_input_state(rich_sway_seats, type_mismatch_inputs)

        missing_member_inputs = [copy.deepcopy(rich_sway_inputs[0])]
        with self.assertRaisesRegex(da.AcceptanceError, "missing from sway get_inputs"):
            da.verify_seat0_input_state(rich_sway_seats, missing_member_inputs)

    def test_owned_scene_cleanup_isolates_timeout_preserves_failure_and_rejects_dirty_pass(
        self,
    ) -> None:
        spawned_procs: list[subprocess.Popen[str]] = []
        spawned_fixtures: list[da.PersistentGuestInputFixture] = []

        helper_code = "\n".join(
            [
                "import json, os, signal, sys, time",
                "stop = False",
                "signal.signal(signal.SIGTERM, lambda *_: globals().__setitem__('stop', True))",
                "print(json.dumps({",
                "    'event': 'ready',",
                "    'pid': os.getpid(),",
                "    'seat': 'seat0',",
                "    'capabilities': 3,",
                "    'pointer': True,",
                "    'keyboard': True,",
                "    'keymap_format': 'xkb_v1',",
                "    'keymap_bytes': 4096,",
                "    'setup_keypresses_sent': 0,",
                "}), flush=True)",
                "deadline = time.monotonic() + 30.0",
                "while not stop and time.monotonic() < deadline:",
                "    time.sleep(0.05)",
                "sys.exit(0)",
            ]
        )

        def _spawn_sleeper() -> subprocess.Popen[str]:
            proc = subprocess.Popen(
                [sys.executable, "-u", "-c", "import time; time.sleep(30)"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            spawned_procs.append(proc)
            return proc

        def _spawn_ready_fixture(log_path: Path) -> da.PersistentGuestInputFixture:
            fix = da.PersistentGuestInputFixture(
                [sys.executable, "-u", "-c", helper_code],
                env=self._isolated_guest_env(),
                stderr_log_path=log_path,
            )
            fix.start(ready_timeout_sec=3.0)
            spawned_fixtures.append(fix)
            return fix

        class _UnreapablePostKillOwner:
            def __init__(self, pid: int = 99101) -> None:
                self.pid = pid
                self.returncode: int | None = None
                self.stdout = None
                self.calls: list[str] = []

            def poll(self) -> int | None:
                return None

            def terminate(self) -> None:
                self.calls.append("terminate")

            def kill(self) -> None:
                self.calls.append("kill")

            def wait(self, timeout: float | None = None) -> int:
                self.calls.append(f"wait:{timeout}")
                raise subprocess.TimeoutExpired(
                    cmd="unreapable-owned-proc",
                    timeout=timeout or 0.0,
                )

        class _TerminatePermissionErrorOwner:
            def __init__(self, pid: int = 99102) -> None:
                self.pid = pid
                self.returncode: int | None = None
                self.stdout = None

            def poll(self) -> int | None:
                return None

            def terminate(self) -> None:
                raise PermissionError("terminate denied api_key=super-secret-cleanup-key")

            def kill(self) -> None:
                raise PermissionError("kill denied api_key=super-secret-cleanup-key")

            def wait(self, timeout: float | None = None) -> int:
                raise subprocess.TimeoutExpired(
                    cmd="denied-owned-proc",
                    timeout=timeout or 0.0,
                )

        try:
            # 1. Post-kill TimeoutExpired + OSError on earlier owners must NOT abort remaining owners.
            art1 = self.base / "scene-1"
            art1.mkdir(mode=0o700)
            sway_log_1 = (art1 / "sway.log").open("w", encoding="utf-8")
            waybar_log_1 = (art1 / "waybar.log").open("w", encoding="utf-8")
            desktop_log_1 = (art1 / "threadlane-desktop.log").open("w", encoding="utf-8")
            sway_log_1.write("sway-running\n")

            hung_desktop = _UnreapablePostKillOwner()
            denied_waybar = _TerminatePermissionErrorOwner()
            real_fixture_1 = _spawn_ready_fixture(art1 / "persistent-input.log")
            real_sway_1 = _spawn_sleeper()

            cleanup_1 = da.cleanup_owned_scene_processes(
                [
                    ("threadlane_desktop", hung_desktop, 0.1, 0.1),
                    ("waybar", denied_waybar, 0.1, 0.1),
                    ("persistent_input", real_fixture_1, 2.0, 1.0),
                    ("sway", real_sway_1, 2.0, 1.0),
                ],
                log_handles={
                    "sway": sway_log_1,
                    "waybar": waybar_log_1,
                    "threadlane_desktop": desktop_log_1,
                },
            )
            self.assertFalse(cleanup_1["ok"])
            self.assertEqual(hung_desktop.calls[:3], ["terminate", "wait:0.1", "kill"])
            self.assertFalse(cleanup_1["owners"]["threadlane_desktop"]["reaped"])
            self.assertTrue(cleanup_1["owners"]["threadlane_desktop"]["killed_with_sigkill"])
            self.assertFalse(cleanup_1["owners"]["waybar"]["ok"])
            self.assertFalse(real_fixture_1.is_running())
            self.assertTrue(cleanup_1["owners"]["persistent_input"]["reaped"])
            self.assertTrue(cleanup_1["owners"]["persistent_input"]["ok"])
            self.assertIsNotNone(real_sway_1.poll())
            self.assertTrue(cleanup_1["owners"]["sway"]["reaped"])
            self.assertTrue(cleanup_1["owners"]["sway"]["ok"])
            self.assertTrue(sway_log_1.closed)
            self.assertTrue(waybar_log_1.closed)
            self.assertTrue(desktop_log_1.closed)
            joined_errors = "\n".join(cleanup_1["errors"])
            self.assertNotIn("super-secret-cleanup-key", joined_errors)
            self.assertIn("[REDACTED]", joined_errors)

            # 2. When scene already failed, record_owned_scene_failure captures diagnostics before
            # stopping Sway, cleans up remaining owners despite TimeoutExpired, and preserves original error.
            art2 = self.base / "scene-2"
            art2.mkdir(mode=0o700)
            sway_log_2 = (art2 / "sway.log").open("w", encoding="utf-8")
            waybar_log_2 = (art2 / "waybar.log").open("w", encoding="utf-8")
            desktop_log_2 = (art2 / "threadlane-desktop.log").open("w", encoding="utf-8")
            desktop_log_2.write("outside-click-trace\n")

            hung_desktop_2 = _UnreapablePostKillOwner(pid=99201)
            real_waybar_2 = _spawn_sleeper()
            real_fixture_2 = _spawn_ready_fixture(art2 / "persistent-input.log")
            real_sway_2 = _spawn_sleeper()
            sway_alive_at_diag: list[bool] = []

            def _diag_runner(
                argv: list[str],
                **_kwargs: object,
            ) -> subprocess.CompletedProcess[str]:
                sway_alive_at_diag.append(real_sway_2.poll() is None)
                if argv[0] == "grim":
                    Path(argv[3]).write_bytes(b"\x89PNG\r\n\x1a\nstub")
                    return subprocess.CompletedProcess(argv, 0, "", "")
                return subprocess.CompletedProcess(argv, 0, "[]", "")

            report_2_path = art2 / "vm-acceptance-report.json"
            original_exc = RuntimeError("Popup remained visible after outside click")
            failed_doc = da.record_owned_scene_failure(
                report_path=report_2_path,
                exc=original_exc,
                compositor_invariants={"focus_follows_mouse": "yes"},
                checks={"click_outside_dismiss": False},
                artifact_dir=art2,
                client_env={
                    **self._isolated_guest_env(),
                    "SWAYSOCK": str(self.runtime_dir / "sway-ipc.sock"),
                },
                owned_log_paths={
                    "sway": art2 / "sway.log",
                    "waybar": art2 / "waybar.log",
                    "persistent_input": art2 / "persistent-input.log",
                    "threadlane_desktop": art2 / "threadlane-desktop.log",
                },
                owners=[
                    ("threadlane_desktop", hung_desktop_2, 0.1, 0.1),
                    ("waybar", real_waybar_2, 2.0, 1.0),
                    ("persistent_input", real_fixture_2, 2.0, 1.0),
                    ("sway", real_sway_2, 2.0, 1.0),
                ],
                log_handles={
                    "sway": sway_log_2,
                    "waybar": waybar_log_2,
                    "threadlane_desktop": desktop_log_2,
                },
                command_runner=_diag_runner,
            )
            self.assertTrue(sway_alive_at_diag)
            self.assertTrue(all(sway_alive_at_diag))
            self.assertIsNotNone(real_waybar_2.poll())
            self.assertFalse(real_fixture_2.is_running())
            self.assertIsNotNone(real_sway_2.poll())
            self.assertEqual(failed_doc["status"], "failed")
            self.assertEqual(
                failed_doc["error"],
                "Popup remained visible after outside click",
            )
            self.assertFalse(failed_doc["scene_cleanup"]["ok"])
            self.assertTrue(failed_doc["scene_cleanup"]["owners"]["sway"]["reaped"])
            self.assertTrue(
                failed_doc["scene_cleanup"]["owners"]["persistent_input"]["reaped"]
            )
            saved_2 = json.loads(report_2_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_2["status"], "failed")
            self.assertEqual(
                saved_2["error"],
                "Popup remained visible after outside click",
            )

            # 3. If scene otherwise succeeded but cleanup fails, refuse passed qualification and
            # overwrite any stale report with honest failed status.
            art3 = self.base / "scene-3"
            art3.mkdir(mode=0o700)
            report_3_path = art3 / "vm-acceptance-report.json"
            report_3_path.write_text('{"status": "passed"}\n', encoding="utf-8")
            sway_log_3 = (art3 / "sway.log").open("w", encoding="utf-8")
            waybar_log_3 = (art3 / "waybar.log").open("w", encoding="utf-8")
            desktop_log_3 = (art3 / "threadlane-desktop.log").open("w", encoding="utf-8")

            exited_desktop_3 = subprocess.Popen(
                [sys.executable, "-c", "raise SystemExit(0)"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            spawned_procs.append(exited_desktop_3)
            exited_desktop_3.wait(timeout=3.0)
            real_waybar_3 = _spawn_sleeper()
            real_fixture_3 = _spawn_ready_fixture(art3 / "persistent-input.log")
            hung_sway_3 = _UnreapablePostKillOwner(pid=99301)

            with self.assertRaisesRegex(
                da.SceneCleanupError,
                "Owned scene cleanup failed after acceptance checks",
            ) as cleanup_exc_ctx:
                da.complete_owned_scene_and_write_passed_report(
                    report_path=report_3_path,
                    report_payload={
                        "schema_version": 2,
                        "status": "passed",
                        "desktop_exit_code": 0,
                        "compositor_invariants": {"focus_follows_mouse": "yes"},
                        "guest_input": {"seat": "seat0"},
                        "checks": {key: True for key in da.REQUIRED_VM_CHECKS},
                    },
                    owners=[
                        ("threadlane_desktop", exited_desktop_3, 1.0, 0.5),
                        ("waybar", real_waybar_3, 2.0, 1.0),
                        ("persistent_input", real_fixture_3, 2.0, 1.0),
                        ("sway", hung_sway_3, 0.1, 0.1),
                    ],
                    log_handles={
                        "sway": sway_log_3,
                        "waybar": waybar_log_3,
                        "threadlane_desktop": desktop_log_3,
                    },
                )
            self.assertIsNotNone(real_waybar_3.poll())
            self.assertFalse(real_fixture_3.is_running())
            self.assertFalse(cleanup_exc_ctx.exception.scene_cleanup["ok"])
            saved_3 = json.loads(report_3_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_3["status"], "failed")
            with self.assertRaisesRegex(da.AcceptanceError, "did not pass"):
                da.verify_vm_acceptance_report(report_3_path)

            # 4. Clean 4-owner shutdown writes passed report with scene_cleanup and cleanup_shutdown.
            art4 = self.base / "scene-4"
            art4.mkdir(mode=0o700)
            report_4_path = art4 / "vm-acceptance-report.json"
            sway_log_4 = (art4 / "sway.log").open("w", encoding="utf-8")
            waybar_log_4 = (art4 / "waybar.log").open("w", encoding="utf-8")
            desktop_log_4 = (art4 / "threadlane-desktop.log").open("w", encoding="utf-8")

            exited_desktop_4 = subprocess.Popen(
                [sys.executable, "-c", "raise SystemExit(0)"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            spawned_procs.append(exited_desktop_4)
            exited_desktop_4.wait(timeout=3.0)
            real_waybar_4 = _spawn_sleeper()
            real_fixture_4 = _spawn_ready_fixture(art4 / "persistent-input.log")
            real_sway_4 = _spawn_sleeper()

            clean_res = da.complete_owned_scene_and_write_passed_report(
                report_path=report_4_path,
                report_payload={
                    "schema_version": 2,
                    "status": "passed",
                    "desktop_exit_code": 0,
                    "compositor_invariants": {"focus_follows_mouse": "yes"},
                    "guest_input": {"seat": "seat0"},
                    "checks": {key: True for key in da.REQUIRED_VM_CHECKS},
                },
                owners=[
                    ("threadlane_desktop", exited_desktop_4, 1.0, 0.5),
                    ("waybar", real_waybar_4, 2.0, 1.0),
                    ("persistent_input", real_fixture_4, 2.0, 1.0),
                    ("sway", real_sway_4, 2.0, 1.0),
                ],
                log_handles={
                    "sway": sway_log_4,
                    "waybar": waybar_log_4,
                    "threadlane_desktop": desktop_log_4,
                },
            )
            self.assertTrue(clean_res["ok"])
            saved_4 = json.loads(report_4_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_4["status"], "passed")
            self.assertTrue(saved_4["scene_cleanup"]["ok"])
            self.assertTrue(saved_4["guest_input"]["cleanup_shutdown"]["ok"])
        finally:
            for fix in spawned_fixtures:
                fix.stop(term_timeout_sec=0.5, kill_timeout_sec=0.5)
            for proc in spawned_procs:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=1.0)


def _fill_rgb_rect(
    buf: bytearray,
    *,
    img_w: int,
    img_h: int,
    x0: int,
    y0: int,
    x1: int,
    y1: int,
    rgb: tuple[int, int, int],
) -> None:
    cx0 = max(0, min(img_w, x0))
    cx1 = max(cx0, min(img_w, x1))
    cy0 = max(0, min(img_h, y0))
    cy1 = max(cy0, min(img_h, y1))
    if cx1 <= cx0 or cy1 <= cy0:
        return
    row_bytes = bytes(rgb) * (cx1 - cx0)
    for py in range(cy0, cy1):
        base = (py * img_w + cx0) * 3
        buf[base : base + len(row_bytes)] = row_bytes


def _build_painted_main_frame(
    width: int,
    height: int,
    *,
    light_theme: bool = False,
    paint_workspace_body: bool = True,
) -> bytes:
    if light_theme:
        bg_base = (244, 244, 245)
        bg_chrome = (237, 237, 240)
        bg_panel = (255, 255, 255)
        bg_elevated = (220, 220, 224)
        fg_primary = (24, 24, 27)
        fg_muted = (82, 82, 91)
        accent = (2, 132, 199)
    else:
        bg_base = (18, 18, 21)
        bg_chrome = (21, 21, 25)
        bg_panel = (24, 24, 28)
        bg_elevated = (36, 36, 43)
        fg_primary = (244, 244, 246)
        fg_muted = (156, 163, 175)
        accent = (56, 189, 248)

    buf = bytearray(bytes(bg_base) * (width * height))

    # Titlebar chrome + brand logo + "Threadlane" glyph strokes + window control icons
    _fill_rgb_rect(buf, img_w=width, img_h=height, x0=0, y0=0, x1=width, y1=42, rgb=bg_chrome)
    _fill_rgb_rect(buf, img_w=width, img_h=height, x0=24, y0=12, x1=42, y1=30, rgb=accent)
    _fill_rgb_rect(buf, img_w=width, img_h=height, x0=50, y0=15, x1=136, y1=27, rgb=fg_primary)
    _fill_rgb_rect(
        buf,
        img_w=width,
        img_h=height,
        x0=width - 108,
        y0=16,
        x1=width - 28,
        y1=26,
        rgb=fg_muted,
    )

    if width >= 761:
        # Wide inline sidebar (0..272)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=0, y0=42, x1=272, y1=height, rgb=bg_chrome)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=20, y0=60, x1=248, y1=98, rgb=bg_elevated)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=34, y0=73, x1=128, y1=85, rgb=fg_primary)
        for nav_y in (112, 156, 200, 260):
            _fill_rgb_rect(
                buf,
                img_w=width,
                img_h=height,
                x0=34,
                y0=nav_y,
                x1=164,
                y1=nav_y + 10,
                rgb=fg_muted,
            )
        body_x0 = 304
        body_y0 = 64
    else:
        # Narrow compact navigation bar (y: 42..96)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=0, y0=42, x1=width, y1=96, rgb=bg_chrome)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=18, y0=50, x1=92, y1=88, rgb=bg_elevated)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=28, y0=64, x1=80, y1=75, rgb=fg_primary)
        _fill_rgb_rect(buf, img_w=width, img_h=height, x0=108, y0=64, x1=330, y1=74, rgb=fg_muted)
        body_x0 = 24
        body_y0 = 116

    if paint_workspace_body:
        _fill_rgb_rect(
            buf,
            img_w=width,
            img_h=height,
            x0=body_x0,
            y0=body_y0,
            x1=min(width - 24, body_x0 + 180),
            y1=body_y0 + 16,
            rgb=fg_primary,
        )
        _fill_rgb_rect(
            buf,
            img_w=width,
            img_h=height,
            x0=body_x0,
            y0=body_y0 + 28,
            x1=width - 28,
            y1=body_y0 + 38,
            rgb=fg_muted,
        )
        _fill_rgb_rect(
            buf,
            img_w=width,
            img_h=height,
            x0=body_x0,
            y0=body_y0 + 64,
            x1=width - 24,
            y1=min(height - 32, body_y0 + 190),
            rgb=bg_panel,
        )
        _fill_rgb_rect(
            buf,
            img_w=width,
            img_h=height,
            x0=body_x0 + 16,
            y0=body_y0 + 86,
            x1=min(width - 40, body_x0 + 220),
            y1=body_y0 + 98,
            rgb=fg_primary,
        )

    return bytes(buf)


class DesktopAcceptancePaintReadinessAndDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        short_parent = (
            "/tmp"
            if Path("/tmp").is_dir() and not Path("/tmp").is_symlink()
            else None
        )
        self._tmp = tempfile.TemporaryDirectory(dir=short_parent)
        self.base = Path(self._tmp.name).resolve()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_paint_readiness_rejects_fallback_rectangles_stale_resize_and_partial_paint(
        self,
    ) -> None:
        output_rect = {"x": 0, "y": 0, "width": 1920, "height": 1080}
        node_1040 = {
            "visible": True,
            "shell": "xdg_shell",
            "rect": {"x": 440, "y": 176, "width": 1040, "height": 760},
        }
        node_360 = {
            "visible": True,
            "shell": "xdg_shell",
            "rect": {"x": 780, "y": 306, "width": 360, "height": 500},
        }

        # 1. Pure solid fallback rectangles (dark, white, and bordered GTK gray) must fail.
        for solid_rgb in ((18, 18, 21), (255, 255, 255), (246, 245, 244)):
            fallback_buf = bytearray(bytes(solid_rgb) * (360 * 500))
            # Even adding a 3px outer border/corner ring must not fool interior contrast checks.
            _fill_rgb_rect(
                fallback_buf,
                img_w=360,
                img_h=500,
                x0=0,
                y0=0,
                x1=360,
                y1=3,
                rgb=(90, 90, 96),
            )
            with self.assertRaisesRegex(da.AcceptanceError, "unpainted/fallback rectangle"):
                da.evaluate_main_window_paint_readiness(
                    tree_node=node_360,
                    expected_width=360,
                    expected_height=500,
                    output_rect=output_rect,
                    output_scale=1.0,
                    bar_px_h=32,
                    changed_pixels=360 * 500,
                    bbox_ws=(780, 274, 780 + 360, 274 + 500),
                    window_rgb_bytes=bytes(fallback_buf),
                    screenshot_name="02-main-explicit-resize-360x500.png",
                )

        # 2. Stale 1040x760 buffer when sway get_tree already reports 360x500 must fail.
        stale_wide_rgb = _build_painted_main_frame(1040, 760)
        with self.assertRaisesRegex(da.AcceptanceError, "Rendered main window logical size"):
            da.evaluate_main_window_paint_readiness(
                tree_node=node_360,
                expected_width=360,
                expected_height=500,
                output_rect=output_rect,
                output_scale=1.0,
                bar_px_h=32,
                changed_pixels=1040 * 760,
                bbox_ws=(440, 144, 440 + 1040, 144 + 760),
                window_rgb_bytes=stale_wide_rgb,
                screenshot_name="02-main-explicit-resize-360x500.png",
            )

        # 3. Partial paint (titlebar + sidebar painted, workspace_body still blank) must fail.
        half_painted_rgb = _build_painted_main_frame(
            1040,
            760,
            paint_workspace_body=False,
        )
        with self.assertRaisesRegex(da.AcceptanceError, "workspace_body"):
            da.evaluate_main_window_paint_readiness(
                tree_node=node_1040,
                expected_width=1040,
                expected_height=760,
                output_rect=output_rect,
                output_scale=1.0,
                bar_px_h=32,
                changed_pixels=1040 * 760,
                bbox_ws=(440, 144, 440 + 1040, 144 + 760),
                window_rgb_bytes=half_painted_rgb,
                screenshot_name="01-main-default-1040x760.png",
            )

        # 4. Fully painted dark and light frames for 1040x760 and 360x500 pass cleanly.
        for is_light in (False, True):
            wide_ready = da.evaluate_main_window_paint_readiness(
                tree_node=node_1040,
                expected_width=1040,
                expected_height=760,
                output_rect=output_rect,
                output_scale=1.0,
                bar_px_h=32,
                changed_pixels=1040 * 760 - 120,
                bbox_ws=(440, 144, 440 + 1040, 144 + 760),
                window_rgb_bytes=_build_painted_main_frame(
                    1040,
                    760,
                    light_theme=is_light,
                ),
                screenshot_name="01-main-default-1040x760.png",
            )
            self.assertTrue(wide_ready["painted"])
            self.assertEqual(wide_ready["logical_rect"]["width"], 1040.0)

            narrow_ready = da.evaluate_main_window_paint_readiness(
                tree_node=node_360,
                expected_width=360,
                expected_height=500,
                output_rect=output_rect,
                output_scale=1.0,
                bar_px_h=32,
                changed_pixels=360 * 500 - 80,
                bbox_ws=(780, 274, 780 + 360, 274 + 500),
                window_rgb_bytes=_build_painted_main_frame(
                    360,
                    500,
                    light_theme=is_light,
                ),
                screenshot_name="02-main-explicit-resize-360x500.png",
            )
            self.assertTrue(narrow_ready["painted"])
            self.assertEqual(narrow_ready["logical_rect"]["width"], 360.0)
            self.assertEqual(narrow_ready["logical_rect"]["height"], 500.0)

    def test_bounded_failure_diagnostics_truncates_redacts_and_refuses_private_user_logs(
        self,
    ) -> None:
        artifact_dir = self.base / "vm-artifacts"
        artifact_dir.mkdir(mode=0o700)
        runtime_dir = self.base / "xdg-runtime"
        runtime_dir.mkdir(mode=0o700)

        oversized_log = artifact_dir / "threadlane-desktop.log"
        oversized_log.write_text(
            ("early-line\n" * 3000)
            + "final-line api_key=super-secret-value sk-live1234567890\n",
            encoding="utf-8",
        )
        private_user_log = self.base / "private-home" / ".codex" / "session.log"
        private_user_log.parent.mkdir(parents=True)
        private_user_log.write_text("PRIVATE-USER-CONVERSATION-DATA\n", encoding="utf-8")

        def _fake_runner(
            argv: list[str],
            **_kwargs: object,
        ) -> subprocess.CompletedProcess[str]:
            if argv[0] == "grim":
                out_name = argv[2]
                shot_path = Path(argv[3])
                if out_name == "HEADLESS-1":
                    shot_path.write_bytes(b"\x89PNG\r\n\x1a\nstub")
                    return subprocess.CompletedProcess(argv, 0, "", "")
                return subprocess.CompletedProcess(argv, 1, "", "output unavailable")
            if argv[0] == "swaymsg":
                query = argv[2]
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    json.dumps([{"query": query, "token": "Bearer secret-jwt-token"}]),
                    "",
                )
            raise RuntimeError(f"Unexpected command: {argv}")

        diag = da.collect_bounded_failure_diagnostics(
            artifact_dir=artifact_dir,
            client_env={
                "XDG_RUNTIME_DIR": str(runtime_dir),
                "WAYLAND_DISPLAY": "wayland-1",
                "SWAYSOCK": str(runtime_dir / "sway-ipc.sock"),
            },
            owned_log_paths={
                "threadlane_desktop": oversized_log,
                "private_escape": private_user_log,
            },
            command_runner=_fake_runner,
        )

        self.assertTrue(diag["outputs"]["HEADLESS-1"]["captured"])
        self.assertFalse(diag["outputs"]["HEADLESS-2"]["captured"])
        self.assertTrue(diag["sway_queries"]["tree"]["captured"])
        saved_tree = (artifact_dir / "failure-sway-tree.json").read_text(encoding="utf-8")
        self.assertNotIn("secret-jwt-token", saved_tree)

        desktop_diag = diag["owned_logs"]["threadlane_desktop"]
        self.assertTrue(desktop_diag["present"])
        self.assertTrue(desktop_diag["truncated"])
        tail_saved = (artifact_dir / desktop_diag["tail_file"]).read_text(encoding="utf-8")
        self.assertLessEqual(len(tail_saved.encode("utf-8")), da.MAX_DIAGNOSTIC_LOG_TAIL_BYTES + 64)
        self.assertIn("final-line", tail_saved)
        self.assertNotIn("super-secret-value", tail_saved)
        self.assertNotIn("sk-live1234567890", tail_saved)

        # Private user log outside artifact_dir must be refused and never copied.
        self.assertFalse(diag["owned_logs"]["private_escape"]["present"])
        self.assertIn("outside owned artifact root", diag["owned_logs"]["private_escape"]["error"])
        diag_json = (artifact_dir / "failure-diagnostics.json").read_text(encoding="utf-8")
        self.assertNotIn("PRIVATE-USER-CONVERSATION-DATA", diag_json)

    def test_resolve_vm_store_artifacts_dir_supports_flat_and_nested_driver_layouts(
        self,
    ) -> None:
        flat_store = self.base / "store-flat"
        (flat_store / "vm-artifacts").mkdir(parents=True)
        (flat_store / "vm-artifacts" / "vm-acceptance-report.json").write_text(
            "{}\n",
            encoding="utf-8",
        )
        self.assertEqual(
            da.resolve_vm_store_artifacts_dir(flat_store),
            flat_store / "vm-artifacts",
        )

        nested_store = self.base / "store-nested"
        nested_dir = nested_store / "vm-artifacts" / "vm-acceptance-artifacts"
        nested_dir.mkdir(parents=True)
        (nested_dir / "vm-acceptance-report.json").write_text("{}\n", encoding="utf-8")
        self.assertEqual(da.resolve_vm_store_artifacts_dir(nested_store), nested_dir)

        empty_store = self.base / "store-empty"
        (empty_store / "vm-artifacts").mkdir(parents=True)
        with self.assertRaisesRegex(da.AcceptanceError, "vm-acceptance-report.json"):
            da.resolve_vm_store_artifacts_dir(empty_store)


if __name__ == "__main__":
    unittest.main()
