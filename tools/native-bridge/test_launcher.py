#!/usr/bin/env python3
"""Transport-only unit tests for the unactivated native Codex launcher prototype.

IMPORTANT: All fixtures and results in this file validate local stdio<->Unix
WebSocket framing, process group lifecycle, and filesystem safety invariants
ONLY. They use a synthetic Python fake executable and are NOT proof of actual
native ChatGPT/Codex desktop idle wake or live UI thread delivery.
"""

from __future__ import annotations

import json
import os
import select
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from typing import Any

import launcher

LAUNCHER_PATH = os.path.abspath(launcher.__file__)

FAKE_CODEX_SOURCE = textwrap.dedent(
    """\
    #!__PYTHON_EXECUTABLE__
    # SYNTHETIC TEST FIXTURE ONLY (transport_only=True).
    # Never invokes the real Codex binary, model, or network.
    import asyncio
    import json
    import os
    import signal
    import socket
    import sys

    def record_event(payload):
        log_path = os.environ.get("FAKE_CODEX_LOG_FILE")
        if not log_path:
            return
        entry = dict(payload)
        entry["transport_only"] = True
        with open(log_path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\\n")

    async def run_ws_server(advertised_sock, mode):
        from websockets.asyncio.server import unix_serve
        from websockets.exceptions import ConnectionClosed

        bind_sock = advertised_sock
        if mode == "ws_symlink":
            ext_dir = os.environ["FAKE_CODEX_EXTERNAL_SOCK_DIR"]
            bind_sock = os.path.join(ext_dir, "physical.sock")

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except Exception:
                pass

        async def handler(ws):
            req_headers = {}
            if getattr(ws, "request", None) is not None and getattr(ws.request, "headers", None) is not None:
                for k in ("Upgrade", "Connection", "Sec-WebSocket-Key", "Sec-WebSocket-Version"):
                    val = ws.request.headers.get(k)
                    if val is not None:
                        req_headers[k.lower()] = val
            record_event({
                "event": "handshake",
                "pid": os.getpid(),
                "headers": req_headers,
            })

            if mode == "ws_send_binary":
                try:
                    await ws.recv()
                    await ws.send(b"\\x00\\x01\\x02\\x03")
                    await stop_event.wait()
                except ConnectionClosed:
                    pass
                return

            if mode == "ws_send_newline":
                try:
                    await ws.recv()
                    await ws.send('{"jsonrpc":"2.0","method":"bad"}\\n{"injected":true}')
                    await stop_event.wait()
                except ConnectionClosed:
                    pass
                return

            notif = os.environ.get("FAKE_CODEX_SERVER_NOTIFICATION")
            if notif:
                await ws.send(notif)

            try:
                async for message in ws:
                    record_event({
                        "event": "ws_frame_received",
                        "frame": message,
                        "is_text": isinstance(message, str),
                    })
                    await ws.send(message)
            except ConnectionClosed:
                pass

        os.umask(0o077)
        async with unix_serve(
            handler,
            path=bind_sock,
            max_size=8 * 1024 * 1024,
            compression=None,
        ):
            if mode == "ws_symlink":
                os.symlink(bind_sock, advertised_sock)
            await stop_event.wait()

    def run_raw_byte_relay(advertised_sock):
        os.umask(0o077)
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            srv.bind(advertised_sock)
            srv.listen(1)
            srv.settimeout(3.0)
            conn, _ = srv.accept()
            with conn:
                conn.settimeout(1.0)
                try:
                    _ = conn.recv(256)
                except OSError:
                    pass
                conn.sendall(b'{"raw_byte_relay":true,"not_websocket":true}\\n')
        finally:
            srv.close()

    def main():
        args = sys.argv[1:]
        mode = os.environ.get("FAKE_CODEX_MODE", "ws_echo")
        record_event({
            "event": "spawn",
            "pid": os.getpid(),
            "pgid": os.getpgrp(),
            "argv": args,
        })

        if (
            mode != "passthrough"
            and "app-server" in args
            and len(args) >= 3
            and args[-2] == "--listen"
            and args[-1].startswith("unix://")
        ):
            sock_path = args[-1][len("unix://"):]
            if mode == "early_exit":
                code = int(os.environ.get("FAKE_CODEX_EXIT_CODE", "42"))
                sys.exit(code)
            if mode == "raw_byte_relay":
                run_raw_byte_relay(sock_path)
                sys.exit(0)
            asyncio.run(run_ws_server(sock_path, mode))
            sys.exit(0)

        record_event({
            "event": "passthrough",
            "pid": os.getpid(),
            "argv": args,
        })
        sys.stdout.write(
            json.dumps({"passthrough_argv": args, "transport_only": True}) + "\\n"
        )
        sys.stdout.flush()
        sys.exit(0)

    if __name__ == "__main__":
        main()
    """
)


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _wait_for_pid_exit(pid: int, timeout: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.02)
    return not _pid_exists(pid)


class LauncherPrototypeTest(unittest.TestCase):
    """Transport-only test suite for launcher.py (not native UI idle wake proof)."""

    def setUp(self) -> None:
        raw_base = tempfile.mkdtemp(prefix="tl-proto-test-")
        self.test_root = os.path.realpath(raw_base)
        os.chmod(self.test_root, 0o700)

        self.runtime_dir = os.path.join(self.test_root, "rt")
        os.mkdir(self.runtime_dir, 0o700)
        os.chmod(self.runtime_dir, 0o700)

        self.bin_dir = os.path.join(self.test_root, "bin")
        os.mkdir(self.bin_dir, 0o700)

        self.fake_codex = os.path.join(self.bin_dir, "fake-codex")
        with open(self.fake_codex, "w", encoding="utf-8") as handle:
            handle.write(
                FAKE_CODEX_SOURCE.replace(
                    "__PYTHON_EXECUTABLE__", sys.executable
                )
            )
        os.chmod(self.fake_codex, 0o755)

        self.log_file = os.path.join(self.test_root, "fake_events.jsonl")

    def tearDown(self) -> None:
        shutil.rmtree(self.test_root, ignore_errors=True)

    def _make_env(self, **overrides: str | None) -> dict[str, str]:
        env = dict(os.environ)
        env["THREADLANE_REAL_CODEX"] = self.fake_codex
        env["XDG_RUNTIME_DIR"] = self.runtime_dir
        env["FAKE_CODEX_LOG_FILE"] = self.log_file
        env.pop("FAKE_CODEX_MODE", None)
        env.pop("FAKE_CODEX_EXIT_CODE", None)
        env.pop("FAKE_CODEX_SERVER_NOTIFICATION", None)
        env.pop("FAKE_CODEX_EXTERNAL_SOCK_DIR", None)
        for key, value in overrides.items():
            if value is None:
                env.pop(key, None)
            else:
                env[key] = value
        return env

    def _read_events(self) -> list[dict[str, Any]]:
        if not os.path.exists(self.log_file):
            return []
        events: list[dict[str, Any]] = []
        with open(self.log_file, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    events.append(json.loads(line))
        return events

    def _wait_for_descriptor_line(
        self, proc: subprocess.Popen[bytes], timeout: float = 5.0
    ) -> str:
        assert proc.stderr is not None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = proc.stderr.readline()
            if line:
                decoded = line.decode("utf-8", errors="replace").strip()
                prefix = "threadlane-prototype-descriptor: "
                if decoded.startswith(prefix):
                    return decoded[len(prefix) :]
            if proc.poll() is not None:
                break
            time.sleep(0.01)
        self.fail("Timed out waiting for descriptor diagnostic line on stderr")

    def _echo_before_eof(
        self, argv: list[str], frame: str, env: dict[str, str]
    ) -> subprocess.CompletedProcess[bytes]:
        """Read the full echo before EOF deliberately closes the WebSocket."""
        proc = subprocess.Popen(
            [sys.executable, LAUNCHER_PATH, *argv],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        try:
            self._wait_for_descriptor_line(proc)
            assert proc.stdin is not None and proc.stdout is not None
            expected = (frame + "\n").encode("utf-8")
            proc.stdin.write(expected)
            proc.stdin.flush()
            echoed = bytearray()
            deadline = time.monotonic() + 5.0
            while len(echoed) < len(expected):
                remaining = deadline - time.monotonic()
                self.assertGreater(remaining, 0, "Timed out waiting for echo")
                ready, _, _ = select.select([proc.stdout], [], [], remaining)
                self.assertTrue(ready, "Timed out waiting for echo")
                chunk = os.read(proc.stdout.fileno(), len(expected) - len(echoed))
                self.assertTrue(chunk, "Unexpected EOF before complete echo")
                echoed.extend(chunk)
            proc.stdin.close()
            proc.stdin = None
            rest, errors = proc.communicate(timeout=5.0)
            return subprocess.CompletedProcess(
                proc.args, proc.returncode, bytes(echoed) + rest, errors
            )
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5.0)
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

    def test_websocket_handshake_and_roundtrip_frames(self) -> None:
        """Verify RFC6455 WS handshake, single backend launch, and unchanged frame round-trip."""
        server_notif = (
            '{"jsonrpc":"2.0","method":"codex/event",'
            '"params":{"transport_only":true,"kind":"server_notification","seq":1}}'
        )
        env = self._make_env(FAKE_CODEX_SERVER_NOTIFICATION=server_notif)

        proc = subprocess.Popen(
            [sys.executable, LAUNCHER_PATH, "app-server"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        try:
            descriptor_path = self._wait_for_descriptor_line(proc)
            self.assertTrue(os.path.exists(descriptor_path))
            desc_stat = os.lstat(descriptor_path)
            self.assertEqual(stat.S_IMODE(desc_stat.st_mode), 0o600)

            with open(descriptor_path, "r", encoding="utf-8") as handle:
                descriptor = json.load(handle)

            self.assertEqual(descriptor["schema_version"], 1)
            self.assertEqual(descriptor["wrapper_pid"], proc.pid)
            self.assertEqual(descriptor["real_codex"], self.fake_codex)
            self.assertEqual(descriptor["transport"], "unix_websocket")
            backend_pid = int(descriptor["backend_pid"])
            socket_path = str(descriptor["socket_path"])
            self.assertTrue(socket_path.startswith(self.runtime_dir + os.sep))
            self.assertLess(len(os.fsencode(socket_path)), 100)
            self.assertTrue(_pid_exists(backend_pid))

            frames_to_send = [
                '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"chatgpt-native","version":"26.1027.30824"},"capabilities":{}}}',
                '{"jsonrpc":"2.0","method":"initialized","params":{}}',
                '{"jsonrpc":"2.0","id":1,"result":{"serverInfo":{"name":"codex-fake","version":"0.166.0-alpha.5.1"}}}',
                '{"jsonrpc":"2.0","id":"req-2","method":"thread/sendUserMessage","params":{"text":"hello","transport_only":true}}',
                '{"jsonrpc":"2.0","id":"req-2","result":{"accepted":true,"transport_only":true}}',
                '{"jsonrpc":"2.0","method":"client/userNotification","params":{"transport_only":true,"active":true}}',
            ]

            assert proc.stdin is not None
            assert proc.stdout is not None
            for frame in frames_to_send:
                proc.stdin.write((frame + "\n").encode("utf-8"))
            proc.stdin.flush()

            expected_count = 1 + len(frames_to_send)
            received_stdout_lines: list[str] = []
            for _ in range(expected_count):
                raw = proc.stdout.readline()
                self.assertTrue(raw, "Unexpected EOF on launcher stdout")
                self.assertTrue(raw.endswith(b"\n"))
                received_stdout_lines.append(raw[:-1].decode("utf-8"))

            self.assertEqual(
                received_stdout_lines,
                [server_notif, *frames_to_send],
            )

            proc.stdin.close()
            rc = proc.wait(timeout=5.0)
            self.assertEqual(rc, 0)

            extra_stdout = proc.stdout.read()
            self.assertEqual(extra_stdout, b"")

            self.assertTrue(_wait_for_pid_exit(backend_pid))
            self.assertFalse(os.path.exists(descriptor_path))
            self.assertFalse(os.path.lexists(socket_path))
            self.assertEqual(os.listdir(self.runtime_dir), [])

            events = self._read_events()
            spawns = [e for e in events if e.get("event") == "spawn"]
            self.assertEqual(len(spawns), 1)
            self.assertEqual(
                spawns[0]["argv"],
                ["app-server", "--listen", f"unix://{socket_path}"],
            )

            handshakes = [e for e in events if e.get("event") == "handshake"]
            self.assertEqual(len(handshakes), 1)
            headers = handshakes[0]["headers"]
            self.assertEqual(headers.get("upgrade", "").lower(), "websocket")
            self.assertIn("upgrade", headers.get("connection", "").lower())
            self.assertEqual(headers.get("sec-websocket-version"), "13")
            self.assertTrue(headers.get("sec-websocket-key"))
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2.0)
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

    def test_stdio_listen_variants_intercepted(self) -> None:
        """Verify ['app-server','--listen','stdio://'], ['app-server','--listen=stdio://'], and ['app-server','--stdio'] are bridged."""
        for variant in (
            ["app-server", "--listen", "stdio://"],
            ["app-server", "--listen=stdio://"],
            ["app-server", "--stdio"],
        ):
            with self.subTest(variant=variant):
                if os.path.exists(self.log_file):
                    os.unlink(self.log_file)
                frame = '{"jsonrpc":"2.0","id":9,"method":"ping"}'
                res = self._echo_before_eof(variant, frame, self._make_env())
                self.assertEqual(res.returncode, 0)
                self.assertEqual(res.stdout.decode("utf-8"), frame + "\n")
                self.assertEqual(os.listdir(self.runtime_dir), [])
                events = self._read_events()
                spawns = [e for e in events if e.get("event") == "spawn"]
                self.assertEqual(len(spawns), 1)

    def test_prefixed_native_shape_roundtrip_and_argv_preservation(self) -> None:
        """Verify native prefixed shape (-c <opaque1> -c <opaque2> app-server --analytics-default-enabled) preserves argv and bridges WS."""
        opaque_1 = 'synthetic_opaque_1="alpha:1=true"'
        opaque_2 = 'synthetic_opaque_2={nested=["app-server","--stdio"],count=2}'
        native_argv = [
            "-c",
            opaque_1,
            "-c",
            opaque_2,
            "app-server",
            "--analytics-default-enabled",
        ]
        server_notif = (
            '{"jsonrpc":"2.0","method":"codex/event",'
            '"params":{"transport_only":true,"kind":"prefixed_native_notif"}}'
        )
        env = self._make_env(FAKE_CODEX_SERVER_NOTIFICATION=server_notif)

        proc = subprocess.Popen(
            [sys.executable, LAUNCHER_PATH, *native_argv],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        try:
            descriptor_path = self._wait_for_descriptor_line(proc)
            with open(descriptor_path, "r", encoding="utf-8") as handle:
                descriptor = json.load(handle)
            backend_pid = int(descriptor["backend_pid"])
            socket_path = str(descriptor["socket_path"])
            self.assertTrue(_pid_exists(backend_pid))

            req_frame = '{"jsonrpc":"2.0","id":101,"method":"initialize","params":{"transport_only":true}}'
            assert proc.stdin is not None
            assert proc.stdout is not None
            proc.stdin.write((req_frame + "\n").encode("utf-8"))
            proc.stdin.flush()

            line1 = proc.stdout.readline().decode("utf-8").rstrip("\n")
            line2 = proc.stdout.readline().decode("utf-8").rstrip("\n")
            self.assertEqual([line1, line2], [server_notif, req_frame])

            proc.stdin.close()
            rc = proc.wait(timeout=5.0)
            self.assertEqual(rc, 0)
            self.assertEqual(proc.stdout.read(), b"")
            self.assertTrue(_wait_for_pid_exit(backend_pid))
            self.assertFalse(os.path.exists(descriptor_path))
            self.assertFalse(os.path.lexists(socket_path))
            self.assertEqual(os.listdir(self.runtime_dir), [])

            events = self._read_events()
            spawns = [e for e in events if e.get("event") == "spawn"]
            self.assertEqual(len(spawns), 1)
            self.assertEqual(
                spawns[0]["argv"],
                [
                    "-c",
                    opaque_1,
                    "-c",
                    opaque_2,
                    "app-server",
                    "--analytics-default-enabled",
                    "--listen",
                    f"unix://{socket_path}",
                ],
            )
            handshakes = [e for e in events if e.get("event") == "handshake"]
            self.assertEqual(len(handshakes), 1)
            self.assertEqual(
                handshakes[0]["headers"].get("upgrade", "").lower(),
                "websocket",
            )
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2.0)
            for stream in (proc.stdin, proc.stdout, proc.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

    def test_plan_backend_argv_and_negative_passthrough_cases(self) -> None:
        """Verify pure argv planner whitelist/order preservation and negative passthrough cases."""
        sock = os.path.join(self.runtime_dir, "s.sock")

        self.assertEqual(
            launcher.plan_backend_argv(
                [
                    "-c",
                    "app-server",
                    "--config",
                    "opaque_cfg",
                    "--config=opaque_eq",
                    "--enable",
                    "feat_a",
                    "--disable",
                    "feat_b",
                    "--strict-config",
                    "app-server",
                    "--analytics-default-enabled",
                    "--strict-config",
                    "-c",
                    "suffix_cfg",
                    "--listen=stdio://",
                ],
                sock,
            ),
            [
                "-c",
                "app-server",
                "--config",
                "opaque_cfg",
                "--config=opaque_eq",
                "--enable",
                "feat_a",
                "--disable",
                "feat_b",
                "--strict-config",
                "app-server",
                "--analytics-default-enabled",
                "--strict-config",
                "-c",
                "suffix_cfg",
                "--listen",
                f"unix://{sock}",
            ],
        )

        sample_sock = os.path.join(self.test_root, "neg-passthrough.sock")
        missing_rt = os.path.join(self.test_root, "neg-nonexistent-rt")
        negative_cases = [
            [],
            ["-c", "app-server"],
            ["--config", "app-server"],
            ["--enable", "app-server"],
            ["--disable", "app-server"],
            ["-c", "app-server", "exec", "noop"],
            ["-c"],
            ["--config"],
            ["--config="],
            ["--enable"],
            ["--disable"],
            ["app-server", "-c"],
            ["app-server", "--config="],
            ["app-server", "--listen"],
            ["--unknown-global", "app-server"],
            ["-c", "k=v", "app-server", "--unknown-flag"],
            ["app-server", "--model", "o4-mini"],
            ["app-server", "--help"],
            ["app-server", "-h"],
            ["app-server", "--print-startup-config"],
            ["app-server", "startup-sign-out"],
            ["app-server", "proxy", "--sock", sample_sock],
            ["app-server", "--listen", f"unix://{sample_sock}"],
            ["app-server", f"--listen=unix://{sample_sock}"],
            ["app-server", "--listen", "ws://127.0.0.1:9999"],
            ["app-server", "--stdio", "--stdio"],
            ["app-server", "--stdio", "--listen", "stdio://"],
            ["app-server", "--listen", "stdio://", "--listen=stdio://"],
        ]

        for argv in negative_cases:
            with self.subTest(argv=argv):
                self.assertIsNone(launcher.plan_backend_argv(argv, sock))
                self.assertFalse(launcher.is_intercepted_argv(argv))
                if not argv:
                    continue
                if os.path.exists(self.log_file):
                    os.unlink(self.log_file)
                env = self._make_env(
                    XDG_RUNTIME_DIR=missing_rt,
                    FAKE_CODEX_MODE="passthrough",
                )
                res = subprocess.run(
                    [sys.executable, LAUNCHER_PATH, *argv],
                    input=b"",
                    capture_output=True,
                    env=env,
                    timeout=5.0,
                    check=False,
                )
                self.assertEqual(res.returncode, 0)
                self.assertEqual(res.stderr, b"")
                payload = json.loads(res.stdout.decode("utf-8"))
                self.assertEqual(payload["passthrough_argv"], argv)
                self.assertFalse(os.path.exists(missing_rt))
                self.assertFalse(os.path.lexists(sample_sock))
                events = self._read_events()
                self.assertEqual(len(events), 2)
                self.assertEqual(events[0]["argv"], argv)
                self.assertEqual(events[1]["event"], "passthrough")
                self.assertEqual(events[1]["argv"], argv)

    def test_raw_non_websocket_socket_rejected(self) -> None:
        """Prove a raw byte relay socket without RFC6455 HTTP Upgrade handshake fails closed."""
        env = self._make_env(FAKE_CODEX_MODE="raw_byte_relay")
        res = subprocess.run(
            [sys.executable, LAUNCHER_PATH, "app-server"],
            input=b'{"jsonrpc":"2.0","id":1,"method":"initialize"}\n',
            capture_output=True,
            env=env,
            timeout=5.0,
            check=False,
        )
        self.assertNotEqual(res.returncode, 0)
        self.assertEqual(res.stdout, b"")
        self.assertEqual(
            res.stderr.decode("utf-8"), launcher.FIXED_ERROR_DIAGNOSTIC
        )
        self.assertEqual(os.listdir(self.runtime_dir), [])
        events = self._read_events()
        spawns = [e for e in events if e.get("event") == "spawn"]
        self.assertEqual(len(spawns), 1)

    def test_unrelated_passthrough_argv(self) -> None:
        """Verify non-intercepted commands exec real binary directly without creating session dirs."""
        missing_rt = os.path.join(self.test_root, "nonexistent-runtime-dir")
        sample_sock = os.path.join(self.test_root, "custom-passthrough.sock")
        passthrough_cases = [
            ["--version"],
            ["exec", "--model", "o4-mini", "--full-auto", "echo hi"],
            ["app-server", "--help"],
            ["app-server", "--listen", f"unix://{sample_sock}"],
            ["app-server", "proxy", "--sock", sample_sock],
        ]
        for argv in passthrough_cases:
            with self.subTest(argv=argv):
                if os.path.exists(self.log_file):
                    os.unlink(self.log_file)
                env = self._make_env(
                    XDG_RUNTIME_DIR=missing_rt,
                    FAKE_CODEX_MODE="passthrough",
                )
                res = subprocess.run(
                    [sys.executable, LAUNCHER_PATH, *argv],
                    input=b"",
                    capture_output=True,
                    env=env,
                    timeout=5.0,
                    check=False,
                )
                self.assertEqual(res.returncode, 0)
                self.assertEqual(res.stderr, b"")
                payload = json.loads(res.stdout.decode("utf-8"))
                self.assertEqual(payload["passthrough_argv"], argv)
                self.assertFalse(os.path.exists(missing_rt))
                self.assertFalse(os.path.lexists(sample_sock))
                self.assertEqual(os.listdir(self.runtime_dir), [])
                events = self._read_events()
                self.assertEqual(len(events), 2)
                self.assertEqual(events[0]["event"], "spawn")
                self.assertEqual(events[0]["argv"], argv)
                self.assertEqual(events[1]["event"], "passthrough")
                self.assertEqual(events[1]["argv"], argv)

    def test_signal_cleanup_terminates_backend_and_removes_socket(self) -> None:
        """Verify SIGTERM and SIGINT cleanly terminate backend process group and remove session files."""
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            with self.subTest(sig=sig):
                proc = subprocess.Popen(
                    [sys.executable, LAUNCHER_PATH, "app-server"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self._make_env(),
                )
                try:
                    descriptor_path = self._wait_for_descriptor_line(proc)
                    with open(descriptor_path, "r", encoding="utf-8") as handle:
                        descriptor = json.load(handle)
                    backend_pid = int(descriptor["backend_pid"])
                    socket_path = str(descriptor["socket_path"])
                    self.assertTrue(_pid_exists(backend_pid))

                    proc.send_signal(sig)
                    rc = proc.wait(timeout=5.0)
                    self.assertEqual(rc, 128 + int(sig))
                    self.assertTrue(_wait_for_pid_exit(backend_pid))
                    self.assertFalse(os.path.exists(descriptor_path))
                    self.assertFalse(os.path.lexists(socket_path))
                    self.assertEqual(os.listdir(self.runtime_dir), [])
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait(timeout=2.0)
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()

    def test_early_backend_error_truthful_nonzero_without_retry(self) -> None:
        """Verify early backend failure returns truthful nonzero exit code with exactly one spawn."""
        env = self._make_env(
            FAKE_CODEX_MODE="early_exit",
            FAKE_CODEX_EXIT_CODE="42",
        )
        res = subprocess.run(
            [sys.executable, LAUNCHER_PATH, "app-server"],
            input=b"",
            capture_output=True,
            env=env,
            timeout=5.0,
            check=False,
        )
        self.assertEqual(res.returncode, 42)
        self.assertEqual(res.stdout, b"")
        self.assertEqual(
            res.stderr.decode("utf-8"), launcher.FIXED_ERROR_DIAGNOSTIC
        )
        self.assertEqual(os.listdir(self.runtime_dir), [])

        events = self._read_events()
        spawns = [e for e in events if e.get("event") == "spawn"]
        self.assertEqual(len(spawns), 1)

    def test_unsafe_runtime_root_rejected_before_process_spawn(self) -> None:
        """Verify missing, symlink, non-0700, or overly long XDG_RUNTIME_DIR fails before spawning."""
        open_dir = os.path.join(self.test_root, "rt-0755")
        os.mkdir(open_dir, 0o755)
        os.chmod(open_dir, 0o755)

        symlink_dir = os.path.join(self.test_root, "rt-symlink")
        os.symlink(self.runtime_dir, symlink_dir)

        regular_file = os.path.join(self.test_root, "rt-file")
        with open(regular_file, "w", encoding="utf-8") as handle:
            handle.write("not a dir\n")
        os.chmod(regular_file, 0o700)

        long_dir = os.path.join(self.test_root, "a" * 95)
        os.mkdir(long_dir, 0o700)
        os.chmod(long_dir, 0o700)

        unsafe_roots: list[str | None] = [
            None,
            "",
            "relative/runtime/dir",
            os.path.join(self.test_root, "does-not-exist"),
            open_dir,
            symlink_dir,
            regular_file,
            long_dir,
        ]

        for bad_root in unsafe_roots:
            with self.subTest(bad_root=bad_root):
                if os.path.exists(self.log_file):
                    os.unlink(self.log_file)
                env = self._make_env(XDG_RUNTIME_DIR=bad_root)
                res = subprocess.run(
                    [sys.executable, LAUNCHER_PATH, "app-server"],
                    input=b"",
                    capture_output=True,
                    env=env,
                    timeout=5.0,
                    check=False,
                )
                self.assertEqual(res.returncode, 1)
                self.assertEqual(res.stdout, b"")
                self.assertEqual(
                    res.stderr.decode("utf-8"), launcher.FIXED_ERROR_DIAGNOSTIC
                )
                self.assertEqual(self._read_events(), [])

    def test_invalid_real_codex_rejected_before_spawn(self) -> None:
        """Verify invalid or self-referential THREADLANE_REAL_CODEX fails closed."""
        non_exec = os.path.join(self.test_root, "non-exec-codex")
        with open(non_exec, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/sh\nexit 0\n")
        os.chmod(non_exec, 0o600)

        invalid_paths: list[str | None] = [
            None,
            "",
            "relative/codex",
            os.path.join(self.test_root, "missing-codex"),
            self.runtime_dir,
            non_exec,
            LAUNCHER_PATH,
        ]
        for bad_codex in invalid_paths:
            with self.subTest(bad_codex=bad_codex):
                env = self._make_env(THREADLANE_REAL_CODEX=bad_codex)
                res = subprocess.run(
                    [sys.executable, LAUNCHER_PATH, "app-server"],
                    input=b"",
                    capture_output=True,
                    env=env,
                    timeout=5.0,
                    check=False,
                )
                self.assertEqual(res.returncode, 1)
                self.assertEqual(res.stdout, b"")
                self.assertEqual(
                    res.stderr.decode("utf-8"), launcher.FIXED_ERROR_DIAGNOSTIC
                )
                self.assertEqual(self._read_events(), [])

    def test_oversized_stdin_and_malformed_utf8_rejected(self) -> None:
        """Verify >8MiB input line and invalid UTF-8 on stdin are rejected and leave no process."""
        payloads = [
            ("oversized", b"x" * (launcher.MAX_FRAME_BYTES + 4096) + b"\n"),
            ("malformed_utf8", b'{"bad":"\xff\xfe"}\n'),
        ]
        for label, bad_bytes in payloads:
            with self.subTest(case=label):
                if os.path.exists(self.log_file):
                    os.unlink(self.log_file)
                proc = subprocess.Popen(
                    [sys.executable, LAUNCHER_PATH, "app-server"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self._make_env(),
                )
                try:
                    descriptor_path = self._wait_for_descriptor_line(proc)
                    with open(descriptor_path, "r", encoding="utf-8") as handle:
                        descriptor = json.load(handle)
                    backend_pid = int(descriptor["backend_pid"])

                    assert proc.stdin is not None
                    assert proc.stdout is not None
                    assert proc.stderr is not None
                    try:
                        proc.stdin.write(bad_bytes)
                        proc.stdin.flush()
                    except BrokenPipeError:
                        pass
                    try:
                        proc.stdin.close()
                    except BrokenPipeError:
                        pass

                    rc = proc.wait(timeout=5.0)
                    self.assertNotEqual(rc, 0)
                    self.assertEqual(proc.stdout.read(), b"")
                    stderr_rest = proc.stderr.read().decode(
                        "utf-8", errors="replace"
                    )
                    self.assertEqual(
                        stderr_rest, launcher.FIXED_ERROR_DIAGNOSTIC
                    )
                    self.assertTrue(_wait_for_pid_exit(backend_pid))
                    self.assertEqual(os.listdir(self.runtime_dir), [])
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait(timeout=2.0)
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()

    def test_invalid_ws_frames_rejected_without_stdout_leak(self) -> None:
        """Verify binary WS frames and unescaped newlines inside WS frames fail closed."""
        for mode in ("ws_send_binary", "ws_send_newline"):
            with self.subTest(mode=mode):
                proc = subprocess.Popen(
                    [sys.executable, LAUNCHER_PATH, "app-server"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self._make_env(FAKE_CODEX_MODE=mode),
                )
                try:
                    descriptor_path = self._wait_for_descriptor_line(proc)
                    with open(descriptor_path, "r", encoding="utf-8") as handle:
                        descriptor = json.load(handle)
                    backend_pid = int(descriptor["backend_pid"])

                    assert proc.stdin is not None
                    assert proc.stdout is not None
                    proc.stdin.write(b'{"jsonrpc":"2.0","id":1,"method":"trigger"}\n')
                    proc.stdin.flush()

                    rc = proc.wait(timeout=5.0)
                    self.assertNotEqual(rc, 0)
                    self.assertEqual(proc.stdout.read(), b"")
                    self.assertTrue(_wait_for_pid_exit(backend_pid))
                    self.assertEqual(os.listdir(self.runtime_dir), [])
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait(timeout=2.0)
                    for stream in (proc.stdin, proc.stdout, proc.stderr):
                        if stream is not None and not stream.closed:
                            stream.close()

    def test_symlink_socket_supported_and_external_target_preserved(self) -> None:
        """Verify symlinked same-UID socket in safe 0700 dir works and external parent is kept."""
        ext_dir = os.path.join(self.test_root, "ext-sock-dir")
        os.mkdir(ext_dir, 0o700)
        os.chmod(ext_dir, 0o700)
        marker_file = os.path.join(ext_dir, "keep.txt")
        with open(marker_file, "w", encoding="utf-8") as handle:
            handle.write("preserve\n")

        env = self._make_env(
            FAKE_CODEX_MODE="ws_symlink",
            FAKE_CODEX_EXTERNAL_SOCK_DIR=ext_dir,
        )
        frame = '{"jsonrpc":"2.0","id":7,"method":"ping"}'
        res = self._echo_before_eof(["app-server"], frame, env)
        self.assertEqual(res.returncode, 0)
        self.assertEqual(res.stdout.decode("utf-8"), frame + "\n")
        self.assertEqual(os.listdir(self.runtime_dir), [])
        self.assertTrue(os.path.isdir(ext_dir))
        self.assertTrue(os.path.isfile(marker_file))


if __name__ == "__main__":
    unittest.main()
