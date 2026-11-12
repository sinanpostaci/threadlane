{
  pkgs,
  lib ? pkgs.lib,
  threadlane,
  threadlaneDesktop,
}:

let
  guestPython = pkgs.python3.withPackages (ps: [ ps.pillow ]);

  desktopAcceptanceLib = pkgs.writeText "threadlane-desktop-acceptance-lib.py" (
    builtins.readFile ../scripts/desktop-acceptance.py
  );

  wlrVirtualPointerXml = "${pkgs.wlrctl.src}/protocol/wlr-virtual-pointer-unstable-v1.xml";
  virtualKeyboardXml = "${pkgs.wlrctl.src}/protocol/virtual-keyboard-unstable-v1.xml";

  persistentInputSource = pkgs.writeText "threadlane-persistent-wayland-input.c" ''
    #define _GNU_SOURCE
    #include <errno.h>
    #include <fcntl.h>
    #include <poll.h>
    #include <signal.h>
    #include <stdbool.h>
    #include <stdint.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include <sys/mman.h>
    #include <time.h>
    #include <unistd.h>
    #include <wayland-client.h>
    #include <xkbcommon/xkbcommon.h>

    #include "virtual-keyboard-unstable-v1-client-protocol.h"
    #include "wlr-virtual-pointer-unstable-v1-client-protocol.h"

    #define MAX_CANDIDATE_SEATS 8

    struct candidate_seat {
      struct wl_seat *seat;
      uint32_t global_name;
      uint32_t bound_version;
      uint32_t capabilities;
      bool has_capabilities;
      bool has_name;
      char name[64];
    };

    struct app_state {
      struct candidate_seat seats[MAX_CANDIDATE_SEATS];
      size_t seat_count;
      bool saw_legacy_seat_v1;
      struct zwlr_virtual_pointer_manager_v1 *pointer_mgr;
      struct zwp_virtual_keyboard_manager_v1 *keyboard_mgr;
    };

    static int g_sig_pipe[2] = { -1, -1 };

    static void handle_stop_signal(int signo) {
      (void)signo;
      int saved_errno = errno;
      if (g_sig_pipe[1] >= 0) {
        const uint8_t byte = 1;
        ssize_t ignored = write(g_sig_pipe[1], &byte, 1);
        (void)ignored;
      }
      errno = saved_errno;
    }

    static void seat_handle_capabilities(
        void *data,
        struct wl_seat *wl_seat,
        uint32_t capabilities
    ) {
      (void)wl_seat;
      struct candidate_seat *entry = data;
      entry->capabilities = capabilities;
      entry->has_capabilities = true;
    }

    static void seat_handle_name(
        void *data,
        struct wl_seat *wl_seat,
        const char *name
    ) {
      (void)wl_seat;
      struct candidate_seat *entry = data;
      if (name != NULL) {
        snprintf(entry->name, sizeof(entry->name), "%s", name);
        entry->has_name = true;
      }
    }

    static const struct wl_seat_listener seat_listener = {
      .capabilities = seat_handle_capabilities,
      .name = seat_handle_name,
    };

    static void registry_handle_global(
        void *data,
        struct wl_registry *registry,
        uint32_t name,
        const char *interface,
        uint32_t version
    ) {
      struct app_state *state = data;
      if (strcmp(interface, wl_seat_interface.name) == 0) {
        if (version < 2) {
          state->saw_legacy_seat_v1 = true;
          return;
        }
        if (state->seat_count >= MAX_CANDIDATE_SEATS) {
          return;
        }
        struct candidate_seat *entry = &state->seats[state->seat_count++];
        memset(entry, 0, sizeof(*entry));
        entry->global_name = name;
        entry->bound_version = 2;
        entry->seat = wl_registry_bind(registry, name, &wl_seat_interface, 2);
        if (entry->seat != NULL) {
          wl_seat_add_listener(entry->seat, &seat_listener, entry);
        }
      } else if (strcmp(interface, zwlr_virtual_pointer_manager_v1_interface.name) == 0) {
        if (state->pointer_mgr == NULL) {
          state->pointer_mgr = wl_registry_bind(
              registry,
              name,
              &zwlr_virtual_pointer_manager_v1_interface,
              1
          );
        }
      } else if (strcmp(interface, zwp_virtual_keyboard_manager_v1_interface.name) == 0) {
        if (state->keyboard_mgr == NULL) {
          state->keyboard_mgr = wl_registry_bind(
              registry,
              name,
              &zwp_virtual_keyboard_manager_v1_interface,
              1
          );
        }
      }
    }

    static void registry_handle_global_remove(
        void *data,
        struct wl_registry *registry,
        uint32_t name
    ) {
      (void)data;
      (void)registry;
      (void)name;
    }

    static const struct wl_registry_listener registry_listener = {
      .global = registry_handle_global,
      .global_remove = registry_handle_global_remove,
    };

    static bool write_all_fd(int fd, const char *buf, size_t len) {
      size_t offset = 0;
      while (offset < len) {
        ssize_t n = write(fd, buf + offset, len - offset);
        if (n < 0) {
          if (errno == EINTR) {
            continue;
          }
          return false;
        }
        offset += (size_t)n;
      }
      return true;
    }

    int main(void) {
      int exit_code = 1;
      struct wl_display *display = NULL;
      struct wl_registry *registry = NULL;
      struct zwlr_virtual_pointer_v1 *vpointer = NULL;
      struct zwp_virtual_keyboard_v1 *vkeyboard = NULL;
      struct xkb_context *xkb_ctx = NULL;
      struct xkb_keymap *keymap = NULL;
      char *keymap_str = NULL;
      int keymap_fd = -1;
      struct app_state state;
      memset(&state, 0, sizeof(state));

      const char *runtime_dir = getenv("XDG_RUNTIME_DIR");
      const char *wayland_display = getenv("WAYLAND_DISPLAY");
      const char *x11_display = getenv("DISPLAY");
      if (runtime_dir == NULL || runtime_dir[0] != '/') {
        fprintf(stderr, "persistent-wayland-input: XDG_RUNTIME_DIR must be an absolute path\n");
        return 10;
      }
      if (strcmp(runtime_dir, "/run/user") == 0 || strncmp(runtime_dir, "/run/user/", 10) == 0) {
        fprintf(stderr, "persistent-wayland-input: refusing host runtime dir %s\n", runtime_dir);
        return 10;
      }
      if (x11_display != NULL && x11_display[0] != '\0') {
        fprintf(stderr, "persistent-wayland-input: refusing X11 DISPLAY=%s\n", x11_display);
        return 10;
      }
      if (wayland_display == NULL || wayland_display[0] == '\0' || strchr(wayland_display, '/') != NULL) {
        fprintf(stderr, "persistent-wayland-input: WAYLAND_DISPLAY must be a bare isolated socket name\n");
        return 10;
      }

      signal(SIGPIPE, SIG_IGN);
      if (pipe2(g_sig_pipe, O_CLOEXEC | O_NONBLOCK) != 0) {
        fprintf(stderr, "persistent-wayland-input: pipe2 failed: %s\n", strerror(errno));
        return 11;
      }

      struct sigaction sa;
      memset(&sa, 0, sizeof(sa));
      sa.sa_handler = handle_stop_signal;
      sigemptyset(&sa.sa_mask);
      if (sigaction(SIGTERM, &sa, NULL) != 0 || sigaction(SIGINT, &sa, NULL) != 0) {
        fprintf(stderr, "persistent-wayland-input: sigaction failed: %s\n", strerror(errno));
        exit_code = 11;
        goto cleanup;
      }

      display = wl_display_connect(NULL);
      if (display == NULL) {
        fprintf(
            stderr,
            "persistent-wayland-input: failed to connect to Wayland display %s in %s: %s\n",
            wayland_display,
            runtime_dir,
            strerror(errno)
        );
        exit_code = 12;
        goto cleanup;
      }

      registry = wl_display_get_registry(display);
      if (registry == NULL) {
        fprintf(stderr, "persistent-wayland-input: wl_display_get_registry failed\n");
        exit_code = 12;
        goto cleanup;
      }
      wl_registry_add_listener(registry, &registry_listener, &state);

      if (wl_display_roundtrip(display) < 0 || wl_display_roundtrip(display) < 0) {
        fprintf(stderr, "persistent-wayland-input: initial Wayland roundtrip failed: %s\n", strerror(errno));
        exit_code = 12;
        goto cleanup;
      }

      if (state.pointer_mgr == NULL) {
        fprintf(stderr, "persistent-wayland-input: missing required protocol zwlr_virtual_pointer_manager_v1\n");
        exit_code = 2;
        goto cleanup;
      }
      if (state.keyboard_mgr == NULL) {
        fprintf(stderr, "persistent-wayland-input: missing required protocol zwp_virtual_keyboard_manager_v1\n");
        exit_code = 3;
        goto cleanup;
      }

      struct candidate_seat *seat0 = NULL;
      for (size_t i = 0; i < state.seat_count; i++) {
        if (state.seats[i].seat != NULL && state.seats[i].has_name && strcmp(state.seats[i].name, "seat0") == 0) {
          seat0 = &state.seats[i];
          break;
        }
      }
      if (seat0 == NULL) {
        fprintf(
            stderr,
            "persistent-wayland-input: target wl_seat 'seat0' not advertised (seat_count=%zu, saw_legacy_v1=%d)\n",
            state.seat_count,
            state.saw_legacy_seat_v1 ? 1 : 0
        );
        exit_code = 4;
        goto cleanup;
      }

      xkb_ctx = xkb_context_new(XKB_CONTEXT_NO_FLAGS);
      if (xkb_ctx == NULL) {
        fprintf(stderr, "persistent-wayland-input: xkb_context_new failed\n");
        exit_code = 5;
        goto cleanup;
      }
      const struct xkb_rule_names rule_names = {
        .rules = "evdev",
        .model = "pc105",
        .layout = "us",
        .variant = "",
        .options = "",
      };
      keymap = xkb_keymap_new_from_names(xkb_ctx, &rule_names, XKB_KEYMAP_COMPILE_NO_FLAGS);
      if (keymap == NULL) {
        fprintf(stderr, "persistent-wayland-input: xkb_keymap_new_from_names failed\n");
        exit_code = 5;
        goto cleanup;
      }
      keymap_str = xkb_keymap_get_as_string(keymap, XKB_KEYMAP_FORMAT_TEXT_V1);
      if (keymap_str == NULL) {
        fprintf(stderr, "persistent-wayland-input: xkb_keymap_get_as_string failed\n");
        exit_code = 5;
        goto cleanup;
      }
      size_t keymap_bytes = strlen(keymap_str) + 1;
      if (keymap_bytes <= 1 || keymap_bytes > UINT32_MAX) {
        fprintf(stderr, "persistent-wayland-input: invalid serialized XKB keymap length %zu\n", keymap_bytes);
        exit_code = 5;
        goto cleanup;
      }

      keymap_fd = memfd_create("threadlane-virtual-keymap", MFD_CLOEXEC);
      if (keymap_fd < 0) {
        fprintf(stderr, "persistent-wayland-input: memfd_create failed: %s\n", strerror(errno));
        exit_code = 5;
        goto cleanup;
      }
      if (!write_all_fd(keymap_fd, keymap_str, keymap_bytes) || lseek(keymap_fd, 0, SEEK_SET) < 0) {
        fprintf(stderr, "persistent-wayland-input: writing XKB keymap memfd failed: %s\n", strerror(errno));
        exit_code = 5;
        goto cleanup;
      }

      vpointer = zwlr_virtual_pointer_manager_v1_create_virtual_pointer(state.pointer_mgr, seat0->seat);
      if (vpointer == NULL) {
        fprintf(stderr, "persistent-wayland-input: create_virtual_pointer on seat0 failed\n");
        exit_code = 6;
        goto cleanup;
      }
      vkeyboard = zwp_virtual_keyboard_manager_v1_create_virtual_keyboard(state.keyboard_mgr, seat0->seat);
      if (vkeyboard == NULL) {
        fprintf(stderr, "persistent-wayland-input: create_virtual_keyboard on seat0 failed\n");
        exit_code = 6;
        goto cleanup;
      }
      /* Attach valid XKB v1 keymap only; setup intentionally sends ZERO keypresses or modifiers. */
      zwp_virtual_keyboard_v1_keymap(
          vkeyboard,
          WL_KEYBOARD_KEYMAP_FORMAT_XKB_V1,
          keymap_fd,
          (uint32_t)keymap_bytes
      );

      /* Unconditionally flush and roundtrip creation even if seat capabilities already existed. */
      if (
          wl_display_flush(display) < 0
          || wl_display_roundtrip(display) < 0
          || wl_display_roundtrip(display) < 0
      ) {
        fprintf(
            stderr,
            "persistent-wayland-input: flush/roundtrip after virtual device creation failed: %s\n",
            strerror(errno)
        );
        exit_code = 6;
        goto cleanup;
      }

      const uint32_t required_caps = WL_SEAT_CAPABILITY_POINTER | WL_SEAT_CAPABILITY_KEYBOARD;
      for (int attempt = 0; attempt < 25 && (seat0->capabilities & required_caps) != required_caps; attempt++) {
        struct timespec req = { .tv_sec = 0, .tv_nsec = 10 * 1000 * 1000 };
        nanosleep(&req, NULL);
        if (wl_display_roundtrip(display) < 0) {
          fprintf(stderr, "persistent-wayland-input: capability roundtrip failed: %s\n", strerror(errno));
          exit_code = 6;
          goto cleanup;
        }
      }

      if ((seat0->capabilities & required_caps) != required_caps) {
        fprintf(
            stderr,
            "persistent-wayland-input: seat0 capabilities=%u, expected pointer|keyboard (%u)\n",
            seat0->capabilities,
            required_caps
        );
        exit_code = 6;
        goto cleanup;
      }

      close(keymap_fd);
      keymap_fd = -1;
      free(keymap_str);
      keymap_str = NULL;
      xkb_keymap_unref(keymap);
      keymap = NULL;
      xkb_context_unref(xkb_ctx);
      xkb_ctx = NULL;

      printf(
          "{\"event\":\"ready\",\"pid\":%ld,\"seat\":\"seat0\",\"capabilities\":%u,"
          "\"pointer\":true,\"keyboard\":true,\"keymap_format\":\"xkb_v1\","
          "\"keymap_bytes\":%zu,\"setup_keypresses_sent\":0}\n",
          (long)getpid(),
          seat0->capabilities,
          keymap_bytes
      );
      fflush(stdout);
      fprintf(
          stderr,
          "persistent-wayland-input: ready pid=%ld seat=seat0 capabilities=%u keymap_bytes=%zu setup_keypresses_sent=0\n",
          (long)getpid(),
          seat0->capabilities,
          keymap_bytes
      );
      fflush(stderr);

      bool stop_requested = false;
      exit_code = 0;
      while (!stop_requested) {
        while (wl_display_prepare_read(display) != 0) {
          if (wl_display_dispatch_pending(display) < 0) {
            fprintf(stderr, "persistent-wayland-input: dispatch_pending failed: %s\n", strerror(errno));
            exit_code = 7;
            stop_requested = true;
            break;
          }
        }
        if (stop_requested) {
          break;
        }
        if (wl_display_flush(display) < 0 && errno != EAGAIN) {
          wl_display_cancel_read(display);
          fprintf(stderr, "persistent-wayland-input: wl_display_flush failed: %s\n", strerror(errno));
          exit_code = 7;
          break;
        }

        struct pollfd fds[2];
        fds[0].fd = wl_display_get_fd(display);
        fds[0].events = POLLIN;
        fds[0].revents = 0;
        fds[1].fd = g_sig_pipe[0];
        fds[1].events = POLLIN;
        fds[1].revents = 0;

        int ret = poll(fds, 2, -1);
        if (ret < 0) {
          wl_display_cancel_read(display);
          if (errno == EINTR) {
            continue;
          }
          fprintf(stderr, "persistent-wayland-input: poll failed: %s\n", strerror(errno));
          exit_code = 7;
          break;
        }
        if (fds[1].revents & POLLIN) {
          wl_display_cancel_read(display);
          stop_requested = true;
          break;
        }
        if (fds[0].revents & (POLLERR | POLLHUP)) {
          wl_display_cancel_read(display);
          fprintf(stderr, "persistent-wayland-input: Wayland display connection closed or errored\n");
          exit_code = 7;
          break;
        }
        if (fds[0].revents & POLLIN) {
          if (wl_display_read_events(display) < 0) {
            fprintf(stderr, "persistent-wayland-input: wl_display_read_events failed: %s\n", strerror(errno));
            exit_code = 7;
            break;
          }
          if (wl_display_dispatch_pending(display) < 0) {
            fprintf(stderr, "persistent-wayland-input: wl_display_dispatch_pending failed: %s\n", strerror(errno));
            exit_code = 7;
            break;
          }
        } else {
          wl_display_cancel_read(display);
        }
      }

      fprintf(stderr, "persistent-wayland-input: shutting down (exit_code=%d)\n", exit_code);
      fflush(stderr);

    cleanup:
      if (keymap_fd >= 0) {
        close(keymap_fd);
      }
      if (keymap_str != NULL) {
        free(keymap_str);
      }
      if (keymap != NULL) {
        xkb_keymap_unref(keymap);
      }
      if (xkb_ctx != NULL) {
        xkb_context_unref(xkb_ctx);
      }
      if (vpointer != NULL) {
        zwlr_virtual_pointer_v1_destroy(vpointer);
      }
      if (vkeyboard != NULL) {
        zwp_virtual_keyboard_v1_destroy(vkeyboard);
      }
      if (state.pointer_mgr != NULL) {
        zwlr_virtual_pointer_manager_v1_destroy(state.pointer_mgr);
      }
      if (state.keyboard_mgr != NULL) {
        zwp_virtual_keyboard_manager_v1_destroy(state.keyboard_mgr);
      }
      for (size_t i = 0; i < state.seat_count; i++) {
        if (state.seats[i].seat != NULL) {
          wl_seat_destroy(state.seats[i].seat);
        }
      }
      if (registry != NULL) {
        wl_registry_destroy(registry);
      }
      if (display != NULL) {
        wl_display_flush(display);
        wl_display_disconnect(display);
      }
      if (g_sig_pipe[0] >= 0) {
        close(g_sig_pipe[0]);
      }
      if (g_sig_pipe[1] >= 0) {
        close(g_sig_pipe[1]);
      }
      return exit_code;
    }
  '';

  persistentWaylandInput =
    pkgs.runCommandCC "threadlane-persistent-wayland-input"
      {
        nativeBuildInputs = [
          pkgs.pkg-config
          pkgs.wayland-scanner
        ];
        buildInputs = [
          pkgs.libxkbcommon
          pkgs.wayland
        ];
      }
      ''
        test -f "${wlrVirtualPointerXml}"
        test -f "${virtualKeyboardXml}"
        wayland-scanner client-header "${wlrVirtualPointerXml}" wlr-virtual-pointer-unstable-v1-client-protocol.h
        wayland-scanner private-code "${wlrVirtualPointerXml}" wlr-virtual-pointer-unstable-v1-protocol.c
        wayland-scanner client-header "${virtualKeyboardXml}" virtual-keyboard-unstable-v1-client-protocol.h
        wayland-scanner private-code "${virtualKeyboardXml}" virtual-keyboard-unstable-v1-protocol.c
        mkdir -p "$out/bin"
        "$CC" -O2 -std=c11 -Wall -Wextra \
          -I. \
          $(pkg-config --cflags wayland-client xkbcommon) \
          "${persistentInputSource}" \
          wlr-virtual-pointer-unstable-v1-protocol.c \
          virtual-keyboard-unstable-v1-protocol.c \
          $(pkg-config --libs wayland-client xkbcommon) \
          -o "$out/bin/threadlane-persistent-wayland-input"
      '';

  vmHarnessScript = pkgs.writeText "threadlane-vm-harness.py" ''
    #!/usr/bin/env python3
    """Isolated Wayland + StatusNotifier tray acceptance harness inside NixOS VM."""

    from __future__ import annotations

    import importlib.util
    import json
    import os
    import pathlib
    import shutil
    import subprocess
    import sys
    import time
    from typing import Any

    from PIL import Image, ImageChops, ImageStat


    def _load_shared_acceptance_lib() -> Any:
        lib_path = pathlib.Path("/etc/threadlane-desktop-acceptance-lib.py")
        spec = importlib.util.spec_from_file_location("desktop_acceptance_lib", lib_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Unable to load shared acceptance module from {lib_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module


    shared = _load_shared_acceptance_lib()


    ARTIFACT_DIR = pathlib.Path("/tmp/vm-acceptance-artifacts")
    ISOLATION_ROOT = pathlib.Path("/tmp/threadlane-vm-isolated")
    BAR_BG_RGB = (17, 24, 39)  # #111827
    DESKTOP_BG_HEX = "#2e6f40"
    DESKTOP_BG_RGB = (46, 111, 64)  # #2e6f40
    EXPECTED_POPUP_LOGICAL_W = 360.0
    EXPECTED_POPUP_LOGICAL_H = 320.0
    POPUP_LOGICAL_TOLERANCE = 4.0
    POPUP_MIN_FILL_RATIO = 0.70
    ICON_PROXIMITY_MAX_GAP_LOGICAL = 64.0

    REQUIRED_CHECK_KEYS = (
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


    class HarnessFailure(RuntimeError):
        """Raised when any native Wayland or tray acceptance check fails."""


    def log(msg: str) -> None:
        print(f"[vm-acceptance] {msg}", flush=True)


    def run_checked(
        argv: list[str],
        *,
        env: dict[str, str],
        timeout: float = 15.0,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            argv,
            env=env,
            check=True,
            text=True,
            capture_output=True,
            timeout=timeout,
        )


    def wait_until(
        description: str,
        predicate: Any,
        *,
        timeout_sec: float = 20.0,
        interval_sec: float = 0.2,
    ) -> Any:
        deadline = time.monotonic() + timeout_sec
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except Exception as exc:  # noqa: BLE001
                last_err = exc
            time.sleep(interval_sec)
        detail = f" (last error: {last_err})" if last_err else ""
        raise HarnessFailure(f"Timed out waiting for {description}{detail}")


    def sway_json(sway_cmd: list[str], env: dict[str, str]) -> Any:
        res = run_checked(["swaymsg", *sway_cmd], env=env, timeout=10.0)
        return json.loads(res.stdout)


    def sway_command(sway_cmd: list[str], env: dict[str, str]) -> None:
        res = run_checked(["swaymsg", "-r", *sway_cmd], env=env, timeout=10.0)
        try:
            payload = json.loads(res.stdout)
        except Exception as exc:  # noqa: BLE001
            raise HarnessFailure(
                f"swaymsg {sway_cmd!r} returned invalid JSON {res.stdout!r}: {exc}"
            ) from exc
        if not isinstance(payload, list) or not payload:
            raise HarnessFailure(
                f"swaymsg {sway_cmd!r} returned unexpected payload: {payload!r}"
            )
        for item in payload:
            if not isinstance(item, dict) or item.get("success") is not True:
                raise HarnessFailure(
                    f"swaymsg {sway_cmd!r} reported command failure: {payload!r}"
                )


    def pointer_move(x: float, y: float, env: dict[str, str]) -> tuple[int, int]:
        ix = int(round(x))
        iy = int(round(y))
        sway_command(["seat", "seat0", "cursor", "set", str(ix), str(iy)], env)
        return (ix, iy)


    def pointer_click(
        x: float,
        y: float,
        env: dict[str, str],
        button: str = "button1",
    ) -> tuple[int, int]:
        pos = pointer_move(x, y, env)
        time.sleep(0.06)
        sway_command(["seat", "seat0", "cursor", "press", button], env)
        time.sleep(0.05)
        sway_command(["seat", "seat0", "cursor", "release", button], env)
        return pos


    def audit_sway_config(config_text: str) -> dict[str, Any]:
        lines = [
            raw.strip()
            for raw in config_text.splitlines()
            if raw.strip() and not raw.strip().startswith("#")
        ]
        focus_values = [
            line.split(None, 1)[1].strip()
            for line in lines
            if line.startswith("focus_follows_mouse ")
        ]
        if focus_values != ["yes"]:
            raise HarnessFailure(
                f"Expected single 'focus_follows_mouse yes' in sway config, got {focus_values!r}"
            )

        for_window_lines = [line for line in lines if line.startswith("for_window ")]
        allowed_main_rule = 'for_window [title="^Threadlane$"] floating enable'
        broad_floating = any(
            "[all]" in line or "app_id" in line or 'title="*"' in line
            for line in for_window_lines
        )
        tray_rules = any(
            line != allowed_main_rule
            or "Status" in line
            or "tray" in line.lower()
            or "resize" in line
            or "move" in line
            for line in for_window_lines
        )
        if broad_floating or tray_rules:
            raise HarnessFailure(
                f"Forbidden broad or tray compositor rule in sway config: {for_window_lines!r}"
            )
        return {
            "focus_follows_mouse": "yes",
            "broad_floating_rules_present": False,
            "tray_compositor_rules_present": False,
            "cursor_positioning_command": "set",
            "main_only_floating_rule": allowed_main_rule,
            "main_default_size_unforced": True,
        }


    def iter_tree_nodes(node: dict[str, Any]) -> list[dict[str, Any]]:
        found = [node]
        for key in ("nodes", "floating_nodes"):
            for child in node.get(key, []) or []:
                if isinstance(child, dict):
                    found.extend(iter_tree_nodes(child))
        return found


    def list_visible_normal_windows(tree: dict[str, Any]) -> list[dict[str, Any]]:
        visible: list[dict[str, Any]] = []
        for node in iter_tree_nodes(tree):
            has_window_identity = (
                node.get("app_id") is not None or node.get("window") is not None
            )
            if has_window_identity and bool(node.get("visible")):
                visible.append(node)
        return visible


    def find_visible_window_by_title(
        tree: dict[str, Any],
        exact_title: str,
    ) -> dict[str, Any] | None:
        for node in list_visible_normal_windows(tree):
            if node.get("name") == exact_title:
                return node
        return None


    def locate_tray_icon_logical_center(
        *,
        output_x: int,
        output_y: int,
        expected_scale: float,
        screenshot_path: pathlib.Path,
        env: dict[str, str],
    ) -> tuple[float, float]:
        logical_w = 160
        logical_h = 32
        region = f"{output_x},{output_y} {logical_w}x{logical_h}"
        run_checked(["grim", "-g", region, str(screenshot_path)], env=env)
        with Image.open(screenshot_path) as img:
            rgb = img.convert("RGB")
            width, height = rgb.size
            expected_px_w = int(round(logical_w * expected_scale))
            expected_px_h = int(round(logical_h * expected_scale))
            if (width, height) != (expected_px_w, expected_px_h):
                raise HarnessFailure(
                    f"Expected panel capture {expected_px_w}x{expected_px_h} at scale {expected_scale}, got {width}x{height}"
                )
            xs: list[int] = []
            ys: list[int] = []
            for py in range(height):
                for px in range(width):
                    r, g, b = rgb.getpixel((px, py))
                    dist = (
                        abs(r - BAR_BG_RGB[0])
                        + abs(g - BAR_BG_RGB[1])
                        + abs(b - BAR_BG_RGB[2])
                    )
                    if dist > 36:
                        xs.append(px)
                        ys.append(py)
            if len(xs) < 12:
                raise HarnessFailure(
                    f"Tray icon pixels not rendered in panel strip {region} (diff pixels={len(xs)})"
                )
            mean_px = sum(xs) / len(xs)
            mean_py = sum(ys) / len(ys)
            return (
                output_x + (mean_px / float(width)) * float(logical_w),
                output_y + (mean_py / float(height)) * float(logical_h),
            )


    def detect_panel_bar_height_px(rgb: Image.Image, expected_scale: float) -> int:
        width, height = rgb.size
        sample_xs = [
            max(0, width - 16),
            max(0, width - 48),
            max(0, width - 96),
        ]
        max_scan_y = min(height - 1, int(round(64.0 * expected_scale)))
        detected: int | None = None
        for py in range(max_scan_y):
            bg_matches = 0
            for sx in sample_xs:
                r, g, b = rgb.getpixel((sx, py))
                dist = (
                    abs(r - DESKTOP_BG_RGB[0])
                    + abs(g - DESKTOP_BG_RGB[1])
                    + abs(b - DESKTOP_BG_RGB[2])
                )
                if dist <= 24:
                    bg_matches += 1
            if bg_matches == len(sample_xs):
                detected = py
                break
        if detected is None:
            raise HarnessFailure(
                f"Could not locate transition from panel strip to solid background {DESKTOP_BG_HEX}"
            )
        expected_px = int(round(32.0 * expected_scale))
        if abs(detected - expected_px) > int(round(6.0 * expected_scale)):
            raise HarnessFailure(
                f"Unexpected panel height {detected}px at scale {expected_scale} (expected ~{expected_px}px)"
            )
        return detected


    def verify_clean_workspace_background(
        *,
        output_info: dict[str, Any],
        screenshot_path: pathlib.Path,
        env: dict[str, str],
    ) -> int:
        out_name = str(output_info["name"])
        scale = float(output_info["scale"])
        out_rect = output_info["rect"]
        expected_px_w = int(round(float(out_rect["width"]) * scale))
        expected_px_h = int(round(float(out_rect["height"]) * scale))

        run_checked(["grim", "-o", out_name, str(screenshot_path)], env=env)
        with Image.open(screenshot_path) as img:
            rgb = img.convert("RGB")
            if rgb.size != (expected_px_w, expected_px_h):
                raise HarnessFailure(
                    f"Expected {out_name} capture {expected_px_w}x{expected_px_h} at scale {scale}, got {rgb.size}"
                )
            bar_px_h = detect_panel_bar_height_px(rgb, scale)
            ws = rgb.crop((0, bar_px_h, expected_px_w, expected_px_h))
            solid_bg = Image.new("RGB", ws.size, DESKTOP_BG_RGB)
            diff = ImageChops.difference(ws, solid_bg)
            r_d, g_d, b_d = diff.split()
            max_d = ImageChops.lighter(ImageChops.lighter(r_d, g_d), b_d)
            non_bg_mask = max_d.point(lambda v: 255 if v > 10 else 0)
            non_bg_pixels = int(non_bg_mask.histogram()[255])
            if non_bg_pixels != 0:
                bad_bbox = non_bg_mask.getbbox()
                raise HarnessFailure(
                    f"Workspace on {out_name} is not clean solid {DESKTOP_BG_HEX}: "
                    f"{non_bg_pixels} non-background pixels in ws bbox {bad_bbox}"
                )
            return bar_px_h


    def count_workspace_diff_pixels(
        before_path: pathlib.Path,
        after_path: pathlib.Path,
        bar_px_h: int,
    ) -> tuple[int, tuple[int, int, int, int] | None, Image.Image]:
        with Image.open(before_path) as before_img, Image.open(after_path) as after_img:
            before_rgb = before_img.convert("RGB")
            after_rgb = after_img.convert("RGB")
            if before_rgb.size != after_rgb.size:
                raise HarnessFailure(
                    f"Output capture size changed between {before_path.name} ({before_rgb.size}) "
                    f"and {after_path.name} ({after_rgb.size})"
                )
            w, h = after_rgb.size
            before_ws = before_rgb.crop((0, bar_px_h, w, h))
            after_ws = after_rgb.crop((0, bar_px_h, w, h))
            diff = ImageChops.difference(after_ws, before_ws)
            r_d, g_d, b_d = diff.split()
            max_d = ImageChops.lighter(ImageChops.lighter(r_d, g_d), b_d)
            mask = max_d.point(lambda v: 255 if v > 18 else 0)
            changed = int(mask.histogram()[255])
            bbox_ws = mask.getbbox()
            return (changed, bbox_ws, after_ws)


    def evaluate_rendered_popup_capture(
        *,
        target_output: dict[str, Any],
        before_target_path: pathlib.Path,
        after_target_path: pathlib.Path,
        target_bar_px_h: int,
        clicked_icon_center: tuple[float, float],
        env: dict[str, str],
        other_output: dict[str, Any] | None = None,
        before_other_path: pathlib.Path | None = None,
        after_other_path: pathlib.Path | None = None,
        other_bar_px_h: int | None = None,
    ) -> dict[str, Any]:
        target_name = str(target_output["name"])
        scale = float(target_output["scale"])
        out_rect = target_output["rect"]
        expected_px_w = int(round(float(out_rect["width"]) * scale))
        expected_px_h = int(round(float(out_rect["height"]) * scale))

        run_checked(["grim", "-o", target_name, str(after_target_path)], env=env)
        with Image.open(after_target_path) as check_img:
            if check_img.size != (expected_px_w, expected_px_h):
                raise HarnessFailure(
                    f"Expected {target_name} capture {expected_px_w}x{expected_px_h}, got {check_img.size}"
                )

        changed_pixels, bbox_ws, after_ws = count_workspace_diff_pixels(
            before_target_path,
            after_target_path,
            target_bar_px_h,
        )
        if changed_pixels == 0 or bbox_ws is None:
            raise HarnessFailure(f"No rendered popup pixels detected on {target_name}")

        left, upper, right, lower = bbox_ws
        phys_x = int(left)
        phys_y = int(target_bar_px_h + upper)
        phys_w = int(right - left)
        phys_h = int(lower - upper)
        bbox_area = max(1, phys_w * phys_h)
        fill_ratio = changed_pixels / float(bbox_area)
        if fill_ratio < POPUP_MIN_FILL_RATIO:
            raise HarnessFailure(
                f"Popup diff bbox on {target_name} is too sparse (fill_ratio={fill_ratio:.3f}, "
                f"changed={changed_pixels}, physical_bbox={phys_x},{phys_y} {phys_w}x{phys_h})"
            )

        out_x = float(out_rect["x"])
        out_y = float(out_rect["y"])
        out_w = float(out_rect["width"])
        out_h = float(out_rect["height"])

        logical_x = out_x + (phys_x / scale)
        logical_y = out_y + (phys_y / scale)
        logical_w = phys_w / scale
        logical_h = phys_h / scale

        if (
            abs(logical_w - EXPECTED_POPUP_LOGICAL_W) > POPUP_LOGICAL_TOLERANCE
            or abs(logical_h - EXPECTED_POPUP_LOGICAL_H) > POPUP_LOGICAL_TOLERANCE
        ):
            raise HarnessFailure(
                f"Expected compact rendered popup ~360x320 logical on {target_name} (scale {scale}), "
                f"measured logical={logical_w:.2f}x{logical_h:.2f} (physical={phys_w}x{phys_h})"
            )

        popup_crop = after_ws.crop((left, upper, right, lower))
        max_stddev = max(ImageStat.Stat(popup_crop).stddev)
        sample_x = min(phys_w - 1, max(0, int(phys_w * (110.0 / 360.0))))
        sample_y = min(phys_h - 1, max(0, int(phys_h * (296.0 / 320.0))))
        sr, sg, sb = popup_crop.getpixel((sample_x, sample_y))
        footer_luma = (sr + sg + sb) / 3.0
        if max_stddev < 8.0 or footer_luma <= 130.0:
            raise HarnessFailure(
                f"Popup surface on {target_name} not fully painted yet "
                f"(max_stddev={max_stddev:.2f}, footer_luma={footer_luma:.1f})"
            )

        within_bounds = (
            logical_x >= out_x - 0.5
            and logical_y >= out_y + (target_bar_px_h / scale) - 0.5
            and logical_x + logical_w <= out_x + out_w + 0.5
            and logical_y + logical_h <= out_y + out_h + 0.5
        )
        if not within_bounds:
            raise HarnessFailure(
                f"Rendered popup logical rect ({logical_x:.2f},{logical_y:.2f} {logical_w:.2f}x{logical_h:.2f}) "
                f"exceeded {target_name} bounds {out_rect}"
            )

        icon_x, icon_y = clicked_icon_center
        horiz_gap = max(0.0, logical_x - icon_x, icon_x - (logical_x + logical_w))
        vert_gap = max(0.0, logical_y - icon_y, icon_y - (logical_y + logical_h))
        proximate = (
            horiz_gap <= ICON_PROXIMITY_MAX_GAP_LOGICAL
            and vert_gap <= ICON_PROXIMITY_MAX_GAP_LOGICAL
        )
        if not proximate:
            raise HarnessFailure(
                f"Rendered popup ({logical_x:.2f},{logical_y:.2f} {logical_w:.2f}x{logical_h:.2f}) "
                f"not proximate to clicked icon {clicked_icon_center} "
                f"(horiz_gap={horiz_gap:.2f}, vert_gap={vert_gap:.2f})"
            )

        other_args = (other_output, before_other_path, after_other_path, other_bar_px_h)
        other_diff_pixels: int | None = None
        if any(arg is not None for arg in other_args):
            if not all(arg is not None for arg in other_args):
                raise HarnessFailure(
                    "Incomplete non-target output capture arguments in evaluate_rendered_popup_capture"
                )
            assert other_output is not None
            assert before_other_path is not None
            assert after_other_path is not None
            assert other_bar_px_h is not None
            other_name = str(other_output["name"])
            run_checked(["grim", "-o", other_name, str(after_other_path)], env=env)
            other_diff_pixels, other_bbox, _ = count_workspace_diff_pixels(
                before_other_path,
                after_other_path,
                other_bar_px_h,
            )
            if other_diff_pixels != 0:
                raise HarnessFailure(
                    f"Popup leaked onto non-target output {other_name}: "
                    f"{other_diff_pixels} diff pixels at ws bbox {other_bbox}"
                )

        tree = sway_json(["-t", "get_tree", "-r"], env)
        in_sway_tree = find_visible_window_by_title(tree, "Threadlane Status") is not None

        return {
            "measurement_method": "grim_pixel_diff",
            "output_name": target_name,
            "output_scale": scale,
            "in_sway_get_tree": in_sway_tree,
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
            "changed_pixels": changed_pixels,
            "fill_ratio": round(fill_ratio, 4),
            "within_output_bounds": True,
            "proximate_to_clicked_icon": True,
            "icon_logical_center": [round(icon_x, 2), round(icon_y, 2)],
            "icon_horiz_gap_logical": round(horiz_gap, 2),
            "icon_vert_gap_logical": round(vert_gap, 2),
            "other_output_diff_pixels": other_diff_pixels,
            "screenshot": after_target_path.name,
        }


    def verify_popup_cleared_outside_main(
        *,
        output_info: dict[str, Any],
        popup_phys_rect: dict[str, Any],
        restored_main_logical_rect: dict[str, Any],
        screenshot_path: pathlib.Path,
        env: dict[str, str],
    ) -> bool:
        out_name = str(output_info["name"])
        scale = float(output_info["scale"])
        out_rect = output_info["rect"]
        out_x = float(out_rect["x"])
        out_y = float(out_rect["y"])

        px = int(popup_phys_rect["x"])
        py = int(popup_phys_rect["y"])
        pw = int(popup_phys_rect["width"])
        ph = int(popup_phys_rect["height"])

        mx = int(round((float(restored_main_logical_rect["x"]) - out_x) * scale)) - 4
        my = int(round((float(restored_main_logical_rect["y"]) - out_y) * scale)) - 4
        mw = int(round(float(restored_main_logical_rect["width"]) * scale)) + 8
        mh = int(round(float(restored_main_logical_rect["height"]) * scale)) + 8

        run_checked(["grim", "-o", out_name, str(screenshot_path)], env=env)
        with Image.open(screenshot_path) as img:
            rgb = img.convert("RGB")
            popup_crop = rgb.crop((px, py, px + pw, py + ph))
            unoccluded_mask = Image.new("L", (pw, ph), 255)
            ox0 = max(0, mx - px)
            oy0 = max(0, my - py)
            ox1 = min(pw, mx + mw - px)
            oy1 = min(ph, my + mh - py)
            if ox1 > ox0 and oy1 > oy0:
                unoccluded_mask.paste(0, (ox0, oy0, ox1, oy1))
            unoccluded_px = int(unoccluded_mask.histogram()[255])
            if unoccluded_px < int(0.25 * pw * ph):
                raise HarnessFailure(
                    f"Restored main window unexpectedly covered >75% of popup area "
                    f"(unoccluded_px={unoccluded_px}, total={pw * ph})"
                )
            solid_bg = Image.new("RGB", (pw, ph), DESKTOP_BG_RGB)
            diff = ImageChops.difference(popup_crop, solid_bg)
            r_d, g_d, b_d = diff.split()
            max_d = ImageChops.lighter(ImageChops.lighter(r_d, g_d), b_d)
            non_bg_mask = max_d.point(lambda v: 255 if v > 12 else 0)
            remaining = ImageChops.darker(non_bg_mask, unoccluded_mask)
            remaining_px = int(remaining.histogram()[255])
            if remaining_px != 0:
                raise HarnessFailure(
                    f"Popup pixels still visible after Open Threadlane ({remaining_px} non-bg pixels)"
                )
            return True


    def capture_and_evaluate_main_window_painted(
        *,
        desktop_proc: subprocess.Popen[str],
        output_info: dict[str, Any],
        before_bg_path: pathlib.Path,
        screenshot_path: pathlib.Path,
        bar_px_h: int,
        expected_width: int,
        expected_height: int,
        env: dict[str, str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if desktop_proc.poll() is not None:
            raise HarnessFailure(
                f"threadlane-desktop exited prematurely with code {desktop_proc.returncode}"
            )
        tree = sway_json(["-t", "get_tree", "-r"], env)
        win = find_visible_window_by_title(tree, "Threadlane")
        if win is None:
            raise HarnessFailure("Main window 'Threadlane' not visible in sway get_tree yet.")

        out_name = str(output_info["name"])
        scale = float(output_info["scale"])
        out_rect = output_info["rect"]
        expected_px_w = int(round(float(out_rect["width"]) * scale))
        expected_px_h = int(round(float(out_rect["height"]) * scale))

        run_checked(["grim", "-o", out_name, str(screenshot_path)], env=env)
        with Image.open(screenshot_path) as check_img:
            if check_img.size != (expected_px_w, expected_px_h):
                raise HarnessFailure(
                    f"Expected {out_name} capture {expected_px_w}x{expected_px_h}, got {check_img.size}"
                )

        changed_pixels, bbox_ws, after_ws = count_workspace_diff_pixels(
            before_bg_path,
            screenshot_path,
            bar_px_h,
        )
        window_rgb_bytes = b""
        if bbox_ws is not None:
            left, upper, right, lower = bbox_ws
            if right > left and lower > upper:
                window_rgb_bytes = (
                    after_ws.crop((left, upper, right, lower)).convert("RGB").tobytes()
                )

        try:
            paint_info = shared.evaluate_main_window_paint_readiness(
                tree_node=win,
                expected_width=expected_width,
                expected_height=expected_height,
                output_rect=out_rect,
                output_scale=scale,
                bar_px_h=bar_px_h,
                changed_pixels=changed_pixels,
                bbox_ws=bbox_ws,
                window_rgb_bytes=window_rgb_bytes,
                screenshot_name=screenshot_path.name,
            )
        except shared.AcceptanceError as exc:
            raise HarnessFailure(str(exc)) from exc

        return (win, paint_info)


    def make_private_dir(path: pathlib.Path, mode: int = 0o700) -> pathlib.Path:
        path.mkdir(parents=True, exist_ok=True)
        path.chmod(mode)
        return path


    def run_inner_session() -> int:
        dbus_addr = os.environ.get("DBUS_SESSION_BUS_ADDRESS", "").strip()
        if not dbus_addr:
            raise HarnessFailure("Missing private DBUS_SESSION_BUS_ADDRESS inside inner session.")

        # Initialize all required check flags to False; each flips True ONLY after its assertion passes.
        checks: dict[str, bool] = {key: False for key in REQUIRED_CHECK_KEYS}

        env = dict(os.environ)
        sway_cfg_dir = make_private_dir(pathlib.Path(env["XDG_CONFIG_HOME"]) / "sway")
        waybar_cfg_dir = make_private_dir(pathlib.Path(env["XDG_CONFIG_HOME"]) / "waybar")

        sway_config_text = (
            "\n".join(
                [
                    "xwayland disable",
                    "focus_follows_mouse yes",
                    "default_border none",
                    "default_floating_border none",
                    f"output * bg {DESKTOP_BG_HEX} solid_color",
                    'for_window [title="^Threadlane$"] floating enable',
                ]
            )
            + "\n"
        )
        compositor_invariants = audit_sway_config(sway_config_text)
        (sway_cfg_dir / "config").write_text(sway_config_text, encoding="utf-8")
        (ARTIFACT_DIR / "sway-config.txt").write_text(sway_config_text, encoding="utf-8")

        (waybar_cfg_dir / "config").write_text(
            json.dumps(
                {
                    "layer": "top",
                    "position": "top",
                    "height": 32,
                    "margin": "0",
                    "spacing": 0,
                    "modules-left": ["tray"],
                    "tray": {
                        "icon-size": 22,
                        "spacing": 6,
                    },
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        (waybar_cfg_dir / "style.css").write_text(
            "\n".join(
                [
                    "* { margin: 0; padding: 0; border: none; border-radius: 0; min-height: 0; }",
                    "window#waybar { background: #111827; color: #f9fafb; }",
                    "#tray { padding: 0 8px; background: #111827; }",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        sway_env = dict(env)
        sway_env["WLR_BACKENDS"] = "headless"
        sway_env["WLR_LIBINPUT_NO_DEVICES"] = "1"
        sway_env["WLR_RENDERER"] = "pixman"
        sway_env["XDG_SESSION_TYPE"] = "wayland"
        sway_env["XDG_CURRENT_DESKTOP"] = "sway"

        sway_log = (ARTIFACT_DIR / "sway.log").open("w", encoding="utf-8")
        waybar_log = (ARTIFACT_DIR / "waybar.log").open("w", encoding="utf-8")
        desktop_log = (ARTIFACT_DIR / "threadlane-desktop.log").open("w", encoding="utf-8")

        sway_proc: subprocess.Popen[str] | None = None
        waybar_proc: subprocess.Popen[str] | None = None
        desktop_proc: subprocess.Popen[str] | None = None
        input_fixture: Any = None
        client_env: dict[str, str] | None = None
        cleanup_executed = False
        scene_log_handles = {
            "sway": sway_log,
            "waybar": waybar_log,
            "threadlane_desktop": desktop_log,
        }
        owned_log_paths = {
            "sway": ARTIFACT_DIR / "sway.log",
            "waybar": ARTIFACT_DIR / "waybar.log",
            "persistent_input": ARTIFACT_DIR / "persistent-input.log",
            "threadlane_desktop": ARTIFACT_DIR / "threadlane-desktop.log",
        }
        report_path = ARTIFACT_DIR / "vm-acceptance-report.json"

        def _scene_owners() -> list[tuple[str, Any, float, float]]:
            return [
                ("threadlane_desktop", desktop_proc, 5.0, 2.0),
                ("waybar", waybar_proc, 5.0, 2.0),
                ("persistent_input", input_fixture, 3.0, 2.0),
                ("sway", sway_proc, 5.0, 2.0),
            ]

        try:
            log("Starting headless Wayland compositor (sway)...")
            sway_proc = subprocess.Popen(
                ["sway", "-c", str(sway_cfg_dir / "config")],
                env=sway_env,
                stdout=sway_log,
                stderr=subprocess.STDOUT,
                text=True,
            )

            runtime_dir = pathlib.Path(env["XDG_RUNTIME_DIR"])

            def _discover_wayland_and_ipc() -> tuple[str, str] | None:
                wl_socks = sorted(
                    p.name
                    for p in runtime_dir.glob("wayland-*")
                    if not p.name.endswith(".lock")
                )
                ipc_socks = sorted(str(p) for p in runtime_dir.glob("sway-ipc.*.sock"))
                if wl_socks and ipc_socks:
                    return (wl_socks[0], ipc_socks[0])
                return None

            wayland_display, sway_sock = wait_until(
                "Wayland socket and sway IPC socket",
                _discover_wayland_and_ipc,
                timeout_sec=20.0,
            )

            client_env = dict(env)
            client_env["WAYLAND_DISPLAY"] = wayland_display
            client_env["SWAYSOCK"] = sway_sock
            client_env["XDG_SESSION_TYPE"] = "wayland"
            client_env["XDG_CURRENT_DESKTOP"] = "sway"
            client_env["GDK_BACKEND"] = "wayland"
            client_env["WEBKIT_DISABLE_DMABUF_RENDERER"] = "1"
            client_env["WEBKIT_DISABLE_COMPOSITING_MODE"] = "1"
            client_env["LIBGL_ALWAYS_SOFTWARE"] = "1"

            rendering_environment = {
                "isolated_vm_only": True,
                "wlr_backends": sway_env["WLR_BACKENDS"],
                "wlr_renderer": sway_env["WLR_RENDERER"],
                "webkit_disable_dmabuf_renderer": client_env["WEBKIT_DISABLE_DMABUF_RENDERER"],
                "webkit_disable_compositing_mode": client_env["WEBKIT_DISABLE_COMPOSITING_MODE"],
                "libgl_always_software": client_env["LIBGL_ALWAYS_SOFTWARE"],
            }

            # Configure dual virtual outputs with known solid compositor background:
            # HEADLESS-1 @ scale 1.0 (1920x1080) and HEADLESS-2 @ scale 2.0 (2560x1440 physical / 1280x720 logical).
            sway_command(
                [
                    "output",
                    "HEADLESS-1",
                    "resolution",
                    "1920x1080",
                    "position",
                    "0",
                    "0",
                    "scale",
                    "1",
                    "bg",
                    DESKTOP_BG_HEX,
                    "solid_color",
                ],
                client_env,
            )
            sway_command(["create_output"], client_env)
            sway_command(
                [
                    "output",
                    "HEADLESS-2",
                    "resolution",
                    "2560x1440",
                    "position",
                    "1920",
                    "0",
                    "scale",
                    "2",
                    "bg",
                    DESKTOP_BG_HEX,
                    "solid_color",
                ],
                client_env,
            )

            outputs = sway_json(["-t", "get_outputs", "-r"], client_env)
            (ARTIFACT_DIR / "outputs.json").write_text(
                json.dumps(outputs, indent=2) + "\n", encoding="utf-8"
            )
            out_by_name = {o["name"]: o for o in outputs}
            if "HEADLESS-1" not in out_by_name or "HEADLESS-2" not in out_by_name:
                raise HarnessFailure(
                    f"Expected HEADLESS-1 and HEADLESS-2 outputs, got {list(out_by_name)}"
                )
            out1 = out_by_name["HEADLESS-1"]
            out2 = out_by_name["HEADLESS-2"]
            if float(out1["scale"]) != 1.0 or float(out2["scale"]) != 2.0:
                raise HarnessFailure(
                    f"Expected output scales 1.0 and 2.0, got {out1['scale']} and {out2['scale']}"
                )

            # Start persistent guest virtual pointer + keyboard BEFORE Waybar and app launch.
            helper_bin = shutil.which("threadlane-persistent-wayland-input")
            if not helper_bin:
                raise HarnessFailure(
                    "Missing threadlane-persistent-wayland-input executable on PATH."
                )
            log("Starting persistent guest Wayland virtual pointer + keyboard fixture on seat0...")
            try:
                input_fixture = shared.PersistentGuestInputFixture(
                    [helper_bin],
                    env=client_env,
                    stderr_log_path=ARTIFACT_DIR / "persistent-input.log",
                )
                input_ready_info = input_fixture.start(ready_timeout_sec=10.0)
            except shared.AcceptanceError as exc:
                raise HarnessFailure(str(exc)) from exc

            def _verified_seat0_before() -> dict[str, Any]:
                seats_payload = sway_json(["-t", "get_seats", "-r"], client_env)
                inputs_payload = sway_json(["-t", "get_inputs", "-r"], client_env)
                (ARTIFACT_DIR / "seats-before.json").write_text(
                    json.dumps(seats_payload, indent=2) + "\n",
                    encoding="utf-8",
                )
                (ARTIFACT_DIR / "inputs-before.json").write_text(
                    json.dumps(inputs_payload, indent=2) + "\n",
                    encoding="utf-8",
                )
                try:
                    return shared.verify_seat0_input_state(seats_payload, inputs_payload)
                except shared.AcceptanceError as exc:
                    raise HarnessFailure(str(exc)) from exc

            before_input_state = wait_until(
                "seat0 pointer|keyboard capabilities (3) and XKB keymap before Waybar/app launch",
                _verified_seat0_before,
                timeout_sec=10.0,
            )
            input_fixture.assert_running("persistent guest input helper before Waybar launch")
            log(
                f"Verified seat0 capabilities={before_input_state['capabilities']} "
                f"(pid={input_ready_info['pid']}, devices={before_input_state['device_identities']})"
            )

            log("Starting Wayland StatusNotifier tray host (waybar)...")
            waybar_proc = subprocess.Popen(
                [
                    "waybar",
                    "-c",
                    str(waybar_cfg_dir / "config"),
                    "-s",
                    str(waybar_cfg_dir / "style.css"),
                ],
                env=client_env,
                stdout=waybar_log,
                stderr=subprocess.STDOUT,
                text=True,
            )

            def _watcher_owned() -> bool:
                res = run_checked(
                    [
                        "gdbus",
                        "call",
                        "--session",
                        "--dest",
                        "org.freedesktop.DBus",
                        "--object-path",
                        "/org/freedesktop/DBus",
                        "--method",
                        "org.freedesktop.DBus.NameHasOwner",
                        "org.kde.StatusNotifierWatcher",
                    ],
                    env=client_env,
                )
                return "(true,)" in res.stdout

            wait_until("org.kde.StatusNotifierWatcher D-Bus ownership", _watcher_owned)

            # Wait for swaybg + waybar to paint clean solid background on both outputs before app launch.
            init_bg_h1_path = ARTIFACT_DIR / "00a-init-bg-headless1.png"
            init_bg_h2_path = ARTIFACT_DIR / "00b-init-bg-headless2.png"
            init_bar1_px_h = wait_until(
                "clean solid compositor background on HEADLESS-1",
                lambda: verify_clean_workspace_background(
                    output_info=out1,
                    screenshot_path=init_bg_h1_path,
                    env=client_env,
                ),
            )
            wait_until(
                "clean solid compositor background on HEADLESS-2",
                lambda: verify_clean_workspace_background(
                    output_info=out2,
                    screenshot_path=init_bg_h2_path,
                    env=client_env,
                ),
            )

            # Verify unified CLI binary is available on PATH alongside threadlane-desktop.
            cli_ver = run_checked(["threadlane", "--version"], env=client_env)
            log(f"Verified CLI binary: {cli_ver.stdout.strip()}")

            input_fixture.assert_running(
                "persistent guest input helper before threadlane-desktop launch"
            )

            # Focus HEADLESS-1 initially using verified global cursor set before launching threadlane-desktop.
            pointer_click(960, 540, client_env)

            log("Launching threadlane-desktop --no-codex-startup-setup...")
            desktop_proc = subprocess.Popen(
                ["threadlane-desktop", "--no-codex-startup-setup"],
                env=client_env,
                stdout=desktop_log,
                stderr=subprocess.STDOUT,
                text=True,
            )

            default_main_shot = ARTIFACT_DIR / "01-main-default-1040x760.png"
            main_node, default_main_paint = wait_until(
                "painted initial native Threadlane main window (1040x760)",
                lambda: capture_and_evaluate_main_window_painted(
                    desktop_proc=desktop_proc,
                    output_info=out1,
                    before_bg_path=init_bg_h1_path,
                    screenshot_path=default_main_shot,
                    bar_px_h=init_bar1_px_h,
                    expected_width=1040,
                    expected_height=760,
                    env=client_env,
                ),
                timeout_sec=35.0,
                interval_sec=0.25,
            )
            (ARTIFACT_DIR / "tree-initial-main.json").write_text(
                json.dumps(main_node, indent=2) + "\n", encoding="utf-8"
            )

            if main_node.get("shell") != "xdg_shell":
                raise HarnessFailure(
                    f"Expected native Wayland xdg_shell for main window, got {main_node.get('shell')!r}"
                )
            checks["wayland_native_shell"] = True

            # Verify requested default main window size (1040x760) BEFORE any explicit swaymsg resize.
            initial_default_main_rect = dict(main_node["rect"])
            if (
                int(initial_default_main_rect["width"]) != 1040
                or int(initial_default_main_rect["height"]) != 760
            ):
                raise HarnessFailure(
                    f"Expected requested default main window size 1040x760 without compositor resize, got {initial_default_main_rect}"
                )
            checks["main_window_geometry"] = True
            checks["no_forced_tray_compositor_rules"] = True

            # Explicit main-window-only responsive resize: 360x500 narrow minimum, then back to 1040x760 wide.
            main_con_id = int(main_node["id"])
            sway_command(
                [
                    f"[con_id={main_con_id}]",
                    "resize",
                    "set",
                    "width",
                    "360",
                    "px",
                    "height",
                    "500",
                    "px",
                ],
                client_env,
            )

            resized_min_shot = ARTIFACT_DIR / "02-main-explicit-resize-360x500.png"
            min_node, resized_min_paint = wait_until(
                "painted main window explicitly resized to 360x500",
                lambda: capture_and_evaluate_main_window_painted(
                    desktop_proc=desktop_proc,
                    output_info=out1,
                    before_bg_path=init_bg_h1_path,
                    screenshot_path=resized_min_shot,
                    bar_px_h=init_bar1_px_h,
                    expected_width=360,
                    expected_height=500,
                    env=client_env,
                ),
                timeout_sec=25.0,
                interval_sec=0.25,
            )
            explicit_resized_min_main_rect = dict(min_node["rect"])

            sway_command(
                [
                    f"[con_id={main_con_id}]",
                    "resize",
                    "set",
                    "width",
                    "1040",
                    "px",
                    "height",
                    "760",
                    "px",
                ],
                client_env,
            )

            restored_wide_shot = ARTIFACT_DIR / "02b-main-restored-wide-1040x760.png"
            wide_node, restored_wide_paint = wait_until(
                "painted main window restored to 1040x760",
                lambda: capture_and_evaluate_main_window_painted(
                    desktop_proc=desktop_proc,
                    output_info=out1,
                    before_bg_path=init_bg_h1_path,
                    screenshot_path=restored_wide_shot,
                    bar_px_h=init_bar1_px_h,
                    expected_width=1040,
                    expected_height=760,
                    env=client_env,
                ),
                timeout_sec=25.0,
                interval_sec=0.25,
            )
            explicit_resized_wide_main_rect = dict(wide_node["rect"])
            checks["min_window_geometry"] = True

            # Verify StatusNotifierItem registration on session D-Bus and rendered panel icons on both outputs.
            def _sni_item_registered() -> bool:
                res = run_checked(
                    [
                        "gdbus",
                        "call",
                        "--session",
                        "--dest",
                        "org.kde.StatusNotifierWatcher",
                        "--object-path",
                        "/StatusNotifierWatcher",
                        "--method",
                        "org.freedesktop.DBus.Properties.Get",
                        "org.kde.StatusNotifierWatcher",
                        "RegisteredStatusNotifierItems",
                    ],
                    env=client_env,
                )
                return "StatusNotifierItem" in res.stdout

            wait_until("StatusNotifierItem registration", _sni_item_registered)

            tray2_center = wait_until(
                "rendered tray icon on scaled target monitor HEADLESS-2",
                lambda: locate_tray_icon_logical_center(
                    output_x=1920,
                    output_y=0,
                    expected_scale=2.0,
                    screenshot_path=ARTIFACT_DIR / "bar-headless-2.png",
                    env=client_env,
                ),
            )
            tray1_center = wait_until(
                "rendered tray icon on primary monitor HEADLESS-1",
                lambda: locate_tray_icon_logical_center(
                    output_x=0,
                    output_y=0,
                    expected_scale=1.0,
                    screenshot_path=ARTIFACT_DIR / "bar-headless-1.png",
                    env=client_env,
                ),
            )

            # Hide main window via real close-to-tray path (default CloseBehavior::HideToTray)
            # so zero normal windows remain on screen during rendered popup pixel measurements.
            log("Hiding main window via real close-to-tray path before rendered popup measurement...")
            sway_command([f"[con_id={main_con_id}]", "kill"], client_env)

            def _no_visible_normal_windows() -> bool:
                if desktop_proc.poll() is not None:
                    raise HarnessFailure(
                        "Closing main window with active tray unexpectedly terminated app."
                    )
                tree = sway_json(["-t", "get_tree", "-r"], client_env)
                return len(list_visible_normal_windows(tree)) == 0

            wait_until("main window hidden to tray with zero visible normal windows", _no_visible_normal_windows)

            # Capture verified clean solid-background baselines on both outputs.
            before_h2_scale2 = ARTIFACT_DIR / "03a-before-headless2-scale2.png"
            before_h1_for_h2 = ARTIFACT_DIR / "03b-before-headless1-while-closed.png"
            bar2_px_h = wait_until(
                "clean baseline on HEADLESS-2 after hiding main",
                lambda: verify_clean_workspace_background(
                    output_info=out2,
                    screenshot_path=before_h2_scale2,
                    env=client_env,
                ),
            )
            bar1_px_h = wait_until(
                "clean baseline on HEADLESS-1 after hiding main",
                lambda: verify_clean_workspace_background(
                    output_info=out1,
                    screenshot_path=before_h1_for_h2,
                    env=client_env,
                ),
            )

            # Open tray popup on HEADLESS-2 (scale 2.0) via real StatusNotifier icon click.
            log(f"Opening tray popup on HEADLESS-2 (scale 2.0) via StatusNotifier icon at {tray2_center}...")
            pointer_click(2560, 360, client_env)
            time.sleep(0.12)
            pointer_click(tray2_center[0], tray2_center[1], client_env)

            open_h2_shot = ARTIFACT_DIR / "04a-open-headless2-scale2.png"
            other_h1_shot = ARTIFACT_DIR / "04b-other-headless1-while-h2-open.png"
            popup_scale2_measurement = wait_until(
                "rendered 360x320 popup on HEADLESS-2 (scale 2.0) with zero pixels on HEADLESS-1",
                lambda: evaluate_rendered_popup_capture(
                    target_output=out2,
                    before_target_path=before_h2_scale2,
                    after_target_path=open_h2_shot,
                    target_bar_px_h=bar2_px_h,
                    clicked_icon_center=tray2_center,
                    env=client_env,
                    other_output=out1,
                    before_other_path=before_h1_for_h2,
                    after_other_path=other_h1_shot,
                    other_bar_px_h=bar1_px_h,
                ),
            )
            checks["panel_tray_pointer_open"] = True

            # Pointer-transit survival test with focus_follows_mouse yes:
            # Move WITHOUT clicking from the panel icon across boundary waypoints into the measured popup
            # and dwell well beyond the 180ms blur grace window.
            pop2_rect = popup_scale2_measurement["logical_rect"]
            pop2_x = float(pop2_rect["x"])
            pop2_y = float(pop2_rect["y"])
            pop2_w = float(pop2_rect["width"])
            pop2_h = float(pop2_rect["height"])
            out2_y = float(out2["rect"]["y"])
            panel2_bottom_logical = out2_y + (bar2_px_h / 2.0)
            entry_x = min(max(tray2_center[0], pop2_x + 28.0), pop2_x + pop2_w - 28.0)
            boundary_y = (
                (panel2_bottom_logical + pop2_y) / 2.0
                if pop2_y > panel2_bottom_logical + 1.0
                else pop2_y + 2.0
            )
            transit_waypoints = [
                (entry_x, panel2_bottom_logical - 2.0),
                (entry_x, boundary_y),
                (pop2_x + pop2_w * 0.25, pop2_y + pop2_h * 0.18),
                (pop2_x + pop2_w * 0.45, pop2_y + pop2_h * 0.45),
                (pop2_x + pop2_w * 0.50, pop2_y + pop2_h * 0.65),
            ]
            log(f"Moving pointer WITHOUT click from panel into popup across {len(transit_waypoints)} points...")
            recorded_waypoints: list[list[int]] = []
            for wx, wy in transit_waypoints:
                ix, iy = pointer_move(wx, wy, client_env)
                recorded_waypoints.append([ix, iy])
                time.sleep(0.07)

            dwell_target_sec = 9.55
            dwell_start_mono = time.monotonic()
            time.sleep(dwell_target_sec)
            while True:
                dwell_elapsed_sec = time.monotonic() - dwell_start_mono
                if dwell_elapsed_sec >= shared.MIN_H2_REFRESH_DWELL_SECONDS:
                    break
                time.sleep(min(0.05, shared.MIN_H2_REFRESH_DWELL_SECONDS - dwell_elapsed_sec))
            dwell_ms = round(dwell_elapsed_sec * 1000.0, 2)
            dwell_shot = ARTIFACT_DIR / "05-after-pointer-transit-dwell-headless2.png"
            dwell_other_shot = ARTIFACT_DIR / "05b-other-headless1-after-dwell.png"
            post_dwell_measurement = evaluate_rendered_popup_capture(
                target_output=out2,
                before_target_path=before_h2_scale2,
                after_target_path=dwell_shot,
                target_bar_px_h=bar2_px_h,
                clicked_icon_center=tray2_center,
                env=client_env,
                other_output=out1,
                before_other_path=before_h1_for_h2,
                after_other_path=dwell_other_shot,
                other_bar_px_h=bar1_px_h,
            )
            if desktop_proc.poll() is not None:
                raise HarnessFailure("Desktop process exited unexpectedly during pointer transit/dwell.")
            checks["pointer_transit_dwell_survival"] = True

            # Click strictly outside the measured popup bounds and verify rendered popup disappears without app exit.
            out2_x = float(out2["rect"]["x"])
            out2_w = float(out2["rect"]["width"])
            out2_h = float(out2["rect"]["height"])
            outside_x = out2_x + out2_w - 100.0
            outside_y = out2_y + out2_h - 100.0
            if (
                pop2_x - 30.0 <= outside_x <= pop2_x + pop2_w + 30.0
                and pop2_y - 30.0 <= outside_y <= pop2_y + pop2_h + 30.0
            ):
                outside_x = out2_x + 100.0
            if (
                pop2_x - 20.0 <= outside_x <= pop2_x + pop2_w + 20.0
                and pop2_y - 20.0 <= outside_y <= pop2_y + pop2_h + 20.0
            ):
                raise HarnessFailure(f"Computed outside click ({outside_x}, {outside_y}) overlaps {pop2_rect}")

            log(f"Testing click-outside dismissal at ({outside_x}, {outside_y})...")
            pointer_click(outside_x, outside_y, client_env)

            after_outside_shot = ARTIFACT_DIR / "06-after-outside-click-headless2.png"

            def _popup_dismissed_on_h2() -> bool:
                run_checked(["grim", "-o", "HEADLESS-2", str(after_outside_shot)], env=client_env)
                changed_px, _, _ = count_workspace_diff_pixels(
                    before_h2_scale2,
                    after_outside_shot,
                    bar2_px_h,
                )
                return changed_px == 0

            wait_until("tray popup dismissed by outside click on HEADLESS-2", _popup_dismissed_on_h2)
            if desktop_proc.poll() is not None:
                raise HarnessFailure("Outside click must hide popup without quitting desktop app.")
            checks["click_outside_dismiss"] = True

            # Reopen on HEADLESS-1 (scale 1.0), measure rendered 360x320 popup & output ownership,
            # then send Escape BEFORE any interior click and verify disappearance.
            time.sleep(0.20)
            before_h1_scale1 = ARTIFACT_DIR / "07a-before-headless1-scale1.png"
            before_h2_for_h1 = ARTIFACT_DIR / "07b-before-headless2-while-closed.png"
            bar1_px_h = verify_clean_workspace_background(
                output_info=out1,
                screenshot_path=before_h1_scale1,
                env=client_env,
            )
            bar2_px_h = verify_clean_workspace_background(
                output_info=out2,
                screenshot_path=before_h2_for_h1,
                env=client_env,
            )

            log(f"Reopening tray popup on HEADLESS-1 (scale 1.0) at {tray1_center} for scale-1 measurement & Escape...")
            pointer_click(960, 540, client_env)
            time.sleep(0.12)
            pointer_click(tray1_center[0], tray1_center[1], client_env)
            interior_click_before_escape = False

            open_h1_shot = ARTIFACT_DIR / "07c-open-headless1-scale1.png"
            other_h2_shot = ARTIFACT_DIR / "07d-other-headless2-while-h1-open.png"
            popup_scale1_measurement = wait_until(
                "rendered 360x320 popup on HEADLESS-1 (scale 1.0) with zero pixels on HEADLESS-2",
                lambda: evaluate_rendered_popup_capture(
                    target_output=out1,
                    before_target_path=before_h1_scale1,
                    after_target_path=open_h1_shot,
                    target_bar_px_h=bar1_px_h,
                    clicked_icon_center=tray1_center,
                    env=client_env,
                    other_output=out2,
                    before_other_path=before_h2_for_h1,
                    after_other_path=other_h2_shot,
                    other_bar_px_h=bar2_px_h,
                ),
            )
            checks["rendered_popup_geometry"] = True
            checks["target_monitor_and_scale"] = True
            checks["reopen_after_dismiss"] = True

            # Send Escape immediately without moving into or clicking inside the popup.
            time.sleep(0.20)
            run_checked(["wtype", "-k", "Escape"], env=client_env)

            after_escape_shot = ARTIFACT_DIR / "08-after-escape-headless1.png"

            def _popup_dismissed_on_h1() -> bool:
                run_checked(["grim", "-o", "HEADLESS-1", str(after_escape_shot)], env=client_env)
                changed_px, _, _ = count_workspace_diff_pixels(
                    before_h1_scale1,
                    after_escape_shot,
                    bar1_px_h,
                )
                return changed_px == 0

            wait_until("tray popup dismissed via Escape before any interior click", _popup_dismissed_on_h1)
            if desktop_proc.poll() is not None:
                raise HarnessFailure("Escape dismissal must not terminate desktop app.")
            input_fixture.assert_running(
                "persistent guest input helper after Escape dismissal"
            )
            checks["escape_dismiss"] = True

            # Reopen tray popup on HEADLESS-1 after Escape dismissal, verify initial paint and
            # cross-output cleanliness, then capture 10 fresh immediate pixel measurements at 1s
            # intervals (>= 9s sampling window across multiple 4s UI refresh cycles) BEFORE clicking
            # "Open Threadlane" using measured popup bounds.
            time.sleep(0.20)
            before_open_ctrl = ARTIFACT_DIR / "09a-before-open-control-headless1.png"
            before_open_ctrl_h2 = ARTIFACT_DIR / "09a-before-open-control-headless2.png"
            bar1_px_h = verify_clean_workspace_background(
                output_info=out1,
                screenshot_path=before_open_ctrl,
                env=client_env,
            )
            bar2_px_h = verify_clean_workspace_background(
                output_info=out2,
                screenshot_path=before_open_ctrl_h2,
                env=client_env,
            )
            log("Reopening tray popup to verify sustained refresh-paint survival and Open Threadlane control...")
            pointer_click(tray1_center[0], tray1_center[1], client_env)
            open_ctrl_shot = ARTIFACT_DIR / "09b-open-for-restore-headless1.png"
            open_ctrl_other_shot = ARTIFACT_DIR / "09b-other-headless2-open-for-restore.png"
            popup_for_open_initial = wait_until(
                "rendered tray popup opened for Open Threadlane control",
                lambda: evaluate_rendered_popup_capture(
                    target_output=out1,
                    before_target_path=before_open_ctrl,
                    after_target_path=open_ctrl_shot,
                    target_bar_px_h=bar1_px_h,
                    clicked_icon_center=tray1_center,
                    env=client_env,
                    other_output=out2,
                    before_other_path=before_open_ctrl_h2,
                    after_other_path=open_ctrl_other_shot,
                    other_bar_px_h=bar2_px_h,
                ),
            )
            reopen_paint_mono = time.monotonic()
            prev_sample_mono = reopen_paint_mono
            refresh_paint_samples: list[dict[str, Any]] = []
            sample_monos: list[float] = []
            for refresh_step in range(1, shared.REQUIRED_H1_REFRESH_SURVIVAL_SAMPLES + 1):
                target_sample_mono = prev_sample_mono + 1.01
                time.sleep(1.01)
                while True:
                    now_mono = time.monotonic()
                    remaining_sec = target_sample_mono - now_mono
                    if remaining_sec <= 0.0:
                        break
                    time.sleep(min(0.05, remaining_sec))
                if desktop_proc.poll() is not None:
                    raise HarnessFailure(
                        f"Desktop process exited unexpectedly before refresh-paint sample {refresh_step}."
                    )
                refresh_measurement = evaluate_rendered_popup_capture(
                    target_output=out1,
                    before_target_path=before_open_ctrl,
                    after_target_path=ARTIFACT_DIR
                    / f"09b-refresh-survival-{refresh_step:02d}-headless1.png",
                    target_bar_px_h=bar1_px_h,
                    clicked_icon_center=tray1_center,
                    env=client_env,
                    other_output=out2,
                    before_other_path=before_open_ctrl_h2,
                    after_other_path=ARTIFACT_DIR
                    / f"09b-refresh-survival-{refresh_step:02d}-other-headless2.png",
                    other_bar_px_h=bar2_px_h,
                )
                sample_mono = time.monotonic()
                sample_elapsed_sec = sample_mono - reopen_paint_mono
                sample_interval_sec = sample_mono - prev_sample_mono
                prev_sample_mono = sample_mono
                sample_monos.append(sample_mono)
                refresh_sample = {
                    "sample_index": refresh_step,
                    "elapsed_seconds": round(sample_elapsed_sec, 4),
                    "interval_seconds": round(sample_interval_sec, 4),
                    **refresh_measurement,
                }
                refresh_paint_samples.append(refresh_sample)
                popup_for_open = refresh_sample

            sampling_window_sec = sample_monos[-1] - sample_monos[0]
            total_refresh_duration_sec = sample_monos[-1] - reopen_paint_mono
            refresh_paint_survival = {
                "ui_refresh_interval_ms": int(shared.UI_REFRESH_INTERVAL_MS),
                "headless2_scale2_dwell": {
                    "dwell_seconds": round(dwell_elapsed_sec, 4),
                    "dwell_ms": dwell_ms,
                    "measurement": post_dwell_measurement,
                },
                "headless1_scale1_reopen": {
                    "initial_measurement": popup_for_open_initial,
                    "target_interval_seconds": 1.0,
                    "sample_count": len(refresh_paint_samples),
                    "sampling_window_seconds": round(sampling_window_sec, 4),
                    "total_duration_seconds": round(total_refresh_duration_sec, 4),
                    "samples": refresh_paint_samples,
                },
            }
            (ARTIFACT_DIR / "refresh-paint-survival.json").write_text(
                json.dumps(refresh_paint_survival, indent=2) + "\n",
                encoding="utf-8",
            )
            open_rect = popup_for_open["logical_rect"]
            open_click_x = float(open_rect["x"]) + float(open_rect["width"]) * (110.0 / 360.0)
            open_click_y = float(open_rect["y"]) + float(open_rect["height"]) * (296.0 / 320.0)
            pointer_click(open_click_x, open_click_y, client_env)

            restored_main = wait_until(
                "main window restored by Open Threadlane tray control",
                lambda: find_visible_window_by_title(
                    sway_json(["-t", "get_tree", "-r"], client_env),
                    "Threadlane",
                ),
            )
            restored_shot = ARTIFACT_DIR / "09c-main-restored-headless1.png"
            wait_until(
                "tray popup hidden after Open Threadlane click",
                lambda: verify_popup_cleared_outside_main(
                    output_info=out1,
                    popup_phys_rect=popup_for_open["physical_rect"],
                    restored_main_logical_rect=restored_main["rect"],
                    screenshot_path=restored_shot,
                    env=client_env,
                ),
            )
            checks["hide_main_to_tray_and_open_control"] = True

            # Reopen tray popup on clean HEADLESS-2 while restored main stays open on HEADLESS-1,
            # then click "Quit" using measured popup bounds.
            time.sleep(0.25)
            before_quit_shot = ARTIFACT_DIR / "10a-before-quit-headless2.png"
            bar2_px_h = verify_clean_workspace_background(
                output_info=out2,
                screenshot_path=before_quit_shot,
                env=client_env,
            )

            log("Reopening tray popup and clicking Quit control via measured popup bounds...")
            pointer_click(2560, 360, client_env)
            time.sleep(0.12)
            pointer_click(tray2_center[0], tray2_center[1], client_env)
            quit_open_shot = ARTIFACT_DIR / "10b-open-for-quit-headless2.png"
            popup_for_quit = wait_until(
                "rendered tray popup opened for Quit control",
                lambda: evaluate_rendered_popup_capture(
                    target_output=out2,
                    before_target_path=before_quit_shot,
                    after_target_path=quit_open_shot,
                    target_bar_px_h=bar2_px_h,
                    clicked_icon_center=tray2_center,
                    env=client_env,
                ),
            )
            quit_rect = popup_for_quit["logical_rect"]
            quit_click_x = float(quit_rect["x"]) + float(quit_rect["width"]) * (318.0 / 360.0)
            quit_click_y = float(quit_rect["y"]) + float(quit_rect["height"]) * (296.0 / 320.0)
            pointer_click(quit_click_x, quit_click_y, client_env)

            exit_code = desktop_proc.wait(timeout=15.0)
            if exit_code != 0:
                raise HarnessFailure(f"Expected clean exit code 0 on Quit, got {exit_code}")
            checks["quit_control"] = True

            input_fixture.assert_running(
                "persistent guest input helper after desktop Quit"
            )

            def _verified_seat0_preserved_after() -> dict[str, Any]:
                input_fixture.assert_running(
                    "persistent guest input helper during post-run seat0 verification"
                )
                seats_after = sway_json(["-t", "get_seats", "-r"], client_env)
                inputs_after = sway_json(["-t", "get_inputs", "-r"], client_env)
                (ARTIFACT_DIR / "seats-after.json").write_text(
                    json.dumps(seats_after, indent=2) + "\n",
                    encoding="utf-8",
                )
                (ARTIFACT_DIR / "inputs-after.json").write_text(
                    json.dumps(inputs_after, indent=2) + "\n",
                    encoding="utf-8",
                )
                try:
                    after_state = shared.verify_seat0_input_state(seats_after, inputs_after)
                    return shared.verify_input_device_identities_preserved(
                        before_input_state,
                        after_state,
                        helper_pid_before=int(input_ready_info["pid"]),
                        helper_pid_after=input_fixture.pid,
                        helper_running_after=input_fixture.is_running(),
                        setup_keypresses_sent=int(input_ready_info["setup_keypresses_sent"]),
                    )
                except shared.AcceptanceError as exc:
                    raise HarnessFailure(str(exc)) from exc

            guest_input_summary = wait_until(
                "preserved seat0 pointer|keyboard device identities before cleanup",
                _verified_seat0_preserved_after,
                timeout_sec=5.0,
            )

            codex_dir = pathlib.Path(env["HOME"]) / ".codex"
            if codex_dir.exists():
                raise HarnessFailure(
                    f"--no-codex-startup-setup violated: {codex_dir} was created"
                )
            checks["no_codex_startup_setup_preserved"] = True

            unverified = [key for key, passed in checks.items() if passed is not True]
            if unverified:
                raise HarnessFailure(f"Refusing passed report with unverified check(s): {unverified}")

            report = {
                "schema_version": 2,
                "status": "passed",
                "wayland_display": wayland_display,
                "desktop_exit_code": exit_code,
                "compositor_invariants": compositor_invariants,
                "guest_input": guest_input_summary,
                "rendering_environment": rendering_environment,
                "main_window_paint_readiness": {
                    "default_1040x760": default_main_paint,
                    "resized_360x500": resized_min_paint,
                    "restored_wide_1040x760": restored_wide_paint,
                },
                "initial_main_rect": initial_default_main_rect,
                "initial_default_main_rect": initial_default_main_rect,
                "min_main_rect": explicit_resized_min_main_rect,
                "explicit_resized_min_main_rect": explicit_resized_min_main_rect,
                "explicit_resized_wide_main_rect": explicit_resized_wide_main_rect,
                "restored_main_rect": restored_main["rect"],
                "scaled_popover_rect": popup_scale2_measurement["logical_rect"],
                "scaled_output": {
                    "name": out2["name"],
                    "scale": out2["scale"],
                    "logical_rect": out2["rect"],
                },
                "rendered_popup_measurements": {
                    "scale_1": popup_scale1_measurement,
                    "scale_2": popup_scale2_measurement,
                },
                "pointer_transit": {
                    "clicked_during_transit": False,
                    "waypoints": recorded_waypoints,
                    "waypoints_count": len(recorded_waypoints),
                    "dwell_ms": dwell_ms,
                    "visible_after_dwell": True,
                    "post_dwell_logical_rect": post_dwell_measurement["logical_rect"],
                },
                "refresh_paint_survival": refresh_paint_survival,
                "escape_dismissal": {
                    "interior_click_before_escape": interior_click_before_escape,
                    "dismissed": True,
                },
                "checks": checks,
            }
            try:
                shared._verify_refresh_paint_survival_section(
                    refresh_paint_survival,
                    pointer_transit=report["pointer_transit"],
                )
            except shared.AcceptanceError as exc:
                raise HarnessFailure(str(exc)) from exc
            shared.complete_owned_scene_and_write_passed_report(
                report_path=report_path,
                report_payload=report,
                owners=_scene_owners(),
                log_handles=scene_log_handles,
            )
            cleanup_executed = True
            log("All native Wayland + StatusNotifier tray acceptance checks passed.")
            return 0
        except Exception as exc:
            prior_cleanup = (
                exc.scene_cleanup
                if isinstance(exc, shared.SceneCleanupError)
                else None
            )
            failed_report = shared.record_owned_scene_failure(
                report_path=report_path,
                exc=exc,
                compositor_invariants=compositor_invariants,
                checks=checks,
                artifact_dir=ARTIFACT_DIR,
                client_env=client_env,
                owned_log_paths=owned_log_paths,
                owners=_scene_owners(),
                log_handles=scene_log_handles,
                prior_cleanup=prior_cleanup,
            )
            cleanup_executed = True
            failure_diagnostics = failed_report.get("failure_diagnostics", {})
            scene_cleanup_summary = failed_report.get("scene_cleanup", {})
            try:
                log(
                    "Bounded failure diagnostics summary: "
                    + json.dumps(
                        {
                            "failed_error": failed_report.get("error"),
                            "checks": checks,
                            "outputs": failure_diagnostics.get("outputs"),
                            "sway_queries": failure_diagnostics.get("sway_queries"),
                            "owned_logs": failure_diagnostics.get("owned_logs"),
                            "cleanup_ok": scene_cleanup_summary.get("ok"),
                            "cleanup_errors": scene_cleanup_summary.get("errors"),
                        },
                        sort_keys=True,
                    )
                )
            except Exception:
                pass
            raise
        finally:
            if not cleanup_executed:
                active_exc = sys.exc_info()[1]
                fallback_cleanup = shared.cleanup_owned_scene_processes(
                    _scene_owners(),
                    log_handles=scene_log_handles,
                )
                cleanup_executed = True
                if active_exc is None and not fallback_cleanup["ok"]:
                    fallback_error = shared.redact_sensitive_text(
                        "Owned scene cleanup failed in finally: "
                        + "; ".join(fallback_cleanup["errors"])
                    )
                    fallback_failed_report = {
                        "schema_version": 2,
                        "status": "failed",
                        "error": fallback_error,
                        "compositor_invariants": compositor_invariants,
                        "checks": checks,
                        "scene_cleanup": fallback_cleanup,
                    }
                    try:
                        report_path.write_text(
                            json.dumps(fallback_failed_report, indent=2) + "\n",
                            encoding="utf-8",
                        )
                    except OSError:
                        pass
                    raise HarnessFailure(fallback_error)


    def main() -> int:
        if "--inner" in sys.argv[1:]:
            return run_inner_session()

        if ARTIFACT_DIR.exists():
            shutil.rmtree(ARTIFACT_DIR)
        ARTIFACT_DIR.mkdir(parents=True, mode=0o755)

        if ISOLATION_ROOT.exists():
            shutil.rmtree(ISOLATION_ROOT)
        make_private_dir(ISOLATION_ROOT, 0o700)

        isolated_env = {
            "PATH": os.environ.get("PATH", "/run/current-system/sw/bin"),
            "LANG": os.environ.get("LANG", "C.UTF-8"),
            "HOME": str(make_private_dir(ISOLATION_ROOT / "home")),
            "XDG_CONFIG_HOME": str(make_private_dir(ISOLATION_ROOT / "xdg-config")),
            "XDG_DATA_HOME": str(make_private_dir(ISOLATION_ROOT / "xdg-data")),
            "XDG_CACHE_HOME": str(make_private_dir(ISOLATION_ROOT / "xdg-cache")),
            "XDG_STATE_HOME": str(make_private_dir(ISOLATION_ROOT / "xdg-state")),
            "XDG_RUNTIME_DIR": str(make_private_dir(ISOLATION_ROOT / "xdg-runtime")),
            "XDG_DATA_DIRS": os.environ.get("XDG_DATA_DIRS", "/run/current-system/sw/share"),
            "GIO_EXTRA_MODULES": os.environ.get("GIO_EXTRA_MODULES", ""),
        }

        proc = subprocess.run(
            ["dbus-run-session", "--", sys.executable, __file__, "--inner"],
            env=isolated_env,
            check=False,
        )
        return proc.returncode


    if __name__ == "__main__":
        sys.exit(main())
  '';
in
pkgs.testers.runNixOSTest {
  name = "threadlane-desktop-acceptance";

  nodes.machine =
    { pkgs, ... }:
    {
      virtualisation.memorySize = 2048;
      virtualisation.cores = 2;

      services.dbus.enable = true;
      services.seatd.enable = true;

      xdg.portal = {
        enable = true;
        wlr.enable = true;
        extraPortals = [ pkgs.xdg-desktop-portal-gtk ];
        config.common.default = "*";
      };

      fonts.packages = [ pkgs.dejavu_fonts ];

      users.users.alice = {
        isNormalUser = true;
        uid = 1000;
        extraGroups = [
          "video"
          "render"
          "input"
          "seat"
        ];
      };

      environment.etc."threadlane-desktop-acceptance-lib.py" = {
        source = desktopAcceptanceLib;
        mode = "0444";
      };

      environment.etc."threadlane-vm-harness.py" = {
        source = vmHarnessScript;
        mode = "0555";
      };

      environment.systemPackages = [
        threadlane
        threadlaneDesktop
        persistentWaylandInput
        guestPython
        pkgs.dbus
        pkgs.glib
        pkgs.grim
        pkgs.jq
        pkgs.procps
        pkgs.sway
        pkgs.swaybg
        pkgs.waybar
        pkgs.wtype
      ];
    };

  testScript = ''
    import importlib.util
    import json
    import os
    from pathlib import Path
    import shutil
    import sys
    import tempfile
    from typing import Any

    start_all()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("dbus.service")

    copy_from_guest = getattr(machine, "copy_from_vm", None) or getattr(
        machine, "copy_from_machine", None
    )
    if copy_from_guest is None:
        raise RuntimeError(
            "NixOS test-driver machine provides neither copy_from_vm nor copy_from_machine"
        )

    try:
        machine.succeed("su - alice -c '${guestPython}/bin/python3 /etc/threadlane-vm-harness.py'")
    finally:
        machine.execute(
            "mkdir -p /tmp/vm-acceptance-artifacts && chmod -R a+rX /tmp/vm-acceptance-artifacts || true"
        )
        copy_from_guest("/tmp/vm-acceptance-artifacts", "vm-artifacts")
        out_env = os.environ.get("out")
        if out_env:
            vm_art_root = Path(out_env) / "vm-artifacts"
            nested_art = vm_art_root / "vm-acceptance-artifacts"
            if nested_art.is_dir() and not (vm_art_root / "vm-acceptance-report.json").exists():
                for entry in list(nested_art.iterdir()):
                    dest = vm_art_root / entry.name
                    if not dest.exists():
                        shutil.move(str(entry), str(dest))
                try:
                    nested_art.rmdir()
                except OSError:
                    pass

    report_raw = machine.succeed("cat /tmp/vm-acceptance-artifacts/vm-acceptance-report.json")
    da_spec = importlib.util.spec_from_file_location(
        "threadlane_desktop_acceptance_lib",
        "${desktopAcceptanceLib}",
    )
    assert da_spec is not None and da_spec.loader is not None, (
        "Unable to load shared desktop acceptance validator in NixOS test driver"
    )
    da_lib: Any = importlib.util.module_from_spec(da_spec)
    sys.modules[da_spec.name] = da_lib
    da_spec.loader.exec_module(da_lib)
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix="-vm-acceptance-report.json",
        encoding="utf-8",
    ) as tmp_report_file:
        tmp_report_file.write(report_raw)
        tmp_report_file.flush()
        da_lib.verify_vm_acceptance_report(Path(tmp_report_file.name))

    report = json.loads(report_raw)
    assert isinstance(report.get("refresh_paint_survival"), dict), (
        f"Missing refresh_paint_survival proof in passed VM report: {report}"
    )
    assert report.get("status") == "passed", f"Unexpected VM report status: {report}"
    required_checks = (
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
    checks = report.get("checks", {})
    for key in required_checks:
        assert checks.get(key) is True, f"VM check {key} missing or failed: {report}"

    guest_input = report.get("guest_input", {})
    assert guest_input.get("seat") == "seat0", f"Unexpected guest_input seat: {guest_input}"
    assert guest_input.get("capabilities_before") == 3, f"Expected seat0 caps 3 before: {guest_input}"
    assert guest_input.get("capabilities_after") == 3, f"Expected seat0 caps 3 after: {guest_input}"
    assert guest_input.get("setup_keypresses_sent") == 0, f"Expected 0 setup keypresses: {guest_input}"
    assert guest_input.get("identities_preserved") is True, f"Device identities not preserved: {guest_input}"

    rendering_env = report.get("rendering_environment", {})
    assert rendering_env.get("webkit_disable_compositing_mode") == "1", (
        f"Expected WEBKIT_DISABLE_COMPOSITING_MODE=1 in isolated VM: {rendering_env}"
    )

    paint_info = report.get("main_window_paint_readiness", {})
    assert isinstance(paint_info, dict), f"Missing main_window_paint_readiness: {report}"
    for stage_key, exp_w, exp_h in (
        ("default_1040x760", 1040, 760),
        ("resized_360x500", 360, 500),
        ("restored_wide_1040x760", 1040, 760),
    ):
        stage_entry = paint_info.get(stage_key)
        assert isinstance(stage_entry, dict) and stage_entry.get("painted") is True, (
            f"Main paint readiness missing or unpainted for {stage_key}: {paint_info}"
        )
        assert stage_entry.get("measurement_method") == "grim_pixel_diff_and_interior_contrast", (
            f"Unexpected measurement_method for {stage_key}: {stage_entry}"
        )
        native_rect = stage_entry.get("native_rect", {})
        assert (
            isinstance(native_rect, dict)
            and native_rect.get("width") == exp_w
            and native_rect.get("height") == exp_h
        ), f"Unexpected native_rect for {stage_key}: {stage_entry}"
        logical_rect = stage_entry.get("logical_rect", {})
        assert (
            isinstance(logical_rect, dict)
            and isinstance(logical_rect.get("width"), (int, float))
            and not isinstance(logical_rect.get("width"), bool)
            and abs(float(logical_rect["width"]) - float(exp_w)) <= 4.0
            and isinstance(logical_rect.get("height"), (int, float))
            and not isinstance(logical_rect.get("height"), bool)
            and abs(float(logical_rect["height"]) - float(exp_h)) <= 4.0
        ), f"Unexpected logical_rect for {stage_key}: {stage_entry}"
        fill_ratio = stage_entry.get("fill_ratio")
        assert (
            isinstance(fill_ratio, (int, float))
            and not isinstance(fill_ratio, bool)
            and float(fill_ratio) >= 0.85
        ), f"Unexpected fill_ratio for {stage_key}: {stage_entry}"
        stddev = stage_entry.get("interior_max_channel_stddev")
        assert (
            isinstance(stddev, (int, float))
            and not isinstance(stddev, bool)
            and float(stddev) >= 4.0
        ), f"Unexpected interior_max_channel_stddev for {stage_key}: {stage_entry}"
        buckets = stage_entry.get("interior_color_buckets")
        assert (
            isinstance(buckets, int)
            and not isinstance(buckets, bool)
            and buckets >= 3
        ), f"Unexpected interior_color_buckets for {stage_key}: {stage_entry}"
        bands = stage_entry.get("band_contrast_pixels", {})
        assert isinstance(bands, dict), f"Missing band_contrast_pixels for {stage_key}: {stage_entry}"
        for band_key in ("titlebar", "navigation", "workspace_body"):
            band_px = bands.get(band_key)
            assert (
                isinstance(band_px, int)
                and not isinstance(band_px, bool)
                and band_px >= 24
            ), f"Insufficient {band_key} contrast for {stage_key}: {stage_entry}"

    scene_cleanup = report.get("scene_cleanup", {})
    assert isinstance(scene_cleanup, dict) and scene_cleanup.get("ok") is True, (
        f"Owned scene cleanup missing or failed: {scene_cleanup}"
    )
    assert scene_cleanup.get("errors") == [], (
        f"Unexpected scene cleanup errors: {scene_cleanup}"
    )
    cleanup_owners = scene_cleanup.get("owners", {})
    assert isinstance(cleanup_owners, dict), f"Missing scene_cleanup.owners: {scene_cleanup}"
    for owner_key in ("threadlane_desktop", "waybar", "persistent_input", "sway"):
        owner_entry = cleanup_owners.get(owner_key)
        assert (
            isinstance(owner_entry, dict)
            and owner_entry.get("present") is True
            and owner_entry.get("reaped") is True
            and owner_entry.get("ok") is True
        ), f"Owned process {owner_key} not cleanly reaped: {scene_cleanup}"
  '';
}
