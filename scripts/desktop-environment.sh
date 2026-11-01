#!/bin/sh
# Process-local defaults for the packaged Threadlane desktop launcher.
# This file is sourced by the Nix wrapper; it does not change global state.

# Usage: threadlane_configure_desktop_environment [nvidia-version-file]
#
# The optional driver path exists for deterministic offline tests. Production
# uses the default /proc path and passes no argument.
threadlane_configure_desktop_environment() {
    _threadlane_nvidia_version_file=${1:-/proc/driver/nvidia/version}

    if [ "${GDK_BACKEND:-}" != "x11" ] \
        && { [ -n "${WAYLAND_DISPLAY:-}" ] || [ "${XDG_SESSION_TYPE:-}" = "wayland" ]; } \
        && [ "${__NV_DISABLE_EXPLICIT_SYNC+x}" != "x" ] \
        && [ -f "$_threadlane_nvidia_version_file" ] \
        && [ -r "$_threadlane_nvidia_version_file" ]; then
        __NV_DISABLE_EXPLICIT_SYNC=1
        export __NV_DISABLE_EXPLICIT_SYNC
    fi

    unset _threadlane_nvidia_version_file
}
