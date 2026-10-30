//! Trusted native-launcher XDG environment restoration for external CLI child processes.
//!
//! When Threadlane's native preview launcher isolates the desktop GUI with private
//! `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, `XDG_CACHE_HOME`, and `XDG_STATE_HOME` directories,
//! it captures the caller's original XDG environment values before overriding them in four
//! fixed marker variables:
//! - [`THREADLANE_CLI_XDG_CONFIG_HOME`]
//! - [`THREADLANE_CLI_XDG_DATA_HOME`]
//! - [`THREADLANE_CLI_XDG_CACHE_HOME`]
//! - [`THREADLANE_CLI_XDG_STATE_HOME`]
//!
//! [`restore_cli_environment`] applies only this four-variable whitelist to a child
//! [`std::process::Command`] without mutating the parent process environment or touching
//! unrelated variables (`HOME`, `CODEX_HOME`, `PATH`, Git settings, or credentials).

use std::ffi::OsString;
use std::process::Command;

/// Native launcher marker capturing the caller's original `XDG_CONFIG_HOME`.
pub const THREADLANE_CLI_XDG_CONFIG_HOME: &str = "THREADLANE_CLI_XDG_CONFIG_HOME";

/// Native launcher marker capturing the caller's original `XDG_DATA_HOME`.
pub const THREADLANE_CLI_XDG_DATA_HOME: &str = "THREADLANE_CLI_XDG_DATA_HOME";

/// Native launcher marker capturing the caller's original `XDG_CACHE_HOME`.
pub const THREADLANE_CLI_XDG_CACHE_HOME: &str = "THREADLANE_CLI_XDG_CACHE_HOME";

/// Native launcher marker capturing the caller's original `XDG_STATE_HOME`.
pub const THREADLANE_CLI_XDG_STATE_HOME: &str = "THREADLANE_CLI_XDG_STATE_HOME";

const XDG_MARKER_WHITELIST: [(&str, &str); 4] = [
    (THREADLANE_CLI_XDG_CONFIG_HOME, "XDG_CONFIG_HOME"),
    (THREADLANE_CLI_XDG_DATA_HOME, "XDG_DATA_HOME"),
    (THREADLANE_CLI_XDG_CACHE_HOME, "XDG_CACHE_HOME"),
    (THREADLANE_CLI_XDG_STATE_HOME, "XDG_STATE_HOME"),
];

/// Restores the caller's original XDG base-directory environment variables on a child
/// [`Command`] using the four trusted `THREADLANE_CLI_XDG_*` native-launcher markers,
/// and removes all four marker variables from the child environment.
///
/// For each whitelisted `(marker, xdg_var)` pair:
/// - **Marker absent** (`None`): leaves `xdg_var` unchanged in the child command.
/// - **Marker present and empty**: removes `xdg_var` from the child command so the spawned
///   process falls back to its normal user default.
/// - **Marker present and non-empty**: sets `xdg_var` in the child command to the exact
///   original [`OsString`] value.
/// - Always removes `marker` from the child command environment (even when absent in the
///   parent) for defense-in-depth.
///
/// Never mutates the parent process environment and never modifies `HOME`, `CODEX_HOME`,
/// `PATH`, authentication variables, Git variables, or arbitrary environment keys.
pub fn restore_cli_environment(command: &mut Command) {
    restore_cli_environment_with(command, |key| std::env::var_os(key));
}

fn restore_cli_environment_with<F>(command: &mut Command, mut lookup_marker: F)
where
    F: FnMut(&str) -> Option<OsString>,
{
    for (marker_key, xdg_key) in XDG_MARKER_WHITELIST {
        if let Some(original_value) = lookup_marker(marker_key) {
            if original_value.is_empty() {
                command.env_remove(xdg_key);
            } else {
                command.env(xdg_key, original_value);
            }
        }
        command.env_remove(marker_key);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::{OsStr, OsString};
    use std::process::Command;

    use super::*;

    fn collect_command_envs(command: &Command) -> BTreeMap<OsString, Option<OsString>> {
        command
            .get_envs()
            .map(|(k, v)| (k.to_os_string(), v.map(OsStr::to_os_string)))
            .collect()
    }

    #[test]
    fn restores_nonempty_markers_to_exact_original_xdg_paths_and_removes_markers() {
        let mut markers = BTreeMap::new();
        markers.insert(
            THREADLANE_CLI_XDG_CONFIG_HOME,
            OsString::from("/home/user/.config-original"),
        );
        markers.insert(
            THREADLANE_CLI_XDG_DATA_HOME,
            OsString::from("/home/user/.local/share-original"),
        );
        markers.insert(
            THREADLANE_CLI_XDG_CACHE_HOME,
            OsString::from("/home/user/.cache-original"),
        );
        markers.insert(
            THREADLANE_CLI_XDG_STATE_HOME,
            OsString::from("/home/user/.local/state-original"),
        );

        let mut cmd = Command::new("opencode");
        cmd.env("XDG_CONFIG_HOME", "/preview/isolated/config")
            .env("XDG_DATA_HOME", "/preview/isolated/data");

        restore_cli_environment_with(&mut cmd, |key| markers.get(key).cloned());

        let envs = collect_command_envs(&cmd);
        assert_eq!(
            envs.get(OsStr::new("XDG_CONFIG_HOME")),
            Some(&Some(OsString::from("/home/user/.config-original")))
        );
        assert_eq!(
            envs.get(OsStr::new("XDG_DATA_HOME")),
            Some(&Some(OsString::from("/home/user/.local/share-original")))
        );
        assert_eq!(
            envs.get(OsStr::new("XDG_CACHE_HOME")),
            Some(&Some(OsString::from("/home/user/.cache-original")))
        );
        assert_eq!(
            envs.get(OsStr::new("XDG_STATE_HOME")),
            Some(&Some(OsString::from("/home/user/.local/state-original")))
        );

        for (marker_key, _) in XDG_MARKER_WHITELIST {
            assert_eq!(
                envs.get(OsStr::new(marker_key)),
                Some(&None),
                "marker {marker_key} must be removed from child env"
            );
        }
    }

    #[test]
    fn removes_child_xdg_vars_when_markers_are_present_and_empty() {
        let mut markers = BTreeMap::new();
        for (marker_key, _) in XDG_MARKER_WHITELIST {
            markers.insert(marker_key, OsString::new());
        }

        let mut cmd = Command::new("opencode");
        cmd.env("XDG_CONFIG_HOME", "/preview/isolated/config")
            .env("XDG_DATA_HOME", "/preview/isolated/data")
            .env("XDG_CACHE_HOME", "/preview/isolated/cache")
            .env("XDG_STATE_HOME", "/preview/isolated/state");

        restore_cli_environment_with(&mut cmd, |key| markers.get(key).cloned());

        let envs = collect_command_envs(&cmd);
        for (marker_key, xdg_key) in XDG_MARKER_WHITELIST {
            assert_eq!(
                envs.get(OsStr::new(xdg_key)),
                Some(&None),
                "empty marker must remove {xdg_key} from child env"
            );
            assert_eq!(
                envs.get(OsStr::new(marker_key)),
                Some(&None),
                "marker {marker_key} must be removed from child env"
            );
        }
    }

    #[test]
    fn leaves_child_xdg_settings_unchanged_when_markers_are_absent_and_removes_markers() {
        let mut cmd = Command::new("opencode");
        cmd.env("XDG_CONFIG_HOME", "/explicit/child/config");

        restore_cli_environment_with(&mut cmd, |_| None);

        let envs = collect_command_envs(&cmd);
        assert_eq!(
            envs.get(OsStr::new("XDG_CONFIG_HOME")),
            Some(&Some(OsString::from("/explicit/child/config")))
        );
        assert_eq!(envs.get(OsStr::new("XDG_DATA_HOME")), None);
        assert_eq!(envs.get(OsStr::new("XDG_CACHE_HOME")), None);
        assert_eq!(envs.get(OsStr::new("XDG_STATE_HOME")), None);

        for (marker_key, _) in XDG_MARKER_WHITELIST {
            assert_eq!(
                envs.get(OsStr::new(marker_key)),
                Some(&None),
                "absent marker {marker_key} must still be removed from child env"
            );
        }
    }

    #[test]
    fn preserves_unrelated_child_env_ignores_arbitrary_keys_and_never_mutates_parent_env() {
        let parent_keys = [
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_CACHE_HOME",
            "XDG_STATE_HOME",
            "HOME",
            "PATH",
            THREADLANE_CLI_XDG_CONFIG_HOME,
            THREADLANE_CLI_XDG_DATA_HOME,
            THREADLANE_CLI_XDG_CACHE_HOME,
            THREADLANE_CLI_XDG_STATE_HOME,
        ];
        let parent_before: Vec<(&str, Option<OsString>)> = parent_keys
            .into_iter()
            .map(|k| (k, std::env::var_os(k)))
            .collect();

        let mut markers = BTreeMap::new();
        markers.insert(
            THREADLANE_CLI_XDG_CONFIG_HOME,
            OsString::from("/restored/config"),
        );
        markers.insert(THREADLANE_CLI_XDG_DATA_HOME, OsString::new());
        markers.insert("THREADLANE_CLI_HOME", OsString::from("/untrusted/home"));
        markers.insert("HOME", OsString::from("/untrusted/home"));
        markers.insert("CODEX_HOME", OsString::from("/untrusted/codex"));
        markers.insert("PATH", OsString::from("/untrusted/path"));
        markers.insert("XDG_RUNTIME_DIR", OsString::from("/untrusted/runtime"));

        let mut queried_keys = Vec::new();
        let mut cmd = Command::new("opencode");
        cmd.env("PATH", "/usr/bin:/bin")
            .env("HOME", "/home/trusted")
            .env("CODEX_HOME", "/home/trusted/.codex")
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("OPENAI_API_KEY", "sk-PreserveChildSetting");

        restore_cli_environment_with(&mut cmd, |key| {
            queried_keys.push(key.to_owned());
            markers.get(key).cloned()
        });

        assert_eq!(
            queried_keys,
            vec![
                THREADLANE_CLI_XDG_CONFIG_HOME.to_owned(),
                THREADLANE_CLI_XDG_DATA_HOME.to_owned(),
                THREADLANE_CLI_XDG_CACHE_HOME.to_owned(),
                THREADLANE_CLI_XDG_STATE_HOME.to_owned(),
            ]
        );

        let envs = collect_command_envs(&cmd);
        assert_eq!(
            envs.get(OsStr::new("XDG_CONFIG_HOME")),
            Some(&Some(OsString::from("/restored/config")))
        );
        assert_eq!(envs.get(OsStr::new("XDG_DATA_HOME")), Some(&None));
        assert_eq!(envs.get(OsStr::new("XDG_CACHE_HOME")), None);
        assert_eq!(envs.get(OsStr::new("XDG_STATE_HOME")), None);
        assert_eq!(
            envs.get(OsStr::new("PATH")),
            Some(&Some(OsString::from("/usr/bin:/bin")))
        );
        assert_eq!(
            envs.get(OsStr::new("HOME")),
            Some(&Some(OsString::from("/home/trusted")))
        );
        assert_eq!(
            envs.get(OsStr::new("CODEX_HOME")),
            Some(&Some(OsString::from("/home/trusted/.codex")))
        );
        assert_eq!(
            envs.get(OsStr::new("GIT_TERMINAL_PROMPT")),
            Some(&Some(OsString::from("0")))
        );
        assert_eq!(
            envs.get(OsStr::new("GIT_CONFIG_GLOBAL")),
            Some(&Some(OsString::from("/dev/null")))
        );
        assert_eq!(
            envs.get(OsStr::new("OPENAI_API_KEY")),
            Some(&Some(OsString::from("sk-PreserveChildSetting")))
        );
        assert_eq!(envs.get(OsStr::new("THREADLANE_CLI_HOME")), None);
        assert_eq!(envs.get(OsStr::new("XDG_RUNTIME_DIR")), None);

        let mut public_cmd = Command::new("opencode");
        restore_cli_environment(&mut public_cmd);

        let parent_after: Vec<(&str, Option<OsString>)> = parent_keys
            .into_iter()
            .map(|k| (k, std::env::var_os(k)))
            .collect();
        assert_eq!(parent_before, parent_after);
    }
}
