use std::io::{Read, Seek, SeekFrom};
#[cfg(target_os = "linux")]
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::chat_navigation::verify_canonical_codex_thread_uri;
#[cfg(target_os = "linux")]
use crate::chat_navigation::{ActivationPortOutcome, CodexActivationRequest};
use crate::AppError;

/// Required `schemaVersion` in `owl-shell-runtime.json`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const QUALIFIED_OWL_RUNTIME_SCHEMA_VERSION: u32 = 1;

/// Required `platform` in `owl-shell-runtime.json`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const QUALIFIED_OWL_RUNTIME_PLATFORM: &str = "linux";

/// Required `arch` in `owl-shell-runtime.json`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const QUALIFIED_OWL_RUNTIME_ARCH: &str = "x64";

/// Exact qualified Owl/Chromium shell runtime version (`electron = chrome = 157.0.8103.39`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const QUALIFIED_OWL_RUNTIME_VERSION: &str = "157.0.8103.39";

/// Expected `package.json` `name` inside the installed Codex `app.asar`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const QUALIFIED_CODEX_ASAR_PACKAGE_NAME: &str = "openai-codex-electron";

/// Marker filename inside the immutable Owl shell application runtime directory.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const OWL_RUNTIME_MARKER_FILENAME: &str = "owl-shell-runtime.json";

/// Optional sibling Electron/Owl runtime `version` filename.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const OWL_RUNTIME_VERSION_FILENAME: &str = "version";

/// Leading token of the measured Owl shell `ProcessSingleton` datagram/stream frame.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const OWL_SINGLETON_START_TOKEN: &str = "START";

/// Exact 3-byte acknowledgement payload returned by Owl/Chromium `ProcessSingletonPosix`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const OWL_SINGLETON_ACK_BYTES: &[u8] = b"ACK";

/// Maximum total byte length permitted for an outgoing Owl singleton frame (`4 KiB`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_OWL_SINGLETON_FRAME_BYTES: usize = 4_096;

/// Maximum bytes read from the singleton stream when awaiting `ACK`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_OWL_ACK_READ_BYTES: u64 = 16;

/// Bounded total wall-clock budget covering focus preflight, singleton `START`/`ACK`, focus
/// dispatch, and focus confirmation.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const DEFAULT_LINUX_ACTIVATION_DEADLINE: Duration = Duration::from_millis(3_000);

/// Finite per-step socket connect/write/read timeout cap within [`DEFAULT_LINUX_ACTIVATION_DEADLINE`].
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const DEFAULT_SOCKET_IO_STEP_TIMEOUT: Duration = Duration::from_millis(750);

/// Strict exclusive upper bound (`< 1 MiB`) for any compositor IPC response payload.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES: usize = 1024 * 1024;

/// Maximum byte length allowed for a validated `HYPRLAND_INSTANCE_SIGNATURE` path component.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_HYPRLAND_INSTANCE_SIGNATURE_BYTES: usize = 128;

/// Magic header prefix for Sway / `i3-ipc` framed messages.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const SWAY_IPC_MAGIC: &[u8; 6] = b"i3-ipc";

/// Exact byte length of a Sway / `i3-ipc` frame header (`<6sII`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const SWAY_IPC_HEADER_BYTES: usize = 14;

/// Sway / `i3-ipc` message type `RUN_COMMAND(0)`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const SWAY_IPC_RUN_COMMAND: u32 = 0;

/// Sway / `i3-ipc` message type `GET_TREE(4)`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const SWAY_IPC_GET_TREE: u32 = 4;

/// Maximum container nesting depth traversed in a Sway `GET_TREE` response.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_SWAY_TREE_DEPTH: usize = 64;

/// Maximum total container nodes inspected in a Sway `GET_TREE` response.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_SWAY_TREE_NODES: usize = 4_096;

/// Maximum byte size allowed for `owl-shell-runtime.json` (`64 KiB`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_OWL_MARKER_JSON_BYTES: u64 = 64 * 1024;

/// Maximum byte size allowed for an optional sibling `version` file (`256 B`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_OWL_VERSION_FILE_BYTES: u64 = 256;

/// Maximum byte size allowed for the Chromium Pickle ASAR header (`16 MiB`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_ASAR_HEADER_BYTES: u32 = 16 * 1024 * 1024;

/// Maximum byte size allowed for the root `package.json` inside `app.asar` (`256 KiB`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) const MAX_ASAR_PACKAGE_JSON_BYTES: u64 = 256 * 1024;

#[cfg(target_os = "linux")]
const MAX_PROC_SMALL_BYTES: u64 = 16 * 1024;

#[cfg(target_os = "linux")]
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 100;

#[cfg(target_os = "linux")]
const MAX_COMPOSITOR_SOCKET_PATH_BYTES: usize = 107;

#[cfg(target_os = "linux")]
const SINGLETON_LOCK_FILENAME: &str = "SingletonLock";

#[cfg(target_os = "linux")]
const SINGLETON_COOKIE_FILENAME: &str = "SingletonCookie";

#[cfg(target_os = "linux")]
const SINGLETON_SOCKET_FILENAME: &str = "SingletonSocket";

#[derive(Debug, Deserialize)]
struct OwlRuntimeMarker {
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    platform: String,
    arch: String,
    #[serde(default)]
    version: Option<String>,
}

#[derive(Deserialize)]
struct AsarArchiveHeader {
    files: AsarRootFiles,
}

#[derive(Deserialize)]
struct AsarRootFiles {
    #[serde(rename = "package.json")]
    package_json: Option<AsarFileEntry>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum AsarOffsetValue {
    String(String),
    Integer(u64),
}

#[derive(Deserialize)]
struct AsarFileEntry {
    size: Option<u64>,
    offset: Option<AsarOffsetValue>,
    #[serde(default)]
    unpacked: bool,
    link: Option<serde::de::IgnoredAny>,
    files: Option<serde::de::IgnoredAny>,
}

#[derive(Debug, Deserialize)]
struct AsarRootPackageJson {
    name: String,
}

fn validate_frame_path_argument(raw: &str, field_label: &'static str) -> Result<(), AppError> {
    if raw.is_empty()
        || raw.trim() != raw
        || !raw.starts_with('/')
        || raw.bytes().any(|b| b == 0)
        || raw.chars().any(char::is_control)
        || raw.contains("//")
        || raw == "/."
        || raw == "/.."
        || raw.contains("/./")
        || raw.contains("/../")
        || raw.ends_with("/.")
        || raw.ends_with("/..")
    {
        return Err(AppError::new(
            "codex_activation_invalid_frame",
            format!("Codex activation {field_label} failed canonical path validation."),
        ));
    }
    Ok(())
}

/// Builds the exact Owl shell `ProcessSingleton` wire frame:
/// `b"START\0<cwd>\0<canonicalExe>\0<canonicalUri>"` with no `nargs`/additional-data fields and
/// no trailing NUL byte, bounded to [`MAX_OWL_SINGLETON_FRAME_BYTES`] (`4 KiB`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn build_owl_singleton_frame(
    cwd: &str,
    canonical_exe: &str,
    canonical_uri: &str,
) -> Result<Vec<u8>, AppError> {
    let verified_uri = verify_canonical_codex_thread_uri(canonical_uri)?;
    validate_frame_path_argument(cwd, "working directory")?;
    validate_frame_path_argument(canonical_exe, "executable")?;
    if canonical_exe == "/" {
        return Err(AppError::new(
            "codex_activation_invalid_frame",
            "Codex activation executable failed canonical path validation.",
        ));
    }

    let total_len = OWL_SINGLETON_START_TOKEN
        .len()
        .saturating_add(1)
        .saturating_add(cwd.len())
        .saturating_add(1)
        .saturating_add(canonical_exe.len())
        .saturating_add(1)
        .saturating_add(verified_uri.len());

    if total_len > MAX_OWL_SINGLETON_FRAME_BYTES {
        return Err(AppError::new(
            "codex_activation_frame_too_large",
            "Codex chat activation frame exceeded the 4 KiB transport bound.",
        ));
    }

    let mut frame = Vec::with_capacity(total_len);
    frame.extend_from_slice(OWL_SINGLETON_START_TOKEN.as_bytes());
    frame.push(0);
    frame.extend_from_slice(cwd.as_bytes());
    frame.push(0);
    frame.extend_from_slice(canonical_exe.as_bytes());
    frame.push(0);
    frame.extend_from_slice(verified_uri.as_bytes());

    if frame.len() > MAX_OWL_SINGLETON_FRAME_BYTES
        || frame.last() == Some(&0)
        || frame.iter().filter(|&&b| b == 0).count() != 3
    {
        return Err(AppError::new(
            "codex_activation_invalid_frame",
            "Codex chat activation frame violated the Owl singleton framing invariant.",
        ));
    }

    Ok(frame)
}

/// Validates `owl-shell-runtime.json` bytes (and optional sibling `version` file contents)
/// against the exact qualified Linux x64 Owl shell `157.0.8103.39` runtime identity.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_owl_runtime_marker_bytes(
    marker_bytes: &[u8],
    sibling_version: Option<&str>,
) -> Result<(), AppError> {
    let unsupported = || {
        AppError::new(
            "codex_runtime_unsupported",
            "Installed Codex runtime is not a qualified Owl shell build for existing-instance navigation.",
        )
    };

    if marker_bytes.is_empty() || (marker_bytes.len() as u64) > MAX_OWL_MARKER_JSON_BYTES {
        return Err(unsupported());
    }

    let marker: OwlRuntimeMarker =
        serde_json::from_slice(marker_bytes).map_err(|_| unsupported())?;
    if marker.schema_version != QUALIFIED_OWL_RUNTIME_SCHEMA_VERSION
        || marker.platform != QUALIFIED_OWL_RUNTIME_PLATFORM
        || marker.arch != QUALIFIED_OWL_RUNTIME_ARCH
    {
        return Err(unsupported());
    }

    let mut saw_verified_version = false;
    if let Some(ref json_version) = marker.version {
        if json_version != QUALIFIED_OWL_RUNTIME_VERSION {
            return Err(unsupported());
        }
        saw_verified_version = true;
    }

    if let Some(file_version) = sibling_version {
        let trimmed = file_version.trim();
        if trimmed != QUALIFIED_OWL_RUNTIME_VERSION {
            return Err(unsupported());
        }
        saw_verified_version = true;
    }

    if !saw_verified_version {
        return Err(unsupported());
    }

    Ok(())
}

/// Parses only the bounded Chromium Pickle header and root `package.json` slice from an ASAR
/// archive stream, verifying `"name" == "openai-codex-electron"` without extracting archive contents.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_asar_package_identity_from_reader<R: Read + Seek>(
    reader: &mut R,
    total_len: u64,
) -> Result<(), AppError> {
    let unsupported = || {
        AppError::new(
            "codex_runtime_unsupported",
            "Installed application archive did not match the qualified Codex desktop package.",
        )
    };

    if total_len < 16 {
        return Err(unsupported());
    }

    reader.seek(SeekFrom::Start(0)).map_err(|_| unsupported())?;
    let mut prefix = [0u8; 16];
    reader.read_exact(&mut prefix).map_err(|_| unsupported())?;

    let size_pickle_payload = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    let header_buf_len = u32::from_le_bytes([prefix[4], prefix[5], prefix[6], prefix[7]]);
    let header_pickle_payload = u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]);
    let json_len = u32::from_le_bytes([prefix[12], prefix[13], prefix[14], prefix[15]]);

    if size_pickle_payload != 4
        || !(8..=MAX_ASAR_HEADER_BYTES).contains(&header_buf_len)
        || header_pickle_payload.checked_add(4) != Some(header_buf_len)
        || json_len == 0
        || json_len > header_buf_len - 8
    {
        return Err(unsupported());
    }

    let aligned_json_len = (u64::from(json_len) + 3) & !3;
    if 8u64.checked_add(aligned_json_len) != Some(u64::from(header_buf_len)) {
        return Err(unsupported());
    }

    let data_offset = 8u64
        .checked_add(u64::from(header_buf_len))
        .ok_or_else(unsupported)?;
    if data_offset > total_len {
        return Err(unsupported());
    }

    let json_len_usize = usize::try_from(json_len).map_err(|_| unsupported())?;
    let mut header_json = vec![0u8; json_len_usize];
    reader
        .read_exact(&mut header_json)
        .map_err(|_| unsupported())?;

    let parsed_header: AsarArchiveHeader =
        serde_json::from_slice(&header_json).map_err(|_| unsupported())?;
    let entry = parsed_header.files.package_json.ok_or_else(unsupported)?;

    if entry.unpacked || entry.link.is_some() || entry.files.is_some() {
        return Err(unsupported());
    }

    let pkg_size = entry.size.ok_or_else(unsupported)?;
    if pkg_size == 0 || pkg_size > MAX_ASAR_PACKAGE_JSON_BYTES {
        return Err(unsupported());
    }

    let pkg_offset = match entry.offset.ok_or_else(unsupported)? {
        AsarOffsetValue::String(raw) => {
            if raw.is_empty() || raw.len() > 20 || !raw.bytes().all(|b| b.is_ascii_digit()) {
                return Err(unsupported());
            }
            raw.parse::<u64>().map_err(|_| unsupported())?
        }
        AsarOffsetValue::Integer(val) => val,
    };

    let pkg_start = data_offset
        .checked_add(pkg_offset)
        .ok_or_else(unsupported)?;
    let pkg_end = pkg_start.checked_add(pkg_size).ok_or_else(unsupported)?;
    if pkg_end > total_len {
        return Err(unsupported());
    }

    reader
        .seek(SeekFrom::Start(pkg_start))
        .map_err(|_| unsupported())?;
    let pkg_size_usize = usize::try_from(pkg_size).map_err(|_| unsupported())?;
    let mut pkg_bytes = vec![0u8; pkg_size_usize];
    reader
        .read_exact(&mut pkg_bytes)
        .map_err(|_| unsupported())?;

    let pkg: AsarRootPackageJson = serde_json::from_slice(&pkg_bytes).map_err(|_| unsupported())?;
    if pkg.name != QUALIFIED_CODEX_ASAR_PACKAGE_NAME {
        return Err(unsupported());
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct HyprlandClientEntry {
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    mapped: Option<bool>,
    #[serde(default)]
    hidden: Option<bool>,
    #[serde(default)]
    class: Option<String>,
    #[serde(default)]
    pid: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SwayWindowProperties {
    #[serde(default)]
    class: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SwayTreeNode {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    pid: Option<i64>,
    #[serde(default)]
    focused: Option<bool>,
    #[serde(default)]
    app_id: Option<String>,
    #[serde(default)]
    window_properties: Option<SwayWindowProperties>,
    #[serde(default)]
    nodes: Vec<SwayTreeNode>,
    #[serde(default)]
    floating_nodes: Vec<SwayTreeNode>,
}

#[derive(Debug, Deserialize)]
struct SwayCommandOutcome {
    #[serde(default)]
    success: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct SwayWindowCandidate {
    pub(crate) con_id: u64,
    pub(crate) focused: bool,
}

fn focus_unavailable_protocol_error() -> AppError {
    AppError::new(
        "codex_focus_unavailable",
        "Verified Codex desktop window could not be prepared for focus in the active compositor.",
    )
}

fn window_ambiguous_protocol_error() -> AppError {
    AppError::new(
        "codex_window_ambiguous",
        "Multiple eligible Codex desktop windows were detected; refusing to guess which window to focus.",
    )
}

/// Validates a `HYPRLAND_INSTANCE_SIGNATURE` single directory component (`1..=128` safe ASCII bytes).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn validate_hyprland_instance_signature(raw: &str) -> Result<String, AppError> {
    if raw.is_empty()
        || raw.len() > MAX_HYPRLAND_INSTANCE_SIGNATURE_BYTES
        || raw.trim() != raw
        || raw == "."
        || raw == ".."
        || raw.contains("..")
        || !raw.bytes().all(|b| {
            matches!(
                b,
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b':' | b'+' | b'-'
            )
        })
    {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(raw.to_string())
}

/// Validates a Hyprland window address as strict lowercase non-zero hex (`0x[0-9a-f]{1,16}`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn validate_hyprland_hex_address(raw: &str) -> Result<String, AppError> {
    let Some(hex_digits) = raw.strip_prefix("0x") else {
        return Err(focus_unavailable_protocol_error());
    };
    if hex_digits.is_empty()
        || hex_digits.len() > 16
        || !hex_digits
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        || hex_digits.bytes().all(|b| b == b'0')
    {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(raw.to_string())
}

/// Parses Hyprland `j/clients` JSON (`< 1 MiB`), requiring exactly one mapped, non-hidden window
/// belonging to `target_pid` with a non-empty class and strict lowercase hex address.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn parse_hyprland_clients_for_pid(
    json_bytes: &[u8],
    target_pid: u32,
) -> Result<String, AppError> {
    if json_bytes.is_empty() || json_bytes.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let clients: Vec<HyprlandClientEntry> =
        serde_json::from_slice(json_bytes).map_err(|_| focus_unavailable_protocol_error())?;

    let expected_pid = i64::from(target_pid);
    let eligible: Vec<&HyprlandClientEntry> = clients
        .iter()
        .filter(|entry| {
            entry.pid == Some(expected_pid)
                && entry.mapped == Some(true)
                && entry.hidden == Some(false)
        })
        .collect();

    if eligible.is_empty() {
        return Err(focus_unavailable_protocol_error());
    }
    if eligible.len() > 1 {
        return Err(window_ambiguous_protocol_error());
    }

    let winner = eligible[0];
    let has_valid_class = winner
        .class
        .as_deref()
        .is_some_and(|cls| !cls.trim().is_empty() && !cls.chars().any(char::is_control));
    if !has_valid_class {
        return Err(focus_unavailable_protocol_error());
    }

    let raw_addr = winner
        .address
        .as_deref()
        .ok_or_else(focus_unavailable_protocol_error)?;
    validate_hyprland_hex_address(raw_addr)
}

/// Verifies a Hyprland `dispatch focuswindow address:<hex>` reply (`"ok"`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_hyprland_dispatch_ok(reply_bytes: &[u8]) -> Result<(), AppError> {
    if reply_bytes.is_empty() || reply_bytes.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let reply = std::str::from_utf8(reply_bytes).map_err(|_| focus_unavailable_protocol_error())?;
    if reply.trim() != "ok" {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(())
}

/// Parses Hyprland `j/activewindow` JSON (`< 1 MiB`) and confirms the active window matches
/// both `target_pid` and `expected_hex_address` while mapped and non-hidden.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_hyprland_active_window(
    json_bytes: &[u8],
    target_pid: u32,
    expected_hex_address: &str,
) -> Result<(), AppError> {
    let expected_addr = validate_hyprland_hex_address(expected_hex_address)?;
    if json_bytes.is_empty() || json_bytes.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let active: HyprlandClientEntry =
        serde_json::from_slice(json_bytes).map_err(|_| focus_unavailable_protocol_error())?;

    if active.pid != Some(i64::from(target_pid))
        || active.mapped != Some(true)
        || active.hidden != Some(false)
    {
        return Err(focus_unavailable_protocol_error());
    }

    let has_valid_class = active
        .class
        .as_deref()
        .is_some_and(|cls| !cls.trim().is_empty() && !cls.chars().any(char::is_control));
    if !has_valid_class {
        return Err(focus_unavailable_protocol_error());
    }

    let raw_addr = active
        .address
        .as_deref()
        .ok_or_else(focus_unavailable_protocol_error)?;
    let validated_active_addr = validate_hyprland_hex_address(raw_addr)?;
    if validated_active_addr != expected_addr {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(())
}

/// Encodes a bounded Sway / `i3-ipc` request frame (`b"i3-ipc" || u32_le(len) || u32_le(type) || payload`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn encode_sway_ipc_frame(
    message_type: u32,
    payload: &[u8],
) -> Result<Vec<u8>, AppError> {
    if payload.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| focus_unavailable_protocol_error())?;
    let mut frame = Vec::with_capacity(SWAY_IPC_HEADER_BYTES.saturating_add(payload.len()));
    frame.extend_from_slice(SWAY_IPC_MAGIC);
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&message_type.to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Decodes and validates a 14-byte Sway / `i3-ipc` response header (`< 1 MiB` payload ceiling).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn decode_sway_ipc_header(
    header: &[u8; SWAY_IPC_HEADER_BYTES],
    expected_type: u32,
) -> Result<usize, AppError> {
    if &header[0..6] != SWAY_IPC_MAGIC {
        return Err(focus_unavailable_protocol_error());
    }
    let payload_len_u32 = u32::from_le_bytes([header[6], header[7], header[8], header[9]]);
    let message_type = u32::from_le_bytes([header[10], header[11], header[12], header[13]]);
    let payload_len =
        usize::try_from(payload_len_u32).map_err(|_| focus_unavailable_protocol_error())?;
    if message_type != expected_type || payload_len >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(payload_len)
}

/// Parses Sway `GET_TREE(4)` JSON (`< 1 MiB`), requiring exactly one client view container for
/// `target_pid` with positive `con_id` and non-empty `app_id` (or XWayland `window_properties.class`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn parse_sway_tree_for_pid(
    json_bytes: &[u8],
    target_pid: u32,
) -> Result<SwayWindowCandidate, AppError> {
    if json_bytes.is_empty() || json_bytes.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let root: SwayTreeNode =
        serde_json::from_slice(json_bytes).map_err(|_| focus_unavailable_protocol_error())?;

    let expected_pid = i64::from(target_pid);
    let mut stack: Vec<(&SwayTreeNode, usize)> = vec![(&root, 0)];
    let mut visited = 0usize;
    let mut matches: Vec<&SwayTreeNode> = Vec::new();

    while let Some((node, depth)) = stack.pop() {
        if depth > MAX_SWAY_TREE_DEPTH {
            return Err(focus_unavailable_protocol_error());
        }
        visited = visited.saturating_add(1);
        if visited > MAX_SWAY_TREE_NODES {
            return Err(focus_unavailable_protocol_error());
        }
        if node.pid == Some(expected_pid) {
            matches.push(node);
        }
        for child in node.nodes.iter().chain(node.floating_nodes.iter()) {
            stack.push((child, depth + 1));
        }
    }

    if matches.is_empty() {
        return Err(focus_unavailable_protocol_error());
    }
    if matches.len() > 1 {
        return Err(window_ambiguous_protocol_error());
    }

    let winner = matches[0];
    let raw_id = winner.id.ok_or_else(focus_unavailable_protocol_error)?;
    let con_id = u64::try_from(raw_id).map_err(|_| focus_unavailable_protocol_error())?;
    if con_id == 0 {
        return Err(focus_unavailable_protocol_error());
    }

    let has_valid_app_id = winner
        .app_id
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty() && !s.chars().any(char::is_control));
    let has_valid_xwayland_class = winner
        .window_properties
        .as_ref()
        .and_then(|wp| wp.class.as_deref())
        .is_some_and(|s| !s.trim().is_empty() && !s.chars().any(char::is_control));
    if !has_valid_app_id && !has_valid_xwayland_class {
        return Err(focus_unavailable_protocol_error());
    }

    Ok(SwayWindowCandidate {
        con_id,
        focused: winner.focused.unwrap_or(false),
    })
}

/// Verifies a Sway `RUN_COMMAND(0)` JSON reply (`[{"success": true, ...}]`).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_sway_run_command_ok(json_bytes: &[u8]) -> Result<(), AppError> {
    if json_bytes.is_empty() || json_bytes.len() >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
        return Err(focus_unavailable_protocol_error());
    }
    let outcomes: Vec<SwayCommandOutcome> =
        serde_json::from_slice(json_bytes).map_err(|_| focus_unavailable_protocol_error())?;
    if outcomes.is_empty() || !outcomes.iter().all(|item| item.success == Some(true)) {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(())
}

/// Confirms via Sway `GET_TREE(4)` that the unique window for `target_pid` still has
/// `id == expected_con_id` and `focused == true`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) fn verify_sway_focused_window(
    json_bytes: &[u8],
    target_pid: u32,
    expected_con_id: u64,
) -> Result<(), AppError> {
    if expected_con_id == 0 {
        return Err(focus_unavailable_protocol_error());
    }
    let candidate = parse_sway_tree_for_pid(json_bytes, target_pid)?;
    if candidate.con_id != expected_con_id || !candidate.focused {
        return Err(focus_unavailable_protocol_error());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
mod linux_imp {
    use super::*;
    use std::ffi::OsString;
    use std::fs::{self, File};
    use std::hint::black_box;
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum RuntimeOwnershipPolicy {
        /// Production requirement: non-root desktop user (`expected_uid != 0`) and root-owned
        /// (`uid == 0`), non-writable application runtime and ASAR files.
        RootImmutable,
        #[cfg(test)]
        /// Test-only policy enabling disposable non-root `TempDir` runtime fixtures while still
        /// enforcing read-only (`0555`/`0444`) file/directory mode bits.
        TestOwner {
            owner_uid: u32,
            require_read_only: bool,
        },
    }

    #[derive(Debug, Clone)]
    pub(crate) enum LinuxProfileSource {
        /// Derives the profile strictly from trusted process `XDG_CONFIG_HOME` / `HOME`.
        NativeEnv,
        #[cfg(test)]
        /// Test-only isolated environment without mutating process-global environment variables.
        ExplicitEnv {
            xdg_config_home: Option<OsString>,
            home: Option<OsString>,
            extra_candidates: Vec<PathBuf>,
        },
    }

    #[derive(Debug, Clone)]
    pub(crate) enum LinuxCompositorSource {
        /// Discovers Hyprland (`XDG_RUNTIME_DIR` + `HYPRLAND_INSTANCE_SIGNATURE`) or Sway
        /// (`SWAYSOCK`) strictly from the trusted process environment.
        NativeEnv,
        #[cfg(test)]
        /// Test-only isolated compositor environment without mutating process-global env vars.
        ExplicitEnv {
            xdg_runtime_dir: Option<OsString>,
            hyprland_instance_signature: Option<OsString>,
            swaysock: Option<OsString>,
            compositor_proc_root: Option<PathBuf>,
            simulate_exe_permission_denied: bool,
            allow_same_pid_as_target: bool,
        },
    }

    #[cfg(test)]
    pub(crate) type PostConnectHook = std::sync::Arc<dyn Fn() + Send + Sync>;

    #[derive(Clone)]
    pub(crate) struct LinuxOwlActivatorConfig {
        pub(crate) deadline: Duration,
        pub(crate) step_timeout: Duration,
        pub(crate) proc_root: PathBuf,
        pub(crate) profile_source: LinuxProfileSource,
        pub(crate) compositor_source: LinuxCompositorSource,
        pub(crate) ownership_policy: RuntimeOwnershipPolicy,
        #[cfg(test)]
        pub(crate) post_connect_hook: Option<PostConnectHook>,
    }

    impl Default for LinuxOwlActivatorConfig {
        fn default() -> Self {
            Self {
                deadline: DEFAULT_LINUX_ACTIVATION_DEADLINE,
                step_timeout: DEFAULT_SOCKET_IO_STEP_TIMEOUT,
                proc_root: PathBuf::from("/proc"),
                profile_source: LinuxProfileSource::NativeEnv,
                compositor_source: LinuxCompositorSource::NativeEnv,
                ownership_policy: RuntimeOwnershipPolicy::RootImmutable,
                #[cfg(test)]
                post_connect_hook: None,
            }
        }
    }

    /// Qualified Linux Owl-shell existing-instance Codex chat activator.
    ///
    /// Preflights a unique eligible window in the supported existing Wayland compositor
    /// (`Hyprland` or `Sway`), sends the exact Owl `START` frame to the verified same-UID Codex
    /// singleton socket, awaits `ACK`, dispatches a single bounded compositor window-focus command
    /// for that exact PID/window, and confirms active focus. Never launches a process, kills an
    /// unresponsive process, overrides compositor focus policy, falls back to OS URL openers, or
    /// mutates desktop/user configuration.
    #[derive(Clone, Default)]
    pub(crate) struct LinuxOwlCodexActivator {
        config: LinuxOwlActivatorConfig,
    }

    impl std::fmt::Debug for LinuxOwlCodexActivator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LinuxOwlCodexActivator")
                .field("deadline", &self.config.deadline)
                .field("step_timeout", &self.config.step_timeout)
                .field("ownership_policy", &self.config.ownership_policy)
                .finish()
        }
    }

    impl LinuxOwlCodexActivator {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        #[cfg(test)]
        pub(crate) fn with_config(config: LinuxOwlActivatorConfig) -> Self {
            Self { config }
        }

        pub(crate) async fn activate(
            &self,
            request: &CodexActivationRequest,
        ) -> ActivationPortOutcome {
            let started = Instant::now();
            let deadline_at = started + self.config.deadline;

            if request.chat_identity.validate().is_err() {
                return ActivationPortOutcome::RejectedBeforeSend(AppError::new(
                    "target_scope_unverified",
                    "Target chat identity failed canonical scope verification.",
                ));
            }
            let verified_uri = match verify_canonical_codex_thread_uri(&request.canonical_uri) {
                Ok(uri) => uri,
                Err(err) => return ActivationPortOutcome::RejectedBeforeSend(err),
            };

            let config_for_precheck = self.config.clone();
            let remaining_precheck = deadline_at.saturating_duration_since(Instant::now());
            if remaining_precheck.is_zero() {
                return ActivationPortOutcome::RejectedBeforeSend(unavailable_timeout_pre_send());
            }

            let precheck_join = tokio::time::timeout(
                remaining_precheck,
                tokio::task::spawn_blocking(move || {
                    inspect_pre_connect_state(&config_for_precheck)
                }),
            )
            .await;

            let pre_state = match precheck_join {
                Ok(Ok(Ok(state))) => state,
                Ok(Ok(Err(err))) => return ActivationPortOutcome::RejectedBeforeSend(err),
                Ok(Err(_)) => {
                    return ActivationPortOutcome::RejectedBeforeSend(
                        AppError::internal_join_error(),
                    );
                }
                Err(_) => {
                    return ActivationPortOutcome::RejectedBeforeSend(
                        unavailable_timeout_pre_send(),
                    );
                }
            };

            let frame = match build_owl_singleton_frame(
                &request.chat_identity.canonical_project_scope,
                &pre_state.canonical_exe_utf8,
                &verified_uri,
            ) {
                Ok(bytes) => bytes,
                Err(err) => return ActivationPortOutcome::RejectedBeforeSend(err),
            };

            // Run read-only compositor focus preflight and eligible-window ambiguity detection
            // strictly BEFORE connecting or sending any URI bytes to `SingletonSocket`.
            let focus_plan =
                match preflight_compositor_focus(&self.config, &pre_state, deadline_at).await {
                    Ok(plan) => plan,
                    Err(err) => return ActivationPortOutcome::RejectedBeforeSend(err),
                };

            if let Err(err) = verify_live_codex_process_unchanged(&self.config, &pre_state) {
                return ActivationPortOutcome::RejectedBeforeSend(err);
            }

            let Some(connect_budget) = step_budget(deadline_at, self.config.step_timeout) else {
                return ActivationPortOutcome::RejectedBeforeSend(unavailable_timeout_pre_send());
            };

            let mut stream = match tokio::time::timeout(
                connect_budget,
                UnixStream::connect(&pre_state.socket_path),
            )
            .await
            {
                Ok(Ok(s)) => s,
                Ok(Err(_)) => {
                    return ActivationPortOutcome::RejectedBeforeSend(app_unavailable_error());
                }
                Err(_) => {
                    return ActivationPortOutcome::RejectedBeforeSend(
                        unavailable_timeout_pre_send(),
                    );
                }
            };

            #[cfg(test)]
            if let Some(ref hook) = self.config.post_connect_hook {
                hook();
            }

            // Re-verify peer credentials, singleton lock/cookies, socket inode, and live process
            // start identity AFTER connect and BEFORE writing a single byte.
            if let Err(err) =
                verify_connected_peer_and_singleton_state(&stream, &self.config, &pre_state)
            {
                drop(stream);
                return ActivationPortOutcome::RejectedBeforeSend(err);
            }

            let Some(write_budget) = step_budget(deadline_at, self.config.step_timeout) else {
                drop(stream);
                return ActivationPortOutcome::RejectedBeforeSend(unavailable_timeout_pre_send());
            };

            // Point of possible effect: once `write_all` begins (or any subsequent focus bytes may
            // be sent), EVERY failure, timeout, truncation, error reply, or active-window mismatch
            // MUST preserve the uncertain latch (`UnknownAfterPossibleSend`) with no retry.
            match tokio::time::timeout(write_budget, stream.write_all(&frame)).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    return ActivationPortOutcome::UnknownAfterPossibleSend(
                        unknown_submission_error(),
                    );
                }
            }

            let Some(shutdown_budget) = step_budget(deadline_at, self.config.step_timeout) else {
                return ActivationPortOutcome::UnknownAfterPossibleSend(unknown_submission_error());
            };
            match tokio::time::timeout(shutdown_budget, stream.shutdown()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    return ActivationPortOutcome::UnknownAfterPossibleSend(
                        unknown_submission_error(),
                    );
                }
            }

            let ack_buf = match read_owl_ack_bounded(
                &mut stream,
                deadline_at,
                self.config.step_timeout,
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(()) => {
                    return ActivationPortOutcome::UnknownAfterPossibleSend(
                        unknown_submission_error(),
                    );
                }
            };
            drop(stream);

            if ack_buf.as_slice() != OWL_SINGLETON_ACK_BYTES {
                return ActivationPortOutcome::UnknownAfterPossibleSend(unknown_submission_error());
            }

            if verify_live_codex_process_unchanged(&self.config, &pre_state).is_err() {
                return ActivationPortOutcome::UnknownAfterPossibleSend(unknown_submission_error());
            }

            if dispatch_and_confirm_compositor_focus(
                &self.config,
                &pre_state,
                &focus_plan,
                deadline_at,
            )
            .await
            .is_err()
            {
                return ActivationPortOutcome::UnknownAfterPossibleSend(unknown_submission_error());
            }

            if verify_live_codex_process_unchanged(&self.config, &pre_state).is_err() {
                return ActivationPortOutcome::UnknownAfterPossibleSend(unknown_submission_error());
            }

            ActivationPortOutcome::Submitted
        }
    }

    #[derive(Debug, Clone)]
    enum QualifiedCompositorEndpoint {
        Hyprland {
            xdg_runtime_dir: PathBuf,
            xdg_runtime_dev: u64,
            xdg_runtime_ino: u64,
            hypr_dir: PathBuf,
            hypr_dev: u64,
            hypr_ino: u64,
            instance_dir: PathBuf,
            instance_dev: u64,
            instance_ino: u64,
            socket_path: PathBuf,
            socket_dev: u64,
            socket_ino: u64,
        },
        Sway {
            private_root_dir: PathBuf,
            private_root_dev: u64,
            private_root_ino: u64,
            parent_dir: PathBuf,
            parent_dev: u64,
            parent_ino: u64,
            socket_path: PathBuf,
            socket_dev: u64,
            socket_ino: u64,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct VerifiedCompositorPeer {
        pid: u32,
        boot_id: String,
        start_ticks: u64,
        comm: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum CompositorFocusTarget {
        Hyprland { hex_address: String },
        Sway { con_id: u64 },
    }

    #[derive(Debug, Clone)]
    struct PreflightFocusPlan {
        peer: VerifiedCompositorPeer,
        target: CompositorFocusTarget,
    }

    #[derive(Clone)]
    struct PreConnectVerifiedState {
        expected_uid: u32,
        profile_dir: PathBuf,
        profile_dev: u64,
        profile_ino: u64,
        lock_target: String,
        expected_pid: u32,
        cookie: String,
        socket_dir: PathBuf,
        socket_dir_dev: u64,
        socket_dir_ino: u64,
        socket_path: PathBuf,
        socket_dev: u64,
        socket_ino: u64,
        boot_id: String,
        start_ticks: u64,
        canonical_exe: PathBuf,
        canonical_exe_utf8: String,
        exe_dev: u64,
        exe_ino: u64,
        compositor: QualifiedCompositorEndpoint,
    }

    fn app_unavailable_error() -> AppError {
        AppError::new(
            "codex_app_unavailable",
            "No verified running Codex desktop instance is available for chat activation.",
        )
    }

    fn unavailable_timeout_pre_send() -> AppError {
        AppError::new(
            "codex_app_unavailable",
            "Timed out before connecting to a verified running Codex desktop instance.",
        )
    }

    fn runtime_unsupported_error() -> AppError {
        AppError::new(
            "codex_runtime_unsupported",
            "Running Codex instance is not using the qualified immutable Owl shell runtime.",
        )
    }

    fn navigation_unsupported_error() -> AppError {
        AppError::new(
            "codex_navigation_unsupported",
            "Existing-instance Codex window focus is unsupported in the current desktop session.",
        )
    }

    fn profile_ambiguous_error() -> AppError {
        AppError::new(
            "codex_profile_ambiguous",
            "Multiple candidate Codex desktop profiles were detected; refusing to guess which instance to activate.",
        )
    }

    fn focus_unavailable_error() -> AppError {
        focus_unavailable_protocol_error()
    }

    fn unknown_submission_error() -> AppError {
        AppError::new(
            "codex_activation_unknown",
            "Codex chat activation outcome is uncertain after transport or focus submission; automatic retry is disabled.",
        )
    }

    fn step_budget(deadline_at: Instant, step_timeout: Duration) -> Option<Duration> {
        let remaining = deadline_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() || step_timeout.is_zero() {
            None
        } else {
            Some(remaining.min(step_timeout))
        }
    }

    async fn read_owl_ack_bounded(
        stream: &mut UnixStream,
        deadline_at: Instant,
        step_timeout: Duration,
    ) -> Result<Vec<u8>, ()> {
        let max_bytes = usize::try_from(MAX_OWL_ACK_READ_BYTES).unwrap_or(16);
        let mut ack_buf = Vec::with_capacity(8);
        let mut chunk = [0u8; 17];
        loop {
            let budget = step_budget(deadline_at, step_timeout).ok_or(())?;
            let n = match tokio::time::timeout(budget, stream.read(&mut chunk)).await {
                Ok(Ok(n)) => n,
                Ok(Err(_)) | Err(_) => return Err(()),
            };
            if n == 0 {
                return Ok(ack_buf);
            }
            if ack_buf.len().saturating_add(n) > max_bytes {
                return Err(());
            }
            ack_buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn constant_time_bytes_eq(left: &[u8], right: &[u8]) -> bool {
        let mut diff = left.len() ^ right.len();
        let max_len = left.len().max(right.len());
        for idx in 0..max_len {
            let l = left.get(idx).copied().unwrap_or(0);
            let r = right.get(idx).copied().unwrap_or(0);
            diff |= usize::from(l ^ r);
        }
        black_box(diff) == 0
    }

    fn current_process_uid() -> Result<u32, AppError> {
        let meta = fs::metadata("/proc/self").map_err(|_| app_unavailable_error())?;
        Ok(meta.uid())
    }

    fn validate_clean_absolute_path(path: &Path) -> Result<(), AppError> {
        let Some(utf8) = path.to_str() else {
            return Err(app_unavailable_error());
        };
        if utf8.is_empty()
            || utf8.trim() != utf8
            || !path.is_absolute()
            || utf8.bytes().any(|b| b == 0)
            || utf8.chars().any(char::is_control)
            || utf8.contains("//")
        {
            return Err(app_unavailable_error());
        }

        let mut saw_normal = false;
        for comp in path.components() {
            match comp {
                Component::RootDir => {}
                Component::Normal(part) => {
                    if part.is_empty() {
                        return Err(app_unavailable_error());
                    }
                    saw_normal = true;
                }
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(app_unavailable_error());
                }
            }
        }

        if !saw_normal {
            return Err(app_unavailable_error());
        }

        Ok(())
    }

    fn verify_no_symlink_components(path: &Path) -> Result<(), AppError> {
        validate_clean_absolute_path(path)?;
        let mut cursor = PathBuf::new();
        for comp in path.components() {
            match comp {
                Component::RootDir => cursor.push("/"),
                Component::Normal(part) => {
                    cursor.push(part);
                    let meta =
                        fs::symlink_metadata(&cursor).map_err(|_| app_unavailable_error())?;
                    if meta.file_type().is_symlink() {
                        return Err(app_unavailable_error());
                    }
                }
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(app_unavailable_error());
                }
            }
        }
        Ok(())
    }

    fn parse_required_clean_env_dir_with(
        raw: OsString,
        err_fn: fn() -> AppError,
    ) -> Result<PathBuf, AppError> {
        if raw.is_empty() {
            return Err(err_fn());
        }
        let s = raw.into_string().map_err(|_| err_fn())?;
        if s.trim().is_empty() || s.trim() != s {
            return Err(err_fn());
        }
        let path = PathBuf::from(s);
        validate_clean_absolute_path(&path).map_err(|_| err_fn())?;
        Ok(path)
    }

    fn verify_no_symlink_components_with(
        path: &Path,
        err_fn: fn() -> AppError,
    ) -> Result<(), AppError> {
        validate_clean_absolute_path(path).map_err(|_| err_fn())?;
        let mut cursor = PathBuf::new();
        for comp in path.components() {
            match comp {
                Component::RootDir => cursor.push("/"),
                Component::Normal(part) => {
                    cursor.push(part);
                    let meta = fs::symlink_metadata(&cursor).map_err(|_| err_fn())?;
                    if meta.file_type().is_symlink() {
                        return Err(err_fn());
                    }
                }
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(err_fn());
                }
            }
        }
        Ok(())
    }

    fn discover_codex_profile_dir(
        source: &LinuxProfileSource,
        expected_uid: u32,
    ) -> Result<(PathBuf, u64, u64), AppError> {
        let (xdg_raw, home_raw, extra_candidates) = match source {
            LinuxProfileSource::NativeEnv => (
                std::env::var_os("XDG_CONFIG_HOME"),
                std::env::var_os("HOME"),
                Vec::<PathBuf>::new(),
            ),
            #[cfg(test)]
            LinuxProfileSource::ExplicitEnv {
                xdg_config_home,
                home,
                extra_candidates,
            } => (
                xdg_config_home.clone(),
                home.clone(),
                extra_candidates.clone(),
            ),
        };

        let selected_profile = match xdg_raw {
            Some(explicit_xdg) => {
                // Explicit trusted `XDG_CONFIG_HOME` defines the authoritative config root.
                // A stale `$HOME/.config/Codex` shadow directory is ignored, and an invalid,
                // empty, missing, or stale explicit `XDG_CONFIG_HOME` fails closed without
                // falling back to `HOME`.
                let xdg_dir =
                    parse_required_clean_env_dir_with(explicit_xdg, app_unavailable_error)?;
                xdg_dir.join("Codex")
            }
            None => {
                let home_os = home_raw.ok_or_else(app_unavailable_error)?;
                let home_dir = parse_required_clean_env_dir_with(home_os, app_unavailable_error)?;
                home_dir.join(".config").join("Codex")
            }
        };

        if !extra_candidates.is_empty() {
            let mut distinct_existing = std::collections::BTreeSet::new();
            if let Ok(m) = fs::symlink_metadata(&selected_profile) {
                distinct_existing.insert((m.dev(), m.ino()));
            }
            for extra in &extra_candidates {
                if let Ok(m) = fs::symlink_metadata(extra) {
                    distinct_existing.insert((m.dev(), m.ino()));
                }
            }
            if distinct_existing.len() > 1 {
                return Err(profile_ambiguous_error());
            }
        }

        verify_no_symlink_components(&selected_profile)?;
        let meta = fs::symlink_metadata(&selected_profile).map_err(|_| app_unavailable_error())?;
        if meta.file_type().is_symlink()
            || !meta.file_type().is_dir()
            || meta.uid() != expected_uid
            || (meta.permissions().mode() & 0o022) != 0
        {
            return Err(app_unavailable_error());
        }

        Ok((selected_profile, meta.dev(), meta.ino()))
    }

    fn read_owned_symlink_utf8(path: &Path, expected_uid: u32) -> Result<String, AppError> {
        let meta = fs::symlink_metadata(path).map_err(|_| app_unavailable_error())?;
        if !meta.file_type().is_symlink() || meta.uid() != expected_uid {
            return Err(app_unavailable_error());
        }
        let target = fs::read_link(path).map_err(|_| app_unavailable_error())?;
        let s = target
            .into_os_string()
            .into_string()
            .map_err(|_| app_unavailable_error())?;
        if s.is_empty() || s.bytes().any(|b| b == 0) || s.chars().any(char::is_control) {
            return Err(app_unavailable_error());
        }
        Ok(s)
    }

    fn parse_singleton_lock_target(raw: &str) -> Result<u32, AppError> {
        let Some((hostname, pid_str)) = raw.rsplit_once('-') else {
            return Err(app_unavailable_error());
        };
        if hostname.is_empty()
            || hostname.len() > 253
            || hostname.contains('/')
            || !hostname.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(app_unavailable_error());
        }
        if pid_str.is_empty()
            || pid_str.len() > 10
            || pid_str.starts_with('0')
            || !pid_str.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(app_unavailable_error());
        }
        let pid: u32 = pid_str.parse().map_err(|_| app_unavailable_error())?;
        if pid == 0 || i32::try_from(pid).is_err() {
            return Err(app_unavailable_error());
        }
        Ok(pid)
    }

    fn validate_cookie_syntax(cookie: &str) -> Result<(), AppError> {
        if cookie.is_empty()
            || cookie.len() > 128
            || cookie.contains('/')
            || !cookie.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(app_unavailable_error());
        }
        Ok(())
    }

    fn read_bounded_proc_string(path: &Path, max_bytes: u64) -> Result<String, AppError> {
        let bytes = read_bounded_proc_bytes(path, max_bytes)?;
        String::from_utf8(bytes).map_err(|_| app_unavailable_error())
    }

    fn read_bounded_proc_bytes(path: &Path, max_bytes: u64) -> Result<Vec<u8>, AppError> {
        let file = File::open(path).map_err(|_| app_unavailable_error())?;
        let mut buf = Vec::new();
        file.take(max_bytes + 1)
            .read_to_end(&mut buf)
            .map_err(|_| app_unavailable_error())?;
        if (buf.len() as u64) > max_bytes {
            return Err(app_unavailable_error());
        }
        Ok(buf)
    }

    fn read_live_proc_identity(
        proc_root: &Path,
        pid: u32,
        expected_uid: u32,
    ) -> Result<(String, u64, PathBuf, String), AppError> {
        let boot_id = read_bounded_proc_string(
            &proc_root.join("sys/kernel/random/boot_id"),
            MAX_OWL_VERSION_FILE_BYTES,
        )?;
        let boot_id = boot_id.trim().to_string();
        if boot_id.is_empty()
            || boot_id.len() > 128
            || !boot_id.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(app_unavailable_error());
        }

        let proc_pid_dir = proc_root.join(pid.to_string());
        let pid_meta = fs::symlink_metadata(&proc_pid_dir).map_err(|_| app_unavailable_error())?;
        if pid_meta.uid() != expected_uid {
            return Err(app_unavailable_error());
        }

        let stat_content =
            read_bounded_proc_string(&proc_pid_dir.join("stat"), MAX_PROC_SMALL_BYTES)?;
        let start_ticks = parse_proc_stat_start_ticks(&stat_content, pid)?;

        let exe_link =
            fs::read_link(proc_pid_dir.join("exe")).map_err(|_| app_unavailable_error())?;
        let exe_utf8 = exe_link
            .clone()
            .into_os_string()
            .into_string()
            .map_err(|_| app_unavailable_error())?;
        if exe_utf8.ends_with(" (deleted)") {
            return Err(app_unavailable_error());
        }
        validate_clean_absolute_path(&exe_link)?;

        Ok((boot_id, start_ticks, exe_link, exe_utf8))
    }

    fn parse_proc_stat_comm_and_start_ticks(
        content: &str,
        expected_pid: u32,
        err_fn: fn() -> AppError,
    ) -> Result<(String, u64), AppError> {
        let Some(open_paren) = content.find('(') else {
            return Err(err_fn());
        };
        let Some(close_paren) = content.rfind(')') else {
            return Err(err_fn());
        };
        if close_paren <= open_paren + 1 {
            return Err(err_fn());
        }

        let parsed_pid: u32 = content[..open_paren].trim().parse().map_err(|_| err_fn())?;
        if parsed_pid != expected_pid {
            return Err(err_fn());
        }

        let comm = content[open_paren + 1..close_paren].to_string();
        if comm.is_empty() || comm.chars().any(char::is_control) {
            return Err(err_fn());
        }

        let rest = content[close_paren + 1..].trim_start();
        let mut fields = rest.split_ascii_whitespace();
        let Some(state_str) = fields.next() else {
            return Err(err_fn());
        };
        if state_str.len() != 1 || state_str.starts_with(['Z', 'X', 'x']) {
            return Err(err_fn());
        }

        // Field 3 is `state` (just consumed); Field 22 is `starttime` (18 fields after `state`).
        let start_ticks_str = fields.nth(18).ok_or_else(err_fn)?;
        let start_ticks: u64 = start_ticks_str.parse().map_err(|_| err_fn())?;
        if start_ticks == 0 {
            return Err(err_fn());
        }

        Ok((comm, start_ticks))
    }

    fn parse_proc_stat_start_ticks(content: &str, expected_pid: u32) -> Result<u64, AppError> {
        let (_comm, start_ticks) =
            parse_proc_stat_comm_and_start_ticks(content, expected_pid, app_unavailable_error)?;
        Ok(start_ticks)
    }

    fn verify_runtime_dir_chain(
        dir: &Path,
        expected_user_uid: u32,
        policy: RuntimeOwnershipPolicy,
    ) -> Result<(), AppError> {
        validate_clean_absolute_path(dir).map_err(|_| runtime_unsupported_error())?;

        let required_owner_uid = match policy {
            RuntimeOwnershipPolicy::RootImmutable => {
                if expected_user_uid == 0 {
                    return Err(runtime_unsupported_error());
                }
                0
            }
            #[cfg(test)]
            RuntimeOwnershipPolicy::TestOwner { owner_uid, .. } => owner_uid,
        };

        let mut cursor = PathBuf::new();
        let components: Vec<Component<'_>> = dir.components().collect();
        for (idx, comp) in components.iter().copied().enumerate() {
            let is_leaf_dir = idx + 1 == components.len();
            match comp {
                Component::RootDir => {
                    cursor.push("/");
                }
                Component::Normal(part) => {
                    cursor.push(part);
                }
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(runtime_unsupported_error());
                }
            }

            let meta = fs::symlink_metadata(&cursor).map_err(|_| runtime_unsupported_error())?;
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(runtime_unsupported_error());
            }

            let mode = meta.permissions().mode() & 0o7777;

            match policy {
                RuntimeOwnershipPolicy::RootImmutable => {
                    // World-writable ancestor directories (such as `/tmp` `01777`) and non-root
                    // owners are strictly rejected across the entire path chain.
                    if (mode & 0o002) != 0 || meta.uid() != required_owner_uid {
                        return Err(runtime_unsupported_error());
                    }
                    if is_leaf_dir && (mode & 0o022) != 0 {
                        return Err(runtime_unsupported_error());
                    }
                    if is_leaf_dir && dir.starts_with("/nix/store") && (mode & 0o222) != 0 {
                        return Err(runtime_unsupported_error());
                    }
                }
                #[cfg(test)]
                RuntimeOwnershipPolicy::TestOwner {
                    owner_uid,
                    require_read_only,
                } => {
                    // Allow sticky `/tmp` (`01777`) as an outer ancestor for disposable test
                    // directories while rejecting non-sticky world-writable ancestors and enforcing
                    // strict ownership and read-only bits on the simulated runtime directory itself.
                    if !is_leaf_dir && (mode & 0o002) != 0 && (mode & 0o1000) == 0 {
                        return Err(runtime_unsupported_error());
                    }
                    if is_leaf_dir {
                        if meta.uid() != owner_uid || (mode & 0o022) != 0 {
                            return Err(runtime_unsupported_error());
                        }
                        if require_read_only && (mode & 0o222) != 0 {
                            return Err(runtime_unsupported_error());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn resolve_verified_runtime_file_path(
        path: &Path,
        expected_user_uid: u32,
        policy: RuntimeOwnershipPolicy,
    ) -> Result<PathBuf, AppError> {
        let parent = path.parent().ok_or_else(runtime_unsupported_error)?;
        verify_runtime_dir_chain(parent, expected_user_uid, policy)?;

        let required_owner_uid = match policy {
            RuntimeOwnershipPolicy::RootImmutable => 0,
            #[cfg(test)]
            RuntimeOwnershipPolicy::TestOwner { owner_uid, .. } => owner_uid,
        };

        let meta = fs::symlink_metadata(path).map_err(|_| runtime_unsupported_error())?;
        if meta.uid() != required_owner_uid {
            return Err(runtime_unsupported_error());
        }
        if meta.file_type().is_symlink() {
            // Permit immutable Nix store symlink farms (`symlinkJoin`) only when the symlink
            // itself resides inside a verified immutable runtime directory and resolves to a
            // canonical target that also passes full immutable runtime chain verification.
            let resolved = fs::canonicalize(path).map_err(|_| runtime_unsupported_error())?;
            let resolved_parent = resolved.parent().ok_or_else(runtime_unsupported_error)?;
            verify_runtime_dir_chain(resolved_parent, expected_user_uid, policy)?;
            return Ok(resolved);
        }

        Ok(path.to_path_buf())
    }

    fn open_verified_runtime_file(
        raw_path: &Path,
        expected_user_uid: u32,
        policy: RuntimeOwnershipPolicy,
        require_executable: bool,
    ) -> Result<(File, u64, u64, u64), AppError> {
        let path_buf = resolve_verified_runtime_file_path(raw_path, expected_user_uid, policy)?;
        let path = path_buf.as_path();

        let required_owner_uid = match policy {
            RuntimeOwnershipPolicy::RootImmutable => 0,
            #[cfg(test)]
            RuntimeOwnershipPolicy::TestOwner { owner_uid, .. } => owner_uid,
        };
        let must_be_completely_read_only = match policy {
            RuntimeOwnershipPolicy::RootImmutable => path.starts_with("/nix/store"),
            #[cfg(test)]
            RuntimeOwnershipPolicy::TestOwner {
                require_read_only, ..
            } => require_read_only,
        };

        let pre_meta = fs::symlink_metadata(path).map_err(|_| runtime_unsupported_error())?;
        let pre_mode = pre_meta.permissions().mode() & 0o7777;
        if pre_meta.file_type().is_symlink()
            || !pre_meta.file_type().is_file()
            || pre_meta.uid() != required_owner_uid
            || (pre_mode & 0o022) != 0
            || (must_be_completely_read_only && (pre_mode & 0o222) != 0)
            || (require_executable && (pre_mode & 0o111) == 0)
        {
            return Err(runtime_unsupported_error());
        }

        let file = File::open(path).map_err(|_| runtime_unsupported_error())?;
        let fd_meta = file.metadata().map_err(|_| runtime_unsupported_error())?;
        let fd_mode = fd_meta.permissions().mode() & 0o7777;
        if !fd_meta.file_type().is_file()
            || fd_meta.dev() != pre_meta.dev()
            || fd_meta.ino() != pre_meta.ino()
            || fd_meta.uid() != required_owner_uid
            || fd_meta.len() != pre_meta.len()
            || (fd_mode & 0o022) != 0
            || (must_be_completely_read_only && (fd_mode & 0o222) != 0)
            || (require_executable && (fd_mode & 0o111) == 0)
        {
            return Err(runtime_unsupported_error());
        }

        let post_meta = fs::symlink_metadata(path).map_err(|_| runtime_unsupported_error())?;
        if post_meta.file_type().is_symlink()
            || post_meta.dev() != fd_meta.dev()
            || post_meta.ino() != fd_meta.ino()
        {
            return Err(runtime_unsupported_error());
        }

        Ok((file, fd_meta.len(), fd_meta.dev(), fd_meta.ino()))
    }

    fn read_verified_runtime_bytes(
        path: &Path,
        expected_user_uid: u32,
        policy: RuntimeOwnershipPolicy,
        max_bytes: u64,
    ) -> Result<Vec<u8>, AppError> {
        let (file, file_len, _dev, _ino) =
            open_verified_runtime_file(path, expected_user_uid, policy, false)?;
        if file_len > max_bytes {
            return Err(runtime_unsupported_error());
        }
        let capacity = usize::try_from(file_len).map_err(|_| runtime_unsupported_error())?;
        let mut buf = Vec::with_capacity(capacity);
        file.take(max_bytes + 1)
            .read_to_end(&mut buf)
            .map_err(|_| runtime_unsupported_error())?;
        if (buf.len() as u64) != file_len || (buf.len() as u64) > max_bytes {
            return Err(runtime_unsupported_error());
        }
        Ok(buf)
    }

    fn extract_cmdline_asar_candidates(cmdline_bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
        let mut asar_paths = Vec::new();
        let args: Vec<&[u8]> = cmdline_bytes.split(|&b| b == 0).skip(1).collect();
        let mut idx = 0usize;
        while idx < args.len() {
            let raw_arg = args[idx];
            idx += 1;
            if raw_arg.is_empty() {
                continue;
            }
            let Ok(arg) = std::str::from_utf8(raw_arg) else {
                return Err(runtime_unsupported_error());
            };
            if arg == "--" {
                break;
            }
            if let Some(app_override) = arg.strip_prefix("--app=") {
                if !app_override.ends_with(".asar") {
                    // Reject unpacked directory or JS overrides (e.g. fixture substitutes).
                    return Err(runtime_unsupported_error());
                }
                let p = PathBuf::from(app_override);
                validate_clean_absolute_path(&p).map_err(|_| runtime_unsupported_error())?;
                asar_paths.push(p);
                continue;
            }
            if arg == "--app" {
                let Some(&next_raw) = args.get(idx) else {
                    return Err(runtime_unsupported_error());
                };
                idx += 1;
                let Ok(next_arg) = std::str::from_utf8(next_raw) else {
                    return Err(runtime_unsupported_error());
                };
                if !next_arg.ends_with(".asar") {
                    return Err(runtime_unsupported_error());
                }
                let p = PathBuf::from(next_arg);
                validate_clean_absolute_path(&p).map_err(|_| runtime_unsupported_error())?;
                asar_paths.push(p);
                continue;
            }
            if arg.starts_with('-') || arg.starts_with("codex://") {
                continue;
            }
            if arg.ends_with(".asar") {
                let p = PathBuf::from(arg);
                validate_clean_absolute_path(&p).map_err(|_| runtime_unsupported_error())?;
                asar_paths.push(p);
            } else if arg.starts_with('/') {
                // Reject explicit non-ASAR filesystem app entrypoints passed on cmdline.
                return Err(runtime_unsupported_error());
            }
        }
        Ok(asar_paths)
    }

    fn qualify_owl_codex_runtime(
        proc_root: &Path,
        pid: u32,
        canonical_exe: &Path,
        expected_user_uid: u32,
        policy: RuntimeOwnershipPolicy,
    ) -> Result<(u64, u64), AppError> {
        let (_exe_file, _exe_len, exe_dev, exe_ino) =
            open_verified_runtime_file(canonical_exe, expected_user_uid, policy, true)?;

        let exe_dir = canonical_exe
            .parent()
            .ok_or_else(runtime_unsupported_error)?;

        let cmdline_bytes = read_bounded_proc_bytes(
            &proc_root.join(pid.to_string()).join("cmdline"),
            MAX_PROC_SMALL_BYTES,
        )
        .map_err(|_| runtime_unsupported_error())?;
        let cmdline_asars = extract_cmdline_asar_candidates(&cmdline_bytes)?;

        let bundled_resources_asar = exe_dir.join("resources").join("app.asar");
        let bundled_root_asar = exe_dir.join("app.asar");
        let bundled_asar = if fs::symlink_metadata(&bundled_resources_asar).is_ok() {
            Some(bundled_resources_asar)
        } else if fs::symlink_metadata(&bundled_root_asar).is_ok() {
            Some(bundled_root_asar)
        } else {
            None
        };

        let resolved_asar = match bundled_asar {
            Some(bundled) => {
                if !cmdline_asars.is_empty() {
                    let b_resolved =
                        resolve_verified_runtime_file_path(&bundled, expected_user_uid, policy)?;
                    let b_meta = fs::symlink_metadata(&b_resolved)
                        .map_err(|_| runtime_unsupported_error())?;
                    for candidate in &cmdline_asars {
                        let c_resolved = resolve_verified_runtime_file_path(
                            candidate,
                            expected_user_uid,
                            policy,
                        )?;
                        let c_meta = fs::symlink_metadata(&c_resolved)
                            .map_err(|_| runtime_unsupported_error())?;
                        if (c_meta.dev(), c_meta.ino()) != (b_meta.dev(), b_meta.ino()) {
                            return Err(runtime_unsupported_error());
                        }
                    }
                }
                bundled
            }
            None => {
                if cmdline_asars.len() != 1 {
                    return Err(runtime_unsupported_error());
                }
                cmdline_asars[0].clone()
            }
        };

        let (mut asar_file, asar_len, _asar_dev, _asar_ino) =
            open_verified_runtime_file(&resolved_asar, expected_user_uid, policy, false)?;
        verify_asar_package_identity_from_reader(&mut asar_file, asar_len)?;

        let mut candidate_marker_dirs = vec![exe_dir.to_path_buf(), exe_dir.join("resources")];
        if let Some(asar_dir) = resolved_asar.parent() {
            candidate_marker_dirs.push(asar_dir.to_path_buf());
            if asar_dir.file_name().and_then(|n| n.to_str()) == Some("resources") {
                if let Some(asar_parent) = asar_dir.parent() {
                    candidate_marker_dirs.push(asar_parent.to_path_buf());
                }
            }
        }

        let exe_version_path = exe_dir.join(OWL_RUNTIME_VERSION_FILENAME);
        let exe_sibling_version = if fs::symlink_metadata(&exe_version_path).is_ok() {
            let v_bytes = read_verified_runtime_bytes(
                &exe_version_path,
                expected_user_uid,
                policy,
                MAX_OWL_VERSION_FILE_BYTES,
            )?;
            Some(String::from_utf8(v_bytes).map_err(|_| runtime_unsupported_error())?)
        } else {
            None
        };

        let mut verified_markers = 0usize;
        let mut seen_marker_inodes = std::collections::BTreeSet::new();
        for dir in &candidate_marker_dirs {
            let candidate_marker = dir.join(OWL_RUNTIME_MARKER_FILENAME);
            if fs::symlink_metadata(&candidate_marker).is_err() {
                continue;
            }
            let resolved_marker =
                resolve_verified_runtime_file_path(&candidate_marker, expected_user_uid, policy)?;
            let marker_meta =
                fs::symlink_metadata(&resolved_marker).map_err(|_| runtime_unsupported_error())?;
            if !seen_marker_inodes.insert((marker_meta.dev(), marker_meta.ino())) {
                continue;
            }
            let marker_bytes = read_verified_runtime_bytes(
                &resolved_marker,
                expected_user_uid,
                policy,
                MAX_OWL_MARKER_JSON_BYTES,
            )?;

            let dir_version_path = dir.join(OWL_RUNTIME_VERSION_FILENAME);
            let local_version = if dir_version_path != exe_version_path
                && fs::symlink_metadata(&dir_version_path).is_ok()
            {
                let v_bytes = read_verified_runtime_bytes(
                    &dir_version_path,
                    expected_user_uid,
                    policy,
                    MAX_OWL_VERSION_FILE_BYTES,
                )?;
                let v_str = String::from_utf8(v_bytes).map_err(|_| runtime_unsupported_error())?;
                if let Some(ref exe_v) = exe_sibling_version {
                    if v_str.trim() != exe_v.trim() {
                        return Err(runtime_unsupported_error());
                    }
                }
                Some(v_str)
            } else {
                exe_sibling_version.clone()
            };

            verify_owl_runtime_marker_bytes(&marker_bytes, local_version.as_deref())?;
            verified_markers += 1;
        }

        if verified_markers == 0 {
            return Err(runtime_unsupported_error());
        }

        Ok((exe_dev, exe_ino))
    }

    fn inspect_pre_connect_state(
        config: &LinuxOwlActivatorConfig,
    ) -> Result<PreConnectVerifiedState, AppError> {
        let expected_uid = current_process_uid()?;
        let (profile_dir, profile_dev, profile_ino) =
            discover_codex_profile_dir(&config.profile_source, expected_uid)?;

        let lock_target =
            read_owned_symlink_utf8(&profile_dir.join(SINGLETON_LOCK_FILENAME), expected_uid)?;
        let expected_pid = parse_singleton_lock_target(&lock_target)?;

        let profile_cookie =
            read_owned_symlink_utf8(&profile_dir.join(SINGLETON_COOKIE_FILENAME), expected_uid)?;
        validate_cookie_syntax(&profile_cookie)?;

        let socket_link_target =
            read_owned_symlink_utf8(&profile_dir.join(SINGLETON_SOCKET_FILENAME), expected_uid)?;
        if socket_link_target.len() > MAX_UNIX_SOCKET_PATH_BYTES {
            return Err(app_unavailable_error());
        }
        let socket_path = PathBuf::from(&socket_link_target);
        validate_clean_absolute_path(&socket_path)?;
        if socket_path.file_name().and_then(|n| n.to_str()) != Some(SINGLETON_SOCKET_FILENAME) {
            return Err(app_unavailable_error());
        }
        let socket_dir = socket_path
            .parent()
            .filter(|p| *p != Path::new("/"))
            .ok_or_else(app_unavailable_error)?
            .to_path_buf();

        verify_no_symlink_components(&socket_dir)?;
        let socket_dir_meta =
            fs::symlink_metadata(&socket_dir).map_err(|_| app_unavailable_error())?;
        if socket_dir_meta.file_type().is_symlink()
            || !socket_dir_meta.file_type().is_dir()
            || socket_dir_meta.uid() != expected_uid
            || (socket_dir_meta.permissions().mode() & 0o077) != 0
        {
            return Err(app_unavailable_error());
        }

        let remote_cookie =
            read_owned_symlink_utf8(&socket_dir.join(SINGLETON_COOKIE_FILENAME), expected_uid)?;
        validate_cookie_syntax(&remote_cookie)?;
        if !constant_time_bytes_eq(profile_cookie.as_bytes(), remote_cookie.as_bytes()) {
            return Err(app_unavailable_error());
        }

        let socket_meta =
            fs::symlink_metadata(&socket_path).map_err(|_| app_unavailable_error())?;
        if socket_meta.file_type().is_symlink()
            || !socket_meta.file_type().is_socket()
            || socket_meta.uid() != expected_uid
            || (socket_meta.permissions().mode() & 0o022) != 0
        {
            return Err(app_unavailable_error());
        }

        let (boot_id, start_ticks, canonical_exe, canonical_exe_utf8) =
            read_live_proc_identity(&config.proc_root, expected_pid, expected_uid)?;

        let (exe_dev, exe_ino) = qualify_owl_codex_runtime(
            &config.proc_root,
            expected_pid,
            &canonical_exe,
            expected_uid,
            config.ownership_policy,
        )?;

        let compositor =
            discover_and_qualify_compositor_endpoint(&config.compositor_source, expected_uid)?;

        Ok(PreConnectVerifiedState {
            expected_uid,
            profile_dir,
            profile_dev,
            profile_ino,
            lock_target,
            expected_pid,
            cookie: profile_cookie,
            socket_dir,
            socket_dir_dev: socket_dir_meta.dev(),
            socket_dir_ino: socket_dir_meta.ino(),
            socket_path,
            socket_dev: socket_meta.dev(),
            socket_ino: socket_meta.ino(),
            boot_id,
            start_ticks,
            canonical_exe,
            canonical_exe_utf8,
            exe_dev,
            exe_ino,
            compositor,
        })
    }

    fn verify_live_codex_process_unchanged(
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
    ) -> Result<(), AppError> {
        let (boot_id_now, start_ticks_now, exe_now, _exe_utf8_now) =
            read_live_proc_identity(&config.proc_root, pre.expected_pid, pre.expected_uid)?;
        if boot_id_now != pre.boot_id
            || start_ticks_now != pre.start_ticks
            || exe_now != pre.canonical_exe
        {
            return Err(app_unavailable_error());
        }
        Ok(())
    }

    fn discover_and_qualify_compositor_endpoint(
        source: &LinuxCompositorSource,
        expected_uid: u32,
    ) -> Result<QualifiedCompositorEndpoint, AppError> {
        let (xdg_runtime_raw, hypr_sig_raw, swaysock_raw) = match source {
            LinuxCompositorSource::NativeEnv => (
                std::env::var_os("XDG_RUNTIME_DIR"),
                std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE"),
                std::env::var_os("SWAYSOCK"),
            ),
            #[cfg(test)]
            LinuxCompositorSource::ExplicitEnv {
                xdg_runtime_dir,
                hyprland_instance_signature,
                swaysock,
                ..
            } => (
                xdg_runtime_dir.clone(),
                hyprland_instance_signature.clone(),
                swaysock.clone(),
            ),
        };

        match (hypr_sig_raw, swaysock_raw) {
            (Some(sig_os), None) => {
                qualify_hyprland_endpoint(xdg_runtime_raw, sig_os, expected_uid)
            }
            (None, Some(sway_os)) => qualify_sway_endpoint(sway_os, expected_uid),
            (None, None) | (Some(_), Some(_)) => Err(navigation_unsupported_error()),
        }
    }

    fn qualify_hyprland_endpoint(
        xdg_runtime_raw: Option<OsString>,
        sig_os: OsString,
        expected_uid: u32,
    ) -> Result<QualifiedCompositorEndpoint, AppError> {
        let xrd_os = xdg_runtime_raw.ok_or_else(focus_unavailable_error)?;
        let xdg_runtime_dir = parse_required_clean_env_dir_with(xrd_os, focus_unavailable_error)?;
        verify_no_symlink_components_with(&xdg_runtime_dir, focus_unavailable_error)?;

        let xrd_meta =
            fs::symlink_metadata(&xdg_runtime_dir).map_err(|_| focus_unavailable_error())?;
        if xrd_meta.file_type().is_symlink()
            || !xrd_meta.file_type().is_dir()
            || xrd_meta.uid() != expected_uid
            || (xrd_meta.permissions().mode() & 0o077) != 0
        {
            return Err(focus_unavailable_error());
        }

        if sig_os.is_empty() {
            return Err(focus_unavailable_error());
        }
        let sig_raw = sig_os
            .into_string()
            .map_err(|_| focus_unavailable_error())?;
        let signature = validate_hyprland_instance_signature(&sig_raw)?;

        let hypr_dir = xdg_runtime_dir.join("hypr");
        let hypr_meta = fs::symlink_metadata(&hypr_dir).map_err(|_| focus_unavailable_error())?;
        if hypr_meta.file_type().is_symlink()
            || !hypr_meta.file_type().is_dir()
            || hypr_meta.uid() != expected_uid
            || (hypr_meta.permissions().mode() & 0o022) != 0
        {
            return Err(focus_unavailable_error());
        }

        let instance_dir = hypr_dir.join(&signature);
        let inst_meta =
            fs::symlink_metadata(&instance_dir).map_err(|_| focus_unavailable_error())?;
        if inst_meta.file_type().is_symlink()
            || !inst_meta.file_type().is_dir()
            || inst_meta.uid() != expected_uid
            || (inst_meta.permissions().mode() & 0o022) != 0
        {
            return Err(focus_unavailable_error());
        }

        let socket_path = instance_dir.join(".socket.sock");
        let Some(sock_utf8) = socket_path.to_str() else {
            return Err(focus_unavailable_error());
        };
        if sock_utf8.len() > MAX_COMPOSITOR_SOCKET_PATH_BYTES {
            return Err(focus_unavailable_error());
        }
        validate_clean_absolute_path(&socket_path).map_err(|_| focus_unavailable_error())?;

        let sock_meta =
            fs::symlink_metadata(&socket_path).map_err(|_| focus_unavailable_error())?;
        if sock_meta.file_type().is_symlink()
            || !sock_meta.file_type().is_socket()
            || sock_meta.uid() != expected_uid
            || (sock_meta.permissions().mode() & 0o022) != 0
        {
            return Err(focus_unavailable_error());
        }

        Ok(QualifiedCompositorEndpoint::Hyprland {
            xdg_runtime_dir,
            xdg_runtime_dev: xrd_meta.dev(),
            xdg_runtime_ino: xrd_meta.ino(),
            hypr_dir,
            hypr_dev: hypr_meta.dev(),
            hypr_ino: hypr_meta.ino(),
            instance_dir,
            instance_dev: inst_meta.dev(),
            instance_ino: inst_meta.ino(),
            socket_path,
            socket_dev: sock_meta.dev(),
            socket_ino: sock_meta.ino(),
        })
    }

    fn qualify_sway_endpoint(
        sway_os: OsString,
        expected_uid: u32,
    ) -> Result<QualifiedCompositorEndpoint, AppError> {
        let socket_path = parse_required_clean_env_dir_with(sway_os, focus_unavailable_error)?;
        let Some(sock_utf8) = socket_path.to_str() else {
            return Err(focus_unavailable_error());
        };
        if sock_utf8.len() > MAX_COMPOSITOR_SOCKET_PATH_BYTES {
            return Err(focus_unavailable_error());
        }
        let parent_dir = socket_path
            .parent()
            .filter(|p| *p != Path::new("/"))
            .ok_or_else(focus_unavailable_error)?
            .to_path_buf();

        validate_clean_absolute_path(&parent_dir).map_err(|_| focus_unavailable_error())?;

        let mut cursor = PathBuf::new();
        let mut private_root: Option<(PathBuf, u64, u64)> = None;
        let mut parent_dev_ino: Option<(u64, u64)> = None;

        for comp in parent_dir.components() {
            match comp {
                Component::RootDir => cursor.push("/"),
                Component::Normal(part) => cursor.push(part),
                Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                    return Err(focus_unavailable_error());
                }
            }
            let meta = fs::symlink_metadata(&cursor).map_err(|_| focus_unavailable_error())?;
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(focus_unavailable_error());
            }
            let mode = meta.permissions().mode() & 0o7777;
            if private_root.is_none() {
                if (mode & 0o002) != 0 && (mode & 0o1000) == 0 {
                    return Err(focus_unavailable_error());
                }
                if meta.uid() == expected_uid && (mode & 0o077) == 0 {
                    private_root = Some((cursor.clone(), meta.dev(), meta.ino()));
                }
            } else if meta.uid() != expected_uid || (mode & 0o022) != 0 {
                return Err(focus_unavailable_error());
            }
            parent_dev_ino = Some((meta.dev(), meta.ino()));
        }

        let Some((private_root_dir, private_root_dev, private_root_ino)) = private_root else {
            return Err(focus_unavailable_error());
        };
        let Some((parent_dev, parent_ino)) = parent_dev_ino else {
            return Err(focus_unavailable_error());
        };

        let sock_meta =
            fs::symlink_metadata(&socket_path).map_err(|_| focus_unavailable_error())?;
        if sock_meta.file_type().is_symlink()
            || !sock_meta.file_type().is_socket()
            || sock_meta.uid() != expected_uid
            || (sock_meta.permissions().mode() & 0o022) != 0
        {
            return Err(focus_unavailable_error());
        }

        Ok(QualifiedCompositorEndpoint::Sway {
            private_root_dir,
            private_root_dev,
            private_root_ino,
            parent_dir,
            parent_dev,
            parent_ino,
            socket_path,
            socket_dev: sock_meta.dev(),
            socket_ino: sock_meta.ino(),
        })
    }

    fn reverify_compositor_endpoint_on_disk(
        endpoint: &QualifiedCompositorEndpoint,
        expected_uid: u32,
    ) -> Result<(), AppError> {
        let check_dir = |dir: &Path, dev: u64, ino: u64, mask: u32| -> Result<(), AppError> {
            let meta = fs::symlink_metadata(dir).map_err(|_| focus_unavailable_error())?;
            if meta.file_type().is_symlink()
                || !meta.file_type().is_dir()
                || meta.uid() != expected_uid
                || (meta.permissions().mode() & mask) != 0
                || meta.dev() != dev
                || meta.ino() != ino
            {
                return Err(focus_unavailable_error());
            }
            Ok(())
        };

        let check_sock = |sock: &Path, dev: u64, ino: u64| -> Result<(), AppError> {
            let meta = fs::symlink_metadata(sock).map_err(|_| focus_unavailable_error())?;
            if meta.file_type().is_symlink()
                || !meta.file_type().is_socket()
                || meta.uid() != expected_uid
                || (meta.permissions().mode() & 0o022) != 0
                || meta.dev() != dev
                || meta.ino() != ino
            {
                return Err(focus_unavailable_error());
            }
            Ok(())
        };

        match endpoint {
            QualifiedCompositorEndpoint::Hyprland {
                xdg_runtime_dir,
                xdg_runtime_dev,
                xdg_runtime_ino,
                hypr_dir,
                hypr_dev,
                hypr_ino,
                instance_dir,
                instance_dev,
                instance_ino,
                socket_path,
                socket_dev,
                socket_ino,
            } => {
                check_dir(xdg_runtime_dir, *xdg_runtime_dev, *xdg_runtime_ino, 0o077)?;
                check_dir(hypr_dir, *hypr_dev, *hypr_ino, 0o022)?;
                check_dir(instance_dir, *instance_dev, *instance_ino, 0o022)?;
                check_sock(socket_path, *socket_dev, *socket_ino)?;
            }
            QualifiedCompositorEndpoint::Sway {
                private_root_dir,
                private_root_dev,
                private_root_ino,
                parent_dir,
                parent_dev,
                parent_ino,
                socket_path,
                socket_dev,
                socket_ino,
            } => {
                check_dir(
                    private_root_dir,
                    *private_root_dev,
                    *private_root_ino,
                    0o077,
                )?;
                check_dir(parent_dir, *parent_dev, *parent_ino, 0o022)?;
                check_sock(socket_path, *socket_dev, *socket_ino)?;
            }
        }
        Ok(())
    }

    fn is_valid_hyprland_comm(comm: &str) -> bool {
        matches!(
            comm,
            "Hyprland" | ".Hyprland-wrapp" | "hyprland" | ".hyprland-wrapp"
        )
    }

    fn is_valid_hyprland_exe_name(name: &str) -> bool {
        matches!(
            name,
            "Hyprland" | ".Hyprland-wrapped" | "hyprland" | ".hyprland-wrapped"
        )
    }

    fn is_valid_sway_comm(comm: &str) -> bool {
        matches!(comm, "sway" | ".sway-wrapped" | "Sway" | ".Sway-wrapped")
    }

    fn is_valid_sway_exe_name(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        lower == "sway"
            || lower == ".sway-wrapped"
            || lower.starts_with("sway-")
            || lower.starts_with(".sway-")
    }

    fn verify_compositor_stream_peer(
        stream: &UnixStream,
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
        expected_prior: Option<&VerifiedCompositorPeer>,
    ) -> Result<VerifiedCompositorPeer, AppError> {
        let peer_cred = stream.peer_cred().map_err(|_| focus_unavailable_error())?;
        if peer_cred.uid() != pre.expected_uid {
            return Err(focus_unavailable_error());
        }
        let raw_pid = peer_cred.pid().ok_or_else(focus_unavailable_error)?;
        if raw_pid <= 0 {
            return Err(focus_unavailable_error());
        }
        let peer_pid = u32::try_from(raw_pid).map_err(|_| focus_unavailable_error())?;

        let (comp_proc_root, simulate_exe_eacces, allow_same_pid) = match &config.compositor_source
        {
            LinuxCompositorSource::NativeEnv => (&config.proc_root, false, false),
            #[cfg(test)]
            LinuxCompositorSource::ExplicitEnv {
                compositor_proc_root,
                simulate_exe_permission_denied,
                allow_same_pid_as_target,
                ..
            } => (
                compositor_proc_root.as_ref().unwrap_or(&config.proc_root),
                *simulate_exe_permission_denied,
                *allow_same_pid_as_target,
            ),
        };

        if !allow_same_pid && peer_pid == pre.expected_pid {
            return Err(focus_unavailable_error());
        }

        let boot_id = read_bounded_proc_string(
            &comp_proc_root.join("sys/kernel/random/boot_id"),
            MAX_OWL_VERSION_FILE_BYTES,
        )
        .map_err(|_| focus_unavailable_error())?;
        let boot_id = boot_id.trim().to_string();
        if boot_id.is_empty()
            || boot_id.len() > 128
            || !boot_id.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(focus_unavailable_error());
        }

        let proc_pid_dir = comp_proc_root.join(peer_pid.to_string());
        let pid_meta =
            fs::symlink_metadata(&proc_pid_dir).map_err(|_| focus_unavailable_error())?;
        if pid_meta.uid() != pre.expected_uid {
            return Err(focus_unavailable_error());
        }

        let stat_content =
            read_bounded_proc_string(&proc_pid_dir.join("stat"), MAX_PROC_SMALL_BYTES)
                .map_err(|_| focus_unavailable_error())?;
        let (stat_comm, start_ticks) =
            parse_proc_stat_comm_and_start_ticks(&stat_content, peer_pid, focus_unavailable_error)?;

        let comm_raw =
            read_bounded_proc_string(&proc_pid_dir.join("comm"), MAX_OWL_VERSION_FILE_BYTES)
                .map_err(|_| focus_unavailable_error())?;
        let comm = comm_raw.trim_end_matches('\n').to_string();
        if comm.is_empty() || comm.chars().any(char::is_control) || comm != stat_comm {
            return Err(focus_unavailable_error());
        }

        let exe_read = if simulate_exe_eacces {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        } else {
            fs::read_link(proc_pid_dir.join("exe"))
        };

        match &pre.compositor {
            QualifiedCompositorEndpoint::Hyprland { .. } => {
                if !is_valid_hyprland_comm(&comm) {
                    return Err(focus_unavailable_error());
                }
                match exe_read {
                    Ok(exe_path) => {
                        let exe_utf8 = exe_path
                            .clone()
                            .into_os_string()
                            .into_string()
                            .map_err(|_| focus_unavailable_error())?;
                        if exe_utf8.ends_with(" (deleted)") {
                            return Err(focus_unavailable_error());
                        }
                        validate_clean_absolute_path(&exe_path)
                            .map_err(|_| focus_unavailable_error())?;
                        let Some(exe_name) = exe_path.file_name().and_then(|n| n.to_str()) else {
                            return Err(focus_unavailable_error());
                        };
                        if !is_valid_hyprland_exe_name(exe_name) {
                            return Err(focus_unavailable_error());
                        }
                    }
                    Err(io_err) if io_err.kind() == std::io::ErrorKind::PermissionDenied => {
                        // Legitimate non-dumpable wrapped Hyprland compositor (`/proc/<pid>/exe`
                        // returns `EACCES` while `SO_PEERCRED`, `comm`, and `stat` are readable).
                    }
                    Err(_) => return Err(focus_unavailable_error()),
                }
            }
            QualifiedCompositorEndpoint::Sway { .. } => {
                if !is_valid_sway_comm(&comm) {
                    return Err(focus_unavailable_error());
                }
                let exe_path = exe_read.map_err(|_| focus_unavailable_error())?;
                let exe_utf8 = exe_path
                    .clone()
                    .into_os_string()
                    .into_string()
                    .map_err(|_| focus_unavailable_error())?;
                if exe_utf8.ends_with(" (deleted)") {
                    return Err(focus_unavailable_error());
                }
                validate_clean_absolute_path(&exe_path).map_err(|_| focus_unavailable_error())?;
                let Some(exe_name) = exe_path.file_name().and_then(|n| n.to_str()) else {
                    return Err(focus_unavailable_error());
                };
                if !is_valid_sway_exe_name(exe_name) {
                    return Err(focus_unavailable_error());
                }
            }
        }

        reverify_compositor_endpoint_on_disk(&pre.compositor, pre.expected_uid)?;

        let verified = VerifiedCompositorPeer {
            pid: peer_pid,
            boot_id,
            start_ticks,
            comm,
        };
        if let Some(prior) = expected_prior {
            if &verified != prior {
                return Err(focus_unavailable_error());
            }
        }
        Ok(verified)
    }

    async fn read_bounded_stream_to_eof(
        stream: &mut UnixStream,
        deadline_at: Instant,
        step_timeout: Duration,
    ) -> Result<Vec<u8>, AppError> {
        let mut out = Vec::new();
        let mut chunk = [0u8; 8_192];
        loop {
            let budget =
                step_budget(deadline_at, step_timeout).ok_or_else(focus_unavailable_error)?;
            let n = match tokio::time::timeout(budget, stream.read(&mut chunk)).await {
                Ok(Ok(n)) => n,
                Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
            };
            if n == 0 {
                return Ok(out);
            }
            if out.len().saturating_add(n) >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
                return Err(focus_unavailable_error());
            }
            out.extend_from_slice(&chunk[..n]);
        }
    }

    async fn read_exact_bounded(
        stream: &mut UnixStream,
        expected_len: usize,
        deadline_at: Instant,
        step_timeout: Duration,
    ) -> Result<Vec<u8>, AppError> {
        if expected_len >= MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES {
            return Err(focus_unavailable_error());
        }
        let mut out = Vec::with_capacity(expected_len.min(8_192));
        let mut chunk = [0u8; 8_192];
        while out.len() < expected_len {
            let budget =
                step_budget(deadline_at, step_timeout).ok_or_else(focus_unavailable_error)?;
            let want = (expected_len - out.len()).min(chunk.len());
            let n = match tokio::time::timeout(budget, stream.read(&mut chunk[..want])).await {
                Ok(Ok(n)) => n,
                Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
            };
            if n == 0 {
                return Err(focus_unavailable_error());
            }
            out.extend_from_slice(&chunk[..n]);
        }
        Ok(out)
    }

    async fn execute_hyprland_command(
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
        socket_path: &Path,
        command: &str,
        prior_peer: Option<&VerifiedCompositorPeer>,
        deadline_at: Instant,
    ) -> Result<(VerifiedCompositorPeer, Vec<u8>), AppError> {
        if command.is_empty()
            || command.len() > 256
            || command.bytes().any(|b| b == 0 || b == b'\n' || b == b'\r')
        {
            return Err(focus_unavailable_error());
        }
        reverify_compositor_endpoint_on_disk(&pre.compositor, pre.expected_uid)?;

        let connect_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        let mut stream =
            match tokio::time::timeout(connect_budget, UnixStream::connect(socket_path)).await {
                Ok(Ok(s)) => s,
                Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
            };

        let verified_peer = verify_compositor_stream_peer(&stream, config, pre, prior_peer)?;

        let write_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        match tokio::time::timeout(write_budget, stream.write_all(command.as_bytes())).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
        }

        let flush_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        match tokio::time::timeout(flush_budget, stream.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
        }

        let response =
            read_bounded_stream_to_eof(&mut stream, deadline_at, config.step_timeout).await?;
        drop(stream);
        Ok((verified_peer, response))
    }

    async fn execute_sway_ipc_command(
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
        socket_path: &Path,
        message_type: u32,
        payload: &[u8],
        prior_peer: Option<&VerifiedCompositorPeer>,
        deadline_at: Instant,
    ) -> Result<(VerifiedCompositorPeer, Vec<u8>), AppError> {
        let frame = encode_sway_ipc_frame(message_type, payload)?;
        reverify_compositor_endpoint_on_disk(&pre.compositor, pre.expected_uid)?;

        let connect_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        let mut stream =
            match tokio::time::timeout(connect_budget, UnixStream::connect(socket_path)).await {
                Ok(Ok(s)) => s,
                Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
            };

        let verified_peer = verify_compositor_stream_peer(&stream, config, pre, prior_peer)?;

        let write_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        match tokio::time::timeout(write_budget, stream.write_all(&frame)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
        }

        let flush_budget =
            step_budget(deadline_at, config.step_timeout).ok_or_else(focus_unavailable_error)?;
        match tokio::time::timeout(flush_budget, stream.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Err(focus_unavailable_error()),
        }

        let header_vec = read_exact_bounded(
            &mut stream,
            SWAY_IPC_HEADER_BYTES,
            deadline_at,
            config.step_timeout,
        )
        .await?;
        let mut header = [0u8; SWAY_IPC_HEADER_BYTES];
        header.copy_from_slice(&header_vec);
        let payload_len = decode_sway_ipc_header(&header, message_type)?;

        let response =
            read_exact_bounded(&mut stream, payload_len, deadline_at, config.step_timeout).await?;
        drop(stream);
        Ok((verified_peer, response))
    }

    async fn preflight_compositor_focus(
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
        deadline_at: Instant,
    ) -> Result<PreflightFocusPlan, AppError> {
        match &pre.compositor {
            QualifiedCompositorEndpoint::Hyprland { socket_path, .. } => {
                let (peer, clients_json) = execute_hyprland_command(
                    config,
                    pre,
                    socket_path,
                    "j/clients",
                    None,
                    deadline_at,
                )
                .await?;
                let hex_address = parse_hyprland_clients_for_pid(&clients_json, pre.expected_pid)?;
                Ok(PreflightFocusPlan {
                    peer,
                    target: CompositorFocusTarget::Hyprland { hex_address },
                })
            }
            QualifiedCompositorEndpoint::Sway { socket_path, .. } => {
                let (peer, tree_json) = execute_sway_ipc_command(
                    config,
                    pre,
                    socket_path,
                    SWAY_IPC_GET_TREE,
                    b"",
                    None,
                    deadline_at,
                )
                .await?;
                let candidate = parse_sway_tree_for_pid(&tree_json, pre.expected_pid)?;
                Ok(PreflightFocusPlan {
                    peer,
                    target: CompositorFocusTarget::Sway {
                        con_id: candidate.con_id,
                    },
                })
            }
        }
    }

    async fn dispatch_and_confirm_compositor_focus(
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
        plan: &PreflightFocusPlan,
        deadline_at: Instant,
    ) -> Result<(), AppError> {
        match (&pre.compositor, &plan.target) {
            (
                QualifiedCompositorEndpoint::Hyprland { socket_path, .. },
                CompositorFocusTarget::Hyprland { hex_address },
            ) => {
                let validated_hex = validate_hyprland_hex_address(hex_address)?;
                let dispatch_cmd = format!("dispatch focuswindow address:{validated_hex}");
                let (_peer_after_focus, dispatch_reply) = execute_hyprland_command(
                    config,
                    pre,
                    socket_path,
                    &dispatch_cmd,
                    Some(&plan.peer),
                    deadline_at,
                )
                .await?;
                verify_hyprland_dispatch_ok(&dispatch_reply)?;

                let (_peer_after_confirm, active_json) = execute_hyprland_command(
                    config,
                    pre,
                    socket_path,
                    "j/activewindow",
                    Some(&plan.peer),
                    deadline_at,
                )
                .await?;
                verify_hyprland_active_window(&active_json, pre.expected_pid, &validated_hex)
            }
            (
                QualifiedCompositorEndpoint::Sway { socket_path, .. },
                CompositorFocusTarget::Sway { con_id },
            ) => {
                if *con_id == 0 {
                    return Err(focus_unavailable_error());
                }
                let focus_cmd = format!("[con_id={con_id}] focus");
                let (_peer_after_focus, focus_reply) = execute_sway_ipc_command(
                    config,
                    pre,
                    socket_path,
                    SWAY_IPC_RUN_COMMAND,
                    focus_cmd.as_bytes(),
                    Some(&plan.peer),
                    deadline_at,
                )
                .await?;
                verify_sway_run_command_ok(&focus_reply)?;

                let (_peer_after_confirm, tree_json) = execute_sway_ipc_command(
                    config,
                    pre,
                    socket_path,
                    SWAY_IPC_GET_TREE,
                    b"",
                    Some(&plan.peer),
                    deadline_at,
                )
                .await?;
                verify_sway_focused_window(&tree_json, pre.expected_pid, *con_id)
            }
            _ => Err(focus_unavailable_error()),
        }
    }

    fn verify_connected_peer_and_singleton_state(
        stream: &UnixStream,
        config: &LinuxOwlActivatorConfig,
        pre: &PreConnectVerifiedState,
    ) -> Result<(), AppError> {
        let peer = stream.peer_cred().map_err(|_| app_unavailable_error())?;
        if peer.uid() != pre.expected_uid {
            return Err(app_unavailable_error());
        }
        let expected_pid_i32 =
            i32::try_from(pre.expected_pid).map_err(|_| app_unavailable_error())?;
        if peer.pid() != Some(expected_pid_i32) {
            return Err(app_unavailable_error());
        }

        let profile_meta =
            fs::symlink_metadata(&pre.profile_dir).map_err(|_| app_unavailable_error())?;
        if profile_meta.file_type().is_symlink()
            || !profile_meta.file_type().is_dir()
            || profile_meta.uid() != pre.expected_uid
            || (profile_meta.permissions().mode() & 0o022) != 0
            || profile_meta.dev() != pre.profile_dev
            || profile_meta.ino() != pre.profile_ino
        {
            return Err(app_unavailable_error());
        }

        let lock_now = read_owned_symlink_utf8(
            &pre.profile_dir.join(SINGLETON_LOCK_FILENAME),
            pre.expected_uid,
        )?;
        if lock_now != pre.lock_target {
            return Err(app_unavailable_error());
        }

        let profile_cookie_now = read_owned_symlink_utf8(
            &pre.profile_dir.join(SINGLETON_COOKIE_FILENAME),
            pre.expected_uid,
        )?;
        if !constant_time_bytes_eq(profile_cookie_now.as_bytes(), pre.cookie.as_bytes()) {
            return Err(app_unavailable_error());
        }

        let socket_link_now = read_owned_symlink_utf8(
            &pre.profile_dir.join(SINGLETON_SOCKET_FILENAME),
            pre.expected_uid,
        )?;
        if Path::new(&socket_link_now) != pre.socket_path {
            return Err(app_unavailable_error());
        }

        let socket_dir_meta =
            fs::symlink_metadata(&pre.socket_dir).map_err(|_| app_unavailable_error())?;
        if socket_dir_meta.file_type().is_symlink()
            || !socket_dir_meta.file_type().is_dir()
            || socket_dir_meta.uid() != pre.expected_uid
            || (socket_dir_meta.permissions().mode() & 0o077) != 0
            || socket_dir_meta.dev() != pre.socket_dir_dev
            || socket_dir_meta.ino() != pre.socket_dir_ino
        {
            return Err(app_unavailable_error());
        }

        let remote_cookie_now = read_owned_symlink_utf8(
            &pre.socket_dir.join(SINGLETON_COOKIE_FILENAME),
            pre.expected_uid,
        )?;
        if !constant_time_bytes_eq(remote_cookie_now.as_bytes(), pre.cookie.as_bytes()) {
            return Err(app_unavailable_error());
        }

        let socket_meta =
            fs::symlink_metadata(&pre.socket_path).map_err(|_| app_unavailable_error())?;
        if socket_meta.file_type().is_symlink()
            || !socket_meta.file_type().is_socket()
            || socket_meta.uid() != pre.expected_uid
            || (socket_meta.permissions().mode() & 0o022) != 0
            || socket_meta.dev() != pre.socket_dev
            || socket_meta.ino() != pre.socket_ino
        {
            return Err(app_unavailable_error());
        }

        let (boot_id_now, start_ticks_now, exe_now, _exe_utf8_now) =
            read_live_proc_identity(&config.proc_root, pre.expected_pid, pre.expected_uid)?;
        if boot_id_now != pre.boot_id
            || start_ticks_now != pre.start_ticks
            || exe_now != pre.canonical_exe
        {
            return Err(app_unavailable_error());
        }

        let exe_meta =
            fs::symlink_metadata(&pre.canonical_exe).map_err(|_| app_unavailable_error())?;
        if exe_meta.file_type().is_symlink()
            || !exe_meta.file_type().is_file()
            || exe_meta.dev() != pre.exe_dev
            || exe_meta.ino() != pre.exe_ino
        {
            return Err(app_unavailable_error());
        }

        Ok(())
    }
}

#[cfg(target_os = "linux")]
pub(crate) use linux_imp::LinuxOwlCodexActivator;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const TEST_UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn build_synthetic_asar(header_json: &str, payload: &[u8]) -> Vec<u8> {
        let json_bytes = header_json.as_bytes();
        let json_len = u32::try_from(json_bytes.len()).expect("json_len u32");
        let padding_len = (4 - (json_bytes.len() % 4)) % 4;
        let aligned_len = u32::try_from(json_bytes.len() + padding_len).expect("aligned u32");
        let header_pickle_payload = 4 + aligned_len;
        let header_buf_len = 4 + header_pickle_payload;

        let mut out = Vec::new();
        out.extend_from_slice(&4u32.to_le_bytes());
        out.extend_from_slice(&header_buf_len.to_le_bytes());
        out.extend_from_slice(&header_pickle_payload.to_le_bytes());
        out.extend_from_slice(&json_len.to_le_bytes());
        out.extend_from_slice(json_bytes);
        out.resize(out.len() + padding_len, 0u8);
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn owl_singleton_frame_matches_measured_protocol_without_nargs_or_trailing_nul() {
        let uri = format!("codex://threads/{TEST_UUID}");
        let frame = build_owl_singleton_frame(
            "/home/alice/projects/threadlane",
            "/nix/store/0123456789abcdef-codex/libexec/codex/owl-shell",
            &uri,
        )
        .expect("valid Owl singleton frame");

        let expected = format!(
            "START\0/home/alice/projects/threadlane\0/nix/store/0123456789abcdef-codex/libexec/codex/owl-shell\0codex://threads/{TEST_UUID}"
        );
        assert_eq!(frame, expected.as_bytes());
        assert_ne!(frame.last().copied(), Some(0));
        assert_eq!(frame.iter().filter(|&&b| b == 0).count(), 3);

        // Verify none of the bogus Electron-style `nargs` tokens (`"2"`, `"0"`) are injected.
        let parts: Vec<&[u8]> = frame.split(|&b| b == 0).collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0], b"START");
        assert_eq!(parts[1], b"/home/alice/projects/threadlane");
        assert_eq!(
            parts[2],
            b"/nix/store/0123456789abcdef-codex/libexec/codex/owl-shell"
        );
        assert_eq!(parts[3], uri.as_bytes());
    }

    #[test]
    fn owl_singleton_frame_rejects_oversized_or_malformed_inputs() {
        let uri = format!("codex://threads/{TEST_UUID}");
        let huge_cwd = format!("/{}", "a".repeat(MAX_OWL_SINGLETON_FRAME_BYTES));
        let err = build_owl_singleton_frame(&huge_cwd, "/nix/store/pkg/owl-shell", &uri)
            .expect_err("oversized frame must fail");
        assert_eq!(err.code, "codex_activation_frame_too_large");

        for bad_uri in [
            "https://example.com",
            "codex://threads/00000000-0000-0000-0000-000000000000",
            "codex://threads/550E8400-E29B-41D4-A716-446655440000",
            "codex://threads/550e8400-e29b-41d4-a716-446655440000?x=1",
        ] {
            assert!(
                build_owl_singleton_frame("/repo", "/nix/store/pkg/owl-shell", bad_uri).is_err()
            );
        }

        for bad_cwd in [
            "",
            "relative/path",
            "/repo/../escape",
            "/repo\0bad",
            "/repo\nbad",
        ] {
            assert!(
                build_owl_singleton_frame(bad_cwd, "/nix/store/pkg/owl-shell", &uri).is_err(),
                "expected bad cwd {bad_cwd:?} to fail"
            );
        }

        for bad_exe in [
            "",
            "/",
            "owl-shell",
            "/nix/store/../evil",
            "/nix/store/owl\0shell",
        ] {
            assert!(
                build_owl_singleton_frame("/repo", bad_exe, &uri).is_err(),
                "expected bad exe {bad_exe:?} to fail"
            );
        }
    }

    #[test]
    fn owl_runtime_marker_accepts_only_qualified_linux_x64_157_0_8103_39() {
        let valid_with_json_version = br#"{
            "schemaVersion": 1,
            "platform": "linux",
            "arch": "x64",
            "version": "157.0.8103.39",
            "extraBuildField": "ignored"
        }"#;
        verify_owl_runtime_marker_bytes(valid_with_json_version, None)
            .expect("qualified JSON version should pass");
        verify_owl_runtime_marker_bytes(valid_with_json_version, Some("157.0.8103.39\n"))
            .expect("matching JSON + sibling version should pass");

        let valid_with_sibling_only = br#"{
            "schemaVersion": 1,
            "platform": "linux",
            "arch": "x64"
        }"#;
        verify_owl_runtime_marker_bytes(valid_with_sibling_only, Some("157.0.8103.39\n"))
            .expect("qualified sibling version file should pass");

        // Missing version in both marker and sibling file fails closed.
        assert_eq!(
            verify_owl_runtime_marker_bytes(valid_with_sibling_only, None)
                .expect_err("missing version must fail")
                .code,
            "codex_runtime_unsupported"
        );

        // Version mismatch in sibling file fails closed even if JSON matches.
        assert_eq!(
            verify_owl_runtime_marker_bytes(valid_with_json_version, Some("158.0.0.0\n"))
                .expect_err("mismatched sibling version must fail")
                .code,
            "codex_runtime_unsupported"
        );

        for unqualified in [
            br#"{"schemaVersion":2,"platform":"linux","arch":"x64","version":"157.0.8103.39"}"#
                .as_slice(),
            br#"{"schemaVersion":1,"platform":"darwin","arch":"x64","version":"157.0.8103.39"}"#
                .as_slice(),
            br#"{"schemaVersion":1,"platform":"linux","arch":"arm64","version":"157.0.8103.39"}"#
                .as_slice(),
            br#"{"schemaVersion":1,"platform":"linux","arch":"x64","version":"34.2.0"}"#.as_slice(),
        ] {
            assert_eq!(
                verify_owl_runtime_marker_bytes(unqualified, None)
                    .expect_err("unqualified marker must fail")
                    .code,
                "codex_runtime_unsupported"
            );
        }
    }

    #[test]
    fn asar_package_reader_verifies_codex_package_and_rejects_unqualified_or_corrupt_archives() {
        let pkg_bytes = br#"{"name":"openai-codex-electron","version":"0.1.0"}"#;
        let header = format!(
            r#"{{"files":{{"other.js":{{"size":4,"offset":"0"}},"package.json":{{"size":{},"offset":"4"}}}}}}"#,
            pkg_bytes.len()
        );
        let mut payload = b"noop".to_vec();
        payload.extend_from_slice(pkg_bytes);
        let valid_asar = build_synthetic_asar(&header, &payload);
        let valid_len = valid_asar.len() as u64;
        verify_asar_package_identity_from_reader(&mut Cursor::new(valid_asar), valid_len)
            .expect("qualified Codex ASAR should pass");

        // Wrong package name is rejected.
        let wrong_pkg = br#"{"name":"some-other-electron-app","version":"1.0.0"}"#;
        let wrong_header = format!(
            r#"{{"files":{{"package.json":{{"size":{},"offset":"0"}}}}}}"#,
            wrong_pkg.len()
        );
        let wrong_asar = build_synthetic_asar(&wrong_header, wrong_pkg);
        let wrong_len = wrong_asar.len() as u64;
        assert_eq!(
            verify_asar_package_identity_from_reader(&mut Cursor::new(wrong_asar), wrong_len)
                .expect_err("wrong ASAR package name must fail")
                .code,
            "codex_runtime_unsupported"
        );

        // Unpacked package.json is rejected.
        let unpacked_header = r#"{"files":{"package.json":{"size":50,"unpacked":true}}}"#;
        let unpacked_asar = build_synthetic_asar(unpacked_header, pkg_bytes);
        let unpacked_len = unpacked_asar.len() as u64;
        assert!(verify_asar_package_identity_from_reader(
            &mut Cursor::new(unpacked_asar),
            unpacked_len
        )
        .is_err());

        // Out-of-bounds offset is rejected without panicking.
        let oob_header = format!(
            r#"{{"files":{{"package.json":{{"size":{},"offset":"999999"}}}}}}"#,
            pkg_bytes.len()
        );
        let oob_asar = build_synthetic_asar(&oob_header, pkg_bytes);
        let oob_len = oob_asar.len() as u64;
        assert!(
            verify_asar_package_identity_from_reader(&mut Cursor::new(oob_asar), oob_len).is_err()
        );
    }

    #[test]
    fn hyprland_protocol_validators_enforce_signature_hex_address_uniqueness_and_bounds() {
        for valid_sig in ["his_1", "4982374_1731340000_ab:cd+ef-01.2"] {
            assert_eq!(
                validate_hyprland_instance_signature(valid_sig).expect("valid signature"),
                valid_sig
            );
        }
        let oversized_sig = "a".repeat(MAX_HYPRLAND_INSTANCE_SIGNATURE_BYTES + 1);
        for bad_sig in [
            "",
            " ",
            " his_1",
            "his_1 ",
            ".",
            "..",
            "a..b",
            "sig/sub",
            "sig\\sub",
            "sig with space",
            "sig\0null",
            oversized_sig.as_str(),
        ] {
            assert_eq!(
                validate_hyprland_instance_signature(bad_sig)
                    .expect_err("invalid Hyprland signature must fail")
                    .code,
                "codex_focus_unavailable"
            );
        }

        for valid_addr in ["0x1", "0x55a1b2c3d4e0", "0xffffffffffffffff"] {
            assert_eq!(
                validate_hyprland_hex_address(valid_addr).expect("valid hex address"),
                valid_addr
            );
        }
        for bad_addr in [
            "",
            "55a1b2c3d4e0",
            "0x",
            "0x0",
            "0x000000",
            "0x55A1",
            "0x123456789abcdef01",
            "0x55a1;dispatch exec sh",
        ] {
            assert_eq!(
                validate_hyprland_hex_address(bad_addr)
                    .expect_err("invalid Hyprland hex address must fail")
                    .code,
                "codex_focus_unavailable"
            );
        }

        let clients_json = br#"[
            {"address":"0x1111","mapped":true,"hidden":false,"class":"Other","pid":900},
            {"address":"0x2222","mapped":false,"hidden":false,"class":"Codex","pid":4242},
            {"address":"0x3333","mapped":true,"hidden":true,"class":"Codex","pid":4242},
            {"address":"0x55a1b2c3d4e0","mapped":true,"hidden":false,"class":"Codex","pid":4242}
        ]"#;
        assert_eq!(
            parse_hyprland_clients_for_pid(clients_json, 4242).expect("unique mapped non-hidden"),
            "0x55a1b2c3d4e0"
        );
        assert_eq!(
            parse_hyprland_clients_for_pid(clients_json, 9999)
                .expect_err("missing target PID must fail")
                .code,
            "codex_focus_unavailable"
        );

        let ambig_json = br#"[
            {"address":"0x55a1","mapped":true,"hidden":false,"class":"Codex","pid":4242},
            {"address":"0x55a2","mapped":true,"hidden":false,"class":"Codex","pid":4242}
        ]"#;
        assert_eq!(
            parse_hyprland_clients_for_pid(ambig_json, 4242)
                .expect_err("two eligible windows must fail as ambiguous")
                .code,
            "codex_window_ambiguous"
        );

        let bad_class_json = br#"[
            {"address":"0x55a1","mapped":true,"hidden":false,"class":"   ","pid":4242}
        ]"#;
        assert_eq!(
            parse_hyprland_clients_for_pid(bad_class_json, 4242)
                .expect_err("blank class must fail")
                .code,
            "codex_focus_unavailable"
        );

        let oversized_buf = vec![b' '; MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES];
        assert_eq!(
            parse_hyprland_clients_for_pid(&oversized_buf, 4242)
                .expect_err("1 MiB response must fail")
                .code,
            "codex_focus_unavailable"
        );

        verify_hyprland_dispatch_ok(b"ok").expect("ok reply");
        verify_hyprland_dispatch_ok(b"ok\n").expect("ok newline reply");
        for bad_dispatch in [b"".as_slice(), b"unknown request".as_slice(), &[0xff, 0xfe]] {
            assert_eq!(
                verify_hyprland_dispatch_ok(bad_dispatch)
                    .expect_err("non-ok dispatch reply must fail")
                    .code,
                "codex_focus_unavailable"
            );
        }

        let active_ok =
            br#"{"address":"0x55a1b2c3d4e0","mapped":true,"hidden":false,"class":"Codex","pid":4242}"#;
        verify_hyprland_active_window(active_ok, 4242, "0x55a1b2c3d4e0")
            .expect("matching active window");
        assert_eq!(
            verify_hyprland_active_window(active_ok, 4242, "0x55a1b2c3d4e1")
                .expect_err("wrong active hex address must fail")
                .code,
            "codex_focus_unavailable"
        );
        assert_eq!(
            verify_hyprland_active_window(active_ok, 4243, "0x55a1b2c3d4e0")
                .expect_err("wrong active PID must fail")
                .code,
            "codex_focus_unavailable"
        );
    }

    #[test]
    fn sway_ipc_framing_and_tree_validators_enforce_bounds_and_unique_focus() {
        let encoded = encode_sway_ipc_frame(SWAY_IPC_RUN_COMMAND, b"[con_id=42] focus")
            .expect("encode sway frame");
        let mut hdr = [0u8; SWAY_IPC_HEADER_BYTES];
        hdr.copy_from_slice(&encoded[..SWAY_IPC_HEADER_BYTES]);
        let decoded_len =
            decode_sway_ipc_header(&hdr, SWAY_IPC_RUN_COMMAND).expect("decode sway header");
        assert_eq!(decoded_len, b"[con_id=42] focus".len());
        assert!(decode_sway_ipc_header(&hdr, SWAY_IPC_GET_TREE).is_err());

        let mut bad_magic = hdr;
        bad_magic[0] = b'X';
        assert!(decode_sway_ipc_header(&bad_magic, SWAY_IPC_RUN_COMMAND).is_err());

        let mut oversized_hdr = [0u8; SWAY_IPC_HEADER_BYTES];
        oversized_hdr[0..6].copy_from_slice(SWAY_IPC_MAGIC);
        let limit_u32 = u32::try_from(MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES).expect("limit u32");
        oversized_hdr[6..10].copy_from_slice(&limit_u32.to_le_bytes());
        oversized_hdr[10..14].copy_from_slice(&SWAY_IPC_GET_TREE.to_le_bytes());
        assert!(decode_sway_ipc_header(&oversized_hdr, SWAY_IPC_GET_TREE).is_err());

        let tree_unfocused = br#"{
            "id": 1,
            "focused": false,
            "nodes": [{"id": 7, "pid": 900, "focused": true, "app_id": "other"}],
            "floating_nodes": [
                {"id": 42, "pid": 4242, "focused": false, "window_properties": {"class": "Codex"}}
            ]
        }"#;
        let cand =
            parse_sway_tree_for_pid(tree_unfocused, 4242).expect("unique floating candidate");
        assert_eq!(
            cand,
            SwayWindowCandidate {
                con_id: 42,
                focused: false,
            }
        );
        assert!(verify_sway_focused_window(tree_unfocused, 4242, 42).is_err());

        let tree_focused = br#"{
            "id": 1,
            "focused": false,
            "nodes": [{"id": 42, "pid": 4242, "focused": true, "app_id": "Codex"}]
        }"#;
        verify_sway_focused_window(tree_focused, 4242, 42).expect("confirmed Sway focus");
        assert!(verify_sway_focused_window(tree_focused, 4242, 99).is_err());

        let tree_ambig = br#"{
            "id": 1,
            "nodes": [
                {"id": 42, "pid": 4242, "focused": true, "app_id": "Codex"},
                {"id": 43, "pid": 4242, "focused": false, "app_id": "Codex"}
            ]
        }"#;
        assert_eq!(
            parse_sway_tree_for_pid(tree_ambig, 4242)
                .expect_err("ambiguous Sway containers must fail")
                .code,
            "codex_window_ambiguous"
        );

        let mut deep_json = String::from(r#"{"id":42,"pid":4242,"focused":true,"app_id":"Codex"}"#);
        for _ in 0..=MAX_SWAY_TREE_DEPTH {
            deep_json = format!(r#"{{"id":1,"nodes":[{deep_json}]}}"#);
        }
        assert_eq!(
            parse_sway_tree_for_pid(deep_json.as_bytes(), 4242)
                .expect_err("excessive Sway tree depth must fail")
                .code,
            "codex_focus_unavailable"
        );

        verify_sway_run_command_ok(br#"[{"success":true}]"#).expect("Sway success true");
        for bad_cmd_reply in [
            br#"[]"#.as_slice(),
            br#"[{"success":false}]"#.as_slice(),
            br#"[{"success":true},{"success":false}]"#.as_slice(),
        ] {
            assert!(verify_sway_run_command_ok(bad_cmd_reply).is_err());
        }
    }

    #[cfg(target_os = "linux")]
    mod linux_socket_tests {
        use super::*;
        use crate::chat_navigation::{
            BoxActivationFuture, ChatNavigationController, ChatNavigationControllerConfig,
            CodexActivationPort, NavigationSubmissionOutcome, ResolvedCodexChatTarget,
            ValidatedCodexChatRequest, VerifiedChatMatchOrigin,
        };
        use linux_imp::{
            LinuxCompositorSource, LinuxOwlActivatorConfig, LinuxOwlCodexActivator,
            LinuxProfileSource, RuntimeOwnershipPolicy,
        };
        use serde::Serialize;
        use std::collections::VecDeque;
        use std::fs::{self, DirBuilder, OpenOptions};
        use std::io::Write as _;
        use std::os::unix::fs::{symlink, DirBuilderExt, MetadataExt, PermissionsExt};
        use std::os::unix::net::{UnixListener as StdUnixListener, UnixStream as StdUnixStream};
        use std::path::{Path, PathBuf};
        use std::process::{Child, ChildStdin, Command, Stdio};
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Instant;
        use tempfile::TempDir;
        use threadlane_core::observation::WorkspaceChatIdentity;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{UnixListener, UnixStream};

        const SECRET_COOKIE: &str = "9876543210987654321";
        const FAKE_COMPOSITOR_SPEC_ENV: &str = "THREADLANE_TEST_FAKE_COMPOSITOR_SPEC";
        const DEFAULT_HYPR_SIGNATURE: &str = "his_test_1";
        const DEFAULT_HYPR_WINDOW_ADDR: &str = "0x55a1b2c3d4e0";
        const DEFAULT_SWAY_CON_ID: u64 = 42;
        const COMPOSITOR_START_TICKS: u64 = 313_370;

        static COMPOSITOR_SPEC_SEQ: AtomicU64 = AtomicU64::new(1);

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct FakeHyprWindow {
            address: String,
            mapped: bool,
            hidden: bool,
            class: String,
            pid: i64,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct FakeSwayWindow {
            id: i64,
            pid: i64,
            focused: bool,
            app_id: Option<String>,
            xwayland_class: Option<String>,
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum FakeDispatchBehavior {
            FocusTargetAndReplyOk,
            KeepUnfocusedAndReplyOk,
            ReplyError,
            HangMs(u64),
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum FakeCompositorFlavor {
            Hyprland {
                clients: Vec<FakeHyprWindow>,
                initial_active: Option<FakeHyprWindow>,
                dispatch_behavior: FakeDispatchBehavior,
            },
            Sway {
                windows: Vec<FakeSwayWindow>,
                dispatch_behavior: FakeDispatchBehavior,
            },
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        enum ScriptedCompositorReply {
            CloseImmediately,
            RawBytes(Vec<u8>),
            RepeatedByte {
                byte: u8,
                count: usize,
            },
            SwayRawHeaderAndBody {
                magic: [u8; 6],
                declared_len: u32,
                msg_type: u32,
                body: Vec<u8>,
            },
            HangMs(u64),
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct FakeCompositorSpec {
            socket_path: PathBuf,
            socket_mode: u32,
            ready_socket_path: PathBuf,
            event_log_path: PathBuf,
            max_connections: usize,
            hard_lifetime_ms: u64,
            flavor: FakeCompositorFlavor,
            scripted_overrides: Vec<Option<ScriptedCompositorReply>>,
        }

        struct FakeCompositorRuntimeState {
            flavor: FakeCompositorFlavor,
            hypr_active: Option<FakeHyprWindow>,
            overrides: VecDeque<Option<ScriptedCompositorReply>>,
        }

        impl FakeCompositorRuntimeState {
            fn from_spec(spec: &FakeCompositorSpec) -> Self {
                let hypr_active = match &spec.flavor {
                    FakeCompositorFlavor::Hyprland { initial_active, .. } => initial_active.clone(),
                    FakeCompositorFlavor::Sway { .. } => None,
                };
                Self {
                    flavor: spec.flavor.clone(),
                    hypr_active,
                    overrides: spec.scripted_overrides.iter().cloned().collect(),
                }
            }
        }

        fn append_wire_event(event_log_path: &Path, line: &str) {
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(event_log_path)
            {
                let _ = file.write_all(line.as_bytes());
                let _ = file.write_all(b"\n");
                let _ = file.flush();
            }
        }

        async fn apply_scripted_reply(stream: &mut UnixStream, reply: ScriptedCompositorReply) {
            match reply {
                ScriptedCompositorReply::CloseImmediately => {
                    let _ = stream.shutdown().await;
                }
                ScriptedCompositorReply::RawBytes(bytes) => {
                    let _ = stream.write_all(&bytes).await;
                    let _ = stream.shutdown().await;
                }
                ScriptedCompositorReply::RepeatedByte { byte, count } => {
                    let chunk = [byte; 8_192];
                    let mut remaining = count;
                    while remaining > 0 {
                        let step = remaining.min(chunk.len());
                        if stream.write_all(&chunk[..step]).await.is_err() {
                            break;
                        }
                        remaining -= step;
                    }
                    let _ = stream.shutdown().await;
                }
                ScriptedCompositorReply::SwayRawHeaderAndBody {
                    magic,
                    declared_len,
                    msg_type,
                    body,
                } => {
                    let mut out = Vec::with_capacity(SWAY_IPC_HEADER_BYTES + body.len());
                    out.extend_from_slice(&magic);
                    out.extend_from_slice(&declared_len.to_le_bytes());
                    out.extend_from_slice(&msg_type.to_le_bytes());
                    out.extend_from_slice(&body);
                    let _ = stream.write_all(&out).await;
                    let _ = stream.shutdown().await;
                }
                ScriptedCompositorReply::HangMs(ms) => {
                    tokio::time::sleep(Duration::from_millis(ms.min(5_000))).await;
                }
            }
        }

        fn serialize_sway_tree(windows: &[FakeSwayWindow]) -> Vec<u8> {
            let nodes: Vec<serde_json::Value> = windows
                .iter()
                .map(|w| {
                    let wp = w
                        .xwayland_class
                        .as_ref()
                        .map(|cls| serde_json::json!({ "class": cls }));
                    serde_json::json!({
                        "id": w.id,
                        "pid": w.pid,
                        "focused": w.focused,
                        "app_id": w.app_id,
                        "window_properties": wp,
                        "nodes": [],
                        "floating_nodes": [],
                    })
                })
                .collect();
            serde_json::to_vec(&serde_json::json!({
                "id": 1,
                "focused": false,
                "nodes": nodes,
                "floating_nodes": [],
            }))
            .expect("serialize fake Sway tree")
        }

        async fn handle_fake_compositor_client(
            mut stream: UnixStream,
            state: Arc<Mutex<FakeCompositorRuntimeState>>,
            event_log_path: PathBuf,
        ) {
            let is_hyprland = {
                let guard = state.lock().expect("lock fake compositor state");
                matches!(guard.flavor, FakeCompositorFlavor::Hyprland { .. })
            };

            if is_hyprland {
                let mut cmd_buf = [0u8; 1024];
                let n = match tokio::time::timeout(
                    Duration::from_millis(500),
                    stream.read(&mut cmd_buf),
                )
                .await
                {
                    Ok(Ok(n)) if n > 0 => n,
                    _ => return,
                };
                let cmd = String::from_utf8_lossy(&cmd_buf[..n]).to_string();
                append_wire_event(&event_log_path, &format!("COMP:HYPR:{cmd}"));

                let scripted = {
                    let mut guard = state.lock().expect("lock fake compositor state");
                    guard.overrides.pop_front().flatten()
                };
                if let Some(reply) = scripted {
                    apply_scripted_reply(&mut stream, reply).await;
                    return;
                }

                enum HyprAction {
                    Reply(Vec<u8>),
                    Hang(u64),
                }

                let action = {
                    let mut guard = state.lock().expect("lock fake compositor state");
                    let hypr_active = guard.hypr_active.clone();
                    match &guard.flavor {
                        FakeCompositorFlavor::Hyprland {
                            clients,
                            dispatch_behavior,
                            ..
                        } => {
                            if cmd == "j/clients" {
                                HyprAction::Reply(
                                    serde_json::to_vec(clients).expect("serialize clients"),
                                )
                            } else if let Some(addr) =
                                cmd.strip_prefix("dispatch focuswindow address:")
                            {
                                match dispatch_behavior {
                                    FakeDispatchBehavior::FocusTargetAndReplyOk => {
                                        let matched = clients
                                            .iter()
                                            .find(|c| c.address == addr && c.mapped && !c.hidden)
                                            .cloned();
                                        if let Some(win) = matched {
                                            guard.hypr_active = Some(win);
                                        }
                                        HyprAction::Reply(b"ok".to_vec())
                                    }
                                    FakeDispatchBehavior::KeepUnfocusedAndReplyOk => {
                                        HyprAction::Reply(b"ok".to_vec())
                                    }
                                    FakeDispatchBehavior::ReplyError => {
                                        HyprAction::Reply(b"unknown request".to_vec())
                                    }
                                    FakeDispatchBehavior::HangMs(ms) => HyprAction::Hang(*ms),
                                }
                            } else if cmd == "j/activewindow" {
                                let bytes = match hypr_active {
                                    Some(win) => {
                                        serde_json::to_vec(&win).expect("serialize active window")
                                    }
                                    None => b"{}".to_vec(),
                                };
                                HyprAction::Reply(bytes)
                            } else {
                                HyprAction::Reply(b"err".to_vec())
                            }
                        }
                        FakeCompositorFlavor::Sway { .. } => unreachable!(),
                    }
                };

                match action {
                    HyprAction::Reply(bytes) => {
                        let _ = stream.write_all(&bytes).await;
                        let _ = stream.shutdown().await;
                    }
                    HyprAction::Hang(ms) => {
                        tokio::time::sleep(Duration::from_millis(ms.min(5_000))).await;
                    }
                }
            } else {
                let mut hdr = [0u8; SWAY_IPC_HEADER_BYTES];
                let hdr_res =
                    tokio::time::timeout(Duration::from_millis(500), stream.read_exact(&mut hdr))
                        .await;
                if !matches!(hdr_res, Ok(Ok(_))) {
                    return;
                }
                if &hdr[0..6] != SWAY_IPC_MAGIC {
                    return;
                }
                let len = u32::from_le_bytes([hdr[6], hdr[7], hdr[8], hdr[9]]) as usize;
                let msg_type = u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]);
                if len > 65_536 {
                    return;
                }
                let mut payload = vec![0u8; len];
                if len > 0 {
                    let read_res = tokio::time::timeout(
                        Duration::from_millis(500),
                        stream.read_exact(&mut payload),
                    )
                    .await;
                    if !matches!(read_res, Ok(Ok(_))) {
                        return;
                    }
                }
                let payload_str = String::from_utf8_lossy(&payload);
                append_wire_event(
                    &event_log_path,
                    &format!("COMP:SWAY:{msg_type}:{payload_str}"),
                );

                let scripted = {
                    let mut guard = state.lock().expect("lock fake compositor state");
                    guard.overrides.pop_front().flatten()
                };
                if let Some(reply) = scripted {
                    apply_scripted_reply(&mut stream, reply).await;
                    return;
                }

                enum SwayAction {
                    Frame(Vec<u8>),
                    Hang(u64),
                }

                let action = {
                    let mut guard = state.lock().expect("lock fake compositor state");
                    match &mut guard.flavor {
                        FakeCompositorFlavor::Sway {
                            windows,
                            dispatch_behavior,
                        } => {
                            if msg_type == SWAY_IPC_GET_TREE {
                                let tree = serialize_sway_tree(windows);
                                SwayAction::Frame(
                                    encode_sway_ipc_frame(SWAY_IPC_GET_TREE, &tree)
                                        .expect("encode GET_TREE"),
                                )
                            } else if msg_type == SWAY_IPC_RUN_COMMAND {
                                match dispatch_behavior {
                                    FakeDispatchBehavior::FocusTargetAndReplyOk => {
                                        let target_id = payload_str
                                            .strip_prefix("[con_id=")
                                            .and_then(|rest| rest.strip_suffix("] focus"))
                                            .and_then(|digits| digits.parse::<i64>().ok());
                                        if let Some(cid) = target_id {
                                            for win in windows.iter_mut() {
                                                win.focused = win.id == cid;
                                            }
                                        }
                                        SwayAction::Frame(
                                            encode_sway_ipc_frame(
                                                SWAY_IPC_RUN_COMMAND,
                                                br#"[{"success":true}]"#,
                                            )
                                            .expect("encode RUN_COMMAND ok"),
                                        )
                                    }
                                    FakeDispatchBehavior::KeepUnfocusedAndReplyOk => {
                                        SwayAction::Frame(
                                            encode_sway_ipc_frame(
                                                SWAY_IPC_RUN_COMMAND,
                                                br#"[{"success":true}]"#,
                                            )
                                            .expect("encode RUN_COMMAND ok"),
                                        )
                                    }
                                    FakeDispatchBehavior::ReplyError => SwayAction::Frame(
                                        encode_sway_ipc_frame(
                                            SWAY_IPC_RUN_COMMAND,
                                            br#"[{"success":false,"error":"focus rejected"}]"#,
                                        )
                                        .expect("encode RUN_COMMAND error"),
                                    ),
                                    FakeDispatchBehavior::HangMs(ms) => SwayAction::Hang(*ms),
                                }
                            } else {
                                SwayAction::Frame(
                                    encode_sway_ipc_frame(msg_type, b"{}").expect("encode empty"),
                                )
                            }
                        }
                        FakeCompositorFlavor::Hyprland { .. } => unreachable!(),
                    }
                };

                match action {
                    SwayAction::Frame(frame) => {
                        let _ = stream.write_all(&frame).await;
                        let _ = stream.shutdown().await;
                    }
                    SwayAction::Hang(ms) => {
                        tokio::time::sleep(Duration::from_millis(ms.min(5_000))).await;
                    }
                }
            }
        }

        #[tokio::test]
        #[ignore]
        async fn fixture_compositor_child_process_entrypoint() {
            let Some(spec_path_os) = std::env::var_os(FAKE_COMPOSITOR_SPEC_ENV) else {
                return;
            };
            let spec_bytes =
                fs::read(PathBuf::from(spec_path_os)).expect("read fake compositor spec");
            let spec: FakeCompositorSpec =
                serde_json::from_slice(&spec_bytes).expect("parse fake compositor spec");

            if spec.socket_path.exists() {
                let _ = fs::remove_file(&spec.socket_path);
            }
            let listener =
                UnixListener::bind(&spec.socket_path).expect("bind fake compositor socket");
            fs::set_permissions(
                &spec.socket_path,
                fs::Permissions::from_mode(spec.socket_mode),
            )
            .expect("chmod fake compositor socket");

            let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
            std::thread::spawn(move || {
                let mut byte = [0u8; 1];
                let _ = std::io::stdin().read(&mut byte);
                let _ = stop_tx.send(true);
            });

            {
                let mut ready =
                    StdUnixStream::connect(&spec.ready_socket_path).expect("connect ready socket");
                ready.write_all(b"OK").expect("write ready OK");
                ready.flush().expect("flush ready OK");
            }

            let state = Arc::new(Mutex::new(FakeCompositorRuntimeState::from_spec(&spec)));
            let hard_deadline =
                tokio::time::Instant::now() + Duration::from_millis(spec.hard_lifetime_ms);
            let mut workers = Vec::new();

            for _ in 0..spec.max_connections {
                if *stop_rx.borrow() {
                    break;
                }
                let accepted = tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = tokio::time::sleep_until(hard_deadline) => break,
                    res = listener.accept() => res,
                };
                let Ok((stream, _)) = accepted else {
                    break;
                };
                let mut worker_stop = stop_rx.clone();
                let worker_state = Arc::clone(&state);
                let log_path = spec.event_log_path.clone();
                workers.push(tokio::spawn(async move {
                    tokio::select! {
                        _ = worker_stop.changed() => {}
                        _ = tokio::time::sleep_until(hard_deadline) => {}
                        () = handle_fake_compositor_client(stream, worker_state, log_path) => {}
                    }
                }));
            }

            for worker in workers {
                let _ = tokio::time::timeout(Duration::from_millis(100), worker).await;
            }
        }

        struct FakeCompositorChild {
            child: Child,
            stdin: Option<ChildStdin>,
            pid: u32,
        }

        impl FakeCompositorChild {
            fn spawn(base_dir: &Path, mut spec: FakeCompositorSpec) -> Self {
                let seq = COMPOSITOR_SPEC_SEQ.fetch_add(1, Ordering::Relaxed);
                let spec_path = base_dir.join(format!("comp_spec_{seq}.json"));
                let ready_sock_path = base_dir.join(format!("comp_rdy_{seq}.sock"));
                spec.ready_socket_path = ready_sock_path.clone();

                let ready_listener =
                    StdUnixListener::bind(&ready_sock_path).expect("bind ready listener");
                ready_listener
                    .set_nonblocking(true)
                    .expect("set_nonblocking ready listener");

                let encoded_spec = serde_json::to_vec(&spec).expect("encode fake compositor spec");
                fs::write(&spec_path, encoded_spec).expect("write fake compositor spec");

                let full_mod = module_path!();
                let rel_mod = full_mod.split_once("::").map_or(full_mod, |(_, rest)| rest);
                let exact_test = format!("{rel_mod}::fixture_compositor_child_process_entrypoint");

                let mut child = Command::new(std::env::current_exe().expect("current test binary"))
                    .arg("--exact")
                    .arg("--ignored")
                    .arg("--quiet")
                    .arg("--test-threads=1")
                    .arg(&exact_test)
                    .env_remove("WAYLAND_DISPLAY")
                    .env_remove("DISPLAY")
                    .env_remove("HYPRLAND_INSTANCE_SIGNATURE")
                    .env_remove("SWAYSOCK")
                    .env(FAKE_COMPOSITOR_SPEC_ENV, &spec_path)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("spawn fake compositor child");

                let pid = child.id();
                let stdin = child.stdin.take();

                let wait_deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    match ready_listener.accept() {
                        Ok((mut ready_stream, _)) => {
                            ready_stream
                                .set_read_timeout(Some(Duration::from_secs(2)))
                                .expect("ready read timeout");
                            let mut ok_buf = [0u8; 2];
                            ready_stream.read_exact(&mut ok_buf).expect("read ready OK");
                            assert_eq!(&ok_buf, b"OK");
                            break;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            if let Some(status) = child.try_wait().expect("try_wait child") {
                                let mut out_bytes = Vec::new();
                                let mut err_bytes = Vec::new();
                                if let Some(mut so) = child.stdout.take() {
                                    let _ = so.read_to_end(&mut out_bytes);
                                }
                                if let Some(mut se) = child.stderr.take() {
                                    let _ = se.read_to_end(&mut err_bytes);
                                }
                                panic!(
                                    "fake compositor child exited early ({status}): stdout={} stderr={}",
                                    String::from_utf8_lossy(&out_bytes),
                                    String::from_utf8_lossy(&err_bytes),
                                );
                            }
                            assert!(
                                Instant::now() < wait_deadline,
                                "timed out waiting for fake compositor child readiness"
                            );
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(err) => panic!("ready_listener accept failed: {err}"),
                    }
                }
                let _ = fs::remove_file(&ready_sock_path);

                Self { child, stdin, pid }
            }

            fn shutdown(&mut self) {
                drop(self.stdin.take());
                let wait_until = Instant::now() + Duration::from_millis(40);
                loop {
                    match self.child.try_wait() {
                        Ok(Some(_)) => return,
                        Ok(None) if Instant::now() < wait_until => {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        _ => break,
                    }
                }
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }

        impl Drop for FakeCompositorChild {
            fn drop(&mut self) {
                self.shutdown();
            }
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum ActiveFixtureCompositor {
            Hyprland,
            Sway,
        }

        struct DisposableOwlHarness {
            _tmp: TempDir,
            base_dir: PathBuf,
            xdg_dir: PathBuf,
            profile_dir: PathBuf,
            socket_dir: PathBuf,
            socket_path: PathBuf,
            proc_root: PathBuf,
            runtime_dir: PathBuf,
            exe_path: PathBuf,
            comp_xdg_runtime_dir: PathBuf,
            hypr_socket_path: PathBuf,
            sway_socket_path: PathBuf,
            comp_hypr_exe_path: PathBuf,
            comp_sway_exe_path: PathBuf,
            event_log_path: PathBuf,
            owner_uid: u32,
            self_pid: u32,
            active_compositor: ActiveFixtureCompositor,
            compositor_child: Option<FakeCompositorChild>,
        }

        impl Drop for DisposableOwlHarness {
            fn drop(&mut self) {
                if let Some(mut comp) = self.compositor_child.take() {
                    comp.shutdown();
                }
                restore_writable_recursive(&self.base_dir);
            }
        }

        fn restore_writable_recursive(path: &Path) {
            if let Ok(meta) = fs::symlink_metadata(path) {
                if meta.file_type().is_dir() {
                    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
                    if let Ok(entries) = fs::read_dir(path) {
                        for entry in entries.flatten() {
                            restore_writable_recursive(&entry.path());
                        }
                    }
                } else if meta.file_type().is_file() {
                    let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
                }
            }
        }

        fn build_codex_asar_bytes() -> Vec<u8> {
            let pkg_bytes = br#"{"name":"openai-codex-electron","version":"1.0.0"}"#;
            let header = format!(
                r#"{{"files":{{"package.json":{{"size":{},"offset":"0"}}}}}}"#,
                pkg_bytes.len()
            );
            build_synthetic_asar(&header, pkg_bytes)
        }

        impl DisposableOwlHarness {
            fn new() -> Self {
                let self_pid = std::process::id();
                Self::new_with_flavor(Self::default_hyprland_flavor(self_pid))
            }

            fn new_sway() -> Self {
                let self_pid = std::process::id();
                Self::new_with_flavor(Self::default_sway_flavor(self_pid))
            }

            fn default_hyprland_flavor(codex_pid: u32) -> FakeCompositorFlavor {
                let other_win = FakeHyprWindow {
                    address: "0x1000a0".to_string(),
                    mapped: true,
                    hidden: false,
                    class: "Threadlane".to_string(),
                    pid: i64::from(codex_pid) + 500,
                };
                let codex_win = FakeHyprWindow {
                    address: DEFAULT_HYPR_WINDOW_ADDR.to_string(),
                    mapped: true,
                    hidden: false,
                    class: "Codex".to_string(),
                    pid: i64::from(codex_pid),
                };
                FakeCompositorFlavor::Hyprland {
                    clients: vec![other_win.clone(), codex_win],
                    // Start unfocused so URI ACK alone cannot pass active-window verification.
                    initial_active: Some(other_win),
                    dispatch_behavior: FakeDispatchBehavior::FocusTargetAndReplyOk,
                }
            }

            fn default_sway_flavor(codex_pid: u32) -> FakeCompositorFlavor {
                FakeCompositorFlavor::Sway {
                    windows: vec![
                        FakeSwayWindow {
                            id: 7,
                            pid: i64::from(codex_pid) + 500,
                            focused: true,
                            app_id: Some("threadlane".to_string()),
                            xwayland_class: None,
                        },
                        FakeSwayWindow {
                            id: i64::try_from(DEFAULT_SWAY_CON_ID).expect("i64 con_id"),
                            pid: i64::from(codex_pid),
                            focused: false,
                            app_id: Some("Codex".to_string()),
                            xwayland_class: None,
                        },
                    ],
                    dispatch_behavior: FakeDispatchBehavior::FocusTargetAndReplyOk,
                }
            }

            fn new_with_flavor(flavor: FakeCompositorFlavor) -> Self {
                // Keep socket paths short (< 100/107 bytes) under `/tmp` inside a strict `0700`
                // owned root while sealing the simulated Owl runtime subtree read-only.
                let tmp = TempDir::new_in("/tmp").expect("tempdir in /tmp");
                let base_dir = tmp.path().to_path_buf();
                fs::set_permissions(&base_dir, fs::Permissions::from_mode(0o700))
                    .expect("chmod 0700 base");

                let owner_uid = fs::metadata("/proc/self").expect("proc self").uid();
                let self_pid = std::process::id();

                let mk_dir = |rel: &str, mode: u32| -> PathBuf {
                    let p = base_dir.join(rel);
                    DirBuilder::new()
                        .mode(mode)
                        .recursive(true)
                        .create(&p)
                        .expect("mkdir");
                    fs::set_permissions(&p, fs::Permissions::from_mode(mode)).expect("chmod dir");
                    p
                };

                let xdg_dir = mk_dir("xdg", 0o700);
                let profile_dir = mk_dir("xdg/Codex", 0o700);
                let socket_dir = mk_dir("sock", 0o700);
                let socket_path = socket_dir.join("SingletonSocket");

                let comp_xdg_runtime_dir = mk_dir("xrd", 0o700);
                let _hypr_root = mk_dir("xrd/hypr", 0o700);
                let hypr_inst_dir = mk_dir(&format!("xrd/hypr/{DEFAULT_HYPR_SIGNATURE}"), 0o700);
                let hypr_socket_path = hypr_inst_dir.join(".socket.sock");

                let sway_dir = mk_dir("sway", 0o700);
                let sway_socket_path = sway_dir.join("sway-ipc.sock");

                let comp_bin_dir = mk_dir("cbin", 0o700);
                let comp_hypr_exe_path = comp_bin_dir.join(".Hyprland-wrapped");
                fs::write(&comp_hypr_exe_path, b"").expect("write fake Hyprland exe");
                let comp_sway_exe_path = comp_bin_dir.join(".sway-wrapped");
                fs::write(&comp_sway_exe_path, b"").expect("write fake Sway exe");

                let event_log_path = base_dir.join("wire_events.log");

                let proc_root = mk_dir("proc", 0o700);
                let proc_sys_dir = mk_dir("proc/sys/kernel/random", 0o700);
                fs::write(
                    proc_sys_dir.join("boot_id"),
                    "a1b2c3d4-e5f6-4789-8012-3456789abcde\n",
                )
                .expect("write boot_id");

                let proc_pid_dir = mk_dir(&format!("proc/{self_pid}"), 0o700);
                let stat_line = format!(
                    "{self_pid} (owl-shell) S 1 {self_pid} {self_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 424242 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                );
                fs::write(proc_pid_dir.join("stat"), stat_line).expect("write stat");

                let runtime_dir = mk_dir("rt", 0o755);
                let resources_dir = mk_dir("rt/resources", 0o755);
                let exe_path = runtime_dir.join("owl-shell");
                fs::write(&exe_path, b"ELF-OWL-STUB").expect("write fake exe");
                fs::set_permissions(&exe_path, fs::Permissions::from_mode(0o555))
                    .expect("chmod 0555 exe");

                let marker_path = runtime_dir.join(OWL_RUNTIME_MARKER_FILENAME);
                fs::write(
                    &marker_path,
                    br#"{"schemaVersion":1,"platform":"linux","arch":"x64","version":"157.0.8103.39"}"#,
                )
                .expect("write owl-shell-runtime.json");
                fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o444))
                    .expect("chmod 0444 marker");

                let asar_path = resources_dir.join("app.asar");
                fs::write(&asar_path, build_codex_asar_bytes()).expect("write app.asar");
                fs::set_permissions(&asar_path, fs::Permissions::from_mode(0o444))
                    .expect("chmod 0444 asar");

                fs::set_permissions(&resources_dir, fs::Permissions::from_mode(0o555))
                    .expect("chmod 0555 resources");
                fs::set_permissions(&runtime_dir, fs::Permissions::from_mode(0o555))
                    .expect("chmod 0555 runtime");

                symlink(&exe_path, proc_pid_dir.join("exe")).expect("symlink proc exe");
                let mut cmdline = exe_path.as_os_str().as_encoded_bytes().to_vec();
                cmdline.push(0);
                fs::write(proc_pid_dir.join("cmdline"), cmdline).expect("write cmdline");

                symlink(
                    format!("nixos-host-{self_pid}"),
                    profile_dir.join("SingletonLock"),
                )
                .expect("symlink SingletonLock");
                symlink(SECRET_COOKIE, profile_dir.join("SingletonCookie"))
                    .expect("symlink profile SingletonCookie");
                symlink(&socket_path, profile_dir.join("SingletonSocket"))
                    .expect("symlink SingletonSocket");
                symlink(SECRET_COOKIE, socket_dir.join("SingletonCookie"))
                    .expect("symlink socket SingletonCookie");

                let active_compositor = match &flavor {
                    FakeCompositorFlavor::Hyprland { .. } => ActiveFixtureCompositor::Hyprland,
                    FakeCompositorFlavor::Sway { .. } => ActiveFixtureCompositor::Sway,
                };

                let mut harness = Self {
                    _tmp: tmp,
                    base_dir,
                    xdg_dir,
                    profile_dir,
                    socket_dir,
                    socket_path,
                    proc_root,
                    runtime_dir,
                    exe_path,
                    comp_xdg_runtime_dir,
                    hypr_socket_path,
                    sway_socket_path,
                    comp_hypr_exe_path,
                    comp_sway_exe_path,
                    event_log_path,
                    owner_uid,
                    self_pid,
                    active_compositor,
                    compositor_child: None,
                };
                harness.restart_compositor(flavor, Vec::new(), 0o600);
                harness
            }

            fn restart_compositor(
                &mut self,
                flavor: FakeCompositorFlavor,
                scripted_overrides: Vec<Option<ScriptedCompositorReply>>,
                socket_mode: u32,
            ) -> u32 {
                if let Some(mut existing) = self.compositor_child.take() {
                    existing.shutdown();
                }
                let (comp_sock, comm, exe_target) = match &flavor {
                    FakeCompositorFlavor::Hyprland { .. } => {
                        self.active_compositor = ActiveFixtureCompositor::Hyprland;
                        (
                            self.hypr_socket_path.clone(),
                            ".Hyprland-wrapp",
                            self.comp_hypr_exe_path.clone(),
                        )
                    }
                    FakeCompositorFlavor::Sway { .. } => {
                        self.active_compositor = ActiveFixtureCompositor::Sway;
                        (
                            self.sway_socket_path.clone(),
                            ".sway-wrapped",
                            self.comp_sway_exe_path.clone(),
                        )
                    }
                };
                let spec = FakeCompositorSpec {
                    socket_path: comp_sock,
                    socket_mode,
                    ready_socket_path: PathBuf::new(),
                    event_log_path: self.event_log_path.clone(),
                    max_connections: 32,
                    hard_lifetime_ms: 8_000,
                    flavor,
                    scripted_overrides,
                };
                let child = FakeCompositorChild::spawn(&self.base_dir, spec);
                let comp_pid = child.pid;
                assert_ne!(
                    comp_pid, self.self_pid,
                    "compositor fixture child PID must differ from simulated Codex PID"
                );
                self.write_compositor_proc_identity(
                    comp_pid,
                    comm,
                    Some(&exe_target),
                    COMPOSITOR_START_TICKS,
                );
                self.compositor_child = Some(child);
                comp_pid
            }

            fn compositor_pid(&self) -> u32 {
                self.compositor_child
                    .as_ref()
                    .expect("active compositor child")
                    .pid
            }

            fn write_compositor_proc_identity(
                &self,
                comp_pid: u32,
                comm: &str,
                exe_target: Option<&Path>,
                start_ticks: u64,
            ) {
                let dir = self.proc_root.join(comp_pid.to_string());
                if !dir.exists() {
                    DirBuilder::new()
                        .mode(0o700)
                        .recursive(true)
                        .create(&dir)
                        .expect("create proc/<comp_pid>");
                }
                fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
                    .expect("chmod proc/<comp_pid>");
                let stat_line = format!(
                    "{comp_pid} ({comm}) S 1 {comp_pid} {comp_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 {start_ticks} 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                );
                fs::write(dir.join("stat"), stat_line).expect("write comp stat");
                fs::write(dir.join("comm"), format!("{comm}\n")).expect("write comp comm");
                let exe_link = dir.join("exe");
                let _ = fs::remove_file(&exe_link);
                if let Some(target) = exe_target {
                    symlink(target, exe_link).expect("symlink comp exe");
                }
            }

            fn bind_singleton_listener(&self) -> UnixListener {
                if self.socket_path.exists() {
                    let _ = fs::remove_file(&self.socket_path);
                }
                let listener = UnixListener::bind(&self.socket_path).expect("bind SingletonSocket");
                fs::set_permissions(&self.socket_path, fs::Permissions::from_mode(0o600))
                    .expect("chmod SingletonSocket 0600");
                listener
            }

            fn wire_events(&self) -> Vec<String> {
                match fs::read_to_string(&self.event_log_path) {
                    Ok(raw) => raw.lines().map(str::to_string).collect(),
                    Err(_) => Vec::new(),
                }
            }

            fn clear_wire_events(&self) {
                let _ = fs::remove_file(&self.event_log_path);
            }

            fn compositor_source(&self, simulate_exe_eacces: bool) -> LinuxCompositorSource {
                match self.active_compositor {
                    ActiveFixtureCompositor::Hyprland => LinuxCompositorSource::ExplicitEnv {
                        xdg_runtime_dir: Some(self.comp_xdg_runtime_dir.clone().into_os_string()),
                        hyprland_instance_signature: Some(DEFAULT_HYPR_SIGNATURE.into()),
                        swaysock: None,
                        compositor_proc_root: Some(self.proc_root.clone()),
                        simulate_exe_permission_denied: simulate_exe_eacces,
                        allow_same_pid_as_target: false,
                    },
                    ActiveFixtureCompositor::Sway => LinuxCompositorSource::ExplicitEnv {
                        xdg_runtime_dir: None,
                        hyprland_instance_signature: None,
                        swaysock: Some(self.sway_socket_path.clone().into_os_string()),
                        compositor_proc_root: Some(self.proc_root.clone()),
                        simulate_exe_permission_denied: simulate_exe_eacces,
                        allow_same_pid_as_target: false,
                    },
                }
            }

            fn config(&self) -> LinuxOwlActivatorConfig {
                LinuxOwlActivatorConfig {
                    deadline: Duration::from_millis(1_200),
                    step_timeout: Duration::from_millis(450),
                    proc_root: self.proc_root.clone(),
                    profile_source: LinuxProfileSource::ExplicitEnv {
                        xdg_config_home: Some(self.xdg_dir.clone().into_os_string()),
                        home: None,
                        extra_candidates: Vec::new(),
                    },
                    compositor_source: self.compositor_source(false),
                    ownership_policy: RuntimeOwnershipPolicy::TestOwner {
                        owner_uid: self.owner_uid,
                        require_read_only: true,
                    },
                    post_connect_hook: None,
                }
            }

            fn sample_request() -> CodexActivationRequest {
                let identity = WorkspaceChatIdentity::new(
                    "src_local",
                    "bnd_alpha",
                    TEST_UUID,
                    "/workspace/project-alpha",
                )
                .expect("valid WorkspaceChatIdentity");
                CodexActivationRequest {
                    chat_identity: identity,
                    canonical_uri: format!("codex://threads/{TEST_UUID}"),
                }
            }
        }

        struct TestLinuxPort(LinuxOwlCodexActivator);

        impl CodexActivationPort for TestLinuxPort {
            fn activate<'a>(
                &'a self,
                request: &'a CodexActivationRequest,
            ) -> BoxActivationFuture<'a> {
                Box::pin(self.0.activate(request))
            }
        }

        async fn join_bounded<T>(handle: tokio::task::JoinHandle<T>, label: &str) -> T {
            tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .unwrap_or_else(|_| panic!("timed out joining {label}"))
                .unwrap_or_else(|err| panic!("task {label} panicked or aborted: {err}"))
        }

        async fn read_singleton_frame_bounded(stream: &mut UnixStream) -> Vec<u8> {
            let mut incoming = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut incoming))
                .await
                .expect("timed out reading singleton frame until client write EOF")
                .expect("read singleton frame");
            incoming
        }

        #[tokio::test]
        async fn hyprland_happy_path_preflights_sends_exact_owl_frame_once_focuses_and_confirms() {
            let harness = DisposableOwlHarness::new();
            assert_ne!(harness.compositor_pid(), harness.self_pid);
            let listener = harness.bind_singleton_listener();

            let captured = Arc::new(Mutex::new(Vec::<u8>::new()));
            let captured_server = Arc::clone(&captured);
            let event_log = harness.event_log_path.clone();

            let server = tokio::spawn(async move {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept())
                        .await
                        .expect("accept timeout")
                        .expect("accept client");
                let mut incoming = read_singleton_frame_bounded(&mut stream).await;
                append_wire_event(&event_log, "OWL_SINGLETON:START");
                *captured_server.lock().expect("lock captured") = std::mem::take(&mut incoming);
                stream.write_all(b"ACK").await.expect("write ACK");
                stream.shutdown().await.expect("shutdown server");
            });

            let activator = LinuxOwlCodexActivator::with_config(harness.config());
            let outcome = activator
                .activate(&DisposableOwlHarness::sample_request())
                .await;
            assert_eq!(outcome, ActivationPortOutcome::Submitted);

            join_bounded(server, "hyprland singleton server").await;
            let expected_frame = format!(
                "START\0/workspace/project-alpha\0{}\0codex://threads/{TEST_UUID}",
                harness.exe_path.to_str().expect("utf8 exe")
            );
            assert_eq!(
                captured.lock().expect("lock").as_slice(),
                expected_frame.as_bytes()
            );
            let expected_focus =
                format!("COMP:HYPR:dispatch focuswindow address:{DEFAULT_HYPR_WINDOW_ADDR}");
            assert_eq!(
                harness.wire_events(),
                vec![
                    "COMP:HYPR:j/clients",
                    "OWL_SINGLETON:START",
                    expected_focus.as_str(),
                    "COMP:HYPR:j/activewindow",
                ]
            );
        }

        #[tokio::test]
        async fn sway_happy_path_preflights_tree_sends_exact_owl_frame_once_focuses_and_confirms() {
            let harness = DisposableOwlHarness::new_sway();
            assert_ne!(harness.compositor_pid(), harness.self_pid);
            let listener = harness.bind_singleton_listener();

            let captured = Arc::new(Mutex::new(Vec::<u8>::new()));
            let captured_server = Arc::clone(&captured);
            let event_log = harness.event_log_path.clone();

            let server = tokio::spawn(async move {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept())
                        .await
                        .expect("accept timeout")
                        .expect("accept client");
                let incoming = read_singleton_frame_bounded(&mut stream).await;
                append_wire_event(&event_log, "OWL_SINGLETON:START");
                *captured_server.lock().expect("lock captured") = incoming;
                stream.write_all(b"ACK").await.expect("write ACK");
                stream.shutdown().await.expect("shutdown server");
            });

            let activator = LinuxOwlCodexActivator::with_config(harness.config());
            let outcome = activator
                .activate(&DisposableOwlHarness::sample_request())
                .await;
            assert_eq!(outcome, ActivationPortOutcome::Submitted);

            join_bounded(server, "sway singleton server").await;
            let expected_frame = format!(
                "START\0/workspace/project-alpha\0{}\0codex://threads/{TEST_UUID}",
                harness.exe_path.to_str().expect("utf8 exe")
            );
            assert_eq!(
                captured.lock().expect("lock").as_slice(),
                expected_frame.as_bytes()
            );
            let expected_focus = format!("COMP:SWAY:0:[con_id={DEFAULT_SWAY_CON_ID}] focus");
            assert_eq!(
                harness.wire_events(),
                vec![
                    "COMP:SWAY:4:",
                    "OWL_SINGLETON:START",
                    expected_focus.as_str(),
                    "COMP:SWAY:4:",
                ]
            );
        }

        #[tokio::test]
        async fn nondumpable_wrapped_hyprland_peer_succeeds_while_foreign_or_spoofed_peers_fail() {
            // 1. Legitimate non-dumpable `.Hyprland-wrapp` (`EACCES` on `/proc/<pid>/exe`) succeeds.
            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let mut cfg = harness.config();
                cfg.compositor_source = harness.compositor_source(true);
                let activator = LinuxOwlCodexActivator::with_config(cfg);
                assert_eq!(
                    activator
                        .activate(&DisposableOwlHarness::sample_request())
                        .await,
                    ActivationPortOutcome::Submitted
                );
                join_bounded(server, "wrapped hyprland singleton server").await;
            }

            // 2. Foreign compositor `comm` (`python3`), bad `exe`, or `ENOENT` on `exe` fails pre-send.
            {
                let harness = DisposableOwlHarness::new();
                let _listener = harness.bind_singleton_listener();
                let comp_pid = harness.compositor_pid();

                // Foreign `comm` even with `simulate_exe_permission_denied: true`.
                harness.write_compositor_proc_identity(
                    comp_pid,
                    "python3",
                    None,
                    COMPOSITOR_START_TICKS,
                );
                let mut cfg = harness.config();
                cfg.compositor_source = harness.compositor_source(true);
                match LinuxOwlCodexActivator::with_config(cfg)
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => {
                        panic!("expected codex_focus_unavailable for foreign comm, got {other:?}")
                    }
                }

                // Valid `comm` (`.Hyprland-wrapp`) but missing `/proc/<pid>/exe` (`ENOENT`, not `EACCES`).
                harness.write_compositor_proc_identity(
                    comp_pid,
                    ".Hyprland-wrapp",
                    None,
                    COMPOSITOR_START_TICKS,
                );
                match LinuxOwlCodexActivator::with_config(harness.config())
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => {
                        panic!("expected codex_focus_unavailable on ENOENT exe, got {other:?}")
                    }
                }

                // Valid `comm` (`Hyprland`) with foreign executable basename (`evil-wm`).
                let foreign_exe = harness.base_dir.join("cbin/evil-wm");
                fs::write(&foreign_exe, b"").expect("write foreign exe");
                harness.write_compositor_proc_identity(
                    comp_pid,
                    "Hyprland",
                    Some(&foreign_exe),
                    COMPOSITOR_START_TICKS,
                );
                match LinuxOwlCodexActivator::with_config(harness.config())
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => {
                        panic!("expected codex_focus_unavailable on foreign exe, got {other:?}")
                    }
                }
            }

            // 3. Sway rejects `EACCES` on `/proc/<pid>/exe` before send.
            {
                let harness = DisposableOwlHarness::new_sway();
                let _listener = harness.bind_singleton_listener();
                let mut cfg = harness.config();
                cfg.compositor_source = harness.compositor_source(true);
                match LinuxOwlCodexActivator::with_config(cfg)
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => panic!("expected Sway EACCES to fail before send, got {other:?}"),
                }
            }

            // 4. Target Codex PID spoofing its own compositor socket (`peer_pid == expected_pid` with
            //    `allow_same_pid_as_target == false`) is rejected before send.
            {
                let mut harness = DisposableOwlHarness::new();
                if let Some(mut child) = harness.compositor_child.take() {
                    child.shutdown();
                }
                let _ = fs::remove_file(&harness.hypr_socket_path);
                let spoof_comp_listener =
                    UnixListener::bind(&harness.hypr_socket_path).expect("bind spoof comp");
                fs::set_permissions(&harness.hypr_socket_path, fs::Permissions::from_mode(0o600))
                    .expect("chmod spoof comp");
                let _singleton_listener = harness.bind_singleton_listener();

                let spoof_task = tokio::spawn(async move {
                    let accepted = tokio::time::timeout(
                        Duration::from_millis(500),
                        spoof_comp_listener.accept(),
                    )
                    .await;
                    if let Ok(Ok((mut s, _))) = accepted {
                        let mut buf = [0u8; 64];
                        let _ = tokio::time::timeout(Duration::from_millis(100), s.read(&mut buf))
                            .await;
                    }
                });

                match LinuxOwlCodexActivator::with_config(harness.config())
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => panic!("expected same-PID compositor spoof to fail, got {other:?}"),
                }
                join_bounded(spoof_task, "spoof compositor listener").await;
            }
        }

        #[tokio::test]
        async fn explicit_xdg_config_home_precedence_and_ambiguous_or_missing_profiles_behave_strictly(
        ) {
            let harness = DisposableOwlHarness::new();

            // 1. No bound SingletonSocket -> codex_app_unavailable before send.
            match LinuxOwlCodexActivator::with_config(harness.config())
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_app_unavailable");
                    assert!(!err.message.contains(SECRET_COOKIE));
                }
                other => panic!("expected codex_app_unavailable, got {other:?}"),
            }

            // 2. Explicit `XDG_CONFIG_HOME` is authoritative even when a stale `$HOME/.config/Codex`
            //    exists with a dead/foreign lock and stale cookie.
            let stale_home = harness.base_dir.join("stale-home");
            let stale_codex = stale_home.join(".config").join("Codex");
            DirBuilder::new()
                .mode(0o700)
                .recursive(true)
                .create(&stale_codex)
                .expect("create stale_codex");
            fs::set_permissions(&stale_codex, fs::Permissions::from_mode(0o700))
                .expect("chmod stale_codex");
            symlink("dead-host-999999", stale_codex.join("SingletonLock"))
                .expect("stale SingletonLock");
            symlink("stale_cookie_value", stale_codex.join("SingletonCookie"))
                .expect("stale SingletonCookie");

            let listener = harness.bind_singleton_listener();
            let server = tokio::spawn(async move {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(3), listener.accept())
                        .await
                        .expect("accept timeout")
                        .expect("accept");
                let _incoming = read_singleton_frame_bounded(&mut stream).await;
                stream.write_all(b"ACK").await.expect("write ACK");
                stream.shutdown().await.expect("shutdown");
            });

            let mut xdg_precedence_cfg = harness.config();
            xdg_precedence_cfg.profile_source = LinuxProfileSource::ExplicitEnv {
                xdg_config_home: Some(harness.xdg_dir.clone().into_os_string()),
                home: Some(stale_home.clone().into_os_string()),
                extra_candidates: Vec::new(),
            };
            assert_eq!(
                LinuxOwlCodexActivator::with_config(xdg_precedence_cfg)
                    .activate(&DisposableOwlHarness::sample_request())
                    .await,
                ActivationPortOutcome::Submitted
            );
            join_bounded(server, "xdg precedence singleton server").await;

            // 3. Missing/empty explicit `XDG_CONFIG_HOME` fails closed WITHOUT falling back to `$HOME/.config/Codex`.
            let _live_listener = harness.bind_singleton_listener();
            let missing_xdg = harness.base_dir.join("missing-xdg").into_os_string();
            let empty_xdg = std::ffi::OsString::new();
            for bad_explicit_xdg in [missing_xdg, empty_xdg] {
                let mut no_fallback_cfg = harness.config();
                no_fallback_cfg.profile_source = LinuxProfileSource::ExplicitEnv {
                    xdg_config_home: Some(bad_explicit_xdg),
                    home: Some(stale_home.clone().into_os_string()),
                    extra_candidates: Vec::new(),
                };
                match LinuxOwlCodexActivator::with_config(no_fallback_cfg)
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_app_unavailable");
                    }
                    other => panic!("expected fail-closed codex_app_unavailable, got {other:?}"),
                }
            }

            // 4. Multiple candidate profile directories fail closed with `codex_profile_ambiguous`.
            let mut ambig_cfg = harness.config();
            ambig_cfg.profile_source = LinuxProfileSource::ExplicitEnv {
                xdg_config_home: Some(harness.xdg_dir.clone().into_os_string()),
                home: None,
                extra_candidates: vec![stale_codex],
            };
            match LinuxOwlCodexActivator::with_config(ambig_cfg)
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_profile_ambiguous");
                }
                other => panic!("expected codex_profile_ambiguous, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn unsupported_compositor_env_and_missing_hidden_or_ambiguous_windows_reject_pre_send(
        ) {
            let mut harness = DisposableOwlHarness::new();
            let _listener = harness.bind_singleton_listener();

            // 1. Neither `HYPRLAND_INSTANCE_SIGNATURE` nor `SWAYSOCK` -> codex_navigation_unsupported.
            let mut none_cfg = harness.config();
            none_cfg.compositor_source = LinuxCompositorSource::ExplicitEnv {
                xdg_runtime_dir: Some(harness.comp_xdg_runtime_dir.clone().into_os_string()),
                hyprland_instance_signature: None,
                swaysock: None,
                compositor_proc_root: Some(harness.proc_root.clone()),
                simulate_exe_permission_denied: false,
                allow_same_pid_as_target: false,
            };
            match LinuxOwlCodexActivator::with_config(none_cfg)
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_navigation_unsupported");
                }
                other => panic!("expected codex_navigation_unsupported, got {other:?}"),
            }

            // 2. Both `HYPRLAND_INSTANCE_SIGNATURE` and `SWAYSOCK` set -> codex_navigation_unsupported.
            let mut both_cfg = harness.config();
            both_cfg.compositor_source = LinuxCompositorSource::ExplicitEnv {
                xdg_runtime_dir: Some(harness.comp_xdg_runtime_dir.clone().into_os_string()),
                hyprland_instance_signature: Some(DEFAULT_HYPR_SIGNATURE.into()),
                swaysock: Some(harness.sway_socket_path.clone().into_os_string()),
                compositor_proc_root: Some(harness.proc_root.clone()),
                simulate_exe_permission_denied: false,
                allow_same_pid_as_target: false,
            };
            match LinuxOwlCodexActivator::with_config(both_cfg)
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_navigation_unsupported");
                }
                other => {
                    panic!("expected codex_navigation_unsupported when both set, got {other:?}")
                }
            }

            // 3. Only hidden/unmapped windows for Codex PID -> codex_focus_unavailable pre-send.
            harness.clear_wire_events();
            harness.restart_compositor(
                FakeCompositorFlavor::Hyprland {
                    clients: vec![FakeHyprWindow {
                        address: DEFAULT_HYPR_WINDOW_ADDR.to_string(),
                        mapped: true,
                        hidden: true,
                        class: "Codex".to_string(),
                        pid: i64::from(harness.self_pid),
                    }],
                    initial_active: None,
                    dispatch_behavior: FakeDispatchBehavior::FocusTargetAndReplyOk,
                },
                Vec::new(),
                0o600,
            );
            match LinuxOwlCodexActivator::with_config(harness.config())
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_focus_unavailable");
                }
                other => {
                    panic!("expected codex_focus_unavailable for hidden window, got {other:?}")
                }
            }
            assert_eq!(harness.wire_events(), vec!["COMP:HYPR:j/clients"]);

            // 4. Two mapped non-hidden Codex windows -> codex_window_ambiguous pre-send.
            harness.clear_wire_events();
            harness.restart_compositor(
                FakeCompositorFlavor::Hyprland {
                    clients: vec![
                        FakeHyprWindow {
                            address: "0x55a1".to_string(),
                            mapped: true,
                            hidden: false,
                            class: "Codex".to_string(),
                            pid: i64::from(harness.self_pid),
                        },
                        FakeHyprWindow {
                            address: "0x55a2".to_string(),
                            mapped: true,
                            hidden: false,
                            class: "Codex".to_string(),
                            pid: i64::from(harness.self_pid),
                        },
                    ],
                    initial_active: None,
                    dispatch_behavior: FakeDispatchBehavior::FocusTargetAndReplyOk,
                },
                Vec::new(),
                0o600,
            );
            match LinuxOwlCodexActivator::with_config(harness.config())
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_window_ambiguous");
                }
                other => panic!("expected codex_window_ambiguous, got {other:?}"),
            }
            assert_eq!(harness.wire_events(), vec!["COMP:HYPR:j/clients"]);

            // 5. World-writable (`0666`) compositor socket is rejected before connecting/sending.
            harness.clear_wire_events();
            harness.restart_compositor(
                DisposableOwlHarness::default_hyprland_flavor(harness.self_pid),
                Vec::new(),
                0o666,
            );
            match LinuxOwlCodexActivator::with_config(harness.config())
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_focus_unavailable");
                }
                other => panic!("expected codex_focus_unavailable on 0666 socket, got {other:?}"),
            }
            assert!(harness.wire_events().is_empty());
        }

        #[tokio::test]
        async fn preflight_compositor_malformed_truncated_oversized_and_timeout_reject_pre_send() {
            let mut harness = DisposableOwlHarness::new();
            let _listener = harness.bind_singleton_listener();

            // 1. Empty EOF, malformed JSON, and oversized (`1 MiB`) preflight `j/clients` replies.
            for override_reply in [
                ScriptedCompositorReply::CloseImmediately,
                ScriptedCompositorReply::RawBytes(b"{not-an-array".to_vec()),
                ScriptedCompositorReply::RepeatedByte {
                    byte: b' ',
                    count: MAX_COMPOSITOR_RESPONSE_LIMIT_BYTES,
                },
            ] {
                harness.clear_wire_events();
                harness.restart_compositor(
                    DisposableOwlHarness::default_hyprland_flavor(harness.self_pid),
                    vec![Some(override_reply)],
                    0o600,
                );
                match LinuxOwlCodexActivator::with_config(harness.config())
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => panic!("expected pre-send codex_focus_unavailable, got {other:?}"),
                }
                assert_eq!(harness.wire_events(), vec!["COMP:HYPR:j/clients"]);
            }

            // 2. Truncated Sway `i3-ipc` payload and preflight hang exceeding deadline.
            for sway_override in [
                ScriptedCompositorReply::SwayRawHeaderAndBody {
                    magic: *SWAY_IPC_MAGIC,
                    declared_len: 128,
                    msg_type: SWAY_IPC_GET_TREE,
                    body: b"{\"id\":1".to_vec(),
                },
                ScriptedCompositorReply::HangMs(400),
            ] {
                harness.clear_wire_events();
                harness.restart_compositor(
                    DisposableOwlHarness::default_sway_flavor(harness.self_pid),
                    vec![Some(sway_override)],
                    0o600,
                );
                let mut cfg = harness.config();
                cfg.deadline = Duration::from_millis(150);
                cfg.step_timeout = Duration::from_millis(120);
                match LinuxOwlCodexActivator::with_config(cfg)
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_focus_unavailable");
                    }
                    other => {
                        panic!("expected pre-send codex_focus_unavailable on Sway, got {other:?}")
                    }
                }
                assert_eq!(harness.wire_events(), vec!["COMP:SWAY:4:"]);
            }
        }

        #[tokio::test]
        async fn production_root_immutable_policy_rejects_user_owned_runtime_directory() {
            let harness = DisposableOwlHarness::new();
            let _listener = harness.bind_singleton_listener();

            let mut cfg = harness.config();
            cfg.ownership_policy = RuntimeOwnershipPolicy::RootImmutable;
            let activator = LinuxOwlCodexActivator::with_config(cfg);

            let outcome = activator
                .activate(&DisposableOwlHarness::sample_request())
                .await;
            match outcome {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_runtime_unsupported");
                    assert!(!err.message.contains(SECRET_COOKIE));
                    let sock_str = harness.socket_path.to_str().expect("socket utf8");
                    assert!(!err.message.contains(sock_str));
                }
                other => panic!("expected RejectedBeforeSend, got {other:?}"),
            }
            assert!(harness.wire_events().is_empty());
        }

        #[tokio::test]
        async fn unpacked_cmdline_app_override_and_writable_runtime_are_rejected_before_send() {
            let harness = DisposableOwlHarness::new();
            let _listener = harness.bind_singleton_listener();

            // 1. Inject `--app=/tmp/unpacked-fixture` into `/proc/<pid>/cmdline`.
            let proc_cmdline = harness
                .proc_root
                .join(harness.self_pid.to_string())
                .join("cmdline");
            let mut bad_cmdline = harness.exe_path.as_os_str().as_encoded_bytes().to_vec();
            bad_cmdline.push(0);
            bad_cmdline.extend_from_slice(b"--app=/tmp/unpacked-fixture");
            bad_cmdline.push(0);
            fs::write(&proc_cmdline, bad_cmdline).expect("write bad cmdline");

            let activator = LinuxOwlCodexActivator::with_config(harness.config());
            match activator
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_runtime_unsupported");
                }
                other => panic!("expected RejectedBeforeSend for --app override, got {other:?}"),
            }

            // 2. Restore clean cmdline, then make runtime dir writable (`0755`) -> must also fail.
            let mut clean_cmdline = harness.exe_path.as_os_str().as_encoded_bytes().to_vec();
            clean_cmdline.push(0);
            fs::write(&proc_cmdline, clean_cmdline).expect("restore cmdline");
            fs::set_permissions(&harness.runtime_dir, fs::Permissions::from_mode(0o755))
                .expect("make runtime_dir writable");

            match activator
                .activate(&DisposableOwlHarness::sample_request())
                .await
            {
                ActivationPortOutcome::RejectedBeforeSend(err) => {
                    assert_eq!(err.code, "codex_runtime_unsupported");
                }
                other => panic!("expected RejectedBeforeSend for writable runtime, got {other:?}"),
            }
            assert!(harness.wire_events().is_empty());
        }

        #[tokio::test]
        async fn post_connect_cookie_peer_pid_and_process_drift_abort_with_zero_bytes_written() {
            // 1. Cookie drift AFTER connect() and BEFORE write_all().
            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let remote_cookie_link = harness.socket_dir.join("SingletonCookie");

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let mut received = Vec::new();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        stream.read_to_end(&mut received),
                    )
                    .await;
                    received
                });

                let mut cfg = harness.config();
                cfg.post_connect_hook = Some(Arc::new(move || {
                    let _ = fs::remove_file(&remote_cookie_link);
                    symlink("0000000000000000000", &remote_cookie_link)
                        .expect("rotate cookie after connect");
                }));

                let activator = LinuxOwlCodexActivator::with_config(cfg);
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_app_unavailable");
                        assert!(!err.message.contains(SECRET_COOKIE));
                        assert!(!err.message.contains("0000000000000000000"));
                    }
                    other => panic!("expected RejectedBeforeSend on cookie drift, got {other:?}"),
                }

                let bytes_on_wire = join_bounded(server, "cookie drift server").await;
                assert!(
                    bytes_on_wire.is_empty(),
                    "zero bytes may be written when cookie drifts post-connect"
                );
            }

            // 2. Peer PID mismatch via real kernel `SO_PEERCRED` (`SingletonLock` claims `other_pid`,
            //    while the socket listener belongs to `self_pid`).
            {
                let mut harness = DisposableOwlHarness::new();
                let other_pid = harness
                    .self_pid
                    .saturating_add(1)
                    .max(harness.compositor_pid().saturating_add(1));
                harness.restart_compositor(
                    DisposableOwlHarness::default_hyprland_flavor(other_pid),
                    Vec::new(),
                    0o600,
                );
                let listener = harness.bind_singleton_listener();

                let other_proc_dir = harness.proc_root.join(other_pid.to_string());
                DirBuilder::new()
                    .mode(0o700)
                    .create(&other_proc_dir)
                    .expect("mkdir other_proc_dir");
                fs::set_permissions(&other_proc_dir, fs::Permissions::from_mode(0o700))
                    .expect("chmod other_proc_dir");
                let stat_line = format!(
                    "{other_pid} (owl-shell) S 1 {other_pid} {other_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 424242 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                );
                fs::write(other_proc_dir.join("stat"), stat_line).expect("write other stat");
                symlink(&harness.exe_path, other_proc_dir.join("exe")).expect("symlink other exe");
                let mut cmdline = harness.exe_path.as_os_str().as_encoded_bytes().to_vec();
                cmdline.push(0);
                fs::write(other_proc_dir.join("cmdline"), cmdline).expect("write other cmdline");

                let lock_path = harness.profile_dir.join("SingletonLock");
                fs::remove_file(&lock_path).expect("remove lock");
                symlink(format!("nixos-host-{other_pid}"), &lock_path)
                    .expect("point lock at other_pid");

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let mut received = Vec::new();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        stream.read_to_end(&mut received),
                    )
                    .await;
                    received
                });

                let activator = LinuxOwlCodexActivator::with_config(harness.config());
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_app_unavailable");
                    }
                    other => {
                        panic!("expected RejectedBeforeSend on peer PID mismatch, got {other:?}")
                    }
                }

                let bytes_on_wire = join_bounded(server, "peer pid mismatch server").await;
                assert!(
                    bytes_on_wire.is_empty(),
                    "zero bytes may be written when kernel peer PID disagrees with SingletonLock"
                );
            }

            // 3. Process `start_ticks` drift (PID reuse) AFTER connect() and BEFORE write_all().
            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let stat_path = harness
                    .proc_root
                    .join(harness.self_pid.to_string())
                    .join("stat");
                let self_pid = harness.self_pid;

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let mut received = Vec::new();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(2),
                        stream.read_to_end(&mut received),
                    )
                    .await;
                    received
                });

                let mut cfg = harness.config();
                cfg.post_connect_hook = Some(Arc::new(move || {
                    let drifted_stat = format!(
                        "{self_pid} (owl-shell) S 1 {self_pid} {self_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 999999 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                    );
                    fs::write(&stat_path, drifted_stat).expect("drift start_ticks");
                }));

                let activator = LinuxOwlCodexActivator::with_config(cfg);
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::RejectedBeforeSend(err) => {
                        assert_eq!(err.code, "codex_app_unavailable");
                    }
                    other => {
                        panic!("expected RejectedBeforeSend on start_ticks drift, got {other:?}")
                    }
                }

                let bytes_on_wire = join_bounded(server, "start ticks drift server").await;
                assert!(
                    bytes_on_wire.is_empty(),
                    "zero bytes may be written when process start_ticks drifts post-connect"
                );
            }
        }

        #[tokio::test]
        async fn post_send_ack_focus_and_pid_reuse_failures_become_unknown_and_never_replay() {
            // 1. ACK timeout after bytes are sent -> UnknownAfterPossibleSend.
            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    tokio::time::sleep(Duration::from_millis(250)).await;
                });

                let mut cfg = harness.config();
                cfg.deadline = Duration::from_millis(140);
                cfg.step_timeout = Duration::from_millis(120);
                let activator = LinuxOwlCodexActivator::with_config(cfg);
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(err.code, "codex_activation_unknown");
                    }
                    other => {
                        panic!("expected UnknownAfterPossibleSend on ACK timeout, got {other:?}")
                    }
                }
                join_bounded(server, "ack timeout singleton server").await;
            }

            // 2. Empty EOF, `SHUTDOWN`, `ACK\0`, `NACK`, truncated `AC`, and >16-byte response -> UnknownAfterPossibleSend.
            for bad_reply in [
                b"".as_slice(),
                b"AC".as_slice(),
                b"SHUTDOWN".as_slice(),
                b"ACK\0".as_slice(),
                b"NACK".as_slice(),
                b"ACK_EXTRA_GARBAGE_PAST_SIXTEEN_BYTES".as_slice(),
            ] {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let reply = bad_reply.to_vec();

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    if !reply.is_empty() {
                        stream.write_all(&reply).await.expect("write bad reply");
                    }
                    let _ = stream.shutdown().await;
                });

                let activator = LinuxOwlCodexActivator::with_config(harness.config());
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(
                            err.code, "codex_activation_unknown",
                            "bad reply {bad_reply:?} must produce codex_activation_unknown"
                        );
                    }
                    other => panic!(
                        "expected UnknownAfterPossibleSend for bad reply {bad_reply:?}, got {other:?}"
                    ),
                }
                join_bounded(server, "bad ack singleton server").await;
            }

            // 3. Valid Owl `ACK` alone NEVER yields `Submitted` when Hyprland/Sway focus fails,
            //    leaves the window unfocused, times out, or detects post-ACK Codex/compositor PID reuse;
            //    and `ChatNavigationController` latches `codex_activation_unknown` with zero replay!
            {
                let mut harness = DisposableOwlHarness::new();
                harness.restart_compositor(
                    FakeCompositorFlavor::Hyprland {
                        clients: vec![
                            FakeHyprWindow {
                                address: "0x1000a0".to_string(),
                                mapped: true,
                                hidden: false,
                                class: "Threadlane".to_string(),
                                pid: i64::from(harness.self_pid) + 500,
                            },
                            FakeHyprWindow {
                                address: DEFAULT_HYPR_WINDOW_ADDR.to_string(),
                                mapped: true,
                                hidden: false,
                                class: "Codex".to_string(),
                                pid: i64::from(harness.self_pid),
                            },
                        ],
                        initial_active: Some(FakeHyprWindow {
                            address: "0x1000a0".to_string(),
                            mapped: true,
                            hidden: false,
                            class: "Threadlane".to_string(),
                            pid: i64::from(harness.self_pid) + 500,
                        }),
                        // Returns `"ok"` on `dispatch focuswindow`, but active window remains `0x1000a0`!
                        dispatch_behavior: FakeDispatchBehavior::KeepUnfocusedAndReplyOk,
                    },
                    Vec::new(),
                    0o600,
                );

                let listener = harness.bind_singleton_listener();
                let event_log = harness.event_log_path.clone();
                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    append_wire_event(&event_log, "OWL_SINGLETON:START");
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let controller = ChatNavigationController::with_config(
                    TestLinuxPort(LinuxOwlCodexActivator::with_config(harness.config())),
                    ChatNavigationControllerConfig {
                        coalesce_window: Duration::from_millis(750),
                        max_recent_submissions: 16,
                        max_in_flight: 16,
                        max_unknown_targets: 16,
                    },
                );

                let sample = DisposableOwlHarness::sample_request();
                let validated = ValidatedCodexChatRequest {
                    source_id: sample.chat_identity.source_id.clone(),
                    thread_uuid: sample.chat_identity.thread_uuid.clone(),
                    job_id: None,
                };
                let resolved = ResolvedCodexChatTarget {
                    chat_identity: sample.chat_identity,
                    source_id: validated.source_id.clone(),
                    thread_uuid: validated.thread_uuid.clone(),
                    job_id: None,
                    canonical_uri: sample.canonical_uri,
                    match_origin: VerifiedChatMatchOrigin::Chat,
                };

                let err_first = controller
                    .activate_resolved(&resolved, &validated)
                    .await
                    .expect_err("unfocused window after ACK must fail as unknown");
                assert_eq!(err_first.code, "codex_activation_unknown");
                assert_eq!(controller.unknown_latch_count(), 1);
                join_bounded(server, "unfocused hyprland singleton server").await;

                let events_after_first = harness.wire_events();
                let expected_focus =
                    format!("COMP:HYPR:dispatch focuswindow address:{DEFAULT_HYPR_WINDOW_ADDR}");
                assert_eq!(
                    events_after_first,
                    vec![
                        "COMP:HYPR:j/clients",
                        "OWL_SINGLETON:START",
                        expected_focus.as_str(),
                        "COMP:HYPR:j/activewindow",
                    ]
                );

                // Second attempt on the latched chat is refused BEFORE any new preflight or send!
                let err_replay = controller
                    .activate_resolved(&resolved, &validated)
                    .await
                    .expect_err("latched unknown target must never replay");
                assert_eq!(err_replay.code, "codex_activation_unknown");
                assert_eq!(harness.wire_events(), events_after_first);
            }

            // 4. Compositor PID reuse (`start_ticks` drift between preflight and post-ACK dispatch),
            //    Codex target PID reuse after `ACK`, post-ACK focus command rejection, and post-ACK
            //    focus dispatch timeout all fail closed as `UnknownAfterPossibleSend`.
            {
                let harness = DisposableOwlHarness::new_sway();
                let listener = harness.bind_singleton_listener();
                let comp_pid = harness.compositor_pid();
                let comp_stat_path = harness.proc_root.join(comp_pid.to_string()).join("stat");

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    // Simulate compositor PID reuse before post-ACK `RUN_COMMAND` executes.
                    let drifted_comp_stat = format!(
                        "{comp_pid} (.sway-wrapped) S 1 {comp_pid} {comp_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 888888 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                    );
                    fs::write(&comp_stat_path, drifted_comp_stat).expect("drift comp stat");
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let activator = LinuxOwlCodexActivator::with_config(harness.config());
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(err.code, "codex_activation_unknown");
                    }
                    other => panic!(
                        "expected UnknownAfterPossibleSend on compositor PID reuse, got {other:?}"
                    ),
                }
                join_bounded(server, "compositor pid reuse singleton server").await;
            }

            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let self_pid = harness.self_pid;
                let codex_stat_path = harness.proc_root.join(self_pid.to_string()).join("stat");

                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    // Simulate Codex target PID reuse before `ACK` returns.
                    let drifted_codex_stat = format!(
                        "{self_pid} (owl-shell) S 1 {self_pid} {self_pid} 0 -1 4194304 0 0 0 0 1 1 0 0 20 0 1 0 777777 1000 100 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0\n"
                    );
                    fs::write(&codex_stat_path, drifted_codex_stat).expect("drift codex stat");
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let activator = LinuxOwlCodexActivator::with_config(harness.config());
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(err.code, "codex_activation_unknown");
                    }
                    other => panic!(
                        "expected UnknownAfterPossibleSend on Codex PID reuse, got {other:?}"
                    ),
                }
                join_bounded(server, "codex pid reuse singleton server").await;
            }

            {
                let mut harness = DisposableOwlHarness::new_sway();
                harness.restart_compositor(
                    FakeCompositorFlavor::Sway {
                        windows: vec![FakeSwayWindow {
                            id: i64::try_from(DEFAULT_SWAY_CON_ID).expect("i64 con_id"),
                            pid: i64::from(harness.self_pid),
                            focused: false,
                            app_id: Some("Codex".to_string()),
                            xwayland_class: None,
                        }],
                        dispatch_behavior: FakeDispatchBehavior::ReplyError,
                    },
                    Vec::new(),
                    0o600,
                );
                let listener = harness.bind_singleton_listener();
                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let activator = LinuxOwlCodexActivator::with_config(harness.config());
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(err.code, "codex_activation_unknown");
                    }
                    other => panic!(
                        "expected UnknownAfterPossibleSend on Sway focus ReplyError, got {other:?}"
                    ),
                }
                join_bounded(server, "sway reply error singleton server").await;
            }

            {
                let mut harness = DisposableOwlHarness::new();
                harness.restart_compositor(
                    FakeCompositorFlavor::Hyprland {
                        clients: vec![FakeHyprWindow {
                            address: DEFAULT_HYPR_WINDOW_ADDR.to_string(),
                            mapped: true,
                            hidden: false,
                            class: "Codex".to_string(),
                            pid: i64::from(harness.self_pid),
                        }],
                        initial_active: None,
                        dispatch_behavior: FakeDispatchBehavior::HangMs(350),
                    },
                    Vec::new(),
                    0o600,
                );
                let listener = harness.bind_singleton_listener();
                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let mut cfg = harness.config();
                cfg.deadline = Duration::from_millis(200);
                cfg.step_timeout = Duration::from_millis(120);
                let activator = LinuxOwlCodexActivator::with_config(cfg);
                match activator
                    .activate(&DisposableOwlHarness::sample_request())
                    .await
                {
                    ActivationPortOutcome::UnknownAfterPossibleSend(err) => {
                        assert_eq!(err.code, "codex_activation_unknown");
                    }
                    other => panic!(
                        "expected UnknownAfterPossibleSend on post-ACK focus hang, got {other:?}"
                    ),
                }
                join_bounded(server, "hyprland hang singleton server").await;
            }

            // 5. Controller coalesces rapid post-`Submitted` duplicate click to a single wire send.
            {
                let harness = DisposableOwlHarness::new();
                let listener = harness.bind_singleton_listener();
                let event_log = harness.event_log_path.clone();
                let server = tokio::spawn(async move {
                    let (mut stream, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .expect("accept timeout")
                            .expect("accept");
                    let _incoming = read_singleton_frame_bounded(&mut stream).await;
                    append_wire_event(&event_log, "OWL_SINGLETON:START");
                    stream.write_all(b"ACK").await.expect("write ACK");
                    stream.shutdown().await.expect("shutdown");
                });

                let controller = ChatNavigationController::new(TestLinuxPort(
                    LinuxOwlCodexActivator::with_config(harness.config()),
                ));
                let sample = DisposableOwlHarness::sample_request();
                let validated = ValidatedCodexChatRequest {
                    source_id: sample.chat_identity.source_id.clone(),
                    thread_uuid: sample.chat_identity.thread_uuid.clone(),
                    job_id: None,
                };
                let resolved = ResolvedCodexChatTarget {
                    chat_identity: sample.chat_identity,
                    source_id: validated.source_id.clone(),
                    thread_uuid: validated.thread_uuid.clone(),
                    job_id: None,
                    canonical_uri: sample.canonical_uri,
                    match_origin: VerifiedChatMatchOrigin::Chat,
                };

                assert_eq!(
                    controller
                        .activate_resolved(&resolved, &validated)
                        .await
                        .expect("first activation"),
                    NavigationSubmissionOutcome::Submitted
                );
                join_bounded(server, "coalesce singleton server").await;

                let events_once = harness.wire_events();
                assert_eq!(
                    events_once
                        .iter()
                        .filter(|e| *e == "OWL_SINGLETON:START")
                        .count(),
                    1
                );

                assert_eq!(
                    controller
                        .activate_resolved(&resolved, &validated)
                        .await
                        .expect("coalesced duplicate click"),
                    NavigationSubmissionOutcome::CoalescedDuplicate
                );
                assert_eq!(harness.wire_events(), events_once);
            }
        }
    }
}
