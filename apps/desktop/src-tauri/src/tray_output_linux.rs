//! Linux Wayland activation-specific output hint resolution and bounded read-only Hyprland
//! compositor disambiguation.
//!
//! When `StatusNotifierItem::Activate` omits coordinates (`(0, 0)`, such as Dux / Quickshell
//! `item.activate()`) or emits panel-local offsets instead of global compositor coordinates, this
//! module resolves whether a trustworthy activation-specific monitor hint exists without ever
//! pinning `main`'s monitor or blindly guessing `monitors[0]`.
//!
//! On confirmed Hyprland sessions only, a single bounded read-only Unix socket query
//! (`j/cursorpos` + `j/monitors` directly over `$XDG_RUNTIME_DIR/hypr/<sig>/.socket.sock` with no
//! external shell, subprocesses, polling, input hooks, or background service) may disambiguate
//! missing or panel-local click output. All other Wayland compositors and non-pointer
//! (`MenuOpenStatus`) invocations leave monitor selection unpinned so `zwlr_layer_shell_v1` `NULL`
//! output lets the compositor pick the active output.

#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Component, Path, PathBuf};
#[cfg(unix)]
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::tray::tray_linux::{LinuxFallbackMonitorPolicy, LinuxMonitorWorkarea};
use crate::tray::{PopoverActivationSource, RawCoordinate2D, RawSize2D, RawTrayAnchor};

/// Per-request read/write timeout for the bounded read-only Hyprland IPC query.
#[cfg(target_os = "linux")]
pub const HYPRLAND_SOCKET_IO_TIMEOUT: Duration = Duration::from_millis(80);

/// Total wall-clock budget across both read-only Hyprland queries (`j/cursorpos` + `j/monitors`).
#[cfg(target_os = "linux")]
pub const HYPRLAND_TOTAL_QUERY_BUDGET: Duration = Duration::from_millis(180);

/// Hard upper bound on bytes read from Hyprland `j/cursorpos`.
#[cfg(unix)]
const MAX_HYPRLAND_CURSOR_RESPONSE_BYTES: usize = 4 * 1024;

/// Hard upper bound on bytes read from Hyprland `j/monitors`.
#[cfg(unix)]
const MAX_HYPRLAND_MONITORS_RESPONSE_BYTES: usize = 256 * 1024;

/// Maximum safe length for `HYPRLAND_INSTANCE_SIGNATURE`.
const MAX_HYPRLAND_SIGNATURE_LEN: usize = 128;

/// Rounding tolerance (logical pixels) when matching Hyprland fractional-scale logical monitor
/// geometry against GDK `xdg_output` monitor geometry.
const HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX: i32 = 2;

/// Screen-edge band width (logical pixels) used to recognize panel/bar regions when
/// disambiguating panel-local SNI coordinates on confirmed Hyprland.
const PANEL_LOCAL_EDGE_BAND_PX: f64 = 128.0;

/// Tolerance (logical pixels) between SNI point coordinates and cursor monitor-local coordinates
/// when recognizing panel-local coordinate offsets.
const PANEL_LOCAL_COORD_MATCH_TOLERANCE_PX: f64 = 64.0;

/// Maximum small widget/icon-local offset (pixels) recognized when a different monitor's panel
/// edge was clicked on confirmed Hyprland.
const WIDGET_LOCAL_MAX_OFFSET_PX: f64 = 48.0;

/// Optional GDK monitor hardware identity metadata paired positionally with
/// [`LinuxMonitorWorkarea`] entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinuxMonitorIdentity {
    pub manufacturer: Option<String>,
    pub model: Option<String>,
}

/// Captured desktop environment variables used to verify a confirmed Hyprland session before
/// attempting any read-only IPC socket query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HyprlandEnvSnapshot {
    pub xdg_runtime_dir: Option<PathBuf>,
    pub hyprland_instance_signature: Option<String>,
    pub xdg_current_desktop: Option<String>,
    pub desktop_session: Option<String>,
    pub wayland_display: Option<String>,
    pub sway_sock: Option<String>,
}

#[cfg(target_os = "linux")]
impl HyprlandEnvSnapshot {
    pub fn from_process_env() -> Self {
        Self {
            xdg_runtime_dir: std::env::var_os("XDG_RUNTIME_DIR")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            hyprland_instance_signature: std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok(),
            xdg_current_desktop: std::env::var("XDG_CURRENT_DESKTOP").ok(),
            desktop_session: std::env::var("DESKTOP_SESSION").ok(),
            wayland_display: std::env::var("WAYLAND_DISPLAY").ok(),
            sway_sock: std::env::var("SWAYSOCK").ok(),
        }
    }
}

/// Validated Hyprland monitor entry with rotation-adjusted physical and logical geometry.
#[derive(Debug, Clone, PartialEq)]
pub struct HyprlandLogicalMonitor {
    pub name: String,
    pub make: String,
    pub model: String,
    pub x: i32,
    pub y: i32,
    pub rotated_physical_width: i32,
    pub rotated_physical_height: i32,
    pub logical_width: i32,
    pub logical_height: i32,
    pub scale: f64,
}

impl HyprlandLogicalMonitor {
    /// Returns `true` when global logical coordinate `(lx, ly)` lies within this monitor's
    /// logical rectangle `[x, x + logical_width) x [y, y + logical_height)`.
    pub fn contains_logical_point(&self, lx: f64, ly: f64) -> bool {
        if !lx.is_finite() || !ly.is_finite() || self.logical_width <= 0 || self.logical_height <= 0
        {
            return false;
        }
        let min_x = self.x as f64;
        let max_x = min_x + (self.logical_width as f64);
        let min_y = self.y as f64;
        let max_y = min_y + (self.logical_height as f64);
        lx >= min_x && lx < max_x && ly >= min_y && ly < max_y
    }
}

/// Single click-time read-only snapshot fetched from confirmed Hyprland (`j/cursorpos` and
/// `j/monitors`).
#[derive(Debug, Clone, PartialEq)]
pub struct HyprlandClickSnapshot {
    pub cursor_x: f64,
    pub cursor_y: f64,
    pub monitors: Vec<HyprlandLogicalMonitor>,
}

/// Output of Wayland activation anchor and monitor-hint resolution prior to layer-shell planning.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedWaylandActivationInput {
    /// Trusted global anchor to honor for `IconAnchored` placement, or `None` when coordinates
    /// were absent (`(0, 0)`) or disambiguated as untrusted panel-local offsets.
    pub trusted_global_anchor: Option<RawTrayAnchor>,
    /// Fallback monitor policy applied when `trusted_global_anchor` is `None`.
    pub fallback_policy: LinuxFallbackMonitorPolicy,
}

#[derive(Debug, Deserialize)]
struct HyprlandCursorPosWire {
    x: f64,
    y: f64,
}

#[derive(Debug, Deserialize)]
struct HyprlandMonitorWire {
    #[serde(default)]
    name: String,
    #[serde(default)]
    make: String,
    #[serde(default)]
    model: String,
    x: i32,
    y: i32,
    width: i32,
    height: i32,
    scale: f64,
    #[serde(default)]
    transform: u8,
    #[serde(default)]
    disabled: bool,
}

fn is_valid_hyprland_signature(sig: &str) -> bool {
    if sig.is_empty()
        || sig.len() > MAX_HYPRLAND_SIGNATURE_LEN
        || sig != sig.trim()
        || sig == "."
        || sig == ".."
    {
        return false;
    }
    sig.bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn is_valid_wayland_display(display: &str) -> bool {
    !display.is_empty()
        && display == display.trim()
        && !display.contains('/')
        && !display.contains('\0')
        && display != "."
        && display != ".."
}

fn is_valid_runtime_dir_path(path: &Path) -> bool {
    if !path.is_absolute() || path.as_os_str().is_empty() {
        return false;
    }
    let raw = path.to_string_lossy();
    if raw.contains('\0') {
        return false;
    }
    !path
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

/// Returns `true` only when environment variables unambiguously confirm an active Hyprland Wayland
/// desktop session (refusing Sway, GNOME, foreign desktops, leaked signatures, or invalid values).
pub fn is_confirmed_hyprland_env(env: &HyprlandEnvSnapshot) -> bool {
    let Some(wayland_display) = env.wayland_display.as_deref() else {
        return false;
    };
    if !is_valid_wayland_display(wayland_display) {
        return false;
    }

    if env
        .sway_sock
        .as_deref()
        .is_some_and(|sock| !sock.trim().is_empty())
    {
        return false;
    }

    let Some(sig) = env.hyprland_instance_signature.as_deref() else {
        return false;
    };
    if !is_valid_hyprland_signature(sig) {
        return false;
    }

    let current_desktop = env
        .xdg_current_desktop
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(desktop) = current_desktop {
        return desktop
            .split(':')
            .any(|token| token.trim().eq_ignore_ascii_case("hyprland"));
    }

    let session = env
        .desktop_session
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(sess) = session {
        let lower = sess.to_ascii_lowercase();
        return lower == "hyprland"
            || lower.starts_with("hyprland-")
            || lower.ends_with("-hyprland");
    }

    false
}

/// Resolves the private `$XDG_RUNTIME_DIR/hypr/<HYPRLAND_INSTANCE_SIGNATURE>/.socket.sock` path
/// when `env` confirms an active Hyprland session.
pub fn resolve_confirmed_hyprland_socket_path(env: &HyprlandEnvSnapshot) -> Option<PathBuf> {
    if !is_confirmed_hyprland_env(env) {
        return None;
    }
    let runtime_dir = env.xdg_runtime_dir.as_deref()?;
    if !is_valid_runtime_dir_path(runtime_dir) {
        return None;
    }
    let sig = env.hyprland_instance_signature.as_deref()?;
    Some(runtime_dir.join("hypr").join(sig).join(".socket.sock"))
}

/// Parses Hyprland `j/cursorpos` JSON bytes into finite `(x, y)` logical coordinates.
pub fn parse_hyprland_cursor_pos(raw: &[u8]) -> Option<(f64, f64)> {
    let parsed: HyprlandCursorPosWire = serde_json::from_slice(raw).ok()?;
    if !parsed.x.is_finite() || !parsed.y.is_finite() {
        return None;
    }
    Some((parsed.x, parsed.y))
}

/// Parses Hyprland `j/monitors` JSON bytes into validated enabled monitors, accounting for Wayland
/// rotation transforms (`transform % 2 == 1` swaps width and height for 90°/270° orientations) and
/// fractional/mixed scaling factors.
pub fn parse_hyprland_monitors(raw: &[u8]) -> Option<Vec<HyprlandLogicalMonitor>> {
    let entries: Vec<HyprlandMonitorWire> = serde_json::from_slice(raw).ok()?;
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        if entry.disabled {
            continue;
        }
        if entry.width <= 0
            || entry.height <= 0
            || !entry.scale.is_finite()
            || entry.scale <= 0.0
            || entry.scale > 16.0
            || entry.transform > 7
        {
            continue;
        }
        let (rot_w, rot_h) = if entry.transform % 2 == 1 {
            (entry.height, entry.width)
        } else {
            (entry.width, entry.height)
        };
        let logical_width = ((rot_w as f64) / entry.scale).round() as i32;
        let logical_height = ((rot_h as f64) / entry.scale).round() as i32;
        if logical_width <= 0 || logical_height <= 0 {
            continue;
        }
        out.push(HyprlandLogicalMonitor {
            name: entry.name.trim().to_string(),
            make: entry.make.trim().to_string(),
            model: entry.model.trim().to_string(),
            x: entry.x,
            y: entry.y,
            rotated_physical_width: rot_w,
            rotated_physical_height: rot_h,
            logical_width,
            logical_height,
            scale: entry.scale,
        });
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

#[cfg(unix)]
fn validate_hyprland_socket_ownership_and_type(
    runtime_dir: &Path,
    signature: &str,
) -> Option<PathBuf> {
    let runtime_meta = std::fs::symlink_metadata(runtime_dir).ok()?;
    if runtime_meta.file_type().is_symlink() || !runtime_meta.file_type().is_dir() {
        return None;
    }
    let owner_uid = runtime_meta.uid();

    let hypr_dir = runtime_dir.join("hypr");
    let hypr_meta = std::fs::symlink_metadata(&hypr_dir).ok()?;
    if hypr_meta.file_type().is_symlink()
        || !hypr_meta.file_type().is_dir()
        || hypr_meta.uid() != owner_uid
    {
        return None;
    }

    let instance_dir = hypr_dir.join(signature);
    let instance_meta = std::fs::symlink_metadata(&instance_dir).ok()?;
    if instance_meta.file_type().is_symlink()
        || !instance_meta.file_type().is_dir()
        || instance_meta.uid() != owner_uid
    {
        return None;
    }

    let socket_path = instance_dir.join(".socket.sock");
    let socket_meta = std::fs::symlink_metadata(&socket_path).ok()?;
    if socket_meta.file_type().is_symlink()
        || !socket_meta.file_type().is_socket()
        || socket_meta.uid() != owner_uid
    {
        return None;
    }

    Some(socket_path)
}

#[cfg(unix)]
fn query_hyprland_socket_once(
    socket_path: &Path,
    command: &[u8],
    timeout: Duration,
    max_bytes: usize,
) -> Option<Vec<u8>> {
    if timeout.is_zero() || max_bytes == 0 {
        return None;
    }
    let mut stream = UnixStream::connect(socket_path).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    stream.set_write_timeout(Some(timeout)).ok()?;
    stream.write_all(command).ok()?;

    let mut buf = Vec::new();
    let limit = (max_bytes as u64).saturating_add(1);
    stream.take(limit).read_to_end(&mut buf).ok()?;
    if buf.is_empty() || buf.len() > max_bytes {
        return None;
    }
    Some(buf)
}

#[cfg(unix)]
fn query_hyprland_click_snapshot_at_socket(
    socket_path: &Path,
    io_timeout: Duration,
    total_budget: Duration,
) -> Option<HyprlandClickSnapshot> {
    let started = Instant::now();
    let first_timeout = io_timeout.min(total_budget);
    let cursor_bytes = query_hyprland_socket_once(
        socket_path,
        b"j/cursorpos",
        first_timeout,
        MAX_HYPRLAND_CURSOR_RESPONSE_BYTES,
    )?;
    let (cursor_x, cursor_y) = parse_hyprland_cursor_pos(&cursor_bytes)?;

    let remaining = total_budget.checked_sub(started.elapsed())?;
    if remaining.is_zero() {
        return None;
    }
    let monitors_bytes = query_hyprland_socket_once(
        socket_path,
        b"j/monitors",
        io_timeout.min(remaining),
        MAX_HYPRLAND_MONITORS_RESPONSE_BYTES,
    )?;
    let monitors = parse_hyprland_monitors(&monitors_bytes)?;

    Some(HyprlandClickSnapshot {
        cursor_x,
        cursor_y,
        monitors,
    })
}

/// Performs a single bounded, read-only Hyprland IPC snapshot (`j/cursorpos` + `j/monitors`) for
/// `env` when Hyprland is confirmed and its private user-owned Unix socket validates cleanly.
#[cfg(unix)]
pub fn query_confirmed_hyprland_click_snapshot(
    env: &HyprlandEnvSnapshot,
    io_timeout: Duration,
    total_budget: Duration,
) -> Option<HyprlandClickSnapshot> {
    let _expected_path = resolve_confirmed_hyprland_socket_path(env)?;
    let runtime_dir = env.xdg_runtime_dir.as_deref()?;
    let signature = env.hyprland_instance_signature.as_deref()?;
    let socket_path = validate_hyprland_socket_ownership_and_type(runtime_dir, signature)?;
    query_hyprland_click_snapshot_at_socket(&socket_path, io_timeout, total_budget)
}

/// Performs a single bounded, read-only click-time Hyprland IPC snapshot using the current process
/// environment.
#[cfg(target_os = "linux")]
pub fn query_confirmed_hyprland_click_snapshot_from_env() -> Option<HyprlandClickSnapshot> {
    let env = HyprlandEnvSnapshot::from_process_env();
    query_confirmed_hyprland_click_snapshot(
        &env,
        HYPRLAND_SOCKET_IO_TIMEOUT,
        HYPRLAND_TOTAL_QUERY_BUDGET,
    )
}

fn identity_matches_hyprland_monitor(
    hypr: &HyprlandLogicalMonitor,
    identity: &LinuxMonitorIdentity,
) -> bool {
    let gdk_model = identity
        .model
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let gdk_make = identity
        .manufacturer
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let model_match = match gdk_model {
        Some(gm) => {
            (!hypr.model.is_empty() && gm.eq_ignore_ascii_case(&hypr.model))
                || (!hypr.name.is_empty() && gm.eq_ignore_ascii_case(&hypr.name))
        }
        None => false,
    };
    if !model_match {
        return false;
    }
    match (gdk_make, hypr.make.is_empty()) {
        (Some(gmake), false) => gmake.eq_ignore_ascii_case(&hypr.make),
        _ => true,
    }
}

fn select_unique_gdk_candidate(
    candidates: &[usize],
    hypr: &HyprlandLogicalMonitor,
    gdk_monitors: &[LinuxMonitorWorkarea],
    gdk_identities: &[LinuxMonitorIdentity],
) -> Option<(usize, usize)> {
    let chosen_slice = match candidates {
        [] => return None,
        [only] => *only,
        many => {
            let mut narrowed = many.iter().copied().filter(|&idx| {
                gdk_identities
                    .get(idx)
                    .is_some_and(|id| identity_matches_hyprland_monitor(hypr, id))
            });
            let first = narrowed.next()?;
            if narrowed.next().is_some() {
                return None;
            }
            first
        }
    };
    let gdk_mon_idx = gdk_monitors.get(chosen_slice)?.monitor_index?;
    Some((chosen_slice, gdk_mon_idx))
}

/// Matches a validated [`HyprlandLogicalMonitor`] uniquely to a live GDK monitor entry, returning
/// `(slice_index, gdk_monitor_index)`.
///
/// Uses logical geometry matching (supporting rotated and fractional-scale monitors even when GDK3
/// reports an integer `scale_factor`) with a fallback pass for GDK3 integer-divided sizes when
/// `xdg_output` logical sizing is absent. Returns `None` if no unique match exists.
pub fn match_hyprland_monitor_to_gdk(
    hypr: &HyprlandLogicalMonitor,
    gdk_monitors: &[LinuxMonitorWorkarea],
    gdk_identities: &[LinuxMonitorIdentity],
) -> Option<(usize, usize)> {
    // Pass 1: exact logical geometry match.
    let exact: Vec<usize> = gdk_monitors
        .iter()
        .enumerate()
        .filter(|(_, g)| {
            g.monitor_index.is_some()
                && g.geometry_x == hypr.x
                && g.geometry_y == hypr.y
                && g.geometry_width == hypr.logical_width
                && g.geometry_height == hypr.logical_height
        })
        .map(|(idx, _)| idx)
        .collect();
    if let Some(matched) = select_unique_gdk_candidate(&exact, hypr, gdk_monitors, gdk_identities) {
        return Some(matched);
    }

    // Pass 2: tolerance-bounded logical geometry match for fractional rounding differences.
    let approx: Vec<usize> = gdk_monitors
        .iter()
        .enumerate()
        .filter(|(_, g)| {
            g.monitor_index.is_some()
                && g.geometry_x.abs_diff(hypr.x) <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
                && g.geometry_y.abs_diff(hypr.y) <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
                && g.geometry_width.abs_diff(hypr.logical_width)
                    <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
                && g.geometry_height.abs_diff(hypr.logical_height)
                    <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
        })
        .map(|(idx, _)| idx)
        .collect();
    if let Some(matched) = select_unique_gdk_candidate(&approx, hypr, gdk_monitors, gdk_identities)
    {
        return Some(matched);
    }

    // Pass 3: origin match + GDK integer-scaled rotated physical size (for GDK builds without
    // `xdg_output` fractional logical size) or exact hardware identity match at that origin.
    let fallback: Vec<usize> = gdk_monitors
        .iter()
        .enumerate()
        .filter(|(idx, g)| {
            if g.monitor_index.is_none()
                || g.geometry_x.abs_diff(hypr.x) > HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
                || g.geometry_y.abs_diff(hypr.y) > HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
            {
                return false;
            }
            let g_scale = if g.scale_factor.is_finite() && g.scale_factor > 0.0 {
                g.scale_factor
            } else {
                1.0
            };
            let int_scaled_w = ((hypr.rotated_physical_width as f64) / g_scale).round() as i32;
            let int_scaled_h = ((hypr.rotated_physical_height as f64) / g_scale).round() as i32;
            let int_size_match = g.geometry_width.abs_diff(int_scaled_w)
                <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32
                && g.geometry_height.abs_diff(int_scaled_h)
                    <= HYPRLAND_GEOMETRY_MATCH_TOLERANCE_PX as u32;
            let id_match = gdk_identities
                .get(*idx)
                .is_some_and(|id| identity_matches_hyprland_monitor(hypr, id));
            int_size_match || id_match
        })
        .map(|(idx, _)| idx)
        .collect();
    select_unique_gdk_candidate(&fallback, hypr, gdk_monitors, gdk_identities)
}

fn resolve_cursor_monitor_in_snapshot<'a>(
    snapshot: &'a HyprlandClickSnapshot,
    gdk_monitors: &[LinuxMonitorWorkarea],
    gdk_identities: &[LinuxMonitorIdentity],
) -> Option<(&'a HyprlandLogicalMonitor, usize, usize)> {
    if !snapshot.cursor_x.is_finite()
        || !snapshot.cursor_y.is_finite()
        || (snapshot.cursor_x == 0.0 && snapshot.cursor_y == 0.0)
    {
        return None;
    }

    let mut containing = snapshot
        .monitors
        .iter()
        .filter(|mon| mon.contains_logical_point(snapshot.cursor_x, snapshot.cursor_y));
    let hypr_mon = containing.next()?;
    if containing.next().is_some() {
        // Overlapping / mirrored monitors at the cursor point are ambiguous; refuse to guess.
        return None;
    }

    let (gdk_slice_idx, gdk_mon_idx) =
        match_hyprland_monitor_to_gdk(hypr_mon, gdk_monitors, gdk_identities)?;
    Some((hypr_mon, gdk_slice_idx, gdk_mon_idx))
}

fn is_likely_panel_local_sni_point(
    raw_anchor: Option<RawTrayAnchor>,
    cursor_hypr_mon: &HyprlandLogicalMonitor,
    cursor_x: f64,
    cursor_y: f64,
) -> bool {
    let Some(anchor) = raw_anchor else {
        return false;
    };
    let has_usable_rect = anchor.tray_rect.is_some_and(|r| match r.size {
        RawSize2D::Physical { width, height } | RawSize2D::Logical { width, height } => {
            width.is_finite() && height.is_finite() && (width > 1.0 || height > 1.0)
        }
    });
    if has_usable_rect {
        return false;
    }
    let (sx, sy) = match anchor.click_position {
        Some(RawCoordinate2D::Physical { x, y } | RawCoordinate2D::Logical { x, y })
            if x.is_finite() && y.is_finite() =>
        {
            (x, y)
        }
        _ => return false,
    };
    if sx < 0.0 || sy < 0.0 {
        return false;
    }

    let cursor_local_x = cursor_x - (cursor_hypr_mon.x as f64);
    let cursor_local_y = cursor_y - (cursor_hypr_mon.y as f64);
    let mon_w = cursor_hypr_mon.logical_width as f64;
    let mon_h = cursor_hypr_mon.logical_height as f64;
    let scale = if cursor_hypr_mon.scale.is_finite() && cursor_hypr_mon.scale > 0.0 {
        cursor_hypr_mon.scale
    } else {
        1.0
    };

    let on_top_edge = cursor_local_y <= PANEL_LOCAL_EDGE_BAND_PX;
    let on_bottom_edge = cursor_local_y >= (mon_h - PANEL_LOCAL_EDGE_BAND_PX).max(0.0);
    let on_left_edge = cursor_local_x <= PANEL_LOCAL_EDGE_BAND_PX;
    let on_right_edge = cursor_local_x >= (mon_w - PANEL_LOCAL_EDGE_BAND_PX).max(0.0);
    if !(on_top_edge || on_bottom_edge || on_left_edge || on_right_edge) {
        return false;
    }

    let logical_tol = PANEL_LOCAL_COORD_MATCH_TOLERANCE_PX;
    let phys_tol = PANEL_LOCAL_COORD_MATCH_TOLERANCE_PX * scale.max(1.0);
    let phys_local_x = cursor_local_x * scale;
    let phys_local_y = cursor_local_y * scale;

    // 1. Monitor-local logical offset `(cursor_x - mon.x, cursor_y - mon.y)`.
    if (sx - cursor_local_x).abs() <= logical_tol && (sy - cursor_local_y).abs() <= logical_tol {
        return true;
    }

    // 2. Monitor-local physical offset `((cursor_x - mon.x) * scale, (cursor_y - mon.y) * scale)`.
    if (sx - phys_local_x).abs() <= phys_tol && (sy - phys_local_y).abs() <= phys_tol {
        return true;
    }

    // 3. Horizontal (top or bottom) bar-local coordinates where `x` tracks the bar length and `y`
    //    is within the bar thickness.
    let bar_thickness_limit = PANEL_LOCAL_EDGE_BAND_PX * scale.max(1.0);
    if (on_top_edge || on_bottom_edge)
        && sy <= bar_thickness_limit
        && ((sx - cursor_local_x).abs() <= logical_tol || (sx - phys_local_x).abs() <= phys_tol)
    {
        return true;
    }

    // 4. Vertical (left or right) bar-local coordinates where `y` tracks the bar length and `x` is
    //    within the bar thickness.
    if (on_left_edge || on_right_edge)
        && sx <= bar_thickness_limit
        && ((sy - cursor_local_y).abs() <= logical_tol || (sy - phys_local_y).abs() <= phys_tol)
    {
        return true;
    }

    // 5. Small icon/widget-local offset relative to the tray item itself while clicking a panel on
    //    a different monitor.
    sx <= WIDGET_LOCAL_MAX_OFFSET_PX && sy <= WIDGET_LOCAL_MAX_OFFSET_PX
}

/// Resolves whether a Linux Wayland tray activation carries a trusted global anchor, a trustworthy
/// activation-specific monitor hint, or should delegate output choice to the Wayland compositor
/// (`zwlr_layer_shell_v1` `NULL` output).
pub fn resolve_wayland_activation_input(
    source: PopoverActivationSource,
    gdk_monitors: &[LinuxMonitorWorkarea],
    gdk_identities: &[LinuxMonitorIdentity],
    raw_anchor: Option<RawTrayAnchor>,
    matched_sni_monitor_slice_idx: Option<usize>,
    hyprland_snapshot: Option<&HyprlandClickSnapshot>,
) -> ResolvedWaylandActivationInput {
    match source {
        PopoverActivationSource::MenuOpenStatus => {
            if raw_anchor.is_some() && matched_sni_monitor_slice_idx.is_some() {
                ResolvedWaylandActivationInput {
                    trusted_global_anchor: raw_anchor,
                    fallback_policy: LinuxFallbackMonitorPolicy::CompositorDefault {
                        sizing_monitor_hint: None,
                    },
                }
            } else {
                // Menu / keyboard invocations may lack a pointer; never pin `main`, `monitors[0]`,
                // or a stale click monitor.
                ResolvedWaylandActivationInput {
                    trusted_global_anchor: None,
                    fallback_policy: LinuxFallbackMonitorPolicy::CompositorDefault {
                        sizing_monitor_hint: None,
                    },
                }
            }
        }
        PopoverActivationSource::TrayIconClick => {
            let cursor_match = hyprland_snapshot.and_then(|snap| {
                resolve_cursor_monitor_in_snapshot(snap, gdk_monitors, gdk_identities)
            });

            match matched_sni_monitor_slice_idx {
                None => match cursor_match {
                    Some((_, _, gdk_mon_idx)) => ResolvedWaylandActivationInput {
                        trusted_global_anchor: None,
                        fallback_policy: LinuxFallbackMonitorPolicy::PinMonitor(gdk_mon_idx),
                    },
                    None => ResolvedWaylandActivationInput {
                        trusted_global_anchor: None,
                        fallback_policy: LinuxFallbackMonitorPolicy::CompositorDefault {
                            sizing_monitor_hint: None,
                        },
                    },
                },
                Some(sni_slice_idx) => {
                    if let (Some(snap), Some((hypr_mon, cursor_slice_idx, cursor_gdk_idx))) =
                        (hyprland_snapshot, cursor_match)
                    {
                        if cursor_slice_idx != sni_slice_idx
                            && is_likely_panel_local_sni_point(
                                raw_anchor,
                                hypr_mon,
                                snap.cursor_x,
                                snap.cursor_y,
                            )
                        {
                            return ResolvedWaylandActivationInput {
                                trusted_global_anchor: None,
                                fallback_policy: LinuxFallbackMonitorPolicy::PinMonitor(
                                    cursor_gdk_idx,
                                ),
                            };
                        }
                    }

                    ResolvedWaylandActivationInput {
                        trusted_global_anchor: raw_anchor,
                        fallback_policy: LinuxFallbackMonitorPolicy::CompositorDefault {
                            sizing_monitor_hint: None,
                        },
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tray::RawTrayRect;

    fn real_dp1_dp2_gdk_monitors(dp2_gdk_scale: f64) -> Vec<LinuxMonitorWorkarea> {
        vec![
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: 0,
                geometry_y: 1632,
                geometry_width: 2560,
                geometry_height: 1440,
                workarea_x: 0,
                workarea_y: 1632,
                workarea_width: 2560,
                workarea_height: 1440,
                scale_factor: 1.0,
                is_primary: true,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 2560,
                geometry_y: 0,
                geometry_width: 1728,
                geometry_height: 3072,
                workarea_x: 2560,
                workarea_y: 0,
                workarea_width: 1728,
                workarea_height: 3072,
                scale_factor: dp2_gdk_scale,
                is_primary: false,
            },
        ]
    }

    fn real_dp1_dp2_hyprland_monitors_json() -> &'static str {
        r#"[
            {
                "id": 0,
                "name": "DP-1",
                "make": "Dell Inc.",
                "model": "U2724D",
                "width": 2560,
                "height": 1440,
                "x": 0,
                "y": 1632,
                "scale": 1.0,
                "transform": 0,
                "focused": true,
                "disabled": false
            },
            {
                "id": 1,
                "name": "DP-2",
                "make": "LG Electronics",
                "model": "27GN950",
                "width": 3840,
                "height": 2160,
                "x": 2560,
                "y": 0,
                "scale": 1.25,
                "transform": 3,
                "focused": false,
                "disabled": false
            },
            {
                "id": 2,
                "name": "HDMI-A-1",
                "width": 1920,
                "height": 1080,
                "x": 0,
                "y": 0,
                "scale": 1.0,
                "transform": 0,
                "focused": false,
                "disabled": true
            }
        ]"#
    }

    #[test]
    fn hyprland_env_confirmation_rejects_sway_foreign_desktops_and_path_traversal() {
        let valid = HyprlandEnvSnapshot {
            xdg_runtime_dir: Some(PathBuf::from("/run/user/1000")),
            hyprland_instance_signature: Some("abc123_1700000000_999".to_string()),
            xdg_current_desktop: Some("Hyprland".to_string()),
            desktop_session: None,
            wayland_display: Some("wayland-1".to_string()),
            sway_sock: None,
        };
        assert!(is_confirmed_hyprland_env(&valid));
        assert_eq!(
            resolve_confirmed_hyprland_socket_path(&valid),
            Some(PathBuf::from(
                "/run/user/1000/hypr/abc123_1700000000_999/.socket.sock"
            ))
        );

        let colon_desktop = HyprlandEnvSnapshot {
            xdg_current_desktop: Some("wlroots:Hyprland".to_string()),
            ..valid.clone()
        };
        assert!(is_confirmed_hyprland_env(&colon_desktop));

        // Leaked HYPRLAND_INSTANCE_SIGNATURE inside Sway or GNOME must be rejected.
        for foreign_desktop in ["sway", "GNOME", "KDE", "niri"] {
            let foreign = HyprlandEnvSnapshot {
                xdg_current_desktop: Some(foreign_desktop.to_string()),
                ..valid.clone()
            };
            assert!(
                !is_confirmed_hyprland_env(&foreign),
                "foreign desktop {foreign_desktop} must never confirm Hyprland"
            );
            assert_eq!(resolve_confirmed_hyprland_socket_path(&foreign), None);
        }

        // Presence of SWAYSOCK refuses Hyprland even if DESKTOP_SESSION mentions hyprland.
        let sway_sock_set = HyprlandEnvSnapshot {
            xdg_current_desktop: None,
            desktop_session: Some("hyprland".to_string()),
            sway_sock: Some("/run/user/1000/sway-ipc.sock".to_string()),
            ..valid.clone()
        };
        assert!(!is_confirmed_hyprland_env(&sway_sock_set));

        // Unset desktop identifiers refuse unconfirmed Hyprland.
        let unconfirmed = HyprlandEnvSnapshot {
            xdg_current_desktop: None,
            desktop_session: None,
            ..valid.clone()
        };
        assert!(!is_confirmed_hyprland_env(&unconfirmed));

        // Unsafe signatures, wayland display, or relative runtime dirs are rejected.
        for bad_sig in ["", "..", ".", "../other", "a/b", "sig with space", "sig\n"] {
            let bad = HyprlandEnvSnapshot {
                hyprland_instance_signature: Some(bad_sig.to_string()),
                ..valid.clone()
            };
            assert_eq!(resolve_confirmed_hyprland_socket_path(&bad), None);
        }
        let bad_wayland = HyprlandEnvSnapshot {
            wayland_display: Some("../wayland-0".to_string()),
            ..valid.clone()
        };
        assert_eq!(resolve_confirmed_hyprland_socket_path(&bad_wayland), None);

        let bad_runtime = HyprlandEnvSnapshot {
            xdg_runtime_dir: Some(PathBuf::from("relative/runtime")),
            ..valid
        };
        assert_eq!(resolve_confirmed_hyprland_socket_path(&bad_runtime), None);
    }

    #[test]
    fn hyprland_rotated_270_mixed_scale_dp1_dp2_geometry_and_negative_origins_match_uniquely() {
        let hypr_monitors =
            parse_hyprland_monitors(real_dp1_dp2_hyprland_monitors_json().as_bytes())
                .expect("parse real DP-1/DP-2 Hyprland monitors");
        assert_eq!(hypr_monitors.len(), 2, "disabled HDMI-A-1 must be filtered");

        let dp1 = &hypr_monitors[0];
        assert_eq!(
            (dp1.x, dp1.y, dp1.logical_width, dp1.logical_height),
            (0, 1632, 2560, 1440)
        );
        let dp2 = &hypr_monitors[1];
        assert_eq!(
            (
                dp2.x,
                dp2.y,
                dp2.rotated_physical_width,
                dp2.rotated_physical_height,
                dp2.logical_width,
                dp2.logical_height
            ),
            (2560, 0, 2160, 3840, 1728, 3072)
        );

        // GTK3 reports integer scale 2.0 on fractional 1.25x Wayland outputs while xdg_output
        // provides logical 1728x3072 geometry: matching must succeed cleanly.
        let gdk_monitors = real_dp1_dp2_gdk_monitors(2.0);
        let identities = vec![
            LinuxMonitorIdentity {
                manufacturer: Some("Dell Inc.".to_string()),
                model: Some("U2724D".to_string()),
            },
            LinuxMonitorIdentity {
                manufacturer: Some("LG Electronics".to_string()),
                model: Some("27GN950".to_string()),
            },
        ];
        assert_eq!(
            match_hyprland_monitor_to_gdk(dp1, &gdk_monitors, &identities),
            Some((0, 0))
        );
        assert_eq!(
            match_hyprland_monitor_to_gdk(dp2, &gdk_monitors, &identities),
            Some((1, 1))
        );

        // Even if GDK lacks xdg_output logical size and reports integer-divided 1080x1920 @ 2x,
        // Pass 3 matches DP-2 uniquely by origin + rotated physical / gdk_scale.
        let mut gdk_without_xdg_output = real_dp1_dp2_gdk_monitors(2.0);
        gdk_without_xdg_output[1].geometry_width = 1080;
        gdk_without_xdg_output[1].geometry_height = 1920;
        assert_eq!(
            match_hyprland_monitor_to_gdk(dp2, &gdk_without_xdg_output, &[]),
            Some((1, 1))
        );

        // Negative logical origins and 90-degree rotated monitors also match accurately.
        let neg_json = r#"[
            {
                "name": "DP-LEFT",
                "width": 1920,
                "height": 1080,
                "x": -1920,
                "y": -200,
                "scale": 1.0,
                "transform": 0
            },
            {
                "name": "DP-ROT90",
                "width": 3840,
                "height": 2160,
                "x": 0,
                "y": -400,
                "scale": 1.5,
                "transform": 1
            }
        ]"#;
        let neg_hypr = parse_hyprland_monitors(neg_json.as_bytes()).expect("parse negative layout");
        assert_eq!(
            (neg_hypr[1].logical_width, neg_hypr[1].logical_height),
            (1440, 2560)
        );
        let neg_gdk = vec![
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: -1920,
                geometry_y: -200,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: -1920,
                workarea_y: -200,
                workarea_width: 1920,
                workarea_height: 1080,
                scale_factor: 1.0,
                is_primary: false,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 0,
                geometry_y: -400,
                geometry_width: 1440,
                geometry_height: 2560,
                workarea_x: 0,
                workarea_y: -400,
                workarea_width: 1440,
                workarea_height: 2560,
                scale_factor: 2.0,
                is_primary: true,
            },
        ];
        let snap_left = HyprlandClickSnapshot {
            cursor_x: -120.0,
            cursor_y: -180.0,
            monitors: neg_hypr,
        };
        let resolved_left = resolve_wayland_activation_input(
            PopoverActivationSource::TrayIconClick,
            &neg_gdk,
            &[],
            None,
            None,
            Some(&snap_left),
        );
        assert_eq!(resolved_left.trusted_global_anchor, None);
        assert_eq!(
            resolved_left.fallback_policy,
            LinuxFallbackMonitorPolicy::PinMonitor(0)
        );
    }

    #[test]
    fn hyprland_panel_local_disambiguation_preserves_global_anchors_and_menu_null_output() {
        let monitors = vec![
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 2560,
                geometry_height: 1440,
                workarea_x: 0,
                workarea_y: 0,
                workarea_width: 2560,
                workarea_height: 1440,
                scale_factor: 1.0,
                is_primary: true,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 2560,
                geometry_y: 0,
                geometry_width: 1728,
                geometry_height: 3072,
                workarea_x: 2560,
                workarea_y: 0,
                workarea_width: 1728,
                workarea_height: 3072,
                scale_factor: 2.0,
                is_primary: false,
            },
        ];
        let hypr_monitors = vec![
            HyprlandLogicalMonitor {
                name: "DP-1".to_string(),
                make: String::new(),
                model: String::new(),
                x: 0,
                y: 0,
                rotated_physical_width: 2560,
                rotated_physical_height: 1440,
                logical_width: 2560,
                logical_height: 1440,
                scale: 1.0,
            },
            HyprlandLogicalMonitor {
                name: "DP-2".to_string(),
                make: String::new(),
                model: String::new(),
                x: 2560,
                y: 0,
                rotated_physical_width: 2160,
                rotated_physical_height: 3840,
                logical_width: 1728,
                logical_height: 3072,
                scale: 1.25,
            },
        ];
        let zero_rect = Some(RawTrayRect {
            position: RawCoordinate2D::Physical { x: 0.0, y: 0.0 },
            size: RawSize2D::Physical {
                width: 0.0,
                height: 0.0,
            },
        });

        // 1. Panel-local (1600, 18) emitted from DP-2 top bar collides with DP-1 (0, 0) but
        //    matches cursor local (4160 - 2560 = 1600, 18) on DP-2 -> disambiguated to DP-2
        //    fallback (never faked IconAnchored on DP-1 or DP-2).
        let local_dp2_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 1600.0, y: 18.0 }),
            tray_rect: zero_rect,
        };
        let snap_dp2_top = HyprlandClickSnapshot {
            cursor_x: 4160.0,
            cursor_y: 18.0,
            monitors: hypr_monitors.clone(),
        };
        let disambiguated = resolve_wayland_activation_input(
            PopoverActivationSource::TrayIconClick,
            &monitors,
            &[],
            Some(local_dp2_anchor),
            Some(0),
            Some(&snap_dp2_top),
        );
        assert_eq!(disambiguated.trusted_global_anchor, None);
        assert_eq!(
            disambiguated.fallback_policy,
            LinuxFallbackMonitorPolicy::PinMonitor(1)
        );

        // 2. Bottom-bar local (800, 14) on DP-2 when cursor is at (3360, 3058) -> disambiguated.
        let local_dp2_bottom = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 800.0, y: 14.0 }),
            tray_rect: zero_rect,
        };
        let snap_dp2_bottom = HyprlandClickSnapshot {
            cursor_x: 3360.0,
            cursor_y: 3058.0,
            monitors: hypr_monitors.clone(),
        };
        let disambiguated_bottom = resolve_wayland_activation_input(
            PopoverActivationSource::TrayIconClick,
            &monitors,
            &[],
            Some(local_dp2_bottom),
            Some(0),
            Some(&snap_dp2_bottom),
        );
        assert_eq!(disambiguated_bottom.trusted_global_anchor, None);
        assert_eq!(
            disambiguated_bottom.fallback_policy,
            LinuxFallbackMonitorPolicy::PinMonitor(1)
        );

        // 3. Genuine global anchor on DP-1 (400, 18) triggered via keyboard while cursor sits in
        //    the middle of DP-2 (3400, 1500) -> NOT overridden, honored as trusted global anchor!
        let global_dp1_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 400.0, y: 18.0 }),
            tray_rect: zero_rect,
        };
        let snap_dp2_mid = HyprlandClickSnapshot {
            cursor_x: 3400.0,
            cursor_y: 1500.0,
            monitors: hypr_monitors.clone(),
        };
        let preserved_global = resolve_wayland_activation_input(
            PopoverActivationSource::TrayIconClick,
            &monitors,
            &[],
            Some(global_dp1_anchor),
            Some(0),
            Some(&snap_dp2_mid),
        );
        assert_eq!(
            preserved_global.trusted_global_anchor,
            Some(global_dp1_anchor)
        );

        // 4. Uninitialized (0, 0) cursor in Hyprland (no pointer) yields CompositorDefault.
        let snap_zero_cursor = HyprlandClickSnapshot {
            cursor_x: 0.0,
            cursor_y: 0.0,
            monitors: hypr_monitors.clone(),
        };
        let no_pointer = resolve_wayland_activation_input(
            PopoverActivationSource::TrayIconClick,
            &monitors,
            &[],
            None,
            None,
            Some(&snap_zero_cursor),
        );
        assert_eq!(no_pointer.trusted_global_anchor, None);
        assert_eq!(
            no_pointer.fallback_policy,
            LinuxFallbackMonitorPolicy::CompositorDefault {
                sizing_monitor_hint: None,
            }
        );

        // 5. MenuOpenStatus never relies on pointer cursor snapshot -> CompositorDefault.
        let menu_open = resolve_wayland_activation_input(
            PopoverActivationSource::MenuOpenStatus,
            &monitors,
            &[],
            None,
            None,
            Some(&snap_dp2_top),
        );
        assert_eq!(menu_open.trusted_global_anchor, None);
        assert_eq!(
            menu_open.fallback_policy,
            LinuxFallbackMonitorPolicy::CompositorDefault {
                sizing_monitor_hint: None,
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_socket_hyprland_query_reads_valid_json_and_bounds_slow_oversized_or_symlink_sockets() {
        use std::os::unix::net::UnixListener;
        use std::thread;

        let tmp = tempfile::TempDir::new().expect("tempdir");
        let sig = "test_sig_123";
        let instance_dir = tmp.path().join("hypr").join(sig);
        std::fs::create_dir_all(&instance_dir).expect("create hypr instance dir");
        let sock_path = instance_dir.join(".socket.sock");

        let env = HyprlandEnvSnapshot {
            xdg_runtime_dir: Some(tmp.path().to_path_buf()),
            hyprland_instance_signature: Some(sig.to_string()),
            xdg_current_desktop: Some("Hyprland".to_string()),
            desktop_session: None,
            wayland_display: Some("wayland-1".to_string()),
            sway_sock: None,
        };

        // 1. Valid read-only roundtrip over real Unix domain socket.
        let listener = UnixListener::bind(&sock_path).expect("bind unix socket");
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut cmd = [0u8; 64];
                let n = stream.read(&mut cmd).expect("read cmd");
                match &cmd[..n] {
                    b"j/cursorpos" => {
                        stream
                            .write_all(br#"{"x":3400,"y":18}"#)
                            .expect("write pos");
                    }
                    b"j/monitors" => {
                        stream
                            .write_all(real_dp1_dp2_hyprland_monitors_json().as_bytes())
                            .expect("write monitors");
                    }
                    other => panic!("unexpected command: {other:?}"),
                }
            }
        });

        let snap = query_confirmed_hyprland_click_snapshot(
            &env,
            Duration::from_millis(250),
            Duration::from_millis(500),
        )
        .expect("valid Hyprland click snapshot");
        server.join().expect("server thread");

        assert_eq!((snap.cursor_x, snap.cursor_y), (3400.0, 18.0));
        assert_eq!(snap.monitors.len(), 2);
        let dp2 = &snap.monitors[1];
        assert_eq!(dp2.name, "DP-2");
        assert_eq!((dp2.logical_width, dp2.logical_height), (1728, 3072));

        // 2. Oversized response is rejected fail-closed.
        let _ = std::fs::remove_file(&sock_path);
        let listener_big = UnixListener::bind(&sock_path).expect("bind oversized socket");
        let server_big = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener_big.accept() {
                let mut cmd = [0u8; 64];
                let _ = stream.read(&mut cmd);
                let _ = stream.write_all(&vec![b' '; MAX_HYPRLAND_CURSOR_RESPONSE_BYTES + 128]);
            }
        });
        assert_eq!(
            query_confirmed_hyprland_click_snapshot(
                &env,
                Duration::from_millis(200),
                Duration::from_millis(400),
            ),
            None
        );
        server_big.join().expect("oversized server");

        // 3. Stalling socket times out cleanly within bounded timeout.
        let _ = std::fs::remove_file(&sock_path);
        let listener_slow = UnixListener::bind(&sock_path).expect("bind slow socket");
        let server_slow = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener_slow.accept() {
                let mut cmd = [0u8; 64];
                let _ = stream.read(&mut cmd);
                thread::sleep(Duration::from_millis(160));
            }
        });
        let started = Instant::now();
        assert_eq!(
            query_confirmed_hyprland_click_snapshot(
                &env,
                Duration::from_millis(40),
                Duration::from_millis(80),
            ),
            None
        );
        assert!(
            started.elapsed() < Duration::from_millis(150),
            "slow socket must time out promptly"
        );
        server_slow.join().expect("slow server");

        // 4. Symlinked socket is rejected before connect.
        let _ = std::fs::remove_file(&sock_path);
        let real_target = tmp.path().join("other.sock");
        let _target_listener = UnixListener::bind(&real_target).expect("bind target");
        std::os::unix::fs::symlink(&real_target, &sock_path).expect("symlink socket");
        assert_eq!(
            query_confirmed_hyprland_click_snapshot(
                &env,
                Duration::from_millis(200),
                Duration::from_millis(400),
            ),
            None
        );
    }
}
