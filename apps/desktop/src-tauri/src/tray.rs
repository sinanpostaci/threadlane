#[path = "tray_linux.rs"]
mod tray_linux;

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::image::Image;
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};
#[cfg(not(target_os = "linux"))]
use tauri::LogicalSize;
use tauri::{
    AppHandle, Emitter, Manager, PhysicalPosition, State, WebviewUrl, WebviewWindow,
    WebviewWindowBuilder, Window, WindowEvent,
};

use crate::AppError;

/// Canonical label of the primary Threadlane desktop window.
pub const MAIN_WINDOW_LABEL: &str = "main";

/// Canonical label of the frameless system tray status popover window.
pub const TRAY_WINDOW_LABEL: &str = "tray";

/// Shared frontend route loaded by the hidden tray popover window.
pub const TRAY_WINDOW_URL: &str = "index.html?surface=tray";

/// Event emitted when the tray popover window is opened.
pub const TRAY_OPENED_EVENT: &str = "threadlane:tray-opened";

/// Event emitted when persisted tray preferences or tray availability changes.
pub const TRAY_PREFERENCES_CHANGED_EVENT: &str = "threadlane:tray-preferences-changed";

/// Stable identifier for the single application tray icon.
pub const TRAY_ICON_ID: &str = "threadlane-tray";

/// Tray menu item identifier for opening the compact status popover.
pub const MENU_ID_OPEN_STATUS: &str = "tray_open_status";

/// Tray menu item identifier for restoring the main Threadlane window.
pub const MENU_ID_OPEN_MAIN: &str = "tray_open_main";

/// Tray menu item identifier for terminating the desktop application.
pub const MENU_ID_QUIT: &str = "tray_quit";

/// Tray menu label for opening the compact status popover.
pub const MENU_LABEL_OPEN_STATUS: &str = "Open status";

/// Tray menu label for opening the main Threadlane window.
pub const MENU_LABEL_OPEN_MAIN: &str = "Open Threadlane";

/// Tray menu label for quitting the desktop application.
pub const MENU_LABEL_QUIT: &str = "Quit";

/// Initial logical width of the tray popover window in pixels.
pub const TRAY_POPOVER_WIDTH: f64 = 360.0;

/// Initial logical height of the tray popover window in pixels, fitted to the compact 360x320
/// status popup contract (up to [`TRAY_MAX_CONTRACT_ROWS`] concise two-line rows) without carrying
/// over [`MAIN_WINDOW_MIN_HEIGHT`].
pub const TRAY_POPOVER_HEIGHT: f64 = 320.0;

/// Compact minimum logical width floor for constrained display workareas.
pub const TRAY_POPOVER_MIN_WIDTH: f64 = 280.0;

/// Compact minimum logical height floor for constrained display workareas.
pub const TRAY_POPOVER_MIN_HEIGHT: f64 = 240.0;

/// Maximum status rows displayed by the shared tray popover frontend contract (`TRAY_MAX_ROWS`).
pub const TRAY_MAX_CONTRACT_ROWS: usize = 3;

/// Minimum logical height configured on the primary `main` window (`tauri.conf.json`), which must
/// never be inherited by or enforced upon the compact `tray` popover window.
pub const MAIN_WINDOW_MIN_HEIGHT: f64 = 500.0;

const _: () = assert!(
    TRAY_MAX_CONTRACT_ROWS == 3
        && (TRAY_POPOVER_HEIGHT as u32) < (MAIN_WINDOW_MIN_HEIGHT as u32)
        && (TRAY_POPOVER_MIN_HEIGHT as u32) < (MAIN_WINDOW_MIN_HEIGHT as u32)
);

/// Relative path to the parent-rendered monochrome PNG icon used as a macOS template image.
pub const MACOS_TRAY_ICON_REL_PATH: &str = "icons/tray.png";

/// Bounded app-owned file name under `app_data_dir` for persisted tray preferences.
pub const TRAY_PREFERENCES_FILENAME: &str = "tray-preferences.json";

/// Cross-process lock file name under `app_data_dir` for tray preferences mutations.
pub const TRAY_PREFERENCES_LOCK_FILENAME: &str = "tray-preferences.lock";

/// Schema version for [`StoredTrayPreferences`].
pub const TRAY_PREFERENCES_SCHEMA_VERSION: u32 = 1;

/// Maximum permitted byte size for `tray-preferences.json`.
pub const MAX_TRAY_PREFERENCES_FILE_BYTES: u64 = 4096;

/// Lock file name under `app_data_dir` used to coordinate single-instance desktop startup.
pub const SINGLE_INSTANCE_LOCK_FILENAME: &str = "desktop-instance.lock";

/// Endpoint discovery file name under `app_data_dir` for single-instance activation.
pub const SINGLE_INSTANCE_ENDPOINT_FILENAME: &str = "desktop-instance.json";

/// Schema version for the single-instance endpoint file.
const SINGLE_INSTANCE_SCHEMA_VERSION: u32 = 1;

/// Maximum permitted byte size for `desktop-instance.json` and activation wire payloads.
const MAX_SINGLE_INSTANCE_BYTES: u64 = 4096;

/// Grace window after opening the popover during which transient focus-loss events are ignored.
pub const POPOVER_BLUR_GRACE_PERIOD: Duration = Duration::from_millis(180);

/// Window after an automatic focus-loss hide during which an immediately arriving tray left-click
/// is treated as completing that close toggle rather than reopening the popover.
pub const POPOVER_BLUR_REOPEN_SUPPRESSION: Duration = Duration::from_millis(250);

/// Minimum interval between consecutive left-click toggle events to prevent duplicate backend triggers.
pub const TRAY_CLICK_DEBOUNCE: Duration = Duration::from_millis(90);

#[cfg(not(target_os = "linux"))]
const NON_LINUX_MISSING_COORDS_NOTICE: &str = "Tray host did not provide icon coordinates; status popup used fallback monitor placement instead of anchoring to the tray icon.";

const TRAY_ESCAPE_INIT_SCRIPT: &str = r#"
window.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && !event.defaultPrevented) {
    if (window.__TAURI_INTERNALS__ && typeof window.__TAURI_INTERNALS__.invoke === 'function') {
      window.__TAURI_INTERNALS__.invoke('desktop_tray_action', { action: 'hide' }).catch(() => {});
    }
  }
});
"#;

/// User-configurable behavior when the main Threadlane window receives a close request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CloseBehavior {
    /// Hide the main window to the system tray when the tray is available.
    #[default]
    HideToTray,
    /// Terminate the desktop UI process when closing the main window.
    Quit,
}

/// Typed request payload for `desktop_set_tray_preferences`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetTrayPreferencesRequest {
    pub close_behavior: CloseBehavior,
}

/// Typed response payload for `desktop_tray_preferences`, `desktop_set_tray_preferences`,
/// and the `threadlane:tray-opened` / `threadlane:tray-preferences-changed` events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrayPreferencesResponse {
    pub close_behavior: CloseBehavior,
    pub tray_available: bool,
    pub message: Option<String>,
}

/// Supported actions for the `desktop_tray_action` IPC command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrayAction {
    ShowMain,
    Hide,
    Quit,
}

fn default_preferences_schema_version() -> u32 {
    TRAY_PREFERENCES_SCHEMA_VERSION
}

/// Bounded on-disk representation of user tray preferences under `app_data_dir`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredTrayPreferences {
    #[serde(default = "default_preferences_schema_version")]
    pub schema_version: u32,
    pub close_behavior: CloseBehavior,
}

/// Result of loading `tray-preferences.json` at desktop startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialPreferencesLoad {
    pub close_behavior: CloseBehavior,
    pub warning: Option<String>,
}

/// Pure decision outcome when the `main` window receives a close request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainCloseDecision {
    /// Prevent window destruction and hide `main` to the system tray.
    HideMainToTray,
    /// Terminate the desktop application and close any hidden `tray` popover window.
    QuitApp,
}

/// Pure decision outcome when the `tray` popover receives a close or blur event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopoverDismissDecision {
    /// Prevent window destruction and hide only the `tray` popover.
    HidePopoverOnly,
    /// Ignore a transient focus-loss event immediately following popover activation.
    IgnoreTransientBlur,
    /// Allow the `tray` popover window to close because explicit app quit is in progress.
    AllowCloseForQuit,
}

/// Pure recovery decision when system tray availability changes at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayAvailabilityRecoveryDecision {
    /// No window visibility recovery is required.
    NoAction,
    /// Restore, unminimize, and focus the hidden `main` window so the app never stays unreachable.
    RestoreHiddenMain,
}

/// Physical rectangle of a tray icon in screen pixels.
#[cfg(any(not(target_os = "linux"), test))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TrayRectPx {
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
}

/// Physical bounds and DPI scale factor of a display monitor.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MonitorBoundsPx {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
    pub scale_factor: f64,
}

impl Default for MonitorBoundsPx {
    fn default() -> Self {
        Self {
            x: 0,
            y: 0,
            width: 1280,
            height: 800,
            scale_factor: 1.0,
        }
    }
}

/// Raw 2D coordinate preserving whether the toolkit supplied physical or logical units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawCoordinate2D {
    Physical { x: f64, y: f64 },
    Logical { x: f64, y: f64 },
}

impl RawCoordinate2D {
    pub fn from_tauri_position(position: tauri::Position) -> Self {
        match position {
            tauri::Position::Physical(pos) => Self::Physical {
                x: pos.x as f64,
                y: pos.y as f64,
            },
            tauri::Position::Logical(pos) => Self::Logical { x: pos.x, y: pos.y },
        }
    }
}

/// Raw 2D size preserving whether the toolkit supplied physical or logical units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawSize2D {
    Physical { width: f64, height: f64 },
    Logical { width: f64, height: f64 },
}

impl RawSize2D {
    pub fn from_tauri_size(size: tauri::Size) -> Self {
        match size {
            tauri::Size::Physical(sz) => Self::Physical {
                width: sz.width as f64,
                height: sz.height as f64,
            },
            tauri::Size::Logical(sz) => Self::Logical {
                width: sz.width,
                height: sz.height,
            },
        }
    }
}

/// Raw tray icon rectangle captured before monitor-specific scale conversion.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawTrayRect {
    pub position: RawCoordinate2D,
    pub size: RawSize2D,
}

impl RawTrayRect {
    pub fn from_tauri_rect(rect: &tauri::Rect) -> Self {
        Self {
            position: RawCoordinate2D::from_tauri_position(rect.position),
            size: RawSize2D::from_tauri_size(rect.size),
        }
    }
}

/// Raw activation coordinates captured from a tray icon click event prior to monitor resolution.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RawTrayAnchor {
    pub click_position: Option<RawCoordinate2D>,
    pub tray_rect: Option<RawTrayRect>,
}

impl RawTrayAnchor {
    pub fn from_click_event(position: PhysicalPosition<f64>, rect: &tauri::Rect) -> Self {
        Self {
            click_position: Some(RawCoordinate2D::Physical {
                x: position.x,
                y: position.y,
            }),
            tray_rect: Some(RawTrayRect::from_tauri_rect(rect)),
        }
    }
}

/// Resolved target monitor and physical anchor coordinates computed using that monitor's own scale.
#[cfg(any(not(target_os = "linux"), test))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedTrayAnchor {
    pub monitor: MonitorBoundsPx,
    pub click_position_px: Option<(f64, f64)>,
    pub tray_rect_px: Option<TrayRectPx>,
    pub has_valid_anchor: bool,
}

/// Reason why a popover open operation could not anchor directly to tray icon coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnanchoredPlacementReason {
    /// Tray host omitted icon coordinates (e.g. `(0, 0)` sentinel or context-menu open).
    MissingTrayCoordinates,
    /// Wayland `xdg_toplevel` surfaces do not permit client-driven tray-anchored placement.
    #[cfg(any(target_os = "linux", test))]
    WaylandCompositorManaged,
    /// Both valid tray coordinates and Wayland client positioning support are absent.
    #[cfg(any(target_os = "linux", test))]
    WaylandMissingCoordinatesAndCompositorSupport,
    /// Display backend does not expose client window positioning.
    #[cfg(any(target_os = "linux", test))]
    BackendUnsupported,
}

/// Truthful classification of how the tray status popover was placed on screen.
#[derive(Debug, Clone, PartialEq)]
pub enum PopoverPlacementOutcome {
    /// Positioned relative to real tray icon geometry or activation coordinates on a backend that
    /// honors client window coordinates.
    IconAnchored { x: i32, y: i32, scale_factor: f64 },
    /// Positioned at a safe monitor fallback corner because valid icon coordinates were absent.
    /// Explicitly not counted as genuine icon-anchored placement.
    FallbackMonitorCorner {
        x: i32,
        y: i32,
        scale_factor: f64,
        reason: UnanchoredPlacementReason,
        notice: String,
    },
    /// Compositor or window system controls placement (e.g. Wayland `xdg_toplevel`); `set_position`
    /// is not treated as icon-anchored placement.
    #[cfg(any(target_os = "linux", test))]
    CompositorManaged {
        scale_factor: f64,
        has_activation_coordinates: bool,
        reason: UnanchoredPlacementReason,
        notice: String,
    },
}

impl PopoverPlacementOutcome {
    /// Returns `true` strictly when the window was positioned using real tray icon/activation
    /// coordinates on a display backend that honors client positioning.
    pub fn is_icon_anchored(&self) -> bool {
        matches!(self, Self::IconAnchored { .. })
    }

    /// Returns the human-readable diagnostic message when icon-anchored placement was unavailable.
    pub fn diagnostic_notice(&self) -> Option<&str> {
        match self {
            Self::IconAnchored { .. } => None,
            Self::FallbackMonitorCorner { notice, .. } => Some(notice.as_str()),
            #[cfg(any(target_os = "linux", test))]
            Self::CompositorManaged { notice, .. } => Some(notice.as_str()),
        }
    }

    /// Returns the scale factor of the monitor selected for this open operation.
    pub fn scale_factor(&self) -> f64 {
        match self {
            Self::IconAnchored { scale_factor, .. }
            | Self::FallbackMonitorCorner { scale_factor, .. } => *scale_factor,
            #[cfg(any(target_os = "linux", test))]
            Self::CompositorManaged { scale_factor, .. } => *scale_factor,
        }
    }

    /// Validates structural truthfulness invariants across placement variants.
    pub fn validate_invariants(&self) -> bool {
        let valid_scale = self.scale_factor().is_finite() && self.scale_factor() > 0.0;
        if !valid_scale {
            return false;
        }
        match self {
            Self::IconAnchored { x, y, .. } => {
                self.is_icon_anchored()
                    && self.diagnostic_notice().is_none()
                    && *x > i32::MIN
                    && *y > i32::MIN
            }
            Self::FallbackMonitorCorner {
                x,
                y,
                reason,
                notice,
                ..
            } => {
                !self.is_icon_anchored()
                    && !notice.is_empty()
                    && matches!(reason, UnanchoredPlacementReason::MissingTrayCoordinates)
                    && *x > i32::MIN
                    && *y > i32::MIN
            }
            #[cfg(any(target_os = "linux", test))]
            Self::CompositorManaged {
                has_activation_coordinates,
                reason,
                notice,
                ..
            } => {
                let _ = *has_activation_coordinates;
                !self.is_icon_anchored()
                    && !notice.is_empty()
                    && !matches!(reason, UnanchoredPlacementReason::MissingTrayCoordinates)
            }
        }
    }
}

/// Origin of a request to open the tray status popover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopoverActivationSource {
    /// Triggered by left-clicking / activating the tray icon.
    TrayIconClick,
    /// Triggered from the context menu (`Open status`) where no fresh icon click coordinates exist.
    MenuOpenStatus,
}

/// Pure lifecycle decision when a tray icon click event arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayToggleDecision {
    /// Ignore non-left click, pre-release event on macOS, or duplicate bounce within debounce window.
    IgnoreClick,
    /// Popover is currently visible; hide it cleanly.
    HideVisiblePopover,
    /// Popover was just auto-hidden by focus loss milliseconds before this click arrived; keep it
    /// closed rather than immediately reopening.
    SuppressReopenAfterBlur,
    /// Popover is hidden; open and present it.
    OpenPopover,
}

/// Input bundle for evaluating [`TrayState::evaluate_tray_click_toggle`].
#[derive(Debug, Clone, Copy)]
pub struct TrayClickEvaluationInput {
    pub button: MouseButton,
    pub button_state: MouseButtonState,
    pub is_linux: bool,
    pub popover_currently_visible: bool,
    pub raw_anchor: RawTrayAnchor,
    pub now: Instant,
}

/// Normalized startup argument fingerprint used to detect conflicting second-instance launches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchFingerprint {
    pub state_dir: Option<PathBuf>,
    pub demo_state_dir: Option<PathBuf>,
    pub registry_dir: PathBuf,
    pub host_executable: Option<PathBuf>,
}

impl LaunchFingerprint {
    pub fn new(
        state_dir: Option<PathBuf>,
        demo_state_dir: Option<PathBuf>,
        registry_dir: PathBuf,
        host_executable: Option<PathBuf>,
    ) -> Self {
        Self {
            state_dir: state_dir.map(|p| normalize_path_for_comparison(&p)),
            demo_state_dir: demo_state_dir.map(|p| normalize_path_for_comparison(&p)),
            registry_dir: normalize_path_for_comparison(&registry_dir),
            host_executable: host_executable.map(|p| normalize_path_for_comparison(&p)),
        }
    }
}

fn normalize_path_for_comparison(path: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(path) {
        return canonical;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if normalized.as_os_str().is_empty() {
        path.to_path_buf()
    } else {
        normalized
    }
}

/// Evaluates whether a second launch's CLI arguments are compatible with the running instance.
///
/// Never silently retargets the running instance when runtime, demo, registry, or host executable
/// arguments differ.
pub fn evaluate_second_launch_compatibility(
    running: &LaunchFingerprint,
    incoming: &LaunchFingerprint,
) -> Result<(), String> {
    if running.state_dir != incoming.state_dir {
        return Err(
            "Another Threadlane desktop instance is already running with a different --state-dir; refusing to retarget existing instance."
                .to_string(),
        );
    }
    if running.demo_state_dir != incoming.demo_state_dir {
        return Err(
            "Another Threadlane desktop instance is already running with a different --demo-state-dir; refusing to retarget existing instance."
                .to_string(),
        );
    }
    if running.registry_dir != incoming.registry_dir {
        return Err(
            "Another Threadlane desktop instance is already running with a different --registry-dir; refusing to retarget existing instance."
                .to_string(),
        );
    }
    if running.host_executable != incoming.host_executable {
        return Err(
            "Another Threadlane desktop instance is already running with a different --host-executable; refusing to retarget existing instance."
                .to_string(),
        );
    }
    Ok(())
}

/// Pure decision function for handling a close request on the `main` window.
pub fn resolve_main_close_decision(
    close_behavior: CloseBehavior,
    tray_available: bool,
    explicit_quit_requested: bool,
) -> MainCloseDecision {
    if explicit_quit_requested {
        return MainCloseDecision::QuitApp;
    }
    match close_behavior {
        CloseBehavior::HideToTray if tray_available => MainCloseDecision::HideMainToTray,
        CloseBehavior::HideToTray | CloseBehavior::Quit => MainCloseDecision::QuitApp,
    }
}

/// Pure decision function for handling a close request on the `tray` popover window.
pub fn resolve_popover_close_decision(explicit_quit_requested: bool) -> PopoverDismissDecision {
    if explicit_quit_requested {
        PopoverDismissDecision::AllowCloseForQuit
    } else {
        PopoverDismissDecision::HidePopoverOnly
    }
}

/// Pure decision function for handling a focus-loss event on the `tray` popover window.
pub fn resolve_popover_blur_decision(
    explicit_quit_requested: bool,
    elapsed_since_open: Option<Duration>,
) -> PopoverDismissDecision {
    if explicit_quit_requested {
        return PopoverDismissDecision::AllowCloseForQuit;
    }
    if let Some(elapsed) = elapsed_since_open {
        if elapsed < POPOVER_BLUR_GRACE_PERIOD {
            return PopoverDismissDecision::IgnoreTransientBlur;
        }
    }
    PopoverDismissDecision::HidePopoverOnly
}

/// Pure recovery decision when system tray availability is lost or unavailable.
pub fn resolve_tray_availability_recovery(
    tray_available: bool,
    main_visible: bool,
    explicit_quit_requested: bool,
) -> TrayAvailabilityRecoveryDecision {
    if !explicit_quit_requested && !tray_available && !main_visible {
        TrayAvailabilityRecoveryDecision::RestoreHiddenMain
    } else {
        TrayAvailabilityRecoveryDecision::NoAction
    }
}

/// Determines whether a tray icon mouse click event should toggle the status popover.
///
/// On macOS and Windows, `tray-icon` emits both `Down` and `Up` events per click, so toggling
/// triggers on `MouseButton::Left` with `MouseButtonState::Up`. On Linux (`ksni`), StatusNotifier
/// `Activate(x, y)` is a single D-Bus activation call; left-click activations are accepted with
/// a short debounce guard against duplicate backend emissions.
pub fn should_toggle_popover_on_tray_click(
    button: MouseButton,
    button_state: MouseButtonState,
    is_linux: bool,
    elapsed_since_last_click: Option<Duration>,
) -> bool {
    if button != MouseButton::Left {
        return false;
    }
    if let Some(elapsed) = elapsed_since_last_click {
        if elapsed < TRAY_CLICK_DEBOUNCE {
            return false;
        }
    }
    if is_linux {
        true
    } else {
        button_state == MouseButtonState::Up
    }
}

/// Ensures that privileged execution approval and host lifecycle commands are only invoked
/// from the primary `main` window and never from the read-only `tray` popover surface.
pub fn ensure_main_window_label(window_label: &str) -> Result<(), AppError> {
    if window_label == MAIN_WINDOW_LABEL {
        Ok(())
    } else {
        Err(AppError::new(
            "forbidden_surface",
            "Execution approval and session lifecycle controls are only permitted from the main Threadlane window.",
        ))
    }
}

/// Verifies that the calling Tauri [`Window`] is the primary `main` window.
pub fn ensure_main_window_only(window: &Window) -> Result<(), AppError> {
    ensure_main_window_label(window.label())
}

/// Selects the monitor containing `anchor_px` (if usable), falling back to the first available
/// monitor or [`MonitorBoundsPx::default`].
#[cfg(test)]
pub fn select_monitor_for_point(
    monitors: &[MonitorBoundsPx],
    anchor_px: Option<(f64, f64)>,
) -> MonitorBoundsPx {
    if monitors.is_empty() {
        return MonitorBoundsPx::default();
    }
    if let Some((ax, ay)) = anchor_px {
        if ax.is_finite() && ay.is_finite() && !(ax == 0.0 && ay == 0.0) {
            let px = ax.round() as i32;
            let py = ay.round() as i32;
            for monitor in monitors {
                let right = monitor.x.saturating_add(monitor.width as i32);
                let bottom = monitor.y.saturating_add(monitor.height as i32);
                if px >= monitor.x && px < right && py >= monitor.y && py < bottom {
                    return *monitor;
                }
            }
        }
    }
    monitors[0]
}

#[cfg(any(not(target_os = "linux"), test))]
fn is_usable_tray_rect(rect: &TrayRectPx, monitor: &MonitorBoundsPx) -> bool {
    if !rect.x.is_finite()
        || !rect.y.is_finite()
        || !rect.width.is_finite()
        || !rect.height.is_finite()
    {
        return false;
    }
    if rect.width <= 1.0 && rect.height <= 1.0 {
        return false;
    }
    let tolerance = 256.0;
    let min_x = (monitor.x as f64) - tolerance;
    let max_x = (monitor.x as f64) + (monitor.width as f64) + tolerance;
    let min_y = (monitor.y as f64) - tolerance;
    let max_y = (monitor.y as f64) + (monitor.height as f64) + tolerance;
    rect.x >= min_x && rect.x <= max_x && rect.y >= min_y && rect.y <= max_y
}

#[cfg(any(not(target_os = "linux"), test))]
fn is_usable_click_point(point: (f64, f64), monitor: &MonitorBoundsPx) -> bool {
    let (x, y) = point;
    if !x.is_finite() || !y.is_finite() {
        return false;
    }
    if x == 0.0 && y == 0.0 {
        return false;
    }
    let tolerance = 256.0;
    let min_x = (monitor.x as f64) - tolerance;
    let max_x = (monitor.x as f64) + (monitor.width as f64) + tolerance;
    let min_y = (monitor.y as f64) - tolerance;
    let max_y = (monitor.y as f64) + (monitor.height as f64) + tolerance;
    x >= min_x && x <= max_x && y >= min_y && y <= max_y
}

/// Computes a monitor-clamped physical `(x, y)` position for the tray popover window.
///
/// Uses `tray_rect` when non-empty, else `click_position` when valid and non-zero, and otherwise
/// falls back to a safe top-right (Linux/macOS) or bottom-right (Windows) position inside the
/// target monitor bounds.
#[cfg(any(not(target_os = "linux"), test))]
pub fn compute_popover_position_px(
    monitor: MonitorBoundsPx,
    popover_logical_size: (f64, f64),
    click_position: Option<(f64, f64)>,
    tray_rect: Option<TrayRectPx>,
    prefer_bottom_fallback: bool,
) -> (i32, i32) {
    let scale = if monitor.scale_factor.is_finite() && monitor.scale_factor > 0.0 {
        monitor.scale_factor
    } else {
        1.0
    };
    let logical_w = if popover_logical_size.0.is_finite() && popover_logical_size.0 > 0.0 {
        popover_logical_size.0
    } else {
        TRAY_POPOVER_WIDTH
    };
    let logical_h = if popover_logical_size.1.is_finite() && popover_logical_size.1 > 0.0 {
        popover_logical_size.1
    } else {
        TRAY_POPOVER_HEIGHT
    };

    let pop_w = (logical_w * scale).round().max(120.0) as i32;
    let pop_h = (logical_h * scale).round().max(120.0) as i32;
    let edge_margin = (12.0 * scale).round().max(4.0) as i32;
    let anchor_gap = (6.0 * scale).round().max(2.0) as i32;

    let mon_left = monitor.x;
    let mon_top = monitor.y;
    let mon_right = monitor.x.saturating_add(monitor.width as i32);
    let mon_bottom = monitor.y.saturating_add(monitor.height as i32);
    let mon_mid_y = mon_top.saturating_add((monitor.height as i32) / 2);

    let (raw_x, raw_y) = match tray_rect.filter(|r| is_usable_tray_rect(r, &monitor)) {
        Some(rect) => {
            let center_x = (rect.x + rect.width / 2.0).round() as i32;
            let rect_top = rect.y.round() as i32;
            let rect_bottom = (rect.y + rect.height).round() as i32;
            let x = center_x.saturating_sub(pop_w / 2);
            let y = if rect_top <= mon_mid_y {
                rect_bottom.saturating_add(anchor_gap)
            } else {
                rect_top.saturating_sub(pop_h).saturating_sub(anchor_gap)
            };
            (x, y)
        }
        None => match click_position.filter(|pt| is_usable_click_point(*pt, &monitor)) {
            Some((cx, cy)) => {
                let click_x = cx.round() as i32;
                let click_y = cy.round() as i32;
                let x = click_x.saturating_sub(pop_w / 2);
                let y = if click_y <= mon_mid_y {
                    click_y.saturating_add(anchor_gap)
                } else {
                    click_y.saturating_sub(pop_h).saturating_sub(anchor_gap)
                };
                (x, y)
            }
            None => {
                let fallback_x = mon_right.saturating_sub(pop_w).saturating_sub(edge_margin);
                let panel_reserve = (36.0 * scale).round() as i32;
                let fallback_y = if prefer_bottom_fallback {
                    mon_bottom
                        .saturating_sub(pop_h)
                        .saturating_sub(edge_margin.saturating_add(panel_reserve))
                } else {
                    mon_top.saturating_add(edge_margin.saturating_add(panel_reserve))
                };
                (fallback_x, fallback_y)
            }
        },
    };

    let min_x = mon_left.saturating_add(edge_margin);
    let max_x = mon_right
        .saturating_sub(pop_w)
        .saturating_sub(edge_margin)
        .max(min_x);
    let min_y = mon_top.saturating_add(edge_margin);
    let max_y = mon_bottom
        .saturating_sub(pop_h)
        .saturating_sub(edge_margin)
        .max(min_y);

    (raw_x.clamp(min_x, max_x), raw_y.clamp(min_y, max_y))
}

#[cfg(any(not(target_os = "linux"), test))]
#[derive(Debug, Clone, Copy, PartialEq)]
struct LogicalMonitorSpan {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

#[cfg(any(not(target_os = "linux"), test))]
fn sanitize_scale_factor(scale_factor: f64) -> f64 {
    if scale_factor.is_finite() && scale_factor > 0.0 {
        scale_factor
    } else {
        1.0
    }
}

#[cfg(any(not(target_os = "linux"), test))]
fn derive_logical_monitor_spans(monitors: &[MonitorBoundsPx]) -> Vec<LogicalMonitorSpan> {
    let n = monitors.len();
    let mut origins: Vec<Option<(f64, f64)>> = vec![None; n];
    let logical_sizes: Vec<(f64, f64)> = monitors
        .iter()
        .map(|m| {
            let s = sanitize_scale_factor(m.scale_factor);
            ((m.width.max(1) as f64) / s, (m.height.max(1) as f64) / s)
        })
        .collect();

    for (idx, mon) in monitors.iter().enumerate() {
        if mon.x == 0 && mon.y == 0 {
            origins[idx] = Some((0.0, 0.0));
        }
    }
    if origins.iter().all(Option::is_none) && !monitors.is_empty() {
        let s = sanitize_scale_factor(monitors[0].scale_factor);
        origins[0] = Some(((monitors[0].x as f64) / s, (monitors[0].y as f64) / s));
    }

    for _ in 0..n {
        let mut progress = false;
        for i in 0..n {
            if origins[i].is_some() {
                continue;
            }
            for j in 0..n {
                let Some((jx, jy)) = origins[j] else {
                    continue;
                };
                let m_i = &monitors[i];
                let m_j = &monitors[j];
                let (jw, jh) = logical_sizes[j];
                let (iw, ih) = logical_sizes[i];

                let candidate_x = if m_i.x == m_j.x.saturating_add(m_j.width as i32) {
                    Some(jx + jw)
                } else if m_i.x.saturating_add(m_i.width as i32) == m_j.x {
                    Some(jx - iw)
                } else if m_i.x == m_j.x {
                    Some(jx)
                } else {
                    None
                };

                let candidate_y = if m_i.y == m_j.y.saturating_add(m_j.height as i32) {
                    Some(jy + jh)
                } else if m_i.y.saturating_add(m_i.height as i32) == m_j.y {
                    Some(jy - ih)
                } else if m_i.y == m_j.y {
                    Some(jy)
                } else {
                    None
                };

                if let (Some(cx), Some(cy)) = (candidate_x, candidate_y) {
                    origins[i] = Some((cx, cy));
                    progress = true;
                    break;
                }
            }
        }
        if !progress {
            break;
        }
    }

    monitors
        .iter()
        .enumerate()
        .map(|(idx, mon)| {
            let s = sanitize_scale_factor(mon.scale_factor);
            let fallback = ((mon.x as f64) / s, (mon.y as f64) / s);
            let (lx, ly) = origins[idx].unwrap_or(fallback);
            let (lw, lh) = logical_sizes[idx];
            LogicalMonitorSpan {
                x: lx,
                y: ly,
                width: lw,
                height: lh,
            }
        })
        .collect()
}

#[cfg(any(not(target_os = "linux"), test))]
fn point_in_physical_monitor(monitor: &MonitorBoundsPx, px: f64, py: f64, tolerance: f64) -> bool {
    if !px.is_finite() || !py.is_finite() {
        return false;
    }
    let min_x = (monitor.x as f64) - tolerance;
    let max_x = (monitor.x as f64) + (monitor.width as f64) + tolerance;
    let min_y = (monitor.y as f64) - tolerance;
    let max_y = (monitor.y as f64) + (monitor.height as f64) + tolerance;
    if tolerance == 0.0 {
        px >= min_x && px < max_x && py >= min_y && py < max_y
    } else {
        px >= min_x && px <= max_x && py >= min_y && py <= max_y
    }
}

#[cfg(any(not(target_os = "linux"), test))]
fn point_in_logical_span(span: &LogicalMonitorSpan, lx: f64, ly: f64, tolerance: f64) -> bool {
    if !lx.is_finite() || !ly.is_finite() {
        return false;
    }
    let min_x = span.x - tolerance;
    let max_x = span.x + span.width + tolerance;
    let min_y = span.y - tolerance;
    let max_y = span.y + span.height + tolerance;
    if tolerance == 0.0 {
        lx >= min_x && lx < max_x && ly >= min_y && ly < max_y
    } else {
        lx >= min_x && lx <= max_x && ly >= min_y && ly <= max_y
    }
}

/// Resolves raw tray activation geometry against `monitors` using the containing monitor's own
/// `scale_factor` rather than converting logical coordinates upfront with `monitors.first()`.
#[cfg(any(not(target_os = "linux"), test))]
pub fn resolve_tray_anchor_for_monitors(
    monitors: &[MonitorBoundsPx],
    raw_anchor: Option<RawTrayAnchor>,
) -> ResolvedTrayAnchor {
    let default_monitor = monitors.first().copied().unwrap_or_default();
    let effective_monitors: &[MonitorBoundsPx] = if monitors.is_empty() {
        std::slice::from_ref(&default_monitor)
    } else {
        monitors
    };
    let logical_spans = derive_logical_monitor_spans(effective_monitors);

    let Some(anchor) = raw_anchor else {
        return ResolvedTrayAnchor {
            monitor: default_monitor,
            click_position_px: None,
            tray_rect_px: None,
            has_valid_anchor: false,
        };
    };

    let mut matched_monitor: Option<(usize, MonitorBoundsPx)> = None;
    let mut resolved_rect_px: Option<TrayRectPx> = None;

    if let Some(raw_rect) = anchor.tray_rect {
        'rect_search: for &tolerance in &[0.0, 32.0] {
            for (idx, (mon, span)) in effective_monitors
                .iter()
                .zip(logical_spans.iter())
                .enumerate()
            {
                let scale = sanitize_scale_factor(mon.scale_factor);
                match raw_rect.position {
                    RawCoordinate2D::Physical { x, y } => {
                        let (pw, ph) = match raw_rect.size {
                            RawSize2D::Physical { width, height } => (width, height),
                            RawSize2D::Logical { width, height } => (width * scale, height * scale),
                        };
                        if !x.is_finite()
                            || !y.is_finite()
                            || !pw.is_finite()
                            || !ph.is_finite()
                            || (pw <= 1.0 && ph <= 1.0)
                        {
                            continue;
                        }
                        let cx = x + pw / 2.0;
                        let cy = y + ph / 2.0;
                        if point_in_physical_monitor(mon, cx, cy, tolerance) {
                            let candidate = TrayRectPx {
                                x,
                                y,
                                width: pw,
                                height: ph,
                            };
                            if is_usable_tray_rect(&candidate, mon) {
                                matched_monitor = Some((idx, *mon));
                                resolved_rect_px = Some(candidate);
                                break 'rect_search;
                            }
                        }
                    }
                    RawCoordinate2D::Logical { x, y } => {
                        let (lw, lh) = match raw_rect.size {
                            RawSize2D::Logical { width, height } => (width, height),
                            RawSize2D::Physical { width, height } => {
                                (width / scale, height / scale)
                            }
                        };
                        if !x.is_finite()
                            || !y.is_finite()
                            || !lw.is_finite()
                            || !lh.is_finite()
                            || (lw <= 1.0 && lh <= 1.0)
                        {
                            continue;
                        }
                        let lcx = x + lw / 2.0;
                        let lcy = y + lh / 2.0;
                        if point_in_logical_span(span, lcx, lcy, tolerance) {
                            let candidate = TrayRectPx {
                                x: (mon.x as f64) + (x - span.x) * scale,
                                y: (mon.y as f64) + (y - span.y) * scale,
                                width: lw * scale,
                                height: lh * scale,
                            };
                            if is_usable_tray_rect(&candidate, mon) {
                                matched_monitor = Some((idx, *mon));
                                resolved_rect_px = Some(candidate);
                                break 'rect_search;
                            }
                        }
                    }
                }
            }
        }
    }

    let mut resolved_click_px: Option<(f64, f64)> = None;
    if let Some(raw_click) = anchor.click_position {
        if let Some((idx, mon)) = matched_monitor {
            let span = &logical_spans[idx];
            let scale = sanitize_scale_factor(mon.scale_factor);
            let candidate_pt = match raw_click {
                RawCoordinate2D::Physical { x, y } => (x, y),
                RawCoordinate2D::Logical { x, y } => (
                    (mon.x as f64) + (x - span.x) * scale,
                    (mon.y as f64) + (y - span.y) * scale,
                ),
            };
            if is_usable_click_point(candidate_pt, &mon) {
                resolved_click_px = Some(candidate_pt);
            }
        } else {
            'click_search: for &tolerance in &[0.0, 32.0] {
                for (idx, (mon, span)) in effective_monitors
                    .iter()
                    .zip(logical_spans.iter())
                    .enumerate()
                {
                    let scale = sanitize_scale_factor(mon.scale_factor);
                    match raw_click {
                        RawCoordinate2D::Physical { x, y } => {
                            if !(x == 0.0 && y == 0.0)
                                && point_in_physical_monitor(mon, x, y, tolerance)
                                && is_usable_click_point((x, y), mon)
                            {
                                matched_monitor = Some((idx, *mon));
                                resolved_click_px = Some((x, y));
                                break 'click_search;
                            }
                        }
                        RawCoordinate2D::Logical { x, y } => {
                            if !(x == 0.0 && y == 0.0)
                                && point_in_logical_span(span, x, y, tolerance)
                            {
                                let pt = (
                                    (mon.x as f64) + (x - span.x) * scale,
                                    (mon.y as f64) + (y - span.y) * scale,
                                );
                                if is_usable_click_point(pt, mon) {
                                    matched_monitor = Some((idx, *mon));
                                    resolved_click_px = Some(pt);
                                    break 'click_search;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let final_monitor = matched_monitor
        .map(|(_, mon)| mon)
        .unwrap_or(default_monitor);
    let has_valid_anchor = resolved_rect_px.is_some() || resolved_click_px.is_some();

    ResolvedTrayAnchor {
        monitor: final_monitor,
        click_position_px: resolved_click_px,
        tray_rect_px: resolved_rect_px,
        has_valid_anchor,
    }
}

/// Computes compact logical `(width, height)` dimensions for the tray popover on `monitor`,
/// fitting up to [`TRAY_MAX_CONTRACT_ROWS`] status rows without carrying over `main`'s minimum
/// window height (`500px`).
#[cfg(any(not(target_os = "linux"), test))]
pub fn compute_popover_logical_size_for_monitor(monitor: MonitorBoundsPx) -> (f64, f64) {
    let scale = sanitize_scale_factor(monitor.scale_factor);
    let logical_mon_w = (monitor.width.max(1) as f64) / scale;
    let logical_mon_h = (monitor.height.max(1) as f64) / scale;
    let avail_w = (logical_mon_w - 24.0).max(160.0);
    let avail_h = (logical_mon_h - 24.0).max(160.0);

    let width = TRAY_POPOVER_WIDTH
        .max(TRAY_POPOVER_MIN_WIDTH)
        .min(avail_w)
        .round();
    let height = TRAY_POPOVER_HEIGHT
        .max(TRAY_POPOVER_MIN_HEIGHT)
        .min(avail_h)
        .round();
    (width, height)
}

/// Bounded persistent store for `tray-preferences.json` under `app_data_dir`.
/// Error code returned when `tray-preferences.json` has been renamed into place on disk,
/// but synchronizing its parent directory failed so crash durability is uncertain.
pub const PREFERENCES_DURABILITY_UNCERTAIN_CODE: &str = "preferences_durability_uncertain";

const PREFERENCES_DURABILITY_UNCERTAIN_MESSAGE: &str = "Tray close behavior preference was written to disk, but parent directory synchronization failed; durability across crash or power loss is uncertain.";

type DirSyncFn = Arc<dyn Fn(&Path) -> std::io::Result<()> + Send + Sync>;

#[derive(Debug)]
enum AtomicWriteError {
    PreRename(std::io::Error),
    PostRenameSync(std::io::Error),
}

impl std::fmt::Display for AtomicWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::PreRename(_) => "atomic write failed before rename",
            Self::PostRenameSync(_) => "atomic write durability is uncertain after rename",
        })
    }
}

impl std::error::Error for AtomicWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PreRename(error) | Self::PostRenameSync(error) => Some(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SaveFailureOutcome {
    PreRename(AppError),
    PostRenameUncertain {
        reconciled: StoredTrayPreferences,
        error: AppError,
    },
}

impl SaveFailureOutcome {
    #[cfg(test)]
    fn into_error(self) -> AppError {
        match self {
            Self::PreRename(err) => err,
            Self::PostRenameUncertain { error, .. } => error,
        }
    }
}

#[derive(Clone)]
pub struct TrayPreferencesStore {
    app_data_dir: PathBuf,
    process_lock: Arc<Mutex<()>>,
    dir_sync: DirSyncFn,
}

impl std::fmt::Debug for TrayPreferencesStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrayPreferencesStore")
            .field("app_data_dir", &self.app_data_dir)
            .finish()
    }
}

impl TrayPreferencesStore {
    pub fn new(app_data_dir: impl Into<PathBuf>) -> Self {
        Self {
            app_data_dir: app_data_dir.into(),
            process_lock: Arc::new(Mutex::new(())),
            dir_sync: Arc::new(sync_parent_directory),
        }
    }

    #[cfg(test)]
    fn with_dir_sync<F>(app_data_dir: impl Into<PathBuf>, dir_sync: F) -> Self
    where
        F: Fn(&Path) -> std::io::Result<()> + Send + Sync + 'static,
    {
        Self {
            app_data_dir: app_data_dir.into(),
            process_lock: Arc::new(Mutex::new(())),
            dir_sync: Arc::new(dir_sync),
        }
    }

    pub fn preferences_path(&self) -> PathBuf {
        self.app_data_dir.join(TRAY_PREFERENCES_FILENAME)
    }

    pub fn lock_path(&self) -> PathBuf {
        self.app_data_dir.join(TRAY_PREFERENCES_LOCK_FILENAME)
    }

    /// Loads the persisted close behavior or returns [`CloseBehavior::HideToTray`] by default.
    ///
    /// If `tray-preferences.json` is corrupt, oversized, a symlink, or contains unexpected fields,
    /// the file on disk is preserved without modification and the default `HideToTray` behavior
    /// is returned along with a sanitized diagnostic message.
    pub fn load_or_default(&self) -> InitialPreferencesLoad {
        let _guard = match self.process_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        let prefs_path = self.preferences_path();
        match read_and_validate_preferences_file(&prefs_path) {
            Ok(Some(stored)) => InitialPreferencesLoad {
                close_behavior: stored.close_behavior,
                warning: None,
            },
            Ok(None) => InitialPreferencesLoad {
                close_behavior: CloseBehavior::default(),
                warning: None,
            },
            Err(err) => InitialPreferencesLoad {
                close_behavior: CloseBehavior::default(),
                warning: Some(err.message),
            },
        }
    }

    /// Atomically persists `close_behavior` to `tray-preferences.json`.
    ///
    /// Refuses to overwrite an existing corrupt or unexpected file so user/forensic state is
    /// preserved, returning a sanitized [`AppError`]. Never reports durable success if
    /// parent-directory synchronization fails after rename.
    #[cfg(test)]
    pub fn save(&self, close_behavior: CloseBehavior) -> Result<StoredTrayPreferences, AppError> {
        self.save_outcome(close_behavior)
            .map_err(SaveFailureOutcome::into_error)
    }

    fn save_outcome(
        &self,
        close_behavior: CloseBehavior,
    ) -> Result<StoredTrayPreferences, SaveFailureOutcome> {
        let _guard = self.process_lock.lock().map_err(|_| {
            SaveFailureOutcome::PreRename(AppError::new(
                "internal_lock_error",
                "Tray preferences lock failed.",
            ))
        })?;

        prepare_private_app_data_dir(&self.app_data_dir).map_err(SaveFailureOutcome::PreRename)?;

        let lock_file =
            open_private_lock_file(&self.lock_path()).map_err(SaveFailureOutcome::PreRename)?;
        lock_file.lock_exclusive().map_err(|_| {
            SaveFailureOutcome::PreRename(AppError::new(
                "preferences_lock_failed",
                "Unable to lock tray preferences file for writing.",
            ))
        })?;
        let _unlock_guard = FileUnlockGuard(&lock_file);

        let prefs_path = self.preferences_path();
        // Inspect any existing preferences file before replacing it; never silently overwrite
        // a corrupt, unexpected, or symlinked file.
        let _existing = read_and_validate_preferences_file(&prefs_path)
            .map_err(SaveFailureOutcome::PreRename)?;

        let stored = StoredTrayPreferences {
            schema_version: TRAY_PREFERENCES_SCHEMA_VERSION,
            close_behavior,
        };
        let mut encoded = serde_json::to_vec_pretty(&stored).map_err(|_| {
            SaveFailureOutcome::PreRename(AppError::new(
                "preferences_save_failed",
                "Unable to serialize tray preferences.",
            ))
        })?;
        encoded.push(b'\n');

        if (encoded.len() as u64) > MAX_TRAY_PREFERENCES_FILE_BYTES {
            return Err(SaveFailureOutcome::PreRename(AppError::new(
                "preferences_save_failed",
                "Serialized tray preferences exceed the maximum allowed file size.",
            )));
        }

        match write_atomic_file_with_sync(
            &self.app_data_dir,
            &prefs_path,
            ".tray-preferences.json.tmp.",
            &encoded,
            self.dir_sync.as_ref(),
        ) {
            Ok(()) => Ok(stored),
            Err(AtomicWriteError::PreRename(_)) => {
                Err(SaveFailureOutcome::PreRename(AppError::new(
                    "preferences_save_failed",
                    "Unable to save tray close behavior preference.",
                )))
            }
            Err(AtomicWriteError::PostRenameSync(_)) => {
                let reconciled = match read_and_validate_preferences_file(&prefs_path) {
                    Ok(Some(on_disk)) => on_disk,
                    _ => stored,
                };
                Err(SaveFailureOutcome::PostRenameUncertain {
                    reconciled,
                    error: AppError::new(
                        PREFERENCES_DURABILITY_UNCERTAIN_CODE,
                        PREFERENCES_DURABILITY_UNCERTAIN_MESSAGE,
                    ),
                })
            }
        }
    }
}

struct FileUnlockGuard<'a>(&'a File);

impl Drop for FileUnlockGuard<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn prepare_private_app_data_dir(dir: &Path) -> Result<(), AppError> {
    if dir.as_os_str().is_empty() {
        return Err(AppError::new(
            "preferences_save_failed",
            "Application data directory path is empty.",
        ));
    }
    match fs::symlink_metadata(dir) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_dir() {
                return Err(AppError::new(
                    "security_violation",
                    "Application data directory must be a regular non-symlink directory.",
                ));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(dir).map_err(|_| {
                AppError::new(
                    "preferences_save_failed",
                    "Unable to initialize application data directory.",
                )
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
            }
        }
        Err(_) => {
            return Err(AppError::new(
                "preferences_save_failed",
                "Unable to inspect application data directory.",
            ));
        }
    }
    Ok(())
}

fn open_private_lock_file(lock_path: &Path) -> Result<File, AppError> {
    if let Ok(meta) = fs::symlink_metadata(lock_path) {
        if meta.file_type().is_symlink() || !meta.file_type().is_file() {
            return Err(AppError::new(
                "security_violation",
                "Lock path must be a regular non-symlink file.",
            ));
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .map_err(|_| {
            AppError::new(
                "preferences_lock_failed",
                "Unable to open application lock file.",
            )
        })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
    }
    Ok(file)
}

fn read_and_validate_preferences_file(
    prefs_path: &Path,
) -> Result<Option<StoredTrayPreferences>, AppError> {
    let sym_meta = match fs::symlink_metadata(prefs_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(AppError::new(
                "preferences_read_failed",
                "Unable to inspect existing tray preferences file.",
            ));
        }
    };

    if sym_meta.file_type().is_symlink() || !sym_meta.file_type().is_file() {
        return Err(AppError::new(
            "security_violation",
            "Existing tray preferences path is not a regular file and was preserved without overwriting.",
        ));
    }

    if sym_meta.len() > MAX_TRAY_PREFERENCES_FILE_BYTES {
        return Err(AppError::new(
            "corrupt_preferences",
            "Existing tray preferences file exceeds the size bound and was preserved without overwriting.",
        ));
    }

    let file = File::open(prefs_path).map_err(|_| {
        AppError::new(
            "preferences_read_failed",
            "Unable to open existing tray preferences file.",
        )
    })?;
    let mut buf = Vec::with_capacity((sym_meta.len() as usize).saturating_add(1));
    file.take(MAX_TRAY_PREFERENCES_FILE_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|_| {
            AppError::new(
                "preferences_read_failed",
                "Unable to read existing tray preferences file.",
            )
        })?;

    if (buf.len() as u64) > MAX_TRAY_PREFERENCES_FILE_BYTES {
        return Err(AppError::new(
            "corrupt_preferences",
            "Existing tray preferences file exceeds the size bound and was preserved without overwriting.",
        ));
    }

    let parsed: StoredTrayPreferences = serde_json::from_slice(&buf).map_err(|_| {
        AppError::new(
            "corrupt_preferences",
            "Existing tray preferences file is corrupt or contains unexpected fields and was preserved without overwriting.",
        )
    })?;

    if parsed.schema_version != TRAY_PREFERENCES_SCHEMA_VERSION {
        return Err(AppError::new(
            "corrupt_preferences",
            "Existing tray preferences file uses an unsupported schema version and was preserved without overwriting.",
        ));
    }

    Ok(Some(parsed))
}

#[cfg(unix)]
fn sync_parent_directory(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(dir: &Path) -> std::io::Result<()> {
    let _ = dir;
    Ok(())
}

fn write_atomic_file(
    dir: &Path,
    target_path: &Path,
    temp_prefix: &str,
    encoded: &[u8],
) -> Result<(), AtomicWriteError> {
    write_atomic_file_with_sync(
        dir,
        target_path,
        temp_prefix,
        encoded,
        &sync_parent_directory,
    )
}

fn write_atomic_file_with_sync(
    dir: &Path,
    target_path: &Path,
    temp_prefix: &str,
    encoded: &[u8],
    dir_sync: &(dyn Fn(&Path) -> std::io::Result<()> + Send + Sync),
) -> Result<(), AtomicWriteError> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(temp_prefix);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o600));
    }
    let mut temp = builder
        .tempfile_in(dir)
        .map_err(AtomicWriteError::PreRename)?;
    let temp_meta = fs::symlink_metadata(temp.path()).map_err(AtomicWriteError::PreRename)?;
    if temp_meta.file_type().is_symlink() || !temp_meta.file_type().is_file() {
        return Err(AtomicWriteError::PreRename(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "temporary file is not a regular file",
        )));
    }

    temp.write_all(encoded)
        .map_err(AtomicWriteError::PreRename)?;
    temp.flush().map_err(AtomicWriteError::PreRename)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(AtomicWriteError::PreRename)?;
    }
    temp.as_file()
        .sync_all()
        .map_err(AtomicWriteError::PreRename)?;

    if let Ok(existing_meta) = fs::symlink_metadata(target_path) {
        if existing_meta.file_type().is_symlink() || !existing_meta.file_type().is_file() {
            return Err(AtomicWriteError::PreRename(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "target path is not a regular file",
            )));
        }
    }

    temp.persist(target_path)
        .map_err(|err| AtomicWriteError::PreRename(err.error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(target_path, fs::Permissions::from_mode(0o600))
            .map_err(AtomicWriteError::PostRenameSync)?;
    }
    dir_sync(dir).map_err(AtomicWriteError::PostRenameSync)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SingleInstanceEndpointRecord {
    schema_version: u32,
    pid: u32,
    addr: String,
    token: String,
    fingerprint: LaunchFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SingleInstanceWireRequest {
    token: String,
    fingerprint: LaunchFingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SingleInstanceWireResponse {
    accepted: bool,
    conflict: bool,
    message: Option<String>,
}

/// Outcome of attempting to acquire the single-instance lock for `app_data_dir`.
pub enum SingleInstanceAcquisition {
    /// This process is the primary instance and owns the active [`SingleInstanceGuard`].
    Primary(SingleInstanceGuard),
    /// An existing compatible instance was notified and restored its `main` window.
    RestoredExisting,
}

/// Active guard keeping the single-instance lock file and local loopback activation listener alive.
pub struct SingleInstanceGuard {
    lock_file: File,
    endpoint_path: PathBuf,
    listen_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    acceptor_thread: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for SingleInstanceGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SingleInstanceGuard")
            .field("endpoint_path", &self.endpoint_path)
            .field("listen_addr", &self.listen_addr)
            .finish()
    }
}

impl Drop for SingleInstanceGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.listen_addr, Duration::from_millis(100));
        if let Some(handle) = self.acceptor_thread.take() {
            let _ = handle.join();
        }
        let _ = fs::remove_file(&self.endpoint_path);
        let _ = self.lock_file.unlock();
    }
}

impl SingleInstanceGuard {
    /// Acquires the single-instance lock in `app_data_dir` or delegates activation to an
    /// already-running instance.
    ///
    /// If an existing instance is running with conflicting CLI arguments, returns an [`AppError`]
    /// without retargeting or disturbing the running instance.
    pub fn acquire_or_notify<F>(
        app_data_dir: &Path,
        fingerprint: LaunchFingerprint,
        on_activate: F,
    ) -> Result<SingleInstanceAcquisition, AppError>
    where
        F: Fn() + Send + Sync + 'static,
    {
        prepare_private_app_data_dir(app_data_dir)?;
        let lock_path = app_data_dir.join(SINGLE_INSTANCE_LOCK_FILENAME);
        let endpoint_path = app_data_dir.join(SINGLE_INSTANCE_ENDPOINT_FILENAME);

        for attempt in 0..3 {
            let lock_file = open_private_lock_file(&lock_path)?;
            match lock_file.try_lock_exclusive() {
                Ok(()) => {
                    return Self::start_primary(
                        app_data_dir,
                        lock_file,
                        endpoint_path,
                        fingerprint,
                        Arc::new(on_activate),
                    )
                    .map(SingleInstanceAcquisition::Primary);
                }
                Err(_) => match notify_existing_instance(&endpoint_path, &fingerprint) {
                    Ok(SingleInstanceWireResponse { accepted: true, .. }) => {
                        return Ok(SingleInstanceAcquisition::RestoredExisting);
                    }
                    Ok(SingleInstanceWireResponse {
                        accepted: false,
                        conflict: true,
                        message,
                    }) => {
                        return Err(AppError::new(
                                "instance_argument_conflict",
                                message.unwrap_or_else(|| {
                                    "Another Threadlane desktop instance is already running with different startup arguments."
                                        .to_string()
                                }),
                            ));
                    }
                    Ok(SingleInstanceWireResponse {
                        accepted: false,
                        conflict: false,
                        message,
                    }) => {
                        return Err(AppError::new(
                            "instance_activation_failed",
                            message.unwrap_or_else(|| {
                                "Existing Threadlane desktop instance rejected activation request."
                                    .to_string()
                            }),
                        ));
                    }
                    Err(err) => {
                        if attempt < 2 {
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        return Err(err);
                    }
                },
            }
        }

        Err(AppError::new(
            "instance_lock_busy",
            "Another Threadlane desktop instance is currently starting or shutting down.",
        ))
    }

    fn start_primary(
        app_data_dir: &Path,
        lock_file: File,
        endpoint_path: PathBuf,
        fingerprint: LaunchFingerprint,
        on_activate: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Self, AppError> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|_| {
            AppError::new(
                "instance_bind_failed",
                "Unable to bind local single-instance activation listener.",
            )
        })?;
        let listen_addr = listener.local_addr().map_err(|_| {
            AppError::new(
                "instance_bind_failed",
                "Unable to resolve local single-instance activation address.",
            )
        })?;

        let token = generate_instance_token(listen_addr);
        let record = SingleInstanceEndpointRecord {
            schema_version: SINGLE_INSTANCE_SCHEMA_VERSION,
            pid: std::process::id(),
            addr: listen_addr.to_string(),
            token: token.clone(),
            fingerprint: fingerprint.clone(),
        };
        let mut encoded = serde_json::to_vec_pretty(&record).map_err(|_| {
            AppError::new(
                "instance_endpoint_failed",
                "Unable to serialize single-instance endpoint metadata.",
            )
        })?;
        encoded.push(b'\n');

        write_atomic_file(
            app_data_dir,
            &endpoint_path,
            ".desktop-instance.json.tmp.",
            &encoded,
        )
        .map_err(|err| match err {
            AtomicWriteError::PreRename(_) => AppError::new(
                "instance_endpoint_failed",
                "Unable to write single-instance endpoint file.",
            ),
            AtomicWriteError::PostRenameSync(_) => AppError::new(
                "instance_endpoint_failed",
                "Single-instance endpoint file was written, but parent directory synchronization failed.",
            ),
        })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let running_fingerprint = fingerprint;

        let acceptor_thread = std::thread::Builder::new()
            .name("threadlane-single-instance".to_string())
            .spawn(move || {
                for incoming in listener.incoming() {
                    if thread_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = incoming else {
                        continue;
                    };
                    let _ = stream.set_read_timeout(Some(Duration::from_millis(1500)));
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(1500)));

                    let mut line = String::new();
                    let mut reader =
                        BufReader::new(Read::by_ref(&mut stream).take(MAX_SINGLE_INSTANCE_BYTES));
                    if reader.read_line(&mut line).is_err() || line.trim().is_empty() {
                        continue;
                    }

                    let response = match serde_json::from_str::<SingleInstanceWireRequest>(
                        line.trim(),
                    ) {
                        Ok(req) if req.token == token => {
                            match evaluate_second_launch_compatibility(
                                &running_fingerprint,
                                &req.fingerprint,
                            ) {
                                Ok(()) => {
                                    on_activate();
                                    SingleInstanceWireResponse {
                                        accepted: true,
                                        conflict: false,
                                        message: None,
                                    }
                                }
                                Err(reason) => SingleInstanceWireResponse {
                                    accepted: false,
                                    conflict: true,
                                    message: Some(reason),
                                },
                            }
                        }
                        Ok(_) => SingleInstanceWireResponse {
                            accepted: false,
                            conflict: false,
                            message: Some(
                                "Single-instance activation token did not match running instance."
                                    .to_string(),
                            ),
                        },
                        Err(_) => SingleInstanceWireResponse {
                            accepted: false,
                            conflict: false,
                            message: Some(
                                "Malformed single-instance activation request.".to_string(),
                            ),
                        },
                    };

                    if let Ok(mut bytes) = serde_json::to_vec(&response) {
                        bytes.push(b'\n');
                        let _ = stream.write_all(&bytes);
                        let _ = stream.flush();
                    }
                }
            })
            .map_err(|_| {
                AppError::new(
                    "instance_thread_failed",
                    "Unable to spawn single-instance activation listener thread.",
                )
            })?;

        Ok(Self {
            lock_file,
            endpoint_path,
            listen_addr,
            shutdown,
            acceptor_thread: Some(acceptor_thread),
        })
    }
}

fn generate_instance_token(addr: SocketAddr) -> String {
    let mut hasher = Sha256::new();
    hasher.update(std::process::id().to_le_bytes());
    hasher.update(addr.to_string().as_bytes());
    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    hasher.update(now_nanos.to_le_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

fn notify_existing_instance(
    endpoint_path: &Path,
    incoming_fingerprint: &LaunchFingerprint,
) -> Result<SingleInstanceWireResponse, AppError> {
    let meta = fs::symlink_metadata(endpoint_path).map_err(|_| {
        AppError::new(
            "instance_endpoint_missing",
            "Existing Threadlane desktop instance lock is held, but its activation endpoint file is unavailable.",
        )
    })?;
    if meta.file_type().is_symlink()
        || !meta.file_type().is_file()
        || meta.len() > MAX_SINGLE_INSTANCE_BYTES
    {
        return Err(AppError::new(
            "security_violation",
            "Existing Threadlane desktop instance endpoint file failed validation.",
        ));
    }

    let raw = fs::read(endpoint_path).map_err(|_| {
        AppError::new(
            "instance_endpoint_read_failed",
            "Unable to read existing Threadlane desktop instance endpoint file.",
        )
    })?;
    let endpoint: SingleInstanceEndpointRecord = serde_json::from_slice(&raw).map_err(|_| {
        AppError::new(
            "instance_endpoint_corrupt",
            "Existing Threadlane desktop instance endpoint file is malformed.",
        )
    })?;
    if endpoint.schema_version != SINGLE_INSTANCE_SCHEMA_VERSION || endpoint.pid == 0 {
        return Err(AppError::new(
            "instance_endpoint_corrupt",
            "Existing Threadlane desktop instance endpoint schema version or PID is invalid.",
        ));
    }

    let addr: SocketAddr = endpoint.addr.parse().map_err(|_| {
        AppError::new(
            "instance_endpoint_corrupt",
            "Existing Threadlane desktop instance endpoint address is invalid.",
        )
    })?;
    if !addr.ip().is_loopback() {
        return Err(AppError::new(
            "security_violation",
            "Single-instance activation endpoint must use a loopback address.",
        ));
    }

    // Perform an upfront compatibility check so argument conflicts are caught deterministically
    // even before sending over the socket, and also verified by the primary instance.
    if let Err(reason) =
        evaluate_second_launch_compatibility(&endpoint.fingerprint, incoming_fingerprint)
    {
        return Ok(SingleInstanceWireResponse {
            accepted: false,
            conflict: true,
            message: Some(reason),
        });
    }

    let mut stream =
        TcpStream::connect_timeout(&addr, Duration::from_millis(1500)).map_err(|_| {
            AppError::new(
                "instance_connect_failed",
                "Unable to connect to the running Threadlane desktop instance.",
            )
        })?;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(1500)));

    let req = SingleInstanceWireRequest {
        token: endpoint.token,
        fingerprint: incoming_fingerprint.clone(),
    };
    let mut payload = serde_json::to_vec(&req).map_err(|_| {
        AppError::new(
            "instance_request_failed",
            "Unable to encode single-instance activation request.",
        )
    })?;
    payload.push(b'\n');
    stream.write_all(&payload).map_err(|_| {
        AppError::new(
            "instance_request_failed",
            "Unable to send single-instance activation request.",
        )
    })?;
    stream.flush().map_err(|_| {
        AppError::new(
            "instance_request_failed",
            "Unable to flush single-instance activation request.",
        )
    })?;

    let mut line = String::new();
    let mut reader = BufReader::new(stream.take(MAX_SINGLE_INSTANCE_BYTES));
    reader.read_line(&mut line).map_err(|_| {
        AppError::new(
            "instance_response_failed",
            "Unable to read activation response from running Threadlane desktop instance.",
        )
    })?;

    serde_json::from_str::<SingleInstanceWireResponse>(line.trim()).map_err(|_| {
        AppError::new(
            "instance_response_corrupt",
            "Received malformed activation response from running Threadlane desktop instance.",
        )
    })
}

struct TrayRuntimeInner {
    close_behavior: CloseBehavior,
    tray_available: bool,
    persistence_warning: Option<String>,
    tray_status_message: Option<String>,
    placement_notice: Option<String>,
    status_message: Option<String>,
    explicit_quit_requested: bool,
    last_popover_opened_at: Option<Instant>,
    last_popover_blur_hidden_at: Option<Instant>,
    last_tray_click_at: Option<Instant>,
    last_raw_anchor: Option<RawTrayAnchor>,
    tray_icon: Option<TrayIcon>,
    _single_instance: Option<SingleInstanceGuard>,
}

/// Managed desktop state for the system tray icon, popover lifecycle, preferences store,
/// and single-instance coordinator.
///
/// ### Platform tray host-detection note
/// Tauri's `TrayIcon` (`tray-icon 0.25.1`, including the Linux `ksni` StatusNotifierItem backend)
/// does not push asynchronous notification callbacks when an external panel or `StatusNotifierWatcher`
/// host applet crashes after registration. Threadlane therefore:
/// 1. Validates D-Bus session bus presence on Linux before claiming tray availability and avoids
///    panicking if tray creation fails.
/// 2. Hides `main` on close only when `tray_available` is `true`, falling back to quitting when
///    the tray is unavailable so the application never becomes unreachable.
/// 3. Immediately restores, unminimizes, and focuses a hidden `main` window if any runtime tray
///    operation reports that the tray is unavailable, or when a second launch activates the
///    running instance.
#[derive(Clone)]
pub struct TrayState {
    store: TrayPreferencesStore,
    inner: Arc<Mutex<TrayRuntimeInner>>,
}

impl TrayState {
    pub fn new(
        store: TrayPreferencesStore,
        initial: InitialPreferencesLoad,
        tray_available: bool,
        tray_init_message: Option<String>,
        tray_icon: Option<TrayIcon>,
        single_instance: Option<SingleInstanceGuard>,
    ) -> Self {
        let persistence_warning = initial.warning;
        let tray_status_message = tray_init_message;
        let status_message = combine_status_messages(
            persistence_warning.clone(),
            tray_status_message.clone(),
            None,
            tray_available,
        );
        Self {
            store,
            inner: Arc::new(Mutex::new(TrayRuntimeInner {
                close_behavior: initial.close_behavior,
                tray_available,
                persistence_warning,
                tray_status_message,
                placement_notice: None,
                status_message,
                explicit_quit_requested: false,
                last_popover_opened_at: None,
                last_popover_blur_hidden_at: None,
                last_tray_click_at: None,
                last_raw_anchor: None,
                tray_icon,
                _single_instance: single_instance,
            })),
        }
    }

    pub fn preferences_response(&self) -> TrayPreferencesResponse {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        TrayPreferencesResponse {
            close_behavior: guard.close_behavior,
            tray_available: guard.tray_available,
            message: guard.status_message.clone(),
        }
    }

    pub fn update_close_behavior(
        &self,
        close_behavior: CloseBehavior,
    ) -> Result<TrayPreferencesResponse, AppError> {
        match self.store.save_outcome(close_behavior) {
            Ok(stored) => {
                let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                guard.close_behavior = stored.close_behavior;
                guard.persistence_warning = None;
                guard.status_message = combine_status_messages(
                    guard.persistence_warning.clone(),
                    guard.tray_status_message.clone(),
                    guard.placement_notice.clone(),
                    guard.tray_available,
                );
                Ok(TrayPreferencesResponse {
                    close_behavior: guard.close_behavior,
                    tray_available: guard.tray_available,
                    message: guard.status_message.clone(),
                })
            }
            Err(SaveFailureOutcome::PreRename(err)) => Err(err),
            Err(SaveFailureOutcome::PostRenameUncertain { reconciled, error }) => {
                let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                guard.close_behavior = reconciled.close_behavior;
                guard.persistence_warning = Some(error.message.clone());
                guard.status_message = combine_status_messages(
                    guard.persistence_warning.clone(),
                    guard.tray_status_message.clone(),
                    guard.placement_notice.clone(),
                    guard.tray_available,
                );
                Err(error)
            }
        }
    }

    pub fn main_close_decision(&self) -> MainCloseDecision {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        resolve_main_close_decision(
            guard.close_behavior,
            guard.tray_available,
            guard.explicit_quit_requested,
        )
    }

    pub fn popover_close_decision(&self) -> PopoverDismissDecision {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        resolve_popover_close_decision(guard.explicit_quit_requested)
    }

    pub fn popover_blur_decision(&self) -> PopoverDismissDecision {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let elapsed = guard.last_popover_opened_at.map(|t| t.elapsed());
        resolve_popover_blur_decision(guard.explicit_quit_requested, elapsed)
    }

    pub fn request_explicit_quit(&self) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.explicit_quit_requested = true;
    }

    pub fn is_explicit_quit_requested(&self) -> bool {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.explicit_quit_requested
    }

    pub fn record_popover_opened(&self) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.last_popover_opened_at = Some(Instant::now());
    }

    pub fn record_popover_blur_hidden(&self) {
        self.record_popover_blur_hidden_at(Instant::now());
    }

    pub fn record_popover_blur_hidden_at(&self, now: Instant) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.last_popover_blur_hidden_at = Some(now);
    }

    /// Evaluates a tray icon mouse click event against button/state filters, duplicate-emission
    /// debounce, current popover visibility, and post-blur reopen suppression.
    pub fn evaluate_tray_click_toggle(
        &self,
        input: TrayClickEvaluationInput,
    ) -> TrayToggleDecision {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let elapsed_since_click = guard
            .last_tray_click_at
            .map(|prev| input.now.saturating_duration_since(prev));
        if !should_toggle_popover_on_tray_click(
            input.button,
            input.button_state,
            input.is_linux,
            elapsed_since_click,
        ) {
            return TrayToggleDecision::IgnoreClick;
        }

        guard.last_tray_click_at = Some(input.now);
        guard.last_raw_anchor = Some(input.raw_anchor);

        if input.popover_currently_visible {
            guard.last_popover_blur_hidden_at = None;
            return TrayToggleDecision::HideVisiblePopover;
        }

        if let Some(blur_hidden_at) = guard.last_popover_blur_hidden_at {
            if input.now.saturating_duration_since(blur_hidden_at) < POPOVER_BLUR_REOPEN_SUPPRESSION
            {
                guard.last_popover_blur_hidden_at = None;
                return TrayToggleDecision::SuppressReopenAfterBlur;
            }
        }

        guard.last_popover_blur_hidden_at = None;
        TrayToggleDecision::OpenPopover
    }

    /// Resolves the raw activation anchor appropriate for `source` without reusing stale click
    /// coordinates when opening from the context menu (`Open status`).
    pub fn anchor_for_activation(&self, source: PopoverActivationSource) -> Option<RawTrayAnchor> {
        match source {
            PopoverActivationSource::TrayIconClick => self.last_raw_anchor(),
            PopoverActivationSource::MenuOpenStatus => {
                let icon = {
                    let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.tray_icon.clone()
                };
                icon.and_then(|ti| ti.rect().ok().flatten())
                    .map(|rect| RawTrayAnchor {
                        click_position: None,
                        tray_rect: Some(RawTrayRect::from_tauri_rect(&rect)),
                    })
            }
        }
    }

    pub fn last_raw_anchor(&self) -> Option<RawTrayAnchor> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.last_raw_anchor
    }

    pub fn set_placement_notice(&self, notice: Option<String>) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.placement_notice = notice.filter(|msg| !msg.trim().is_empty());
        guard.status_message = combine_status_messages(
            guard.persistence_warning.clone(),
            guard.tray_status_message.clone(),
            guard.placement_notice.clone(),
            guard.tray_available,
        );
    }

    pub fn record_popover_open_outcome(
        &self,
        outcome: &PopoverPlacementOutcome,
        effective_notice: Option<String>,
    ) -> TrayPreferencesResponse {
        debug_assert!(outcome.validate_invariants());
        let resolved_notice =
            effective_notice.or_else(|| outcome.diagnostic_notice().map(str::to_string));
        self.set_placement_notice(resolved_notice);
        self.preferences_response()
    }

    #[cfg(any(target_os = "linux", test))]
    pub fn record_runtime_capability_notice(
        &self,
        capability_notice: &str,
    ) -> TrayPreferencesResponse {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.placement_notice = tray_linux::combine_popover_notices(
            guard.placement_notice.as_deref(),
            Some(capability_notice),
        );
        guard.status_message = combine_status_messages(
            guard.persistence_warning.clone(),
            guard.tray_status_message.clone(),
            guard.placement_notice.clone(),
            guard.tray_available,
        );
        TrayPreferencesResponse {
            close_behavior: guard.close_behavior,
            tray_available: guard.tray_available,
            message: guard.status_message.clone(),
        }
    }

    pub fn mark_tray_unavailable(&self, reason: impl Into<String>) -> TrayPreferencesResponse {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.tray_available = false;
        guard.tray_status_message = Some(reason.into());
        guard.status_message = combine_status_messages(
            guard.persistence_warning.clone(),
            guard.tray_status_message.clone(),
            guard.placement_notice.clone(),
            guard.tray_available,
        );
        TrayPreferencesResponse {
            close_behavior: guard.close_behavior,
            tray_available: guard.tray_available,
            message: guard.status_message.clone(),
        }
    }

    #[cfg(any(target_os = "linux", test))]
    pub fn apply_linux_open_outcome(
        &self,
        open_outcome: Result<tray_linux::LinuxOpenOutcome, String>,
    ) -> LinuxOpenPublicationAction {
        match open_outcome {
            Ok(tray_linux::LinuxOpenOutcome::Presented {
                outcome,
                effective_notice,
            }) => {
                let payload = self.record_popover_open_outcome(&outcome, effective_notice);
                LinuxOpenPublicationAction::EmitOpened(payload)
            }
            Ok(tray_linux::LinuxOpenOutcome::SupersededSameGeneration) => {
                LinuxOpenPublicationAction::EmitOpened(self.preferences_response())
            }
            Ok(tray_linux::LinuxOpenOutcome::SupersededOrCancelled) => {
                LinuxOpenPublicationAction::NoPublication
            }
            Err(_) => LinuxOpenPublicationAction::RecoverMainOnCurrentFailure,
        }
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxOpenPublicationAction {
    EmitOpened(TrayPreferencesResponse),
    NoPublication,
    RecoverMainOnCurrentFailure,
}

fn combine_status_messages(
    warning: Option<String>,
    tray_init_message: Option<String>,
    placement_notice: Option<String>,
    tray_available: bool,
) -> Option<String> {
    let mut parts = Vec::with_capacity(3);
    if let Some(w) = warning.filter(|s| !s.is_empty()) {
        parts.push(w);
    }
    if let Some(t) = tray_init_message.filter(|s| !s.is_empty()) {
        parts.push(t);
    }
    if tray_available {
        if let Some(p) = placement_notice.filter(|s| !s.is_empty()) {
            parts.push(p);
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}

/// Restores, unminimizes, shows, and focuses the primary `main` window.
pub fn show_and_focus_main_window(app: &AppHandle) {
    #[cfg(target_os = "linux")]
    {
        if !tray_linux::is_linux_gtk_main_thread() {
            let app_clone = app.clone();
            if app
                .run_on_main_thread(move || {
                    tray_linux::release_linux_outside_click_backdrops(&app_clone);
                    if let Some(tray_window) = app_clone.get_webview_window(TRAY_WINDOW_LABEL) {
                        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
                        let _ = tray_window.hide();
                    }
                    if let Some(main_window) = app_clone.get_webview_window(MAIN_WINDOW_LABEL) {
                        let _ = main_window.show();
                        let _ = main_window.unminimize();
                        let _ = main_window.set_focus();
                    }
                })
                .is_ok()
            {
                return;
            }
        } else {
            tray_linux::release_linux_outside_click_backdrops(app);
            if let Some(tray_window) = app.get_webview_window(TRAY_WINDOW_LABEL) {
                tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
                let _ = tray_window.hide();
            }
        }
    }
    if let Some(main_window) = app.get_webview_window(MAIN_WINDOW_LABEL) {
        let _ = main_window.show();
        let _ = main_window.unminimize();
        let _ = main_window.set_focus();
    }
}

/// Hides only the `tray` popover window (and releases any transient Linux Wayland outside-click
/// surfaces fail-open) without affecting `main` or running workers.
pub fn hide_popover_window(app: &AppHandle) {
    #[cfg(target_os = "linux")]
    {
        if !tray_linux::is_linux_gtk_main_thread() {
            let app_clone = app.clone();
            if app
                .run_on_main_thread(move || {
                    tray_linux::release_linux_outside_click_backdrops(&app_clone);
                    if let Some(tray_window) = app_clone.get_webview_window(TRAY_WINDOW_LABEL) {
                        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
                        let _ = tray_window.hide();
                    }
                })
                .is_ok()
            {
                return;
            }
        } else {
            tray_linux::release_linux_outside_click_backdrops(app);
        }
    }
    if let Some(tray_window) = app.get_webview_window(TRAY_WINDOW_LABEL) {
        #[cfg(target_os = "linux")]
        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
        let _ = tray_window.hide();
    }
}

/// Hides the Linux `tray` popover window only when `(generation, attempt_ticket)` still matches
/// the exact unsuperseded, non-cancelled outside-click dismissal epoch on the GTK main thread.
#[cfg(target_os = "linux")]
pub(crate) fn hide_popover_window_if_linux_epoch_current(
    app: &AppHandle,
    generation: u64,
    attempt_ticket: u64,
) {
    if !tray_linux::fence_linux_popover_dismiss_epoch_on_main_thread(
        app,
        generation,
        attempt_ticket,
    ) {
        return;
    }
    if let Some(tray_window) = app.get_webview_window(TRAY_WINDOW_LABEL) {
        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
        if tray_linux::is_linux_dismiss_epoch_unsuperseded_on_main_thread(
            generation,
            attempt_ticket,
        ) {
            let _ = tray_window.hide();
        }
    }
    if tray_linux::is_linux_dismiss_epoch_unsuperseded_on_main_thread(generation, attempt_ticket) {
        if let Some(tray) = app.try_state::<TrayState>() {
            if !tray.is_explicit_quit_requested() {
                ensure_main_reachable_if_tray_unavailable(app, &tray);
            }
        }
    }
}

/// Terminates the desktop application cleanly on explicit user quit without starting new jobs,
/// cancelling independent `threadlane-host` executions, or restarting Codex.
pub fn execute_explicit_quit(app: &AppHandle, tray: &TrayState) {
    tray.request_explicit_quit();
    #[cfg(target_os = "linux")]
    {
        if !tray_linux::is_linux_gtk_main_thread() {
            let app_clone = app.clone();
            if app
                .run_on_main_thread(move || {
                    tray_linux::release_linux_outside_click_backdrops(&app_clone);
                    if let Some(tray_window) = app_clone.get_webview_window(TRAY_WINDOW_LABEL) {
                        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
                        let _ = tray_window.hide();
                        let _ = tray_window.close();
                    }
                    if let Some(main_window) = app_clone.get_webview_window(MAIN_WINDOW_LABEL) {
                        let _ = main_window.close();
                    }
                    app_clone.exit(0);
                })
                .is_ok()
            {
                return;
            }
        } else {
            tray_linux::release_linux_outside_click_backdrops(app);
        }
    }
    if let Some(tray_window) = app.get_webview_window(TRAY_WINDOW_LABEL) {
        #[cfg(target_os = "linux")]
        tray_linux::hide_linux_popover_gtk_window_on_main_thread(&tray_window);
        let _ = tray_window.hide();
        let _ = tray_window.close();
    }
    if let Some(main_window) = app.get_webview_window(MAIN_WINDOW_LABEL) {
        let _ = main_window.close();
    }
    app.exit(0);
}

/// Ensures `main` is visible and reachable if `tray_available` is false while `main` is hidden.
pub fn ensure_main_reachable_if_tray_unavailable(app: &AppHandle, tray: &TrayState) {
    let prefs = tray.preferences_response();
    let main_visible = app
        .get_webview_window(MAIN_WINDOW_LABEL)
        .and_then(|w| w.is_visible().ok())
        .unwrap_or(true);
    if resolve_tray_availability_recovery(
        prefs.tray_available,
        main_visible,
        tray.is_explicit_quit_requested(),
    ) == TrayAvailabilityRecoveryDecision::RestoreHiddenMain
    {
        show_and_focus_main_window(app);
    }
}

/// Handles a close action initiated via `desktop_window_action(WindowAction::Close)`.
pub fn handle_window_action_close(
    app: &AppHandle,
    window: &Window,
    tray: &TrayState,
) -> Result<(), AppError> {
    if window.label() == TRAY_WINDOW_LABEL {
        hide_popover_window(app);
        ensure_main_reachable_if_tray_unavailable(app, tray);
        return Ok(());
    }

    match tray.main_close_decision() {
        MainCloseDecision::HideMainToTray => {
            if window.hide().is_err() {
                let updated = tray.mark_tray_unavailable(
                    "Unable to hide window to system tray; keeping main window visible.",
                );
                let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &updated);
                show_and_focus_main_window(app);
                return Err(AppError::new(
                    "window_hide_failed",
                    "Unable to hide desktop window to the system tray.",
                ));
            }
            Ok(())
        }
        MainCloseDecision::QuitApp => {
            execute_explicit_quit(app, tray);
            Ok(())
        }
    }
}

/// Handles Tauri native [`WindowEvent`] notifications for both `main` and `tray` windows.
pub fn handle_window_event(window: &Window, event: &WindowEvent) {
    let app = window.app_handle();
    let Some(tray) = app.try_state::<TrayState>() else {
        return;
    };

    match window.label() {
        MAIN_WINDOW_LABEL => match event {
            WindowEvent::CloseRequested { api, .. } => match tray.main_close_decision() {
                MainCloseDecision::HideMainToTray => {
                    api.prevent_close();
                    if window.hide().is_err() {
                        let updated = tray.mark_tray_unavailable(
                            "Unable to hide window to system tray; keeping main window reachable.",
                        );
                        let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &updated);
                        show_and_focus_main_window(app);
                    }
                }
                MainCloseDecision::QuitApp => {
                    tray.request_explicit_quit();
                    #[cfg(target_os = "linux")]
                    tray_linux::release_linux_outside_click_backdrops(app);
                    if let Some(tray_window) = app.get_webview_window(TRAY_WINDOW_LABEL) {
                        let _ = tray_window.hide();
                        let _ = tray_window.close();
                    }
                    app.exit(0);
                }
            },
            #[cfg(not(target_os = "linux"))]
            WindowEvent::Focused(true) => {
                if let Some(popover) = app.get_webview_window(TRAY_WINDOW_LABEL) {
                    if popover.is_visible().unwrap_or(false)
                        && tray.popover_blur_decision() == PopoverDismissDecision::HidePopoverOnly
                    {
                        tray.record_popover_blur_hidden();
                        let _ = popover.hide();
                        ensure_main_reachable_if_tray_unavailable(app, &tray);
                    }
                }
            }
            _ => {}
        },
        TRAY_WINDOW_LABEL => match event {
            WindowEvent::CloseRequested { api, .. } => match tray.popover_close_decision() {
                PopoverDismissDecision::HidePopoverOnly
                | PopoverDismissDecision::IgnoreTransientBlur => {
                    api.prevent_close();
                    hide_popover_window(app);
                    ensure_main_reachable_if_tray_unavailable(app, &tray);
                }
                PopoverDismissDecision::AllowCloseForQuit => {
                    #[cfg(target_os = "linux")]
                    tray_linux::release_linux_outside_click_backdrops(app);
                }
            },
            WindowEvent::Focused(false) => match tray.popover_blur_decision() {
                PopoverDismissDecision::HidePopoverOnly => {
                    #[cfg(target_os = "linux")]
                    {
                        tray_linux::handle_linux_popover_unfocused(app, &tray);
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        tray.record_popover_blur_hidden();
                        let _ = window.hide();
                        ensure_main_reachable_if_tray_unavailable(app, &tray);
                    }
                }
                PopoverDismissDecision::IgnoreTransientBlur
                | PopoverDismissDecision::AllowCloseForQuit => {}
            },
            WindowEvent::ScaleFactorChanged { .. } => {
                if let Some(popover) = app.get_webview_window(TRAY_WINDOW_LABEL) {
                    let monitors = resolve_available_monitors(app);
                    let raw_anchor = tray.last_raw_anchor();
                    #[cfg(target_os = "linux")]
                    {
                        tray_linux::refresh_linux_popover_size_on_scale_change(
                            app, &popover, &monitors, raw_anchor,
                        );
                    }
                    #[cfg(not(target_os = "linux"))]
                    {
                        let resolved = resolve_tray_anchor_for_monitors(&monitors, raw_anchor);
                        let (lw, lh) = compute_popover_logical_size_for_monitor(resolved.monitor);
                        apply_non_linux_popover_logical_size(&popover, lw, lh);
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_non_linux_popover_logical_size(
    popover: &WebviewWindow,
    logical_width: f64,
    logical_height: f64,
) {
    let target = LogicalSize::new(logical_width, logical_height);
    let _ = popover.set_min_size(None::<LogicalSize<f64>>);
    let _ = popover.set_max_size(None::<LogicalSize<f64>>);
    let _ = popover.set_min_size(Some(target));
    let _ = popover.set_max_size(Some(target));
    let _ = popover.set_size(target);
}

fn ensure_or_create_tray_popover_window(
    app: &AppHandle,
) -> Result<(WebviewWindow, Option<String>), String> {
    if let Some(existing) = app.get_webview_window(TRAY_WINDOW_LABEL) {
        #[cfg(target_os = "linux")]
        tray_linux::attach_linux_main_window_outside_click_guard(app);
        return Ok((existing, None));
    }

    let popover = WebviewWindowBuilder::new(
        app,
        TRAY_WINDOW_LABEL,
        WebviewUrl::App(TRAY_WINDOW_URL.into()),
    )
    .title("Threadlane Status")
    .inner_size(TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT)
    .min_inner_size(TRAY_POPOVER_MIN_WIDTH, TRAY_POPOVER_MIN_HEIGHT)
    .max_inner_size(TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT)
    .resizable(false)
    .decorations(false)
    .transparent(true)
    .always_on_top(true)
    .skip_taskbar(true)
    .visible(false)
    .focused(false)
    .initialization_script(TRAY_ESCAPE_INIT_SCRIPT)
    .build()
    .map_err(|err| format!("Failed to create tray popover window: {err}"))?;

    #[cfg(target_os = "linux")]
    let init_notice = tray_linux::initialize_linux_popover_window(app, &popover);
    #[cfg(not(target_os = "linux"))]
    let init_notice: Option<String> = None;

    Ok((popover, init_notice))
}

fn resolve_available_monitors(app: &AppHandle) -> Vec<MonitorBoundsPx> {
    let mut out = Vec::new();
    if let Ok(monitors) = app.available_monitors() {
        for monitor in monitors {
            let pos = monitor.position();
            let size = monitor.size();
            out.push(MonitorBoundsPx {
                x: pos.x,
                y: pos.y,
                width: size.width,
                height: size.height,
                scale_factor: monitor.scale_factor(),
            });
        }
    }
    if out.is_empty() {
        if let Ok(Some(primary)) = app.primary_monitor() {
            let pos = primary.position();
            let size = primary.size();
            out.push(MonitorBoundsPx {
                x: pos.x,
                y: pos.y,
                width: size.width,
                height: size.height,
                scale_factor: primary.scale_factor(),
            });
        }
    }
    out
}

/// Shows and presents the compact `tray` popover window for `source`, anchoring to real tray
/// icon/activation geometry when supported by the active platform/compositor and emitting
/// `threadlane:tray-opened` with truthful capability status.
pub fn open_popover_window_with_source(
    app: &AppHandle,
    tray: &TrayState,
    source: PopoverActivationSource,
) {
    #[cfg(target_os = "linux")]
    if !tray_linux::is_linux_gtk_main_thread() {
        let app_clone = app.clone();
        let tray_clone = tray.clone();
        if app
            .run_on_main_thread(move || {
                open_popover_window_with_source(&app_clone, &tray_clone, source);
            })
            .is_err()
        {
            show_and_focus_main_window(app);
        }
        return;
    }

    let Ok((popover, init_notice)) = ensure_or_create_tray_popover_window(app) else {
        show_and_focus_main_window(app);
        return;
    };
    if let Some(notice) = init_notice {
        tray.set_placement_notice(Some(notice));
    }

    let raw_anchor = tray.anchor_for_activation(source);
    let monitors = resolve_available_monitors(app);

    #[cfg(target_os = "linux")]
    {
        let open_outcome = tray_linux::open_and_present_linux_popover(
            app,
            &popover,
            &monitors,
            raw_anchor,
            || tray.record_popover_opened(),
        );
        match tray.apply_linux_open_outcome(open_outcome) {
            LinuxOpenPublicationAction::EmitOpened(payload) => {
                let _ = app.emit(TRAY_OPENED_EVENT, &payload);
            }
            LinuxOpenPublicationAction::NoPublication => {}
            LinuxOpenPublicationAction::RecoverMainOnCurrentFailure => {
                show_and_focus_main_window(app);
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let resolved = resolve_tray_anchor_for_monitors(&monitors, raw_anchor);
        let (logical_w, logical_h) = compute_popover_logical_size_for_monitor(resolved.monitor);
        apply_non_linux_popover_logical_size(&popover, logical_w, logical_h);
        let (x, y) = compute_popover_position_px(
            resolved.monitor,
            (logical_w, logical_h),
            resolved.click_position_px,
            resolved.tray_rect_px,
            false,
        );
        let outcome = if resolved.has_valid_anchor {
            PopoverPlacementOutcome::IconAnchored {
                x,
                y,
                scale_factor: resolved.monitor.scale_factor,
            }
        } else {
            PopoverPlacementOutcome::FallbackMonitorCorner {
                x,
                y,
                scale_factor: resolved.monitor.scale_factor,
                reason: UnanchoredPlacementReason::MissingTrayCoordinates,
                notice: NON_LINUX_MISSING_COORDS_NOTICE.to_string(),
            }
        };
        let _ = popover.set_position(PhysicalPosition::new(x, y));
        tray.record_popover_opened();
        if popover.show().is_err() {
            show_and_focus_main_window(app);
            return;
        }
        let _ = popover.unminimize();
        let _ = popover.set_focus();

        let notice = outcome.diagnostic_notice().map(str::to_string);
        let payload = tray.record_popover_open_outcome(&outcome, notice);
        let _ = app.emit(TRAY_OPENED_EVENT, &payload);
    }
}

#[cfg(target_os = "linux")]
fn platform_tray_environment_check() -> Result<(), String> {
    let has_session_bus_env =
        std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some_and(|val| !val.is_empty());
    let has_runtime_bus_socket = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|dir| !dir.is_empty())
        .is_some_and(|dir| Path::new(&dir).join("bus").exists());

    if has_session_bus_env || has_runtime_bus_socket {
        Ok(())
    } else {
        Err(
            "System tray is unavailable because no desktop D-Bus session bus was detected."
                .to_string(),
        )
    }
}

#[cfg(not(target_os = "linux"))]
fn platform_tray_environment_check() -> Result<(), String> {
    Ok(())
}

/// Synthesizes a 32x32 monochrome transparent RGBA connection-node icon matching `icons/tray.svg`
/// as a safe fallback if `icons/tray.png` has not been rendered to disk yet.
pub fn synthesize_monochrome_tray_icon_rgba() -> (Vec<u8>, u32, u32) {
    let width = 32u32;
    let height = 32u32;
    let mut rgba = vec![0u8; (width * height * 4) as usize];

    let mut set_px = |x: i32, y: i32, alpha: u8| {
        if x >= 0 && x < width as i32 && y >= 0 && y < height as i32 {
            let idx = ((y as u32 * width + x as u32) * 4) as usize;
            if alpha > rgba[idx + 3] {
                rgba[idx] = 0;
                rgba[idx + 1] = 0;
                rgba[idx + 2] = 0;
                rgba[idx + 3] = alpha;
            }
        }
    };

    let mut fill_circle = |cx: f32, cy: f32, radius: f32| {
        let min_x = (cx - radius - 1.0).floor() as i32;
        let max_x = (cx + radius + 1.0).ceil() as i32;
        let min_y = (cy - radius - 1.0).floor() as i32;
        let max_y = (cy + radius + 1.0).ceil() as i32;
        for y in min_y..=max_y {
            for x in min_x..=max_x {
                let dx = (x as f32 + 0.5) - cx;
                let dy = (y as f32 + 0.5) - cy;
                let dist = (dx * dx + dy * dy).sqrt();
                if dist <= radius - 0.5 {
                    set_px(x, y, 255);
                } else if dist <= radius + 0.5 {
                    let a = ((radius + 0.5 - dist).clamp(0.0, 1.0) * 255.0) as u8;
                    set_px(x, y, a);
                }
            }
        }
    };

    // Left vertical stem from (10, 6) to (10, 26)
    for y in 6..=26 {
        fill_circle(10.0, y as f32, 1.5);
    }
    // Right stem from (22, 6) down to (22, 14) and curved branch to (10, 18)
    for y in 6..=14 {
        fill_circle(22.0, y as f32, 1.5);
    }
    for x in 10..=19 {
        fill_circle(x as f32, 18.0, 1.5);
    }
    fill_circle(21.2, 16.2, 1.5);
    fill_circle(20.0, 17.4, 1.5);

    // Three connection nodes matching icons/tray.svg
    fill_circle(10.0, 6.0, 3.0);
    fill_circle(10.0, 26.0, 3.0);
    fill_circle(22.0, 6.0, 3.0);

    (rgba, width, height)
}

fn macos_tray_icon_candidates(app: &AppHandle) -> Vec<PathBuf> {
    let mut candidate_paths = Vec::new();
    if let Ok(res_dir) = app.path().resource_dir() {
        candidate_paths.push(res_dir.join(MACOS_TRAY_ICON_REL_PATH));
        candidate_paths.push(res_dir.join("tray.png"));
    }
    candidate_paths.push(Path::new(env!("CARGO_MANIFEST_DIR")).join(MACOS_TRAY_ICON_REL_PATH));
    candidate_paths
}

fn load_tray_icon_image(app: &AppHandle) -> (Image<'static>, bool) {
    let template_candidates = macos_tray_icon_candidates(app);

    if cfg!(target_os = "macos") {
        for candidate in template_candidates {
            if let Ok(img) = Image::from_path(&candidate) {
                return (
                    Image::new_owned(img.rgba().to_vec(), img.width(), img.height()),
                    true,
                );
            }
        }

        let (rgba, w, h) = synthesize_monochrome_tray_icon_rgba();
        return (Image::new_owned(rgba, w, h), true);
    }

    if let Some(default_icon) = app.default_window_icon() {
        return (
            Image::new_owned(
                default_icon.rgba().to_vec(),
                default_icon.width(),
                default_icon.height(),
            ),
            false,
        );
    }

    for candidate in template_candidates {
        if let Ok(img) = Image::from_path(&candidate) {
            return (
                Image::new_owned(img.rgba().to_vec(), img.width(), img.height()),
                false,
            );
        }
    }

    let (rgba, w, h) = synthesize_monochrome_tray_icon_rgba();
    (Image::new_owned(rgba, w, h), false)
}

fn build_system_tray(app: &AppHandle) -> Result<TrayIcon, String> {
    platform_tray_environment_check()?;

    let open_status_item = MenuItem::with_id(
        app,
        MENU_ID_OPEN_STATUS,
        MENU_LABEL_OPEN_STATUS,
        true,
        None::<&str>,
    )
    .map_err(|err| format!("Failed to create Open status menu item: {err}"))?;
    let open_main_item = MenuItem::with_id(
        app,
        MENU_ID_OPEN_MAIN,
        MENU_LABEL_OPEN_MAIN,
        true,
        None::<&str>,
    )
    .map_err(|err| format!("Failed to create Open Threadlane menu item: {err}"))?;
    let separator = PredefinedMenuItem::separator(app)
        .map_err(|err| format!("Failed to create tray menu separator: {err}"))?;
    let quit_item = MenuItem::with_id(app, MENU_ID_QUIT, MENU_LABEL_QUIT, true, None::<&str>)
        .map_err(|err| format!("Failed to create Quit menu item: {err}"))?;

    let menu = Menu::with_items(
        app,
        &[&open_status_item, &open_main_item, &separator, &quit_item],
    )
    .map_err(|err| format!("Failed to construct system tray menu: {err}"))?;

    let (icon_image, use_as_template) = load_tray_icon_image(app);

    TrayIconBuilder::with_id(TRAY_ICON_ID)
        .icon(icon_image)
        .icon_as_template(use_as_template)
        .tooltip("Threadlane")
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| {
            let Some(tray) = app.try_state::<TrayState>() else {
                return;
            };
            match event.id().as_ref() {
                MENU_ID_OPEN_STATUS => {
                    open_popover_window_with_source(
                        app,
                        &tray,
                        PopoverActivationSource::MenuOpenStatus,
                    );
                }
                MENU_ID_OPEN_MAIN => {
                    hide_popover_window(app);
                    show_and_focus_main_window(app);
                }
                MENU_ID_QUIT => {
                    execute_explicit_quit(app, &tray);
                }
                _ => {}
            }
        })
        .on_tray_icon_event(|tray_handle, event| {
            if let TrayIconEvent::Click {
                position,
                rect,
                button,
                button_state,
                ..
            } = event
            {
                let app = tray_handle.app_handle();
                let Some(tray) = app.try_state::<TrayState>() else {
                    return;
                };
                let popover_visible = app
                    .get_webview_window(TRAY_WINDOW_LABEL)
                    .and_then(|w| w.is_visible().ok())
                    .unwrap_or(false);
                let raw_anchor = RawTrayAnchor::from_click_event(position, &rect);
                match tray.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                    button,
                    button_state,
                    is_linux: cfg!(target_os = "linux"),
                    popover_currently_visible: popover_visible,
                    raw_anchor,
                    now: Instant::now(),
                }) {
                    TrayToggleDecision::IgnoreClick
                    | TrayToggleDecision::SuppressReopenAfterBlur => {}
                    TrayToggleDecision::HideVisiblePopover => {
                        hide_popover_window(app);
                        ensure_main_reachable_if_tray_unavailable(app, &tray);
                    }
                    TrayToggleDecision::OpenPopover => {
                        open_popover_window_with_source(
                            app,
                            &tray,
                            PopoverActivationSource::TrayIconClick,
                        );
                    }
                }
            }
        })
        .build(app)
        .map_err(|err| format!("Failed to initialize system tray icon: {err}"))
}

/// Initializes the tray preferences store, hidden frameless `tray` popover window, and single
/// OS system tray icon without panicking on tray failure.
pub fn setup_tray(
    app: &AppHandle,
    app_data_dir: &Path,
    single_instance: Option<SingleInstanceGuard>,
) -> Result<(), String> {
    let store = TrayPreferencesStore::new(app_data_dir.to_path_buf());
    let initial = store.load_or_default();

    #[cfg(target_os = "linux")]
    tray_linux::attach_linux_main_window_outside_click_guard(app);

    let init_placement_notice = ensure_or_create_tray_popover_window(app)
        .ok()
        .and_then(|(_, notice)| notice);

    let (tray_available, tray_init_message, tray_icon) = match build_system_tray(app) {
        Ok(icon) => (true, None, Some(icon)),
        Err(_err) => (
            false,
            Some(
                "System tray is unavailable in this desktop session; closing the main window will quit Threadlane instead of hiding."
                    .to_string(),
            ),
            None,
        ),
    };

    let state = TrayState::new(
        store,
        initial,
        tray_available,
        tray_init_message,
        tray_icon,
        single_instance,
    );
    state.set_placement_notice(init_placement_notice);
    app.manage(state);
    Ok(())
}

/// Returns the current tray close-behavior preference and system tray availability.
#[tauri::command]
pub(crate) fn desktop_tray_preferences(
    tray: State<'_, TrayState>,
) -> Result<TrayPreferencesResponse, AppError> {
    Ok(tray.preferences_response())
}

/// Persists and immediately applies the user's tray close-behavior preference without relaunch.
#[tauri::command]
pub(crate) fn desktop_set_tray_preferences(
    app: AppHandle,
    window: Window,
    tray: State<'_, TrayState>,
    request: SetTrayPreferencesRequest,
) -> Result<TrayPreferencesResponse, AppError> {
    ensure_main_window_only(&window)?;
    match tray.update_close_behavior(request.close_behavior) {
        Ok(response) => {
            let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &response);
            Ok(response)
        }
        Err(err) => {
            if err.code == PREFERENCES_DURABILITY_UNCERTAIN_CODE {
                let reconciled = tray.preferences_response();
                let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &reconciled);
            }
            Err(err)
        }
    }
}

/// Executes a tray surface action (`show_main`, `hide`, or `quit`).
#[tauri::command]
pub(crate) fn desktop_tray_action(
    app: AppHandle,
    window: Window,
    tray: State<'_, TrayState>,
    action: TrayAction,
) -> Result<(), AppError> {
    match action {
        TrayAction::ShowMain => {
            hide_popover_window(&app);
            show_and_focus_main_window(&app);
            Ok(())
        }
        TrayAction::Hide => {
            if window.label() == TRAY_WINDOW_LABEL {
                hide_popover_window(&app);
                ensure_main_reachable_if_tray_unavailable(&app, &tray);
                return Ok(());
            }
            let prefs = tray.preferences_response();
            if prefs.tray_available {
                window.hide().map_err(|_| {
                    AppError::new(
                        "window_hide_failed",
                        "Unable to hide desktop window to system tray.",
                    )
                })
            } else {
                show_and_focus_main_window(&app);
                Err(AppError::new(
                    "tray_unavailable",
                    "System tray is unavailable; refusing to hide main window.",
                ))
            }
        }
        TrayAction::Quit => {
            execute_explicit_quit(&app, &tray);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tempfile::TempDir;

    #[test]
    fn default_preferences_are_hide_to_tray_with_null_message_wire_format() {
        let tmp = TempDir::new().expect("tempdir");
        let store = TrayPreferencesStore::new(tmp.path().join("app-data"));
        let initial = store.load_or_default();
        assert_eq!(initial.close_behavior, CloseBehavior::HideToTray);
        assert_eq!(initial.warning, None);

        let state = TrayState::new(store, initial, true, None, None, None);
        let response = state.preferences_response();
        assert_eq!(response.close_behavior, CloseBehavior::HideToTray);
        assert!(response.tray_available);
        assert_eq!(response.message, None);

        let json = serde_json::to_value(&response).expect("serialize response");
        assert_eq!(json["close_behavior"], "hide_to_tray");
        assert_eq!(json["tray_available"], true);
        assert!(json.get("message").is_some());
        assert_eq!(json["message"], serde_json::Value::Null);
    }

    #[test]
    fn roundtrip_save_and_reload_updates_immediately_and_persists_atomically() {
        let tmp = TempDir::new().expect("tempdir");
        let app_data = tmp.path().join("app-data");
        let store = TrayPreferencesStore::new(&app_data);
        let initial = store.load_or_default();
        let state = TrayState::new(store.clone(), initial, true, None, None, None);

        let updated = state
            .update_close_behavior(CloseBehavior::Quit)
            .expect("save quit preference");
        assert_eq!(updated.close_behavior, CloseBehavior::Quit);
        assert_eq!(state.main_close_decision(), MainCloseDecision::QuitApp);

        let reopened = TrayPreferencesStore::new(&app_data).load_or_default();
        assert_eq!(reopened.close_behavior, CloseBehavior::Quit);
        assert_eq!(reopened.warning, None);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = fs::symlink_metadata(store.preferences_path())
                .expect("prefs metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(file_mode, 0o600);
        }

        let back = state
            .update_close_behavior(CloseBehavior::HideToTray)
            .expect("save hide_to_tray preference");
        assert_eq!(back.close_behavior, CloseBehavior::HideToTray);
        assert_eq!(
            state.main_close_decision(),
            MainCloseDecision::HideMainToTray
        );
    }

    #[test]
    fn corrupt_or_unexpected_preferences_files_are_preserved_and_never_silently_overwritten() {
        let tmp = TempDir::new().expect("tempdir");
        let app_data = tmp.path().join("app-data");
        fs::create_dir_all(&app_data).expect("create app_data");
        let store = TrayPreferencesStore::new(&app_data);
        let prefs_path = store.preferences_path();

        // Case 1: Malformed JSON is preserved on load and rejected on save.
        let malformed = "{not-valid-json\n";
        fs::write(&prefs_path, malformed).expect("write malformed");

        let loaded = store.load_or_default();
        assert_eq!(loaded.close_behavior, CloseBehavior::HideToTray);
        assert!(loaded.warning.is_some());
        assert_eq!(
            fs::read_to_string(&prefs_path).expect("read preserved malformed"),
            malformed
        );

        let save_err = store
            .save(CloseBehavior::Quit)
            .expect_err("saving over malformed file must fail");
        assert_eq!(save_err.code, "corrupt_preferences");
        assert!(!save_err.message.contains(app_data.to_str().unwrap()));
        assert_eq!(
            fs::read_to_string(&prefs_path).expect("malformed still preserved"),
            malformed
        );

        // Case 2: Unknown field rejected by deny_unknown_fields and preserved on disk.
        let unexpected_field =
            "{\n  \"schema_version\": 1,\n  \"close_behavior\": \"quit\",\n  \"extra\": 42\n}\n";
        fs::write(&prefs_path, unexpected_field).expect("write unexpected field");

        let loaded_unexpected = store.load_or_default();
        assert_eq!(loaded_unexpected.close_behavior, CloseBehavior::HideToTray);
        assert!(loaded_unexpected.warning.is_some());

        let unexpected_err = store
            .save(CloseBehavior::Quit)
            .expect_err("saving over unexpected-field file must fail");
        assert_eq!(unexpected_err.code, "corrupt_preferences");
        assert_eq!(
            fs::read_to_string(&prefs_path).expect("unexpected field file preserved"),
            unexpected_field
        );

        // Case 3: Unsupported schema version rejected and preserved.
        let bad_schema = "{\n  \"schema_version\": 99,\n  \"close_behavior\": \"quit\"\n}\n";
        fs::write(&prefs_path, bad_schema).expect("write bad schema");
        let schema_err = store
            .save(CloseBehavior::HideToTray)
            .expect_err("saving over unsupported schema must fail");
        assert_eq!(schema_err.code, "corrupt_preferences");
        assert_eq!(
            fs::read_to_string(&prefs_path).expect("bad schema preserved"),
            bad_schema
        );
    }

    #[test]
    fn setter_request_dto_denies_unknown_fields_and_validates_variants() {
        let valid: SetTrayPreferencesRequest =
            serde_json::from_str(r#"{"close_behavior":"hide_to_tray"}"#).expect("valid hide");
        assert_eq!(valid.close_behavior, CloseBehavior::HideToTray);

        let valid_quit: SetTrayPreferencesRequest =
            serde_json::from_str(r#"{"close_behavior":"quit"}"#).expect("valid quit");
        assert_eq!(valid_quit.close_behavior, CloseBehavior::Quit);

        assert!(
            serde_json::from_str::<SetTrayPreferencesRequest>(
                r#"{"close_behavior":"quit","unexpected":true}"#
            )
            .is_err(),
            "unknown fields must be rejected"
        );
        assert!(
            serde_json::from_str::<SetTrayPreferencesRequest>(r#"{"close_behavior":"minimize"}"#)
                .is_err(),
            "unknown enum variant must be rejected"
        );
    }

    #[test]
    fn save_failure_returns_sanitized_error_and_leaves_in_memory_state_unchanged() {
        let tmp = TempDir::new().expect("tempdir");
        let blocked_app_data = tmp.path().join("not-a-directory");
        fs::write(&blocked_app_data, b"regular file blocking dir creation").expect("write blocker");

        let store = TrayPreferencesStore::new(&blocked_app_data);
        let initial = InitialPreferencesLoad {
            close_behavior: CloseBehavior::HideToTray,
            warning: None,
        };
        let state = TrayState::new(store, initial, true, None, None, None);

        let err = state
            .update_close_behavior(CloseBehavior::Quit)
            .expect_err("save must fail when app_data_dir is a regular file");
        assert_eq!(err.code, "security_violation");
        assert!(!err.message.contains(blocked_app_data.to_str().unwrap()));
        assert_eq!(
            state.preferences_response().close_behavior,
            CloseBehavior::HideToTray
        );
    }

    #[test]
    fn post_rename_directory_sync_failure_reconciles_memory_with_disk_and_exposes_uncertainty() {
        let tmp = TempDir::new().expect("tempdir");
        let app_data = tmp.path().join("app-data");
        let fail_dir_sync = Arc::new(AtomicBool::new(true));
        let sync_calls = Arc::new(AtomicUsize::new(0));

        let fail_flag = Arc::clone(&fail_dir_sync);
        let calls_counter = Arc::clone(&sync_calls);
        let store = TrayPreferencesStore::with_dir_sync(&app_data, move |dir| {
            calls_counter.fetch_add(1, Ordering::SeqCst);
            if fail_flag.load(Ordering::SeqCst) {
                return Err(std::io::Error::other(format!(
                    "simulated fsync failure on {}",
                    dir.display()
                )));
            }
            sync_parent_directory(dir)
        });

        let initial = store.load_or_default();
        assert_eq!(initial.close_behavior, CloseBehavior::HideToTray);
        let state = TrayState::new(
            store.clone(),
            initial,
            true,
            Some("Tray host notice.".to_string()),
            None,
            None,
        );

        let err = state
            .update_close_behavior(CloseBehavior::Quit)
            .expect_err("post-rename directory sync failure must not report false durable success");
        assert_eq!(err.code, PREFERENCES_DURABILITY_UNCERTAIN_CODE);
        assert!(!err.message.contains(app_data.to_str().unwrap()));
        assert!(!err.message.contains("simulated fsync failure"));
        assert!(!err.message.to_ascii_lowercase().contains("rolled back"));
        assert!(!err.message.to_ascii_lowercase().contains("unchanged"));
        assert_eq!(
            sync_calls.load(Ordering::SeqCst),
            1,
            "must not automatically retry failed directory sync"
        );

        // The renamed file on disk retains the new value with private 0600 permissions.
        let reopened = TrayPreferencesStore::new(&app_data).load_or_default();
        assert_eq!(reopened.close_behavior, CloseBehavior::Quit);
        assert_eq!(reopened.warning, None);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = fs::symlink_metadata(store.preferences_path())
                .expect("prefs metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(file_mode, 0o600);
        }

        // In-memory state and close decision reconcile with the renamed on-disk file and expose the warning.
        let reconciled = state.preferences_response();
        assert_eq!(reconciled.close_behavior, CloseBehavior::Quit);
        assert_eq!(state.main_close_decision(), MainCloseDecision::QuitApp);
        let status_msg = reconciled
            .message
            .expect("uncertainty warning must be exposed in preferences response");
        assert!(status_msg.contains(&err.message));
        assert!(status_msg.contains("Tray host notice."));

        // Subsequent durable save succeeds, clears the persistence warning, and keeps tray init notice.
        fail_dir_sync.store(false, Ordering::SeqCst);
        let recovered = state
            .update_close_behavior(CloseBehavior::HideToTray)
            .expect("save succeeds once directory sync recovers");
        assert_eq!(recovered.close_behavior, CloseBehavior::HideToTray);
        assert_eq!(
            recovered.message.as_deref(),
            Some("Tray host notice."),
            "persistence uncertainty warning should clear after durable save"
        );
        assert_eq!(sync_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn fallback_and_explicit_quit_decisions_prevent_unreachable_hidden_app() {
        // 1. HideToTray when tray is available hides main window.
        assert_eq!(
            resolve_main_close_decision(CloseBehavior::HideToTray, true, false),
            MainCloseDecision::HideMainToTray
        );

        // 2. HideToTray when tray is unavailable falls back to QuitApp rather than hiding an unreachable app.
        assert_eq!(
            resolve_main_close_decision(CloseBehavior::HideToTray, false, false),
            MainCloseDecision::QuitApp
        );

        // 3. Quit preference always quits even when tray is available.
        assert_eq!(
            resolve_main_close_decision(CloseBehavior::Quit, true, false),
            MainCloseDecision::QuitApp
        );

        // 4. Explicit Quit overrides HideToTray even when tray is available and popover exists.
        assert_eq!(
            resolve_main_close_decision(CloseBehavior::HideToTray, true, true),
            MainCloseDecision::QuitApp
        );
        assert_eq!(
            resolve_popover_close_decision(false),
            PopoverDismissDecision::HidePopoverOnly
        );
        assert_eq!(
            resolve_popover_close_decision(true),
            PopoverDismissDecision::AllowCloseForQuit
        );

        // 5. Popover blur hides only popover after grace window, ignores transient open blur, and allows quit.
        assert_eq!(
            resolve_popover_blur_decision(false, Some(Duration::from_millis(20))),
            PopoverDismissDecision::IgnoreTransientBlur
        );
        assert_eq!(
            resolve_popover_blur_decision(false, Some(Duration::from_millis(350))),
            PopoverDismissDecision::HidePopoverOnly
        );
        assert_eq!(
            resolve_popover_blur_decision(true, Some(Duration::from_millis(10))),
            PopoverDismissDecision::AllowCloseForQuit
        );

        // 6. Tray host disappearance while main is hidden triggers RestoreHiddenMain.
        assert_eq!(
            resolve_tray_availability_recovery(false, false, false),
            TrayAvailabilityRecoveryDecision::RestoreHiddenMain
        );
        assert_eq!(
            resolve_tray_availability_recovery(true, false, false),
            TrayAvailabilityRecoveryDecision::NoAction
        );
        assert_eq!(
            resolve_tray_availability_recovery(false, true, false),
            TrayAvailabilityRecoveryDecision::NoAction
        );
        assert_eq!(
            resolve_tray_availability_recovery(false, false, true),
            TrayAvailabilityRecoveryDecision::NoAction
        );
    }

    #[test]
    fn tray_click_filter_and_surface_authority_guards_behave_deterministically() {
        assert!(!should_toggle_popover_on_tray_click(
            MouseButton::Right,
            MouseButtonState::Up,
            false,
            None
        ));
        assert!(!should_toggle_popover_on_tray_click(
            MouseButton::Left,
            MouseButtonState::Down,
            false,
            None
        ));
        assert!(should_toggle_popover_on_tray_click(
            MouseButton::Left,
            MouseButtonState::Up,
            false,
            None
        ));
        assert!(should_toggle_popover_on_tray_click(
            MouseButton::Left,
            MouseButtonState::Down,
            true,
            None
        ));
        assert!(!should_toggle_popover_on_tray_click(
            MouseButton::Left,
            MouseButtonState::Up,
            true,
            Some(Duration::from_millis(20))
        ));

        assert!(ensure_main_window_label("main").is_ok());
        let tray_err = ensure_main_window_label("tray").expect_err("tray must be rejected");
        assert_eq!(tray_err.code, "forbidden_surface");
    }

    #[test]
    fn popover_positioning_handles_top_bar_bottom_taskbar_hidpi_and_linux_zero_fallback() {
        let monitor = MonitorBoundsPx {
            x: 0,
            y: 0,
            width: 1920,
            height: 1080,
            scale_factor: 1.0,
        };

        // Top-right tray icon -> popover placed below icon and clamped within right monitor margin.
        let top_rect = TrayRectPx {
            x: 1880.0,
            y: 4.0,
            width: 24.0,
            height: 24.0,
        };
        let (x_top, y_top) = compute_popover_position_px(
            monitor,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
            None,
            Some(top_rect),
            false,
        );
        assert_eq!(x_top, 1920 - 360 - 12);
        assert_eq!(y_top, 28 + 6);

        // Bottom-right taskbar tray icon -> popover placed above taskbar icon.
        let bottom_rect = TrayRectPx {
            x: 1850.0,
            y: 1040.0,
            width: 24.0,
            height: 24.0,
        };
        let (x_bot, y_bot) = compute_popover_position_px(
            monitor,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
            None,
            Some(bottom_rect),
            true,
        );
        assert_eq!(x_bot, 1920 - 360 - 12);
        assert_eq!(y_bot, 1040 - (TRAY_POPOVER_HEIGHT as i32) - 6);

        // Linux host giving (0, 0) click position and 0x0 rect -> safe top-right fallback inside monitor.
        let zero_rect = TrayRectPx {
            x: 0.0,
            y: 0.0,
            width: 0.0,
            height: 0.0,
        };
        let (x_fb, y_fb) = compute_popover_position_px(
            monitor,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
            Some((0.0, 0.0)),
            Some(zero_rect),
            false,
        );
        assert_eq!(x_fb, 1920 - 360 - 12);
        assert!(y_fb >= 12 && y_fb + (TRAY_POPOVER_HEIGHT as i32) <= 1080 - 12);
    }

    #[test]
    fn mixed_scale_monitors_resolve_logical_and_physical_anchors_with_target_monitor_scale() {
        // Primary monitor: 2x Retina/HiDPI (physical 0..3840 x 0..2160, logical 0..1920 x 0..1080).
        // Secondary monitor on right: 1x external (physical 3840..5760 x 0..1080, logical 1920..3840 x 0..1080).
        let monitors = [
            MonitorBoundsPx {
                x: 0,
                y: 0,
                width: 3840,
                height: 2160,
                scale_factor: 2.0,
            },
            MonitorBoundsPx {
                x: 3840,
                y: 0,
                width: 1920,
                height: 1080,
                scale_factor: 1.0,
            },
        ];

        assert_eq!(
            select_monitor_for_point(&monitors, Some((4200.0, 12.0))),
            monitors[1]
        );
        assert_eq!(
            select_monitor_for_point(&monitors, Some((0.0, 0.0))),
            monitors[0]
        );

        // Logical tray rect on secondary 1x monitor at logical (3760, 4) size 28x22:
        // Must resolve to `monitors[1]` (scale 1.0), NOT multiply by `monitors[0]`'s 2.0 scale!
        let secondary_logical_anchor = RawTrayAnchor {
            click_position: None,
            tray_rect: Some(RawTrayRect {
                position: RawCoordinate2D::Logical { x: 3760.0, y: 4.0 },
                size: RawSize2D::Logical {
                    width: 28.0,
                    height: 22.0,
                },
            }),
        };
        let resolved_second =
            resolve_tray_anchor_for_monitors(&monitors, Some(secondary_logical_anchor));
        assert!(resolved_second.has_valid_anchor);
        assert_eq!(resolved_second.monitor, monitors[1]);
        assert_eq!(
            resolved_second.tray_rect_px,
            Some(TrayRectPx {
                x: 3840.0 + (3760.0 - 1920.0),
                y: 4.0,
                width: 28.0,
                height: 22.0,
            })
        );

        let (logical_w, logical_h) =
            compute_popover_logical_size_for_monitor(resolved_second.monitor);
        assert_eq!((logical_w, logical_h), (360.0, 320.0));
        assert!(logical_h < MAIN_WINDOW_MIN_HEIGHT);

        // Reopen on primary 2x monitor with logical click (1880, 12) -> physical (3760, 24).
        let primary_logical_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Logical { x: 1880.0, y: 12.0 }),
            tray_rect: None,
        };
        let resolved_first =
            resolve_tray_anchor_for_monitors(&monitors, Some(primary_logical_click));
        assert!(resolved_first.has_valid_anchor);
        assert_eq!(resolved_first.monitor, monitors[0]);
        assert_eq!(resolved_first.click_position_px, Some((3760.0, 24.0)));

        // Missing coordinates (0, 0) never claim `has_valid_anchor`.
        let missing_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 0.0, y: 0.0 }),
            tray_rect: Some(RawTrayRect {
                position: RawCoordinate2D::Physical { x: 0.0, y: 0.0 },
                size: RawSize2D::Physical {
                    width: 0.0,
                    height: 0.0,
                },
            }),
        };
        let resolved_missing = resolve_tray_anchor_for_monitors(&monitors, Some(missing_anchor));
        assert!(!resolved_missing.has_valid_anchor);

        // Constrained display height (e.g. 300px logical) clamps cleanly below TRAY_POPOVER_HEIGHT and MAIN_WINDOW_MIN_HEIGHT.
        let small_size = compute_popover_logical_size_for_monitor(MonitorBoundsPx {
            x: 0,
            y: 0,
            width: 1600,
            height: 600,
            scale_factor: 2.0,
        });
        assert_eq!(small_size, (360.0, 300.0 - 24.0));
        assert!(small_size.1 < TRAY_POPOVER_HEIGHT);
        assert!(small_size.1 < MAIN_WINDOW_MIN_HEIGHT);
    }

    #[test]
    fn tray_click_toggle_handles_repeated_click_blur_bounce_suppression_debounce_and_menu_isolation(
    ) {
        let tmp = TempDir::new().expect("tempdir");
        let store = TrayPreferencesStore::new(tmp.path().join("app-data"));
        let initial = store.load_or_default();
        let state = TrayState::new(store, initial, true, None, None, None);

        let t0 = Instant::now();
        let anchor_a = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 1880.0, y: 14.0 }),
            tray_rect: None,
        };

        // 1. Initial left click opens popover and stores raw anchor.
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: false,
                raw_anchor: anchor_a,
                now: t0,
            }),
            TrayToggleDecision::OpenPopover
        );
        assert_eq!(
            state.anchor_for_activation(PopoverActivationSource::TrayIconClick),
            Some(anchor_a)
        );
        // Context-menu `Open status` never reuses stale click coordinates from `anchor_a`.
        assert_eq!(
            state.anchor_for_activation(PopoverActivationSource::MenuOpenStatus),
            None
        );

        // 2. Duplicate backend click within 90ms debounce is ignored.
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: true,
                raw_anchor: anchor_a,
                now: t0 + Duration::from_millis(25),
            }),
            TrayToggleDecision::IgnoreClick
        );

        // 3. Right click is ignored without overwriting `anchor_a`.
        let bogus_right_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 50.0, y: 50.0 }),
            tray_rect: None,
        };
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Right,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: true,
                raw_anchor: bogus_right_anchor,
                now: t0 + Duration::from_millis(200),
            }),
            TrayToggleDecision::IgnoreClick
        );
        assert_eq!(state.last_raw_anchor(), Some(anchor_a));

        // 4. Repeated left click while popover is visible hides it cleanly.
        let t_close = t0 + Duration::from_millis(400);
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: true,
                raw_anchor: anchor_a,
                now: t_close,
            }),
            TrayToggleDecision::HideVisiblePopover
        );

        // 5. If clicking the panel icon caused focus-out hide 40ms before the SNI Activate event
        //    arrived, `SuppressReopenAfterBlur` prevents immediately bouncing back open.
        let t_blur = t_close + Duration::from_millis(500);
        state.record_popover_blur_hidden_at(t_blur);
        let t_bounce = t_blur + Duration::from_millis(40);
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: false,
                raw_anchor: anchor_a,
                now: t_bounce,
            }),
            TrayToggleDecision::SuppressReopenAfterBlur
        );

        // 6. A deliberate subsequent click after debounce reopens cleanly.
        assert_eq!(
            state.evaluate_tray_click_toggle(TrayClickEvaluationInput {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                is_linux: true,
                popover_currently_visible: false,
                raw_anchor: anchor_a,
                now: t_bounce + Duration::from_millis(150),
            }),
            TrayToggleDecision::OpenPopover
        );
    }

    #[test]
    fn placement_notices_surface_truthfully_and_clear_when_icon_anchored_open_succeeds() {
        let tmp = TempDir::new().expect("tempdir");
        let store = TrayPreferencesStore::new(tmp.path().join("app-data"));
        let initial = store.load_or_default();
        let state = TrayState::new(store, initial, true, None, None, None);

        state.set_placement_notice(Some(
            tray_linux::WAYLAND_COMPOSITOR_PLACEMENT_NOTICE.to_string(),
        ));
        assert_eq!(
            state.preferences_response().message.as_deref(),
            Some(tray_linux::WAYLAND_COMPOSITOR_PLACEMENT_NOTICE)
        );

        let fallback_outcome = PopoverPlacementOutcome::FallbackMonitorCorner {
            x: 1548,
            y: 12,
            scale_factor: 1.0,
            reason: UnanchoredPlacementReason::MissingTrayCoordinates,
            notice: tray_linux::MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string(),
        };
        assert!(fallback_outcome.validate_invariants());
        let fallback_resp = state.record_popover_open_outcome(&fallback_outcome, None);
        assert_eq!(
            fallback_resp.message.as_deref(),
            Some(tray_linux::MISSING_TRAY_COORDINATES_FALLBACK_NOTICE)
        );

        let anchored_outcome = PopoverPlacementOutcome::IconAnchored {
            x: 1548,
            y: 30,
            scale_factor: 2.0,
        };
        assert!(anchored_outcome.validate_invariants());
        let anchored_resp = state.record_popover_open_outcome(&anchored_outcome, None);
        assert_eq!(
            anchored_resp.message, None,
            "icon-anchored open without capability limits must clear stale fallback notice"
        );

        // Wayland outside-click capability notices (missing RGBA visual or failed surface
        // creation) surface accurately without marking the system tray unavailable, and clear on
        // a subsequent full-capability open.
        let rgba_limited = state.record_popover_open_outcome(
            &anchored_outcome,
            Some(tray_linux::WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE.to_string()),
        );
        assert!(rgba_limited.tray_available);
        assert_eq!(
            rgba_limited.message.as_deref(),
            Some(tray_linux::WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE)
        );
        let prefs_rgba = state.preferences_response();
        assert!(prefs_rgba.tray_available);
        assert_eq!(
            prefs_rgba.message.as_deref(),
            Some(tray_linux::WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE)
        );

        let missing_coords = tray_linux::MISSING_TRAY_COORDINATES_FALLBACK_NOTICE;
        let surface_unavail = tray_linux::WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE;
        let combined_fallback_and_surface = tray_linux::combine_popover_notices(
            fallback_outcome.diagnostic_notice(),
            Some(surface_unavail),
        );
        let surface_limited =
            state.record_popover_open_outcome(&fallback_outcome, combined_fallback_and_surface);
        let surfaced_msg = surface_limited
            .message
            .expect("combined fallback + backdrop capability notice");
        assert!(surfaced_msg.contains(missing_coords));
        assert!(surfaced_msg.contains(surface_unavail));

        // Async runtime capability notices (e.g. post-open backdrop configure timeout) combine
        // cleanly with any active placement fallback without duplicating warnings, and clear on the
        // next full-capability open.
        let _ = state.record_popover_open_outcome(&fallback_outcome, None);
        let runtime_limited = state.record_runtime_capability_notice(surface_unavail);
        assert!(runtime_limited.tray_available);
        let runtime_msg = runtime_limited
            .message
            .expect("runtime capability notice combined with fallback");
        assert!(runtime_msg.contains(missing_coords));
        assert!(runtime_msg.contains(surface_unavail));
        let runtime_deduped = state.record_runtime_capability_notice(surface_unavail);
        assert_eq!(
            runtime_deduped.message.as_deref(),
            Some(runtime_msg.as_str())
        );

        let recovered = state.record_popover_open_outcome(&anchored_outcome, None);
        assert_eq!(recovered.message, None);
        assert_eq!(state.preferences_response().message, None);
    }

    #[test]
    fn single_instance_restores_compatible_second_launch_and_rejects_conflicting_arguments() {
        let tmp = TempDir::new().expect("tempdir");
        let app_data = tmp.path().join("app-data");
        let registry_a = tmp.path().join("registry-a");
        let registry_b = tmp.path().join("registry-b");

        let fp_primary = LaunchFingerprint::new(
            Some(tmp.path().join("state-a")),
            None,
            registry_a.clone(),
            None,
        );

        let restore_count = Arc::new(AtomicUsize::new(0));
        let restore_clone = Arc::clone(&restore_count);

        let primary =
            SingleInstanceGuard::acquire_or_notify(&app_data, fp_primary.clone(), move || {
                restore_clone.fetch_add(1, Ordering::SeqCst);
            })
            .expect("acquire primary instance");
        let SingleInstanceAcquisition::Primary(guard) = primary else {
            panic!("expected primary acquisition");
        };

        // Compatible second launch restores existing instance instead of starting a second one.
        let second = SingleInstanceGuard::acquire_or_notify(&app_data, fp_primary, || {
            panic!("second launch must not become primary");
        })
        .expect("second launch with identical fingerprint must succeed");
        assert!(matches!(
            second,
            SingleInstanceAcquisition::RestoredExisting
        ));
        assert_eq!(restore_count.load(Ordering::SeqCst), 1);

        // Conflicting second launch (--registry-dir differs) is rejected without triggering restore or retargeting.
        let fp_conflict =
            LaunchFingerprint::new(Some(tmp.path().join("state-a")), None, registry_b, None);
        let conflict_err =
            match SingleInstanceGuard::acquire_or_notify(&app_data, fp_conflict, || {}) {
                Ok(_) => panic!("conflicting second launch must be rejected"),
                Err(err) => err,
            };
        assert_eq!(conflict_err.code, "instance_argument_conflict");
        assert_eq!(restore_count.load(Ordering::SeqCst), 1);

        drop(guard);

        // After primary guard drops, a new instance can acquire primary ownership cleanly.
        let next_primary = SingleInstanceGuard::acquire_or_notify(
            &app_data,
            LaunchFingerprint::new(None, None, registry_a, None),
            || {},
        )
        .expect("reacquire after drop");
        assert!(matches!(
            next_primary,
            SingleInstanceAcquisition::Primary(_)
        ));
    }

    #[test]
    fn synthesized_monochrome_icon_has_transparent_background_and_black_alpha_glyph() {
        let (rgba, width, height) = synthesize_monochrome_tray_icon_rgba();
        assert_eq!(width, 32);
        assert_eq!(height, 32);
        assert_eq!(rgba.len(), (32 * 32 * 4) as usize);

        // Corner pixel (0, 0) must be fully transparent (no square background).
        assert_eq!(&rgba[0..4], &[0, 0, 0, 0]);

        let mut opaque_pixels = 0usize;
        for px in rgba.as_chunks::<4>().0 {
            assert_eq!(px[0], 0, "red channel must be monochrome zero");
            assert_eq!(px[1], 0, "green channel must be monochrome zero");
            assert_eq!(px[2], 0, "blue channel must be monochrome zero");
            if px[3] == 255 {
                opaque_pixels += 1;
            }
        }
        assert!(
            opaque_pixels > 40,
            "expected visible opaque node/stem pixels"
        );
    }

    #[test]
    fn linux_open_outcome_publication_preserves_newer_notice_and_fences_superseded_attempts() {
        let tmp = TempDir::new().expect("tempdir");
        let store = TrayPreferencesStore::new(tmp.path().join("app-data"));
        let initial = store.load_or_default();
        let state = TrayState::new(store, initial, true, None, None, None);

        let anchored_outcome = PopoverPlacementOutcome::IconAnchored {
            x: 1548,
            y: 30,
            scale_factor: 1.0,
        };

        // 1. Normal presented open updates placement notice and emits `threadlane:tray-opened`.
        let presented_outcome = tray_linux::LinuxOpenOutcome::Presented {
            outcome: anchored_outcome,
            effective_notice: Some(
                tray_linux::MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string(),
            ),
        };
        let presented = state.apply_linux_open_outcome(Ok(presented_outcome));
        assert_eq!(
            presented,
            LinuxOpenPublicationAction::EmitOpened(TrayPreferencesResponse {
                close_behavior: CloseBehavior::HideToTray,
                tray_available: true,
                message: Some(tray_linux::MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            })
        );

        // 2. Same-generation nested refresh publishes a newer capability notice before outer open
        //    unwinds: outer `SupersededSameGeneration` must NOT clear the newer notice with `None`.
        state.set_placement_notice(Some(
            tray_linux::WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string(),
        ));
        let ok_same_gen = Ok(tray_linux::LinuxOpenOutcome::SupersededSameGeneration);
        let same_gen = state.apply_linux_open_outcome(ok_same_gen);
        assert_eq!(
            same_gen,
            LinuxOpenPublicationAction::EmitOpened(TrayPreferencesResponse {
                close_behavior: CloseBehavior::HideToTray,
                tray_available: true,
                message: Some(
                    tray_linux::WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
            })
        );
        assert_eq!(
            state.preferences_response().message.as_deref(),
            Some(tray_linux::WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
        );

        // 3. Stale open superseded by a newer generation or cancelled by hide/quit emits nothing
        //    and leaves the newer owner's notice untouched.
        state.set_placement_notice(Some(
            tray_linux::WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE.to_string(),
        ));
        let ok_cancelled = Ok(tray_linux::LinuxOpenOutcome::SupersededOrCancelled);
        let cancelled = state.apply_linux_open_outcome(ok_cancelled);
        assert_eq!(cancelled, LinuxOpenPublicationAction::NoPublication);
        assert_eq!(
            state.preferences_response().message.as_deref(),
            Some(tray_linux::WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE)
        );

        // 4. Current unsuperseded error recovers `main` without indiscriminate global teardown.
        let current_err = state.apply_linux_open_outcome(Err("present failed".to_string()));
        assert_eq!(
            current_err,
            LinuxOpenPublicationAction::RecoverMainOnCurrentFailure
        );
    }
}
