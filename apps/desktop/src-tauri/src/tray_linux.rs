//! Linux-specific system tray popup sizing, monitor/scale resolution, Wayland `zwlr_layer_shell_v1`
//! anchoring, and display-backend placement handling (`StatusNotifierItem` / GTK3 / Wayland & X11).
//!
//! ### Architectural invariants
//! 1. **Per-monitor scale fidelity**: Logical icon geometry and `StatusNotifierItem::Activate(x, y)`
//!    coordinates are matched against each monitor's own geometry, panel-reserved workarea, and
//!    `scale_factor` — never converted upfront using `monitors.first()`.
//! 2. **Native Wayland layer-shell anchoring (`gtk-layer-shell`)**: On Wayland compositors that
//!    advertise `zwlr_layer_shell_v1` (such as Sway, Hyprland, and other wlroots compositors), the
//!    hidden `tray` `GtkApplicationWindow` initializes its layer-shell role before realization,
//!    binds to the resolved target `gdk::Monitor`, sets `exclusive_zone = 0` (never reserving panel
//!    struts), and anchors two perpendicular edges with clamped margins near the tray activation
//!    point without stretching fullscreen or calling bogus `gtk_window_move`.
//! 3. **Truthful limited capabilities & fallbacks**:
//!    - On Wayland compositors without `zwlr_layer_shell_v1` (such as GNOME / Mutter), or if the
//!      toolkit window was already mapped/realized and could not assume the layer role, placement
//!      is reported honestly as [`PopoverPlacementOutcome::CompositorManaged`] without fake
//!      `set_position` calls.
//!    - When `StatusNotifierItem` coordinates are genuinely absent (e.g. `(0, 0)` sentinel or
//!      context-menu `Open status`), placement uses a safe monitor corner without blindly guessing
//!      `monitors[0]` when an active monitor hint is available, and explicitly reports
//!      [`PopoverPlacementOutcome::FallbackMonitorCorner`] rather than claiming icon anchoring.
//! 4. **Pointer-follow-focus & native Wayland outside-click dismissal**: On Wayland layer-shell
//!    (`protocol_version >= 4`), the popup maps on `Layer::Overlay` strictly above bounded
//!    per-monitor (`<= MAX_WAYLAND_OUTSIDE_CLICK_MONITORS`) owned GTK transparent RGBA
//!    outside-click surfaces on `Layer::Top` (`KeyboardMode::None`, `exclusive_zone = 0`). Each
//!    backdrop is realized with an explicit empty `cairo::Region` (`&empty_region` with `0`
//!    rectangles; never left unshaped/null, which defaults to the full surface input region)
//!    before its first map, registers main-thread ownership before mapping, and enables its
//!    bounded `(0, 0, width, height)` input region only after validated compositor configuration,
//!    popup visibility, and generation checks succeed (with a finite bounded configure timeout).
//!    Explicit `(generation, attempt_ticket)` pending-map and controlled-remap admission waits for
//!    real native GTK `visible + mapped + layer` confirmation after Tao's asynchronous
//!    `WindowRequest::Visible(true)` dispatch (up to a finite deadline) and preserves admission
//!    across live `LayerShell::set_monitor` unmap/remap transitions and same-generation nested
//!    refreshes without blanket map resurrection. Inside-popup clicks reach the `Layer::Overlay`
//!    popup directly, pointer motion anywhere never dismisses, and the first genuine outside
//!    button press releases the pressed event's exact master-pointer implicit grab
//!    (`gdk_device_ungrab`) before disabling input regions, hiding, or closing backdrops,
//!    revalidates the exact `(surface_id, generation, admission_seq, attempt_ticket)` token across
//!    both ungrab and fail-open teardown before hiding the popup, and consumes only that dismissal
//!    click. Meanwhile `Exclusive -> OnDemand` keyboard focus and `WaylandLayerFocusTracker`
//!    preserve immediate `Escape` dismissal and protect hover transit from the tray icon under
//!    `focus_follows_mouse yes`.

#[cfg(any(target_os = "linux", test))]
use std::cell::RefCell;
#[cfg(any(target_os = "linux", test))]
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::cell::Cell;
#[cfg(target_os = "linux")]
use std::rc::Rc;
#[cfg(target_os = "linux")]
use std::sync::{Mutex, OnceLock};

#[cfg(target_os = "linux")]
use gtk::glib;
#[cfg(target_os = "linux")]
use gtk::prelude::*;
#[cfg(target_os = "linux")]
use gtk_layer_shell::{Edge, KeyboardMode, Layer, LayerShell};
#[cfg(target_os = "linux")]
use tauri::{AppHandle, Emitter, LogicalSize, Manager, WebviewWindow};

#[cfg(target_os = "linux")]
use super::{
    ensure_main_reachable_if_tray_unavailable, hide_popover_window,
    hide_popover_window_if_linux_epoch_current, TrayState, MAIN_WINDOW_LABEL,
    TRAY_PREFERENCES_CHANGED_EVENT, TRAY_WINDOW_LABEL,
};

#[cfg(any(target_os = "linux", test))]
use super::{
    MonitorBoundsPx, PopoverPlacementOutcome, RawCoordinate2D, RawSize2D, RawTrayAnchor,
    RawTrayRect, UnanchoredPlacementReason, TRAY_POPOVER_HEIGHT, TRAY_POPOVER_MIN_HEIGHT,
    TRAY_POPOVER_MIN_WIDTH, TRAY_POPOVER_WIDTH,
};

/// Truthful diagnostic surfaced when running on a Wayland compositor (such as GNOME / Mutter) that
/// does not advertise `zwlr_layer_shell_v1`, leaving standard `xdg_toplevel` placement to the compositor.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_COMPOSITOR_PLACEMENT_NOTICE: &str = "Wayland compositor does not advertise zwlr_layer_shell_v1; standard client windows cannot self-anchor to the tray icon, so the compositor controls status popup placement.";

/// Truthful diagnostic surfaced on Wayland when `zwlr_layer_shell_v1` is unavailable and the tray
/// host also omitted activation coordinates.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_MISSING_COORDS_NOTICE: &str = "Wayland tray host omitted icon coordinates and this compositor does not advertise zwlr_layer_shell_v1; the compositor controls status popup placement.";

/// Truthful diagnostic surfaced if the GTK window was already realized and could not transition to
/// a `zwlr_layer_shell_v1` surface before first map.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_REALIZED_FALLBACK_NOTICE: &str = "Wayland tray window was already realized before layer-shell initialization could run; falling back to compositor-managed xdg_toplevel placement.";

/// Truthful diagnostic surfaced when `zwlr_layer_shell_v1` is below protocol version 4, meaning
/// `KeyboardMode::OnDemand` is unavailable so external-window outside clicks cannot unfocus the
/// exclusive layer surface (`Escape`, repeated tray clicks, and clicks on Threadlane's main window
/// still dismiss).
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE: &str = "Wayland compositor supports zwlr_layer_shell_v1 below version 4 (no OnDemand keyboard mode); Escape, repeated tray clicks, and Threadlane main-window clicks dismiss the popup, while clicks on external windows may not unfocus it.";

/// Truthful diagnostic surfaced when the display server honors popup anchoring/placement, but the
/// tray host omitted icon coordinates (e.g. `(0, 0)` or context-menu open) so monitor corner
/// fallback ran instead of anchoring to the tray icon.
#[cfg(any(target_os = "linux", test))]
pub const MISSING_TRAY_COORDINATES_FALLBACK_NOTICE: &str = "Tray host did not provide icon coordinates; status popup used fallback monitor placement instead of anchoring to the tray icon.";

/// Truthful diagnostic surfaced when the active GDK display backend cannot position top-level
/// popup windows.
#[cfg(any(target_os = "linux", test))]
pub const UNKNOWN_LINUX_BACKEND_PLACEMENT_NOTICE: &str = "Linux display backend does not support client-driven tray popup positioning; window placement is managed by the window system.";

/// Truthful diagnostic surfaced when Wayland `zwlr_layer_shell_v1` v4+ is active, but the GDK
/// screen does not expose an RGBA visual so transparent outside-click surfaces cannot be mapped
/// without obscuring the desktop (`Escape`, repeated tray clicks, Threadlane main-window clicks,
/// and clicks on focusable external windows still dismiss).
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE: &str = "Display screen does not provide a transparent RGBA visual for Wayland outside-click surfaces; empty-workspace clicks may not dismiss the popup, while Escape, repeated tray clicks, and window clicks still work.";

/// Truthful diagnostic surfaced when Wayland `zwlr_layer_shell_v1` v4+ is active, but creating or
/// mapping a native layer-shell outside-click input surface failed (`Escape`, repeated tray clicks,
/// Threadlane main-window clicks, and clicks on focusable external windows still dismiss).
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE: &str = "Unable to initialize native Wayland outside-click input surfaces; empty-workspace clicks may not dismiss the popup, while Escape, repeated tray clicks, and window clicks still work.";

/// Window role and Wayland layer-shell namespace identifier applied to the compact Linux tray
/// status window so compositors and window managers distinguish it from `main`.
#[cfg(any(target_os = "linux", test))]
pub const LINUX_TRAY_WINDOW_ROLE: &str = "threadlane-tray-popover";

/// Window role and Wayland layer-shell namespace identifier applied to transient per-monitor
/// transparent outside-click input surfaces mapped strictly while the Wayland tray popup is open.
#[cfg(any(target_os = "linux", test))]
pub const LINUX_TRAY_BACKDROP_WINDOW_ROLE: &str = "threadlane-tray-outside-click";

/// Hard upper bound on simultaneously mapped per-monitor Wayland outside-click input surfaces so
/// pathological monitor counts can never allocate unbounded GTK top-level windows.
#[cfg(any(target_os = "linux", test))]
pub const MAX_WAYLAND_OUTSIDE_CLICK_MONITORS: usize = 16;

/// Finite bounded deadline for mapped Wayland outside-click surfaces to receive a valid compositor
/// configuration before failing open and releasing every surface for that admission.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_BACKDROP_CONFIGURE_TIMEOUT: Duration = Duration::from_millis(500);

/// Finite bounded deadline waiting for the native GTK/Wayland `tray` popup to become visible,
/// mapped, and layer-backed after Tao's asynchronous `WindowRequest::Visible(true)` dispatch or a
/// live `LayerShell::set_monitor` remap.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_POPUP_PENDING_MAP_TIMEOUT: Duration = Duration::from_millis(1000);

/// Placeholder pre-configure default dimension (`1x1`) set on childless GTK backdrop windows prior
/// to realization so GTK's `200x200` fallback size never masquerades as a compositor configure.
#[cfg(any(target_os = "linux", test))]
pub const BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM: i32 = 1;

#[cfg(any(target_os = "linux", test))]
const BACKDROP_MAX_UNCONFIGURED_DIM: i32 = 4;

#[cfg(any(target_os = "linux", test))]
const BACKDROP_MIN_VALID_CONFIGURED_DIM: i32 = 32;

#[cfg(any(target_os = "linux", test))]
const BACKDROP_ROUNDING_TOLERANCE_PX: i32 = 2;

/// Delay before transitioning a newly mapped Wayland layer-shell popover from
/// `KeyboardMode::Exclusive` to `KeyboardMode::OnDemand` so the surface acquires initial keyboard
/// focus (`Escape`) and survives initial hover motion off the panel.
#[cfg(target_os = "linux")]
const WAYLAND_ON_DEMAND_TRANSITION_DELAY: Duration = Duration::from_millis(250);

/// Verification window after a Wayland layer-shell focus-loss notification before hiding, ensuring
/// pointer movement from an external panel tray icon into the popover under `focus_follows_mouse yes`
/// never dismisses the popover when the pointer enters it.
#[cfg(any(target_os = "linux", test))]
pub const WAYLAND_FOCUS_LOSS_VERIFY_DELAY: Duration = Duration::from_millis(160);

/// Active Linux GDK display server protocol backing the desktop window.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinuxDisplayBackend {
    /// Native Wayland display (`GdkWaylandDisplay`).
    Wayland,
    /// X11 or XWayland display (`GdkX11Display`) honoring root-coordinate moves.
    X11,
    /// Unrecognized or non-positioning GDK backend (e.g. Broadway).
    Unknown,
}

/// Classifies a GTK3 `GdkDisplay` GObject type name into [`LinuxDisplayBackend`].
#[cfg(any(target_os = "linux", test))]
pub fn classify_gdk_display_type_name(type_name: &str) -> LinuxDisplayBackend {
    if type_name.contains("Wayland") {
        LinuxDisplayBackend::Wayland
    } else if type_name.contains("X11") {
        LinuxDisplayBackend::X11
    } else {
        LinuxDisplayBackend::Unknown
    }
}

/// Specific reason why `zwlr_layer_shell_v1` could not be used on a Wayland display.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandLayerShellUnsupportedReason {
    /// Compositor does not advertise `zwlr_layer_shell_v1` (e.g. GNOME / Mutter).
    CompositorProtocolUnsupported,
    /// Underlying `GtkWindow` was already realized/mapped and could not become a layer surface.
    WindowAlreadyRealized,
}

/// Availability and protocol version of `zwlr_layer_shell_v1` for the `tray` popover window.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandLayerShellSupport {
    Supported { protocol_version: u32 },
    Unsupported(WaylandLayerShellUnsupportedReason),
}

/// Keyboard interactivity strategy selected for a Wayland layer-shell popover.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandKeyboardPolicy {
    /// Protocol >= 4: acquire immediate focus via `Exclusive`, then switch to `OnDemand` so
    /// outside clicks on workspace/windows release focus cleanly while `Escape` works from open.
    ExclusiveThenOnDemand,
    /// Protocol 1..=3: `OnDemand` does not exist in protocol; keep `Exclusive` and surface an
    /// explicit limited capability notice for external-window outside clicks.
    ExclusiveLimitedExternalOutsideClick,
}

/// Relative `zwlr_layer_shell_v1` stacking layer used to order the interactive tray popup strictly
/// above transient outside-click input surfaces without relying on same-layer map order.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WaylandShellLayer {
    /// `zwlr_layer_shell_v1` `Layer::Top` (layer 2): used for per-monitor transparent outside-click
    /// input surfaces on protocol `>= 4`, and for legacy protocol `< 4` popovers where outside-click
    /// backdrops are not mapped.
    Top,
    /// `zwlr_layer_shell_v1` `Layer::Overlay` (layer 3): strictly above `Layer::Top` so inside-popup
    /// clicks and pointer motion always route to the popup surface first.
    Overlay,
}

#[cfg(target_os = "linux")]
impl WaylandShellLayer {
    fn to_gtk_layer(self) -> Layer {
        match self {
            Self::Top => Layer::Top,
            Self::Overlay => Layer::Overlay,
        }
    }
}

/// Explicit Wayland `zwlr_layer_shell_v1` layer, anchor, margin, monitor, and exclusive-zone
/// parameters.
///
/// Always anchors exactly one vertical edge (`Top` xor `Bottom`) and one horizontal edge (`Left`
/// xor `Right`) with `exclusive_zone == 0` so the popup never reserves workspace struts and never
/// stretches across an output axis.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaylandLayerShellPlacement {
    pub monitor_index: Option<usize>,
    pub layer: WaylandShellLayer,
    pub anchor_top: bool,
    pub anchor_bottom: bool,
    pub anchor_left: bool,
    pub anchor_right: bool,
    pub margin_top: i32,
    pub margin_bottom: i32,
    pub margin_left: i32,
    pub margin_right: i32,
    pub exclusive_zone: i32,
    pub keyboard_policy: WaylandKeyboardPolicy,
    /// Estimated GDK logical rectangle `(x, y, width, height)` occupied by the compact popup on
    /// its target monitor.
    pub estimated_gdk_bounds: (i32, i32, i32, i32),
}

#[cfg(any(target_os = "linux", test))]
impl WaylandLayerShellPlacement {
    /// Verifies that the layer-shell configuration never stretches fullscreen (no opposite edge
    /// pairs anchored simultaneously) and never reserves panel exclusive zones.
    pub fn is_compact_non_exclusive(&self) -> bool {
        let single_vertical = self.anchor_top ^ self.anchor_bottom;
        let single_horizontal = self.anchor_left ^ self.anchor_right;
        let (_, _, w, h) = self.estimated_gdk_bounds;
        single_vertical && single_horizontal && self.exclusive_zone == 0 && w > 0 && h > 0
    }

    /// Verifies that the popup's Wayland layer is strictly above `backdrop`'s layer rather than
    /// relying on same-layer map order.
    pub fn orders_above_backdrop(&self, backdrop: &WaylandOutsideClickBackdropSpec) -> bool {
        self.layer > backdrop.layer
    }

    /// Returns `true` when global GDK logical coordinate `(gx, gy)` lies within the popup's
    /// estimated bounds.
    pub fn contains_gdk_point(&self, gx: i32, gy: i32) -> bool {
        let (bx, by, bw, bh) = self.estimated_gdk_bounds;
        bw > 0
            && bh > 0
            && gx >= bx
            && gx < bx.saturating_add(bw)
            && gy >= by
            && gy < by.saturating_add(bh)
    }
}

/// Specification for a single bounded, owned GTK transparent transient outside-click input surface
/// bound to one actual `gdk::Monitor` while a Wayland `zwlr_layer_shell_v1` v4+ popup is open.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WaylandOutsideClickBackdropSpec {
    pub monitor_index: usize,
    pub geometry_x: i32,
    pub geometry_y: i32,
    pub width: i32,
    pub height: i32,
    pub scale_factor: f64,
    pub layer: WaylandShellLayer,
    pub exclusive_zone: i32,
    pub keyboard_interactivity_none: bool,
    pub transparent_rgba: bool,
}

#[cfg(any(target_os = "linux", test))]
impl WaylandOutsideClickBackdropSpec {
    /// Builds a `Layer::Top`, `KeyboardMode::None`, `exclusive_zone = 0`, transparent RGBA
    /// backdrop specification for an enumerated `gdk::Monitor` within [`MAX_WAYLAND_OUTSIDE_CLICK_MONITORS`].
    pub fn for_monitor(monitor: &LinuxMonitorWorkarea) -> Option<Self> {
        let monitor_index = monitor.monitor_index?;
        if monitor_index >= MAX_WAYLAND_OUTSIDE_CLICK_MONITORS {
            return None;
        }
        Some(Self {
            monitor_index,
            geometry_x: monitor.geometry_x,
            geometry_y: monitor.geometry_y,
            width: monitor.geometry_width.max(1),
            height: monitor.geometry_height.max(1),
            scale_factor: sanitize_scale(monitor.scale_factor),
            layer: WaylandShellLayer::Top,
            exclusive_zone: 0,
            keyboard_interactivity_none: true,
            transparent_rgba: true,
        })
    }

    /// Surface-local logical input bounds `(0, 0, width, height)` for this monitor's layer
    /// surface. Always anchored at `(0, 0)` in surface-local space even when the monitor's global
    /// compositor origin `(geometry_x, geometry_y)` is negative.
    pub fn surface_local_input_bounds(&self) -> (i32, i32, i32, i32) {
        (0, 0, self.width.max(1), self.height.max(1))
    }

    /// Returns `true` when global GDK logical coordinate `(gx, gy)` lies within this monitor's
    /// logical geometry.
    pub fn contains_gdk_point(&self, gx: i32, gy: i32) -> bool {
        self.width > 0
            && self.height > 0
            && gx >= self.geometry_x
            && gx < self.geometry_x.saturating_add(self.width)
            && gy >= self.geometry_y
            && gy < self.geometry_y.saturating_add(self.height)
    }

    /// Validates all safety invariants required before mapping a native outside-click surface.
    pub fn validate_invariants(&self) -> bool {
        let (lx, ly, lw, lh) = self.surface_local_input_bounds();
        self.monitor_index < MAX_WAYLAND_OUTSIDE_CLICK_MONITORS
            && self.width > 0
            && self.height > 0
            && self.scale_factor.is_finite()
            && self.scale_factor > 0.0
            && self.layer == WaylandShellLayer::Top
            && self.exclusive_zone == 0
            && self.keyboard_interactivity_none
            && self.transparent_rgba
            && lx == 0
            && ly == 0
            && lw == self.width
            && lh == self.height
    }

    /// Classifies a GTK `size-allocate` `(alloc_width, alloc_height)` against this monitor's
    /// logical bounds so pre-configure `1x1` placeholders remain empty while valid compositor
    /// workspace configurations clamp cleanly to `(0, 0, width, height)`.
    pub fn classify_allocation(
        &self,
        alloc_width: i32,
        alloc_height: i32,
    ) -> BackdropAllocationCheck {
        if !self.validate_invariants() || alloc_width <= 0 || alloc_height <= 0 {
            return BackdropAllocationCheck::InvalidGeometry;
        }
        if (self.width > BACKDROP_MAX_UNCONFIGURED_DIM
            || self.height > BACKDROP_MAX_UNCONFIGURED_DIM)
            && alloc_width <= BACKDROP_MAX_UNCONFIGURED_DIM
            && alloc_height <= BACKDROP_MAX_UNCONFIGURED_DIM
        {
            return BackdropAllocationCheck::PendingUnconfigured;
        }
        let min_w = self.width.clamp(1, BACKDROP_MIN_VALID_CONFIGURED_DIM);
        let min_h = self.height.clamp(1, BACKDROP_MIN_VALID_CONFIGURED_DIM);
        let max_w = self.width.saturating_add(BACKDROP_ROUNDING_TOLERANCE_PX);
        let max_h = self.height.saturating_add(BACKDROP_ROUNDING_TOLERANCE_PX);
        if alloc_width < min_w
            || alloc_height < min_h
            || alloc_width > max_w
            || alloc_height > max_h
        {
            return BackdropAllocationCheck::InvalidGeometry;
        }
        BackdropAllocationCheck::ValidBounded {
            width: alloc_width.min(self.width).max(1),
            height: alloc_height.min(self.height).max(1),
        }
    }
}

/// Lifecycle phase of an owned per-monitor Wayland outside-click surface.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropLifecyclePhase {
    /// Realized with an explicit empty `cairo::Region` (`&empty_region` with `0` rectangles; never
    /// an unshaped/null full-surface region) prior to its first map.
    CreatedEmpty,
    /// Mapped on `Layer::Top` while retaining its explicit empty `cairo::Region`, awaiting a
    /// validated compositor configuration.
    MappedEmpty,
    /// Validated compositor configuration, popup visibility, generation, and main-thread ownership
    /// were confirmed and the bounded `(0, 0, width, height)` input region is active.
    ActiveBounded { width: i32, height: i32 },
}

#[cfg(any(target_os = "linux", test))]
impl BackdropLifecyclePhase {
    pub fn is_active_bounded(self) -> bool {
        matches!(self, Self::ActiveBounded { .. })
    }
}

/// Result of validating a GTK surface allocation against [`WaylandOutsideClickBackdropSpec`].
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropAllocationCheck {
    /// Pre-configure GTK placeholder size (`1..=4` px) before `zwlr_layer_surface_v1.configure`
    /// arrives; keep the explicit empty input region active and await configure or timeout.
    PendingUnconfigured,
    /// Validated surface-local `(0, 0, width, height)` bounds within the monitor's geometry.
    ValidBounded { width: i32, height: i32 },
    /// Non-positive, collapsed, or out-of-bounds geometry requiring immediate fail-open teardown.
    InvalidGeometry,
}

/// Result of synchronizing Wayland outside-click backdrops for an open generation.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropSyncOutcome {
    /// Popup was not visible or no Wayland v4+ monitor specs applied; zero surfaces mapped.
    NotApplicable,
    /// Admission was cancelled (e.g. reentrant hide/unmap/monitor-removal or newer open
    /// generation); all partial windows were left empty and destroyed with zero active ownership.
    CancelledFailOpen,
    /// All per-monitor surfaces were admitted; `enabled_count` already received valid compositor
    /// configurations and `pending_configure_count` remain empty awaiting configure within
    /// [`WAYLAND_BACKDROP_CONFIGURE_TIMEOUT`].
    Admitted {
        enabled_count: usize,
        pending_configure_count: usize,
    },
}

/// Specific reason why native Wayland outside-click input surfaces could not be mapped.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandBackdropUnavailableReason {
    /// Active GDK screen does not advertise a 32-bit RGBA visual; mapping an opaque window would
    /// obscure the workspace, so outside-click surfaces are skipped cleanly.
    TransparentVisualUnavailable,
    /// Native layer-shell initialization, monitor binding, GDK realization, or bounded compositor
    /// configuration failed; any partial surfaces were torn down fail-open.
    SurfaceCreationFailed,
}

#[cfg(any(target_os = "linux", test))]
impl WaylandBackdropUnavailableReason {
    pub fn capability_notice(self) -> &'static str {
        match self {
            Self::TransparentVisualUnavailable => WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE,
            Self::SurfaceCreationFailed => WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE,
        }
    }
}

/// Pointer input classification evaluated by [`drive_outside_click_backdrop_press`].
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropPointerEvent {
    /// Initial discrete mouse button press (`gdk::EventType::ButtonPress`).
    ButtonPress { button: u32 },
    /// Pointer motion or crossing (`MotionNotify`, `EnterNotify`, `LeaveNotify`) — must never
    /// dismiss the popup.
    PointerMotion,
    /// Button release or synthesized multi-click continuation (`ButtonRelease`, `2button-press`,
    /// `3button-press`) — must not trigger a new dismissal.
    NonInitialButtonEvent,
}

/// Exact coordinator and tracker token identifying the single live outside-click backdrop
/// admission authorized to ungrab and dismiss the current popup.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveBackdropDismissToken {
    pub surface_id: u64,
    pub generation: u64,
    pub admission_seq: u64,
    pub attempt_ticket: u64,
}

/// Role of the `gdk::Device` attached directly to a `gdk::EventButton` (`event.device()`),
/// without reading or promoting `source_device` / slave devices.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropDeviceRole {
    /// `gdk::DeviceType::Master` with a non-keyboard `gdk::InputSource`.
    MasterPointer,
    /// `gdk::DeviceType::Master` with `gdk::InputSource::Keyboard`.
    MasterKeyboard,
    /// `gdk::DeviceType::Slave` or `gdk::DeviceType::Floating`.
    SlaveOrFloating,
}

/// Candidate `gdk::Device` metadata extracted from `event.device()` for master-pointer validation.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackdropCandidateDevice<D, Dev> {
    pub handle: Dev,
    pub display: D,
    pub role: BackdropDeviceRole,
    pub seat_master_pointer: Option<Dev>,
}

/// Live identity of a tracked backdrop window sampled from its coordinator handle.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedBackdropWindowIdentity<T, W, D> {
    pub toplevel: T,
    pub native_window: W,
    pub display: D,
    pub is_live: bool,
}

/// Native GTK/GDK button-press metadata passed into [`drive_outside_click_backdrop_press`].
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackdropNativePressContext<T, W, D, Dev> {
    pub is_gtk_main_thread: bool,
    pub pointer_event: BackdropPointerEvent,
    pub event_time: u32,
    pub backend_display: D,
    pub callback_toplevel: Option<T>,
    pub event_window: Option<(W, D, bool)>,
    pub callback_window: Option<(W, D, bool)>,
    pub device: Option<BackdropCandidateDevice<D, Dev>>,
}

/// Outcome returned by [`drive_outside_click_backdrop_press`].
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropDismissOutcome {
    /// Motion, crossing, release, multi-click continuation, or `button == 0`: no ungrab or hide.
    IgnoredNonPress,
    /// Inactive, stale, unconfigured, or already-fenced surface/popup: stale handles cleaned up
    /// fail-open without ungrabbing or dismissing any current popup.
    RejectedStaleOrInactive,
    /// Off-main-thread, missing/destroyed/mismatched window or display, or missing/non-master
    /// pointer device: rejected before ungrab without altering active surfaces or popup.
    RejectedInvalidNativeTarget,
    /// Reentrant refresh, reopen, or hide/quit during `gdk_device_ungrab` invalidated the exact
    /// dismissal token before surface teardown: newer state preserved untouched.
    SupersededOrCancelledAfterUngrab,
    /// Reentrant refresh, reopen, or hide/quit during fail-open surface release invalidated the
    /// exact dismissal token before popup hide: newer popup and surfaces preserved untouched.
    SupersededOrCancelledDuringRelease,
    /// Master pointer ungrabbed first, backdrops released fail-open, and exact epoch popup hidden.
    DismissedPopover,
}

/// Legacy test-only action returned by [`drive_backdrop_pointer_event`].
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackdropInputDecision {
    /// Keep the popup and all active-generation backdrops open (e.g. pointer motion).
    IgnoreKeepOpen,
    /// Event arrived from a stale or already-closed open generation; release only stale handles
    /// without dismissing any newer open popup or consuming another click.
    ReleaseStaleGenerationOnly,
    /// First outside button press for the active generation: all backdrops have been released
    /// fail-open; caller must consume this single click and hide the popup while preserving the
    /// running app.
    ConsumeClickAndDismissPopover,
}

/// Target surface resolved by compositor layer ordering for a global GDK logical coordinate.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandPointerHitTarget {
    /// Hit the `Layer::Overlay` popup surface directly above any `Layer::Top` backdrop.
    PopupOverlay,
    /// Missed the popup and hit `monitor_index`'s `Layer::Top` outside-click backdrop at
    /// surface-local `(surface_local_x, surface_local_y)`.
    OutsideBackdrop {
        monitor_index: usize,
        surface_local_x: i32,
        surface_local_y: i32,
    },
    /// Outside any Wayland layer-shell backdrop (e.g. X11, GNOME `xdg_toplevel`, legacy `< v4`, or
    /// outside monitor bounds).
    Unhandled,
}

/// Main-thread backend abstraction for realizing with an explicit empty input region, mapping,
/// post-configure bounded region enabling, timeout scheduling, and fail-open teardown of native
/// GTK outside-click input surfaces.
#[cfg(any(target_os = "linux", test))]
pub trait OutsideClickSurfaceBackend {
    type SurfaceHandle: Clone;

    /// Returns `true` when the display screen provides a transparent RGBA visual so mapping
    /// outside-click surfaces will never paint an opaque rectangle.
    fn has_rgba_visual(&self) -> bool;

    /// Returns `true` when the Wayland tray popup is currently visible and mapped for
    /// `generation` (and explicit quit has not been requested).
    fn is_popup_visible_for_generation(&self, generation: u64) -> bool;

    /// Creates, configures (`Layer::Top`, `KeyboardMode::None`, `exclusive_zone = 0`, 4-edge
    /// monitor-local stretch, RGBA draw), and realizes one transient backdrop surface for `spec`
    /// with an **explicit empty `cairo::Region`** (`&empty_region` with `0` rectangles; never left
    /// unshaped/null, which defaults to the full surface input region) BEFORE its first map.
    fn create_realized_empty_surface(
        &self,
        surface_id: u64,
        generation: u64,
        spec: &WaylandOutsideClickBackdropSpec,
    ) -> Result<Self::SurfaceHandle, WaylandBackdropUnavailableReason>;

    /// Maps a realized `CreatedEmpty` backdrop surface onto `Layer::Top` while its input shape
    /// remains explicitly empty. May synchronously trigger GTK `size-allocate`, `hide`, `unmap`,
    /// or `monitor-removed` callbacks.
    fn map_empty_surface(
        &self,
        surface: &Self::SurfaceHandle,
    ) -> Result<(), WaylandBackdropUnavailableReason>;

    /// Applies the validated surface-local `(0, 0, width, height)` `cairo::Region` input shape
    /// after ownership, popup visibility, and active generation have been verified.
    fn enable_bounded_input_region(
        &self,
        surface: &Self::SurfaceHandle,
        width: i32,
        height: i32,
    ) -> Result<(), WaylandBackdropUnavailableReason>;

    /// Schedules a finite main-thread deadline check for `admission_seq` when any mapped surface
    /// has not yet received its compositor configuration.
    fn schedule_configure_timeout(&self, generation: u64, admission_seq: u64, timeout: Duration);

    /// Schedules a finite main-thread deadline check for `(generation, attempt_ticket)` while
    /// awaiting asynchronous native GTK popup map confirmation.
    fn schedule_pending_map_timeout(
        &self,
        _generation: u64,
        _attempt_ticket: u64,
        _timeout: Duration,
    ) {
    }

    /// Defers a single re-check onto GTK idle when `connect_map` fires before native
    /// `visible && mapped && is_layer_window` flags have all settled.
    fn schedule_pending_map_idle_probe(&self, _generation: u64, _attempt_ticket: u64) {}

    /// Releases `surface` fail-open: applies an explicit empty `cairo::Region` input shape first
    /// (`&empty_region` with `0` rectangles; never an unshaped/null full-surface region),
    /// disconnects signal handlers, hides, and closes the window.
    fn release_surface_fail_open(&self, surface: Self::SurfaceHandle);
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone)]
pub struct PendingPopupMapAdmission {
    pub generation: u64,
    pub attempt_ticket: u64,
    pub deadline: Instant,
    pub keyboard_policy: Option<WaylandKeyboardPolicy>,
    pub plan_valid: bool,
    pub specs: Vec<WaylandOutsideClickBackdropSpec>,
    pub base_notice: Option<String>,
    pub idle_probe_scheduled: bool,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone)]
struct ExpiredPopupMapAttempt {
    generation: u64,
    attempt_ticket: u64,
    keyboard_policy: Option<WaylandKeyboardPolicy>,
    specs_non_empty: bool,
    base_notice: Option<String>,
    notice_already_published: bool,
    idle_probe_scheduled: bool,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedPopupMapAttempt {
    pub generation: u64,
    pub attempt_ticket: u64,
    pub admitted: bool,
    pub capability_notice: Option<&'static str>,
    pub effective_notice: Option<String>,
    pub should_publish_notice: bool,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingSettledPopupRedraw {
    generation: u64,
    attempt_ticket: u64,
    idle_scheduled: bool,
}

#[cfg(any(target_os = "linux", test))]
struct TrackedBackdropSurface<H> {
    surface_id: u64,
    generation: u64,
    admission_seq: u64,
    spec: WaylandOutsideClickBackdropSpec,
    phase: BackdropLifecyclePhase,
    pending_configured_size: Option<(i32, i32)>,
    handle: H,
}

#[cfg(any(target_os = "linux", test))]
impl<H> TrackedBackdropSurface<H> {
    fn matches_admission(&self, surface_id: u64, admission_seq: u64) -> bool {
        self.surface_id == surface_id && self.admission_seq == admission_seq
    }

    fn matches_gen(&self, surface_id: u64, generation: u64) -> bool {
        self.surface_id == surface_id && self.generation == generation
    }
}

/// Pure main-thread coordinator state owning all active Wayland outside-click input surfaces and
/// any armed pending-popup-map operation.
///
/// Stored inside a `RefCell` that is **only** borrowed across short in-memory mutations and never
/// held across any GTK or [`OutsideClickSurfaceBackend`] call so reentrant `show`,
/// `size-allocate`, `hide`, `unmap`, or `monitor-removed` callbacks can never encounter a borrowed
/// `RefCell` or skip cleanup.
#[cfg(any(target_os = "linux", test))]
pub struct OutsideClickCoordinatorState<H> {
    active_generation: Option<u64>,
    active_admission_seq: Option<u64>,
    /// Monotonic ticket of the latest reserved open/refresh operation. Kept strictly separate from
    /// `active_admission_seq` so aborting a failed current attempt clears active surface ownership
    /// while still identifying that attempt as the current unsuperseded owner for failure notices.
    latest_attempt_ticket: u64,
    admission_in_progress: bool,
    next_attempt_ticket: u64,
    next_surface_id: u64,
    pending_map_admission: Option<PendingPopupMapAdmission>,
    expired_map_attempt: Option<ExpiredPopupMapAttempt>,
    last_finalized_attempt: Option<FinalizedPopupMapAttempt>,
    pending_settled_redraw: Option<PendingSettledPopupRedraw>,
    surfaces: Vec<TrackedBackdropSurface<H>>,
}

#[cfg(any(target_os = "linux", test))]
impl<H> Default for OutsideClickCoordinatorState<H> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(target_os = "linux", test))]
impl<H> OutsideClickCoordinatorState<H> {
    pub fn new() -> Self {
        Self {
            active_generation: None,
            active_admission_seq: None,
            latest_attempt_ticket: 0,
            admission_in_progress: false,
            next_attempt_ticket: 1,
            next_surface_id: 1,
            pending_map_admission: None,
            expired_map_attempt: None,
            last_finalized_attempt: None,
            pending_settled_redraw: None,
            surfaces: Vec::new(),
        }
    }

    pub fn active_generation(&self) -> Option<u64> {
        self.active_generation
    }

    #[cfg(test)]
    pub fn active_admission_seq(&self) -> Option<u64> {
        self.active_admission_seq
    }

    pub fn latest_attempt_ticket(&self) -> u64 {
        self.latest_attempt_ticket
    }

    pub fn pending_map_ticket(&self) -> Option<(u64, u64)> {
        self.pending_map_admission
            .as_ref()
            .map(|p| (p.generation, p.attempt_ticket))
    }

    pub fn active_pending_or_expired_map_ticket(&self) -> Option<(u64, u64)> {
        self.pending_map_ticket().or_else(|| {
            self.expired_map_attempt
                .as_ref()
                .filter(|e| e.attempt_ticket == self.latest_attempt_ticket)
                .map(|e| (e.generation, e.attempt_ticket))
        })
    }

    pub fn pending_settled_redraw_epoch(&self) -> Option<(u64, u64)> {
        self.pending_settled_redraw
            .map(|p| (p.generation, p.attempt_ticket))
    }

    #[cfg(test)]
    pub fn is_pending_settled_redraw_idle_scheduled(&self) -> bool {
        self.pending_settled_redraw
            .is_some_and(|p| p.idle_scheduled)
    }

    pub fn active_surface_count(&self) -> usize {
        self.surfaces.len()
    }

    pub fn enabled_surface_count(&self) -> usize {
        self.surfaces
            .iter()
            .filter(|s| s.phase.is_active_bounded())
            .count()
    }

    #[cfg(test)]
    pub fn contains_surface(&self, surface_id: u64) -> bool {
        self.surfaces.iter().any(|s| s.surface_id == surface_id)
    }

    #[cfg(test)]
    pub fn surface_phase(&self, surface_id: u64) -> Option<BackdropLifecyclePhase> {
        self.surfaces
            .iter()
            .find(|s| s.surface_id == surface_id)
            .map(|s| s.phase)
    }
}

/// Reserves a fresh monotonic `attempt_ticket` before pre-sync teardown, controlled remap, or
/// queued `popover.show()`, invalidating any older pending-map operation.
#[cfg(any(target_os = "linux", test))]
pub fn reserve_attempt_ticket<H>(state_cell: &RefCell<OutsideClickCoordinatorState<H>>) -> u64 {
    let mut state = state_cell.borrow_mut();
    let ticket = state.next_attempt_ticket;
    state.next_attempt_ticket = state.next_attempt_ticket.saturating_add(1);
    state.latest_attempt_ticket = ticket;
    state.pending_map_admission = None;
    state.expired_map_attempt = None;
    state.last_finalized_attempt = None;
    state.pending_settled_redraw = None;
    ticket
}

/// Returns `true` when a newer open generation or newer `attempt_ticket` has superseded
/// `(generation, attempt_ticket)` in [`OutsideClickCoordinatorState`].
#[cfg(any(target_os = "linux", test))]
pub fn is_attempt_superseded_in_coordinator<H>(
    state_cell: &RefCell<OutsideClickCoordinatorState<H>>,
    generation: u64,
    attempt_ticket: u64,
) -> bool {
    let state = state_cell.borrow();
    state.latest_attempt_ticket > attempt_ticket
        || state
            .active_generation
            .is_some_and(|active| active > generation)
}

/// Drains all tracked outside-click surfaces and cancels any pending-map admission in a leaf
/// `RefCell` borrow, invoking `release_fn` on each handle strictly after dropping the borrow.
///
/// Returns `latest_attempt_ticket` captured inside the drain borrow so callers can scope
/// post-teardown assertions against reentrant newer owners.
#[cfg(any(target_os = "linux", test))]
pub fn drive_release_all_with<H, F>(
    state_cell: &RefCell<OutsideClickCoordinatorState<H>>,
    mut release_fn: F,
) -> u64
where
    F: FnMut(H),
{
    let (drained_epoch_ticket, drained) = {
        let mut state = state_cell.borrow_mut();
        state.active_generation = None;
        state.active_admission_seq = None;
        state.admission_in_progress = false;
        state.pending_map_admission = None;
        state.expired_map_attempt = None;
        state.last_finalized_attempt = None;
        state.pending_settled_redraw = None;
        let surfaces = std::mem::take(&mut state.surfaces);
        (state.latest_attempt_ticket, surfaces)
    };
    for tracked in drained {
        release_fn(tracked.handle);
    }
    drained_epoch_ticket
}

/// Fail-open releases every tracked outside-click surface across all monitors and clears active
/// generation ownership without holding a `RefCell` borrow across backend teardown.
#[cfg(test)]
pub fn drive_release_all<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
) {
    let _ = drive_release_all_with(state_cell, |handle| {
        backend.release_surface_fail_open(handle);
    });
}

/// Releases tracked surfaces fail-open up to `(generation, max_attempt_ticket)` and rechecks
/// coordinator epoch **after** backend teardown (which may reenter and start a newer attempt).
///
/// Returns `true` only when `(generation, max_attempt_ticket)` remains unsuperseded both before and
/// after releasing handles.
#[cfg(any(target_os = "linux", test))]
pub fn drive_release_up_to_attempt<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    max_attempt_ticket: u64,
) -> bool {
    let drained = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > max_attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            None
        } else {
            state.active_generation = None;
            state.active_admission_seq = None;
            state.admission_in_progress = false;
            if state
                .pending_map_admission
                .as_ref()
                .is_some_and(|p| p.attempt_ticket <= max_attempt_ticket)
            {
                state.pending_map_admission = None;
            }
            if state
                .expired_map_attempt
                .as_ref()
                .is_some_and(|e| e.attempt_ticket <= max_attempt_ticket)
            {
                state.expired_map_attempt = None;
            }
            if state
                .last_finalized_attempt
                .as_ref()
                .is_some_and(|f| f.attempt_ticket <= max_attempt_ticket)
            {
                state.last_finalized_attempt = None;
            }
            if state
                .pending_settled_redraw
                .is_some_and(|p| p.attempt_ticket <= max_attempt_ticket)
            {
                state.pending_settled_redraw = None;
            }
            Some(std::mem::take(&mut state.surfaces))
        }
    };
    let Some(surfaces) = drained else {
        return false;
    };
    for tracked in surfaces {
        backend.release_surface_fail_open(tracked.handle);
    }
    !is_attempt_superseded_in_coordinator(state_cell, generation, max_attempt_ticket)
}

#[cfg(any(target_os = "linux", test))]
fn drive_abort_admission<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    admission_seq: u64,
) {
    let to_release = {
        let mut state = state_cell.borrow_mut();
        if state.active_admission_seq == Some(admission_seq) {
            state.active_generation = None;
            state.active_admission_seq = None;
            state.admission_in_progress = false;
        }
        let mut release_handles = Vec::new();
        let mut keep = Vec::with_capacity(state.surfaces.len());
        for tracked in state.surfaces.drain(..) {
            if tracked.admission_seq == admission_seq {
                release_handles.push(tracked.handle);
            } else {
                keep.push(tracked);
            }
        }
        state.surfaces = keep;
        release_handles
    };
    for handle in to_release {
        backend.release_surface_fail_open(handle);
    }
}

/// Synchronizes per-monitor Wayland outside-click surfaces for a pre-reserved
/// `(generation, attempt_ticket)` epoch without holding a `RefCell` borrow across any GTK/backend
/// call.
///
/// 1. Releases all previous surfaces fail-open and rechecks that `(generation, attempt_ticket)`
///    was not superseded during reentrant teardown.
/// 2. Realizes each new surface with an explicit empty `cairo::Region` (`CreatedEmpty`) BEFORE
///    mapping, registers ownership in `state_cell`, and maps it while empty (`MappedEmpty`).
/// 3. If cancellation, hide, monitor removal, a newer same-generation attempt, a newer open
///    generation, or a sibling monitor failure occurs during admission, aborts only this
///    `attempt_ticket` fail-open while preserving any newer admission.
/// 4. Validates any configured allocations received during map, enables bounded input regions
///    after ownership and popup-visibility verification, and schedules a finite configure timeout
///    when any surface is still awaiting compositor configuration.
#[cfg(any(target_os = "linux", test))]
pub fn drive_sync_for_attempt<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    popup_visible: bool,
    specs: &[WaylandOutsideClickBackdropSpec],
) -> Result<BackdropSyncOutcome, WaylandBackdropUnavailableReason> {
    if is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket) {
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    }
    if !popup_visible || specs.is_empty() {
        if !drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket) {
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }
        return Ok(BackdropSyncOutcome::NotApplicable);
    }

    let this_admission_seq = attempt_ticket;
    let previous_handles = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }
        state.latest_attempt_ticket = state.latest_attempt_ticket.max(attempt_ticket);
        state.next_attempt_ticket = state
            .next_attempt_ticket
            .max(attempt_ticket.saturating_add(1));
        if state
            .pending_map_admission
            .as_ref()
            .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
        {
            state.pending_map_admission = None;
        }
        state.active_generation = Some(generation);
        state.active_admission_seq = Some(this_admission_seq);
        state.admission_in_progress = true;
        state
            .surfaces
            .drain(..)
            .map(|t| t.handle)
            .collect::<Vec<_>>()
    };
    for prev_handle in previous_handles {
        backend.release_surface_fail_open(prev_handle);
    }

    if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq)
        || !backend.is_popup_visible_for_generation(generation)
    {
        drive_abort_admission(state_cell, backend, this_admission_seq);
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    }

    if !backend.has_rgba_visual() {
        let reason = WaylandBackdropUnavailableReason::TransparentVisualUnavailable;
        drive_abort_admission(state_cell, backend, this_admission_seq);
        if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq) {
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }
        return Err(reason);
    }

    let bounded_specs = &specs[..specs.len().min(MAX_WAYLAND_OUTSIDE_CLICK_MONITORS)];
    for spec in bounded_specs {
        if !spec.validate_invariants() {
            let reason = WaylandBackdropUnavailableReason::SurfaceCreationFailed;
            drive_abort_admission(state_cell, backend, this_admission_seq);
            if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq) {
                return Ok(BackdropSyncOutcome::CancelledFailOpen);
            }
            return Err(reason);
        }
    }

    for spec in bounded_specs {
        if !backend.is_popup_visible_for_generation(generation) {
            drive_abort_admission(state_cell, backend, this_admission_seq);
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }

        let surface_id = {
            let mut state = state_cell.borrow_mut();
            if state.active_admission_seq != Some(this_admission_seq)
                || state.active_generation != Some(generation)
                || state.latest_attempt_ticket > this_admission_seq
            {
                None
            } else {
                let id = state.next_surface_id;
                state.next_surface_id = state.next_surface_id.saturating_add(1);
                Some(id)
            }
        };
        let Some(surface_id) = surface_id else {
            drive_abort_admission(state_cell, backend, this_admission_seq);
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        };

        let handle = match backend.create_realized_empty_surface(surface_id, generation, spec) {
            Ok(h) => h,
            Err(reason) => {
                drive_abort_admission(state_cell, backend, this_admission_seq);
                if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq)
                {
                    return Ok(BackdropSyncOutcome::CancelledFailOpen);
                }
                return Err(reason);
            }
        };

        let popup_still_visible = backend.is_popup_visible_for_generation(generation);
        let registered = {
            let mut state = state_cell.borrow_mut();
            if popup_still_visible
                && state.active_admission_seq == Some(this_admission_seq)
                && state.active_generation == Some(generation)
                && state.latest_attempt_ticket == this_admission_seq
            {
                state.surfaces.push(TrackedBackdropSurface {
                    surface_id,
                    generation,
                    admission_seq: this_admission_seq,
                    spec: *spec,
                    phase: BackdropLifecyclePhase::CreatedEmpty,
                    pending_configured_size: None,
                    handle: handle.clone(),
                });
                true
            } else {
                false
            }
        };
        if !registered {
            backend.release_surface_fail_open(handle);
            drive_abort_admission(state_cell, backend, this_admission_seq);
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }

        if let Err(reason) = backend.map_empty_surface(&handle) {
            backend.release_surface_fail_open(handle);
            drive_abort_admission(state_cell, backend, this_admission_seq);
            if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq) {
                return Ok(BackdropSyncOutcome::CancelledFailOpen);
            }
            return Err(reason);
        }

        let popup_visible_after_map = backend.is_popup_visible_for_generation(generation);
        let (still_admitted, invalid_sync_geometry) = {
            let mut state = state_cell.borrow_mut();
            if !popup_visible_after_map
                || state.active_admission_seq != Some(this_admission_seq)
                || state.active_generation != Some(generation)
                || state.latest_attempt_ticket > this_admission_seq
            {
                (false, false)
            } else if let Some(tracked) = state
                .surfaces
                .iter_mut()
                .find(|s| s.matches_admission(surface_id, this_admission_seq))
            {
                tracked.phase = BackdropLifecyclePhase::MappedEmpty;
                let invalid = tracked.pending_configured_size.is_some_and(|(w, h)| {
                    tracked.spec.classify_allocation(w, h)
                        == BackdropAllocationCheck::InvalidGeometry
                });
                (true, invalid)
            } else {
                (false, false)
            }
        };
        if !still_admitted {
            backend.release_surface_fail_open(handle);
            drive_abort_admission(state_cell, backend, this_admission_seq);
            return Ok(BackdropSyncOutcome::CancelledFailOpen);
        }
        if invalid_sync_geometry {
            let reason = WaylandBackdropUnavailableReason::SurfaceCreationFailed;
            drive_abort_admission(state_cell, backend, this_admission_seq);
            if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq) {
                return Ok(BackdropSyncOutcome::CancelledFailOpen);
            }
            return Err(reason);
        }
    }

    if !backend.is_popup_visible_for_generation(generation) {
        drive_abort_admission(state_cell, backend, this_admission_seq);
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    }

    let pending_configs: Option<Vec<(u64, i32, i32)>> = {
        let mut state = state_cell.borrow_mut();
        if state.active_admission_seq != Some(this_admission_seq)
            || state.active_generation != Some(generation)
            || state.latest_attempt_ticket > this_admission_seq
        {
            None
        } else {
            state.admission_in_progress = false;
            let mut configs = Vec::new();
            for tracked in &mut state.surfaces {
                if let Some((w, h)) = tracked.pending_configured_size.take() {
                    configs.push((tracked.surface_id, w, h));
                }
            }
            Some(configs)
        }
    };
    let Some(configs) = pending_configs else {
        drive_abort_admission(state_cell, backend, this_admission_seq);
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    };

    for (surface_id, raw_w, raw_h) in configs {
        let cfg_res = drive_surface_configured_with_ticket(
            state_cell, backend, surface_id, generation, raw_w, raw_h,
        )
        .map_err(|(_, reason)| reason);
        if let Err(reason) = cfg_res {
            if is_attempt_superseded_in_coordinator(state_cell, generation, this_admission_seq) {
                return Ok(BackdropSyncOutcome::CancelledFailOpen);
            }
            return Err(reason);
        }
    }

    let counts = {
        let state = state_cell.borrow();
        if state.active_admission_seq != Some(this_admission_seq)
            || state.active_generation != Some(generation)
            || state.latest_attempt_ticket > this_admission_seq
        {
            None
        } else {
            let enabled = state.enabled_surface_count();
            let total = state.surfaces.len();
            Some((enabled, total.saturating_sub(enabled)))
        }
    };
    let Some((enabled_count, pending_configure_count)) = counts else {
        drive_abort_admission(state_cell, backend, this_admission_seq);
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    };

    if pending_configure_count > 0 {
        backend.schedule_configure_timeout(
            generation,
            this_admission_seq,
            WAYLAND_BACKDROP_CONFIGURE_TIMEOUT,
        );
    }

    Ok(BackdropSyncOutcome::Admitted {
        enabled_count,
        pending_configure_count,
    })
}

/// Synchronizes per-monitor Wayland outside-click surfaces for `generation` by reserving a fresh
/// monotonic `attempt_ticket` and delegating to [`drive_sync_for_attempt`].
#[cfg(test)]
pub fn drive_sync_for_open<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    popup_visible: bool,
    specs: &[WaylandOutsideClickBackdropSpec],
) -> Result<BackdropSyncOutcome, WaylandBackdropUnavailableReason> {
    if state_cell
        .borrow()
        .active_generation
        .is_some_and(|active| active > generation)
    {
        return Ok(BackdropSyncOutcome::CancelledFailOpen);
    }
    let attempt_ticket = reserve_attempt_ticket(state_cell);
    drive_sync_for_attempt(
        state_cell,
        backend,
        generation,
        attempt_ticket,
        popup_visible,
        specs,
    )
}

#[cfg(any(target_os = "linux", test))]
pub fn drive_surface_configured_with_ticket<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    surface_id: u64,
    generation: u64,
    raw_width: i32,
    raw_height: i32,
) -> Result<(), (u64, WaylandBackdropUnavailableReason)> {
    enum ConfigureStep<H> {
        Ignore,
        RecordedDuringAdmission,
        Evaluate {
            admission_seq: u64,
            phase: BackdropLifecyclePhase,
            check: BackdropAllocationCheck,
            handle: H,
        },
    }

    let step = {
        let mut state = state_cell.borrow_mut();
        let admission_in_progress = state.admission_in_progress;
        let active_admission_seq = state.active_admission_seq;
        if state.active_generation != Some(generation) {
            ConfigureStep::Ignore
        } else if let Some(tracked) = state.surfaces.iter_mut().find(|s| {
            s.matches_gen(surface_id, generation) && Some(s.admission_seq) == active_admission_seq
        }) {
            let check = tracked.spec.classify_allocation(raw_width, raw_height);
            if admission_in_progress {
                if check != BackdropAllocationCheck::PendingUnconfigured {
                    tracked.pending_configured_size = Some((raw_width, raw_height));
                }
                ConfigureStep::RecordedDuringAdmission
            } else {
                ConfigureStep::Evaluate {
                    admission_seq: tracked.admission_seq,
                    phase: tracked.phase,
                    check,
                    handle: tracked.handle.clone(),
                }
            }
        } else {
            ConfigureStep::Ignore
        }
    };

    let (admission_seq, phase, check, handle) = match step {
        ConfigureStep::Ignore | ConfigureStep::RecordedDuringAdmission => return Ok(()),
        ConfigureStep::Evaluate {
            admission_seq,
            phase,
            check,
            handle,
        } => (admission_seq, phase, check, handle),
    };

    if !backend.is_popup_visible_for_generation(generation) {
        drive_abort_admission(state_cell, backend, admission_seq);
        return Ok(());
    }

    match check {
        BackdropAllocationCheck::PendingUnconfigured => {
            if matches!(phase, BackdropLifecyclePhase::ActiveBounded { .. }) {
                let reason = WaylandBackdropUnavailableReason::SurfaceCreationFailed;
                drive_abort_admission(state_cell, backend, admission_seq);
                return Err((admission_seq, reason));
            }
            Ok(())
        }
        BackdropAllocationCheck::InvalidGeometry => {
            let reason = WaylandBackdropUnavailableReason::SurfaceCreationFailed;
            drive_abort_admission(state_cell, backend, admission_seq);
            Err((admission_seq, reason))
        }
        BackdropAllocationCheck::ValidBounded { width, height } => {
            if phase == (BackdropLifecyclePhase::ActiveBounded { width, height }) {
                return Ok(());
            }

            let still_owned_before_enable = {
                let state = state_cell.borrow();
                state.active_generation == Some(generation)
                    && state.active_admission_seq == Some(admission_seq)
                    && state
                        .surfaces
                        .iter()
                        .any(|s| s.matches_admission(surface_id, admission_seq))
            };
            if !still_owned_before_enable {
                backend.release_surface_fail_open(handle);
                return Ok(());
            }

            if let Err(reason) = backend.enable_bounded_input_region(&handle, width, height) {
                backend.release_surface_fail_open(handle);
                drive_abort_admission(state_cell, backend, admission_seq);
                return Err((admission_seq, reason));
            }

            let popup_still_visible = backend.is_popup_visible_for_generation(generation);
            let marked_active = {
                let mut state = state_cell.borrow_mut();
                if popup_still_visible
                    && state.active_generation == Some(generation)
                    && state.active_admission_seq == Some(admission_seq)
                {
                    if let Some(tracked) = state
                        .surfaces
                        .iter_mut()
                        .find(|s| s.matches_admission(surface_id, admission_seq))
                    {
                        tracked.phase = BackdropLifecyclePhase::ActiveBounded { width, height };
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if !marked_active {
                backend.release_surface_fail_open(handle);
                drive_abort_admission(state_cell, backend, admission_seq);
            }
            Ok(())
        }
    }
}

/// Validates a compositor allocation for `surface_id` and enables its bounded `(0, 0, width,
/// height)` input region only after confirming active generation, popup visibility, and registered
/// main-thread ownership.
#[cfg(test)]
pub fn drive_surface_configured<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    surface_id: u64,
    generation: u64,
    raw_width: i32,
    raw_height: i32,
) -> Result<(), WaylandBackdropUnavailableReason> {
    drive_surface_configured_with_ticket(
        state_cell, backend, surface_id, generation, raw_width, raw_height,
    )
    .map_err(|(_, reason)| reason)
}

/// Enforces the finite configure deadline for `admission_seq`: if the admission is still current
/// and any mapped surface has not reached [`BackdropLifecyclePhase::ActiveBounded`], tears down
/// every surface for that admission fail-open and reports
/// [`WaylandBackdropUnavailableReason::SurfaceCreationFailed`].
#[cfg(any(target_os = "linux", test))]
pub fn drive_configure_timeout<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    admission_seq: u64,
) -> Result<bool, WaylandBackdropUnavailableReason> {
    let status = {
        let state = state_cell.borrow();
        if state.active_generation != Some(generation)
            || state.active_admission_seq != Some(admission_seq)
        {
            None
        } else {
            let all_enabled = !state.admission_in_progress
                && !state.surfaces.is_empty()
                && state.surfaces.iter().all(|s| s.phase.is_active_bounded());
            Some(all_enabled)
        }
    };
    let Some(all_enabled) = status else {
        return Ok(false);
    };

    if !backend.is_popup_visible_for_generation(generation) {
        drive_abort_admission(state_cell, backend, admission_seq);
        return Ok(true);
    }
    if !all_enabled {
        let reason = WaylandBackdropUnavailableReason::SurfaceCreationFailed;
        drive_abort_admission(state_cell, backend, admission_seq);
        return Err(reason);
    }
    Ok(false)
}

/// Evaluates an input event delivered to a backdrop created for `surface_generation`.
///
/// - Stale or post-close generations release only non-current handles without dismissing any newer
///   open popup.
/// - Pointer motion and non-initial button events never dismiss.
/// - The first genuine `ButtonPress` (`button >= 1`) for the active generation releases all
///   backdrops fail-open **before** returning [`BackdropInputDecision::ConsumeClickAndDismissPopover`].
#[cfg(test)]
pub fn drive_backdrop_pointer_event<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    surface_generation: u64,
    event: BackdropPointerEvent,
) -> BackdropInputDecision {
    let stale_to_release: Option<Vec<B::SurfaceHandle>> = {
        let mut state = state_cell.borrow_mut();
        if state.active_generation != Some(surface_generation) {
            let active_gen = state.active_generation;
            let mut retained = Vec::with_capacity(state.surfaces.len());
            let mut stale = Vec::new();
            for tracked in state.surfaces.drain(..) {
                if Some(tracked.generation) == active_gen {
                    retained.push(tracked);
                } else {
                    stale.push(tracked.handle);
                }
            }
            state.surfaces = retained;
            Some(stale)
        } else {
            None
        }
    };
    if let Some(stale_handles) = stale_to_release {
        for handle in stale_handles {
            backend.release_surface_fail_open(handle);
        }
        return BackdropInputDecision::ReleaseStaleGenerationOnly;
    }

    match event {
        BackdropPointerEvent::ButtonPress { button } if button >= 1 => {
            drive_release_all(state_cell, backend);
            BackdropInputDecision::ConsumeClickAndDismissPopover
        }
        BackdropPointerEvent::ButtonPress { .. }
        | BackdropPointerEvent::PointerMotion
        | BackdropPointerEvent::NonInitialButtonEvent => BackdropInputDecision::IgnoreKeepOpen,
    }
}

#[cfg(any(target_os = "linux", test))]
fn ends_with_sentence_terminator(text: &str) -> bool {
    text.ends_with('.') || text.ends_with('!') || text.ends_with('?')
}

#[cfg(any(target_os = "linux", test))]
fn contains_composed_notice_segment(existing: &str, candidate: &str) -> bool {
    if existing == candidate {
        return true;
    }
    for (start, _) in existing.match_indices(candidate) {
        let end = start + candidate.len();
        let valid_start = start == 0
            || existing[..start]
                .strip_suffix(' ')
                .is_some_and(ends_with_sentence_terminator);
        let valid_end = end == existing.len()
            || (ends_with_sentence_terminator(candidate) && existing[end..].starts_with(' '));
        if valid_start && valid_end {
            return true;
        }
    }
    false
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn combine_popover_notices(
    primary: Option<&str>,
    capability: Option<&str>,
) -> Option<String> {
    match (
        primary.map(str::trim).filter(|s| !s.is_empty()),
        capability.map(str::trim).filter(|s| !s.is_empty()),
    ) {
        (Some(p), Some(c)) if contains_composed_notice_segment(p, c) => Some(p.to_string()),
        (Some(p), Some(c)) => Some(format!("{p} {c}")),
        (Some(p), None) => Some(p.to_string()),
        (None, Some(c)) => Some(c.to_string()),
        (None, None) => None,
    }
}

/// GDK logical monitor geometry, panel-reserved workarea, and scale factor on Linux.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LinuxMonitorWorkarea {
    pub monitor_index: Option<usize>,
    pub geometry_x: i32,
    pub geometry_y: i32,
    pub geometry_width: i32,
    pub geometry_height: i32,
    pub workarea_x: i32,
    pub workarea_y: i32,
    pub workarea_width: i32,
    pub workarea_height: i32,
    pub scale_factor: f64,
    pub is_primary: bool,
}

#[cfg(any(target_os = "linux", test))]
impl Default for LinuxMonitorWorkarea {
    fn default() -> Self {
        Self {
            monitor_index: None,
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 1280,
            geometry_height: 800,
            workarea_x: 0,
            workarea_y: 0,
            workarea_width: 1280,
            workarea_height: 800,
            scale_factor: 1.0,
            is_primary: true,
        }
    }
}

#[cfg(any(target_os = "linux", test))]
impl LinuxMonitorWorkarea {
    /// Converts a physical [`MonitorBoundsPx`] reported by Tauri/`tao` on Linux into GDK logical
    /// geometry when direct `gdk::Monitor` enumeration is unavailable.
    pub fn from_monitor_bounds_px(
        bounds: MonitorBoundsPx,
        monitor_index: Option<usize>,
        is_primary: bool,
    ) -> Self {
        let scale = sanitize_scale(bounds.scale_factor);
        let geom_x = (bounds.x as f64 / scale).round() as i32;
        let geom_y = (bounds.y as f64 / scale).round() as i32;
        let geom_w = ((bounds.width as f64 / scale).round() as i32).max(1);
        let geom_h = ((bounds.height as f64 / scale).round() as i32).max(1);
        Self {
            monitor_index,
            geometry_x: geom_x,
            geometry_y: geom_y,
            geometry_width: geom_w,
            geometry_height: geom_h,
            workarea_x: geom_x,
            workarea_y: geom_y,
            workarea_width: geom_w,
            workarea_height: geom_h,
            scale_factor: scale,
            is_primary,
        }
    }
}

/// Complete Linux popover sizing and placement plan computed for a single open operation.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq)]
pub struct LinuxPopoverPlan {
    pub backend: LinuxDisplayBackend,
    pub target_monitor: LinuxMonitorWorkarea,
    /// Compact native popover dimensions `(width, height)` in GDK logical pixels.
    pub logical_size: (i32, i32),
    /// GDK logical `(x, y)` root coordinates to apply via `gtk_win.move_`, populated **only**
    /// when `backend == LinuxDisplayBackend::X11`. Always `None` on Wayland.
    pub gdk_position: Option<(i32, i32)>,
    /// Native Wayland `zwlr_layer_shell_v1` anchor/margin plan when supported.
    pub layer_shell: Option<WaylandLayerShellPlacement>,
    /// Bounded per-monitor transparent transient outside-click input surfaces to map strictly while
    /// a Wayland `zwlr_layer_shell_v1` v4+ popup is open. Empty on legacy `< v4`, `CompositorManaged`,
    /// `X11`, and `Unknown` backends.
    pub outside_click_backdrops: Vec<WaylandOutsideClickBackdropSpec>,
    /// Optional capability limitation notice (e.g. legacy `zwlr_layer_shell_v1` < v4).
    pub capability_notice: Option<String>,
    pub outcome: PopoverPlacementOutcome,
}

#[cfg(any(target_os = "linux", test))]
impl LinuxPopoverPlan {
    /// Returns the human-readable diagnostic or limited-capability notice to expose for this open,
    /// or `None` when full icon-anchored placement and input capabilities are active.
    pub fn effective_notice(&self) -> Option<String> {
        combine_popover_notices(
            self.outcome.diagnostic_notice(),
            self.capability_notice.as_deref(),
        )
    }

    /// Resolves which Wayland surface receives a pointer event at global GDK logical coordinate
    /// `(gx, gy)`, enforcing strict relative layer ordering (`Layer::Overlay` > `Layer::Top`)
    /// rather than same-layer map order.
    pub fn route_gdk_point(&self, gx: i32, gy: i32) -> WaylandPointerHitTarget {
        let Some(ls) = &self.layer_shell else {
            return WaylandPointerHitTarget::Unhandled;
        };
        let matching_backdrop = self
            .outside_click_backdrops
            .iter()
            .find(|b| b.contains_gdk_point(gx, gy));
        let above_any_backdrop = match matching_backdrop {
            None => true,
            Some(backdrop) => ls.orders_above_backdrop(backdrop),
        };
        if ls.contains_gdk_point(gx, gy) && above_any_backdrop {
            return WaylandPointerHitTarget::PopupOverlay;
        }
        if let Some(backdrop) = matching_backdrop {
            let local_x = gx.saturating_sub(backdrop.geometry_x);
            let local_y = gy.saturating_sub(backdrop.geometry_y);
            return WaylandPointerHitTarget::OutsideBackdrop {
                monitor_index: backdrop.monitor_index,
                surface_local_x: local_x,
                surface_local_y: local_y,
            };
        }
        WaylandPointerHitTarget::Unhandled
    }

    /// Verifies bounded per-monitor outside-click invariants and strict `Layer::Overlay` >
    /// `Layer::Top` ordering for the computed plan.
    pub fn validate_outside_click_plan(&self) -> bool {
        if self.outside_click_backdrops.len() > MAX_WAYLAND_OUTSIDE_CLICK_MONITORS {
            return false;
        }
        if self.outside_click_backdrops.is_empty() {
            return true;
        }
        if self.backend != LinuxDisplayBackend::Wayland {
            return false;
        }
        let Some(ls) = &self.layer_shell else {
            return false;
        };
        if !ls.is_compact_non_exclusive()
            || ls.keyboard_policy != WaylandKeyboardPolicy::ExclusiveThenOnDemand
        {
            return false;
        }
        let (bx, by, bw, bh) = ls.estimated_gdk_bounds;
        if self.route_gdk_point(bx.saturating_add(bw / 2), by.saturating_add(bh / 2))
            != WaylandPointerHitTarget::PopupOverlay
        {
            return false;
        }
        for backdrop in &self.outside_click_backdrops {
            if !backdrop.validate_invariants() || !ls.orders_above_backdrop(backdrop) {
                return false;
            }
            if let WaylandPointerHitTarget::OutsideBackdrop {
                monitor_index,
                surface_local_x,
                surface_local_y,
            } = self.route_gdk_point(backdrop.geometry_x, backdrop.geometry_y)
            {
                let (_, _, lw, lh) = backdrop.surface_local_input_bounds();
                if monitor_index != backdrop.monitor_index
                    || surface_local_x < 0
                    || surface_local_x >= lw
                    || surface_local_y < 0
                    || surface_local_y >= lh
                {
                    return false;
                }
            }
        }
        true
    }
}

/// Decision returned when evaluating a deferred Wayland layer-shell focus-loss check.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaylandFocusCheckDecision {
    /// Keep the popover visible (stale generation, unmapped, pointer entered popover, or window
    /// regained active keyboard focus).
    KeepOpen,
    /// Focus was genuinely lost outside the popover; dismiss and record blur timestamp.
    DismissPopover,
}

/// Pure generation-scoped tracker protecting Wayland layer-shell popovers from premature dismissal
/// when moving the pointer from an external StatusNotifier tray bar across a gap into the popup
/// under `focus_follows_mouse yes`, while fencing explicit controlled layer-shell remaps across
/// `LayerShell::set_monitor` transitions and still dismissing on genuine outside clicks.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Default)]
pub struct WaylandLayerFocusTracker {
    open_generation: u64,
    mapped: bool,
    pointer_inside: bool,
    latest_attempt_ticket: u64,
    controlled_remap_generation: Option<u64>,
    controlled_remap_ticket: Option<u64>,
    controlled_remap_depth: u32,
    cancelled_generation: Option<u64>,
}

#[cfg(any(target_os = "linux", test))]
impl WaylandLayerFocusTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances the open generation when the popover is about to be shown, invalidating any
    /// pending deferred focus-loss timers or controlled remaps from earlier opens.
    pub fn on_opening(&mut self) -> u64 {
        self.open_generation = self.open_generation.saturating_add(1);
        self.mapped = true;
        self.pointer_inside = false;
        self.controlled_remap_generation = None;
        self.controlled_remap_ticket = None;
        self.controlled_remap_depth = 0;
        self.cancelled_generation = None;
        self.open_generation
    }

    pub fn current_generation(&self) -> u64 {
        self.open_generation
    }

    pub fn latest_attempt_ticket(&self) -> u64 {
        self.latest_attempt_ticket
    }

    /// Returns `true` when the current generation is live-mapped or awaiting an armed pending
    /// layer-shell map/remap and has not been hidden, dismissed, or cancelled.
    pub fn has_live_or_pending_open(&self) -> bool {
        self.open_generation != 0
            && self.cancelled_generation != Some(self.open_generation)
            && (self.mapped
                || (self.controlled_remap_generation == Some(self.open_generation)
                    && self.controlled_remap_depth > 0))
    }

    /// Returns `true` when `(expected_generation, attempt_ticket)` has been superseded by a newer
    /// open generation or a newer reserved `attempt_ticket`.
    pub fn is_attempt_superseded(&self, expected_generation: u64, attempt_ticket: u64) -> bool {
        expected_generation == 0
            || self.open_generation > expected_generation
            || self.latest_attempt_ticket > attempt_ticket
    }

    /// Starts a fresh open attempt fenced by `attempt_ticket`, optionally arming asynchronous
    /// Wayland layer-shell map admission (`mapped = false` until native `visible + mapped + layer`
    /// is confirmed for `(generation, attempt_ticket)`).
    pub fn begin_open_attempt(&mut self, attempt_ticket: u64, arm_layer_map: bool) -> u64 {
        let generation = self.on_opening();
        self.latest_attempt_ticket = self.latest_attempt_ticket.max(attempt_ticket);
        if arm_layer_map {
            self.mapped = false;
            self.controlled_remap_generation = Some(generation);
            self.controlled_remap_ticket = Some(attempt_ticket);
            self.controlled_remap_depth = 1;
        }
        generation
    }

    /// Starts a same-generation scale/hotplug refresh attempt fenced by `attempt_ticket` when the
    /// current generation is live or awaiting an armed pending map.
    pub fn begin_refresh_attempt(
        &mut self,
        attempt_ticket: u64,
        explicit_quit_requested: bool,
        arm_layer_map: bool,
    ) -> Option<u64> {
        if explicit_quit_requested || !self.has_live_or_pending_open() {
            return None;
        }
        let generation = self.open_generation;
        self.latest_attempt_ticket = self.latest_attempt_ticket.max(attempt_ticket);
        if arm_layer_map {
            self.mapped = false;
            self.pointer_inside = false;
            self.controlled_remap_generation = Some(generation);
            self.controlled_remap_ticket = Some(attempt_ticket);
            self.controlled_remap_depth = 1;
        }
        Some(generation)
    }

    /// Aborts and fences `(expected_generation, attempt_ticket)` without disturbing any newer
    /// open generation or newer same-generation `attempt_ticket`.
    pub fn abort_attempt_remap(&mut self, expected_generation: u64, attempt_ticket: u64) {
        if self.is_attempt_superseded(expected_generation, attempt_ticket) {
            return;
        }
        if self.controlled_remap_generation == Some(expected_generation)
            && self.controlled_remap_ticket == Some(attempt_ticket)
        {
            self.controlled_remap_generation = None;
            self.controlled_remap_ticket = None;
            self.controlled_remap_depth = 0;
        }
        if self.open_generation == expected_generation {
            self.mapped = false;
            self.pointer_inside = false;
            self.cancelled_generation = Some(expected_generation);
        }
    }

    /// Disarms an expired pending-map token for `(expected_generation, attempt_ticket)` when its
    /// finite deadline elapses, preventing late map/refresh resurrection while leaving
    /// `cancelled_generation` unset so active-visible capability failure is reported truthfully.
    pub fn disarm_expired_attempt_remap(
        &mut self,
        expected_generation: u64,
        attempt_ticket: u64,
    ) -> bool {
        if self.is_attempt_superseded(expected_generation, attempt_ticket)
            || self.open_generation != expected_generation
        {
            return false;
        }
        if self.controlled_remap_generation == Some(expected_generation)
            && self.controlled_remap_ticket == Some(attempt_ticket)
        {
            self.controlled_remap_generation = None;
            self.controlled_remap_ticket = None;
            self.controlled_remap_depth = 0;
        }
        self.mapped = false;
        self.pointer_inside = false;
        true
    }

    /// Completes pending layer-shell map/remap admission for `(expected_generation, attempt_ticket)`
    /// once native `visible && mapped && is_layer_window` is confirmed. Never clobbers a newer
    /// open generation or newer same-generation `attempt_ticket`.
    pub fn complete_attempt_map(
        &mut self,
        expected_generation: u64,
        attempt_ticket: u64,
        window_visible_and_mapped: bool,
    ) -> bool {
        if self.is_attempt_superseded(expected_generation, attempt_ticket)
            || self.open_generation != expected_generation
        {
            return false;
        }
        let was_armed = self.controlled_remap_generation == Some(expected_generation)
            && self.controlled_remap_ticket == Some(attempt_ticket)
            && self.controlled_remap_depth > 0;
        if !was_armed
            || self.cancelled_generation == Some(expected_generation)
            || !window_visible_and_mapped
        {
            if !was_armed || self.cancelled_generation == Some(expected_generation) {
                self.mapped = false;
                self.pointer_inside = false;
            }
            return false;
        }
        self.controlled_remap_generation = None;
        self.controlled_remap_ticket = None;
        self.controlled_remap_depth = 0;
        self.mapped = true;
        true
    }

    /// Records a GTK `unmap` signal and returns `Some((generation, attempt_ticket))` when a
    /// controlled remap / pending-map token is armed for the current non-cancelled generation;
    /// otherwise fences the generation identically to [`Self::on_unmapped`].
    pub fn on_transient_unmap_preserving_epoch(&mut self) -> Option<(u64, u64)> {
        self.mapped = false;
        self.pointer_inside = false;
        let armed_for_current = self.open_generation != 0
            && self.cancelled_generation != Some(self.open_generation)
            && self.controlled_remap_generation == Some(self.open_generation)
            && self.controlled_remap_depth > 0;
        if armed_for_current {
            self.controlled_remap_ticket
                .map(|ticket| (self.open_generation, ticket))
        } else {
            self.controlled_remap_generation = None;
            self.controlled_remap_ticket = None;
            self.controlled_remap_depth = 0;
            if self.open_generation != 0 {
                self.cancelled_generation = Some(self.open_generation);
            }
            None
        }
    }

    /// Arms an explicit controlled layer-shell remap (such as `LayerShell::set_monitor` during
    /// popup presentation, scale refresh, or monitor hotplug) for `expected_generation`.
    #[cfg(test)]
    pub fn begin_controlled_remap(&mut self, expected_generation: u64) -> bool {
        if expected_generation == 0
            || self.open_generation != expected_generation
            || self.cancelled_generation == Some(expected_generation)
        {
            return false;
        }
        let already_armed = self.controlled_remap_generation == Some(expected_generation)
            && self.controlled_remap_depth > 0;
        if !self.mapped && !already_armed {
            return false;
        }
        self.controlled_remap_generation = Some(expected_generation);
        self.controlled_remap_depth = self.controlled_remap_depth.saturating_add(1);
        true
    }

    /// Records a GTK `unmap` signal on the layer window.
    #[cfg(test)]
    pub fn on_transient_unmap(&mut self) {
        let _ = self.on_transient_unmap_preserving_epoch();
        if self.open_generation != 0
            && self.controlled_remap_generation == Some(self.open_generation)
            && self.controlled_remap_depth > 0
        {
            return;
        }
        self.controlled_remap_generation = None;
        self.controlled_remap_ticket = None;
        self.controlled_remap_depth = 0;
        if self.open_generation != 0 {
            self.cancelled_generation = Some(self.open_generation);
        }
    }

    /// Aborts an armed controlled remap for `expected_generation` without disturbing any newer
    /// generation that may have opened reentrantly.
    #[cfg(test)]
    pub fn abort_controlled_remap(&mut self, expected_generation: u64) {
        if self.controlled_remap_generation == Some(expected_generation) {
            self.controlled_remap_generation = None;
            self.controlled_remap_ticket = None;
            self.controlled_remap_depth = 0;
        }
        if expected_generation != 0 && self.open_generation == expected_generation {
            self.mapped = false;
            self.pointer_inside = false;
            self.cancelled_generation = Some(expected_generation);
        }
    }

    /// Completes an explicit controlled remap for `expected_generation`, restoring `mapped = true`
    /// if and only if the controlled remap remained armed (not hidden, dismissed, or superseded by
    /// a newer open generation) and `window_visible_and_mapped` confirms the native layer window is
    /// live.
    #[cfg(test)]
    pub fn complete_controlled_remap(
        &mut self,
        expected_generation: u64,
        window_visible_and_mapped: bool,
    ) -> bool {
        let was_armed = self.controlled_remap_generation == Some(expected_generation)
            && self.controlled_remap_depth > 0;
        if was_armed {
            self.controlled_remap_depth -= 1;
            if self.controlled_remap_depth == 0 {
                self.controlled_remap_generation = None;
                self.controlled_remap_ticket = None;
            }
        }
        if expected_generation == 0 || self.open_generation != expected_generation {
            return false;
        }
        if !was_armed
            || self.cancelled_generation == Some(expected_generation)
            || !window_visible_and_mapped
        {
            self.mapped = false;
            self.pointer_inside = false;
            return false;
        }
        self.mapped = true;
        true
    }

    /// Returns `true` when `expected_generation` was explicitly hidden/dismissed or superseded by
    /// a newer open generation.
    pub fn is_fenced_or_superseded(&self, expected_generation: u64) -> bool {
        expected_generation == 0
            || self.open_generation != expected_generation
            || self.cancelled_generation == Some(expected_generation)
    }

    /// Marks the pointer as currently inside the popover surface (or one of its child widgets).
    pub fn on_pointer_entered(&mut self) {
        if self.mapped {
            self.pointer_inside = true;
        }
    }

    /// Updates pointer presence from a GTK leave-notify crossing detail, ignoring transitions into
    /// child widgets (`is_inferior == true`, such as moving onto the inner `WebKitWebView`).
    pub fn on_pointer_left(&mut self, is_inferior_child_crossing: bool) {
        if !is_inferior_child_crossing {
            self.pointer_inside = false;
        }
    }

    /// Resets and fences state when the popover genuinely hides, dismisses, closes, or loses its
    /// monitor, cancelling any in-flight controlled remap for the current generation.
    pub fn on_unmapped(&mut self) {
        self.mapped = false;
        self.pointer_inside = false;
        self.controlled_remap_generation = None;
        self.controlled_remap_ticket = None;
        self.controlled_remap_depth = 0;
        if self.open_generation != 0 {
            self.cancelled_generation = Some(self.open_generation);
        }
    }

    pub fn is_mapped(&self) -> bool {
        self.mapped
    }

    pub fn is_pointer_inside(&self) -> bool {
        self.mapped && self.pointer_inside
    }

    /// Evaluates whether a deferred focus-loss timer for `scheduled_generation` should dismiss the
    /// popover.
    pub fn evaluate_deferred_unfocus(
        &self,
        scheduled_generation: u64,
        window_currently_visible: bool,
        window_currently_active: bool,
    ) -> WaylandFocusCheckDecision {
        if !self.mapped
            || self.open_generation != scheduled_generation
            || !window_currently_visible
            || window_currently_active
            || self.pointer_inside
        {
            WaylandFocusCheckDecision::KeepOpen
        } else {
            WaylandFocusCheckDecision::DismissPopover
        }
    }
}

/// Distinguishes expected backdrop-admission cancellation (popup hidden, explicit quit, or
/// superseded by a newer open generation) from an active-visible capability failure where the
/// Wayland `zwlr_layer_shell_v1` v4+ popup remains visibly open for `generation` without
/// outside-click surfaces.
#[cfg(any(target_os = "linux", test))]
pub fn resolve_wayland_backdrop_admission_notice(
    sync_result: Option<Result<BackdropSyncOutcome, WaylandBackdropUnavailableReason>>,
    tracker: &WaylandLayerFocusTracker,
    generation: u64,
    window_currently_visible: bool,
    explicit_quit_requested: bool,
) -> Option<&'static str> {
    let popup_still_active_visible = window_currently_visible
        && !explicit_quit_requested
        && !tracker.is_fenced_or_superseded(generation);

    if let Some(Err(reason)) = sync_result {
        if !popup_still_active_visible {
            return None;
        }
        return Some(reason.capability_notice());
    }

    match sync_result {
        Some(Ok(BackdropSyncOutcome::Admitted { .. })) => None,
        Some(Ok(BackdropSyncOutcome::CancelledFailOpen))
        | Some(Ok(BackdropSyncOutcome::NotApplicable))
        | None => {
            if popup_still_active_visible {
                Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
            } else {
                None
            }
        }
        Some(Err(_)) => unreachable!(),
    }
}

/// Snapshot of native GTK popup window state sampled on the GTK main thread.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePopupMapSample {
    pub window_visible: bool,
    pub window_mapped: bool,
    pub is_layer_window: bool,
    pub explicit_quit_requested: bool,
}

#[cfg(any(target_os = "linux", test))]
impl NativePopupMapSample {
    pub fn is_live_mapped_layer(self) -> bool {
        self.window_visible
            && self.window_mapped
            && self.is_layer_window
            && !self.explicit_quit_requested
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopupAttemptKind {
    Open,
    Refresh,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopupAttemptExecutionOutcome {
    NotActive,
    SupersededSameGeneration,
    SupersededOrCancelled,
    ArmedPendingMap {
        effective_notice: Option<String>,
    },
    FinalizedSynchronously {
        admitted: bool,
        capability_notice: Option<&'static str>,
        effective_notice: Option<String>,
        should_publish_notice: bool,
    },
}

#[cfg(test)]
impl PopupAttemptExecutionOutcome {
    pub fn is_armed_pending(&self) -> bool {
        matches!(self, Self::ArmedPendingMap { .. })
    }

    pub fn is_admitted_sync(&self) -> bool {
        matches!(self, Self::FinalizedSynchronously { admitted: true, .. })
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingMapTrigger {
    ImmediatePostConfigure,
    MapSignal,
    IdleProbe,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PendingMapResolution {
    NoOpOrSuperseded,
    StillPending {
        remaining_timeout: Duration,
    },
    Finalized {
        admitted: bool,
        capability_notice: Option<&'static str>,
        effective_notice: Option<String>,
        should_publish_notice: bool,
    },
}

#[cfg(test)]
impl PendingMapResolution {
    pub fn is_admitted_finalized(&self) -> bool {
        matches!(self, Self::Finalized { admitted: true, .. })
    }
}

/// Truthful result of presenting the Linux tray popover, carrying explicit superseded/cancelled
/// outcomes so `tray.rs` never performs global teardown on a stale error or overwrites a newer
/// attempt's notice with a stale `None`.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq)]
pub enum LinuxOpenOutcome {
    Presented {
        outcome: PopoverPlacementOutcome,
        effective_notice: Option<String>,
    },
    SupersededSameGeneration,
    SupersededOrCancelled,
}

/// Main-thread popup map lifecycle adapter uniting [`OutsideClickSurfaceBackend`] with
/// ticket-fenced [`WaylandLayerFocusTracker`] transitions and live GTK window sampling.
#[cfg(any(target_os = "linux", test))]
pub trait PopupMapLifecycleBackend: OutsideClickSurfaceBackend {
    fn begin_tracker_attempt<FReserve>(
        &self,
        kind: PopupAttemptKind,
        arm_layer_map: bool,
        reserve_ticket_fn: FReserve,
    ) -> Option<(u64, u64)>
    where
        FReserve: FnOnce() -> u64;

    fn abort_tracker_attempt(&self, generation: u64, attempt_ticket: u64);

    fn disarm_expired_tracker_attempt(&self, generation: u64, attempt_ticket: u64) -> bool;

    fn complete_tracker_attempt_map(
        &self,
        generation: u64,
        attempt_ticket: u64,
        window_visible_and_mapped: bool,
    ) -> bool;

    fn schedule_on_demand_transition(
        &self,
        generation: u64,
        attempt_ticket: u64,
        keyboard_policy: WaylandKeyboardPolicy,
    );

    fn is_popup_content_hierarchy_settled(&self) -> bool;

    fn schedule_settled_popup_redraw_idle(&self, generation: u64, attempt_ticket: u64);

    fn queue_settled_popup_redraw(&self);

    fn sample_popup_state(&self) -> (WaylandLayerFocusTracker, NativePopupMapSample);
}

/// Main-thread outside-click dismissal adapter extending [`PopupMapLifecycleBackend`] with
/// native backdrop window/device identity checks, single master-pointer implicit-grab release,
/// and epoch-fenced popup hiding.
#[cfg(any(target_os = "linux", test))]
pub trait PopupOutsideClickDismissBackend: PopupMapLifecycleBackend {
    type ToplevelWindow: Clone + PartialEq + Eq;
    type NativeWindow: Clone + PartialEq + Eq;
    type NativeDisplay: Clone + PartialEq + Eq;
    type PointerDevice: Clone + PartialEq + Eq;

    fn inspect_tracked_surface_window(
        &self,
        surface: &Self::SurfaceHandle,
    ) -> Option<
        TrackedBackdropWindowIdentity<
            Self::ToplevelWindow,
            Self::NativeWindow,
            Self::NativeDisplay,
        >,
    >;

    fn ungrab_master_pointer(&self, master_pointer: &Self::PointerDevice, event_time: u32);

    fn hide_popup_for_dismissed_epoch(&self, generation: u64, attempt_ticket: u64);
}

#[cfg(any(target_os = "linux", test))]
fn is_popup_live_for_dismiss_token(
    tracker: &WaylandLayerFocusTracker,
    sample: NativePopupMapSample,
    token: ActiveBackdropDismissToken,
) -> bool {
    !sample.explicit_quit_requested
        && sample.is_live_mapped_layer()
        && tracker.is_mapped()
        && tracker.current_generation() == token.generation
        && tracker.latest_attempt_ticket() == token.attempt_ticket
        && !tracker.is_fenced_or_superseded(token.generation)
        && !tracker.is_attempt_superseded(token.generation, token.attempt_ticket)
}

/// Shared production outside-click backdrop button-press ordering seam:
///
/// 1. Validates GTK main-thread execution, genuine initial `ButtonPress` (`button >= 1`), exact
///    active enabled `(surface_id, generation, admission_seq, attempt_ticket)` ownership, live
///    unfenced popup state, exact `event.window == callback.window == tracked.window` + display
///    identity, and `event.device` itself being the seat's `MasterPointer` on the same display
///    (never promoting slave or `source_device` handles).
/// 2. Releases the pressed event's master-pointer implicit grab **once** via
///    [`PopupOutsideClickDismissBackend::ungrab_master_pointer`] at `ctx.event_time` with owned
///    window/device handles and zero `RefCell`/`Mutex` borrows held — strictly **before**
///    disabling any input region, disconnecting handlers, hiding, or closing owned backdrops.
/// 3. Revalidates the exact dismissal token **after** `ungrab` before surface teardown, and
///    revalidates **again** after fail-open surface release before hiding the popup so synchronous
///    Wayland crossing or teardown reentry (including same-generation `Refresh`, newer-generation
///    `Open`, or `hide`) never releases or hides newer state.
#[cfg(any(target_os = "linux", test))]
pub fn drive_outside_click_backdrop_press<B: PopupOutsideClickDismissBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    surface_id: u64,
    surface_generation: u64,
    ctx: BackdropNativePressContext<
        B::ToplevelWindow,
        B::NativeWindow,
        B::NativeDisplay,
        B::PointerDevice,
    >,
) -> BackdropDismissOutcome {
    if !ctx.is_gtk_main_thread {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    }

    match ctx.pointer_event {
        BackdropPointerEvent::ButtonPress { button } if button >= 1 => {}
        BackdropPointerEvent::ButtonPress { .. }
        | BackdropPointerEvent::PointerMotion
        | BackdropPointerEvent::NonInitialButtonEvent => {
            return BackdropDismissOutcome::IgnoredNonPress;
        }
    }

    enum CoordinatorTargetLookup<H> {
        Matched(ActiveBackdropDismissToken, H),
        InactiveOrStale(Vec<H>),
    }

    let lookup = {
        let mut state = state_cell.borrow_mut();
        let exact_active = if surface_generation != 0
            && !state.admission_in_progress
            && state.pending_map_admission.is_none()
            && state.active_generation == Some(surface_generation)
        {
            state
                .active_admission_seq
                .filter(|&seq| seq != 0 && state.latest_attempt_ticket == seq)
                .and_then(|admission_seq| {
                    state
                        .surfaces
                        .iter()
                        .find(|s| {
                            s.surface_id == surface_id
                                && s.generation == surface_generation
                                && s.admission_seq == admission_seq
                                && s.phase.is_active_bounded()
                        })
                        .map(|s| {
                            (
                                ActiveBackdropDismissToken {
                                    surface_id,
                                    generation: surface_generation,
                                    admission_seq,
                                    attempt_ticket: admission_seq,
                                },
                                s.handle.clone(),
                            )
                        })
                })
        } else {
            None
        };

        if let Some((token, tracked_handle)) = exact_active {
            CoordinatorTargetLookup::Matched(token, tracked_handle)
        } else {
            let active_gen = state.active_generation;
            let active_seq = state.active_admission_seq;
            let mut retained = Vec::with_capacity(state.surfaces.len());
            let mut stale_handles = Vec::new();
            for tracked in state.surfaces.drain(..) {
                if Some(tracked.generation) == active_gen
                    && Some(tracked.admission_seq) == active_seq
                {
                    retained.push(tracked);
                } else {
                    stale_handles.push(tracked.handle);
                }
            }
            state.surfaces = retained;
            CoordinatorTargetLookup::InactiveOrStale(stale_handles)
        }
    };

    let (token, tracked_handle) = match lookup {
        CoordinatorTargetLookup::Matched(token, tracked_handle) => (token, tracked_handle),
        CoordinatorTargetLookup::InactiveOrStale(stale_handles) => {
            for handle in stale_handles {
                backend.release_surface_fail_open(handle);
            }
            return BackdropDismissOutcome::RejectedStaleOrInactive;
        }
    };

    let (pre_tracker, pre_sample) = backend.sample_popup_state();
    if !is_popup_live_for_dismiss_token(&pre_tracker, pre_sample, token) {
        return BackdropDismissOutcome::RejectedStaleOrInactive;
    }

    let Some(tracked_window) = backend.inspect_tracked_surface_window(&tracked_handle) else {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    };
    if !tracked_window.is_live || tracked_window.display != ctx.backend_display {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    }
    let Some(owned_callback_toplevel) = ctx.callback_toplevel else {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    };
    if owned_callback_toplevel != tracked_window.toplevel {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    }
    let Some((owned_ev_win, ev_disp, ev_live)) = ctx.event_window else {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    };
    let Some((owned_cb_win, cb_disp, cb_live)) = ctx.callback_window else {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    };
    if !ev_live
        || !cb_live
        || ev_disp != ctx.backend_display
        || cb_disp != ctx.backend_display
        || owned_ev_win != owned_cb_win
        || owned_cb_win != tracked_window.native_window
    {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    }
    let Some(candidate_device) = ctx.device else {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    };
    if candidate_device.role != BackdropDeviceRole::MasterPointer
        || candidate_device.display != ctx.backend_display
        || candidate_device.seat_master_pointer.as_ref() != Some(&candidate_device.handle)
    {
        return BackdropDismissOutcome::RejectedInvalidNativeTarget;
    }
    let owned_master_pointer = candidate_device.handle;

    // Release the pressed master pointer's implicit GDK grab strictly BEFORE disabling any
    // backdrop input shape, disconnecting handlers, hiding, or closing the backdrop window, with
    // owned window/device refs alive and zero `RefCell`/`Mutex` borrows held across the call.
    backend.ungrab_master_pointer(&owned_master_pointer, ctx.event_time);
    drop((
        owned_master_pointer,
        owned_ev_win,
        owned_cb_win,
        owned_callback_toplevel,
        tracked_window,
        tracked_handle,
    ));

    // Revalidate the exact dismissal token AFTER `gdk_device_ungrab` (which may synchronously
    // emit Wayland crossing/grab-broken callbacks) and BEFORE tearing down surfaces.
    let coord_still_exact_after_ungrab = {
        let state = state_cell.borrow();
        !state.admission_in_progress
            && state.pending_map_admission.is_none()
            && state.active_generation == Some(token.generation)
            && state.active_admission_seq == Some(token.admission_seq)
            && state.latest_attempt_ticket == token.attempt_ticket
            && state.surfaces.iter().any(|s| {
                s.surface_id == token.surface_id
                    && s.generation == token.generation
                    && s.admission_seq == token.admission_seq
                    && s.phase.is_active_bounded()
            })
    };
    if !coord_still_exact_after_ungrab {
        return BackdropDismissOutcome::SupersededOrCancelledAfterUngrab;
    }
    let (mid_tracker, mid_sample) = backend.sample_popup_state();
    if !is_popup_live_for_dismiss_token(&mid_tracker, mid_sample, token) {
        let coord_superseded = is_attempt_superseded_in_coordinator(
            state_cell,
            token.generation,
            token.attempt_ticket,
        );
        if !coord_superseded
            && !mid_tracker.is_attempt_superseded(token.generation, token.attempt_ticket)
        {
            let _ = drive_release_up_to_attempt(
                state_cell,
                backend,
                token.generation,
                token.attempt_ticket,
            );
        }
        return BackdropDismissOutcome::SupersededOrCancelledAfterUngrab;
    }

    let still_current_after_release =
        drive_release_up_to_attempt(state_cell, backend, token.generation, token.attempt_ticket);
    if !still_current_after_release {
        return BackdropDismissOutcome::SupersededOrCancelledDuringRelease;
    }

    // Revalidate the exact dismissal token AGAIN after fail-open surface release before hiding
    // the popup so reentry inside `hide()`/`close()` cannot hide a newer refresh or reopen.
    let coord_post_release_exact = {
        let state = state_cell.borrow();
        state.active_generation.is_none()
            && state.active_admission_seq.is_none()
            && !state.admission_in_progress
            && state.pending_map_admission.is_none()
            && state.latest_attempt_ticket == token.attempt_ticket
            && state.surfaces.is_empty()
    };
    if !coord_post_release_exact {
        return BackdropDismissOutcome::SupersededOrCancelledDuringRelease;
    }
    let (post_tracker, post_sample) = backend.sample_popup_state();
    if !is_popup_live_for_dismiss_token(&post_tracker, post_sample, token) {
        return BackdropDismissOutcome::SupersededOrCancelledDuringRelease;
    }

    backend.hide_popup_for_dismissed_epoch(token.generation, token.attempt_ticket);
    BackdropDismissOutcome::DismissedPopover
}

/// Returns `true` when `type_name` identifies a real WebKitGTK webview widget (`WebKitWebView` or
/// `WebView` subclass) rather than a GTK window, container, or CSD titlebar widget.
#[cfg(any(target_os = "linux", test))]
pub fn is_webkit_webview_type_name(type_name: &str) -> bool {
    type_name == "WebKitWebView"
        || type_name.starts_with("WebKitWebView")
        || type_name.ends_with("WebView")
}

/// Evaluates whether a Wayland `Exclusive -> OnDemand` keyboard-mode transition for
/// `(scheduled_generation, scheduled_attempt_ticket)` should be scheduled or applied.
///
/// Fenced by **both** `scheduled_generation` and `scheduled_attempt_ticket` so an older same-gen
/// timer cannot alter newer geometry or focus, while transitioning active-visible v4 popups
/// (`ExclusiveThenOnDemand`) independently of backdrop admission success (including post-deadline
/// failure popups).
#[cfg(any(target_os = "linux", test))]
pub fn should_apply_wayland_on_demand_transition(
    tracker: &WaylandLayerFocusTracker,
    coordinator_latest_ticket: u64,
    scheduled_generation: u64,
    scheduled_attempt_ticket: u64,
    keyboard_policy: WaylandKeyboardPolicy,
    sample: NativePopupMapSample,
) -> bool {
    keyboard_policy == WaylandKeyboardPolicy::ExclusiveThenOnDemand
        && scheduled_generation != 0
        && scheduled_attempt_ticket != 0
        && coordinator_latest_ticket == scheduled_attempt_ticket
        && !tracker.is_fenced_or_superseded(scheduled_generation)
        && !tracker.is_attempt_superseded(scheduled_generation, scheduled_attempt_ticket)
        && sample.is_live_mapped_layer()
}

/// Evaluates whether a post-`::map` full GTK/WebKit widget redraw for
/// `(scheduled_generation, scheduled_attempt_ticket)` remains valid for the current live mapped
/// layer popup.
#[cfg(any(target_os = "linux", test))]
pub fn should_request_settled_popup_redraw(
    tracker: &WaylandLayerFocusTracker,
    coordinator_latest_ticket: u64,
    scheduled_generation: u64,
    scheduled_attempt_ticket: u64,
    sample: NativePopupMapSample,
) -> bool {
    scheduled_generation != 0
        && scheduled_attempt_ticket != 0
        && coordinator_latest_ticket == scheduled_attempt_ticket
        && !tracker.is_fenced_or_superseded(scheduled_generation)
        && !tracker.is_attempt_superseded(scheduled_generation, scheduled_attempt_ticket)
        && sample.is_live_mapped_layer()
}

/// Requests an epoch-fenced widget redraw once the live mapped layer popup's content subtree
/// (`GtkBin::child()` through `WebKitWebView`) is mapped and allocated beyond `1x1`.
///
/// Redraws immediately when content is already settled; otherwise records `(generation,
/// attempt_ticket)` in [`OutsideClickCoordinatorState`] so a subsequent `size-allocate` or content
/// `map` signal schedules at most one fenced GTK idle completion.
#[cfg(any(target_os = "linux", test))]
pub fn drive_request_settled_popup_redraw<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
) {
    let (tracker, sample) = backend.sample_popup_state();
    let coord_latest = state_cell.borrow().latest_attempt_ticket;
    if !should_request_settled_popup_redraw(
        &tracker,
        coord_latest,
        generation,
        attempt_ticket,
        sample,
    ) {
        let mut state = state_cell.borrow_mut();
        if state
            .pending_settled_redraw
            .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
        {
            state.pending_settled_redraw = None;
        }
        return;
    }
    if backend.is_popup_content_hierarchy_settled() {
        {
            let mut state = state_cell.borrow_mut();
            if state.latest_attempt_ticket != attempt_ticket {
                return;
            }
            if state
                .pending_settled_redraw
                .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
            {
                state.pending_settled_redraw = None;
            }
        }
        backend.queue_settled_popup_redraw();
    } else {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket == attempt_ticket {
            let keep_idle_flag = state.pending_settled_redraw.is_some_and(|p| {
                p.generation == generation && p.attempt_ticket == attempt_ticket && p.idle_scheduled
            });
            state.pending_settled_redraw = Some(PendingSettledPopupRedraw {
                generation,
                attempt_ticket,
                idle_scheduled: keep_idle_flag,
            });
        }
    }
}

/// Evaluates an actual popup `size-allocate` or content `map` signal against any pending
/// settled-redraw wait, scheduling at most one GTK idle callback per `(generation, attempt_ticket)`
/// while an idle is in flight and retaining the pending wait until the idle confirms settled
/// content.
#[cfg(any(target_os = "linux", test))]
pub fn drive_settled_popup_redraw_signal<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
) {
    let Some(pending) = state_cell.borrow().pending_settled_redraw else {
        return;
    };
    let (tracker, sample) = backend.sample_popup_state();
    let coord_latest = state_cell.borrow().latest_attempt_ticket;
    if !should_request_settled_popup_redraw(
        &tracker,
        coord_latest,
        pending.generation,
        pending.attempt_ticket,
        sample,
    ) {
        let mut state = state_cell.borrow_mut();
        if state
            .pending_settled_redraw
            .is_some_and(|p| p.attempt_ticket <= pending.attempt_ticket)
        {
            state.pending_settled_redraw = None;
        }
        return;
    }
    if pending.idle_scheduled || !backend.is_popup_content_hierarchy_settled() {
        return;
    }
    let should_schedule = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket == pending.attempt_ticket {
            if let Some(entry) = state.pending_settled_redraw.as_mut() {
                if entry.generation == pending.generation
                    && entry.attempt_ticket == pending.attempt_ticket
                    && !entry.idle_scheduled
                {
                    entry.idle_scheduled = true;
                    true
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    };
    if should_schedule {
        backend.schedule_settled_popup_redraw_idle(pending.generation, pending.attempt_ticket);
    }
}

/// Completes a queued settled-redraw idle step for `(generation, attempt_ticket)`.
///
/// Executes `queue_settled_popup_redraw` and clears the pending wait once live epoch ownership and
/// settled content allocation are confirmed; if content transiently became unsettled before idle
/// ran, resets `idle_scheduled = false` while retaining the matching pending wait for the next
/// actual `size-allocate` / `map` signal without self-rescheduling.
#[cfg(any(target_os = "linux", test))]
pub fn drive_settled_popup_redraw_idle<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
) {
    {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket != attempt_ticket {
            if state
                .pending_settled_redraw
                .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
            {
                state.pending_settled_redraw = None;
            }
            return;
        }
        let matches_pending = state
            .pending_settled_redraw
            .is_some_and(|p| p.generation == generation && p.attempt_ticket == attempt_ticket);
        if !matches_pending {
            return;
        }
    }

    let (tracker, sample) = backend.sample_popup_state();
    let coord_latest = state_cell.borrow().latest_attempt_ticket;
    if !should_request_settled_popup_redraw(
        &tracker,
        coord_latest,
        generation,
        attempt_ticket,
        sample,
    ) {
        let mut state = state_cell.borrow_mut();
        if state
            .pending_settled_redraw
            .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
        {
            state.pending_settled_redraw = None;
        }
        return;
    }

    if !backend.is_popup_content_hierarchy_settled() {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket == attempt_ticket {
            if let Some(entry) = state.pending_settled_redraw.as_mut() {
                if entry.generation == generation && entry.attempt_ticket == attempt_ticket {
                    entry.idle_scheduled = false;
                }
            }
        }
        return;
    }

    let claimed = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket == attempt_ticket
            && state
                .pending_settled_redraw
                .is_some_and(|p| p.generation == generation && p.attempt_ticket == attempt_ticket)
        {
            state.pending_settled_redraw = None;
            true
        } else {
            false
        }
    };
    if claimed {
        backend.queue_settled_popup_redraw();
    }
}

#[cfg(any(target_os = "linux", test))]
fn notify_confirmed_mapped_popup_lifecycle<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    keyboard_policy: Option<WaylandKeyboardPolicy>,
    tracker: &WaylandLayerFocusTracker,
    sample: NativePopupMapSample,
) {
    if let Some(policy) = keyboard_policy {
        let coord_latest = state_cell.borrow().latest_attempt_ticket;
        if should_apply_wayland_on_demand_transition(
            tracker,
            coord_latest,
            generation,
            attempt_ticket,
            policy,
            sample,
        ) {
            backend.schedule_on_demand_transition(generation, attempt_ticket, policy);
        }
    }
    drive_request_settled_popup_redraw(state_cell, backend, generation, attempt_ticket);
}

/// Returns `true` when `(generation, attempt_ticket)` remains the current unsuperseded,
/// non-cancelled presentation owner across both [`OutsideClickCoordinatorState`] and
/// [`WaylandLayerFocusTracker`].
#[cfg(any(target_os = "linux", test))]
pub fn is_presentation_epoch_current<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
) -> bool {
    if is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket) {
        return false;
    }
    let (tracker, sample) = backend.sample_popup_state();
    !sample.explicit_quit_requested
        && !tracker.is_fenced_or_superseded(generation)
        && !tracker.is_attempt_superseded(generation, attempt_ticket)
}

/// Epoch-guarded native popup presentation steps shared between the GTK runtime and deterministic
/// reentry tests so callback-capable GTK/Tauri writes (`set_size`, `resize`, `set_monitor`,
/// `show_all`, `popover.show()`) halt stale outer frames immediately on supersession or hide.
#[cfg(any(target_os = "linux", test))]
pub trait PopupPresentationTarget {
    fn apply_webview_logical_size(&mut self, logical_width: i32, logical_height: i32);

    fn apply_gtk_window_hints(
        &mut self,
        logical_width: i32,
        logical_height: i32,
        is_layer_surface: bool,
    );

    fn apply_layer_shell_surface_props(&mut self, layer: WaylandShellLayer, exclusive_zone: i32);

    fn apply_layer_shell_monitor(
        &mut self,
        placement_monitor_index: Option<usize>,
        fallback_target_monitor_index: Option<usize>,
    );

    fn apply_layer_shell_anchors_and_margins(&mut self, placement: &WaylandLayerShellPlacement);

    fn apply_initial_layer_keyboard_mode(&mut self, keyboard_policy: WaylandKeyboardPolicy);

    fn apply_gdk_root_position(&mut self, gdk_x: i32, gdk_y: i32);

    fn show_popup_children(&mut self);

    fn publish_pre_show_open_state(&mut self, base_notice: Option<String>);

    fn present_popup_window(&mut self) -> Result<(), String>;

    fn focus_and_present_non_layer_popup(&mut self);
}

/// Executes Wayland layer-shell popup size, monitor, anchor/margin, keyboard-mode, notice, and
/// show writes with production epoch checkpoints after each callback-capable operation.
#[cfg(any(target_os = "linux", test))]
#[allow(clippy::too_many_arguments)]
pub fn drive_present_layer_popup_with_checkpoints<B, T>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    target: &mut T,
    kind: PopupAttemptKind,
    generation: u64,
    attempt_ticket: u64,
    logical_size: (i32, i32),
    layer_placement: Option<&WaylandLayerShellPlacement>,
    fallback_target_monitor_index: Option<usize>,
    base_notice: Option<String>,
) -> Result<(), String>
where
    B: PopupMapLifecycleBackend,
    T: PopupPresentationTarget,
{
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    let (gdk_w, gdk_h) = logical_size;
    target.apply_webview_logical_size(gdk_w, gdk_h);
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    target.apply_gtk_window_hints(gdk_w, gdk_h, true);
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    if let Some(lp) = layer_placement {
        debug_assert!(lp.is_compact_non_exclusive());
        target.apply_layer_shell_surface_props(lp.layer, lp.exclusive_zone);
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }

        target.apply_layer_shell_monitor(lp.monitor_index, fallback_target_monitor_index);
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }

        target.apply_layer_shell_anchors_and_margins(lp);
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }

        if kind == PopupAttemptKind::Open {
            target.apply_initial_layer_keyboard_mode(lp.keyboard_policy);
            if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
                return Ok(());
            }
        }
    }

    if kind == PopupAttemptKind::Open {
        target.show_popup_children();
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }
        target.publish_pre_show_open_state(base_notice);
        target.present_popup_window()?;
    }

    Ok(())
}

/// Executes non-layer fallback (X11 / compositor-managed Wayland) popup sizing, positioning,
/// notice, show, and focus writes with production epoch checkpoints.
#[cfg(any(target_os = "linux", test))]
#[allow(clippy::too_many_arguments)]
pub fn drive_present_non_layer_popup_with_checkpoints<B, T>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    target: &mut T,
    kind: PopupAttemptKind,
    generation: u64,
    attempt_ticket: u64,
    logical_size: (i32, i32),
    x11_gdk_position: Option<(i32, i32)>,
    base_notice: Option<String>,
) -> Result<(), String>
where
    B: PopupMapLifecycleBackend,
    T: PopupPresentationTarget,
{
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    let (gdk_w, gdk_h) = logical_size;
    target.apply_webview_logical_size(gdk_w, gdk_h);
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    target.apply_gtk_window_hints(gdk_w, gdk_h, false);
    if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
        return Ok(());
    }

    if let Some((gdk_x, gdk_y)) = x11_gdk_position {
        target.apply_gdk_root_position(gdk_x, gdk_y);
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }
    }

    if kind == PopupAttemptKind::Open {
        target.show_popup_children();
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }
        target.publish_pre_show_open_state(base_notice);
        target.present_popup_window()?;
        if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
            return Ok(());
        }
        if let Some((gdk_x, gdk_y)) = x11_gdk_position {
            target.apply_gdk_root_position(gdk_x, gdk_y);
            if !is_presentation_epoch_current(state_cell, backend, generation, attempt_ticket) {
                return Ok(());
            }
        }
        target.focus_and_present_non_layer_popup();
    }

    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn classify_superseded_attempt<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
) -> PopupAttemptExecutionOutcome {
    let (tracker, sample) = backend.sample_popup_state();
    let same_gen_live = !sample.explicit_quit_requested
        && tracker.current_generation() == generation
        && !tracker.is_fenced_or_superseded(generation)
        && (tracker.latest_attempt_ticket() > attempt_ticket
            || is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket));
    if same_gen_live {
        PopupAttemptExecutionOutcome::SupersededSameGeneration
    } else {
        PopupAttemptExecutionOutcome::SupersededOrCancelled
    }
}

/// Cleans up a failed presentation attempt and distinguishes expected cancellation/supersession
/// (sampled **before** own `abort_tracker_attempt`, which marks `cancelled_generation`) from a
/// genuine current presentation failure that must recover `main`.
#[cfg(any(target_os = "linux", test))]
pub(crate) fn cleanup_and_classify_presentation_err<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    err: String,
) -> Result<PopupAttemptExecutionOutcome, String> {
    let pre_abort_superseded_or_cancelled = {
        let coord_superseded =
            is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket);
        let (pre_tracker, pre_sample) = backend.sample_popup_state();
        coord_superseded
            || pre_sample.explicit_quit_requested
            || pre_tracker.is_fenced_or_superseded(generation)
            || pre_tracker.is_attempt_superseded(generation, attempt_ticket)
    };

    backend.abort_tracker_attempt(generation, attempt_ticket);
    let still_current =
        drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
    let (post_tracker, post_sample) = backend.sample_popup_state();

    if pre_abort_superseded_or_cancelled
        || !still_current
        || post_tracker.is_attempt_superseded(generation, attempt_ticket)
        || post_sample.explicit_quit_requested
    {
        return Ok(classify_superseded_attempt(
            state_cell,
            backend,
            generation,
            attempt_ticket,
        ));
    }

    Err(err)
}

#[cfg(any(target_os = "linux", test))]
fn record_finalized_attempt_if_current<H>(
    state_cell: &RefCell<OutsideClickCoordinatorState<H>>,
    finalized: FinalizedPopupMapAttempt,
) {
    let mut state = state_cell.borrow_mut();
    if state.latest_attempt_ticket == finalized.attempt_ticket
        && (state.active_generation.is_none()
            || state.active_generation == Some(finalized.generation))
    {
        state.last_finalized_attempt = Some(finalized);
    }
}

/// Shared production coordinator for Wayland layer-shell popup initial open and scale/hotplug
/// refresh attempts.
///
/// Reserves `(generation, attempt_ticket)` upfront before pre-teardown, controlled remap, or
/// asynchronous `popover.show()`, arms [`PendingPopupMapAdmission`] with a finite absolute
/// deadline before running `configure_and_present_fn`, samples fresh monotonic time after
/// presentation, and completes backdrop admission either immediately when already mapped within
/// the absolute deadline or asynchronously once `connect_map` confirms native `visible + mapped +
/// layer` state.
#[cfg(any(target_os = "linux", test))]
#[allow(clippy::too_many_arguments)]
pub fn drive_execute_popup_map_attempt<B, FClock, FPresent>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    kind: PopupAttemptKind,
    keyboard_policy: Option<WaylandKeyboardPolicy>,
    plan_valid: bool,
    specs: &[WaylandOutsideClickBackdropSpec],
    base_notice: Option<String>,
    mut now_fn: FClock,
    pending_map_timeout: Duration,
    configure_and_present_fn: FPresent,
) -> Result<PopupAttemptExecutionOutcome, String>
where
    B: PopupMapLifecycleBackend,
    FClock: FnMut() -> Instant,
    FPresent: FnOnce(u64, u64) -> Result<(), String>,
{
    let res = backend.begin_tracker_attempt(kind, true, || reserve_attempt_ticket(state_cell));
    let Some((generation, attempt_ticket)) = res else {
        return Ok(PopupAttemptExecutionOutcome::NotActive);
    };

    if !drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket) {
        return Ok(classify_superseded_attempt(
            state_cell,
            backend,
            generation,
            attempt_ticket,
        ));
    }

    let (tracker_pre, sample_pre) = backend.sample_popup_state();
    if sample_pre.explicit_quit_requested
        || tracker_pre.is_fenced_or_superseded(generation)
        || tracker_pre.is_attempt_superseded(generation, attempt_ticket)
    {
        backend.abort_tracker_attempt(generation, attempt_ticket);
        let _ = drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        return Ok(classify_superseded_attempt(
            state_cell,
            backend,
            generation,
            attempt_ticket,
        ));
    }

    let entry_now = now_fn();
    let deadline = entry_now + pending_map_timeout;
    {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            drop(state);
            return Ok(classify_superseded_attempt(
                state_cell,
                backend,
                generation,
                attempt_ticket,
            ));
        }
        state.pending_map_admission = Some(PendingPopupMapAdmission {
            generation,
            attempt_ticket,
            deadline,
            keyboard_policy,
            plan_valid,
            specs: specs.to_vec(),
            base_notice: base_notice.clone(),
            idle_probe_scheduled: false,
        });
    }

    if let Err(err) = configure_and_present_fn(generation, attempt_ticket) {
        return cleanup_and_classify_presentation_err(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            err,
        );
    }

    let sync_finalized = {
        let mut state = state_cell.borrow_mut();
        if state
            .last_finalized_attempt
            .as_ref()
            .is_some_and(|f| f.generation == generation && f.attempt_ticket == attempt_ticket)
        {
            state.last_finalized_attempt.take()
        } else {
            None
        }
    };
    if let Some(finalized) = sync_finalized {
        if is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket) {
            return Ok(classify_superseded_attempt(
                state_cell,
                backend,
                generation,
                attempt_ticket,
            ));
        }
        return Ok(PopupAttemptExecutionOutcome::FinalizedSynchronously {
            admitted: finalized.admitted,
            capability_notice: finalized.capability_notice,
            effective_notice: finalized.effective_notice,
            should_publish_notice: finalized.should_publish_notice,
        });
    }

    let post_present_now = now_fn().max(entry_now);
    match drive_pending_map_progress(
        state_cell,
        backend,
        generation,
        attempt_ticket,
        PendingMapTrigger::ImmediatePostConfigure,
        post_present_now,
    ) {
        PendingMapResolution::NoOpOrSuperseded => Ok(classify_superseded_attempt(
            state_cell,
            backend,
            generation,
            attempt_ticket,
        )),
        PendingMapResolution::StillPending { remaining_timeout } => {
            backend.schedule_pending_map_timeout(generation, attempt_ticket, remaining_timeout);
            Ok(PopupAttemptExecutionOutcome::ArmedPendingMap {
                effective_notice: base_notice,
            })
        }
        PendingMapResolution::Finalized {
            admitted,
            capability_notice,
            effective_notice,
            should_publish_notice,
        } => Ok(PopupAttemptExecutionOutcome::FinalizedSynchronously {
            admitted,
            capability_notice,
            effective_notice,
            should_publish_notice,
        }),
    }
}

#[cfg(any(target_os = "linux", test))]
fn drive_expired_map_late_progress<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    trigger: PendingMapTrigger,
) -> PendingMapResolution {
    let matches_expired = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            if state
                .expired_map_attempt
                .as_ref()
                .is_some_and(|e| e.attempt_ticket <= attempt_ticket)
            {
                state.expired_map_attempt = None;
            }
            false
        } else {
            state.expired_map_attempt.as_ref().is_some_and(|e| {
                e.generation == generation
                    && e.attempt_ticket == attempt_ticket
                    && state.latest_attempt_ticket == attempt_ticket
            })
        }
    };
    if !matches_expired {
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let (tracker, sample) = backend.sample_popup_state();
    if sample.explicit_quit_requested
        || tracker.is_fenced_or_superseded(generation)
        || tracker.is_attempt_superseded(generation, attempt_ticket)
    {
        let mut state = state_cell.borrow_mut();
        if state
            .expired_map_attempt
            .as_ref()
            .is_some_and(|e| e.attempt_ticket <= attempt_ticket)
        {
            state.expired_map_attempt = None;
        }
        return PendingMapResolution::NoOpOrSuperseded;
    }

    if trigger == PendingMapTrigger::MapSignal || !sample.is_live_mapped_layer() {
        if trigger == PendingMapTrigger::MapSignal {
            let should_schedule_idle = {
                let mut state = state_cell.borrow_mut();
                if state.latest_attempt_ticket == attempt_ticket {
                    if let Some(expired) = state.expired_map_attempt.as_mut() {
                        if expired.generation == generation
                            && expired.attempt_ticket == attempt_ticket
                            && !expired.idle_probe_scheduled
                        {
                            expired.idle_probe_scheduled = true;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if should_schedule_idle {
                backend.schedule_pending_map_idle_probe(generation, attempt_ticket);
            }
        } else if trigger == PendingMapTrigger::IdleProbe {
            let mut state = state_cell.borrow_mut();
            if let Some(expired) = state.expired_map_attempt.as_mut() {
                if expired.generation == generation && expired.attempt_ticket == attempt_ticket {
                    expired.idle_probe_scheduled = false;
                }
            }
        }
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let expired = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket != attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            return PendingMapResolution::NoOpOrSuperseded;
        }
        match &state.expired_map_attempt {
            Some(e) if e.generation == generation && e.attempt_ticket == attempt_ticket => {
                state.expired_map_attempt.take().unwrap()
            }
            _ => return PendingMapResolution::NoOpOrSuperseded,
        }
    };

    // Never resurrect `tracker.mapped` or admit outside-click surfaces after deadline expiry,
    // while still scheduling `Exclusive -> OnDemand` and a settled widget redraw for the
    // now-mapped popup after `::map` unwinds.
    notify_confirmed_mapped_popup_lifecycle(
        state_cell,
        backend,
        generation,
        attempt_ticket,
        expired.keyboard_policy,
        &tracker,
        sample,
    );

    if expired.notice_already_published || !expired.specs_non_empty {
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let capability_notice = resolve_wayland_backdrop_admission_notice(
        None,
        &tracker,
        generation,
        sample.window_visible,
        sample.explicit_quit_requested,
    );
    let should_publish_notice = capability_notice.is_some();
    if !should_publish_notice {
        return PendingMapResolution::NoOpOrSuperseded;
    }
    let effective_notice =
        combine_popover_notices(expired.base_notice.as_deref(), capability_notice);
    let finalized = FinalizedPopupMapAttempt {
        generation,
        attempt_ticket,
        admitted: false,
        capability_notice,
        effective_notice,
        should_publish_notice,
    };
    record_finalized_attempt_if_current(state_cell, finalized.clone());
    PendingMapResolution::Finalized {
        admitted: finalized.admitted,
        capability_notice: finalized.capability_notice,
        effective_notice: finalized.effective_notice,
        should_publish_notice: finalized.should_publish_notice,
    }
}

/// Advances an explicitly armed `(generation, attempt_ticket)` pending-map operation.
///
/// Always defers [`PendingMapTrigger::MapSignal`] to at most one epoch/ticket-fenced GTK idle step
/// after the popup `::map` stack unwinds (even when `visible && mapped && is_layer_window` flags
/// are already `true` inside `connect_map`), and completes backdrop admission via
/// [`drive_sync_for_attempt`] on the pre-reserved `attempt_ticket` only after native mapping is
/// confirmed outside `::map`.
#[cfg(any(target_os = "linux", test))]
pub fn drive_pending_map_progress<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    trigger: PendingMapTrigger,
    now: Instant,
) -> PendingMapResolution {
    let pending_snapshot = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            if state
                .pending_map_admission
                .as_ref()
                .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
            {
                state.pending_map_admission = None;
            }
            if state
                .expired_map_attempt
                .as_ref()
                .is_some_and(|e| e.attempt_ticket <= attempt_ticket)
            {
                state.expired_map_attempt = None;
            }
            return PendingMapResolution::NoOpOrSuperseded;
        }
        match &state.pending_map_admission {
            Some(p) if p.generation == generation && p.attempt_ticket == attempt_ticket => {
                Some(p.clone())
            }
            _ => None,
        }
    };
    let Some(pending_snapshot) = pending_snapshot else {
        return drive_expired_map_late_progress(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            trigger,
        );
    };

    if trigger != PendingMapTrigger::MapSignal && now >= pending_snapshot.deadline {
        return drive_pending_map_deadline(state_cell, backend, generation, attempt_ticket, now);
    }

    let (tracker, sample) = backend.sample_popup_state();
    if sample.explicit_quit_requested
        || tracker.is_fenced_or_superseded(generation)
        || tracker.is_attempt_superseded(generation, attempt_ticket)
    {
        backend.abort_tracker_attempt(generation, attempt_ticket);
        let _ = drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        return PendingMapResolution::NoOpOrSuperseded;
    }

    if trigger == PendingMapTrigger::MapSignal || !sample.is_live_mapped_layer() {
        if trigger == PendingMapTrigger::MapSignal {
            let should_schedule_idle = {
                let mut state = state_cell.borrow_mut();
                if state.latest_attempt_ticket == attempt_ticket {
                    if let Some(p) = state.pending_map_admission.as_mut() {
                        if p.generation == generation
                            && p.attempt_ticket == attempt_ticket
                            && !p.idle_probe_scheduled
                        {
                            p.idle_probe_scheduled = true;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            };
            if should_schedule_idle {
                backend.schedule_pending_map_idle_probe(generation, attempt_ticket);
            }
        } else if trigger == PendingMapTrigger::IdleProbe {
            let mut state = state_cell.borrow_mut();
            if let Some(p) = state.pending_map_admission.as_mut() {
                if p.generation == generation && p.attempt_ticket == attempt_ticket {
                    p.idle_probe_scheduled = false;
                }
            }
        }
        return PendingMapResolution::StillPending {
            remaining_timeout: pending_snapshot.deadline.saturating_duration_since(now),
        };
    }

    let pending = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            return PendingMapResolution::NoOpOrSuperseded;
        }
        match &state.pending_map_admission {
            Some(p) if p.generation == generation && p.attempt_ticket == attempt_ticket => {
                state.pending_map_admission.take().unwrap()
            }
            _ => return PendingMapResolution::NoOpOrSuperseded,
        }
    };

    if !backend.complete_tracker_attempt_map(generation, attempt_ticket, true) {
        let _ = drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        return PendingMapResolution::NoOpOrSuperseded;
    }

    if pending.specs.is_empty() {
        let still_current =
            drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        let (post_tracker, post_sample) = backend.sample_popup_state();
        if !still_current
            || post_sample.explicit_quit_requested
            || post_tracker.is_fenced_or_superseded(generation)
            || post_tracker.is_attempt_superseded(generation, attempt_ticket)
        {
            return PendingMapResolution::NoOpOrSuperseded;
        }
        notify_confirmed_mapped_popup_lifecycle(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            pending.keyboard_policy,
            &post_tracker,
            post_sample,
        );
        let finalized = FinalizedPopupMapAttempt {
            generation,
            attempt_ticket,
            admitted: false,
            capability_notice: None,
            effective_notice: pending.base_notice,
            should_publish_notice: true,
        };
        record_finalized_attempt_if_current(state_cell, finalized.clone());
        return PendingMapResolution::Finalized {
            admitted: finalized.admitted,
            capability_notice: finalized.capability_notice,
            effective_notice: finalized.effective_notice,
            should_publish_notice: finalized.should_publish_notice,
        };
    }

    if !pending.plan_valid {
        let still_current =
            drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        let (post_tracker, post_sample) = backend.sample_popup_state();
        if !still_current || post_tracker.is_attempt_superseded(generation, attempt_ticket) {
            return PendingMapResolution::NoOpOrSuperseded;
        }
        notify_confirmed_mapped_popup_lifecycle(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            pending.keyboard_policy,
            &post_tracker,
            post_sample,
        );
        let capability_notice = resolve_wayland_backdrop_admission_notice(
            None,
            &post_tracker,
            generation,
            post_sample.window_visible,
            post_sample.explicit_quit_requested,
        );
        let should_publish_notice = !post_sample.explicit_quit_requested
            && !post_tracker.is_fenced_or_superseded(generation)
            && capability_notice.is_some();
        let effective_notice =
            combine_popover_notices(pending.base_notice.as_deref(), capability_notice);
        let finalized = FinalizedPopupMapAttempt {
            generation,
            attempt_ticket,
            admitted: false,
            capability_notice,
            effective_notice,
            should_publish_notice,
        };
        record_finalized_attempt_if_current(state_cell, finalized.clone());
        return PendingMapResolution::Finalized {
            admitted: finalized.admitted,
            capability_notice: finalized.capability_notice,
            effective_notice: finalized.effective_notice,
            should_publish_notice: finalized.should_publish_notice,
        };
    }

    let sync_result = drive_sync_for_attempt(
        state_cell,
        backend,
        generation,
        attempt_ticket,
        true,
        &pending.specs,
    );
    if is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket) {
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let (mid_tracker, mid_sample) = backend.sample_popup_state();
    if mid_tracker.is_attempt_superseded(generation, attempt_ticket) {
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let admitted_ok = matches!(sync_result, Ok(BackdropSyncOutcome::Admitted { .. }))
        && mid_sample.is_live_mapped_layer()
        && !mid_tracker.is_fenced_or_superseded(generation);

    if !admitted_ok {
        let still_current =
            drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
        let (post_tracker, post_sample) = backend.sample_popup_state();
        if !still_current || post_tracker.is_attempt_superseded(generation, attempt_ticket) {
            return PendingMapResolution::NoOpOrSuperseded;
        }
        notify_confirmed_mapped_popup_lifecycle(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            pending.keyboard_policy,
            &post_tracker,
            post_sample,
        );
        let capability_notice = resolve_wayland_backdrop_admission_notice(
            Some(sync_result),
            &post_tracker,
            generation,
            post_sample.window_visible,
            post_sample.explicit_quit_requested,
        );
        let should_publish_notice = !post_sample.explicit_quit_requested
            && !post_tracker.is_fenced_or_superseded(generation)
            && capability_notice.is_some();
        let effective_notice =
            combine_popover_notices(pending.base_notice.as_deref(), capability_notice);
        let finalized = FinalizedPopupMapAttempt {
            generation,
            attempt_ticket,
            admitted: false,
            capability_notice,
            effective_notice,
            should_publish_notice,
        };
        record_finalized_attempt_if_current(state_cell, finalized.clone());
        return PendingMapResolution::Finalized {
            admitted: finalized.admitted,
            capability_notice: finalized.capability_notice,
            effective_notice: finalized.effective_notice,
            should_publish_notice: finalized.should_publish_notice,
        };
    }

    notify_confirmed_mapped_popup_lifecycle(
        state_cell,
        backend,
        generation,
        attempt_ticket,
        pending.keyboard_policy,
        &mid_tracker,
        mid_sample,
    );
    let effective_notice = combine_popover_notices(pending.base_notice.as_deref(), None);
    let finalized = FinalizedPopupMapAttempt {
        generation,
        attempt_ticket,
        admitted: true,
        capability_notice: None,
        effective_notice,
        should_publish_notice: true,
    };
    record_finalized_attempt_if_current(state_cell, finalized.clone());
    PendingMapResolution::Finalized {
        admitted: finalized.admitted,
        capability_notice: finalized.capability_notice,
        effective_notice: finalized.effective_notice,
        should_publish_notice: finalized.should_publish_notice,
    }
}

/// Enforces the finite absolute pending-map deadline for `(generation, attempt_ticket)`, cleaning
/// up only that attempt's state, disarming late map/refresh resurrection, and truthfully reporting
/// active-visible capability failure when the popup remains visibly open without backdrops.
#[cfg(any(target_os = "linux", test))]
pub fn drive_pending_map_deadline<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    attempt_ticket: u64,
    now: Instant,
) -> PendingMapResolution {
    let pending = {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket > attempt_ticket
            || state
                .active_generation
                .is_some_and(|active| active > generation)
        {
            if state
                .pending_map_admission
                .as_ref()
                .is_some_and(|p| p.attempt_ticket <= attempt_ticket)
            {
                state.pending_map_admission = None;
            }
            if state
                .expired_map_attempt
                .as_ref()
                .is_some_and(|e| e.attempt_ticket <= attempt_ticket)
            {
                state.expired_map_attempt = None;
            }
            return PendingMapResolution::NoOpOrSuperseded;
        }
        match &state.pending_map_admission {
            Some(p) if p.generation == generation && p.attempt_ticket == attempt_ticket => {
                if now < p.deadline {
                    return PendingMapResolution::StillPending {
                        remaining_timeout: p.deadline.saturating_duration_since(now),
                    };
                }
                state.pending_map_admission.take().unwrap()
            }
            _ => return PendingMapResolution::NoOpOrSuperseded,
        }
    };

    let disarmed = backend.disarm_expired_tracker_attempt(generation, attempt_ticket);
    let still_current =
        drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
    let (post_tracker, post_sample) = backend.sample_popup_state();
    if !disarmed || !still_current || post_tracker.is_attempt_superseded(generation, attempt_ticket)
    {
        return PendingMapResolution::NoOpOrSuperseded;
    }

    let capability_notice = if pending.specs.is_empty() {
        None
    } else {
        resolve_wayland_backdrop_admission_notice(
            None,
            &post_tracker,
            generation,
            post_sample.window_visible,
            post_sample.explicit_quit_requested,
        )
    };
    let should_publish_notice = !post_sample.explicit_quit_requested
        && !post_tracker.is_fenced_or_superseded(generation)
        && capability_notice.is_some();

    if post_sample.is_live_mapped_layer() {
        notify_confirmed_mapped_popup_lifecycle(
            state_cell,
            backend,
            generation,
            attempt_ticket,
            pending.keyboard_policy,
            &post_tracker,
            post_sample,
        );
    } else if !post_sample.explicit_quit_requested
        && !post_tracker.is_fenced_or_superseded(generation)
        && (pending.keyboard_policy.is_some() || !pending.specs.is_empty())
    {
        let mut state = state_cell.borrow_mut();
        if state.latest_attempt_ticket == attempt_ticket
            && (state.active_generation.is_none() || state.active_generation == Some(generation))
        {
            state.expired_map_attempt = Some(ExpiredPopupMapAttempt {
                generation,
                attempt_ticket,
                keyboard_policy: pending.keyboard_policy,
                specs_non_empty: !pending.specs.is_empty(),
                base_notice: pending.base_notice.clone(),
                notice_already_published: should_publish_notice,
                idle_probe_scheduled: false,
            });
        }
    }

    let effective_notice =
        combine_popover_notices(pending.base_notice.as_deref(), capability_notice);
    let finalized = FinalizedPopupMapAttempt {
        generation,
        attempt_ticket,
        admitted: false,
        capability_notice,
        effective_notice,
        should_publish_notice,
    };
    record_finalized_attempt_if_current(state_cell, finalized.clone());
    PendingMapResolution::Finalized {
        admitted: finalized.admitted,
        capability_notice: finalized.capability_notice,
        effective_notice: finalized.effective_notice,
        should_publish_notice: finalized.should_publish_notice,
    }
}

/// Releases tracked outside-click surfaces fail-open when the GTK layer popup emits `unmap`,
/// preserving [`PendingPopupMapAdmission`] **only** when `preserved_remap_epoch` confirms an
/// armed controlled remap for that exact `(generation, attempt_ticket)` (or newer).
#[cfg(any(target_os = "linux", test))]
pub fn drive_popup_transient_unmap<B: OutsideClickSurfaceBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    preserved_remap_epoch: Option<(u64, u64)>,
) {
    let drained = {
        let mut state = state_cell.borrow_mut();
        state.active_generation = None;
        state.active_admission_seq = None;
        state.admission_in_progress = false;
        let keep_pending = match (&state.pending_map_admission, preserved_remap_epoch) {
            (Some(pending), Some((gen, ticket))) => {
                (pending.generation == gen && pending.attempt_ticket >= ticket)
                    || pending.generation > gen
            }
            _ => false,
        };
        if !keep_pending {
            state.pending_map_admission = None;
        }
        let keep_redraw = match (state.pending_settled_redraw, preserved_remap_epoch) {
            (Some(p), Some((gen, ticket))) => {
                (p.generation == gen && p.attempt_ticket >= ticket) || p.generation > gen
            }
            _ => false,
        };
        if !keep_redraw {
            state.pending_settled_redraw = None;
        }
        if preserved_remap_epoch.is_none() {
            state.expired_map_attempt = None;
        }
        std::mem::take(&mut state.surfaces)
    };
    for tracked in drained {
        backend.release_surface_fail_open(tracked.handle);
    }
}

/// Evaluates a compositor `size-allocate` callback using ticket-bearing configure admission and
/// re-samples ownership and native popup visibility **after** fail-open teardown before returning
/// any capability notice to publish.
#[cfg(any(target_os = "linux", test))]
pub fn drive_async_surface_configured<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    surface_id: u64,
    generation: u64,
    raw_width: i32,
    raw_height: i32,
) -> Option<&'static str> {
    match drive_surface_configured_with_ticket(
        state_cell, backend, surface_id, generation, raw_width, raw_height,
    ) {
        Ok(()) => None,
        Err((attempt_ticket, reason)) => {
            let _ = drive_release_up_to_attempt(state_cell, backend, generation, attempt_ticket);
            if is_attempt_superseded_in_coordinator(state_cell, generation, attempt_ticket) {
                return None;
            }
            let (post_tracker, post_sample) = backend.sample_popup_state();
            if post_tracker.is_attempt_superseded(generation, attempt_ticket) {
                return None;
            }
            resolve_wayland_backdrop_admission_notice(
                Some(Err(reason)),
                &post_tracker,
                generation,
                post_sample.window_visible,
                post_sample.explicit_quit_requested,
            )
        }
    }
}

/// Evaluates the finite `500ms` backdrop configure deadline for `(generation, admission_seq)` and
/// re-samples ownership and native popup visibility **after** fail-open teardown before returning
/// any capability notice to publish.
#[cfg(any(target_os = "linux", test))]
pub fn drive_async_configure_timeout<B: PopupMapLifecycleBackend>(
    state_cell: &RefCell<OutsideClickCoordinatorState<B::SurfaceHandle>>,
    backend: &B,
    generation: u64,
    admission_seq: u64,
) -> Option<&'static str> {
    match drive_configure_timeout(state_cell, backend, generation, admission_seq) {
        Ok(_) => None,
        Err(reason) => {
            let _ = drive_release_up_to_attempt(state_cell, backend, generation, admission_seq);
            if is_attempt_superseded_in_coordinator(state_cell, generation, admission_seq) {
                return None;
            }
            let (post_tracker, post_sample) = backend.sample_popup_state();
            if post_tracker.is_attempt_superseded(generation, admission_seq) {
                return None;
            }
            resolve_wayland_backdrop_admission_notice(
                Some(Err(reason)),
                &post_tracker,
                generation,
                post_sample.window_visible,
                post_sample.explicit_quit_requested,
            )
        }
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq)]
enum GdkResolvedAnchor {
    Rect {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    Point {
        x: f64,
        y: f64,
    },
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq)]
struct PhysicalSpan {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

#[cfg(any(target_os = "linux", test))]
fn sanitize_scale(scale_factor: f64) -> f64 {
    if scale_factor.is_finite() && scale_factor > 0.0 {
        scale_factor
    } else {
        1.0
    }
}

#[cfg(any(target_os = "linux", test))]
fn derive_physical_spans(monitors: &[LinuxMonitorWorkarea]) -> Vec<PhysicalSpan> {
    let n = monitors.len();
    let mut resolved: Vec<Option<(f64, f64)>> = vec![None; n];
    let sizes: Vec<(f64, f64)> = monitors
        .iter()
        .map(|m| {
            let s = sanitize_scale(m.scale_factor);
            (
                (m.geometry_width.max(1) as f64) * s,
                (m.geometry_height.max(1) as f64) * s,
            )
        })
        .collect();

    for (idx, mon) in monitors.iter().enumerate() {
        if mon.geometry_x == 0 && mon.geometry_y == 0 {
            resolved[idx] = Some((0.0, 0.0));
        }
    }
    if resolved.iter().all(Option::is_none) && !monitors.is_empty() {
        let s = sanitize_scale(monitors[0].scale_factor);
        resolved[0] = Some((
            (monitors[0].geometry_x as f64) * s,
            (monitors[0].geometry_y as f64) * s,
        ));
    }

    for _ in 0..n {
        let mut progress = false;
        for i in 0..n {
            if resolved[i].is_some() {
                continue;
            }
            for j in 0..n {
                let Some((jx, jy)) = resolved[j] else {
                    continue;
                };
                let m_i = &monitors[i];
                let m_j = &monitors[j];
                let (jw, jh) = sizes[j];
                let (iw, ih) = sizes[i];

                let candidate_x = if m_i.geometry_x == m_j.geometry_x + m_j.geometry_width {
                    Some(jx + jw)
                } else if m_i.geometry_x + m_i.geometry_width == m_j.geometry_x {
                    Some(jx - iw)
                } else if m_i.geometry_x == m_j.geometry_x {
                    Some(jx)
                } else {
                    None
                };

                let candidate_y = if m_i.geometry_y == m_j.geometry_y + m_j.geometry_height {
                    Some(jy + jh)
                } else if m_i.geometry_y + m_i.geometry_height == m_j.geometry_y {
                    Some(jy - ih)
                } else if m_i.geometry_y == m_j.geometry_y {
                    Some(jy)
                } else {
                    None
                };

                if let (Some(cx), Some(cy)) = (candidate_x, candidate_y) {
                    resolved[i] = Some((cx, cy));
                    progress = true;
                    break;
                } else if let Some(cx) = candidate_x {
                    let scale_i = sanitize_scale(m_i.scale_factor);
                    let cy = jy + ((m_i.geometry_y - m_j.geometry_y) as f64) * scale_i;
                    resolved[i] = Some((cx, cy));
                    progress = true;
                    break;
                } else if let Some(cy) = candidate_y {
                    let scale_i = sanitize_scale(m_i.scale_factor);
                    let cx = jx + ((m_i.geometry_x - m_j.geometry_x) as f64) * scale_i;
                    resolved[i] = Some((cx, cy));
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
            let s = sanitize_scale(mon.scale_factor);
            let fallback_origin = ((mon.geometry_x as f64) * s, (mon.geometry_y as f64) * s);
            let (px, py) = resolved[idx].unwrap_or(fallback_origin);
            let (pw, ph) = sizes[idx];
            PhysicalSpan {
                x: px,
                y: py,
                width: pw,
                height: ph,
            }
        })
        .collect()
}

#[cfg(any(target_os = "linux", test))]
fn point_in_gdk_geometry(monitor: &LinuxMonitorWorkarea, gx: f64, gy: f64, tolerance: f64) -> bool {
    if !gx.is_finite() || !gy.is_finite() {
        return false;
    }
    let min_x = (monitor.geometry_x as f64) - tolerance;
    let max_x = (monitor.geometry_x as f64) + (monitor.geometry_width as f64) + tolerance;
    let min_y = (monitor.geometry_y as f64) - tolerance;
    let max_y = (monitor.geometry_y as f64) + (monitor.geometry_height as f64) + tolerance;
    if tolerance == 0.0 {
        gx >= min_x && gx < max_x && gy >= min_y && gy < max_y
    } else {
        gx >= min_x && gx <= max_x && gy >= min_y && gy <= max_y
    }
}

#[cfg(any(target_os = "linux", test))]
fn point_in_physical_span(span: &PhysicalSpan, px: f64, py: f64, tolerance: f64) -> bool {
    if !px.is_finite() || !py.is_finite() {
        return false;
    }
    let min_x = span.x - tolerance;
    let max_x = span.x + span.width + tolerance;
    let min_y = span.y - tolerance;
    let max_y = span.y + span.height + tolerance;
    if tolerance == 0.0 {
        px >= min_x && px < max_x && py >= min_y && py < max_y
    } else {
        px >= min_x && px <= max_x && py >= min_y && py <= max_y
    }
}

#[cfg(any(target_os = "linux", test))]
fn physical_to_gdk_on_monitor(
    monitor: &LinuxMonitorWorkarea,
    span: &PhysicalSpan,
    px: f64,
    py: f64,
) -> (f64, f64) {
    let scale = sanitize_scale(monitor.scale_factor);
    let gx = (monitor.geometry_x as f64) + (px - span.x) / scale;
    let gy = (monitor.geometry_y as f64) + (py - span.y) / scale;
    (gx, gy)
}

#[cfg(any(target_os = "linux", test))]
fn gdk_to_physical_on_monitor(
    monitor: &LinuxMonitorWorkarea,
    span: &PhysicalSpan,
    gdk_x: i32,
    gdk_y: i32,
) -> (i32, i32) {
    let scale = sanitize_scale(monitor.scale_factor);
    let local_x = (gdk_x - monitor.geometry_x) as f64;
    let local_y = (gdk_y - monitor.geometry_y) as f64;
    let phys_x = (span.x + local_x * scale).round() as i32;
    let phys_y = (span.y + local_y * scale).round() as i32;
    (phys_x, phys_y)
}

#[cfg(any(target_os = "linux", test))]
fn resolve_rect_on_linux_monitors(
    backend: LinuxDisplayBackend,
    monitors: &[LinuxMonitorWorkarea],
    spans: &[PhysicalSpan],
    rect: RawTrayRect,
) -> Option<(usize, LinuxMonitorWorkarea, GdkResolvedAnchor)> {
    match rect.position {
        RawCoordinate2D::Logical { x, y } => {
            if !x.is_finite() || !y.is_finite() {
                return None;
            }
            for &tolerance in &[0.0, 32.0] {
                for (idx, monitor) in monitors.iter().enumerate() {
                    let scale = sanitize_scale(monitor.scale_factor);
                    let (gw, gh) = match rect.size {
                        RawSize2D::Logical { width, height } => (width, height),
                        RawSize2D::Physical { width, height } => (width / scale, height / scale),
                    };
                    if !gw.is_finite() || !gh.is_finite() || (gw <= 1.0 && gh <= 1.0) {
                        continue;
                    }
                    if point_in_gdk_geometry(monitor, x + gw / 2.0, y + gh / 2.0, tolerance) {
                        return Some((
                            idx,
                            *monitor,
                            GdkResolvedAnchor::Rect {
                                x,
                                y,
                                width: gw,
                                height: gh,
                            },
                        ));
                    }
                }
            }
            None
        }
        RawCoordinate2D::Physical { x, y } => {
            if !x.is_finite() || !y.is_finite() {
                return None;
            }
            let prefer_physical_first = matches!(backend, LinuxDisplayBackend::X11);
            let passes: &[bool] = if prefer_physical_first {
                &[true, false]
            } else {
                &[false, true]
            };

            for &tolerance in &[0.0, 32.0] {
                for &as_physical in passes {
                    for (idx, (monitor, span)) in monitors.iter().zip(spans.iter()).enumerate() {
                        let scale = sanitize_scale(monitor.scale_factor);
                        if as_physical {
                            let (pw, ph) = match rect.size {
                                RawSize2D::Physical { width, height } => (width, height),
                                RawSize2D::Logical { width, height } => {
                                    (width * scale, height * scale)
                                }
                            };
                            if !pw.is_finite()
                                || !ph.is_finite()
                                || (pw / scale <= 1.0 && ph / scale <= 1.0)
                            {
                                continue;
                            }
                            if point_in_physical_span(span, x + pw / 2.0, y + ph / 2.0, tolerance) {
                                let (gx, gy) = physical_to_gdk_on_monitor(monitor, span, x, y);
                                return Some((
                                    idx,
                                    *monitor,
                                    GdkResolvedAnchor::Rect {
                                        x: gx,
                                        y: gy,
                                        width: pw / scale,
                                        height: ph / scale,
                                    },
                                ));
                            }
                        } else {
                            let (gw, gh) = match rect.size {
                                RawSize2D::Logical { width, height } => (width, height),
                                RawSize2D::Physical { width, height } => (width, height),
                            };
                            if !gw.is_finite() || !gh.is_finite() || (gw <= 1.0 && gh <= 1.0) {
                                continue;
                            }
                            let cx = x + gw / 2.0;
                            let cy = y + gh / 2.0;
                            if point_in_gdk_geometry(monitor, cx, cy, tolerance) {
                                return Some((
                                    idx,
                                    *monitor,
                                    GdkResolvedAnchor::Rect {
                                        x,
                                        y,
                                        width: gw,
                                        height: gh,
                                    },
                                ));
                            }
                        }
                    }
                }
            }
            None
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn resolve_point_on_linux_monitors(
    backend: LinuxDisplayBackend,
    monitors: &[LinuxMonitorWorkarea],
    spans: &[PhysicalSpan],
    coord: RawCoordinate2D,
) -> Option<(usize, LinuxMonitorWorkarea, GdkResolvedAnchor)> {
    match coord {
        RawCoordinate2D::Logical { x, y } => {
            if !x.is_finite() || !y.is_finite() || (x == 0.0 && y == 0.0) {
                return None;
            }
            for &tolerance in &[0.0, 32.0] {
                for (idx, monitor) in monitors.iter().enumerate() {
                    if point_in_gdk_geometry(monitor, x, y, tolerance) {
                        return Some((idx, *monitor, GdkResolvedAnchor::Point { x, y }));
                    }
                }
            }
            None
        }
        RawCoordinate2D::Physical { x, y } => {
            if !x.is_finite() || !y.is_finite() || (x == 0.0 && y == 0.0) {
                return None;
            }
            // On X11, StatusNotifierItem::Activate(x, y) carries X11 root pixels, which map via
            // each monitor's physical span and scale factor into GDK coordinates. On Wayland,
            // panels using `xdg_output` typically emit compositor logical coordinates matching
            // GDK geometry directly, while some hosts emit physical output pixels.
            let passes: &[bool] = match backend {
                LinuxDisplayBackend::X11 => &[true, false],
                LinuxDisplayBackend::Wayland | LinuxDisplayBackend::Unknown => &[false, true],
            };

            for &tolerance in &[0.0, 32.0] {
                for &as_physical in passes {
                    for (idx, (monitor, span)) in monitors.iter().zip(spans.iter()).enumerate() {
                        if as_physical {
                            if point_in_physical_span(span, x, y, tolerance) {
                                let (gx, gy) = physical_to_gdk_on_monitor(monitor, span, x, y);
                                return Some((
                                    idx,
                                    *monitor,
                                    GdkResolvedAnchor::Point { x: gx, y: gy },
                                ));
                            }
                        } else if point_in_gdk_geometry(monitor, x, y, tolerance) {
                            return Some((idx, *monitor, GdkResolvedAnchor::Point { x, y }));
                        }
                    }
                }
            }
            None
        }
    }
}

/// Computes the compact native GDK logical `(width, height)` for the tray status popup on
/// `monitor`, fitting up to 3 status rows without carrying over `main`'s `500px` minimum height.
#[cfg(any(target_os = "linux", test))]
pub fn compute_linux_popover_gdk_size(
    monitor: &LinuxMonitorWorkarea,
    desired_logical_size: (f64, f64),
) -> (i32, i32) {
    let edge_margin = 12;
    let work_w = monitor.workarea_width.max(1);
    let work_h = monitor.workarea_height.max(1);
    let avail_w = (work_w - 2 * edge_margin).max(160);
    let avail_h = (work_h - 2 * edge_margin).max(160);

    let desired_w = if desired_logical_size.0.is_finite() && desired_logical_size.0 > 0.0 {
        desired_logical_size.0
    } else {
        TRAY_POPOVER_WIDTH
    }
    .max(TRAY_POPOVER_MIN_WIDTH)
    .round() as i32;

    let desired_h = if desired_logical_size.1.is_finite() && desired_logical_size.1 > 0.0 {
        desired_logical_size.1
    } else {
        TRAY_POPOVER_HEIGHT
    }
    .max(TRAY_POPOVER_MIN_HEIGHT)
    .round() as i32;

    (desired_w.min(avail_w), desired_h.min(avail_h))
}

#[cfg(any(target_os = "linux", test))]
fn compute_wayland_layer_shell_anchored_placement(
    target_monitor: &LinuxMonitorWorkarea,
    target_span: &PhysicalSpan,
    logical_size: (i32, i32),
    anchor: GdkResolvedAnchor,
    layer: WaylandShellLayer,
    keyboard_policy: WaylandKeyboardPolicy,
) -> (WaylandLayerShellPlacement, (i32, i32)) {
    let (pop_w, pop_h) = logical_size;
    let mon_w = target_monitor.geometry_width.max(1);
    let mon_h = target_monitor.geometry_height.max(1);
    let edge_margin = 12;
    // Flush (`0`) against the panel's exclusive zone so Wayland compositors (`exclusive_zone = 0`)
    // place the popup immediately beneath/above/beside the bar with zero exposed workspace gap
    // where `focus_follows_mouse yes` could steal focus during pointer transit from the icon.
    let panel_gap = 0;
    let panel_edge_band = 56.0;

    let (local_cx, local_top, local_bottom) = match anchor {
        GdkResolvedAnchor::Rect {
            x,
            y,
            width,
            height,
        } => (
            (x + width / 2.0) - (target_monitor.geometry_x as f64),
            y - (target_monitor.geometry_y as f64),
            (y + height) - (target_monitor.geometry_y as f64),
        ),
        GdkResolvedAnchor::Point { x, y } => {
            let lx = x - (target_monitor.geometry_x as f64);
            let ly = y - (target_monitor.geometry_y as f64);
            (lx, ly, ly)
        }
    };

    let is_top_panel = local_top <= panel_edge_band;
    let is_bottom_panel = !is_top_panel && local_bottom >= (mon_h as f64) - panel_edge_band;
    let is_left_side_panel = !is_top_panel && !is_bottom_panel && local_cx <= panel_edge_band;
    let is_right_side_panel = !is_top_panel
        && !is_bottom_panel
        && !is_left_side_panel
        && local_cx >= (mon_w as f64) - panel_edge_band;

    let clamped_left = if mon_w >= pop_w + 2 * edge_margin {
        (local_cx.round() as i32 - pop_w / 2).clamp(edge_margin, mon_w - pop_w - edge_margin)
    } else {
        ((mon_w - pop_w).max(0)) / 2
    };
    let clamped_right = (mon_w - (clamped_left + pop_w)).max(0);

    let (anchor_left, anchor_right, margin_left, margin_right, est_local_x) = if is_left_side_panel
    {
        // With `exclusive_zone = 0`, `Edge::Left` already starts immediately inside any left dock.
        (true, false, panel_gap, 0, panel_gap)
    } else if is_right_side_panel {
        // `Edge::Right` starts immediately inside any right dock.
        (
            false,
            true,
            0,
            panel_gap,
            (mon_w - pop_w - panel_gap).max(0),
        )
    } else if local_cx > (mon_w as f64) / 2.0 {
        (false, true, 0, clamped_right, clamped_left)
    } else {
        (true, false, clamped_left, 0, clamped_left)
    };

    let max_clamped_top = (mon_h - pop_h - edge_margin).max(edge_margin);
    let (anchor_top, anchor_bottom, margin_top, margin_bottom, est_local_y) = if is_top_panel {
        // `exclusive_zone = 0` shifts `Edge::Top` past top-panel exclusive zones automatically.
        (true, false, panel_gap, 0, panel_gap)
    } else if is_bottom_panel {
        // `exclusive_zone = 0` shifts `Edge::Bottom` above bottom-panel exclusive zones.
        (
            false,
            true,
            0,
            panel_gap,
            (mon_h - pop_h - panel_gap).max(0),
        )
    } else if is_left_side_panel || is_right_side_panel {
        let centered_top =
            (local_top.round() as i32 - pop_h / 2).clamp(edge_margin, max_clamped_top);
        if local_top <= (mon_h as f64) / 2.0 {
            (true, false, centered_top, 0, centered_top)
        } else {
            let bot_margin = (mon_h - (centered_top + pop_h)).max(edge_margin);
            (false, true, 0, bot_margin, centered_top)
        }
    } else if local_top <= (mon_h as f64) / 2.0 {
        let below_top = (local_bottom.round() as i32 + 6).clamp(edge_margin, max_clamped_top);
        (true, false, below_top, 0, below_top)
    } else {
        let above_top = (local_top.round() as i32 - pop_h - 6).clamp(edge_margin, max_clamped_top);
        let bot_margin = (mon_h - (above_top + pop_h)).max(edge_margin);
        (false, true, 0, bot_margin, above_top)
    };

    let est_gdk_x = target_monitor.geometry_x.saturating_add(est_local_x);
    let est_gdk_y = target_monitor.geometry_y.saturating_add(est_local_y);
    let placement = WaylandLayerShellPlacement {
        monitor_index: target_monitor.monitor_index,
        layer,
        anchor_top,
        anchor_bottom,
        anchor_left,
        anchor_right,
        margin_top,
        margin_bottom,
        margin_left,
        margin_right,
        exclusive_zone: 0,
        keyboard_policy,
        estimated_gdk_bounds: (est_gdk_x, est_gdk_y, pop_w, pop_h),
    };
    let phys_pos = gdk_to_physical_on_monitor(target_monitor, target_span, est_gdk_x, est_gdk_y);

    (placement, phys_pos)
}

#[cfg(any(target_os = "linux", test))]
fn plan_wayland_outside_click_backdrops(
    monitors: &[LinuxMonitorWorkarea],
) -> Vec<WaylandOutsideClickBackdropSpec> {
    let mut backdrops = Vec::with_capacity(monitors.len().min(MAX_WAYLAND_OUTSIDE_CLICK_MONITORS));
    for monitor in monitors {
        if backdrops.len() >= MAX_WAYLAND_OUTSIDE_CLICK_MONITORS {
            break;
        }
        if let Some(spec) = WaylandOutsideClickBackdropSpec::for_monitor(monitor) {
            if !backdrops
                .iter()
                .any(|existing: &WaylandOutsideClickBackdropSpec| {
                    existing.monitor_index == spec.monitor_index
                })
            {
                backdrops.push(spec);
            }
        }
    }
    backdrops
}

#[cfg(any(target_os = "linux", test))]
fn select_unanchored_fallback_monitor(
    monitors: &[LinuxMonitorWorkarea],
    preferred_monitor_index: Option<usize>,
) -> (usize, LinuxMonitorWorkarea) {
    if let Some(pref_idx) = preferred_monitor_index {
        if let Some((slice_idx, found)) = monitors
            .iter()
            .copied()
            .enumerate()
            .find(|(i, m)| m.monitor_index == Some(pref_idx) || *i == pref_idx)
        {
            return (slice_idx, found);
        }
    }
    if let Some((slice_idx, primary)) = monitors
        .iter()
        .copied()
        .enumerate()
        .find(|(_, m)| m.is_primary)
    {
        return (slice_idx, primary);
    }
    monitors
        .first()
        .copied()
        .map(|m| (0, m))
        .unwrap_or_else(|| (0, LinuxMonitorWorkarea::default()))
}

/// Pure planner for Linux tray popup monitor selection, compact 3-row sizing, Wayland
/// `zwlr_layer_shell_v1` anchoring, X11 workarea clamping, and truthful capability/fallback
/// reporting.
#[cfg(any(target_os = "linux", test))]
pub fn plan_linux_popover_placement(
    backend: LinuxDisplayBackend,
    wayland_support: WaylandLayerShellSupport,
    monitors: &[LinuxMonitorWorkarea],
    raw_anchor: Option<RawTrayAnchor>,
    preferred_fallback_monitor_index: Option<usize>,
    desired_logical_size: (f64, f64),
) -> LinuxPopoverPlan {
    let default_monitor = LinuxMonitorWorkarea::default();
    let effective_monitors: &[LinuxMonitorWorkarea] = if monitors.is_empty() {
        std::slice::from_ref(&default_monitor)
    } else {
        monitors
    };
    let spans = derive_physical_spans(effective_monitors);

    let resolved = raw_anchor.and_then(|anchor| {
        anchor
            .tray_rect
            .and_then(|rect| {
                resolve_rect_on_linux_monitors(backend, effective_monitors, &spans, rect)
            })
            .or_else(|| {
                anchor.click_position.and_then(|pt| {
                    resolve_point_on_linux_monitors(backend, effective_monitors, &spans, pt)
                })
            })
    });

    let (target_slice_idx, target_monitor, gdk_anchor) = match resolved {
        Some((idx, mon, anchor)) => (idx, mon, Some(anchor)),
        None => {
            let (idx, mon) = select_unanchored_fallback_monitor(
                effective_monitors,
                preferred_fallback_monitor_index,
            );
            (idx, mon, None)
        }
    };
    let target_span = spans
        .get(target_slice_idx)
        .copied()
        .unwrap_or(PhysicalSpan {
            x: 0.0,
            y: 0.0,
            width: target_monitor.geometry_width.max(1) as f64,
            height: target_monitor.geometry_height.max(1) as f64,
        });

    let scale = sanitize_scale(target_monitor.scale_factor);
    let logical_size = compute_linux_popover_gdk_size(&target_monitor, desired_logical_size);
    let (pop_w, pop_h) = logical_size;

    match backend {
        LinuxDisplayBackend::Wayland => match wayland_support {
            WaylandLayerShellSupport::Supported { protocol_version } => {
                let (shell_layer, keyboard_policy, outside_click_backdrops, capability_notice) =
                    if protocol_version >= 4 {
                        (
                            WaylandShellLayer::Overlay,
                            WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                            plan_wayland_outside_click_backdrops(monitors),
                            None,
                        )
                    } else {
                        (
                            WaylandShellLayer::Top,
                            WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick,
                            Vec::new(),
                            Some(WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE.to_string()),
                        )
                    };

                match gdk_anchor {
                    Some(anchor) => {
                        let (placement, (phys_x, phys_y)) =
                            compute_wayland_layer_shell_anchored_placement(
                                &target_monitor,
                                &target_span,
                                logical_size,
                                anchor,
                                shell_layer,
                                keyboard_policy,
                            );
                        LinuxPopoverPlan {
                            backend,
                            target_monitor,
                            logical_size,
                            gdk_position: None,
                            layer_shell: Some(placement),
                            outside_click_backdrops,
                            capability_notice,
                            outcome: PopoverPlacementOutcome::IconAnchored {
                                x: phys_x,
                                y: phys_y,
                                scale_factor: scale,
                            },
                        }
                    }
                    None => {
                        let edge_margin = 12;
                        let fallback_local_x =
                            (target_monitor.geometry_width - pop_w - edge_margin).max(edge_margin);
                        let est_gdk_x = target_monitor.geometry_x.saturating_add(fallback_local_x);
                        let est_gdk_y = target_monitor.geometry_y.saturating_add(edge_margin);
                        let (phys_x, phys_y) = gdk_to_physical_on_monitor(
                            &target_monitor,
                            &target_span,
                            est_gdk_x,
                            est_gdk_y,
                        );
                        // Only pin `monitor_index` on missing-coordinate fallback if the caller
                        // supplied an explicit active-monitor hint; otherwise let the compositor
                        // place the fallback corner on the user's currently active output rather
                        // than guessing `monitors[0]`.
                        let fallback_monitor_pin =
                            preferred_fallback_monitor_index.and(target_monitor.monitor_index);
                        let placement = WaylandLayerShellPlacement {
                            monitor_index: fallback_monitor_pin,
                            layer: shell_layer,
                            anchor_top: true,
                            anchor_bottom: false,
                            anchor_left: false,
                            anchor_right: true,
                            margin_top: edge_margin,
                            margin_bottom: 0,
                            margin_left: 0,
                            margin_right: edge_margin,
                            exclusive_zone: 0,
                            keyboard_policy,
                            estimated_gdk_bounds: (est_gdk_x, est_gdk_y, pop_w, pop_h),
                        };
                        LinuxPopoverPlan {
                            backend,
                            target_monitor,
                            logical_size,
                            gdk_position: None,
                            layer_shell: Some(placement),
                            outside_click_backdrops,
                            capability_notice,
                            outcome: PopoverPlacementOutcome::FallbackMonitorCorner {
                                x: phys_x,
                                y: phys_y,
                                scale_factor: scale,
                                reason: UnanchoredPlacementReason::MissingTrayCoordinates,
                                notice: MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string(),
                            },
                        }
                    }
                }
            }
            WaylandLayerShellSupport::Unsupported(unsupported_reason) => {
                let has_activation_coordinates = gdk_anchor.is_some();
                let (reason, notice) = match unsupported_reason {
                    WaylandLayerShellUnsupportedReason::WindowAlreadyRealized => (
                        UnanchoredPlacementReason::WaylandCompositorManaged,
                        WAYLAND_REALIZED_FALLBACK_NOTICE.to_string(),
                    ),
                    WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported => {
                        if has_activation_coordinates {
                            (
                                UnanchoredPlacementReason::WaylandCompositorManaged,
                                WAYLAND_COMPOSITOR_PLACEMENT_NOTICE.to_string(),
                            )
                        } else {
                            (
                                UnanchoredPlacementReason::WaylandMissingCoordinatesAndCompositorSupport,
                                WAYLAND_MISSING_COORDS_NOTICE.to_string(),
                            )
                        }
                    }
                };
                LinuxPopoverPlan {
                    backend,
                    target_monitor,
                    logical_size,
                    gdk_position: None,
                    layer_shell: None,
                    outside_click_backdrops: Vec::new(),
                    capability_notice: None,
                    outcome: PopoverPlacementOutcome::CompositorManaged {
                        scale_factor: scale,
                        has_activation_coordinates,
                        reason,
                        notice,
                    },
                }
            }
        },
        LinuxDisplayBackend::X11 => {
            let margin = 12;
            let anchor_gap = 6;
            let work_left = target_monitor.workarea_x;
            let work_top = target_monitor.workarea_y;
            let work_right = work_left.saturating_add(target_monitor.workarea_width);
            let work_bottom = work_top.saturating_add(target_monitor.workarea_height);
            let geom_mid_y = target_monitor
                .geometry_y
                .saturating_add(target_monitor.geometry_height / 2);

            let min_x = work_left.saturating_add(margin);
            let max_x = work_right
                .saturating_sub(pop_w)
                .saturating_sub(margin)
                .max(min_x);
            let min_y = work_top.saturating_add(margin);
            let max_y = work_bottom
                .saturating_sub(pop_h)
                .saturating_sub(margin)
                .max(min_y);

            match gdk_anchor {
                Some(anchor) => {
                    let (raw_x, raw_y) = match anchor {
                        GdkResolvedAnchor::Rect {
                            x,
                            y,
                            width,
                            height,
                        } => {
                            let center_x = (x + width / 2.0).round() as i32;
                            let top_y = y.round() as i32;
                            let bottom_y = (y + height).round() as i32;
                            let px = center_x.saturating_sub(pop_w / 2);
                            let py = if top_y <= geom_mid_y {
                                bottom_y.saturating_add(anchor_gap)
                            } else {
                                top_y.saturating_sub(pop_h).saturating_sub(anchor_gap)
                            };
                            (px, py)
                        }
                        GdkResolvedAnchor::Point { x, y } => {
                            let click_x = x.round() as i32;
                            let click_y = y.round() as i32;
                            let px = click_x.saturating_sub(pop_w / 2);
                            let py = if click_y <= geom_mid_y {
                                click_y.saturating_add(anchor_gap)
                            } else {
                                click_y.saturating_sub(pop_h).saturating_sub(anchor_gap)
                            };
                            (px, py)
                        }
                    };
                    let gdk_x = raw_x.clamp(min_x, max_x);
                    let gdk_y = raw_y.clamp(min_y, max_y);
                    let (phys_x, phys_y) =
                        gdk_to_physical_on_monitor(&target_monitor, &target_span, gdk_x, gdk_y);
                    LinuxPopoverPlan {
                        backend,
                        target_monitor,
                        logical_size,
                        gdk_position: Some((gdk_x, gdk_y)),
                        layer_shell: None,
                        outside_click_backdrops: Vec::new(),
                        capability_notice: None,
                        outcome: PopoverPlacementOutcome::IconAnchored {
                            x: phys_x,
                            y: phys_y,
                            scale_factor: scale,
                        },
                    }
                }
                None => {
                    let gdk_x = max_x;
                    let panel_reserve = if target_monitor.workarea_y > target_monitor.geometry_y {
                        0
                    } else {
                        36
                    };
                    let gdk_y = work_top
                        .saturating_add(margin)
                        .saturating_add(panel_reserve)
                        .clamp(min_y, max_y);
                    let (phys_x, phys_y) =
                        gdk_to_physical_on_monitor(&target_monitor, &target_span, gdk_x, gdk_y);
                    LinuxPopoverPlan {
                        backend,
                        target_monitor,
                        logical_size,
                        gdk_position: Some((gdk_x, gdk_y)),
                        layer_shell: None,
                        outside_click_backdrops: Vec::new(),
                        capability_notice: None,
                        outcome: PopoverPlacementOutcome::FallbackMonitorCorner {
                            x: phys_x,
                            y: phys_y,
                            scale_factor: scale,
                            reason: UnanchoredPlacementReason::MissingTrayCoordinates,
                            notice: MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string(),
                        },
                    }
                }
            }
        }
        LinuxDisplayBackend::Unknown => LinuxPopoverPlan {
            backend,
            target_monitor,
            logical_size,
            gdk_position: None,
            layer_shell: None,
            outside_click_backdrops: Vec::new(),
            capability_notice: None,
            outcome: PopoverPlacementOutcome::CompositorManaged {
                scale_factor: scale,
                has_activation_coordinates: gdk_anchor.is_some(),
                reason: UnanchoredPlacementReason::BackendUnsupported,
                notice: UNKNOWN_LINUX_BACKEND_PLACEMENT_NOTICE.to_string(),
            },
        },
    }
}

#[cfg(target_os = "linux")]
struct LinuxTrayRuntimeState {
    popover_signals_attached: bool,
    main_click_guard_attached: bool,
    layer_monitor_pinned: bool,
    focus_tracker: WaylandLayerFocusTracker,
}

#[cfg(target_os = "linux")]
impl LinuxTrayRuntimeState {
    fn new() -> Self {
        Self {
            popover_signals_attached: false,
            main_click_guard_attached: false,
            layer_monitor_pinned: false,
            focus_tracker: WaylandLayerFocusTracker::new(),
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_runtime_state() -> &'static Mutex<LinuxTrayRuntimeState> {
    static STATE: OnceLock<Mutex<LinuxTrayRuntimeState>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(LinuxTrayRuntimeState::new()))
}

/// Returns `true` when GTK is initialized and the caller is executing on the GTK main thread.
#[cfg(target_os = "linux")]
pub fn is_linux_gtk_main_thread() -> bool {
    gtk::is_initialized_main_thread()
}

/// Builds an explicit empty `cairo::Region` (`num_rectangles() == 0`) for pre-map realization and
/// fail-open input-shape cleanup. `GdkWindow::input_shape_combine_region` (`gdk 0.18.2`) takes
/// `&cairo::Region`, and passing `&empty` applies an explicit zero-rectangle region (whereas an
/// unshaped/null GDK input shape defaults to the full surface input region and is NOT fail-open).
#[cfg(target_os = "linux")]
fn create_fail_open_empty_input_region() -> gtk::cairo::Region {
    let empty = gtk::cairo::Region::create();
    debug_assert!(empty.is_empty());
    debug_assert_eq!(empty.num_rectangles(), 0);
    empty
}

/// Builds a validated surface-local `(0, 0, width, height)` `cairo::Region` for a configured
/// Wayland outside-click surface.
#[cfg(target_os = "linux")]
fn create_bounded_surface_input_region(width: i32, height: i32) -> Option<gtk::cairo::Region> {
    if width <= 0 || height <= 0 {
        return None;
    }
    let rect = gtk::cairo::RectangleInt::new(0, 0, width, height);
    let region = gtk::cairo::Region::create_rectangle(&rect);
    if region.is_empty() || region.num_rectangles() != 1 {
        return None;
    }
    Some(region)
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct GtkBackdropSignalHandlers {
    released: bool,
    draw_handler: Option<glib::SignalHandlerId>,
    button_press_handler: Option<glib::SignalHandlerId>,
    size_allocate_handler: Option<glib::SignalHandlerId>,
}

#[cfg(target_os = "linux")]
#[derive(Clone)]
struct GtkBackdropSurface {
    surface_id: u64,
    generation: u64,
    window: gtk::Window,
    handlers: Rc<RefCell<GtkBackdropSignalHandlers>>,
}

#[cfg(target_os = "linux")]
fn release_gtk_backdrop_surface_fail_open(surface: GtkBackdropSurface) {
    // 1. Fail-open input shape first: explicit empty `cairo::Region` (`&empty_region` with 0
    //    rectangles; an unshaped/null GDK input region defaults to the full surface input region
    //    and is NOT fail-open).
    let empty_region = create_fail_open_empty_input_region();
    if let Some(gdk_win) = surface.window.window() {
        gdk_win.input_shape_combine_region(&empty_region, 0, 0);
    }

    let (already_released, button_id, size_alloc_id, draw_id) = {
        let mut handlers = surface.handlers.borrow_mut();
        let was_released = handlers.released;
        handlers.released = true;
        (
            was_released,
            handlers.button_press_handler.take(),
            handlers.size_allocate_handler.take(),
            handlers.draw_handler.take(),
        )
    };
    if already_released {
        return;
    }

    // 2. Disconnect finite per-open signal handlers immediately so captured handles drop without
    //    waiting on deferred GTK idle destruction.
    if let Some(id) = button_id {
        surface.window.disconnect(id);
    }
    if let Some(id) = size_alloc_id {
        surface.window.disconnect(id);
    }
    if let Some(id) = draw_id {
        surface.window.disconnect(id);
    }

    // 3. Unmap and destroy the realized top-level GTK window safely.
    surface.window.hide();
    surface.window.set_deletable(true);
    surface.window.close();
}

/// Releases an implicit GDK pointer grab on `master_pointer` at `event_time` via public
/// `gdk_device_ungrab`.
///
/// On GTK3/Wayland (`gdk/wayland/gdkdevice-wayland.c`), `wl_pointer.button` press sets an
/// implicit master-pointer grab (`_gdk_display_add_device_grab(..., implicit = TRUE)`) bound to
/// the pressed `GdkWindow`. Synchronously hiding/closing that backdrop `GdkWindow` on button press
/// without ungrabbing leaves the implicit grab active, causing GDK's client-side grab routing to
/// deliver subsequent pointer button events (even after `wl_pointer.enter` on the reopened popup)
/// to the destroyed backdrop window. Public `gdk_device_ungrab` clears the Wayland implicit grab
/// serial range (`gdk_wayland_device_ungrab`) and updates GDK's display grab state.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn ungrab_gdk_master_pointer_device(master_pointer: &gdk::Device, event_time: u32) {
    use gtk::glib::translate::ToGlibPtr;
    debug_assert!(gtk::is_initialized_main_thread());
    debug_assert_eq!(master_pointer.device_type(), gdk::DeviceType::Master);
    debug_assert_ne!(master_pointer.source(), gdk::InputSource::Keyboard);
    // SAFETY:
    // 1. Caller verified `gtk::is_initialized_main_thread()`, guaranteeing GTK/GDK initialization
    //    and exclusive main-thread access to GDK device state.
    // 2. `master_pointer` is a live owned `gdk::Device` GObject reference validated as a
    //    `gdk::DeviceType::Master` pointer equal to its `gdk::Seat::pointer()` on the active
    //    `gdk::Display`; `to_glib_none().0` provides a valid non-null `*mut GdkDevice` whose
    //    `Stash` temporary lives for the full synchronous C call.
    // 3. All `RefCell` borrows and `Mutex` guards are dropped prior to calling this helper, so
    //    any synchronous crossing/grab-broken callbacks emitted by GDK cannot alias Rust borrows.
    unsafe {
        gdk::ffi::gdk_device_ungrab(master_pointer.to_glib_none().0, event_time);
    }
}

#[cfg(target_os = "linux")]
struct GtkOutsideClickBackend {
    app: AppHandle,
    display: gdk::Display,
}

#[cfg(target_os = "linux")]
impl GtkOutsideClickBackend {
    fn new(app: AppHandle, display: gdk::Display) -> Self {
        Self { app, display }
    }
}

#[cfg(target_os = "linux")]
impl OutsideClickSurfaceBackend for GtkOutsideClickBackend {
    type SurfaceHandle = GtkBackdropSurface;

    fn has_rgba_visual(&self) -> bool {
        self.display.default_screen().rgba_visual().is_some()
    }

    fn is_popup_visible_for_generation(&self, generation: u64) -> bool {
        if !gtk::is_initialized_main_thread() {
            return false;
        }
        if self
            .app
            .try_state::<TrayState>()
            .is_some_and(|tray| tray.is_explicit_quit_requested())
        {
            return false;
        }
        let tracker_matches = {
            let guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.focus_tracker.is_mapped()
                && guard.focus_tracker.current_generation() == generation
        };
        if !tracker_matches {
            return false;
        }
        let Some(popover) = self.app.get_webview_window(TRAY_WINDOW_LABEL) else {
            return false;
        };
        let Ok(gtk_win) = popover.gtk_window() else {
            return false;
        };
        gtk_win.is_visible() && gtk_win.is_mapped() && gtk_win.is_layer_window()
    }

    fn create_realized_empty_surface(
        &self,
        surface_id: u64,
        generation: u64,
        spec: &WaylandOutsideClickBackdropSpec,
    ) -> Result<Self::SurfaceHandle, WaylandBackdropUnavailableReason> {
        if !gtk::is_initialized_main_thread()
            || !gtk_layer_shell::is_supported()
            || gtk_layer_shell::protocol_version() < 4
            || !spec.validate_invariants()
        {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        }
        let Some(gdk_mon) = self.display.monitor(spec.monitor_index as i32) else {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        };
        let rgba_visual = if spec.transparent_rgba {
            Some(
                self.display
                    .default_screen()
                    .rgba_visual()
                    .ok_or(WaylandBackdropUnavailableReason::TransparentVisualUnavailable)?,
            )
        } else {
            None
        };

        let backdrop_win = gtk::Window::new(gtk::WindowType::Toplevel);
        backdrop_win.init_layer_shell();
        if !backdrop_win.is_layer_window() {
            backdrop_win.set_deletable(true);
            backdrop_win.close();
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        }

        backdrop_win.set_namespace(LINUX_TRAY_BACKDROP_WINDOW_ROLE);
        backdrop_win.set_role(LINUX_TRAY_BACKDROP_WINDOW_ROLE);
        backdrop_win.set_layer(spec.layer.to_gtk_layer());
        backdrop_win.set_exclusive_zone(spec.exclusive_zone);
        if spec.keyboard_interactivity_none {
            backdrop_win.set_keyboard_mode(KeyboardMode::None);
        }
        backdrop_win.set_monitor(&gdk_mon);

        // Stretch across this monitor's usable workspace (`exclusive_zone = 0` on `Layer::Top`
        // automatically respects real panel exclusive zones without guessing panel bounds).
        backdrop_win.set_anchor(Edge::Top, true);
        backdrop_win.set_anchor(Edge::Bottom, true);
        backdrop_win.set_anchor(Edge::Left, true);
        backdrop_win.set_anchor(Edge::Right, true);
        backdrop_win.set_layer_shell_margin(Edge::Top, 0);
        backdrop_win.set_layer_shell_margin(Edge::Bottom, 0);
        backdrop_win.set_layer_shell_margin(Edge::Left, 0);
        backdrop_win.set_layer_shell_margin(Edge::Right, 0);

        backdrop_win.set_decorated(false);
        backdrop_win.set_skip_taskbar_hint(true);
        backdrop_win.set_skip_pager_hint(true);
        backdrop_win.set_accept_focus(false);
        backdrop_win.set_focus_on_map(false);
        backdrop_win.set_app_paintable(true);
        backdrop_win.set_default_size(
            BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM,
            BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM,
        );
        if let Some(visual) = &rgba_visual {
            backdrop_win.set_visual(Some(visual));
        }
        backdrop_win.add_events(gdk::EventMask::BUTTON_PRESS_MASK);

        let draw_handler = backdrop_win.connect_draw(|_win, cr| {
            cr.set_source_rgba(0.0, 0.0, 0.0, 0.0);
            cr.set_operator(gtk::cairo::Operator::Source);
            let _ = cr.paint();
            glib::Propagation::Stop
        });

        let app_for_click = self.app.clone();
        let display_for_click = self.display.clone();
        let button_press_handler =
            backdrop_win.connect_button_press_event(move |callback_win, event| {
                handle_gtk_backdrop_button_press_event(
                    &app_for_click,
                    &display_for_click,
                    surface_id,
                    generation,
                    callback_win,
                    event,
                )
            });

        let app_for_alloc = self.app.clone();
        let display_for_alloc = self.display.clone();
        let size_allocate_handler = backdrop_win.connect_size_allocate(move |_win, alloc| {
            handle_gtk_backdrop_size_allocate(
                &app_for_alloc,
                &display_for_alloc,
                surface_id,
                generation,
                alloc.width(),
                alloc.height(),
            );
        });

        let handlers = Rc::new(RefCell::new(GtkBackdropSignalHandlers {
            released: false,
            draw_handler: Some(draw_handler),
            button_press_handler: Some(button_press_handler),
            size_allocate_handler: Some(size_allocate_handler),
        }));
        let surface = GtkBackdropSurface {
            surface_id,
            generation,
            window: backdrop_win,
            handlers,
        };

        // Realize and apply an EXPLICIT EMPTY `cairo::Region` (`&empty_region`, 0 rectangles —
        // never left unshaped/null, which defaults to the full surface region) strictly BEFORE
        // the window's first map (`show()`).
        surface.window.realize();
        if !surface.window.is_realized()
            || surface.window.is_mapped()
            || !surface.window.is_layer_window()
        {
            release_gtk_backdrop_surface_fail_open(surface);
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        }
        let Some(gdk_win) = surface.window.window() else {
            release_gtk_backdrop_surface_fail_open(surface);
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        };
        let empty_region = create_fail_open_empty_input_region();
        gdk_win.input_shape_combine_region(&empty_region, 0, 0);

        Ok(surface)
    }

    fn map_empty_surface(
        &self,
        surface: &Self::SurfaceHandle,
    ) -> Result<(), WaylandBackdropUnavailableReason> {
        if surface.handlers.borrow().released {
            return Ok(());
        }
        let Some(gdk_win) = surface.window.window() else {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        };
        let empty_region = create_fail_open_empty_input_region();
        gdk_win.input_shape_combine_region(&empty_region, 0, 0);

        surface.window.show();

        // If `show()` synchronously reentered `hide`, `unmap`, `monitor-removed`, or a newer
        // generation, the coordinator has already released `surface` fail-open.
        if surface.handlers.borrow().released {
            return Ok(());
        }
        if !surface.window.is_mapped()
            || !surface.window.is_layer_window()
            || surface.window.window().is_none()
        {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        }

        let alloc_w = surface.window.allocated_width();
        let alloc_h = surface.window.allocated_height();
        if alloc_w > BACKDROP_MAX_UNCONFIGURED_DIM || alloc_h > BACKDROP_MAX_UNCONFIGURED_DIM {
            handle_gtk_backdrop_size_allocate(
                &self.app,
                &self.display,
                surface.surface_id,
                surface.generation,
                alloc_w,
                alloc_h,
            );
        }
        Ok(())
    }

    fn enable_bounded_input_region(
        &self,
        surface: &Self::SurfaceHandle,
        width: i32,
        height: i32,
    ) -> Result<(), WaylandBackdropUnavailableReason> {
        if surface.handlers.borrow().released
            || !surface.window.is_realized()
            || !surface.window.is_mapped()
            || !surface.window.is_visible()
            || !surface.window.is_layer_window()
        {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        }
        let Some(gdk_win) = surface.window.window() else {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        };
        let Some(bounded_region) = create_bounded_surface_input_region(width, height) else {
            return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
        };
        gdk_win.input_shape_combine_region(&bounded_region, 0, 0);
        surface.window.queue_draw();
        Ok(())
    }

    fn schedule_configure_timeout(&self, generation: u64, admission_seq: u64, timeout: Duration) {
        let app_for_timeout = self.app.clone();
        let display_for_timeout = self.display.clone();
        let _ = glib::timeout_add_local_once(timeout, move || {
            handle_gtk_backdrop_configure_timeout(
                &app_for_timeout,
                &display_for_timeout,
                generation,
                admission_seq,
            );
        });
    }

    fn schedule_pending_map_timeout(
        &self,
        generation: u64,
        attempt_ticket: u64,
        timeout: Duration,
    ) {
        let app_for_timeout = self.app.clone();
        let display_for_timeout = self.display.clone();
        let _ = glib::timeout_add_local_once(timeout, move || {
            handle_gtk_popup_pending_map_timeout(
                &app_for_timeout,
                &display_for_timeout,
                generation,
                attempt_ticket,
            );
        });
    }

    fn schedule_pending_map_idle_probe(&self, generation: u64, attempt_ticket: u64) {
        let app_for_idle = self.app.clone();
        let display_for_idle = self.display.clone();
        let _ = glib::idle_add_local_once(move || {
            handle_gtk_popup_pending_map_progress(
                &app_for_idle,
                &display_for_idle,
                generation,
                attempt_ticket,
                PendingMapTrigger::IdleProbe,
            );
        });
    }

    fn release_surface_fail_open(&self, surface: Self::SurfaceHandle) {
        release_gtk_backdrop_surface_fail_open(surface);
    }
}

#[cfg(target_os = "linux")]
impl PopupMapLifecycleBackend for GtkOutsideClickBackend {
    fn begin_tracker_attempt<FReserve>(
        &self,
        kind: PopupAttemptKind,
        arm_layer_map: bool,
        reserve_ticket_fn: FReserve,
    ) -> Option<(u64, u64)>
    where
        FReserve: FnOnce() -> u64,
    {
        let explicit_quit = self
            .app
            .try_state::<TrayState>()
            .is_some_and(|tray| tray.is_explicit_quit_requested());
        match kind {
            PopupAttemptKind::Open => {
                let ticket = reserve_ticket_fn();
                let mut guard = linux_runtime_state()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let generation = guard
                    .focus_tracker
                    .begin_open_attempt(ticket, arm_layer_map);
                Some((generation, ticket))
            }
            PopupAttemptKind::Refresh => {
                if explicit_quit {
                    return None;
                }
                let eligible = linux_runtime_state()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .focus_tracker
                    .has_live_or_pending_open();
                if !eligible {
                    return None;
                }
                let ticket = reserve_ticket_fn();
                let mut guard = linux_runtime_state()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                guard
                    .focus_tracker
                    .begin_refresh_attempt(ticket, explicit_quit, arm_layer_map)
                    .map(|generation| (generation, ticket))
            }
        }
    }

    fn abort_tracker_attempt(&self, generation: u64, attempt_ticket: u64) {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .focus_tracker
            .abort_attempt_remap(generation, attempt_ticket);
    }

    fn disarm_expired_tracker_attempt(&self, generation: u64, attempt_ticket: u64) -> bool {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard
            .focus_tracker
            .disarm_expired_attempt_remap(generation, attempt_ticket)
    }

    fn complete_tracker_attempt_map(
        &self,
        generation: u64,
        attempt_ticket: u64,
        window_visible_and_mapped: bool,
    ) -> bool {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.focus_tracker.complete_attempt_map(
            generation,
            attempt_ticket,
            window_visible_and_mapped,
        )
    }

    fn schedule_on_demand_transition(
        &self,
        generation: u64,
        attempt_ticket: u64,
        keyboard_policy: WaylandKeyboardPolicy,
    ) {
        schedule_wayland_on_demand_transition(
            &self.app,
            generation,
            attempt_ticket,
            keyboard_policy,
        );
    }

    fn is_popup_content_hierarchy_settled(&self) -> bool {
        self.app
            .get_webview_window(TRAY_WINDOW_LABEL)
            .and_then(|popover| popover.gtk_window().ok())
            .is_some_and(|gtk_win| is_gtk_popup_hierarchy_map_and_allocation_settled(&gtk_win))
    }

    fn schedule_settled_popup_redraw_idle(&self, generation: u64, attempt_ticket: u64) {
        let app_for_idle = self.app.clone();
        let display_for_idle = self.display.clone();
        let _ = glib::idle_add_local_once(move || {
            if !gtk::is_initialized_main_thread() {
                return;
            }
            let backend = GtkOutsideClickBackend::new(app_for_idle, display_for_idle);
            LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
                drive_settled_popup_redraw_idle(cell, &backend, generation, attempt_ticket);
            });
        });
    }

    fn queue_settled_popup_redraw(&self) {
        if let Some(gtk_win) = self
            .app
            .get_webview_window(TRAY_WINDOW_LABEL)
            .and_then(|popover| popover.gtk_window().ok())
        {
            queue_draw_settled_gtk_popup_hierarchy(&gtk_win);
        }
    }

    fn sample_popup_state(&self) -> (WaylandLayerFocusTracker, NativePopupMapSample) {
        let explicit_quit_requested = self
            .app
            .try_state::<TrayState>()
            .is_some_and(|tray| tray.is_explicit_quit_requested());
        let (window_visible, window_mapped, is_layer_window) = self
            .app
            .get_webview_window(TRAY_WINDOW_LABEL)
            .and_then(|popover| popover.gtk_window().ok())
            .map(|gtk_win| {
                (
                    gtk_win.is_visible(),
                    gtk_win.is_mapped(),
                    gtk_win.is_layer_window(),
                )
            })
            .unwrap_or((false, false, false));
        let tracker = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .focus_tracker
            .clone();
        (
            tracker,
            NativePopupMapSample {
                window_visible,
                window_mapped,
                is_layer_window,
                explicit_quit_requested,
            },
        )
    }
}

#[cfg(target_os = "linux")]
impl PopupOutsideClickDismissBackend for GtkOutsideClickBackend {
    type ToplevelWindow = gtk::Window;
    type NativeWindow = gdk::Window;
    type NativeDisplay = gdk::Display;
    type PointerDevice = gdk::Device;

    fn inspect_tracked_surface_window(
        &self,
        surface: &Self::SurfaceHandle,
    ) -> Option<
        TrackedBackdropWindowIdentity<
            Self::ToplevelWindow,
            Self::NativeWindow,
            Self::NativeDisplay,
        >,
    > {
        if !gtk::is_initialized_main_thread() {
            return None;
        }
        let released = surface.handlers.borrow().released;
        let gdk_win = surface.window.window()?;
        let is_live = !released
            && surface.window.is_realized()
            && surface.window.is_mapped()
            && surface.window.is_visible()
            && surface.window.is_layer_window()
            && !gdk_win.is_destroyed()
            && gdk_win.is_visible();
        let display = gdk_win.display();
        Some(TrackedBackdropWindowIdentity {
            toplevel: surface.window.clone(),
            native_window: gdk_win,
            display,
            is_live,
        })
    }

    fn ungrab_master_pointer(&self, master_pointer: &Self::PointerDevice, event_time: u32) {
        ungrab_gdk_master_pointer_device(master_pointer, event_time);
    }

    fn hide_popup_for_dismissed_epoch(&self, generation: u64, attempt_ticket: u64) {
        hide_popover_window_if_linux_epoch_current(&self.app, generation, attempt_ticket);
    }
}

#[cfg(target_os = "linux")]
thread_local! {
    static LINUX_DISPLAY_MONITOR_GUARD_ATTACHED: Cell<bool> = const { Cell::new(false) };
    static LINUX_OUTSIDE_CLICK_STATE: RefCell<OutsideClickCoordinatorState<GtkBackdropSurface>> =
        RefCell::new(OutsideClickCoordinatorState::new());
}

#[cfg(target_os = "linux")]
fn publish_async_backdrop_capability_notice(app: &AppHandle, capability_notice: &str) {
    if let Some(tray) = app.try_state::<TrayState>() {
        let prefs = tray.record_runtime_capability_notice(capability_notice);
        let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &prefs);
        ensure_main_reachable_if_tray_unavailable(app, &tray);
    }
}

#[cfg(target_os = "linux")]
fn schedule_wayland_on_demand_transition(
    app: &AppHandle,
    generation: u64,
    attempt_ticket: u64,
    keyboard_policy: WaylandKeyboardPolicy,
) {
    if keyboard_policy != WaylandKeyboardPolicy::ExclusiveThenOnDemand
        || gtk_layer_shell::protocol_version() < 4
    {
        return;
    }
    let Some(gtk_win) = app
        .get_webview_window(TRAY_WINDOW_LABEL)
        .and_then(|popover| popover.gtk_window().ok())
    else {
        return;
    };
    if !gtk_win.is_layer_window() {
        return;
    }
    let app_for_timer = app.clone();
    let win_weak = gtk_win.downgrade();
    let _ = glib::timeout_add_local_once(WAYLAND_ON_DEMAND_TRANSITION_DELAY, move || {
        if !gtk::is_initialized_main_thread() {
            return;
        }
        let Some(w) = win_weak.upgrade() else {
            return;
        };
        let explicit_quit_requested = app_for_timer
            .try_state::<TrayState>()
            .is_some_and(|tray| tray.is_explicit_quit_requested());
        let sample = NativePopupMapSample {
            window_visible: w.is_visible(),
            window_mapped: w.is_mapped(),
            is_layer_window: w.is_layer_window(),
            explicit_quit_requested,
        };
        let coordinator_latest_ticket =
            LINUX_OUTSIDE_CLICK_STATE.with(|cell| cell.borrow().latest_attempt_ticket());
        let tracker = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .focus_tracker
            .clone();
        if should_apply_wayland_on_demand_transition(
            &tracker,
            coordinator_latest_ticket,
            generation,
            attempt_ticket,
            keyboard_policy,
            sample,
        ) {
            w.set_keyboard_mode(KeyboardMode::OnDemand);
        }
    });
}

#[cfg(target_os = "linux")]
fn is_gtk_widget_mapped_and_allocated(widget: &gtk::Widget) -> bool {
    widget.is_visible()
        && widget.is_mapped()
        && widget.allocated_width() > BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM
        && widget.allocated_height() > BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM
}

#[cfg(target_os = "linux")]
fn gtk_popup_content_root(gtk_win: &gtk::ApplicationWindow) -> Option<gtk::Widget> {
    let titlebar = gtk_win.titlebar();
    gtk_win
        .child()
        .filter(|child| titlebar.as_ref() != Some(child))
}

#[cfg(target_os = "linux")]
fn is_webkit_webview_widget(widget: &gtk::Widget) -> bool {
    is_webkit_webview_type_name(widget.type_().name())
}

#[cfg(target_os = "linux")]
fn is_gtk_popup_content_subtree_settled(widget: &gtk::Widget, saw_webkit: &mut bool) -> bool {
    if is_webkit_webview_widget(widget) {
        if !is_gtk_widget_mapped_and_allocated(widget) {
            return false;
        }
        *saw_webkit = true;
        return true;
    }
    if !widget.is_visible() {
        return true;
    }
    if !is_gtk_widget_mapped_and_allocated(widget) {
        return false;
    }
    if let Some(container) = widget.downcast_ref::<gtk::Container>() {
        for child in container.children() {
            if !is_gtk_popup_content_subtree_settled(&child, saw_webkit) {
                return false;
            }
        }
    }
    true
}

#[cfg(target_os = "linux")]
fn is_gtk_popup_hierarchy_map_and_allocation_settled(gtk_win: &gtk::ApplicationWindow) -> bool {
    if !is_gtk_widget_mapped_and_allocated(gtk_win.upcast_ref::<gtk::Widget>()) {
        return false;
    }
    let Some(content_root) = gtk_popup_content_root(gtk_win) else {
        return false;
    };
    if !is_gtk_widget_mapped_and_allocated(&content_root) {
        return false;
    }
    let mut saw_webkit = false;
    is_gtk_popup_content_subtree_settled(&content_root, &mut saw_webkit) && saw_webkit
}

#[cfg(target_os = "linux")]
fn queue_draw_settled_gtk_popup_hierarchy(gtk_win: &gtk::ApplicationWindow) {
    if gtk_win.is_visible()
        && gtk_win.is_mapped()
        && gtk_win.allocated_width() > 0
        && gtk_win.allocated_height() > 0
    {
        gtk_win.queue_draw();
    }
    if let Some(content_root) = gtk_popup_content_root(gtk_win) {
        for_widget_and_descendants(&content_root, &mut |widget| {
            if widget.is_visible()
                && widget.is_mapped()
                && widget.allocated_width() > 0
                && widget.allocated_height() > 0
            {
                widget.queue_draw();
            }
        });
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_popup_settled_redraw_signal(app: &AppHandle, gtk_win: &gtk::ApplicationWindow) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let has_pending = LINUX_OUTSIDE_CLICK_STATE
        .with(|cell| cell.borrow().pending_settled_redraw_epoch().is_some());
    if !has_pending {
        return;
    }
    let backend = GtkOutsideClickBackend::new(app.clone(), gtk_win.display());
    LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_settled_popup_redraw_signal(cell, &backend);
    });
}

#[cfg(target_os = "linux")]
fn publish_finalized_popup_map_notice(
    app: &AppHandle,
    effective_notice: Option<String>,
    capability_notice: Option<&str>,
) {
    if let Some(tray) = app.try_state::<TrayState>() {
        let before = tray.preferences_response();
        tray.set_placement_notice(effective_notice);
        let after = tray.preferences_response();
        if after != before || capability_notice.is_some() {
            let _ = app.emit(TRAY_PREFERENCES_CHANGED_EVENT, &after);
        }
        if capability_notice.is_some() {
            ensure_main_reachable_if_tray_unavailable(app, &tray);
        }
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_popup_pending_map_progress(
    app: &AppHandle,
    display: &gdk::Display,
    generation: u64,
    attempt_ticket: u64,
    trigger: PendingMapTrigger,
) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let backend = GtkOutsideClickBackend::new(app.clone(), display.clone());
    let resolution = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_pending_map_progress(
            cell,
            &backend,
            generation,
            attempt_ticket,
            trigger,
            Instant::now(),
        )
    });
    if let PendingMapResolution::Finalized {
        admitted: _,
        capability_notice,
        effective_notice,
        should_publish_notice,
    } = resolution
    {
        if should_publish_notice {
            publish_finalized_popup_map_notice(app, effective_notice, capability_notice);
        }
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_popup_pending_map_timeout(
    app: &AppHandle,
    display: &gdk::Display,
    generation: u64,
    attempt_ticket: u64,
) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let backend = GtkOutsideClickBackend::new(app.clone(), display.clone());
    let resolution = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_pending_map_deadline(cell, &backend, generation, attempt_ticket, Instant::now())
    });
    match resolution {
        PendingMapResolution::NoOpOrSuperseded => {}
        PendingMapResolution::StillPending { remaining_timeout } => {
            backend.schedule_pending_map_timeout(generation, attempt_ticket, remaining_timeout);
        }
        PendingMapResolution::Finalized {
            admitted: _,
            capability_notice,
            effective_notice,
            should_publish_notice,
        } => {
            if should_publish_notice {
                publish_finalized_popup_map_notice(app, effective_notice, capability_notice);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_backdrop_size_allocate(
    app: &AppHandle,
    display: &gdk::Display,
    surface_id: u64,
    generation: u64,
    alloc_width: i32,
    alloc_height: i32,
) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let backend = GtkOutsideClickBackend::new(app.clone(), display.clone());
    let notice = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_async_surface_configured(
            cell,
            &backend,
            surface_id,
            generation,
            alloc_width,
            alloc_height,
        )
    });
    if let Some(capability_notice) = notice {
        publish_async_backdrop_capability_notice(app, capability_notice);
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_backdrop_configure_timeout(
    app: &AppHandle,
    display: &gdk::Display,
    generation: u64,
    admission_seq: u64,
) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let backend = GtkOutsideClickBackend::new(app.clone(), display.clone());
    let notice = LINUX_OUTSIDE_CLICK_STATE
        .with(|cell| drive_async_configure_timeout(cell, &backend, generation, admission_seq));
    if let Some(capability_notice) = notice {
        publish_async_backdrop_capability_notice(app, capability_notice);
    }
}

#[cfg(target_os = "linux")]
fn classify_gdk_pointer_event(event_type: gdk::EventType, button: u32) -> BackdropPointerEvent {
    match event_type {
        gdk::EventType::ButtonPress => BackdropPointerEvent::ButtonPress { button },
        gdk::EventType::MotionNotify
        | gdk::EventType::EnterNotify
        | gdk::EventType::LeaveNotify => BackdropPointerEvent::PointerMotion,
        _ => BackdropPointerEvent::NonInitialButtonEvent,
    }
}

#[cfg(target_os = "linux")]
fn classify_gdk_candidate_device(
    device: gdk::Device,
) -> BackdropCandidateDevice<gdk::Display, gdk::Device> {
    let display = device.display();
    let role = match device.device_type() {
        gdk::DeviceType::Master => {
            if device.source() == gdk::InputSource::Keyboard {
                BackdropDeviceRole::MasterKeyboard
            } else {
                BackdropDeviceRole::MasterPointer
            }
        }
        _ => BackdropDeviceRole::SlaveOrFloating,
    };
    let seat_master_pointer = device.seat().and_then(|seat| seat.pointer());
    BackdropCandidateDevice {
        handle: device,
        display,
        role,
        seat_master_pointer,
    }
}

#[cfg(target_os = "linux")]
fn handle_gtk_backdrop_button_press_event(
    app: &AppHandle,
    display: &gdk::Display,
    surface_id: u64,
    surface_generation: u64,
    callback_win: &gtk::Window,
    event: &gdk::EventButton,
) -> glib::Propagation {
    let is_gtk_main_thread = gtk::is_initialized_main_thread();
    let pointer_event = classify_gdk_pointer_event(event.event_type(), event.button());
    let event_time = event.time();
    let (callback_toplevel, event_window, callback_window, device) = if is_gtk_main_thread {
        let cb_toplevel = Some(callback_win.clone());
        let ev_win = event.window().map(|gdk_win| {
            let win_display = gdk_win.display();
            let is_live = !gdk_win.is_destroyed() && gdk_win.is_visible();
            (gdk_win, win_display, is_live)
        });
        let cb_win = callback_win.window().map(|gdk_win| {
            let win_display = gdk_win.display();
            let is_live = callback_win.is_realized()
                && callback_win.is_mapped()
                && callback_win.is_visible()
                && callback_win.is_layer_window()
                && !gdk_win.is_destroyed()
                && gdk_win.is_visible();
            (gdk_win, win_display, is_live)
        });
        let candidate_dev = event.device().map(classify_gdk_candidate_device);
        (cb_toplevel, ev_win, cb_win, candidate_dev)
    } else {
        (None, None, None, None)
    };
    let ctx = BackdropNativePressContext {
        is_gtk_main_thread,
        pointer_event,
        event_time,
        backend_display: display.clone(),
        callback_toplevel,
        event_window,
        callback_window,
        device,
    };
    let backend = GtkOutsideClickBackend::new(app.clone(), display.clone());
    let outcome = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_outside_click_backdrop_press(cell, &backend, surface_id, surface_generation, ctx)
    });

    match outcome {
        BackdropDismissOutcome::IgnoredNonPress
        | BackdropDismissOutcome::RejectedStaleOrInactive
        | BackdropDismissOutcome::RejectedInvalidNativeTarget => glib::Propagation::Proceed,
        BackdropDismissOutcome::SupersededOrCancelledAfterUngrab
        | BackdropDismissOutcome::SupersededOrCancelledDuringRelease
        | BackdropDismissOutcome::DismissedPopover => glib::Propagation::Stop,
    }
}

/// Verifies and fences `(generation, attempt_ticket)` on the GTK main thread right before hiding
/// the native popover window for an outside-click dismissal, ensuring neither
/// `LINUX_OUTSIDE_CLICK_STATE` nor `WaylandLayerFocusTracker` was superseded or cancelled.
#[cfg(target_os = "linux")]
pub(super) fn fence_linux_popover_dismiss_epoch_on_main_thread(
    app: &AppHandle,
    generation: u64,
    attempt_ticket: u64,
) -> bool {
    if !gtk::is_initialized_main_thread() || generation == 0 || attempt_ticket == 0 {
        return false;
    }
    if app
        .try_state::<TrayState>()
        .is_some_and(|tray| tray.is_explicit_quit_requested())
    {
        return false;
    }
    let coord_exact = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        let state = cell.borrow();
        state.active_generation.is_none()
            && state.active_admission_seq.is_none()
            && !state.admission_in_progress
            && state.pending_map_admission.is_none()
            && state.latest_attempt_ticket == attempt_ticket
            && state.surfaces.is_empty()
    });
    if !coord_exact {
        return false;
    }
    let popup_live = app
        .get_webview_window(TRAY_WINDOW_LABEL)
        .and_then(|popover| popover.gtk_window().ok())
        .is_some_and(|gtk_win| {
            gtk_win.is_visible() && gtk_win.is_mapped() && gtk_win.is_layer_window()
        });
    if !popup_live {
        return false;
    }
    let mut guard = linux_runtime_state()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let tracker = &mut guard.focus_tracker;
    if !tracker.is_mapped()
        || tracker.current_generation() != generation
        || tracker.latest_attempt_ticket() != attempt_ticket
        || tracker.is_fenced_or_superseded(generation)
        || tracker.is_attempt_superseded(generation, attempt_ticket)
    {
        return false;
    }
    tracker.abort_attempt_remap(generation, attempt_ticket);
    true
}

/// Returns `true` on the GTK main thread when `(generation, attempt_ticket)` has not been
/// superseded by any newer open generation or newer `attempt_ticket`.
#[cfg(target_os = "linux")]
pub(super) fn is_linux_dismiss_epoch_unsuperseded_on_main_thread(
    generation: u64,
    attempt_ticket: u64,
) -> bool {
    if !gtk::is_initialized_main_thread() || generation == 0 || attempt_ticket == 0 {
        return false;
    }
    let coord_unsuperseded = LINUX_OUTSIDE_CLICK_STATE
        .with(|cell| !is_attempt_superseded_in_coordinator(cell, generation, attempt_ticket));
    if !coord_unsuperseded {
        return false;
    }
    let guard = linux_runtime_state()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    !guard
        .focus_tracker
        .is_attempt_superseded(generation, attempt_ticket)
}

#[cfg(target_os = "linux")]
fn release_outside_click_backdrops_on_main_thread() {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        let drained_epoch_ticket =
            drive_release_all_with(cell, release_gtk_backdrop_surface_fail_open);
        let state = cell.borrow();
        if state.latest_attempt_ticket() == drained_epoch_ticket {
            debug_assert_eq!(state.active_surface_count(), 0);
            debug_assert_eq!(state.active_generation(), None);
        }
    });
}

/// Fail-open releases every active Wayland outside-click input surface across all monitors.
/// Safe to invoke from any thread: executes synchronously when already on the GTK main thread or
/// dispatches onto the main thread via [`AppHandle::run_on_main_thread`].
#[cfg(target_os = "linux")]
pub fn release_linux_outside_click_backdrops(app: &AppHandle) {
    {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.focus_tracker.on_unmapped();
    }
    if gtk::is_initialized_main_thread() {
        release_outside_click_backdrops_on_main_thread();
    } else {
        let _ = app.run_on_main_thread(|| {
            release_outside_click_backdrops_on_main_thread();
        });
    }
}

/// Synchronously hides the native GTK `tray` window when on the GTK main thread so a queued Tao
/// `WindowRequest::Visible(false)` cannot fire a delayed `connect_hide` into an immediate reopen.
#[cfg(target_os = "linux")]
pub fn hide_linux_popover_gtk_window_on_main_thread(popover: &WebviewWindow) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    if let Ok(gtk_win) = popover.gtk_window() {
        gtk_win.hide();
    }
}

#[cfg(target_os = "linux")]
fn attach_linux_display_monitor_lifecycle_guard(app: &AppHandle, display: &gdk::Display) {
    if !gtk::is_initialized_main_thread() {
        return;
    }
    let already_attached =
        LINUX_DISPLAY_MONITOR_GUARD_ATTACHED.with(|attached| attached.replace(true));
    if already_attached {
        return;
    }

    let app_for_removed = app.clone();
    display.connect_monitor_removed(move |_disp, _removed_monitor| {
        release_outside_click_backdrops_on_main_thread();
        {
            let mut guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.layer_monitor_pinned = false;
            guard.focus_tracker.on_unmapped();
        }
        if let Some(popover) = app_for_removed.get_webview_window(TRAY_WINDOW_LABEL) {
            if popover.is_visible().unwrap_or(false) {
                hide_popover_window(&app_for_removed);
                if let Some(tray) = app_for_removed.try_state::<TrayState>() {
                    ensure_main_reachable_if_tray_unavailable(&app_for_removed, &tray);
                }
            }
        }
    });

    let app_for_added = app.clone();
    display.connect_monitor_added(move |_disp, _added_monitor| {
        let Some(popover) = app_for_added.get_webview_window(TRAY_WINDOW_LABEL) else {
            return;
        };
        let live_or_pending = popover.is_visible().unwrap_or(false)
            || linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .focus_tracker
                .has_live_or_pending_open();
        if !live_or_pending {
            return;
        }
        let raw_anchor = app_for_added
            .try_state::<TrayState>()
            .and_then(|tray| tray.last_raw_anchor());
        refresh_linux_popover_size_on_scale_change(&app_for_added, &popover, &[], raw_anchor);
    });
}

#[cfg(target_os = "linux")]
fn for_widget_and_descendants(root: &gtk::Widget, f: &mut dyn FnMut(&gtk::Widget)) {
    f(root);
    if let Some(container) = root.downcast_ref::<gtk::Container>() {
        for child in container.children() {
            for_widget_and_descendants(&child, f);
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_gtk_popover_window_hints(
    gtk_win: &gtk::ApplicationWindow,
    logical_width: i32,
    logical_height: i32,
    is_layer_surface: bool,
) {
    gtk_win.set_decorated(false);
    gtk_win.set_resizable(false);
    gtk_win.set_role(LINUX_TRAY_WINDOW_ROLE);
    if !is_layer_surface {
        gtk_win.set_skip_taskbar_hint(true);
        gtk_win.set_skip_pager_hint(true);
        gtk_win.set_keep_above(true);
        gtk_win.set_type_hint(gdk::WindowTypeHint::Utility);
        gtk_win.set_transient_for(None::<&gtk::Window>);
    }
    gtk_win.set_size_request(logical_width, logical_height);
    gtk_win.set_default_size(logical_width, logical_height);
    gtk_win.resize(logical_width, logical_height);
}

#[cfg(target_os = "linux")]
fn apply_webview_fixed_logical_size(
    popover: &WebviewWindow,
    logical_width: i32,
    logical_height: i32,
) {
    let target = LogicalSize::new(logical_width as f64, logical_height as f64);
    // Clear previous min/max bounds before setting the new fixed size so shrinking or growing
    // across monitors never transiently violates GTK's `min_size <= max_size` invariant.
    let _ = popover.set_min_size(None::<LogicalSize<f64>>);
    let _ = popover.set_max_size(None::<LogicalSize<f64>>);
    let _ = popover.set_min_size(Some(target));
    let _ = popover.set_max_size(Some(target));
    let _ = popover.set_size(target);
}

#[cfg(target_os = "linux")]
fn apply_gtk_layer_shell_surface_props(
    gtk_win: &gtk::ApplicationWindow,
    layer: WaylandShellLayer,
    exclusive_zone: i32,
) {
    gtk_win.set_namespace(LINUX_TRAY_WINDOW_ROLE);
    gtk_win.set_layer(layer.to_gtk_layer());
    gtk_win.set_exclusive_zone(exclusive_zone);
}

#[cfg(target_os = "linux")]
fn apply_gtk_layer_shell_monitor(
    gtk_win: &gtk::ApplicationWindow,
    display: &gdk::Display,
    placement_monitor_index: Option<usize>,
    fallback_target_monitor_index: Option<usize>,
) {
    if let Some(mon_idx) = placement_monitor_index {
        if let Some(gdk_mon) = display.monitor(mon_idx as i32) {
            linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .layer_monitor_pinned = true;
            gtk_win.set_monitor(&gdk_mon);
        }
    } else {
        let was_pinned = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .layer_monitor_pinned;
        if was_pinned {
            if let Some(mon_idx) = fallback_target_monitor_index {
                if let Some(gdk_mon) = display.monitor(mon_idx as i32) {
                    gtk_win.set_monitor(&gdk_mon);
                }
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn apply_gtk_layer_shell_anchors_and_margins(
    gtk_win: &gtk::ApplicationWindow,
    placement: &WaylandLayerShellPlacement,
) {
    // Clear all four anchors before enabling the perpendicular pair so live reconfigurations
    // never transiently hold opposite edges simultaneously.
    gtk_win.set_anchor(Edge::Top, false);
    gtk_win.set_anchor(Edge::Bottom, false);
    gtk_win.set_anchor(Edge::Left, false);
    gtk_win.set_anchor(Edge::Right, false);

    gtk_win.set_anchor(Edge::Top, placement.anchor_top);
    gtk_win.set_anchor(Edge::Bottom, placement.anchor_bottom);
    gtk_win.set_anchor(Edge::Left, placement.anchor_left);
    gtk_win.set_anchor(Edge::Right, placement.anchor_right);

    gtk_win.set_layer_shell_margin(Edge::Top, placement.margin_top);
    gtk_win.set_layer_shell_margin(Edge::Bottom, placement.margin_bottom);
    gtk_win.set_layer_shell_margin(Edge::Left, placement.margin_left);
    gtk_win.set_layer_shell_margin(Edge::Right, placement.margin_right);
}

#[cfg(target_os = "linux")]
fn apply_wayland_layer_shell_placement(
    gtk_win: &gtk::ApplicationWindow,
    display: &gdk::Display,
    placement: &WaylandLayerShellPlacement,
    fallback_target_monitor_index: Option<usize>,
) {
    debug_assert!(placement.is_compact_non_exclusive());
    apply_gtk_layer_shell_surface_props(gtk_win, placement.layer, placement.exclusive_zone);
    apply_gtk_layer_shell_monitor(
        gtk_win,
        display,
        placement.monitor_index,
        fallback_target_monitor_index,
    );
    apply_gtk_layer_shell_anchors_and_margins(gtk_win, placement);
}

#[cfg(target_os = "linux")]
struct GtkPopupPresentationTarget<'a, FRecord> {
    app: &'a AppHandle,
    popover: &'a WebviewWindow,
    gtk_win: &'a gtk::ApplicationWindow,
    display: &'a gdk::Display,
    record_open_before_show: Option<FRecord>,
}

#[cfg(target_os = "linux")]
impl<FRecord: FnOnce()> PopupPresentationTarget for GtkPopupPresentationTarget<'_, FRecord> {
    fn apply_webview_logical_size(&mut self, logical_width: i32, logical_height: i32) {
        apply_webview_fixed_logical_size(self.popover, logical_width, logical_height);
    }

    fn apply_gtk_window_hints(
        &mut self,
        logical_width: i32,
        logical_height: i32,
        is_layer_surface: bool,
    ) {
        apply_gtk_popover_window_hints(
            self.gtk_win,
            logical_width,
            logical_height,
            is_layer_surface,
        );
    }

    fn apply_layer_shell_surface_props(&mut self, layer: WaylandShellLayer, exclusive_zone: i32) {
        apply_gtk_layer_shell_surface_props(self.gtk_win, layer, exclusive_zone);
    }

    fn apply_layer_shell_monitor(
        &mut self,
        placement_monitor_index: Option<usize>,
        fallback_target_monitor_index: Option<usize>,
    ) {
        apply_gtk_layer_shell_monitor(
            self.gtk_win,
            self.display,
            placement_monitor_index,
            fallback_target_monitor_index,
        );
    }

    fn apply_layer_shell_anchors_and_margins(&mut self, placement: &WaylandLayerShellPlacement) {
        apply_gtk_layer_shell_anchors_and_margins(self.gtk_win, placement);
    }

    fn apply_initial_layer_keyboard_mode(&mut self, keyboard_policy: WaylandKeyboardPolicy) {
        // Both policies map in `Exclusive` mode (`gtk-layer-shell` translates `Exclusive` to
        // `keyboard_interactivity = 1` on `zwlr_layer_shell_v1` versions 1..=3);
        // `ExclusiveThenOnDemand` transitions to `OnDemand` once native GTK map is confirmed.
        match keyboard_policy {
            WaylandKeyboardPolicy::ExclusiveThenOnDemand
            | WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick => {
                self.gtk_win.set_keyboard_mode(KeyboardMode::Exclusive);
            }
        }
    }

    fn apply_gdk_root_position(&mut self, gdk_x: i32, gdk_y: i32) {
        self.gtk_win.move_(gdk_x, gdk_y);
    }

    fn show_popup_children(&mut self) {
        // Ensure the actual GtkBin content container and WebKitWebView are marked visible prior to
        // mapping `gtk_win` (Wry skips `show_all()` on Wayland when the parent window starts with
        // `visible: false`), without touching Tao's non-content CSD titlebar slot.
        if let Some(content_root) = gtk_popup_content_root(self.gtk_win) {
            content_root.show_all();
        }
    }

    fn publish_pre_show_open_state(&mut self, base_notice: Option<String>) {
        if let Some(tray) = self.app.try_state::<TrayState>() {
            tray.set_placement_notice(base_notice);
        }
        if let Some(record_fn) = self.record_open_before_show.take() {
            record_fn();
        }
    }

    fn present_popup_window(&mut self) -> Result<(), String> {
        self.popover
            .show()
            .map_err(|err| format!("Failed to show Linux tray popover: {err}"))
    }

    fn focus_and_present_non_layer_popup(&mut self) {
        let _ = self.popover.unminimize();
        let _ = self.popover.set_focus();
        self.gtk_win.present();
    }
}

#[cfg(target_os = "linux")]
fn detect_window_layer_shell_support(gtk_win: &gtk::ApplicationWindow) -> WaylandLayerShellSupport {
    if !gtk_layer_shell::is_supported() {
        return WaylandLayerShellSupport::Unsupported(
            WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
        );
    }
    if gtk_win.is_layer_window() {
        return WaylandLayerShellSupport::Supported {
            protocol_version: gtk_layer_shell::protocol_version(),
        };
    }
    WaylandLayerShellSupport::Unsupported(WaylandLayerShellUnsupportedReason::WindowAlreadyRealized)
}

#[cfg(target_os = "linux")]
fn collect_gdk_monitors(
    display: &gdk::Display,
    fallback_monitors: &[MonitorBoundsPx],
) -> Vec<LinuxMonitorWorkarea> {
    let count = display.n_monitors();
    let mut out = Vec::new();
    for idx in 0..count {
        if let Some(mon) = display.monitor(idx) {
            let geom = mon.geometry();
            let work = mon.workarea();
            let gdk_scale = (mon.scale_factor().max(1)) as f64;
            out.push(LinuxMonitorWorkarea {
                monitor_index: Some(idx as usize),
                geometry_x: geom.x(),
                geometry_y: geom.y(),
                geometry_width: geom.width().max(1),
                geometry_height: geom.height().max(1),
                workarea_x: work.x(),
                workarea_y: work.y(),
                workarea_width: work.width().max(1),
                workarea_height: work.height().max(1),
                scale_factor: gdk_scale,
                is_primary: mon.is_primary(),
            });
        }
    }
    if out.is_empty() {
        for (idx, bounds) in fallback_monitors.iter().copied().enumerate() {
            out.push(LinuxMonitorWorkarea::from_monitor_bounds_px(
                bounds,
                Some(idx),
                idx == 0,
            ));
        }
    }
    out
}

#[cfg(target_os = "linux")]
fn resolve_main_window_monitor_index(app: &AppHandle, display: &gdk::Display) -> Option<usize> {
    let main_win = app.get_webview_window(MAIN_WINDOW_LABEL)?;
    let main_gtk = main_win.gtk_window().ok()?;
    let gdk_win = main_gtk.window()?;
    let active_mon = display.monitor_at_window(&gdk_win)?;
    let count = display.n_monitors();
    for idx in 0..count {
        if display.monitor(idx).as_ref() == Some(&active_mon) {
            return Some(idx as usize);
        }
    }
    None
}

/// Attaches a non-intrusive click listener to Threadlane's `main` window hierarchy so clicking
/// anywhere on `main` immediately dismisses an open `tray` popover even on Wayland compositors
/// where layer-shell surfaces hold keyboard interactivity.
#[cfg(target_os = "linux")]
pub fn attach_linux_main_window_outside_click_guard(app: &AppHandle) {
    if !gtk::is_initialized_main_thread() {
        let app_clone = app.clone();
        let _ = app.run_on_main_thread(move || {
            attach_linux_main_window_outside_click_guard(&app_clone);
        });
        return;
    }

    let Some(main_win) = app.get_webview_window(MAIN_WINDOW_LABEL) else {
        return;
    };
    let Ok(main_gtk) = main_win.gtk_window() else {
        return;
    };

    {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if guard.main_click_guard_attached {
            return;
        }
        guard.main_click_guard_attached = true;
    }

    let root_widget: gtk::Widget = main_gtk.upcast();
    let app_handle = app.clone();
    for_widget_and_descendants(&root_widget, &mut |widget| {
        widget.add_events(gdk::EventMask::BUTTON_PRESS_MASK);
        let app_clone = app_handle.clone();
        widget.connect_button_press_event(move |_w, _ev| {
            if let Some(popover) = app_clone.get_webview_window(TRAY_WINDOW_LABEL) {
                if popover.is_visible().unwrap_or(false) {
                    if let Some(tray) = app_clone.try_state::<TrayState>() {
                        tray.record_popover_blur_hidden();
                        hide_popover_window(&app_clone);
                        ensure_main_reachable_if_tray_unavailable(&app_clone, &tray);
                    } else {
                        hide_popover_window(&app_clone);
                    }
                }
            }
            glib::Propagation::Proceed
        });
    });
}

#[cfg(target_os = "linux")]
fn attach_popup_content_redraw_signals(
    app: &AppHandle,
    gtk_win: &gtk::ApplicationWindow,
    widget: &gtk::Widget,
) {
    let app_for_alloc = app.clone();
    let win_for_alloc = gtk_win.downgrade();
    widget.connect_size_allocate(move |_w, _alloc| {
        if let Some(win) = win_for_alloc.upgrade() {
            handle_gtk_popup_settled_redraw_signal(&app_for_alloc, &win);
        }
    });
    let app_for_child_map = app.clone();
    let win_for_child_map = gtk_win.downgrade();
    widget.connect_map(move |_w| {
        if let Some(win) = win_for_child_map.upgrade() {
            handle_gtk_popup_settled_redraw_signal(&app_for_child_map, &win);
        }
    });
    if !is_webkit_webview_widget(widget) {
        if let Some(container) = widget.downcast_ref::<gtk::Container>() {
            for child in container.children() {
                attach_popup_content_redraw_signals(app, gtk_win, &child);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn attach_linux_popover_signal_handlers(app: &AppHandle, gtk_win: &gtk::ApplicationWindow) {
    {
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if guard.popover_signals_attached {
            return;
        }
        guard.popover_signals_attached = true;
    }

    gtk_win.add_events(
        gdk::EventMask::ENTER_NOTIFY_MASK
            | gdk::EventMask::LEAVE_NOTIFY_MASK
            | gdk::EventMask::KEY_PRESS_MASK,
    );

    // Native GTK Escape dismissal across the popover window and inner WebKitWebView hierarchy.
    let root_widget: gtk::Widget = gtk_win.clone().upcast();
    let app_for_escape = app.clone();
    for_widget_and_descendants(&root_widget, &mut |widget| {
        widget.add_events(gdk::EventMask::KEY_PRESS_MASK);
        let app_clone = app_for_escape.clone();
        widget.connect_key_press_event(move |_w, event| {
            if event.keyval() == gdk::keys::constants::Escape {
                hide_popover_window(&app_clone);
                if let Some(tray) = app_clone.try_state::<TrayState>() {
                    ensure_main_reachable_if_tray_unavailable(&app_clone, &tray);
                }
                return glib::Propagation::Stop;
            }
            glib::Propagation::Proceed
        });
    });

    let app_for_enter = app.clone();
    gtk_win.connect_enter_notify_event(move |win, _event| {
        let (generation, attempt_ticket) = {
            let mut guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.focus_tracker.on_pointer_entered();
            (
                guard.focus_tracker.current_generation(),
                guard.focus_tracker.latest_attempt_ticket(),
            )
        };
        // Once the pointer enters the layer surface, switch to `OnDemand` so outside clicks
        // release focus cleanly; if focus briefly slipped under `focus_follows_mouse yes` before
        // pointer entry, pulse `Exclusive` -> `OnDemand` to restore `Escape` keyboard focus.
        if win.is_layer_window() && gtk_layer_shell::protocol_version() >= 4 && win.is_visible() {
            if !win.is_active() {
                win.set_keyboard_mode(KeyboardMode::Exclusive);
                schedule_wayland_on_demand_transition(
                    &app_for_enter,
                    generation,
                    attempt_ticket,
                    WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                );
            } else {
                win.set_keyboard_mode(KeyboardMode::OnDemand);
            }
        }
        glib::Propagation::Proceed
    });

    gtk_win.connect_leave_notify_event(|_win, event| {
        let is_inferior = event.detail() == gdk::NotifyType::Inferior;
        let mut guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.focus_tracker.on_pointer_left(is_inferior);
        glib::Propagation::Proceed
    });

    let app_for_win_alloc = app.clone();
    gtk_win.connect_size_allocate(move |win, _alloc| {
        handle_gtk_popup_settled_redraw_signal(&app_for_win_alloc, win);
    });
    if let Some(content_root) = gtk_popup_content_root(gtk_win) {
        attach_popup_content_redraw_signals(app, gtk_win, &content_root);
    }

    let app_for_map = app.clone();
    gtk_win.connect_map(move |win| {
        let pending_or_expired = LINUX_OUTSIDE_CLICK_STATE
            .with(|cell| cell.borrow().active_pending_or_expired_map_ticket());
        let Some((generation, attempt_ticket)) = pending_or_expired else {
            return;
        };
        let display = win.display();
        handle_gtk_popup_pending_map_progress(
            &app_for_map,
            &display,
            generation,
            attempt_ticket,
            PendingMapTrigger::MapSignal,
        );
    });

    let app_for_unmap = app.clone();
    gtk_win.connect_unmap(move |win| {
        let preserved_epoch = {
            let mut guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.focus_tracker.on_transient_unmap_preserving_epoch()
        };
        if !gtk::is_initialized_main_thread() {
            return;
        }
        let backend = GtkOutsideClickBackend::new(app_for_unmap.clone(), win.display());
        LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            drive_popup_transient_unmap(cell, &backend, preserved_epoch);
        });
    });

    gtk_win.connect_hide(|_win| {
        {
            let mut guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.focus_tracker.on_unmapped();
        }
        release_outside_click_backdrops_on_main_thread();
    });
}

/// Initializes the hidden Linux `tray` window before its first map, establishing the native
/// Wayland `zwlr_layer_shell_v1` role when supported by the compositor, attaching native input
/// lifecycle hooks, and returning any truthful initial capability notice.
#[cfg(target_os = "linux")]
pub fn initialize_linux_popover_window(app: &AppHandle, popover: &WebviewWindow) -> Option<String> {
    if !gtk::is_initialized_main_thread() {
        return None;
    }
    attach_linux_main_window_outside_click_guard(app);

    let gtk_win = popover.gtk_window().ok()?;
    let display = gtk_win.display();
    attach_linux_display_monitor_lifecycle_guard(app, &display);

    let backend = classify_gdk_display_type_name(display.type_().name());
    let width = TRAY_POPOVER_WIDTH.round() as i32;
    let height = TRAY_POPOVER_HEIGHT.round() as i32;

    let mut is_layer_surface = false;
    let mut initial_notice = None;

    match backend {
        LinuxDisplayBackend::Wayland => {
            if gtk_layer_shell::is_supported() {
                // `init_layer_shell` MUST execute before `gtk_win` is realized. Never call
                // `unrealize()` on an already-realized Tauri/Wry-owned window (which would
                // destroy backing native/WebKit handles), and never call `init_layer_shell()`
                // on a realized or mapped window.
                if !gtk_win.is_layer_window() && !gtk_win.is_realized() && !gtk_win.is_mapped() {
                    gtk_win.init_layer_shell();
                }

                if gtk_win.is_layer_window() {
                    is_layer_surface = true;
                    let proto = gtk_layer_shell::protocol_version();
                    let initial_placement = WaylandLayerShellPlacement {
                        monitor_index: None,
                        layer: if proto >= 4 {
                            WaylandShellLayer::Overlay
                        } else {
                            WaylandShellLayer::Top
                        },
                        anchor_top: true,
                        anchor_bottom: false,
                        anchor_left: false,
                        anchor_right: true,
                        margin_top: 12,
                        margin_bottom: 0,
                        margin_left: 0,
                        margin_right: 12,
                        exclusive_zone: 0,
                        keyboard_policy: if proto >= 4 {
                            WaylandKeyboardPolicy::ExclusiveThenOnDemand
                        } else {
                            WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick
                        },
                        estimated_gdk_bounds: (0, 12, width, height),
                    };
                    apply_wayland_layer_shell_placement(
                        &gtk_win,
                        &display,
                        &initial_placement,
                        None,
                    );
                    if proto < 4 {
                        initial_notice = Some(WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE.to_string());
                    }
                } else {
                    initial_notice = Some(WAYLAND_REALIZED_FALLBACK_NOTICE.to_string());
                }
            } else {
                initial_notice = Some(WAYLAND_COMPOSITOR_PLACEMENT_NOTICE.to_string());
            }
        }
        LinuxDisplayBackend::Unknown => {
            initial_notice = Some(UNKNOWN_LINUX_BACKEND_PLACEMENT_NOTICE.to_string());
        }
        LinuxDisplayBackend::X11 => {}
    }

    apply_webview_fixed_logical_size(popover, width, height);
    apply_gtk_popover_window_hints(&gtk_win, width, height, is_layer_surface);
    attach_linux_popover_signal_handlers(app, &gtk_win);

    initial_notice
}

/// Recomputes compact 3-row sizing, configures Wayland `zwlr_layer_shell_v1` anchors/margins/monitor
/// or X11 root coordinates for the target monitor's scale and workarea, presents the window, and
/// returns a truthful [`LinuxOpenOutcome`].
#[cfg(target_os = "linux")]
pub fn open_and_present_linux_popover(
    app: &AppHandle,
    popover: &WebviewWindow,
    fallback_monitors: &[MonitorBoundsPx],
    raw_anchor: Option<RawTrayAnchor>,
    record_open_before_show: impl FnOnce(),
) -> Result<LinuxOpenOutcome, String> {
    if !gtk::is_initialized_main_thread() {
        return Err("Linux tray GTK presentation requires the GTK main thread.".to_string());
    }
    attach_linux_main_window_outside_click_guard(app);

    let gtk_win = popover
        .gtk_window()
        .map_err(|err| format!("Failed to access GTK tray window: {err}"))?;
    attach_linux_popover_signal_handlers(app, &gtk_win);

    let display = gtk_win.display();
    attach_linux_display_monitor_lifecycle_guard(app, &display);

    let backend = classify_gdk_display_type_name(display.type_().name());
    let wayland_support = if backend == LinuxDisplayBackend::Wayland {
        detect_window_layer_shell_support(&gtk_win)
    } else {
        WaylandLayerShellSupport::Unsupported(
            WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
        )
    };

    let monitors = collect_gdk_monitors(&display, fallback_monitors);
    let preferred_fallback_monitor_index = resolve_main_window_monitor_index(app, &display);
    let plan = plan_linux_popover_placement(
        backend,
        wayland_support,
        &monitors,
        raw_anchor,
        preferred_fallback_monitor_index,
        (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
    );
    let plan_valid = plan.validate_outside_click_plan();
    debug_assert!(plan_valid);

    let is_layer_surface = plan.layer_shell.is_some() && gtk_win.is_layer_window();
    let (gdk_w, gdk_h) = plan.logical_size;
    let base_notice = plan.effective_notice();
    let keyboard_policy = plan.layer_shell.as_ref().map(|lp| lp.keyboard_policy);
    let gtk_backend = GtkOutsideClickBackend::new(app.clone(), display.clone());

    if is_layer_surface {
        let mut target = GtkPopupPresentationTarget {
            app,
            popover,
            gtk_win: &gtk_win,
            display: &display,
            record_open_before_show: Some(record_open_before_show),
        };
        let attempt_outcome = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            drive_execute_popup_map_attempt(
                cell,
                &gtk_backend,
                PopupAttemptKind::Open,
                keyboard_policy,
                plan_valid,
                &plan.outside_click_backdrops,
                base_notice.clone(),
                Instant::now,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |open_generation, attempt_ticket| {
                    drive_present_layer_popup_with_checkpoints(
                        cell,
                        &gtk_backend,
                        &mut target,
                        PopupAttemptKind::Open,
                        open_generation,
                        attempt_ticket,
                        (gdk_w, gdk_h),
                        plan.layer_shell.as_ref(),
                        plan.target_monitor.monitor_index,
                        base_notice.clone(),
                    )
                },
            )
        })?;

        return match attempt_outcome {
            PopupAttemptExecutionOutcome::NotActive
            | PopupAttemptExecutionOutcome::SupersededOrCancelled => {
                Ok(LinuxOpenOutcome::SupersededOrCancelled)
            }
            PopupAttemptExecutionOutcome::SupersededSameGeneration => {
                Ok(LinuxOpenOutcome::SupersededSameGeneration)
            }
            PopupAttemptExecutionOutcome::ArmedPendingMap { effective_notice } => {
                Ok(LinuxOpenOutcome::Presented {
                    outcome: plan.outcome,
                    effective_notice,
                })
            }
            PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: _,
                capability_notice,
                effective_notice,
                should_publish_notice,
            } => {
                if capability_notice.is_some() && should_publish_notice {
                    if let Some(tray) = app.try_state::<TrayState>() {
                        ensure_main_reachable_if_tray_unavailable(app, &tray);
                    }
                }
                Ok(LinuxOpenOutcome::Presented {
                    outcome: plan.outcome,
                    effective_notice,
                })
            }
        };
    }

    let reserve_fn = || LINUX_OUTSIDE_CLICK_STATE.with(reserve_attempt_ticket);
    let res = gtk_backend.begin_tracker_attempt(PopupAttemptKind::Open, false, reserve_fn);
    let Some((open_generation, attempt_ticket)) = res else {
        return Ok(LinuxOpenOutcome::SupersededOrCancelled);
    };

    let still_current_pre = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_release_up_to_attempt(cell, &gtk_backend, open_generation, attempt_ticket)
    });
    if !still_current_pre {
        return Ok(LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            match classify_superseded_attempt(cell, &gtk_backend, open_generation, attempt_ticket) {
                PopupAttemptExecutionOutcome::SupersededSameGeneration => {
                    LinuxOpenOutcome::SupersededSameGeneration
                }
                _ => LinuxOpenOutcome::SupersededOrCancelled,
            }
        }));
    }

    let x11_gdk_position = (plan.backend == LinuxDisplayBackend::X11)
        .then_some(plan.gdk_position)
        .flatten();
    let mut target = GtkPopupPresentationTarget {
        app,
        popover,
        gtk_win: &gtk_win,
        display: &display,
        record_open_before_show: Some(record_open_before_show),
    };
    let present_res = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_present_non_layer_popup_with_checkpoints(
            cell,
            &gtk_backend,
            &mut target,
            PopupAttemptKind::Open,
            open_generation,
            attempt_ticket,
            (gdk_w, gdk_h),
            x11_gdk_position,
            base_notice.clone(),
        )
    });
    if let Err(err) = present_res {
        let classified = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            cleanup_and_classify_presentation_err(
                cell,
                &gtk_backend,
                open_generation,
                attempt_ticket,
                err,
            )
        })?;
        return Ok(match classified {
            PopupAttemptExecutionOutcome::SupersededSameGeneration => {
                LinuxOpenOutcome::SupersededSameGeneration
            }
            _ => LinuxOpenOutcome::SupersededOrCancelled,
        });
    }

    let still_current_post = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        is_presentation_epoch_current(cell, &gtk_backend, open_generation, attempt_ticket)
    });
    if !still_current_post {
        return Ok(LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            match classify_superseded_attempt(cell, &gtk_backend, open_generation, attempt_ticket) {
                PopupAttemptExecutionOutcome::SupersededSameGeneration => {
                    LinuxOpenOutcome::SupersededSameGeneration
                }
                _ => LinuxOpenOutcome::SupersededOrCancelled,
            }
        }));
    }

    Ok(LinuxOpenOutcome::Presented {
        outcome: plan.outcome,
        effective_notice: base_notice,
    })
}

/// Handles a focus-loss event on the Linux `tray` popover after [`super::POPOVER_BLUR_GRACE_PERIOD`]
/// has elapsed.
///
/// On Wayland layer-shell surfaces, defers dismissal by [`WAYLAND_FOCUS_LOSS_VERIFY_DELAY`] so
/// pointer travel from an external tray icon across a small gap into the popup under
/// `focus_follows_mouse yes` does not dismiss the popup if the pointer enters it.
#[cfg(target_os = "linux")]
pub fn handle_linux_popover_unfocused(app: &AppHandle, tray: &TrayState) {
    if !gtk::is_initialized_main_thread() {
        let app_clone = app.clone();
        let tray_clone = tray.clone();
        let _ = app.run_on_main_thread(move || {
            handle_linux_popover_unfocused(&app_clone, &tray_clone);
        });
        return;
    }

    let Some(popover) = app.get_webview_window(TRAY_WINDOW_LABEL) else {
        release_outside_click_backdrops_on_main_thread();
        return;
    };
    let Ok(gtk_win) = popover.gtk_window() else {
        tray.record_popover_blur_hidden();
        hide_popover_window(app);
        ensure_main_reachable_if_tray_unavailable(app, tray);
        return;
    };

    if !gtk_win.is_layer_window() {
        tray.record_popover_blur_hidden();
        hide_popover_window(app);
        ensure_main_reachable_if_tray_unavailable(app, tray);
        return;
    }
    if !gtk_win.is_visible() {
        release_outside_click_backdrops_on_main_thread();
        return;
    }

    let (pointer_inside, scheduled_generation) = {
        let guard = linux_runtime_state()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        (
            guard.focus_tracker.is_pointer_inside(),
            guard.focus_tracker.current_generation(),
        )
    };
    if pointer_inside {
        return;
    }

    let app_clone = app.clone();
    let tray_clone = tray.clone();
    let win_weak = gtk_win.downgrade();
    let _ = glib::timeout_add_local_once(WAYLAND_FOCUS_LOSS_VERIFY_DELAY, move || {
        if tray_clone.is_explicit_quit_requested() {
            return;
        }
        let Some(w) = win_weak.upgrade() else {
            release_outside_click_backdrops_on_main_thread();
            return;
        };
        let decision = {
            let guard = linux_runtime_state()
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.focus_tracker.evaluate_deferred_unfocus(
                scheduled_generation,
                w.is_visible(),
                w.is_active(),
            )
        };
        if decision == WaylandFocusCheckDecision::DismissPopover {
            tray_clone.record_popover_blur_hidden();
            hide_popover_window(&app_clone);
            ensure_main_reachable_if_tray_unavailable(&app_clone, &tray_clone);
        }
    });
}

/// Reapplies compact 3-row native size constraints and Wayland layer-shell geometry when the
/// display scale factor or monitor workarea changes while the `tray` popover exists on Linux.
#[cfg(target_os = "linux")]
pub fn refresh_linux_popover_size_on_scale_change(
    app: &AppHandle,
    popover: &WebviewWindow,
    fallback_monitors: &[MonitorBoundsPx],
    raw_anchor: Option<RawTrayAnchor>,
) {
    if !gtk::is_initialized_main_thread() {
        let app_clone = app.clone();
        let popover_clone = popover.clone();
        let fallback_vec = fallback_monitors.to_vec();
        let _ = app.run_on_main_thread(move || {
            refresh_linux_popover_size_on_scale_change(
                &app_clone,
                &popover_clone,
                &fallback_vec,
                raw_anchor,
            );
        });
        return;
    }

    let Ok(gtk_win) = popover.gtk_window() else {
        release_outside_click_backdrops_on_main_thread();
        return;
    };
    let display = gtk_win.display();
    attach_linux_display_monitor_lifecycle_guard(app, &display);

    let backend = classify_gdk_display_type_name(display.type_().name());
    let wayland_support = if backend == LinuxDisplayBackend::Wayland {
        detect_window_layer_shell_support(&gtk_win)
    } else {
        WaylandLayerShellSupport::Unsupported(
            WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
        )
    };
    let monitors = collect_gdk_monitors(&display, fallback_monitors);
    let preferred_fallback = resolve_main_window_monitor_index(app, &display);
    let plan = plan_linux_popover_placement(
        backend,
        wayland_support,
        &monitors,
        raw_anchor,
        preferred_fallback,
        (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
    );
    let plan_valid = plan.validate_outside_click_plan();
    debug_assert!(plan_valid);

    let is_layer_surface = plan.layer_shell.is_some() && gtk_win.is_layer_window();
    let (gdk_w, gdk_h) = plan.logical_size;
    let base_notice = plan.effective_notice();
    let keyboard_policy = plan.layer_shell.as_ref().map(|lp| lp.keyboard_policy);
    let gtk_backend = GtkOutsideClickBackend::new(app.clone(), display.clone());

    if is_layer_surface {
        let mut target = GtkPopupPresentationTarget {
            app,
            popover,
            gtk_win: &gtk_win,
            display: &display,
            record_open_before_show: None::<fn()>,
        };
        let attempt_res = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
            drive_execute_popup_map_attempt(
                cell,
                &gtk_backend,
                PopupAttemptKind::Refresh,
                keyboard_policy,
                plan_valid,
                &plan.outside_click_backdrops,
                base_notice.clone(),
                Instant::now,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |refresh_generation, attempt_ticket| {
                    drive_present_layer_popup_with_checkpoints(
                        cell,
                        &gtk_backend,
                        &mut target,
                        PopupAttemptKind::Refresh,
                        refresh_generation,
                        attempt_ticket,
                        (gdk_w, gdk_h),
                        plan.layer_shell.as_ref(),
                        plan.target_monitor.monitor_index,
                        base_notice.clone(),
                    )
                },
            )
        });

        match attempt_res {
            Ok(PopupAttemptExecutionOutcome::ArmedPendingMap { effective_notice }) => {
                publish_finalized_popup_map_notice(app, effective_notice, None);
            }
            Ok(PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: _,
                capability_notice,
                effective_notice,
                should_publish_notice,
            }) => {
                if should_publish_notice {
                    publish_finalized_popup_map_notice(app, effective_notice, capability_notice);
                }
            }
            Ok(PopupAttemptExecutionOutcome::NotActive)
            | Ok(PopupAttemptExecutionOutcome::SupersededSameGeneration)
            | Ok(PopupAttemptExecutionOutcome::SupersededOrCancelled)
            | Err(_) => {}
        }
        return;
    }

    if !gtk_win.is_visible() {
        return;
    }
    let reserve_fn = || LINUX_OUTSIDE_CLICK_STATE.with(reserve_attempt_ticket);
    let Some((refresh_generation, attempt_ticket)) =
        gtk_backend.begin_tracker_attempt(PopupAttemptKind::Refresh, false, reserve_fn)
    else {
        return;
    };
    let x11_gdk_position = (plan.backend == LinuxDisplayBackend::X11)
        .then_some(plan.gdk_position)
        .flatten();
    let mut target = GtkPopupPresentationTarget {
        app,
        popover,
        gtk_win: &gtk_win,
        display: &display,
        record_open_before_show: None::<fn()>,
    };
    let _ = LINUX_OUTSIDE_CLICK_STATE.with(|cell| {
        drive_present_non_layer_popup_with_checkpoints(
            cell,
            &gtk_backend,
            &mut target,
            PopupAttemptKind::Refresh,
            refresh_generation,
            attempt_ticket,
            (gdk_w, gdk_h),
            x11_gdk_position,
            None,
        )
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tray::MAIN_WINDOW_MIN_HEIGHT;

    #[test]
    fn display_backend_classifier_distinguishes_wayland_x11_and_unknown() {
        assert_eq!(
            classify_gdk_display_type_name("GdkWaylandDisplay"),
            LinuxDisplayBackend::Wayland
        );
        assert_eq!(
            classify_gdk_display_type_name("GdkX11Display"),
            LinuxDisplayBackend::X11
        );
        assert_eq!(
            classify_gdk_display_type_name("GdkBroadwayDisplay"),
            LinuxDisplayBackend::Unknown
        );
        assert_eq!(
            classify_gdk_display_type_name(""),
            LinuxDisplayBackend::Unknown
        );
    }

    #[test]
    fn wayland_layer_shell_anchors_to_target_output_and_preserves_compact_non_exclusive_geometry() {
        let monitors = [
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 0,
                workarea_width: 1920,
                workarea_height: 1080,
                scale_factor: 1.0,
                is_primary: true,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 1920,
                workarea_y: 0,
                workarea_width: 1920,
                workarea_height: 1080,
                scale_factor: 2.0,
                is_primary: false,
            },
        ];

        // 1. Sway/Waybar click near top-right on secondary 2x monitor (logical 3790, 16):
        let top_right_second = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 3790.0, y: 16.0 }),
            tray_rect: None,
        };
        let plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(top_right_second),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(
            plan.gdk_position, None,
            "Wayland layer-shell must never issue bogus X11 root move coordinates"
        );
        assert!(plan.outcome.is_icon_anchored());
        assert_eq!(plan.effective_notice(), None);
        assert_eq!(plan.target_monitor.monitor_index, Some(1));
        assert_eq!(plan.target_monitor.scale_factor, 2.0);
        assert_eq!(plan.logical_size, (360, 320));
        assert!((plan.logical_size.1 as f64) < MAIN_WINDOW_MIN_HEIGHT);

        let ls = plan.layer_shell.expect("layer_shell placement must exist");
        assert!(ls.is_compact_non_exclusive());
        assert_eq!(ls.monitor_index, Some(1));
        assert_eq!(ls.exclusive_zone, 0);
        assert!(ls.anchor_top && !ls.anchor_bottom);
        assert!(ls.anchor_right && !ls.anchor_left);
        assert_eq!(ls.margin_top, 0);
        assert_eq!(ls.margin_right, 12);
        assert_eq!(ls.margin_left, 0);
        assert_eq!(ls.margin_bottom, 0);
        assert_eq!(
            ls.keyboard_policy,
            WaylandKeyboardPolicy::ExclusiveThenOnDemand
        );

        // 2. Reopen on Monitor 1 bottom-left bar (48, 1066) flips perpendicular edges cleanly
        // without leaving stale Top/Right anchors or margins active.
        let bottom_left_first = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 48.0, y: 1066.0 }),
            tray_rect: None,
        };
        let reopen_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(bottom_left_first),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(reopen_plan.outcome.is_icon_anchored());
        assert_eq!(reopen_plan.target_monitor.monitor_index, Some(0));
        assert_eq!(reopen_plan.target_monitor.scale_factor, 1.0);
        let ls_reopen = reopen_plan.layer_shell.expect("layer_shell reopen");
        assert!(ls_reopen.is_compact_non_exclusive());
        assert_eq!(ls_reopen.monitor_index, Some(0));
        assert!(!ls_reopen.anchor_top && ls_reopen.anchor_bottom);
        assert!(ls_reopen.anchor_left && !ls_reopen.anchor_right);
        assert_eq!(ls_reopen.margin_bottom, 0);
        assert_eq!(ls_reopen.margin_left, 12);
        assert_eq!(ls_reopen.margin_top, 0);
        assert_eq!(ls_reopen.margin_right, 0);

        // 3. Center-top tray icon at (600, 12) centers horizontally around x=600 (left=420).
        let center_left_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 600.0, y: 12.0 }),
            tray_rect: None,
        };
        let centered_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(center_left_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        let ls_centered = centered_plan.layer_shell.expect("layer_shell centered");
        assert!(ls_centered.is_compact_non_exclusive());
        assert!(ls_centered.anchor_top && ls_centered.anchor_left);
        assert_eq!(ls_centered.margin_left, 600 - 180);
        assert_eq!(ls_centered.margin_top, 0);
    }

    #[test]
    fn wayland_missing_coordinates_never_claim_icon_anchor_or_blindly_force_first_monitor() {
        let monitors = [
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 0,
                workarea_width: 1920,
                workarea_height: 1080,
                scale_factor: 1.0,
                is_primary: true,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 1920,
                geometry_y: 0,
                geometry_width: 2560,
                geometry_height: 1440,
                workarea_x: 1920,
                workarea_y: 0,
                workarea_width: 2560,
                workarea_height: 1440,
                scale_factor: 2.0,
                is_primary: false,
            },
        ];

        let zero_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 0.0, y: 0.0 }),
            tray_rect: Some(RawTrayRect {
                position: RawCoordinate2D::Physical { x: 0.0, y: 0.0 },
                size: RawSize2D::Physical {
                    width: 0.0,
                    height: 0.0,
                },
            }),
        };

        // Without an active-monitor hint, missing coordinates leave `monitor_index: None` so the
        // Wayland compositor places the fallback corner on the active output instead of forcing `monitors[0]`.
        let unhinted = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(zero_anchor),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(!unhinted.outcome.is_icon_anchored());
        assert_eq!(unhinted.gdk_position, None);
        let ls_unhinted = unhinted.layer_shell.expect("fallback layer_shell");
        assert!(ls_unhinted.is_compact_non_exclusive());
        assert_eq!(ls_unhinted.monitor_index, None);
        assert_eq!(
            unhinted.outcome.diagnostic_notice(),
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE)
        );

        // When `main` is active on Monitor 1 (`preferred_fallback_monitor_index = Some(1)`),
        // fallback targets Monitor 1's scale (2.0) rather than guessing `monitors[0]` (1.0),
        // and still reports `FallbackMonitorCorner` honestly.
        let hinted = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            None,
            Some(1),
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(!hinted.outcome.is_icon_anchored());
        assert_eq!(hinted.target_monitor.monitor_index, Some(1));
        assert_eq!(hinted.target_monitor.scale_factor, 2.0);
        assert_eq!(hinted.outcome.scale_factor(), 2.0);
        assert_eq!(
            hinted.layer_shell.expect("hinted ls").monitor_index,
            Some(1)
        );
        assert!(matches!(
            hinted.outcome,
            PopoverPlacementOutcome::FallbackMonitorCorner {
                reason: UnanchoredPlacementReason::MissingTrayCoordinates,
                ..
            }
        ));
    }

    #[test]
    fn wayland_unsupported_compositor_and_legacy_protocol_report_truthful_limits() {
        let monitors = [LinuxMonitorWorkarea {
            monitor_index: Some(0),
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: 0,
            workarea_y: 32,
            workarea_width: 1920,
            workarea_height: 1048,
            scale_factor: 2.0,
            is_primary: true,
        }];
        let valid_anchor = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 1880.0, y: 14.0 }),
            tray_rect: None,
        };

        // GNOME / Mutter (no zwlr_layer_shell_v1): CompositorManaged, no layer_shell, no move_().
        let gnome_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(valid_anchor),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(gnome_plan.gdk_position, None);
        assert_eq!(gnome_plan.layer_shell, None);
        assert!(!gnome_plan.outcome.is_icon_anchored());
        assert_eq!(
            gnome_plan.outcome,
            PopoverPlacementOutcome::CompositorManaged {
                scale_factor: 2.0,
                has_activation_coordinates: true,
                reason: UnanchoredPlacementReason::WaylandCompositorManaged,
                notice: WAYLAND_COMPOSITOR_PLACEMENT_NOTICE.to_string(),
            }
        );

        // Already-realized fallback: truthful realized notice, never claims icon anchor.
        let realized_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::WindowAlreadyRealized,
            ),
            &monitors,
            Some(valid_anchor),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(!realized_plan.outcome.is_icon_anchored());
        assert_eq!(
            realized_plan.effective_notice().as_deref(),
            Some(WAYLAND_REALIZED_FALLBACK_NOTICE)
        );

        // Legacy zwlr_layer_shell_v1 v3 (< v4): anchors to icon, surfaces limited outside-click notice.
        let legacy_proto_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 3,
            },
            &monitors,
            Some(valid_anchor),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(legacy_proto_plan.outcome.is_icon_anchored());
        assert_eq!(
            legacy_proto_plan
                .layer_shell
                .expect("v3 layer_shell")
                .keyboard_policy,
            WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick
        );
        assert_eq!(
            legacy_proto_plan.effective_notice().as_deref(),
            Some(WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE)
        );
    }

    #[test]
    fn wayland_focus_tracker_preserves_hover_from_icon_into_popup_and_dismisses_outside_click() {
        assert!(WAYLAND_FOCUS_LOSS_VERIFY_DELAY < crate::tray::POPOVER_BLUR_REOPEN_SUPPRESSION);
        let converted = LinuxMonitorWorkarea::from_monitor_bounds_px(
            MonitorBoundsPx {
                x: 3840,
                y: 0,
                width: 3840,
                height: 2160,
                scale_factor: 2.0,
            },
            Some(1),
            false,
        );
        assert_eq!(converted.geometry_x, 1920);
        assert_eq!(converted.geometry_width, 1920);
        assert_eq!(converted.geometry_height, 1080);

        let mut tracker = WaylandLayerFocusTracker::new();
        let gen1 = tracker.on_opening();
        assert_eq!(gen1, 1);

        // 1. Under `focus_follows_mouse yes`, moving diagonally from the tray bar across a gap into
        //    the popup triggers focus-out first, followed milliseconds later by pointer-enter.
        tracker.on_pointer_entered();
        // Moving from the outer GtkWindow onto the inner WebKitWebView emits LeaveNotify(Inferior),
        // which must keep `pointer_inside == true`.
        tracker.on_pointer_left(true);
        assert!(tracker.is_pointer_inside());
        assert_eq!(
            tracker.evaluate_deferred_unfocus(gen1, true, false),
            WaylandFocusCheckDecision::KeepOpen,
            "must never dismiss when pointer moved from tray icon into popup or child webview"
        );

        // 2. Pointer genuinely leaves the popup (`is_inferior == false`) and clicks outside so
        //    window is inactive -> deferred check dismisses cleanly.
        tracker.on_pointer_left(false);
        assert!(!tracker.is_pointer_inside());
        assert_eq!(
            tracker.evaluate_deferred_unfocus(gen1, true, false),
            WaylandFocusCheckDecision::DismissPopover
        );

        // 3. Reopening advances generation; stale timer from `gen1` cannot dismiss `gen2`.
        tracker.on_unmapped();
        let gen2 = tracker.on_opening();
        assert_eq!(gen2, 2);
        assert_eq!(
            tracker.evaluate_deferred_unfocus(gen1, true, false),
            WaylandFocusCheckDecision::KeepOpen,
            "stale focus-loss timer from previous open must be ignored"
        );
        // Nor does it dismiss if the window regained active keyboard focus.
        assert_eq!(
            tracker.evaluate_deferred_unfocus(gen2, true, true),
            WaylandFocusCheckDecision::KeepOpen
        );
    }

    #[test]
    fn x11_plan_uses_target_monitor_scale_and_distinguishes_icon_anchor_from_corner_fallback() {
        // Primary monitor is 2x HiDPI (physical 0..3840); secondary monitor on the right is 1x (physical 3840..5760).
        let monitors = [
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 2.0,
                is_primary: true,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 1920,
                workarea_y: 28,
                workarea_width: 1920,
                workarea_height: 1052,
                scale_factor: 1.0,
                is_primary: false,
            },
        ];

        // Click on secondary 1x monitor near its top-right panel at physical root (5720, 14):
        // must resolve to Monitor 2 (scale 1.0), NOT divide by Monitor 1's 2.0 scale!
        let second_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 5720.0, y: 14.0 }),
            tray_rect: None,
        };
        let anchored = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(second_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(anchored.outcome.is_icon_anchored());
        assert_eq!(anchored.target_monitor.geometry_x, 1920);
        assert_eq!(anchored.target_monitor.scale_factor, 1.0);
        assert_eq!(
            anchored.gdk_position,
            Some((1920 + 1920 - 360 - 12, 28 + 12))
        );
        assert_eq!(
            anchored.outcome,
            PopoverPlacementOutcome::IconAnchored {
                x: 3840 + (1920 - 360 - 12),
                y: 28 + 12,
                scale_factor: 1.0,
            }
        );
        assert_eq!(anchored.effective_notice(), None);

        // Logical icon rect on secondary 1x monitor at logical (3800, 4) size 24x24:
        let second_logical_rect = RawTrayAnchor {
            click_position: None,
            tray_rect: Some(RawTrayRect {
                position: RawCoordinate2D::Logical { x: 3800.0, y: 4.0 },
                size: RawSize2D::Logical {
                    width: 24.0,
                    height: 24.0,
                },
            }),
        };
        let rect_anchored = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(second_logical_rect),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(rect_anchored.outcome.is_icon_anchored());
        assert_eq!(rect_anchored.target_monitor.geometry_x, 1920);
        assert_eq!(rect_anchored.target_monitor.scale_factor, 1.0);
        assert_eq!(
            rect_anchored.gdk_position,
            Some((1920 + 1920 - 360 - 12, 28 + 12))
        );

        // Missing StatusNotifier coordinates (0, 0) on X11: falls back to corner and explicitly reports unanchored fallback.
        let zero_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical { x: 0.0, y: 0.0 }),
            tray_rect: None,
        };
        let fallback = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(zero_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(
            !fallback.outcome.is_icon_anchored(),
            "corner fallback must never be reported as genuine icon placement"
        );
        assert!(fallback.gdk_position.is_some());
        assert_eq!(
            fallback.outcome.diagnostic_notice(),
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE)
        );
    }

    #[test]
    fn linux_reopen_across_monitors_and_scale_factor_changes_updates_plan_cleanly() {
        let initial_monitors = [LinuxMonitorWorkarea {
            monitor_index: Some(0),
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: 0,
            workarea_y: 28,
            workarea_width: 1920,
            workarea_height: 1052,
            scale_factor: 1.0,
            is_primary: true,
        }];
        let first_open = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &initial_monitors,
            Some(RawTrayAnchor {
                click_position: Some(RawCoordinate2D::Physical { x: 1890.0, y: 14.0 }),
                tray_rect: None,
            }),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(
            first_open.outcome,
            PopoverPlacementOutcome::IconAnchored {
                x: 1920 - 360 - 12,
                y: 28 + 12,
                scale_factor: 1.0,
            }
        );

        // Same monitor changes to 2.0x HiDPI scale before next reopen.
        let scaled_monitors = [LinuxMonitorWorkarea {
            monitor_index: Some(0),
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: 0,
            workarea_y: 28,
            workarea_width: 1920,
            workarea_height: 1052,
            scale_factor: 2.0,
            is_primary: true,
        }];
        let second_open = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &scaled_monitors,
            Some(RawTrayAnchor {
                click_position: Some(RawCoordinate2D::Physical { x: 3780.0, y: 28.0 }),
                tray_rect: None,
            }),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(second_open.logical_size, (360, 320));
        assert_eq!(
            second_open.outcome,
            PopoverPlacementOutcome::IconAnchored {
                x: (1920 - 360 - 12) * 2,
                y: (28 + 12) * 2,
                scale_factor: 2.0,
            }
        );
    }

    #[test]
    fn linux_popover_size_clamps_to_tight_workarea_without_inheriting_main_min_height() {
        let compact_monitor = LinuxMonitorWorkarea {
            monitor_index: Some(0),
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 800,
            geometry_height: 360,
            workarea_x: 0,
            workarea_y: 28,
            workarea_width: 800,
            workarea_height: 332,
            scale_factor: 1.5,
            is_primary: true,
        };
        let (w, h) = compute_linux_popover_gdk_size(
            &compact_monitor,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(w, 360);
        assert_eq!(h, 332 - 24);
        assert!((h as f64) < MAIN_WINDOW_MIN_HEIGHT);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordedInputRegionState {
        DefaultFullUnshaped,
        ExplicitEmpty,
        BoundedRect {
            x: i32,
            y: i32,
            width: i32,
            height: i32,
        },
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordedSurfaceStep {
        RealizedWithEmptyInputRegion,
        MappedWhileEmpty,
        BoundedInputRegionEnabled { width: i32, height: i32 },
        EmptyInputRegionAppliedForCleanup,
        HandlersDisconnected,
        Hidden,
        Closed,
    }

    impl RecordedSurfaceStep {
        fn is_bounded_enable(self) -> bool {
            matches!(self, Self::BoundedInputRegionEnabled { .. })
        }
    }

    #[derive(Debug, Clone)]
    struct RecordedSurfaceEntry {
        handle_id: usize,
        surface_id: u64,
        generation: u64,
        monitor_index: usize,
        local_bounds: (i32, i32, i32, i32),
        realized: bool,
        mapped: bool,
        released: bool,
        closed: bool,
        input_region: RecordedInputRegionState,
        steps: Vec<RecordedSurfaceStep>,
    }

    #[derive(Clone, Copy)]
    enum MapConfigureBehavior {
        /// Emits configured workspace allocation `(spec.width, spec.height - 30)` synchronously
        /// inside `map_empty_surface` (matching `gtk-layer-shell` map roundtrip).
        SyncValidWorkarea,
        /// Emits `(1, 1)` placeholder inside `map_empty_surface`, leaving the surface in
        /// `MappedEmpty` until an explicit `drive_surface_configured` or timeout fires.
        AsyncPendingPlaceholder,
        /// Emits an invalid `(0, 0)` allocation synchronously inside `map_empty_surface`.
        SyncInvalidZeroSize,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum InjectedFailureStage {
        CreateRealize,
        MapSurface,
        EnableBoundedRegion,
    }

    enum ReentrantMapHook {
        None,
        CancelHideOnMonitor(usize),
        ReopenGenerationOnMonitor {
            trigger_monitor: usize,
            new_generation: u64,
            specs: Vec<WaylandOutsideClickBackdropSpec>,
        },
        SameGenRefreshOnMonitor {
            trigger_monitor: usize,
            specs: Vec<WaylandOutsideClickBackdropSpec>,
            base_notice: Option<String>,
            now: Instant,
        },
    }

    struct TrackerBackedPopup {
        tracker: WaylandLayerFocusTracker,
        window_visible: bool,
        window_mapped: bool,
        is_layer_window: bool,
        explicit_quit: bool,
    }

    type ReleaseReentrantHook = Box<dyn FnOnce(&CoordinatorTestHarness)>;

    struct HarnessSharedState {
        rgba_available: bool,
        popup_visible_generation: Option<u64>,
        tracker_popup: Option<TrackerBackedPopup>,
        map_behavior: MapConfigureBehavior,
        fail_monitor_and_stage: Option<(usize, InjectedFailureStage)>,
        reentrant_hook: ReentrantMapHook,
        on_ungrab_hook: Option<ReleaseReentrantHook>,
        on_release_hook: Option<ReleaseReentrantHook>,
        tracked_window_live_override: Option<bool>,
        tracked_window_display_override: Option<usize>,
        next_handle_id: usize,
        entries: Vec<RecordedSurfaceEntry>,
        scheduled_timeouts: Vec<(u64, u64, Duration)>,
        scheduled_pending_map_timeouts: Vec<(u64, u64, Duration)>,
        scheduled_idle_probes: Vec<(u64, u64)>,
        scheduled_on_demand_transitions: Vec<(u64, u64, WaylandKeyboardPolicy)>,
        popup_allocation_settled: bool,
        scheduled_settled_redraw_idles: Vec<(u64, u64)>,
        settled_popup_redraw_requests: Vec<(u64, u64)>,
        ungrab_calls: Vec<(usize, u32)>,
        ungrab_bounded_surface_counts: Vec<usize>,
        hidden_popup_epochs: Vec<(u64, u64)>,
        event_log: Vec<&'static str>,
    }

    impl HarnessSharedState {
        fn is_visible_for_gen(&self, generation: u64) -> bool {
            if let Some(tp) = &self.tracker_popup {
                !tp.explicit_quit
                    && tp.tracker.is_mapped()
                    && tp.tracker.current_generation() == generation
                    && tp.window_visible
                    && tp.window_mapped
                    && tp.is_layer_window
            } else {
                self.popup_visible_generation == Some(generation)
            }
        }
    }

    struct CoordinatorTestHarness {
        state_cell: std::rc::Rc<RefCell<OutsideClickCoordinatorState<usize>>>,
        shared: std::rc::Rc<RefCell<HarnessSharedState>>,
    }

    impl CoordinatorTestHarness {
        fn new(rgba_available: bool, map_behavior: MapConfigureBehavior) -> Self {
            Self {
                state_cell: std::rc::Rc::new(RefCell::new(OutsideClickCoordinatorState::new())),
                shared: std::rc::Rc::new(RefCell::new(HarnessSharedState {
                    rgba_available,
                    popup_visible_generation: None,
                    tracker_popup: None,
                    map_behavior,
                    fail_monitor_and_stage: None,
                    reentrant_hook: ReentrantMapHook::None,
                    on_ungrab_hook: None,
                    on_release_hook: None,
                    tracked_window_live_override: None,
                    tracked_window_display_override: None,
                    next_handle_id: 1,
                    entries: Vec::new(),
                    scheduled_timeouts: Vec::new(),
                    scheduled_pending_map_timeouts: Vec::new(),
                    scheduled_idle_probes: Vec::new(),
                    scheduled_on_demand_transitions: Vec::new(),
                    popup_allocation_settled: true,
                    scheduled_settled_redraw_idles: Vec::new(),
                    settled_popup_redraw_requests: Vec::new(),
                    ungrab_calls: Vec::new(),
                    ungrab_bounded_surface_counts: Vec::new(),
                    hidden_popup_epochs: Vec::new(),
                    event_log: Vec::new(),
                })),
            }
        }

        fn new_tracker_backed(rgba_available: bool, map_behavior: MapConfigureBehavior) -> Self {
            let harness = Self::new(rgba_available, map_behavior);
            harness.shared.borrow_mut().tracker_popup = Some(TrackerBackedPopup {
                tracker: WaylandLayerFocusTracker::new(),
                window_visible: false,
                window_mapped: false,
                is_layer_window: true,
                explicit_quit: false,
            });
            harness
        }

        fn set_popup_visible(&self, generation: Option<u64>) {
            self.shared.borrow_mut().popup_visible_generation = generation;
        }
    }

    impl OutsideClickSurfaceBackend for CoordinatorTestHarness {
        type SurfaceHandle = usize;

        fn has_rgba_visual(&self) -> bool {
            self.shared.borrow().rgba_available
        }

        fn is_popup_visible_for_generation(&self, generation: u64) -> bool {
            self.shared.borrow().is_visible_for_gen(generation)
        }

        fn create_realized_empty_surface(
            &self,
            surface_id: u64,
            generation: u64,
            spec: &WaylandOutsideClickBackdropSpec,
        ) -> Result<Self::SurfaceHandle, WaylandBackdropUnavailableReason> {
            let mut shared = self.shared.borrow_mut();
            if !shared.rgba_available {
                return Err(WaylandBackdropUnavailableReason::TransparentVisualUnavailable);
            }
            if shared.fail_monitor_and_stage
                == Some((spec.monitor_index, InjectedFailureStage::CreateRealize))
            {
                return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
            }
            let handle_id = shared.next_handle_id;
            shared.next_handle_id += 1;
            let mut entry = RecordedSurfaceEntry {
                handle_id,
                surface_id,
                generation,
                monitor_index: spec.monitor_index,
                local_bounds: spec.surface_local_input_bounds(),
                realized: false,
                mapped: false,
                released: false,
                closed: false,
                input_region: RecordedInputRegionState::DefaultFullUnshaped,
                steps: Vec::new(),
            };
            // Realize with an EXPLICIT EMPTY input region strictly before first map.
            entry.realized = true;
            entry.input_region = RecordedInputRegionState::ExplicitEmpty;
            entry
                .steps
                .push(RecordedSurfaceStep::RealizedWithEmptyInputRegion);
            shared.entries.push(entry);
            Ok(handle_id)
        }

        fn map_empty_surface(
            &self,
            surface: &Self::SurfaceHandle,
        ) -> Result<(), WaylandBackdropUnavailableReason> {
            // Proves `state_cell` is NOT borrowed across `map_empty_surface` AND that ownership
            // was registered in `state_cell` prior to mapping.
            let (surface_id, generation, monitor_index, local_bounds, behavior) = {
                let mut shared = self.shared.borrow_mut();
                let fail_stage = shared.fail_monitor_and_stage;
                let behavior = shared.map_behavior;
                let entry = shared
                    .entries
                    .iter_mut()
                    .find(|e| e.handle_id == *surface)
                    .expect("surface handle in ledger");
                assert!(
                    entry.realized && !entry.mapped,
                    "surface must be realized and unmapped before map_empty_surface"
                );
                assert_eq!(
                    entry.input_region,
                    RecordedInputRegionState::ExplicitEmpty,
                    "surface MUST have an explicit empty input region BEFORE its first map"
                );
                {
                    let coord = self.state_cell.borrow();
                    assert!(
                        coord.contains_surface(entry.surface_id),
                        "surface ownership must be registered before map_empty_surface"
                    );
                    assert_eq!(
                        coord.surface_phase(entry.surface_id),
                        Some(BackdropLifecyclePhase::CreatedEmpty)
                    );
                }
                if fail_stage == Some((entry.monitor_index, InjectedFailureStage::MapSurface)) {
                    return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
                }
                entry.mapped = true;
                entry.steps.push(RecordedSurfaceStep::MappedWhileEmpty);
                (
                    entry.surface_id,
                    entry.generation,
                    entry.monitor_index,
                    entry.local_bounds,
                    behavior,
                )
            };

            // Simulate synchronous GTK `size-allocate` arriving inside `show()`.
            match behavior {
                MapConfigureBehavior::SyncValidWorkarea => {
                    let configured_h = (local_bounds.3 - 30).max(32).min(local_bounds.3);
                    let _ = drive_surface_configured(
                        &self.state_cell,
                        self,
                        surface_id,
                        generation,
                        local_bounds.2,
                        configured_h,
                    );
                }
                MapConfigureBehavior::AsyncPendingPlaceholder => {
                    let _ = drive_surface_configured(
                        &self.state_cell,
                        self,
                        surface_id,
                        generation,
                        BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM,
                        BACKDROP_UNCONFIGURED_PLACEHOLDER_DIM,
                    );
                }
                MapConfigureBehavior::SyncInvalidZeroSize => {
                    let _ = drive_surface_configured(
                        &self.state_cell,
                        self,
                        surface_id,
                        generation,
                        0,
                        0,
                    );
                }
            }

            // Exercise optional reentrant GTK cancellation (`hide` / `unmap` / `monitor-removed`)
            // or reentrant new-generation admission inside `show()`.
            let hook = std::mem::replace(
                &mut self.shared.borrow_mut().reentrant_hook,
                ReentrantMapHook::None,
            );
            match hook {
                ReentrantMapHook::None => {}
                ReentrantMapHook::CancelHideOnMonitor(target_mon) => {
                    if monitor_index == target_mon {
                        self.shared.borrow_mut().popup_visible_generation = None;
                        drive_release_all(&self.state_cell, self);
                    } else {
                        self.shared.borrow_mut().reentrant_hook =
                            ReentrantMapHook::CancelHideOnMonitor(target_mon);
                    }
                }
                ReentrantMapHook::ReopenGenerationOnMonitor {
                    trigger_monitor,
                    new_generation,
                    specs,
                } => {
                    if monitor_index == trigger_monitor {
                        self.shared.borrow_mut().popup_visible_generation = Some(new_generation);
                        drive_sync_for_open(&self.state_cell, self, new_generation, true, &specs)
                            .expect("reentrant newer generation open must succeed");
                    } else {
                        self.shared.borrow_mut().reentrant_hook =
                            ReentrantMapHook::ReopenGenerationOnMonitor {
                                trigger_monitor,
                                new_generation,
                                specs,
                            };
                    }
                }
                ReentrantMapHook::SameGenRefreshOnMonitor {
                    trigger_monitor,
                    specs,
                    base_notice,
                    now,
                } => {
                    if monitor_index == trigger_monitor {
                        let refresh_res = drive_execute_popup_map_attempt(
                            &self.state_cell,
                            self,
                            PopupAttemptKind::Refresh,
                            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                            true,
                            &specs,
                            base_notice,
                            || now,
                            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                            |_gen, _ticket| {
                                let mut shared = self.shared.borrow_mut();
                                let tp = shared.tracker_popup.as_mut().expect("tracker popup");
                                tp.window_visible = true;
                                tp.window_mapped = true;
                                tp.is_layer_window = true;
                                Ok(())
                            },
                        )
                        .expect("nested same-generation refresh must succeed");
                        assert!(refresh_res.is_admitted_sync());
                    } else {
                        self.shared.borrow_mut().reentrant_hook =
                            ReentrantMapHook::SameGenRefreshOnMonitor {
                                trigger_monitor,
                                specs,
                                base_notice,
                                now,
                            };
                    }
                }
            }

            Ok(())
        }

        fn enable_bounded_input_region(
            &self,
            surface: &Self::SurfaceHandle,
            width: i32,
            height: i32,
        ) -> Result<(), WaylandBackdropUnavailableReason> {
            let mut shared = self.shared.borrow_mut();
            let fail_stage = shared.fail_monitor_and_stage;
            let entry_gen = shared
                .entries
                .iter()
                .find(|e| e.handle_id == *surface)
                .expect("surface handle in ledger")
                .generation;
            let is_visible_for_entry = shared.is_visible_for_gen(entry_gen);
            let entry = shared
                .entries
                .iter_mut()
                .find(|e| e.handle_id == *surface)
                .expect("surface handle in ledger");
            assert!(entry.realized && entry.mapped && !entry.closed);
            assert!(width > 0 && height > 0);
            assert!(width <= entry.local_bounds.2 && height <= entry.local_bounds.3);
            assert!(
                is_visible_for_entry,
                "bounded input region must only enable while popup is visible for generation"
            );
            {
                // Proves `state_cell` is NOT mutably borrowed across `enable_bounded_input_region`
                // and ownership is registered prior to enabling input capture.
                let coord = self.state_cell.borrow();
                assert_eq!(coord.active_generation(), Some(entry.generation));
                assert!(coord.contains_surface(entry.surface_id));
            }
            if fail_stage
                == Some((
                    entry.monitor_index,
                    InjectedFailureStage::EnableBoundedRegion,
                ))
            {
                return Err(WaylandBackdropUnavailableReason::SurfaceCreationFailed);
            }
            entry.input_region = RecordedInputRegionState::BoundedRect {
                x: 0,
                y: 0,
                width,
                height,
            };
            let enabled_step = RecordedSurfaceStep::BoundedInputRegionEnabled { width, height };
            entry.steps.push(enabled_step);
            Ok(())
        }

        fn schedule_configure_timeout(
            &self,
            generation: u64,
            admission_seq: u64,
            timeout: Duration,
        ) {
            // Proves `state_cell` is unborrowed when scheduling timeout.
            let _ = self.state_cell.borrow();
            self.shared
                .borrow_mut()
                .scheduled_timeouts
                .push((generation, admission_seq, timeout));
        }

        fn schedule_pending_map_timeout(
            &self,
            generation: u64,
            attempt_ticket: u64,
            timeout: Duration,
        ) {
            let _ = self.state_cell.borrow();
            self.shared
                .borrow_mut()
                .scheduled_pending_map_timeouts
                .push((generation, attempt_ticket, timeout));
        }

        fn schedule_pending_map_idle_probe(&self, generation: u64, attempt_ticket: u64) {
            let _ = self.state_cell.borrow();
            self.shared
                .borrow_mut()
                .scheduled_idle_probes
                .push((generation, attempt_ticket));
        }

        fn release_surface_fail_open(&self, surface: Self::SurfaceHandle) {
            // Proves `state_cell` is unborrowed during fail-open release.
            let _ = self.state_cell.borrow();
            let release_hook = {
                let mut shared = self.shared.borrow_mut();
                let already_released = {
                    let entry = shared
                        .entries
                        .iter_mut()
                        .find(|e| e.handle_id == surface)
                        .expect("surface handle in ledger");
                    // Always force explicit empty input shape first (`&empty_region`, 0 rectangles).
                    entry.input_region = RecordedInputRegionState::ExplicitEmpty;
                    if entry.released {
                        true
                    } else {
                        entry.released = true;
                        entry.mapped = false;
                        entry.closed = true;
                        entry
                            .steps
                            .push(RecordedSurfaceStep::EmptyInputRegionAppliedForCleanup);
                        entry.steps.push(RecordedSurfaceStep::HandlersDisconnected);
                        entry.steps.push(RecordedSurfaceStep::Hidden);
                        entry.steps.push(RecordedSurfaceStep::Closed);
                        false
                    }
                };
                if !already_released {
                    shared.event_log.push("surface_released_fail_open");
                }
                shared.on_release_hook.take()
            };
            if let Some(hook) = release_hook {
                hook(self);
            }
        }
    }

    impl PopupMapLifecycleBackend for CoordinatorTestHarness {
        fn begin_tracker_attempt<FReserve>(
            &self,
            kind: PopupAttemptKind,
            arm_layer_map: bool,
            reserve_ticket_fn: FReserve,
        ) -> Option<(u64, u64)>
        where
            FReserve: FnOnce() -> u64,
        {
            match kind {
                PopupAttemptKind::Open => {
                    let ticket = reserve_ticket_fn();
                    let mut shared = self.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().expect("tracker popup");
                    let generation = tp.tracker.begin_open_attempt(ticket, arm_layer_map);
                    Some((generation, ticket))
                }
                PopupAttemptKind::Refresh => {
                    let eligible = {
                        let shared = self.shared.borrow();
                        let tp = shared.tracker_popup.as_ref().expect("tracker popup");
                        !tp.explicit_quit && tp.tracker.has_live_or_pending_open()
                    };
                    if !eligible {
                        return None;
                    }
                    let ticket = reserve_ticket_fn();
                    let mut shared = self.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().expect("tracker popup");
                    tp.tracker
                        .begin_refresh_attempt(ticket, tp.explicit_quit, arm_layer_map)
                        .map(|generation| (generation, ticket))
                }
            }
        }

        fn abort_tracker_attempt(&self, generation: u64, attempt_ticket: u64) {
            let mut shared = self.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().expect("tracker popup");
            tp.tracker.abort_attempt_remap(generation, attempt_ticket);
        }

        fn disarm_expired_tracker_attempt(&self, generation: u64, attempt_ticket: u64) -> bool {
            let mut shared = self.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().expect("tracker popup");
            tp.tracker
                .disarm_expired_attempt_remap(generation, attempt_ticket)
        }

        fn complete_tracker_attempt_map(
            &self,
            generation: u64,
            attempt_ticket: u64,
            window_visible_and_mapped: bool,
        ) -> bool {
            let mut shared = self.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().expect("tracker popup");
            tp.tracker
                .complete_attempt_map(generation, attempt_ticket, window_visible_and_mapped)
        }

        fn schedule_on_demand_transition(
            &self,
            generation: u64,
            attempt_ticket: u64,
            keyboard_policy: WaylandKeyboardPolicy,
        ) {
            let _ = self.state_cell.borrow();
            self.shared
                .borrow_mut()
                .scheduled_on_demand_transitions
                .push((generation, attempt_ticket, keyboard_policy));
        }

        fn is_popup_content_hierarchy_settled(&self) -> bool {
            let _ = self.state_cell.borrow();
            self.shared.borrow().popup_allocation_settled
        }

        fn schedule_settled_popup_redraw_idle(&self, generation: u64, attempt_ticket: u64) {
            let _ = self.state_cell.borrow();
            self.shared
                .borrow_mut()
                .scheduled_settled_redraw_idles
                .push((generation, attempt_ticket));
        }

        fn queue_settled_popup_redraw(&self) {
            let latest_ticket = self.state_cell.borrow().latest_attempt_ticket();
            let mut shared = self.shared.borrow_mut();
            let generation = shared
                .tracker_popup
                .as_ref()
                .map(|tp| tp.tracker.current_generation())
                .or(shared.popup_visible_generation)
                .unwrap_or(0);
            shared
                .settled_popup_redraw_requests
                .push((generation, latest_ticket));
        }

        fn sample_popup_state(&self) -> (WaylandLayerFocusTracker, NativePopupMapSample) {
            let shared = self.shared.borrow();
            let tp = shared.tracker_popup.as_ref().expect("tracker popup");
            (
                tp.tracker.clone(),
                NativePopupMapSample {
                    window_visible: tp.window_visible,
                    window_mapped: tp.window_mapped,
                    is_layer_window: tp.is_layer_window,
                    explicit_quit_requested: tp.explicit_quit,
                },
            )
        }
    }

    impl PopupOutsideClickDismissBackend for CoordinatorTestHarness {
        type ToplevelWindow = usize;
        type NativeWindow = usize;
        type NativeDisplay = usize;
        type PointerDevice = usize;

        fn inspect_tracked_surface_window(
            &self,
            surface: &Self::SurfaceHandle,
        ) -> Option<
            TrackedBackdropWindowIdentity<
                Self::ToplevelWindow,
                Self::NativeWindow,
                Self::NativeDisplay,
            >,
        > {
            assert!(
                self.state_cell.try_borrow_mut().is_ok(),
                "state_cell must not be borrowed across inspect_tracked_surface_window"
            );
            let shared = self.shared.borrow();
            let entry = shared.entries.iter().find(|e| e.handle_id == *surface)?;
            let default_live = entry.realized && entry.mapped && !entry.released && !entry.closed;
            let is_live = shared.tracked_window_live_override.unwrap_or(default_live);
            let display = shared.tracked_window_display_override.unwrap_or(1);
            Some(TrackedBackdropWindowIdentity {
                toplevel: entry.handle_id,
                native_window: 10_000 + entry.handle_id,
                display,
                is_live,
            })
        }

        fn ungrab_master_pointer(&self, master_pointer: &Self::PointerDevice, event_time: u32) {
            assert!(
                self.state_cell.try_borrow_mut().is_ok(),
                "state_cell must not be borrowed across ungrab_master_pointer"
            );
            let ungrab_hook = {
                let mut shared = self.shared.borrow_mut();
                let mut live_bounded_count = 0usize;
                for entry in &shared.entries {
                    if entry.mapped && !entry.released && !entry.closed {
                        assert!(
                            matches!(
                                entry.input_region,
                                RecordedInputRegionState::BoundedRect { .. }
                            ),
                            "surface {} must retain bounded input region during ungrab",
                            entry.handle_id
                        );
                        assert!(
                            matches!(
                                entry.steps.last(),
                                Some(RecordedSurfaceStep::BoundedInputRegionEnabled { .. })
                            ),
                            "surface {} must not tear down before ungrab",
                            entry.handle_id
                        );
                        live_bounded_count += 1;
                    }
                }
                assert!(
                    live_bounded_count >= 1,
                    "at least one live bounded backdrop must exist when ungrabbing"
                );
                shared.ungrab_calls.push((*master_pointer, event_time));
                shared
                    .ungrab_bounded_surface_counts
                    .push(live_bounded_count);
                shared.event_log.push("master_pointer_ungrabbed");
                shared.on_ungrab_hook.take()
            };
            if let Some(hook) = ungrab_hook {
                hook(self);
            }
        }

        fn hide_popup_for_dismissed_epoch(&self, generation: u64, attempt_ticket: u64) {
            assert!(
                self.state_cell.try_borrow_mut().is_ok(),
                "state_cell must not be borrowed across hide_popup_for_dismissed_epoch"
            );
            let coord_exact = {
                let state = self.state_cell.borrow();
                state.active_generation.is_none()
                    && state.active_admission_seq.is_none()
                    && !state.admission_in_progress
                    && state.pending_map_admission.is_none()
                    && state.latest_attempt_ticket == attempt_ticket
                    && state.surfaces.is_empty()
            };
            if !coord_exact {
                return;
            }
            let mut shared = self.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().expect("tracker popup");
            if tp.explicit_quit
                || !tp.window_visible
                || !tp.window_mapped
                || !tp.is_layer_window
                || !tp.tracker.is_mapped()
                || tp.tracker.current_generation() != generation
                || tp.tracker.latest_attempt_ticket() != attempt_ticket
                || tp.tracker.is_fenced_or_superseded(generation)
                || tp.tracker.is_attempt_superseded(generation, attempt_ticket)
            {
                return;
            }
            tp.tracker.abort_attempt_remap(generation, attempt_ticket);
            tp.window_visible = false;
            tp.window_mapped = false;
            shared
                .hidden_popup_epochs
                .push((generation, attempt_ticket));
            shared.event_log.push("popup_hidden_for_dismissed_epoch");
        }
    }

    fn assert_all_surfaces_empty_and_closed(shared: &HarnessSharedState) {
        for entry in &shared.entries {
            assert_eq!((entry.local_bounds.0, entry.local_bounds.1), (0, 0));
            assert_eq!(
                entry.input_region,
                RecordedInputRegionState::ExplicitEmpty,
                "surface {} (gen {}, mon {}) must end with explicit empty input region",
                entry.handle_id,
                entry.generation,
                entry.monitor_index
            );
            assert!(
                entry.released && entry.closed && !entry.mapped,
                "surface {} (gen {}, mon {}) must be disconnected, hidden, and closed",
                entry.handle_id,
                entry.generation,
                entry.monitor_index
            );
            assert_eq!(
                entry.steps.first(),
                Some(&RecordedSurfaceStep::RealizedWithEmptyInputRegion)
            );
            let n = entry.steps.len();
            assert!(
                n >= 5,
                "released surface must record full fail-open sequence"
            );
            assert_eq!(
                &entry.steps[n - 4..],
                &[
                    RecordedSurfaceStep::EmptyInputRegionAppliedForCleanup,
                    RecordedSurfaceStep::HandlersDisconnected,
                    RecordedSurfaceStep::Hidden,
                    RecordedSurfaceStep::Closed,
                ]
            );
        }
    }

    #[test]
    fn wayland_v4_orders_overlay_above_top_across_negative_origin_monitors() {
        assert_ne!(LINUX_TRAY_WINDOW_ROLE, LINUX_TRAY_BACKDROP_WINDOW_ROLE);
        let monitors = [
            // Left 2x HiDPI monitor at negative global compositor coordinates (-1920, -240).
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: -1920,
                geometry_y: -240,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: -1920,
                workarea_y: -210,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 2.0,
                is_primary: false,
            },
            // Center 1x primary monitor at (0, 0).
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 28,
                workarea_width: 1920,
                workarea_height: 1052,
                scale_factor: 1.0,
                is_primary: true,
            },
            // Right 2x HiDPI monitor adjacent horizontally at x=1920 with negative Y offset (-120).
            LinuxMonitorWorkarea {
                monitor_index: Some(2),
                geometry_x: 1920,
                geometry_y: -120,
                geometry_width: 2560,
                geometry_height: 1440,
                workarea_x: 1920,
                workarea_y: -90,
                workarea_width: 2560,
                workarea_height: 1410,
                scale_factor: 2.0,
                is_primary: false,
            },
        ];

        // Waybar SNI Activate(-120, -226) near top-right of negative-origin 2x Monitor 0.
        let neg_origin_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical {
                x: -120.0,
                y: -226.0,
            }),
            tray_rect: None,
        };
        let plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(neg_origin_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(plan.validate_outside_click_plan());
        assert!(plan.outcome.is_icon_anchored());
        assert_eq!(plan.target_monitor.monitor_index, Some(0));
        assert_eq!(plan.target_monitor.scale_factor, 2.0);
        assert_eq!(plan.logical_size, (360, 320));

        let ls = plan.layer_shell.expect("v4 layer_shell placement");
        assert_eq!(ls.layer, WaylandShellLayer::Overlay);
        assert!(ls.is_compact_non_exclusive());
        assert_eq!(ls.estimated_gdk_bounds, (-372, -240, 360, 320));

        // Exactly one bounded Top backdrop per actual monitor, strictly ordered below Overlay
        // popup, with surface-local (0, 0, w, h) bounds and bounded allocation classification.
        assert_eq!(plan.outside_click_backdrops.len(), 3);
        for (expected_idx, backdrop) in plan.outside_click_backdrops.iter().enumerate() {
            assert_eq!(backdrop.monitor_index, expected_idx);
            assert_eq!(backdrop.layer, WaylandShellLayer::Top);
            assert!(ls.orders_above_backdrop(backdrop));
            assert_eq!(backdrop.exclusive_zone, 0);
            assert!(backdrop.keyboard_interactivity_none);
            assert!(backdrop.transparent_rgba);
            assert!(backdrop.validate_invariants());
            let (lx, ly, lw, lh) = backdrop.surface_local_input_bounds();
            assert_eq!((lx, ly), (0, 0));
            assert_eq!(lw, monitors[expected_idx].geometry_width);
            assert_eq!(lh, monitors[expected_idx].geometry_height);
            assert_eq!(
                backdrop.classify_allocation(1, 1),
                BackdropAllocationCheck::PendingUnconfigured
            );
            assert_eq!(
                backdrop.classify_allocation(lw, monitors[expected_idx].workarea_height),
                BackdropAllocationCheck::ValidBounded {
                    width: lw,
                    height: monitors[expected_idx].workarea_height,
                }
            );
            assert_eq!(
                backdrop.classify_allocation(0, lh),
                BackdropAllocationCheck::InvalidGeometry
            );
            assert_eq!(
                backdrop.classify_allocation(lw + 64, lh),
                BackdropAllocationCheck::InvalidGeometry
            );
        }

        // Physical-coordinate click on right-side 2x Monitor 2 with negative Y offset resolves to
        // Monitor 2 without overlapping center Monitor 1's physical span.
        let right_offset_click = RawTrayAnchor {
            click_position: Some(RawCoordinate2D::Physical {
                x: 1920.0 + 5000.0,
                y: -200.0,
            }),
            tray_rect: None,
        };
        let right_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(right_offset_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(right_plan.target_monitor.monitor_index, Some(2));
        assert_eq!(right_plan.target_monitor.scale_factor, 2.0);
        assert!(right_plan.validate_outside_click_plan());

        // Pathological 24-monitor array is capped at MAX_WAYLAND_OUTSIDE_CLICK_MONITORS.
        let many_monitors: Vec<LinuxMonitorWorkarea> = (0..24)
            .map(|idx| LinuxMonitorWorkarea {
                monitor_index: Some(idx),
                geometry_x: (idx as i32) * 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: (idx as i32) * 1920,
                workarea_y: 0,
                workarea_width: 1920,
                workarea_height: 1080,
                scale_factor: 1.0,
                is_primary: idx == 0,
            })
            .collect();
        let capped_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &many_monitors,
            None,
            Some(0),
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert_eq!(
            capped_plan.outside_click_backdrops.len(),
            MAX_WAYLAND_OUTSIDE_CLICK_MONITORS
        );
        assert!(capped_plan.validate_outside_click_plan());

        // Legacy zwlr_layer_shell_v1 (< v4), GNOME CompositorManaged, X11, and unenumerated
        // fallback keep outside_click_backdrops empty and independent.
        let legacy_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 3,
            },
            &monitors,
            Some(neg_origin_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(legacy_plan.outside_click_backdrops.is_empty());
        assert_eq!(
            legacy_plan.layer_shell.expect("legacy ls").layer,
            WaylandShellLayer::Top
        );

        let gnome_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(neg_origin_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(gnome_plan.outside_click_backdrops.is_empty());

        let x11_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::X11,
            WaylandLayerShellSupport::Unsupported(
                WaylandLayerShellUnsupportedReason::CompositorProtocolUnsupported,
            ),
            &monitors,
            Some(neg_origin_click),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(x11_plan.outside_click_backdrops.is_empty());

        let unenumerated_plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &[],
            None,
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );
        assert!(unenumerated_plan.outside_click_backdrops.is_empty());
    }

    #[test]
    fn wayland_lifecycle_realizes_empty_before_map_and_enables_after_configure() {
        let two_monitors = [
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: -1920,
                geometry_y: -240,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: -1920,
                workarea_y: -210,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 2.0,
                is_primary: false,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 1.0,
                is_primary: true,
            },
        ];
        let specs = plan_wayland_outside_click_backdrops(&two_monitors);
        assert_eq!(specs.len(), 2);

        // 1. Asynchronous compositor configuration path: surfaces realize empty -> map empty with
        //    (1, 1) placeholder -> remain ExplicitEmpty while awaiting configure -> schedule
        //    500ms bounded timeout -> enable bounded input region only after valid configure.
        let harness =
            CoordinatorTestHarness::new(true, MapConfigureBehavior::AsyncPendingPlaceholder);
        harness.set_popup_visible(Some(1));
        let async_outcome = drive_sync_for_open(&harness.state_cell, &harness, 1, true, &specs)
            .expect("admit gen 1");
        assert_eq!(
            async_outcome,
            BackdropSyncOutcome::Admitted {
                enabled_count: 0,
                pending_configure_count: 2,
            }
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(1));
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 2);
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 0);

        let (scheduled_gen, scheduled_seq, scheduled_dur) = {
            let shared = harness.shared.borrow();
            assert_eq!(shared.scheduled_timeouts.len(), 1);
            for entry in &shared.entries {
                assert_eq!(entry.input_region, RecordedInputRegionState::ExplicitEmpty);
                assert_eq!(
                    entry.steps,
                    vec![
                        RecordedSurfaceStep::RealizedWithEmptyInputRegion,
                        RecordedSurfaceStep::MappedWhileEmpty,
                    ]
                );
            }
            shared.scheduled_timeouts[0]
        };
        assert_eq!(scheduled_gen, 1);
        assert_eq!(scheduled_dur, WAYLAND_BACKDROP_CONFIGURE_TIMEOUT);

        // Deliver compositor configure for both monitors.
        let surface_ids: Vec<u64> = harness
            .shared
            .borrow()
            .entries
            .iter()
            .map(|e| e.surface_id)
            .collect();
        for sid in surface_ids {
            drive_surface_configured(&harness.state_cell, &harness, sid, 1, 1920, 1050)
                .expect("valid configure enables bounded region");
        }
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);
        for entry in &harness.shared.borrow().entries {
            assert_eq!(
                entry.input_region,
                RecordedInputRegionState::BoundedRect {
                    x: 0,
                    y: 0,
                    width: 1920,
                    height: 1050,
                }
            );
        }
        // Scheduled deadline firing after all surfaces configured is a clean no-op.
        assert_eq!(
            drive_configure_timeout(&harness.state_cell, &harness, scheduled_gen, scheduled_seq),
            Ok(false)
        );
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // 2. Reopen generation 2 with synchronous map roundtrip configure: gen 1 is released
        //    fail-open first, gen 2 realizes empty -> maps empty -> validates & enables bounded
        //    regions without scheduling an unnecessary pending timeout.
        harness.shared.borrow_mut().map_behavior = MapConfigureBehavior::SyncValidWorkarea;
        harness.set_popup_visible(Some(2));
        let sync_outcome = drive_sync_for_open(&harness.state_cell, &harness, 2, true, &specs)
            .expect("reopen gen 2");
        assert_eq!(
            sync_outcome,
            BackdropSyncOutcome::Admitted {
                enabled_count: 2,
                pending_configure_count: 0,
            }
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(2));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);
        assert_eq!(harness.shared.borrow().scheduled_timeouts.len(), 1);

        // Stale button-press carrying generation 1 must NOT dismiss generation 2.
        assert_eq!(
            drive_backdrop_pointer_event(
                &harness.state_cell,
                &harness,
                1,
                BackdropPointerEvent::ButtonPress { button: 1 },
            ),
            BackdropInputDecision::ReleaseStaleGenerationOnly
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(2));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // Scale refresh while hidden releases all surfaces and leaves zero active ownership;
        // repeated coordinator `drive_release_all` calls are idempotent.
        harness.set_popup_visible(None);
        assert_eq!(
            drive_sync_for_open(&harness.state_cell, &harness, 2, false, &specs)
                .expect("hidden refresh"),
            BackdropSyncOutcome::NotApplicable
        );
        drive_release_all(&harness.state_cell, &harness);
        assert_eq!(harness.state_cell.borrow().active_generation(), None);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_admission_cancellation_and_reentrant_reopen_leave_empty_surfaces() {
        let two_monitors: Vec<LinuxMonitorWorkarea> = (0..2)
            .map(|idx| LinuxMonitorWorkarea {
                monitor_index: Some(idx),
                geometry_x: (idx as i32) * 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: (idx as i32) * 1920,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 1.0,
                is_primary: idx == 0,
            })
            .collect();
        let specs = plan_wayland_outside_click_backdrops(&two_monitors);

        // 1. Reentrant hide/unmap/monitor-removed during Monitor 1's `map_empty_surface` after
        //    Monitor 0 already mapped and received sync configure: `state_cell` is NOT borrowed
        //    across `show()`, Monitor 0 was never enabled prematurely, and both partial surfaces
        //    are left explicitly empty and closed with zero active ownership.
        let cancel_harness =
            CoordinatorTestHarness::new(true, MapConfigureBehavior::SyncValidWorkarea);
        cancel_harness.set_popup_visible(Some(1));
        cancel_harness.shared.borrow_mut().reentrant_hook =
            ReentrantMapHook::CancelHideOnMonitor(1);

        let cancel_outcome =
            drive_sync_for_open(&cancel_harness.state_cell, &cancel_harness, 1, true, &specs)
                .expect("reentrant cancellation must not error or panic");
        assert_eq!(cancel_outcome, BackdropSyncOutcome::CancelledFailOpen);
        assert_eq!(cancel_harness.state_cell.borrow().active_generation(), None);
        assert_eq!(cancel_harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(
            cancel_harness.state_cell.borrow().enabled_surface_count(),
            0
        );
        {
            let shared = cancel_harness.shared.borrow();
            assert_eq!(shared.entries.len(), 2);
            for entry in &shared.entries {
                assert!(
                    !entry.steps.iter().any(|s| s.is_bounded_enable()),
                    "cancelled multi-monitor admission must never enable partial input shields"
                );
            }
            assert_all_surfaces_empty_and_closed(&shared);
        }

        // 2. Reentrant new-generation admission (`gen 2`) triggered during `gen 1`'s map: `gen 1`
        //    partial surfaces are emptied and destroyed without clobbering `gen 2`'s active
        //    ownership.
        let reopen_harness =
            CoordinatorTestHarness::new(true, MapConfigureBehavior::SyncValidWorkarea);
        reopen_harness.set_popup_visible(Some(1));
        reopen_harness.shared.borrow_mut().reentrant_hook =
            ReentrantMapHook::ReopenGenerationOnMonitor {
                trigger_monitor: 0,
                new_generation: 2,
                specs: specs.clone(),
            };

        let outer_outcome =
            drive_sync_for_open(&reopen_harness.state_cell, &reopen_harness, 1, true, &specs)
                .expect("outer superseded admission");
        assert_eq!(outer_outcome, BackdropSyncOutcome::CancelledFailOpen);
        assert_eq!(
            reopen_harness.state_cell.borrow().active_generation(),
            Some(2)
        );
        assert_eq!(reopen_harness.state_cell.borrow().active_surface_count(), 2);
        assert_eq!(
            reopen_harness.state_cell.borrow().enabled_surface_count(),
            2
        );
        {
            let shared = reopen_harness.shared.borrow();
            let gen1_entries: Vec<&RecordedSurfaceEntry> = shared
                .entries
                .iter()
                .filter(|e| e.generation == 1)
                .collect();
            assert_eq!(gen1_entries.len(), 1);
            assert_eq!(
                gen1_entries[0].input_region,
                RecordedInputRegionState::ExplicitEmpty
            );
            assert!(gen1_entries[0].closed);
        }
        drive_release_all(&reopen_harness.state_cell, &reopen_harness);
        assert_all_surfaces_empty_and_closed(&reopen_harness.shared.borrow());

        // 3. Popup hidden after async map before compositor configure arrives: late configure
        //    refuses to enable bounded input and tears down all surfaces fail-open.
        let hidden_before_cfg =
            CoordinatorTestHarness::new(true, MapConfigureBehavior::AsyncPendingPlaceholder);
        hidden_before_cfg.set_popup_visible(Some(3));
        drive_sync_for_open(
            &hidden_before_cfg.state_cell,
            &hidden_before_cfg,
            3,
            true,
            &specs,
        )
        .expect("mapped empty");
        let first_sid = hidden_before_cfg.shared.borrow().entries[0].surface_id;
        hidden_before_cfg.set_popup_visible(None);
        drive_surface_configured(
            &hidden_before_cfg.state_cell,
            &hidden_before_cfg,
            first_sid,
            3,
            1920,
            1050,
        )
        .expect("configure after popup hide cleans up cleanly");
        assert_eq!(
            hidden_before_cfg.state_cell.borrow().active_generation(),
            None
        );
        assert_eq!(
            hidden_before_cfg.state_cell.borrow().active_surface_count(),
            0
        );
        assert_all_surfaces_empty_and_closed(&hidden_before_cfg.shared.borrow());
    }

    #[test]
    fn wayland_second_monitor_failure_and_configure_timeout_fail_open_cleanly() {
        let two_monitors: Vec<LinuxMonitorWorkarea> = (0..2)
            .map(|idx| LinuxMonitorWorkarea {
                monitor_index: Some(idx),
                geometry_x: (idx as i32) * 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: (idx as i32) * 1920,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 1.0,
                is_primary: idx == 0,
            })
            .collect();
        let specs = plan_wayland_outside_click_backdrops(&two_monitors);

        // 1. Missing RGBA visual maps zero surfaces and reports accurate capability notice.
        let no_rgba = CoordinatorTestHarness::new(false, MapConfigureBehavior::SyncValidWorkarea);
        no_rgba.set_popup_visible(Some(1));
        let err_rgba = drive_sync_for_open(&no_rgba.state_cell, &no_rgba, 1, true, &specs)
            .expect_err("missing RGBA visual must fail cleanly");
        assert_eq!(
            err_rgba,
            WaylandBackdropUnavailableReason::TransparentVisualUnavailable
        );
        assert_eq!(
            err_rgba.capability_notice(),
            WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE
        );
        assert!(no_rgba.shared.borrow().entries.is_empty());

        // 2. Second-monitor failure after Monitor 0 succeeds across CreateRealize, MapSurface, and
        //    EnableBoundedRegion (where Monitor 0 already enabled its bounded region before
        //    Monitor 1 fails): all created surfaces end ExplicitEmpty + closed with zero ownership.
        for stage in [
            InjectedFailureStage::CreateRealize,
            InjectedFailureStage::MapSurface,
            InjectedFailureStage::EnableBoundedRegion,
        ] {
            let partial =
                CoordinatorTestHarness::new(true, MapConfigureBehavior::SyncValidWorkarea);
            partial.set_popup_visible(Some(1));
            partial.shared.borrow_mut().fail_monitor_and_stage = Some((1, stage));

            let err = drive_sync_for_open(&partial.state_cell, &partial, 1, true, &specs)
                .expect_err("second monitor failure must fail-open");
            assert_eq!(err, WaylandBackdropUnavailableReason::SurfaceCreationFailed);
            assert_eq!(
                err.capability_notice(),
                WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE
            );
            assert_eq!(partial.state_cell.borrow().active_generation(), None);
            assert_eq!(partial.state_cell.borrow().active_surface_count(), 0);
            assert_eq!(partial.state_cell.borrow().enabled_surface_count(), 0);
            assert!(!partial.shared.borrow().entries.is_empty());
            assert_all_surfaces_empty_and_closed(&partial.shared.borrow());
        }

        // 3. Finite bounded failure path when configuration never arrives on Monitor 1 even after
        //    Monitor 0 configured and enabled: timeout tears down BOTH monitors fail-open.
        let timeout_harness =
            CoordinatorTestHarness::new(true, MapConfigureBehavior::AsyncPendingPlaceholder);
        timeout_harness.set_popup_visible(Some(4));
        drive_sync_for_open(
            &timeout_harness.state_cell,
            &timeout_harness,
            4,
            true,
            &specs,
        )
        .expect("initial map pending configure");
        let mon0_sid = timeout_harness.shared.borrow().entries[0].surface_id;
        let (gen, seq, _) = timeout_harness.shared.borrow().scheduled_timeouts[0];
        drive_surface_configured(
            &timeout_harness.state_cell,
            &timeout_harness,
            mon0_sid,
            4,
            1920,
            1050,
        )
        .expect("mon 0 configures");
        assert_eq!(
            timeout_harness.state_cell.borrow().enabled_surface_count(),
            1
        );

        let timeout_err =
            drive_configure_timeout(&timeout_harness.state_cell, &timeout_harness, gen, seq)
                .expect_err("unconfigured second monitor must fail-open on deadline");
        assert_eq!(
            timeout_err,
            WaylandBackdropUnavailableReason::SurfaceCreationFailed
        );
        assert_eq!(
            timeout_harness.state_cell.borrow().active_generation(),
            None
        );
        assert_eq!(
            timeout_harness.state_cell.borrow().active_surface_count(),
            0
        );
        assert_eq!(
            timeout_harness.state_cell.borrow().enabled_surface_count(),
            0
        );
        assert_all_surfaces_empty_and_closed(&timeout_harness.shared.borrow());

        // 4. Invalid zero-sized compositor configuration fails open immediately.
        let bad_geom = CoordinatorTestHarness::new(true, MapConfigureBehavior::SyncInvalidZeroSize);
        bad_geom.set_popup_visible(Some(5));
        let geom_err = drive_sync_for_open(&bad_geom.state_cell, &bad_geom, 5, true, &specs)
            .expect_err("invalid (0, 0) allocation must fail-open");
        assert_eq!(
            geom_err,
            WaylandBackdropUnavailableReason::SurfaceCreationFailed
        );
        assert_eq!(bad_geom.state_cell.borrow().active_surface_count(), 0);
        assert_all_surfaces_empty_and_closed(&bad_geom.shared.borrow());
    }

    #[test]
    fn wayland_input_routing_and_first_outside_press_release_before_popup_hide() {
        let monitors = [
            LinuxMonitorWorkarea {
                monitor_index: Some(0),
                geometry_x: -1920,
                geometry_y: -240,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: -1920,
                workarea_y: -210,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 2.0,
                is_primary: false,
            },
            LinuxMonitorWorkarea {
                monitor_index: Some(1),
                geometry_x: 0,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: 0,
                workarea_y: 28,
                workarea_width: 1920,
                workarea_height: 1052,
                scale_factor: 1.0,
                is_primary: true,
            },
        ];
        let plan = plan_linux_popover_placement(
            LinuxDisplayBackend::Wayland,
            WaylandLayerShellSupport::Supported {
                protocol_version: 4,
            },
            &monitors,
            Some(RawTrayAnchor {
                click_position: Some(RawCoordinate2D::Physical {
                    x: -120.0,
                    y: -226.0,
                }),
                tray_rect: None,
            }),
            None,
            (TRAY_POPOVER_WIDTH, TRAY_POPOVER_HEIGHT),
        );

        let harness = CoordinatorTestHarness::new(true, MapConfigureBehavior::SyncValidWorkarea);
        harness.set_popup_visible(Some(1));
        drive_sync_for_open(
            &harness.state_cell,
            &harness,
            1,
            true,
            &plan.outside_click_backdrops,
        )
        .expect("admit v4 backdrops");
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // 1. Inside-popup click hits Layer::Overlay popup directly above Layer::Top backdrop.
        assert_eq!(
            plan.route_gdk_point(-200, -100),
            WaylandPointerHitTarget::PopupOverlay
        );
        let mut broken_same_layer_plan = plan.clone();
        broken_same_layer_plan.layer_shell.as_mut().unwrap().layer = WaylandShellLayer::Top;
        assert!(!broken_same_layer_plan.validate_outside_click_plan());

        // 2. Pointer motion and non-initial button events never dismiss or alter bounded regions.
        assert_eq!(
            plan.route_gdk_point(-1200, 200),
            WaylandPointerHitTarget::OutsideBackdrop {
                monitor_index: 0,
                surface_local_x: 720,
                surface_local_y: 440,
            }
        );
        assert_eq!(
            drive_backdrop_pointer_event(
                &harness.state_cell,
                &harness,
                1,
                BackdropPointerEvent::PointerMotion,
            ),
            BackdropInputDecision::IgnoreKeepOpen
        );
        assert_eq!(
            drive_backdrop_pointer_event(
                &harness.state_cell,
                &harness,
                1,
                BackdropPointerEvent::NonInitialButtonEvent,
            ),
            BackdropInputDecision::IgnoreKeepOpen
        );
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // 3. First genuine outside ButtonPress releases ALL surfaces fail-open BEFORE the caller
        //    hides the popup, and consumes only that single dismissal click.
        assert_eq!(
            plan.route_gdk_point(900, 500),
            WaylandPointerHitTarget::OutsideBackdrop {
                monitor_index: 1,
                surface_local_x: 900,
                surface_local_y: 500,
            }
        );
        let decision = drive_backdrop_pointer_event(
            &harness.state_cell,
            &harness,
            1,
            BackdropPointerEvent::ButtonPress { button: 1 },
        );
        assert_eq!(
            decision,
            BackdropInputDecision::ConsumeClickAndDismissPopover
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), None);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        harness
            .shared
            .borrow_mut()
            .event_log
            .push("popup_hidden_by_caller");
        {
            let shared = harness.shared.borrow();
            assert_eq!(
                shared.event_log,
                vec![
                    "surface_released_fail_open",
                    "surface_released_fail_open",
                    "popup_hidden_by_caller",
                ]
            );
            assert_all_surfaces_empty_and_closed(&shared);
        }

        // 4. Trailing button-press from the dismissed generation is not consumed as a second click.
        assert_eq!(
            drive_backdrop_pointer_event(
                &harness.state_cell,
                &harness,
                1,
                BackdropPointerEvent::ButtonPress { button: 1 },
            ),
            BackdropInputDecision::ReleaseStaleGenerationOnly
        );
    }

    #[test]
    fn wayland_controlled_remap_restores_admission_across_monitor_switch_and_hotplug() {
        let mon_primary_1x = LinuxMonitorWorkarea {
            monitor_index: Some(0),
            geometry_x: 0,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: 0,
            workarea_y: 28,
            workarea_width: 1920,
            workarea_height: 1052,
            scale_factor: 1.0,
            is_primary: true,
        };
        let mon_secondary_2x = LinuxMonitorWorkarea {
            monitor_index: Some(1),
            geometry_x: 1920,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: 1920,
            workarea_y: 30,
            workarea_width: 1920,
            workarea_height: 1050,
            scale_factor: 2.0,
            is_primary: false,
        };
        let mon_tertiary_1x = LinuxMonitorWorkarea {
            monitor_index: Some(2),
            geometry_x: -1920,
            geometry_y: 0,
            geometry_width: 1920,
            geometry_height: 1080,
            workarea_x: -1920,
            workarea_y: 28,
            workarea_width: 1920,
            workarea_height: 1052,
            scale_factor: 1.0,
            is_primary: false,
        };

        let specs_one = plan_wayland_outside_click_backdrops(&[mon_primary_1x]);
        let specs_two = plan_wayland_outside_click_backdrops(&[mon_primary_1x, mon_secondary_2x]);
        let specs_three = plan_wayland_outside_click_backdrops(&[
            mon_primary_1x,
            mon_secondary_2x,
            mon_tertiary_1x,
        ]);

        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );

        // 1. Initial open on Monitor 0 (1x): arm controlled remap -> show/map -> complete remap ->
        //    admit Monitor 0 backdrop.
        let gen1 = {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            let gen = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(gen));
            tp.window_visible = true;
            tp.window_mapped = true;
            assert!(tp.tracker.complete_controlled_remap(gen, true));
            gen
        };
        let open1_res = drive_sync_for_open(&harness.state_cell, &harness, gen1, true, &specs_one);
        assert_eq!(
            open1_res,
            Ok(BackdropSyncOutcome::Admitted {
                enabled_count: 1,
                pending_configure_count: 0,
            })
        );
        {
            let shared = harness.shared.borrow();
            let tp = shared.tracker_popup.as_ref().unwrap();
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(
                    Some(open1_res),
                    &tp.tracker,
                    gen1,
                    tp.window_visible,
                    tp.explicit_quit,
                ),
                None
            );
        }

        // 2. Reopen on Monitor 1 (2x) while already mapped (`LayerShell::set_monitor` synchronously
        //    emits `connect_unmap` -> `on_transient_unmap` + nested `ScaleFactorChanged` refresh ->
        //    remap): `mapped` is cleared during unmap and restored cleanly on controlled-remap
        //    completion without returning `CancelledFailOpen`.
        let gen2 = {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            let gen = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(gen));
            gen
        };
        drive_release_all(&harness.state_cell, &harness);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_mapped = false;
            tp.tracker.on_transient_unmap();
            assert!(!tp.tracker.is_mapped());
        }
        assert!(!harness.is_popup_visible_for_generation(gen2));

        // Nested `ScaleFactorChanged` refresh firing inside `set_monitor` / `show()` preserves the
        // outer controlled remap token across depth 2 -> 1 -> 0.
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_mapped = true;
            assert!(tp.tracker.begin_controlled_remap(gen2));
            tp.tracker.on_transient_unmap();
            assert!(tp.tracker.complete_controlled_remap(gen2, true));
            assert!(tp.tracker.complete_controlled_remap(gen2, true));
            assert!(tp.tracker.is_mapped());
        }
        let open2_res = drive_sync_for_open(&harness.state_cell, &harness, gen2, true, &specs_two);
        assert_eq!(
            open2_res,
            Ok(BackdropSyncOutcome::Admitted {
                enabled_count: 2,
                pending_configure_count: 0,
            })
        );
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // 3. Hotplug Monitor 2 while `gen2` remains visible (`refresh_linux_popover_size_on_scale_change`
        //    without advancing `open_generation`): `set_monitor` unmap/remap restores `mapped` and
        //    admits all 3 monitors.
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            assert!(tp.tracker.begin_controlled_remap(gen2));
        }
        drive_release_all(&harness.state_cell, &harness);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.tracker.on_transient_unmap();
            assert!(!tp.tracker.is_mapped());
            assert!(tp.tracker.complete_controlled_remap(gen2, true));
            assert!(tp.tracker.is_mapped());
        }
        let hotplug_res =
            drive_sync_for_open(&harness.state_cell, &harness, gen2, true, &specs_three);
        assert_eq!(
            hotplug_res,
            Ok(BackdropSyncOutcome::Admitted {
                enabled_count: 3,
                pending_configure_count: 0,
            })
        );
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 3);

        // 4. Arbitrary unsolicited `unmap` outside a controlled remap fences `gen2` and cannot be
        //    resurrected by a subsequent scale refresh or arbitrary map event.
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_mapped = false;
            tp.tracker.on_transient_unmap();
            assert!(!tp.tracker.is_mapped());
            assert!(tp.tracker.is_fenced_or_superseded(gen2));
            assert!(!tp.tracker.begin_controlled_remap(gen2));
            assert!(!tp.tracker.complete_controlled_remap(gen2, true));
            assert!(!tp.tracker.is_mapped());
        }
        drive_release_all(&harness.state_cell, &harness);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_controlled_remap_fences_hide_newer_generation_and_reports_visible_failure() {
        let two_monitors: Vec<LinuxMonitorWorkarea> = (0..2)
            .map(|idx| LinuxMonitorWorkarea {
                monitor_index: Some(idx),
                geometry_x: (idx as i32) * 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: (idx as i32) * 1920,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: 1.0,
                is_primary: idx == 0,
            })
            .collect();
        let specs = plan_wayland_outside_click_backdrops(&two_monitors);

        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );

        // 1. Genuine `hide` (`on_unmapped`) during controlled remap stays fenced: `mapped` remains
        //    `false`, zero surfaces are admitted, and expected cancellation emits no error notice.
        let gen1 = {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            let gen = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(gen));
            tp.tracker.on_transient_unmap();
            // Reentrant genuine hide during remap:
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
            assert!(!tp.tracker.complete_controlled_remap(gen, true));
            assert!(!tp.tracker.is_mapped());
            assert!(tp.tracker.is_fenced_or_superseded(gen));
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(None, &tp.tracker, gen, false, false),
                None
            );
            gen
        };
        assert_eq!(gen1, 1);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);

        // 2. Reentrant newer open (`gen3`) occurring during `gen2`'s controlled remap: `gen3`
        //    admits and retains its surfaces, while outer `gen2` is fenced without clobbering
        //    `gen3` or reporting a false failure.
        let (gen2, gen3) = {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            let g2 = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(g2));
            tp.tracker.on_transient_unmap();

            // Newer open supersedes `g2` before `g2` completes its remap:
            let g3 = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(g3));
            tp.window_visible = true;
            tp.window_mapped = true;
            assert!(tp.tracker.complete_controlled_remap(g3, true));
            (g2, g3)
        };
        assert_eq!((gen2, gen3), (2, 3));
        let gen3_res = drive_sync_for_open(&harness.state_cell, &harness, gen3, true, &specs);
        assert_eq!(
            gen3_res,
            Ok(BackdropSyncOutcome::Admitted {
                enabled_count: 2,
                pending_configure_count: 0,
            })
        );

        // Outer `gen2` unwinds: fenced in both `complete_controlled_remap` and `drive_sync_for_open`.
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            assert!(!tp.tracker.complete_controlled_remap(gen2, true));
            assert_eq!(tp.tracker.current_generation(), gen3);
            assert!(tp.tracker.is_mapped());
        }
        let stale_gen2_res = drive_sync_for_open(&harness.state_cell, &harness, gen2, true, &specs);
        assert_eq!(stale_gen2_res, Ok(BackdropSyncOutcome::CancelledFailOpen));
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(gen3));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);
        {
            let shared = harness.shared.borrow();
            let tp = shared.tracker_popup.as_ref().unwrap();
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(
                    Some(stale_gen2_res),
                    &tp.tracker,
                    gen2,
                    true,
                    false,
                ),
                None
            );
        }

        // 3. Active-visible capability failure vs expected cancellation: if the popup remains
        //    visibly open and unfenced on `gen4`, a failed remap or unexpected `CancelledFailOpen`
        //    reports `WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE` truthfully, whereas
        //    explicit quit or genuine hide does not.
        drive_release_all(&harness.state_cell, &harness);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            let gen4 = tp.tracker.on_opening();
            assert!(tp.tracker.begin_controlled_remap(gen4));
            tp.window_visible = true;
            tp.window_mapped = false;
            assert!(!tp.tracker.complete_controlled_remap(gen4, false));
            assert!(!tp.tracker.is_fenced_or_superseded(gen4));

            assert_eq!(
                resolve_wayland_backdrop_admission_notice(None, &tp.tracker, gen4, true, false),
                Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
            );
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(
                    Some(Ok(BackdropSyncOutcome::CancelledFailOpen)),
                    &tp.tracker,
                    gen4,
                    true,
                    false,
                ),
                Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
            );
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(None, &tp.tracker, gen4, true, true),
                None,
                "explicit quit during admission is expected cancellation, not capability failure"
            );

            tp.tracker.abort_controlled_remap(gen4);
            assert!(tp.tracker.is_fenced_or_superseded(gen4));
            assert_eq!(
                resolve_wayland_backdrop_admission_notice(None, &tp.tracker, gen4, false, false),
                None
            );
        }
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_cairo_empty_region_is_genuinely_empty_and_distinct_from_bounded_rect() {
        let empty = create_fail_open_empty_input_region();
        assert!(empty.is_empty());
        assert_eq!(empty.num_rectangles(), 0);
        assert!(!empty.contains_point(0, 0));
        assert!(!empty.contains_point(100, 100));

        assert!(create_bounded_surface_input_region(0, 1080).is_none());
        let bounded = create_bounded_surface_input_region(1920, 1050).expect("bounded region");
        assert!(!bounded.is_empty());
        assert_eq!(bounded.num_rectangles(), 1);
        assert!(bounded.contains_point(0, 0));
        assert!(bounded.contains_point(1600, 100));
        assert!(!bounded.contains_point(1920, 1050));

        // Explicit known-bounds subtraction via gtk::cairo::Region::subtract_rectangle works
        // cleanly without any hardcoded panel strip.
        bounded
            .subtract_rectangle(&gtk::cairo::RectangleInt::new(1548, 0, 360, 320))
            .expect("subtract known rectangle");
        assert!(!bounded.contains_point(1600, 100));
        assert!(bounded.contains_point(200, 100));
    }

    fn build_pending_map_test_specs(count: usize) -> Vec<WaylandOutsideClickBackdropSpec> {
        let monitors: Vec<LinuxMonitorWorkarea> = (0..count)
            .map(|idx| LinuxMonitorWorkarea {
                monitor_index: Some(idx),
                geometry_x: (idx as i32) * 1920,
                geometry_y: 0,
                geometry_width: 1920,
                geometry_height: 1080,
                workarea_x: (idx as i32) * 1920,
                workarea_y: 30,
                workarea_width: 1920,
                workarea_height: 1050,
                scale_factor: if idx % 2 == 0 { 1.0 } else { 2.0 },
                is_primary: idx == 0,
            })
            .collect();
        plan_wayland_outside_click_backdrops(&monitors)
    }

    fn tracker_is_mapped(harness: &CoordinatorTestHarness) -> bool {
        harness
            .shared
            .borrow()
            .tracker_popup
            .as_ref()
            .unwrap()
            .tracker
            .is_mapped()
    }

    fn tracker_is_fenced(harness: &CoordinatorTestHarness, generation: u64) -> bool {
        harness
            .shared
            .borrow()
            .tracker_popup
            .as_ref()
            .unwrap()
            .tracker
            .is_fenced_or_superseded(generation)
    }

    fn set_tracker_window_flags(harness: &CoordinatorTestHarness, visible: bool, mapped: bool) {
        let mut shared = harness.shared.borrow_mut();
        let tp = shared.tracker_popup.as_mut().unwrap();
        tp.window_visible = visible;
        tp.window_mapped = mapped;
    }

    #[test]
    fn wayland_queued_show_returns_unmapped_and_later_connect_map_admits_on_reserved_ticket() {
        let specs = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // Simulate Tauri `popover.show()` queuing `WindowRequest::Visible(true)` on GLib without a
        // synchronous GTK map barrier (plus a controlled `LayerShell::set_monitor` transient unmap).
        let exec = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs,
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |generation, attempt_ticket| {
                assert_eq!((generation, attempt_ticket), (1, 1));
                let preserved = {
                    let mut shared = harness.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.is_layer_window = true;
                    tp.tracker.on_transient_unmap_preserving_epoch()
                };
                assert_eq!(preserved, Some((1, 1)));
                drive_popup_transient_unmap(&harness.state_cell, &harness, preserved);
                Ok(())
            },
        )
        .expect("queued open attempt must succeed");

        assert_eq!(
            exec,
            PopupAttemptExecutionOutcome::ArmedPendingMap {
                effective_notice: Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            }
        );
        {
            let state = harness.state_cell.borrow();
            assert_eq!(state.pending_map_ticket(), Some((1, 1)));
            assert_eq!(state.latest_attempt_ticket(), 1);
            assert_eq!(state.active_generation(), None);
            assert_eq!(state.active_admission_seq(), None);
            assert_eq!(state.active_surface_count(), 0);
            assert_eq!(state.enabled_surface_count(), 0);
        }
        {
            let shared = harness.shared.borrow();
            let tp = shared.tracker_popup.as_ref().unwrap();
            assert!(!tp.tracker.is_mapped());
            assert!(tp.tracker.has_live_or_pending_open());
            assert_eq!(tp.tracker.latest_attempt_ticket(), 1);
            assert_eq!(
                shared.scheduled_pending_map_timeouts,
                vec![(1, 1, WAYLAND_POPUP_PENDING_MAP_TIMEOUT)]
            );
            assert!(shared.scheduled_idle_probes.is_empty());
        }

        // `connect_map` arriving before `is_visible()` / layer flags settle schedules at most one
        // idle probe across repeated signal invocations while retaining `PendingPopupMapAdmission`.
        set_tracker_window_flags(&harness, false, true);
        let map_early = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(15),
        );
        assert_eq!(
            map_early,
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(985),
            }
        );
        let map_repeat = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(20),
        );
        assert_eq!(
            map_repeat,
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(980),
            }
        );
        assert_eq!(harness.shared.borrow().scheduled_idle_probes, vec![(1, 1)]);
        assert_eq!(
            harness.state_cell.borrow().pending_map_ticket(),
            Some((1, 1))
        );

        // Once GTK processes `window.show_all()`, native `visible + mapped + layer` settles and
        // the single idle probe admits all monitors on the SAME pre-reserved ticket `(1, 1)`.
        set_tracker_window_flags(&harness, true, true);
        let idle_done = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(25),
        );
        assert_eq!(
            idle_done,
            PendingMapResolution::Finalized {
                admitted: true,
                capability_notice: None,
                effective_notice: Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
                should_publish_notice: true,
            }
        );
        {
            let state = harness.state_cell.borrow();
            assert_eq!(state.pending_map_ticket(), None);
            assert_eq!(state.active_generation(), Some(1));
            assert_eq!(state.active_admission_seq(), Some(1));
            assert_eq!(state.latest_attempt_ticket(), 1);
            assert_eq!(state.active_surface_count(), 2);
            assert_eq!(state.enabled_surface_count(), 2);
        }
        assert!(tracker_is_mapped(&harness));

        // Subsequent firing of the `1000ms` pending-map timer is a clean no-op.
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        drive_release_all(&harness.state_cell, &harness);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_pending_map_rejects_hide_quit_unsolicited_unmap_and_newer_ticket_before_map() {
        let specs_two = build_pending_map_test_specs(2);
        let specs_three = build_pending_map_test_specs(3);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // 1. Genuine `hide` before `connect_map` cancels `(gen = 1, ticket = 1)`.
        let open1 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(open1.is_armed_pending());
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        drive_release_all(&harness.state_cell, &harness);
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                1,
                1,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(10),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);

        // 2. Explicit `quit` before map aborts `(gen = 2, ticket = 2)` even if GTK flips visible+mapped.
        let open2 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(open2.is_armed_pending());
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.explicit_quit = true;
            tp.window_visible = true;
            tp.window_mapped = true;
        }
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                2,
                2,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(10),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.explicit_quit = false;
            tp.window_visible = false;
            tp.window_mapped = false;
        }

        // 3. Unsolicited `unmap` outside a controlled remap fences `gen = 3` and rejects refresh.
        let open3 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert!(open3.is_admitted_sync());
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);
        let preserved = {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_mapped = false;
            tp.tracker.on_transient_unmap_preserving_epoch()
        };
        assert_eq!(preserved, None);
        drive_popup_transient_unmap(&harness.state_cell, &harness, preserved);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(
            drive_execute_popup_map_attempt(
                &harness.state_cell,
                &harness,
                PopupAttemptKind::Refresh,
                Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                true,
                &specs_two,
                None,
                || t0,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |_, _| Ok(()),
            )
            .unwrap(),
            PopupAttemptExecutionOutcome::NotActive
        );

        // 4. Same-generation newer `Refresh` ticket `(4, 5)` while `Open` `(4, 4)` is still queued
        //    unmapped: stale `(4, 4)` callbacks reject without clearing `(4, 5)`'s pending map.
        set_tracker_window_flags(&harness, false, false);
        let open4 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (4, 4));
                Ok(())
            },
        )
        .unwrap();
        assert!(open4.is_armed_pending());
        let refresh4 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_three,
            None,
            || t0 + Duration::from_millis(5),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (4, 5));
                Ok(())
            },
        )
        .unwrap();
        assert!(refresh4.is_armed_pending());
        assert_eq!(
            harness.state_cell.borrow().pending_map_ticket(),
            Some((4, 5))
        );

        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                4,
                4,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(10),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                4,
                4,
                t0 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            harness.state_cell.borrow().pending_map_ticket(),
            Some((4, 5))
        );

        set_tracker_window_flags(&harness, true, true);
        let refresh4_map_signal = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            4,
            5,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(20),
        );
        assert!(matches!(
            refresh4_map_signal,
            PendingMapResolution::StillPending { .. }
        ));
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_idle_probes
                .last()
                .copied(),
            Some((4, 5))
        );
        let refresh4_done = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            4,
            5,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(22),
        );
        assert!(refresh4_done.is_admitted_finalized());
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(5));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 3);

        // 5. Newer generation `Open` `(6, 7)` rejects stale pending callbacks from `(5, 6)`.
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        drive_release_all(&harness.state_cell, &harness);
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (5, 6));
                Ok(())
            },
        )
        .unwrap();
        let open6 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (6, 7));
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert!(open6.is_admitted_sync());
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(30),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(6));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        drive_release_all(&harness.state_cell, &harness);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_pending_map_deadline_has_zero_inputs_reports_failure_and_blocks_resurrection() {
        let specs = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // Popup becomes visible (`gtk_win.is_visible() == true`), yet native layer map never
        // completes within `WAYLAND_POPUP_PENDING_MAP_TIMEOUT` (`1000ms`).
        let open1 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs,
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, false);
                Ok(())
            },
        )
        .unwrap();
        assert!(open1.is_armed_pending());
        assert!(harness.shared.borrow().entries.is_empty());
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);

        // Before `1000ms`, deadline probe keeps waiting with exact remaining duration.
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + Duration::from_millis(400),
            ),
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(600),
            }
        );

        // At `1000ms`, deadline finalizes with 0 input surfaces and truthfully reports active-visible
        // capability failure combined with the base coordinate notice.
        let expected_combined = combine_popover_notices(
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE),
            Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
        );
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: expected_combined,
                should_publish_notice: true,
            }
        );
        assert_eq!(harness.state_cell.borrow().pending_map_ticket(), None);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 0);
        assert!(harness.shared.borrow().entries.is_empty());

        // Late `connect_map`, idle probe, or scale refresh arriving AFTER deadline expiry cannot
        // resurrect `mapped = true` or admit outside-click backdrops.
        set_tracker_window_flags(&harness, true, true);
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                1,
                1,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(1050),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                1,
                1,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(1060),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            assert!(!tp.tracker.complete_attempt_map(1, 1, true));
            assert!(!tp.tracker.is_mapped());
            assert!(!tp.tracker.has_live_or_pending_open());
        }
        assert_eq!(
            drive_execute_popup_map_attempt(
                &harness.state_cell,
                &harness,
                PopupAttemptKind::Refresh,
                Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                true,
                &specs,
                None,
                || t0 + Duration::from_millis(1070),
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |_, _| Ok(()),
            )
            .unwrap(),
            PopupAttemptExecutionOutcome::NotActive
        );
        assert!(harness.shared.borrow().entries.is_empty());

        // Non-visible popup at deadline expiry stays silent (`should_publish_notice == false`).
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs,
            None,
            || t0 + Duration::from_millis(1100),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, false, false);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                2,
                2,
                t0 + Duration::from_millis(2200),
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: None,
                effective_notice: None,
                should_publish_notice: false,
            }
        );
    }

    #[test]
    fn wayland_same_gen_nested_reentry_in_preteardown_remap_and_map_preserves_newer_owner() {
        let specs_one = build_pending_map_test_specs(1);
        let specs_two = build_pending_map_test_specs(2);
        let specs_three = build_pending_map_test_specs(3);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // Stage A: Scoped `drive_release_all_with` teardown reentering a newer open preserves the
        // newer owner's surfaces and exposes `drained_epoch < latest_attempt_ticket()`.
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_one,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 1);

        let specs_two_for_teardown = specs_two.clone();
        harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
            let nested = drive_execute_popup_map_attempt(
                &h.state_cell,
                h,
                PopupAttemptKind::Open,
                Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                true,
                &specs_two_for_teardown,
                None,
                || t0,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |_, _| {
                    set_tracker_window_flags(h, true, true);
                    Ok(())
                },
            )
            .unwrap();
            assert!(nested.is_admitted_sync());
        }));
        let drained_epoch = drive_release_all_with(&harness.state_cell, |handle| {
            harness.release_surface_fail_open(handle);
        });
        assert_eq!(drained_epoch, 1);
        assert_eq!(harness.state_cell.borrow().latest_attempt_ticket(), 2);
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(2));
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(2));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // Stage B: Same-generation nested `Refresh` (`ticket = 4`) during outer `Refresh`
        // (`ticket = 3`) pre-teardown (`drive_release_up_to_attempt`).
        let specs_three_for_pre = specs_three.clone();
        harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
            let inner_refresh = drive_execute_popup_map_attempt(
                &h.state_cell,
                h,
                PopupAttemptKind::Refresh,
                Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                true,
                &specs_three_for_pre,
                Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
                || t0,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |gen, ticket| {
                    assert_eq!((gen, ticket), (2, 4));
                    set_tracker_window_flags(h, true, true);
                    Ok(())
                },
            )
            .unwrap();
            assert!(inner_refresh.is_admitted_sync());
        }));
        let outer_pre_res = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_one,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| panic!("superseded pre-teardown attempt must not run configure_and_present_fn"),
        )
        .unwrap();
        assert_eq!(
            outer_pre_res,
            PopupAttemptExecutionOutcome::SupersededSameGeneration
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(2));
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(4));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 3);
        assert!(tracker_is_mapped(&harness));

        // Stage C: Same-generation nested `Refresh` (`ticket = 6`, reporting capability notice)
        // inside outer `Refresh` (`ticket = 5`) remap/configure returning a stale `Err(...)` —
        // outer unwinds as `Ok(SupersededSameGeneration)` without fencing `gen = 2`, plus newer-gen
        // `Open` (`gen = 4, ticket = 8`) inside outer `Open` (`gen = 3, ticket = 7`) returning
        // `Ok(SupersededOrCancelled)`.
        let outer_remap_err = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (2, 5));
                harness.shared.borrow_mut().rgba_available = false;
                let inner_fail = drive_execute_popup_map_attempt(
                    &harness.state_cell,
                    &harness,
                    PopupAttemptKind::Refresh,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_two,
                    None,
                    || t0,
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |inner_gen, inner_ticket| {
                        assert_eq!((inner_gen, inner_ticket), (2, 6));
                        set_tracker_window_flags(&harness, true, true);
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(
                    inner_fail,
                    PopupAttemptExecutionOutcome::FinalizedSynchronously {
                        admitted: false,
                        capability_notice: Some(WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE),
                        effective_notice: Some(
                            WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE.to_string()
                        ),
                        should_publish_notice: true,
                    }
                );
                Err("stale outer present error after newer refresh".to_string())
            },
        )
        .unwrap();
        assert_eq!(
            outer_remap_err,
            PopupAttemptExecutionOutcome::SupersededSameGeneration
        );
        assert!(!tracker_is_fenced(&harness, 2));
        harness.shared.borrow_mut().rgba_available = true;

        let outer_new_gen = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_one,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (3, 7));
                let inner_open = drive_execute_popup_map_attempt(
                    &harness.state_cell,
                    &harness,
                    PopupAttemptKind::Open,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_two,
                    None,
                    || t0,
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |inner_gen, inner_ticket| {
                        assert_eq!((inner_gen, inner_ticket), (4, 8));
                        set_tracker_window_flags(&harness, true, true);
                        Ok(())
                    },
                )
                .unwrap();
                assert!(inner_open.is_admitted_sync());
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            outer_new_gen,
            PopupAttemptExecutionOutcome::SupersededOrCancelled
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(4));
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(8));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // Stage D: Same-generation nested `Refresh` (`gen = 5, ticket = 10`) triggered inside
        // backdrop `map_empty_surface` while outer `Open` (`gen = 5, ticket = 9`) maps monitor 0.
        harness.shared.borrow_mut().reentrant_hook = ReentrantMapHook::SameGenRefreshOnMonitor {
            trigger_monitor: 0,
            specs: specs_three,
            base_notice: Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            now: t0,
        };
        let outer_map_reentry = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (5, 9));
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            outer_map_reentry,
            PopupAttemptExecutionOutcome::SupersededSameGeneration
        );
        assert_eq!(harness.state_cell.borrow().active_generation(), Some(5));
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(10));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 3);
        assert!(tracker_is_mapped(&harness));

        drive_release_all(&harness.state_cell, &harness);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_current_and_async_configure_failures_release_own_and_fence_post_teardown() {
        let specs_two = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // 1. Current unsuperseded second-monitor failure releases monitor 0's partial surface
        //    fail-open, retains `latest_attempt_ticket == 1`, keeps popup mapped, and reports notice.
        harness.shared.borrow_mut().fail_monitor_and_stage =
            Some((1, InjectedFailureStage::MapSurface));
        let sync_fail = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            sync_fail,
            PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: Some(
                    WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
                should_publish_notice: true,
            }
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(harness.state_cell.borrow().latest_attempt_ticket(), 1);
        assert!(tracker_is_mapped(&harness));

        // 2. Async `size-allocate` invalid geometry on `(gen = 2, ticket = 2)` releases own
        //    surfaces and reports notice when current, but returns `None` when teardown reenters a
        //    newer attempt `(gen = 3, ticket = 4)`.
        {
            let mut shared = harness.shared.borrow_mut();
            shared.fail_monitor_and_stage = None;
            shared.map_behavior = MapConfigureBehavior::AsyncPendingPlaceholder;
        }
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 2);
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 0);
        assert_eq!(
            harness.shared.borrow().scheduled_timeouts.last().copied(),
            Some((2, 2, WAYLAND_BACKDROP_CONFIGURE_TIMEOUT))
        );
        let s2_id = harness
            .shared
            .borrow()
            .entries
            .iter()
            .find(|e| e.generation == 2 && !e.released)
            .unwrap()
            .surface_id;
        assert_eq!(
            drive_async_surface_configured(&harness.state_cell, &harness, s2_id, 2, 0, 0),
            Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(harness.state_cell.borrow().latest_attempt_ticket(), 2);

        // Reentrant newer same-generation refresh during `drive_async_surface_configured` teardown:
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        let s3_id = harness
            .shared
            .borrow()
            .entries
            .iter()
            .find(|e| e.generation == 3 && !e.released)
            .unwrap()
            .surface_id;
        let specs_two_reenter = specs_two.clone();
        harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
            h.shared.borrow_mut().map_behavior = MapConfigureBehavior::SyncValidWorkarea;
            let _ = drive_execute_popup_map_attempt(
                &h.state_cell,
                h,
                PopupAttemptKind::Refresh,
                Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                true,
                &specs_two_reenter,
                None,
                || t0,
                WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                |gen, ticket| {
                    assert_eq!((gen, ticket), (3, 4));
                    set_tracker_window_flags(h, true, true);
                    Ok(())
                },
            )
            .unwrap();
        }));
        assert_eq!(
            drive_async_surface_configured(&harness.state_cell, &harness, s3_id, 3, 0, 0),
            None,
            "post-teardown re-sampling must suppress notice when teardown reentered ticket 4"
        );
        assert_eq!(harness.state_cell.borrow().active_admission_seq(), Some(4));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        // 3. `500ms` `drive_async_configure_timeout`: reports notice when current & visible, and
        //    returns `None` if the popup hides reentrantly during teardown.
        harness.shared.borrow_mut().map_behavior = MapConfigureBehavior::AsyncPendingPlaceholder;
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (3, 5));
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            drive_async_configure_timeout(&harness.state_cell, &harness, 3, 5),
            Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE)
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);

        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (4, 6));
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        harness.shared.borrow_mut().on_release_hook = Some(Box::new(|h| {
            let mut shared = h.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }));
        assert_eq!(
            drive_async_configure_timeout(&harness.state_cell, &harness, 4, 6),
            None,
            "post-teardown re-sampling must suppress notice when popup hid during teardown"
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_on_demand_transition_schedules_on_failure_and_late_map_and_fences_tickets() {
        let specs_two = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            false,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // 1. Mapped v4 popup with RGBA visual failure (`capability_notice == Some(...)`) MUST still
        //    schedule `Exclusive -> OnDemand` so keyboard mode never remains stuck in `Exclusive`.
        let rgba_fail = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rgba_fail,
            PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: false,
                capability_notice: Some(WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE),
                effective_notice: Some(WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE.to_string()),
                should_publish_notice: true,
            }
        );
        assert_eq!(
            harness.shared.borrow().scheduled_on_demand_transitions,
            vec![(1, 1, WaylandKeyboardPolicy::ExclusiveThenOnDemand)]
        );
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                1,
                1,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
        }

        // 2. Queued open that later maps natively with surface creation/map failure (`admitted ==
        //    false`, `capability_notice == Some(...)`) also schedules `OnDemand` on `(2, 2)`.
        {
            let mut shared = harness.shared.borrow_mut();
            shared.rgba_available = true;
            shared.fail_monitor_and_stage = Some((1, InjectedFailureStage::MapSurface));
        }
        set_tracker_window_flags(&harness, false, false);
        let queued_open = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(queued_open.is_armed_pending());
        let entries_before_map2 = harness.shared.borrow().entries.len();
        set_tracker_window_flags(&harness, true, true);
        let async_surface_map_signal = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            2,
            2,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(20),
        );
        assert!(matches!(
            async_surface_map_signal,
            PendingMapResolution::StillPending { .. }
        ));
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_map2);
        let async_surface_fail = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            2,
            2,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(22),
        );
        assert_eq!(
            async_surface_fail,
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: Some(
                    WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
                should_publish_notice: true,
            }
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_on_demand_transitions
                .last()
                .copied(),
            Some((2, 2, WaylandKeyboardPolicy::ExclusiveThenOnDemand))
        );

        // 3. Same-generation newer `Refresh` `(2, 3)` and newer-generation `Open` `(3, 4)` fence
        //    older `OnDemand` callbacks by BOTH `generation` and `attempt_ticket`.
        harness.shared.borrow_mut().fail_monitor_and_stage = None;
        let refresh_ok = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(30),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(refresh_ok.is_admitted_sync());
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(!should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                2,
                2,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
            assert!(should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                2,
                3,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
        }

        let new_gen_ok = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(40),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert!(new_gen_ok.is_admitted_sync());
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(!should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                2,
                3,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
            assert!(should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                3,
                4,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
        }
        drive_release_all(&harness.state_cell, &harness);

        // 4. Legacy `< v4` (`ExclusiveLimitedExternalOutsideClick`) stays truthful and never
        //    schedules or applies an `OnDemand` transition.
        let count_before_legacy = harness
            .shared
            .borrow()
            .scheduled_on_demand_transitions
            .len();
        let legacy_open = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick),
            false,
            &[],
            Some(WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE.to_string()),
            || t0 + Duration::from_millis(50),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            legacy_open,
            PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: false,
                capability_notice: None,
                effective_notice: Some(WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE.to_string()),
                should_publish_notice: true,
            }
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_on_demand_transitions
                .len(),
            count_before_legacy
        );
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(!should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                4,
                5,
                WaylandKeyboardPolicy::ExclusiveLimitedExternalOutsideClick,
                sample,
            ));
        }

        // 5. Unmapped popup at the `1000ms` pending-map deadline followed by a late native
        //    `connect_map` at `1050ms`: admits ZERO backdrop surfaces and keeps `tracker.mapped ==
        //    false`, while scheduling `OnDemand` and publishing the failure notice once visible.
        set_tracker_window_flags(&harness, false, false);
        let late_open = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
            || t0 + Duration::from_millis(100),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| Ok(()),
        )
        .unwrap();
        assert!(late_open.is_armed_pending());
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                5,
                6,
                t0 + Duration::from_millis(1100),
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: None,
                effective_notice: Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE.to_string()),
                should_publish_notice: false,
            }
        );
        assert_eq!(
            harness
                .state_cell
                .borrow()
                .active_pending_or_expired_map_ticket(),
            Some((5, 6))
        );

        let entries_before_late_map = harness.shared.borrow().entries.len();
        let on_demand_before_late_map = harness
            .shared
            .borrow()
            .scheduled_on_demand_transitions
            .len();
        set_tracker_window_flags(&harness, true, true);
        let expected_late_notice = combine_popover_notices(
            Some(MISSING_TRAY_COORDINATES_FALLBACK_NOTICE),
            Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(1150),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_on_demand_transitions
                .len(),
            on_demand_before_late_map
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_idle_probes
                .last()
                .copied(),
            Some((5, 6))
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(1155),
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: expected_late_notice,
                should_publish_notice: true,
            }
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(
            harness.shared.borrow().entries.len(),
            entries_before_late_map
        );
        assert!(!tracker_is_mapped(&harness));
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_on_demand_transitions
                .last()
                .copied(),
            Some((5, 6, WaylandKeyboardPolicy::ExclusiveThenOnDemand))
        );
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(should_apply_wayland_on_demand_transition(
                &tracker,
                latest,
                5,
                6,
                WaylandKeyboardPolicy::ExclusiveThenOnDemand,
                sample,
            ));
        }
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(1160),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(1162),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RecordedKeyboardFocus {
        Unset,
        Exclusive,
        OnDemand,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum RecordedPresentationWrite {
        WebviewSize(i32, i32),
        GtkHints(i32, i32, bool),
        LayerProps(WaylandShellLayer, i32),
        Monitor(Option<usize>, Option<usize>),
        AnchorsAndMargins {
            anchors: (bool, bool, bool, bool),
            margins: (i32, i32, i32, i32),
        },
        InitialKeyboardMode(WaylandKeyboardPolicy),
        GdkRootPosition(i32, i32),
        ShowChildren,
        PublishPreShowNotice(Option<String>),
        PresentPopupWindow,
        FocusNonLayerPopup,
    }

    type SetMonitorReentrantHook = Box<dyn FnOnce(&mut RecordedPopupPresentation)>;
    type PresentWindowReentrantHook =
        Box<dyn FnOnce(&mut RecordedPopupPresentation) -> Result<(), String>>;

    struct RecordedPopupPresentation {
        webview_size: (i32, i32),
        gtk_hints: (i32, i32, bool),
        monitor: (Option<usize>, Option<usize>),
        anchors: (bool, bool, bool, bool),
        margins: (i32, i32, i32, i32),
        keyboard_focus: RecordedKeyboardFocus,
        published_notice: Option<String>,
        writes: Vec<RecordedPresentationWrite>,
        on_set_monitor_hook: Option<SetMonitorReentrantHook>,
        on_present_hook: Option<PresentWindowReentrantHook>,
    }

    impl RecordedPopupPresentation {
        fn new() -> Self {
            Self {
                webview_size: (0, 0),
                gtk_hints: (0, 0, false),
                monitor: (None, None),
                anchors: (false, false, false, false),
                margins: (0, 0, 0, 0),
                keyboard_focus: RecordedKeyboardFocus::Unset,
                published_notice: None,
                writes: Vec::new(),
                on_set_monitor_hook: None,
                on_present_hook: None,
            }
        }
    }

    impl PopupPresentationTarget for RecordedPopupPresentation {
        fn apply_webview_logical_size(&mut self, logical_width: i32, logical_height: i32) {
            self.webview_size = (logical_width, logical_height);
            self.writes.push(RecordedPresentationWrite::WebviewSize(
                logical_width,
                logical_height,
            ));
        }

        fn apply_gtk_window_hints(
            &mut self,
            logical_width: i32,
            logical_height: i32,
            is_layer_surface: bool,
        ) {
            self.gtk_hints = (logical_width, logical_height, is_layer_surface);
            self.writes.push(RecordedPresentationWrite::GtkHints(
                logical_width,
                logical_height,
                is_layer_surface,
            ));
        }

        fn apply_layer_shell_surface_props(
            &mut self,
            layer: WaylandShellLayer,
            exclusive_zone: i32,
        ) {
            let write = RecordedPresentationWrite::LayerProps(layer, exclusive_zone);
            self.writes.push(write);
        }

        fn apply_layer_shell_monitor(
            &mut self,
            placement_monitor_index: Option<usize>,
            fallback_target_monitor_index: Option<usize>,
        ) {
            self.monitor = (placement_monitor_index, fallback_target_monitor_index);
            self.writes.push(RecordedPresentationWrite::Monitor(
                placement_monitor_index,
                fallback_target_monitor_index,
            ));
            if let Some(hook) = self.on_set_monitor_hook.take() {
                hook(self);
            }
        }

        fn apply_layer_shell_anchors_and_margins(
            &mut self,
            placement: &WaylandLayerShellPlacement,
        ) {
            self.anchors = (
                placement.anchor_top,
                placement.anchor_bottom,
                placement.anchor_left,
                placement.anchor_right,
            );
            self.margins = (
                placement.margin_top,
                placement.margin_bottom,
                placement.margin_left,
                placement.margin_right,
            );
            let write = RecordedPresentationWrite::AnchorsAndMargins {
                anchors: self.anchors,
                margins: self.margins,
            };
            self.writes.push(write);
        }

        fn apply_initial_layer_keyboard_mode(&mut self, keyboard_policy: WaylandKeyboardPolicy) {
            self.keyboard_focus = RecordedKeyboardFocus::Exclusive;
            let write = RecordedPresentationWrite::InitialKeyboardMode(keyboard_policy);
            self.writes.push(write);
        }

        fn apply_gdk_root_position(&mut self, gdk_x: i32, gdk_y: i32) {
            self.writes
                .push(RecordedPresentationWrite::GdkRootPosition(gdk_x, gdk_y));
        }

        fn show_popup_children(&mut self) {
            self.writes.push(RecordedPresentationWrite::ShowChildren);
        }

        fn publish_pre_show_open_state(&mut self, base_notice: Option<String>) {
            self.published_notice = base_notice.clone();
            let write = RecordedPresentationWrite::PublishPreShowNotice(base_notice);
            self.writes.push(write);
        }

        fn present_popup_window(&mut self) -> Result<(), String> {
            self.writes
                .push(RecordedPresentationWrite::PresentPopupWindow);
            if let Some(hook) = self.on_present_hook.take() {
                return hook(self);
            }
            Ok(())
        }

        fn focus_and_present_non_layer_popup(&mut self) {
            self.writes
                .push(RecordedPresentationWrite::FocusNonLayerPopup);
        }
    }

    #[test]
    fn wayland_presentation_checkpoints_halt_stale_gtk_writes_after_reentrant_set_monitor() {
        let specs_two = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        let outer_placement = WaylandLayerShellPlacement {
            monitor_index: Some(0),
            layer: WaylandShellLayer::Overlay,
            anchor_top: true,
            anchor_bottom: false,
            anchor_left: false,
            anchor_right: true,
            margin_top: 12,
            margin_bottom: 0,
            margin_left: 0,
            margin_right: 16,
            exclusive_zone: 0,
            keyboard_policy: WaylandKeyboardPolicy::ExclusiveThenOnDemand,
            estimated_gdk_bounds: (1544, 42, 360, 320),
        };
        let inner_placement = WaylandLayerShellPlacement {
            monitor_index: Some(1),
            layer: WaylandShellLayer::Overlay,
            anchor_top: false,
            anchor_bottom: true,
            anchor_left: true,
            anchor_right: false,
            margin_top: 0,
            margin_bottom: 24,
            margin_left: 28,
            margin_right: 0,
            exclusive_zone: 0,
            keyboard_policy: WaylandKeyboardPolicy::ExclusiveThenOnDemand,
            estimated_gdk_bounds: (1948, 760, 340, 296),
        };

        let target_cell = std::rc::Rc::new(RefCell::new(RecordedPopupPresentation::new()));

        // 1. Same-generation newer `Refresh` (`ticket = 2`) reentering inside `set_monitor`
        //    during outer `Open` (`gen = 1, ticket = 1`): outer halts immediately with ZERO
        //    post-nested anchor, margin, size, `Exclusive`, notice, or `show` writes.
        let snapshot_after_same_gen = std::rc::Rc::new(std::cell::Cell::new(0usize));
        {
            let h_state = harness.state_cell.clone();
            let h_shared = harness.shared.clone();
            let specs_inner = specs_two.clone();
            let snap_clone = snapshot_after_same_gen.clone();
            target_cell.borrow_mut().on_set_monitor_hook = Some(Box::new(move |target| {
                let inner_harness = CoordinatorTestHarness {
                    state_cell: h_state,
                    shared: h_shared,
                };
                let inner_res = drive_execute_popup_map_attempt(
                    &inner_harness.state_cell,
                    &inner_harness,
                    PopupAttemptKind::Refresh,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_inner,
                    None,
                    || t0,
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |inner_gen, inner_ticket| {
                        assert_eq!((inner_gen, inner_ticket), (1, 2));
                        drive_present_layer_popup_with_checkpoints(
                            &inner_harness.state_cell,
                            &inner_harness,
                            target,
                            PopupAttemptKind::Refresh,
                            inner_gen,
                            inner_ticket,
                            (340, 296),
                            Some(&inner_placement),
                            Some(1),
                            None,
                        )?;
                        target.keyboard_focus = RecordedKeyboardFocus::OnDemand;
                        set_tracker_window_flags(&inner_harness, true, true);
                        Ok(())
                    },
                )
                .unwrap();
                assert!(inner_res.is_admitted_sync());
                snap_clone.set(target.writes.len());
            }));
        }

        let outer_same_gen = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            Some("stale-open-notice".to_string()),
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (1, 1));
                let mut target_guard = target_cell.borrow_mut();
                let target: &mut RecordedPopupPresentation = &mut target_guard;
                drive_present_layer_popup_with_checkpoints(
                    &harness.state_cell,
                    &harness,
                    target,
                    PopupAttemptKind::Open,
                    gen,
                    ticket,
                    (360, 320),
                    Some(&outer_placement),
                    Some(0),
                    Some("stale-open-notice".to_string()),
                )
            },
        )
        .unwrap();
        assert_eq!(
            outer_same_gen,
            PopupAttemptExecutionOutcome::SupersededSameGeneration
        );
        {
            let target = target_cell.borrow();
            assert_eq!(
                target.writes.len(),
                snapshot_after_same_gen.get(),
                "outer Open must emit ZERO GTK writes after nested same-gen Refresh in set_monitor"
            );
            assert_eq!(target.webview_size, (340, 296));
            assert_eq!(target.gtk_hints, (340, 296, true));
            assert_eq!(target.monitor, (Some(1), Some(1)));
            assert_eq!(target.anchors, (false, true, true, false));
            assert_eq!(target.margins, (0, 24, 28, 0));
            assert_eq!(target.keyboard_focus, RecordedKeyboardFocus::OnDemand);
            assert_eq!(target.published_notice, None);
        }

        // 2. Newer-generation `Open` (`gen = 2, ticket = 4`) reentering inside `set_monitor`
        //    during outer `Refresh` (`gen = 1, ticket = 3`): outer halts immediately with ZERO
        //    subsequent anchor/margin writes.
        let snapshot_after_new_gen = std::rc::Rc::new(std::cell::Cell::new(0usize));
        {
            let h_state = harness.state_cell.clone();
            let h_shared = harness.shared.clone();
            let specs_inner = specs_two.clone();
            let snap_clone = snapshot_after_new_gen.clone();
            target_cell.borrow_mut().on_set_monitor_hook = Some(Box::new(move |target| {
                let inner_harness = CoordinatorTestHarness {
                    state_cell: h_state,
                    shared: h_shared,
                };
                let inner_open = drive_execute_popup_map_attempt(
                    &inner_harness.state_cell,
                    &inner_harness,
                    PopupAttemptKind::Open,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_inner,
                    Some("new-gen-notice".to_string()),
                    || t0,
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |inner_gen, inner_ticket| {
                        assert_eq!((inner_gen, inner_ticket), (2, 4));
                        drive_present_layer_popup_with_checkpoints(
                            &inner_harness.state_cell,
                            &inner_harness,
                            target,
                            PopupAttemptKind::Open,
                            inner_gen,
                            inner_ticket,
                            (360, 320),
                            Some(&inner_placement),
                            Some(1),
                            Some("new-gen-notice".to_string()),
                        )?;
                        set_tracker_window_flags(&inner_harness, true, true);
                        Ok(())
                    },
                )
                .unwrap();
                assert!(inner_open.is_admitted_sync());
                snap_clone.set(target.writes.len());
            }));
        }

        let outer_refresh_stale = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (1, 3));
                let mut target_guard = target_cell.borrow_mut();
                let target: &mut RecordedPopupPresentation = &mut target_guard;
                drive_present_layer_popup_with_checkpoints(
                    &harness.state_cell,
                    &harness,
                    target,
                    PopupAttemptKind::Refresh,
                    gen,
                    ticket,
                    (300, 240),
                    Some(&outer_placement),
                    Some(0),
                    None,
                )
            },
        )
        .unwrap();
        assert_eq!(
            outer_refresh_stale,
            PopupAttemptExecutionOutcome::SupersededOrCancelled
        );
        {
            let target = target_cell.borrow();
            assert_eq!(
                target.writes.len(),
                snapshot_after_new_gen.get(),
                "outer Refresh must emit ZERO GTK writes after nested Open in set_monitor"
            );
            assert_eq!(target.webview_size, (360, 320));
            assert_eq!(target.monitor, (Some(1), Some(1)));
            assert_eq!(target.anchors, (false, true, true, false));
            assert_eq!(target.margins, (0, 24, 28, 0));
            assert_eq!(target.published_notice.as_deref(), Some("new-gen-notice"));
        }

        // 3. Reentrant hide/cancel during layer `set_monitor` AND during non-layer fallback
        //    `present_popup_window` halts immediately without post-hide positioning or focus writes.
        let snapshot_after_layer_hide = std::rc::Rc::new(std::cell::Cell::new(0usize));
        {
            let h_state = harness.state_cell.clone();
            let h_shared = harness.shared.clone();
            let snap_clone = snapshot_after_layer_hide.clone();
            target_cell.borrow_mut().on_set_monitor_hook = Some(Box::new(move |target| {
                let inner_harness = CoordinatorTestHarness {
                    state_cell: h_state,
                    shared: h_shared,
                };
                {
                    let mut shared = inner_harness.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.tracker.on_unmapped();
                }
                drive_release_all(&inner_harness.state_cell, &inner_harness);
                snap_clone.set(target.writes.len());
            }));
        }
        let cancelled_layer = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (3, 5));
                let mut target_guard = target_cell.borrow_mut();
                let target: &mut RecordedPopupPresentation = &mut target_guard;
                drive_present_layer_popup_with_checkpoints(
                    &harness.state_cell,
                    &harness,
                    target,
                    PopupAttemptKind::Open,
                    gen,
                    ticket,
                    (360, 320),
                    Some(&outer_placement),
                    Some(0),
                    None,
                )
            },
        )
        .unwrap();
        assert_eq!(
            cancelled_layer,
            PopupAttemptExecutionOutcome::SupersededOrCancelled
        );
        assert_eq!(
            target_cell.borrow().writes.len(),
            snapshot_after_layer_hide.get()
        );

        let snapshot_after_non_layer_hide = std::rc::Rc::new(std::cell::Cell::new(0usize));
        {
            let h_state = harness.state_cell.clone();
            let h_shared = harness.shared.clone();
            let snap_clone = snapshot_after_non_layer_hide.clone();
            target_cell.borrow_mut().on_present_hook = Some(Box::new(move |target| {
                let inner_harness = CoordinatorTestHarness {
                    state_cell: h_state,
                    shared: h_shared,
                };
                {
                    let mut shared = inner_harness.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.tracker.on_unmapped();
                }
                drive_release_all(&inner_harness.state_cell, &inner_harness);
                snap_clone.set(target.writes.len());
                Ok(())
            }));
        }
        let (non_layer_gen, non_layer_ticket) = harness
            .begin_tracker_attempt(PopupAttemptKind::Open, false, || {
                reserve_attempt_ticket(&harness.state_cell)
            })
            .unwrap();
        assert_eq!((non_layer_gen, non_layer_ticket), (4, 6));
        {
            let mut target_guard = target_cell.borrow_mut();
            let target: &mut RecordedPopupPresentation = &mut target_guard;
            drive_present_non_layer_popup_with_checkpoints(
                &harness.state_cell,
                &harness,
                target,
                PopupAttemptKind::Open,
                non_layer_gen,
                non_layer_ticket,
                (360, 320),
                Some((1548, 40)),
                None,
            )
            .unwrap();
        }
        assert_eq!(
            target_cell.borrow().writes.len(),
            snapshot_after_non_layer_hide.get(),
            "non-layer path must emit ZERO post-show move/focus writes after hide"
        );
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_monotonic_post_present_clock_enforces_true_absolute_pending_map_deadline() {
        let specs_two = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();
        let clock = std::cell::Cell::new(t0);

        // 1. Unmapped `Open` whose presentation takes `350ms`: remaining pending-map timeout is
        //    `650ms` (`1000ms - 350ms`), never extended back to a fresh `1000ms` window.
        let unmapped_open = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || clock.get(),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                clock.set(t0 + Duration::from_millis(350));
                Ok(())
            },
        )
        .unwrap();
        assert!(unmapped_open.is_armed_pending());
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_pending_map_timeouts
                .last()
                .copied(),
            Some((1, 1, Duration::from_millis(650)))
        );
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + Duration::from_millis(999),
            ),
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(1),
            }
        );
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                1,
                1,
                t0 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: None,
                effective_notice: None,
                should_publish_notice: false,
            }
        );

        // 2. Already-mapped `Refresh` whose presentation takes `1025ms` (`> 1000ms` absolute
        //    deadline): post-presentation time sampling expires immediately with ZERO admitted
        //    surfaces while still scheduling `OnDemand` for the mapped failure popup.
        let t1 = t0 + Duration::from_secs(2);
        clock.set(t1);
        let live_open = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || clock.get(),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                set_tracker_window_flags(&harness, true, true);
                Ok(())
            },
        )
        .unwrap();
        assert!(live_open.is_admitted_sync());
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        let t2 = t1 + Duration::from_millis(100);
        clock.set(t2);
        let expired_refresh = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || clock.get(),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |_, _| {
                clock.set(t2 + WAYLAND_POPUP_PENDING_MAP_TIMEOUT + Duration::from_millis(25));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            expired_refresh,
            PopupAttemptExecutionOutcome::FinalizedSynchronously {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: Some(
                    WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
                should_publish_notice: true,
            }
        );
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 0);
        assert!(!tracker_is_mapped(&harness));
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_on_demand_transitions
                .last()
                .copied(),
            Some((2, 3, WaylandKeyboardPolicy::ExclusiveThenOnDemand))
        );
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_presentation_err_distinguishes_expected_hide_cancel_from_current_failure() {
        let specs_two = build_pending_map_test_specs(2);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // 1. Expected `hide` (`on_unmapped`) at the SAME `(generation = 1, ticket = 1)` before
        //    presentation returns `Err(...)` MUST classify as `Ok(SupersededOrCancelled)` rather
        //    than propagating `Err` (`RecoverMainOnCurrentFailure`).
        let hide_then_err = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (1, 1));
                {
                    let mut shared = harness.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.tracker.on_unmapped();
                }
                drive_release_all(&harness.state_cell, &harness);
                Err("window hidden before show completed".to_string())
            },
        );
        assert_eq!(
            hide_then_err,
            Ok(PopupAttemptExecutionOutcome::SupersededOrCancelled)
        );
        assert_eq!(harness.state_cell.borrow().pending_map_ticket(), None);

        // Non-layer fallback open (`cleanup_and_classify_presentation_err` directly) also classifies
        // same-ticket pre-abort hide + `Err` as `Ok(SupersededOrCancelled)`.
        let (nl_gen, nl_ticket) = harness
            .begin_tracker_attempt(PopupAttemptKind::Open, false, || {
                reserve_attempt_ticket(&harness.state_cell)
            })
            .unwrap();
        assert_eq!((nl_gen, nl_ticket), (2, 2));
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        assert_eq!(
            cleanup_and_classify_presentation_err(
                &harness.state_cell,
                &harness,
                nl_gen,
                nl_ticket,
                "non-layer hide during show".to_string(),
            ),
            Ok(PopupAttemptExecutionOutcome::SupersededOrCancelled)
        );

        // 2. Genuine current presentation `Err(...)` on an unsuperseded, non-cancelled attempt
        //    cleans up own state, fences the attempt, and propagates `Err(...)` so `main` recovers.
        let genuine_err = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (3, 3));
                Err("gtk show failed on active attempt".to_string())
            },
        );
        assert_eq!(
            genuine_err,
            Err("gtk show failed on active attempt".to_string())
        );
        assert_eq!(harness.state_cell.borrow().pending_map_ticket(), None);
        assert_eq!(harness.state_cell.borrow().active_surface_count(), 0);
        assert!(tracker_is_fenced(&harness, 3));
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    #[test]
    fn wayland_mapped_true_map_signal_defers_to_single_idle_and_fences_intervening_lifecycle() {
        let specs_two = build_pending_map_test_specs(2);
        let specs_three = build_pending_map_test_specs(3);
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();

        // 1. Even when native `visible + mapped + layer` flags are already `true` inside `::map`,
        //    `PendingMapTrigger::MapSignal` MUST NOT create/map backdrops or request redraws
        //    synchronously, MUST schedule at most ONE idle step across repeated `MapSignal` calls,
        //    and MUST finalize on the queued `IdleProbe` after `::map` unwinds.
        let open1 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (1, 1));
                Ok(())
            },
        )
        .unwrap();
        assert!(open1.is_armed_pending());

        set_tracker_window_flags(&harness, true, true);
        let map_sig1 = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(10),
        );
        assert_eq!(
            map_sig1,
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(990),
            }
        );
        let map_sig2 = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(12),
        );
        assert_eq!(
            map_sig2,
            PendingMapResolution::StillPending {
                remaining_timeout: Duration::from_millis(988),
            }
        );
        {
            let state = harness.state_cell.borrow();
            assert_eq!(state.pending_map_ticket(), Some((1, 1)));
            assert_eq!(state.active_surface_count(), 0);
            assert_eq!(state.enabled_surface_count(), 0);
        }
        {
            let shared = harness.shared.borrow();
            assert!(shared.entries.is_empty());
            assert_eq!(shared.scheduled_idle_probes, vec![(1, 1)]);
            assert!(shared.scheduled_on_demand_transitions.is_empty());
            assert!(shared.settled_popup_redraw_requests.is_empty());
        }

        let idle1 = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            1,
            1,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(15),
        );
        assert!(idle1.is_admitted_finalized());
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);
        assert_eq!(
            harness.shared.borrow().scheduled_on_demand_transitions,
            vec![(1, 1, WaylandKeyboardPolicy::ExclusiveThenOnDemand)]
        );
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests,
            vec![(1, 1)]
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                1,
                1,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(16),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        drive_release_all(&harness.state_cell, &harness);

        // 2a. Newer same-generation ticket `(2, 3)` arriving between mapped-true `MapSignal` on
        //     `(2, 2)` and `(2, 2)`'s queued `IdleProbe` blocks stale admission and stale redraw.
        set_tracker_window_flags(&harness, false, false);
        let open2 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(20),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (2, 2));
                Ok(())
            },
        )
        .unwrap();
        assert!(open2.is_armed_pending());
        let entries_before_2 = harness.shared.borrow().entries.len();
        set_tracker_window_flags(&harness, true, true);
        assert!(matches!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                2,
                2,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(25),
            ),
            PendingMapResolution::StillPending { .. }
        ));
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);

        let refresh2 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_three,
            None,
            || t0 + Duration::from_millis(28),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (2, 3));
                set_tracker_window_flags(&harness, false, false);
                Ok(())
            },
        )
        .unwrap();
        assert!(refresh2.is_armed_pending());
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                2,
                2,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(30),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);
        {
            let (tracker, sample) = harness.sample_popup_state();
            let latest = harness.state_cell.borrow().latest_attempt_ticket();
            assert!(!should_request_settled_popup_redraw(
                &tracker, latest, 2, 2, sample
            ));
        }

        // 2b. `hide` between mapped-true `MapSignal` and `IdleProbe` blocks backdrop creation.
        set_tracker_window_flags(&harness, true, true);
        assert!(matches!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                2,
                3,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(32),
            ),
            PendingMapResolution::StillPending { .. }
        ));
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        drive_release_all(&harness.state_cell, &harness);
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                2,
                3,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(35),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);

        // 2c. `explicit_quit` between mapped-true `MapSignal` and `IdleProbe` blocks admission.
        let open3 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(40),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (3, 4));
                Ok(())
            },
        )
        .unwrap();
        assert!(open3.is_armed_pending());
        set_tracker_window_flags(&harness, true, true);
        assert!(matches!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                3,
                4,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(42),
            ),
            PendingMapResolution::StillPending { .. }
        ));
        harness
            .shared
            .borrow_mut()
            .tracker_popup
            .as_mut()
            .unwrap()
            .explicit_quit = true;
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                3,
                4,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(45),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.explicit_quit = false;
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        drive_release_all(&harness.state_cell, &harness);

        // 2d. `1000ms` pending-map deadline firing between mapped-true `MapSignal` and `IdleProbe`
        //     blocks backdrop creation while keeping active-visible failure notice/focus truthful.
        let open4 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(100),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (4, 5));
                Ok(())
            },
        )
        .unwrap();
        assert!(open4.is_armed_pending());
        set_tracker_window_flags(&harness, true, true);
        assert!(matches!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                4,
                5,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(1090),
            ),
            PendingMapResolution::StillPending { .. }
        ));
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);
        assert_eq!(
            drive_pending_map_deadline(
                &harness.state_cell,
                &harness,
                4,
                5,
                t0 + Duration::from_millis(1100),
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: Some(
                    WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
                should_publish_notice: true,
            }
        );
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                4,
                5,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(1105),
            ),
            PendingMapResolution::NoOpOrSuperseded
        );
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);

        // Also verify `IdleProbe` arriving directly at `now >= deadline` before the timer callback.
        set_tracker_window_flags(&harness, false, false);
        let open5 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(1200),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (5, 6));
                Ok(())
            },
        )
        .unwrap();
        assert!(open5.is_armed_pending());
        set_tracker_window_flags(&harness, true, true);
        assert!(matches!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::MapSignal,
                t0 + Duration::from_millis(2195),
            ),
            PendingMapResolution::StillPending { .. }
        ));
        assert_eq!(
            drive_pending_map_progress(
                &harness.state_cell,
                &harness,
                5,
                6,
                PendingMapTrigger::IdleProbe,
                t0 + Duration::from_millis(2205),
            ),
            PendingMapResolution::Finalized {
                admitted: false,
                capability_notice: Some(WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE),
                effective_notice: Some(
                    WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE.to_string()
                ),
                should_publish_notice: true,
            }
        );
        assert_eq!(harness.shared.borrow().entries.len(), entries_before_2);

        // 3. Shared production same-epoch settled-redraw completion, transient unsettle retention,
        //    stale newer/hide/quit callback fencing, and mapped `ImmediatePostConfigure` `Refresh`.
        assert!(is_webkit_webview_type_name("WebKitWebView"));
        assert!(is_webkit_webview_type_name("WebKitWebViewBase"));
        assert!(is_webkit_webview_type_name("WryWebView"));
        assert!(!is_webkit_webview_type_name("GtkEventBox"));
        assert!(!is_webkit_webview_type_name("GtkBox"));
        assert!(!is_webkit_webview_type_name("GtkApplicationWindow"));

        harness.shared.borrow_mut().popup_allocation_settled = false;
        set_tracker_window_flags(&harness, false, false);
        let open6 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(2300),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (6, 7));
                Ok(())
            },
        )
        .unwrap();
        assert!(open6.is_armed_pending());
        let redraws_before_6 = harness.shared.borrow().settled_popup_redraw_requests.len();
        let redraw_idles_before_6 = harness.shared.borrow().scheduled_settled_redraw_idles.len();
        set_tracker_window_flags(&harness, true, true);
        let _ = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            6,
            7,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(2310),
        );
        let idle6 = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            6,
            7,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(2315),
        );
        assert!(idle6.is_admitted_finalized());
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            Some((6, 7))
        );
        assert!(!harness
            .state_cell
            .borrow()
            .is_pending_settled_redraw_idle_scheduled());
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6
        );

        // Unsettled `size-allocate` signal keeps pending wait without queuing an idle.
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        assert_eq!(
            harness.shared.borrow().scheduled_settled_redraw_idles.len(),
            redraw_idles_before_6
        );

        // Same-epoch allocation settles -> multiple signals queue at most ONE idle.
        harness.shared.borrow_mut().popup_allocation_settled = true;
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        assert_eq!(
            harness.shared.borrow().scheduled_settled_redraw_idles.len(),
            redraw_idles_before_6 + 1
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .scheduled_settled_redraw_idles
                .last()
                .copied(),
            Some((6, 7))
        );
        assert!(harness
            .state_cell
            .borrow()
            .is_pending_settled_redraw_idle_scheduled());
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6
        );

        // Allocation transiently unsettles before idle runs -> retains wait with zero spin idles.
        harness.shared.borrow_mut().popup_allocation_settled = false;
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 6, 7);
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6
        );
        assert_eq!(
            harness.shared.borrow().scheduled_settled_redraw_idles.len(),
            redraw_idles_before_6 + 1
        );
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            Some((6, 7))
        );
        assert!(!harness
            .state_cell
            .borrow()
            .is_pending_settled_redraw_idle_scheduled());

        // Next actual signal once settled again queues one fresh idle and redraws exactly once.
        harness.shared.borrow_mut().popup_allocation_settled = true;
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        assert_eq!(
            harness.shared.borrow().scheduled_settled_redraw_idles.len(),
            redraw_idles_before_6 + 2
        );
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 6, 7);
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6 + 1
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .settled_popup_redraw_requests
                .last()
                .copied(),
            Some((6, 7))
        );
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            None
        );
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 6, 7);
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6 + 1
        );

        // Already-mapped `Refresh` (`ImmediatePostConfigure`) redraws synchronously when settled.
        let refresh6 = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_three,
            None,
            || t0 + Duration::from_millis(2325),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (6, 8));
                Ok(())
            },
        )
        .unwrap();
        assert!(refresh6.is_admitted_sync());
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 3);
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            None
        );
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_before_6 + 2
        );
        assert_eq!(
            harness
                .shared
                .borrow()
                .settled_popup_redraw_requests
                .last()
                .copied(),
            Some((6, 8))
        );

        // Stale newer-epoch, `hide`, and `explicit_quit` callbacks are strict no-ops.
        harness.shared.borrow_mut().popup_allocation_settled = false;
        let refresh6_unsettled = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(2340),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (6, 9));
                Ok(())
            },
        )
        .unwrap();
        assert!(refresh6_unsettled.is_admitted_sync());
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            Some((6, 9))
        );
        harness.shared.borrow_mut().popup_allocation_settled = true;
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);

        // Newer `Open` `(7, 10)` supersedes queued `(6, 9)` idle without losing `(7, 10)` wait.
        harness.shared.borrow_mut().popup_allocation_settled = false;
        set_tracker_window_flags(&harness, false, false);
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(2350),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (7, 10));
                Ok(())
            },
        )
        .unwrap();
        set_tracker_window_flags(&harness, true, true);
        let _ = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            7,
            10,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(2355),
        );
        let _ = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            7,
            10,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(2360),
        );
        let redraws_after_newer = harness.shared.borrow().settled_popup_redraw_requests.len();
        assert_eq!(redraws_after_newer, redraws_before_6 + 2);
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 6, 9);
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_after_newer
        );
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            Some((7, 10))
        );

        // `hide` before queued `(7, 10)` idle cancels wait and blocks redraw.
        harness.shared.borrow_mut().popup_allocation_settled = true;
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        {
            let mut shared = harness.shared.borrow_mut();
            let tp = shared.tracker_popup.as_mut().unwrap();
            tp.window_visible = false;
            tp.window_mapped = false;
            tp.tracker.on_unmapped();
        }
        drive_release_all(&harness.state_cell, &harness);
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            None
        );
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 7, 10);
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_after_newer
        );

        // `explicit_quit` before queued `(8, 11)` idle cancels wait and blocks redraw.
        harness.shared.borrow_mut().popup_allocation_settled = false;
        let _ = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs_two,
            None,
            || t0 + Duration::from_millis(2370),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (8, 11));
                Ok(())
            },
        )
        .unwrap();
        set_tracker_window_flags(&harness, true, true);
        let _ = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            8,
            11,
            PendingMapTrigger::MapSignal,
            t0 + Duration::from_millis(2375),
        );
        let _ = drive_pending_map_progress(
            &harness.state_cell,
            &harness,
            8,
            11,
            PendingMapTrigger::IdleProbe,
            t0 + Duration::from_millis(2380),
        );
        harness.shared.borrow_mut().popup_allocation_settled = true;
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        harness
            .shared
            .borrow_mut()
            .tracker_popup
            .as_mut()
            .unwrap()
            .explicit_quit = true;
        drive_settled_popup_redraw_idle(&harness.state_cell, &harness, 8, 11);
        drive_settled_popup_redraw_signal(&harness.state_cell, &harness);
        assert_eq!(
            harness.state_cell.borrow().pending_settled_redraw_epoch(),
            None
        );
        assert_eq!(
            harness.shared.borrow().settled_popup_redraw_requests.len(),
            redraws_after_newer
        );

        drive_release_all(&harness.state_cell, &harness);
        assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
    }

    const TEST_DISPLAY_PRIMARY: usize = 1;
    const TEST_DISPLAY_SECONDARY: usize = 2;
    const TEST_MASTER_POINTER_ID: usize = 500;
    const TEST_SLAVE_POINTER_ID: usize = 501;
    const TEST_MASTER_KEYBOARD_ID: usize = 502;
    const TEST_OTHER_SEAT_POINTER_ID: usize = 503;

    fn two_monitor_dismiss_test_specs() -> Vec<WaylandOutsideClickBackdropSpec> {
        build_pending_map_test_specs(2)
    }

    fn admit_open_popup_for_dismiss_test(
        harness: &CoordinatorTestHarness,
        specs: &[WaylandOutsideClickBackdropSpec],
        now: Instant,
    ) -> (u64, u64) {
        let mut captured_epoch = None;
        let out = drive_execute_popup_map_attempt(
            &harness.state_cell,
            harness,
            PopupAttemptKind::Open,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            specs,
            None,
            || now,
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |generation, ticket| {
                captured_epoch = Some((generation, ticket));
                Ok(())
            },
        )
        .expect("open attempt must be admitted or await native mapping");
        let (generation, ticket) = captured_epoch.expect("reserved epoch");
        if out.is_armed_pending() {
            set_tracker_window_flags(harness, true, true);
            let map_step = drive_pending_map_progress(
                &harness.state_cell,
                harness,
                generation,
                ticket,
                PendingMapTrigger::MapSignal,
                now + Duration::from_millis(5),
            );
            assert!(matches!(
                map_step,
                PendingMapResolution::StillPending { .. }
            ));
            let idle_step = drive_pending_map_progress(
                &harness.state_cell,
                harness,
                generation,
                ticket,
                PendingMapTrigger::IdleProbe,
                now + Duration::from_millis(10),
            );
            assert!(idle_step.is_admitted_finalized());
        } else {
            // A newer Open can already have a visible, mapped native window during reentry.
            assert!(out.is_admitted_sync());
        }
        let state = harness.state_cell.borrow();
        assert_eq!(state.active_generation(), Some(generation));
        assert_eq!(state.active_admission_seq(), Some(ticket));
        (generation, ticket)
    }

    fn valid_backdrop_press_context(
        handle_id: usize,
        button: u32,
        event_time: u32,
    ) -> BackdropNativePressContext<usize, usize, usize, usize> {
        let native_win = 10_000 + handle_id;
        BackdropNativePressContext {
            is_gtk_main_thread: true,
            pointer_event: BackdropPointerEvent::ButtonPress { button },
            event_time,
            backend_display: TEST_DISPLAY_PRIMARY,
            callback_toplevel: Some(handle_id),
            event_window: Some((native_win, TEST_DISPLAY_PRIMARY, true)),
            callback_window: Some((native_win, TEST_DISPLAY_PRIMARY, true)),
            device: Some(BackdropCandidateDevice {
                handle: TEST_MASTER_POINTER_ID,
                display: TEST_DISPLAY_PRIMARY,
                role: BackdropDeviceRole::MasterPointer,
                seat_master_pointer: Some(TEST_MASTER_POINTER_ID),
            }),
        }
    }

    #[test]
    fn outside_click_press_ungrabs_exact_master_pointer_before_teardown_and_at_most_once() {
        let specs = two_monitor_dismiss_test_specs();
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();
        let (generation, ticket) = admit_open_popup_for_dismiss_test(&harness, &specs, t0);
        assert_eq!((generation, ticket), (1, 1));
        assert_eq!(harness.state_cell.borrow().enabled_surface_count(), 2);

        let (mon1_surface_id, mon1_handle_id, mon0_surface_id, mon0_handle_id) = {
            let shared = harness.shared.borrow();
            assert_eq!(shared.entries.len(), 2);
            (
                shared.entries[1].surface_id,
                shared.entries[1].handle_id,
                shared.entries[0].surface_id,
                shared.entries[0].handle_id,
            )
        };

        let outcome = drive_outside_click_backdrop_press(
            &harness.state_cell,
            &harness,
            mon1_surface_id,
            generation,
            valid_backdrop_press_context(mon1_handle_id, 1, 42_719),
        );
        assert_eq!(outcome, BackdropDismissOutcome::DismissedPopover);

        {
            let coord = harness.state_cell.borrow();
            assert_eq!(coord.active_generation(), None);
            assert_eq!(coord.active_admission_seq(), None);
            assert_eq!(coord.active_surface_count(), 0);
            assert_eq!(coord.enabled_surface_count(), 0);

            let shared = harness.shared.borrow();
            // Exactly one master-pointer ungrab with exact event_time, executed while both
            // active surfaces still held bounded input regions and strictly before fail-open
            // surface teardown and popup hide.
            assert_eq!(shared.ungrab_calls, vec![(TEST_MASTER_POINTER_ID, 42_719)]);
            assert_eq!(shared.ungrab_bounded_surface_counts, vec![2]);
            assert_eq!(shared.hidden_popup_epochs, vec![(1, 1)]);
            assert_eq!(
                shared.event_log,
                vec![
                    "master_pointer_ungrabbed",
                    "surface_released_fail_open",
                    "surface_released_fail_open",
                    "popup_hidden_for_dismissed_epoch",
                ]
            );
            let tp = shared.tracker_popup.as_ref().unwrap();
            assert!(!tp.window_visible && !tp.window_mapped);
            assert!(!tp.tracker.is_mapped());
            assert!(tp.tracker.is_fenced_or_superseded(1));
            assert_all_surfaces_empty_and_closed(&shared);
        }

        // Duplicate / trailing presses on either retired surface from the dismissed open must
        // NEVER ungrab a second time or hide again ("at most once").
        let duplicate_same_surface = drive_outside_click_backdrop_press(
            &harness.state_cell,
            &harness,
            mon1_surface_id,
            generation,
            valid_backdrop_press_context(mon1_handle_id, 1, 42_720),
        );
        assert_eq!(
            duplicate_same_surface,
            BackdropDismissOutcome::RejectedStaleOrInactive
        );
        let duplicate_sibling_surface = drive_outside_click_backdrop_press(
            &harness.state_cell,
            &harness,
            mon0_surface_id,
            generation,
            valid_backdrop_press_context(mon0_handle_id, 1, 42_721),
        );
        assert_eq!(
            duplicate_sibling_surface,
            BackdropDismissOutcome::RejectedStaleOrInactive
        );
        {
            let shared = harness.shared.borrow();
            assert_eq!(shared.ungrab_calls, vec![(TEST_MASTER_POINTER_ID, 42_719)]);
            assert_eq!(shared.hidden_popup_epochs, vec![(1, 1)]);
        }
    }

    #[test]
    fn outside_click_press_rejects_invalid_targets_devices_displays_and_non_press_events() {
        let specs = two_monitor_dismiss_test_specs();
        let harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::SyncValidWorkarea,
        );
        let t0 = Instant::now();
        let (gen1, ticket1) = admit_open_popup_for_dismiss_test(&harness, &specs, t0);
        assert_eq!((gen1, ticket1), (1, 1));
        let (gen1_surface0, gen1_handle0) = {
            let shared = harness.shared.borrow();
            (shared.entries[0].surface_id, shared.entries[0].handle_id)
        };

        // Advance via a same-generation Refresh (`gen = 1, ticket = 2`) so `gen1_surface0`
        // (`ticket = 1`) becomes a superseded same-generation surface ID/token!
        let refresh_out = drive_execute_popup_map_attempt(
            &harness.state_cell,
            &harness,
            PopupAttemptKind::Refresh,
            Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
            true,
            &specs,
            None,
            || t0 + Duration::from_millis(50),
            WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
            |gen, ticket| {
                assert_eq!((gen, ticket), (1, 2));
                Ok(())
            },
        )
        .unwrap();
        assert!(refresh_out.is_admitted_sync());
        let (refresh_surface0, refresh_handle0) = {
            let shared = harness.shared.borrow();
            let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
            assert_eq!(active.len(), 2);
            (active[0].surface_id, active[0].handle_id)
        };

        // Superseded same-generation surface ID (`gen1_surface0`) must be rejected even though
        // `generation == 1` still matches!
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                gen1_surface0,
                1,
                valid_backdrop_press_context(gen1_handle0, 1, 1_000),
            ),
            BackdropDismissOutcome::RejectedStaleOrInactive
        );

        // Open `generation = 2` (`ticket = 3`) so `generation = 1` (`refresh_surface0`) is now a
        // stale generation while `(gen2, ticket3)` is live.
        let (gen2, ticket3) =
            admit_open_popup_for_dismiss_test(&harness, &specs, t0 + Duration::from_millis(100));
        assert_eq!((gen2, ticket3), (2, 3));
        let (live_surface0, live_handle0, live_handle1) = {
            let shared = harness.shared.borrow();
            let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
            assert_eq!(active.len(), 2);
            (
                active[0].surface_id,
                active[0].handle_id,
                active[1].handle_id,
            )
        };
        let live_native_win0 = 10_000 + live_handle0;
        let live_native_win1 = 10_000 + live_handle1;

        // 1. Stale generation rejected without ungrab or dismissing current popup.
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                refresh_surface0,
                1,
                valid_backdrop_press_context(refresh_handle0, 1, 1_001),
            ),
            BackdropDismissOutcome::RejectedStaleOrInactive
        );

        // 2. Off-GTK-main-thread rejected.
        let mut off_thread_ctx = valid_backdrop_press_context(live_handle0, 1, 1_002);
        off_thread_ctx.is_gtk_main_thread = false;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                off_thread_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        // 3. Pointer motion, button release / multi-click continuation, and button == 0 ignored.
        let mut motion_ctx = valid_backdrop_press_context(live_handle0, 1, 1_003);
        motion_ctx.pointer_event = BackdropPointerEvent::PointerMotion;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                motion_ctx,
            ),
            BackdropDismissOutcome::IgnoredNonPress
        );

        let mut release_ctx = valid_backdrop_press_context(live_handle0, 1, 1_004);
        release_ctx.pointer_event = BackdropPointerEvent::NonInitialButtonEvent;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                release_ctx,
            ),
            BackdropDismissOutcome::IgnoredNonPress
        );

        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                valid_backdrop_press_context(live_handle0, 0, 1_005),
            ),
            BackdropDismissOutcome::IgnoredNonPress
        );

        // 4. Device guards: missing device, slave/floating device (even with valid seat master
        //    pointer — NO slave/source_device promotion!), master keyboard, master pointer !=
        //    seat.pointer, and device on different display.
        let mut no_device_ctx = valid_backdrop_press_context(live_handle0, 1, 1_006);
        no_device_ctx.device = None;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                no_device_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut slave_device_ctx = valid_backdrop_press_context(live_handle0, 1, 1_007);
        slave_device_ctx.device = Some(BackdropCandidateDevice {
            handle: TEST_SLAVE_POINTER_ID,
            display: TEST_DISPLAY_PRIMARY,
            role: BackdropDeviceRole::SlaveOrFloating,
            seat_master_pointer: Some(TEST_MASTER_POINTER_ID),
        });
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                slave_device_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut keyboard_device_ctx = valid_backdrop_press_context(live_handle0, 1, 1_008);
        keyboard_device_ctx.device = Some(BackdropCandidateDevice {
            handle: TEST_MASTER_KEYBOARD_ID,
            display: TEST_DISPLAY_PRIMARY,
            role: BackdropDeviceRole::MasterKeyboard,
            seat_master_pointer: Some(TEST_MASTER_KEYBOARD_ID),
        });
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                keyboard_device_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut mismatched_seat_ptr_ctx = valid_backdrop_press_context(live_handle0, 1, 1_009);
        mismatched_seat_ptr_ctx.device = Some(BackdropCandidateDevice {
            handle: TEST_MASTER_POINTER_ID,
            display: TEST_DISPLAY_PRIMARY,
            role: BackdropDeviceRole::MasterPointer,
            seat_master_pointer: Some(TEST_OTHER_SEAT_POINTER_ID),
        });
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                mismatched_seat_ptr_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut wrong_dev_display_ctx = valid_backdrop_press_context(live_handle0, 1, 1_010);
        wrong_dev_display_ctx.device.as_mut().unwrap().display = TEST_DISPLAY_SECONDARY;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                wrong_dev_display_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        // 5. Window & display identity guards: wrong callback GtkWindow, wrong/destroyed/missing
        //    event GdkWindow, wrong/destroyed/missing callback GdkWindow, mismatched displays, and
        //    non-live/wrong-display tracked backdrop handle.
        let mut wrong_toplevel_ctx = valid_backdrop_press_context(live_handle0, 1, 1_011);
        wrong_toplevel_ctx.callback_toplevel = Some(live_handle1);
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                wrong_toplevel_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut wrong_event_win_ctx = valid_backdrop_press_context(live_handle0, 1, 1_012);
        wrong_event_win_ctx.event_window = Some((live_native_win1, TEST_DISPLAY_PRIMARY, true));
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                wrong_event_win_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut destroyed_event_win_ctx = valid_backdrop_press_context(live_handle0, 1, 1_013);
        destroyed_event_win_ctx.event_window =
            Some((live_native_win0, TEST_DISPLAY_PRIMARY, false));
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                destroyed_event_win_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut wrong_event_win_disp_ctx = valid_backdrop_press_context(live_handle0, 1, 1_014);
        wrong_event_win_disp_ctx.event_window =
            Some((live_native_win0, TEST_DISPLAY_SECONDARY, true));
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                wrong_event_win_disp_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        let mut wrong_cb_win_ctx = valid_backdrop_press_context(live_handle0, 1, 1_015);
        wrong_cb_win_ctx.callback_window = Some((live_native_win1, TEST_DISPLAY_PRIMARY, true));
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                wrong_cb_win_ctx,
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );

        harness.shared.borrow_mut().tracked_window_live_override = Some(false);
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                valid_backdrop_press_context(live_handle0, 1, 1_016),
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );
        harness.shared.borrow_mut().tracked_window_live_override = None;

        harness.shared.borrow_mut().tracked_window_display_override = Some(TEST_DISPLAY_SECONDARY);
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                valid_backdrop_press_context(live_handle0, 1, 1_017),
            ),
            BackdropDismissOutcome::RejectedInvalidNativeTarget
        );
        harness.shared.borrow_mut().tracked_window_display_override = None;

        // 6. Explicit quit guard rejects without ungrabbing or hiding.
        harness
            .shared
            .borrow_mut()
            .tracker_popup
            .as_mut()
            .unwrap()
            .explicit_quit = true;
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                valid_backdrop_press_context(live_handle0, 1, 1_018),
            ),
            BackdropDismissOutcome::RejectedStaleOrInactive
        );
        harness
            .shared
            .borrow_mut()
            .tracker_popup
            .as_mut()
            .unwrap()
            .explicit_quit = false;

        // Across ALL rejected cases above: zero ungrabs, zero popup hides, and both current
        // surfaces remain live and `ActiveBounded`!
        {
            let coord = harness.state_cell.borrow();
            assert_eq!(coord.active_generation(), Some(gen2));
            assert_eq!(coord.active_admission_seq(), Some(ticket3));
            assert_eq!(coord.enabled_surface_count(), 2);

            let shared = harness.shared.borrow();
            assert!(shared.ungrab_calls.is_empty());
            assert!(shared.hidden_popup_epochs.is_empty());
            let tp = shared.tracker_popup.as_ref().unwrap();
            assert!(tp.window_visible && tp.window_mapped && tp.tracker.is_mapped());
        }

        // 7. Unconfigured `MappedEmpty` surface on a separate async-placeholder harness also
        //    rejects without ungrabbing or dismissing.
        let unconfigured_harness = CoordinatorTestHarness::new_tracker_backed(
            true,
            MapConfigureBehavior::AsyncPendingPlaceholder,
        );
        let (ug_gen, ug_ticket) =
            admit_open_popup_for_dismiss_test(&unconfigured_harness, &specs, t0);
        assert_eq!((ug_gen, ug_ticket), (1, 1));
        assert_eq!(
            unconfigured_harness
                .state_cell
                .borrow()
                .enabled_surface_count(),
            0
        );
        let (ug_surface0, ug_handle0) = {
            let shared = unconfigured_harness.shared.borrow();
            (shared.entries[0].surface_id, shared.entries[0].handle_id)
        };
        assert_eq!(
            drive_outside_click_backdrop_press(
                &unconfigured_harness.state_cell,
                &unconfigured_harness,
                ug_surface0,
                ug_gen,
                valid_backdrop_press_context(ug_handle0, 1, 2_000),
            ),
            BackdropDismissOutcome::RejectedStaleOrInactive
        );
        {
            let ug_shared = unconfigured_harness.shared.borrow();
            assert!(ug_shared.ungrab_calls.is_empty());
            assert!(ug_shared.hidden_popup_epochs.is_empty());
        }
        drive_release_all(&unconfigured_harness.state_cell, &unconfigured_harness);

        // 8. Finally, a genuine valid button press (`button = 3`) on `live_surface0` still
        //    ungrabs once and dismisses cleanly.
        assert_eq!(
            drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                live_surface0,
                gen2,
                valid_backdrop_press_context(live_handle0, 3, 9_999),
            ),
            BackdropDismissOutcome::DismissedPopover
        );
        {
            let shared = harness.shared.borrow();
            assert_eq!(shared.ungrab_calls, vec![(TEST_MASTER_POINTER_ID, 9_999)]);
            assert_eq!(shared.hidden_popup_epochs, vec![(gen2, ticket3)]);
            assert_all_surfaces_empty_and_closed(&shared);
        }
    }

    #[test]
    fn outside_click_press_reentry_in_ungrab_and_release_preserves_newer_state() {
        let specs = two_monitor_dismiss_test_specs();
        let t0 = Instant::now();

        // Case A: Same-generation `Refresh` reentering synchronously inside `ungrab_master_pointer`
        // (simulating Wayland crossing callbacks emitted by `gdk_device_ungrab`).
        {
            let harness = CoordinatorTestHarness::new_tracker_backed(
                true,
                MapConfigureBehavior::SyncValidWorkarea,
            );
            let (gen1, ticket1) = admit_open_popup_for_dismiss_test(&harness, &specs, t0);
            assert_eq!((gen1, ticket1), (1, 1));
            let (s0, h0) = {
                let shared = harness.shared.borrow();
                (shared.entries[0].surface_id, shared.entries[0].handle_id)
            };

            let specs_for_hook = specs.clone();
            harness.shared.borrow_mut().on_ungrab_hook = Some(Box::new(move |h| {
                let refresh_res = drive_execute_popup_map_attempt(
                    &h.state_cell,
                    h,
                    PopupAttemptKind::Refresh,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_for_hook,
                    None,
                    || t0 + Duration::from_millis(30),
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |gen, ticket| {
                        assert_eq!((gen, ticket), (1, 2));
                        Ok(())
                    },
                )
                .expect("reentrant same-generation refresh during ungrab must succeed");
                assert!(refresh_res.is_admitted_sync());
            }));

            let outcome = drive_outside_click_backdrop_press(
                &harness.state_cell,
                &harness,
                s0,
                gen1,
                valid_backdrop_press_context(h0, 1, 5_001),
            );
            assert_eq!(
                outcome,
                BackdropDismissOutcome::SupersededOrCancelledAfterUngrab
            );
            {
                let coord = harness.state_cell.borrow();
                assert_eq!(coord.active_generation(), Some(1));
                assert_eq!(coord.active_admission_seq(), Some(2));
                assert_eq!(coord.enabled_surface_count(), 2);

                let shared = harness.shared.borrow();
                assert_eq!(shared.ungrab_calls, vec![(TEST_MASTER_POINTER_ID, 5_001)]);
                assert!(shared.hidden_popup_epochs.is_empty());
                let tp = shared.tracker_popup.as_ref().unwrap();
                assert!(tp.window_visible && tp.window_mapped && tp.tracker.is_mapped());
                assert_eq!(tp.tracker.current_generation(), 1);
                assert_eq!(tp.tracker.latest_attempt_ticket(), 2);
            }

            // Newer `(1, 2)` backdrop still dismisses cleanly on the next valid outside click.
            let (new_s0, new_h0) = {
                let shared = harness.shared.borrow();
                let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
                assert_eq!(active.len(), 2);
                (active[0].surface_id, active[0].handle_id)
            };
            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    new_s0,
                    1,
                    valid_backdrop_press_context(new_h0, 1, 5_002),
                ),
                BackdropDismissOutcome::DismissedPopover
            );
            assert_eq!(harness.shared.borrow().hidden_popup_epochs, vec![(1, 2)]);
            assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
        }

        // Case B: Newer-generation `Open` and pure `hide` during `ungrab_master_pointer`.
        {
            let harness = CoordinatorTestHarness::new_tracker_backed(
                true,
                MapConfigureBehavior::SyncValidWorkarea,
            );
            let (gen1, ticket1) = admit_open_popup_for_dismiss_test(&harness, &specs, t0);
            assert_eq!((gen1, ticket1), (1, 1));
            let (s0, h0) = {
                let shared = harness.shared.borrow();
                (shared.entries[0].surface_id, shared.entries[0].handle_id)
            };

            let specs_for_reopen = specs.clone();
            harness.shared.borrow_mut().on_ungrab_hook = Some(Box::new(move |h| {
                // Hide gen 1 then immediately open + admit gen 2 during `ungrab`.
                {
                    let mut shared = h.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.tracker.on_unmapped();
                }
                drive_release_all(&h.state_cell, h);
                let (gen2, ticket2) = admit_open_popup_for_dismiss_test(
                    h,
                    &specs_for_reopen,
                    t0 + Duration::from_millis(40),
                );
                assert_eq!((gen2, ticket2), (2, 2));
            }));

            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    s0,
                    gen1,
                    valid_backdrop_press_context(h0, 1, 6_001),
                ),
                BackdropDismissOutcome::SupersededOrCancelledAfterUngrab
            );
            {
                let coord = harness.state_cell.borrow();
                assert_eq!(coord.active_generation(), Some(2));
                assert_eq!(coord.active_admission_seq(), Some(2));
                assert_eq!(coord.enabled_surface_count(), 2);

                let shared = harness.shared.borrow();
                assert!(shared.hidden_popup_epochs.is_empty());
                let tp = shared.tracker_popup.as_ref().unwrap();
                assert!(tp.window_visible && tp.window_mapped && tp.tracker.is_mapped());
                assert_eq!(tp.tracker.current_generation(), 2);
            }

            // Now test pure `hide` during `ungrab` on `(2, 2)`: no duplicate popup hide.
            let (gen2_s0, gen2_h0) = {
                let shared = harness.shared.borrow();
                let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
                (active[0].surface_id, active[0].handle_id)
            };
            harness.shared.borrow_mut().on_ungrab_hook = Some(Box::new(move |h| {
                {
                    let mut shared = h.shared.borrow_mut();
                    let tp = shared.tracker_popup.as_mut().unwrap();
                    tp.window_visible = false;
                    tp.window_mapped = false;
                    tp.tracker.on_unmapped();
                }
                drive_release_all(&h.state_cell, h);
            }));
            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    gen2_s0,
                    2,
                    valid_backdrop_press_context(gen2_h0, 1, 6_002),
                ),
                BackdropDismissOutcome::SupersededOrCancelledAfterUngrab
            );
            assert!(harness.shared.borrow().hidden_popup_epochs.is_empty());
            assert_all_surfaces_empty_and_closed(&harness.shared.borrow());
        }

        // Case C: Same-generation `Refresh`, newer-generation `Open`, and pure `hide` reentering
        // during fail-open surface release (`on_release_hook` AFTER `ungrab` already succeeded).
        {
            let harness = CoordinatorTestHarness::new_tracker_backed(
                true,
                MapConfigureBehavior::SyncValidWorkarea,
            );
            let (gen1, ticket1) = admit_open_popup_for_dismiss_test(&harness, &specs, t0);
            assert_eq!((gen1, ticket1), (1, 1));
            let (s0, h0) = {
                let shared = harness.shared.borrow();
                (shared.entries[0].surface_id, shared.entries[0].handle_id)
            };

            // 1. Same-generation `Refresh` (`ticket = 2`) during fail-open release of `(1, 1)`.
            let specs_for_refresh = specs.clone();
            harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
                let refresh_res = drive_execute_popup_map_attempt(
                    &h.state_cell,
                    h,
                    PopupAttemptKind::Refresh,
                    Some(WaylandKeyboardPolicy::ExclusiveThenOnDemand),
                    true,
                    &specs_for_refresh,
                    None,
                    || t0 + Duration::from_millis(60),
                    WAYLAND_POPUP_PENDING_MAP_TIMEOUT,
                    |gen, ticket| {
                        assert_eq!((gen, ticket), (1, 2));
                        Ok(())
                    },
                )
                .expect("reentrant same-generation refresh during release must succeed");
                assert!(refresh_res.is_admitted_sync());
            }));

            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    s0,
                    gen1,
                    valid_backdrop_press_context(h0, 1, 7_001),
                ),
                BackdropDismissOutcome::SupersededOrCancelledDuringRelease
            );
            {
                let coord = harness.state_cell.borrow();
                assert_eq!(coord.active_generation(), Some(1));
                assert_eq!(coord.active_admission_seq(), Some(2));
                assert_eq!(coord.enabled_surface_count(), 2);

                let shared = harness.shared.borrow();
                assert_eq!(shared.ungrab_calls, vec![(TEST_MASTER_POINTER_ID, 7_001)]);
                assert!(shared.hidden_popup_epochs.is_empty());
                let tp = shared.tracker_popup.as_ref().unwrap();
                assert!(tp.window_visible && tp.window_mapped && tp.tracker.is_mapped());
                assert_eq!(tp.tracker.current_generation(), 1);
                assert_eq!(tp.tracker.latest_attempt_ticket(), 2);
            }

            // 2. Newer-generation `Open` (`gen = 2, ticket = 3`) during fail-open release of `(1, 2)`.
            let (s_t2, h_t2) = {
                let shared = harness.shared.borrow();
                let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
                (active[0].surface_id, active[0].handle_id)
            };
            let specs_for_open = specs.clone();
            harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
                let (gen2, ticket3) = admit_open_popup_for_dismiss_test(
                    h,
                    &specs_for_open,
                    t0 + Duration::from_millis(80),
                );
                assert_eq!((gen2, ticket3), (2, 3));
            }));

            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    s_t2,
                    1,
                    valid_backdrop_press_context(h_t2, 1, 7_002),
                ),
                BackdropDismissOutcome::SupersededOrCancelledDuringRelease
            );
            {
                let coord = harness.state_cell.borrow();
                assert_eq!(coord.active_generation(), Some(2));
                assert_eq!(coord.active_admission_seq(), Some(3));
                assert_eq!(coord.enabled_surface_count(), 2);
                assert!(harness.shared.borrow().hidden_popup_epochs.is_empty());
            }

            // 3. Pure `hide` during fail-open release of `(2, 3)`: does NOT call hide a second time.
            let (s_t3, h_t3) = {
                let shared = harness.shared.borrow();
                let active: Vec<_> = shared.entries.iter().filter(|e| !e.released).collect();
                (active[0].surface_id, active[0].handle_id)
            };
            harness.shared.borrow_mut().on_release_hook = Some(Box::new(move |h| {
                let mut shared = h.shared.borrow_mut();
                let tp = shared.tracker_popup.as_mut().unwrap();
                tp.window_visible = false;
                tp.window_mapped = false;
                tp.tracker.on_unmapped();
            }));

            assert_eq!(
                drive_outside_click_backdrop_press(
                    &harness.state_cell,
                    &harness,
                    s_t3,
                    2,
                    valid_backdrop_press_context(h_t3, 1, 7_003),
                ),
                BackdropDismissOutcome::SupersededOrCancelledDuringRelease
            );
            {
                let shared = harness.shared.borrow();
                assert_eq!(
                    shared.ungrab_calls,
                    vec![
                        (TEST_MASTER_POINTER_ID, 7_001),
                        (TEST_MASTER_POINTER_ID, 7_002),
                        (TEST_MASTER_POINTER_ID, 7_003),
                    ]
                );
                assert!(shared.hidden_popup_epochs.is_empty());
                assert_all_surfaces_empty_and_closed(&shared);
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn classify_gdk_pointer_event_accepts_only_initial_button_press() {
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::ButtonPress, 1),
            BackdropPointerEvent::ButtonPress { button: 1 }
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::ButtonPress, 3),
            BackdropPointerEvent::ButtonPress { button: 3 }
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::MotionNotify, 1),
            BackdropPointerEvent::PointerMotion
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::EnterNotify, 1),
            BackdropPointerEvent::PointerMotion
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::LeaveNotify, 1),
            BackdropPointerEvent::PointerMotion
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::ButtonRelease, 1),
            BackdropPointerEvent::NonInitialButtonEvent
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::DoubleButtonPress, 1),
            BackdropPointerEvent::NonInitialButtonEvent
        );
        assert_eq!(
            classify_gdk_pointer_event(gdk::EventType::TripleButtonPress, 1),
            BackdropPointerEvent::NonInitialButtonEvent
        );
    }
}
