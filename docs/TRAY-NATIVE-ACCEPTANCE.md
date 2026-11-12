# Native Tray Popover Placement, Input Lifecycle, and Acceptance Scope

This document records the implemented native system tray popover architecture in
`apps/desktop/src-tauri/src/tray.rs` and `apps/desktop/src-tauri/src/tray_linux.rs`,
the truthfulness boundaries enforced across display backends, and the live qualification
gates required before claiming end-to-end desktop acceptance.

## 1. Scope and Current Verification Boundary

- **Implemented in source**:
  - Native Linux Wayland icon-anchored popup placement via `gtk-layer-shell = "0.8.2"`
    (`features = ["v0_6"]`, paired with `gtk = "0.18"` and `gdk = "0.18"` using
    `features = ["v3_24"]`) on compositors advertising `zwlr_layer_shell_v1` (Sway, Hyprland,
    and compatible wlroots compositors).
  - Native Wayland (`zwlr_layer_shell_v1` v4+) empty-workspace and multi-monitor outside-click
    dismissal via bounded, main-thread-owned GTK3 transparent RGBA transient input surfaces
    (`LINUX_TRAY_BACKDROP_WINDOW_ROLE`, at most one per actual `gdk::Monitor`, capped at
    `MAX_WAYLAND_OUTSIDE_CLICK_MONITORS = 16`, `Layer::Top`, `KeyboardMode::None`,
    `exclusive_zone = 0`) ordered strictly below the `Layer::Overlay` popup, with zero additional
    WebKit/Tauri webviews, global input hooks, input grabs, click replay, or extra tray backends.
  - Pre-map empty-region realization and bounded post-configure enablement: each GTK backdrop is
    realized with an **explicit empty `gtk::cairo::Region`** (`&empty_region`, `0` rectangles via
    `GdkWindow::input_shape_combine_region(&empty_region, 0, 0)` in `gdk 0.18.2` — never left
    unshaped/null, which defaults to the full surface input region) before its first `show()`,
    registers main-thread ownership prior to mapping, validates configured allocation bounds +
    popup visibility + open generation before enabling `(0, 0, width, height)` input capture, and
    enforces a finite `500ms` configure deadline (`WAYLAND_BACKDROP_CONFIGURE_TIMEOUT`).
  - Ticket-fenced asynchronous popup-map and controlled-remap admission with truthful failure
    reporting: on Linux Tauri/Tao (`tauri-runtime-wry` -> `tao::window::Window::set_visible`),
    `popover.show()` queues `WindowRequest::Visible(true)` onto a GLib channel rather than acting as
    a synchronous GTK map barrier (`window.show_all()` runs on a later main-loop turn). Both initial
    presentation (`open_and_present_linux_popover`) and live scale/hotplug refreshes
    (`refresh_linux_popover_size_on_scale_change`, `connect_monitor_added`) share a single
    `(generation, attempt_ticket)` coordinator (`drive_execute_popup_map_attempt`,
    `drive_pending_map_progress`, `drive_pending_map_deadline`, and `drive_popup_transient_unmap`)
    that reserves its ticket upfront before pre-teardown/remap/`show()`, arms
    `PendingPopupMapAdmission` with a finite absolute `1000ms` pending-map deadline
    (`WAYLAND_POPUP_PENDING_MAP_TIMEOUT`, separate from the unchanged `500ms`
    `WAYLAND_BACKDROP_CONFIGURE_TIMEOUT`), retains pending admission across controlled
    `LayerShell::set_monitor` transient unmaps, and completes `drive_sync_for_attempt` on that same
    reserved ticket only after explicitly armed `connect_map` (with at most one idle defer) confirms
    native `is_visible() && is_mapped() && is_layer_window()`. Genuine `hide` (synchronously hidden
    on the GTK main thread via `hide_linux_popover_gtk_window_on_main_thread`), `quit`,
    `monitor-removed`, unsolicited unmaps, expired deadlines, stale generations, and same-generation
    newer tickets are strictly fenced with zero blanket map resurrection, while `LinuxOpenOutcome`
    (`Presented`, `SupersededSameGeneration`, `SupersededOrCancelled`) via
    `TrayState::apply_linux_open_outcome` prevents stale errors from triggering global teardown and
    prevents stale `None` outcomes from clearing newer notices.
  - Reentrancy-safe fail-open teardown and Wayland implicit-grab release before backdrop
    destruction: on GTK3/Wayland (`gdk/wayland/gdkdevice-wayland.c`), `wl_pointer.button` press
    installs a client-side implicit master-pointer grab (`_gdk_display_add_device_grab(...,
    implicit = TRUE)`) bound to the pressed `GdkWindow`; synchronously hiding/closing an
    outside-click backdrop on initial `ButtonPress` without releasing that implicit grab caused
    GDK to route subsequent popup button events to the destroyed backdrop window even after
    `wl_pointer.enter` targeted the reopened live popup. `drive_outside_click_backdrop_press`
    validates GTK main-thread execution, genuine initial `ButtonPress` (`button >= 1`), exact
    active enabled `(surface_id, generation, admission_seq, attempt_ticket)` ownership, live
    unfenced popup state, exact `event.window() == callback_win.window() ==
    tracked.window.window()` and `gdk::Display` identity, and `event.device()` itself being the
    seat's `gdk::DeviceType::Master` pointer (`device == seat.pointer()`, never promoting slave or
    `source_device` handles), then calls public `gdk::ffi::gdk_device_ungrab` at most once with
    exact `event.time()` via a single private Linux-only `#[allow(unsafe_code)]` typed-reference
    wrapper (`ungrab_gdk_master_pointer_device`, under crate-root `#![deny(unsafe_code)]`) while
    holding owned GObject references and **zero** `RefCell` borrows or `Mutex` guards — strictly
    **before** disabling input regions, disconnecting handlers, hiding, or closing owned backdrops.
    Because public Wayland `gdk_device_ungrab` may synchronously emit crossing/grab-broken events,
    the exact dismissal token is revalidated **after** `gdk_device_ungrab` before surface teardown
    (`drive_release_up_to_attempt`) and **again** after fail-open surface release before
    epoch-fenced popup hiding (`hide_popover_window_if_linux_epoch_current`), ensuring newer
    same-generation `Refresh`, newer-generation `Open`, or `hide` reentry survives intact. All
    other teardown paths likewise never hold a `RefCell` borrow across `show()` or GTK
    signal/teardown calls, scope post-teardown emptiness checks to `drained_epoch_ticket`,
    re-sample coordinator epoch and native popup visibility **after** fail-open teardown before
    publishing async configure/timeout notices (`drive_async_surface_configured` and
    `drive_async_configure_timeout`), and apply an explicit empty `gtk::cairo::Region`
    (`&empty_region`) before disconnecting per-open signal handlers, hiding, and closing each
    window across repeated `show`, `hide`, `toggle`, `blur`, `close`, `Escape`, `show_main`,
    `quit`, `ScaleFactorChanged`, `monitor-removed`, reentrant same-generation or newer-generation
    attempts, second-monitor failure, pending-map deadline, and configure timeout, surfacing
    truthful capability notices (`WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE` and
    `WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE`) without duplicate notice sentences.
  - Honest `PopoverPlacementOutcome::CompositorManaged` (`xdg_toplevel`) fallback on Wayland
    compositors that do not implement `zwlr_layer_shell_v1` (such as GNOME / Mutter), with
    zero fake `set_position` / `gtk_window_move` calls.
  - Native Linux X11 (`GdkX11Display`) per-monitor workarea clamping and root-coordinate
    positioning.
  - Inexpensive conditional macOS preparation (`#[cfg(not(target_os = "linux"))]`) preserving
    per-monitor `NSStatusItem` rect scaling, compact 3-row sizing, and monochrome template icon
    loading. Windows is out of scope and untouched.
  - Preserved `show_main` / `hide` / `quit` actions, `hide_to_tray` close-behavior persistence,
    single-instance lock and argument-conflict protection, and `main` window reachability
    fallbacks whenever tray creation or presentation fails.
  - Deterministic unit regression tests in `apps/desktop/src-tauri/src/tray.rs` and
    `apps/desktop/src-tauri/src/tray_linux.rs` exercising the shared production functions across
    queued `popover.show()` returning unmapped then admitting via `connect_map` / single idle probe
    on the pre-reserved ticket, `hide` / `quit` / unsolicited `unmap` / newer ticket rejection
    before map, `1000ms` pending-map deadline leaving `0` input surfaces and blocking late map or
    refresh resurrection, same-generation nested reentry across pre-teardown / remap / backdrop map
    preserving newer surfaces, mapped state, and notices, current and async configure/timeout
    failures releasing own surfaces and fencing post-teardown notices, `LinuxOpenOutcome` publication
    fencing in `tray.rs`, `CreatedEmpty` -> `MappedEmpty` -> `ActiveBounded` enablement,
    `1x`/`2x` negative-origin mixed-scale multi-monitor geometry, `Layer::Overlay` > `Layer::Top`
    ordering, inside-popup click routing, shared-seam `drive_outside_click_backdrop_press`
    master-pointer ungrab-before-teardown ordering, at-most-once dismissal, rejection of
    off-thread / stale / unconfigured / motion / release / missing-device / non-master /
    wrong-window / mismatched-display events, and survival of newer same-generation `Refresh` /
    newer-generation `Open` / `hide` reentry during both `ungrab` and fail-open `release`, explicit
    empty `cairo::Region` invariants, missing-coordinate fallbacks, `focus_follows_mouse`
    pointer-crossing tracking, click debounce, and post-blur reopen suppression. In-process harness
    tests prove coordinator/tracker state-machine and ordering invariants only; real GTK3/Wayland
    signal dispatch and compositor behavior require the parent's Linux build and live Sway VM
    qualification gates below.
- **Pending live parent qualification and software WebKitGTK refresh-blanking verification (not claimed by source edits alone)**:
  - Running Linux GTK compilation, `cargo fmt --check`, focused behavioral unit tests,
    `python3 -m unittest scripts/test_desktop_acceptance.py`, and `cargo clippy -- -D warnings`
    in the parent checkout against its local `CARGO_TARGET_DIR`.
  - Executing the strengthened isolated `1x` and `2x` Sway VM acceptance gates (persistent guest
    pointer and keyboard seat capabilities, **without** `for_window [all] floating enable`, **with**
    default `focus_follows_mouse yes`, and preserving unchanged isolated VM software-rendering flags
    `WLR_RENDERER=pixman`, `WEBKIT_DISABLE_DMABUF_RENDERER=1`, `WEBKIT_DISABLE_COMPOSITING_MODE=1`,
    and `LIBGL_ALWAYS_SOFTWARE=1`) to verify live `connect_map` backdrop admission, `>= 9.5s` `2x`
    pointer-transit dwell survival, empty-workspace outside-click implicit-grab release and
    dismissal, `Escape` dismissal, `10` immediate `1s`-target (`<= 2.5s` actual inter-capture time
    including overhead) `1x` reopen pixel and cross-output measurements over `>= 9.0s`
    (`refresh_paint_survival`) before `Open Threadlane`, `Quit`, and owned 4-process cleanup.
  - **Pending software WebKitGTK supplied-buffer backport verification**: while short single-pass
    checks could finish between periodic `4000ms` UI refreshes, sustained mixed-scale reopen across
    multiple refresh cycles exposed blank frames correlating with sparse `13x13` CPU damage updates
    in software WebKitGTK (`2.54.1`; upstream fix [`WebKit/WebKit@bfae1a7`](https://github.com/WebKit/WebKit/commit/bfae1a7),
    packaged separately without changing runtime environment flags, DOM/CSS, or reload behavior).
    Full native tray acceptance is **not** claimed until the parent runner executes the strengthened
    NixOS VM gate against the patched WebKitGTK dependency and passes both `refresh_paint_survival`
    and all 14 checks.

## 2. Implemented Platform Matrix

| Environment | Surface Role & Placement Route | Reported Outcome | Dismissal & Input Route |
| :--- | :--- | :--- | :--- |
| **Linux Wayland (`zwlr_layer_shell_v1` v4+)** (Sway, Hyprland, wlroots) | `LayerShell::init_layer_shell()` before `GtkWindow` realization; popup on `Layer::Overlay` (`exclusive_zone = 0`, `set_monitor(&gdk::Monitor)`, perpendicular `Edge` pair + clamped GDK logical margins) strictly above bounded per-monitor `Layer::Top` transparent RGBA outside-click surfaces (`KeyboardMode::None`, `exclusive_zone = 0`) | `IconAnchored` with valid SNI coordinates; `FallbackMonitorCorner` when coordinates are absent (+ capability notice if RGBA/backdrop creation is unavailable) | First outside button press on per-monitor `Layer::Top` backdrop (consumes that single click and releases popup + all backdrops fail-open), native GTK `Escape`, repeated tray icon click, `main` window `button-press-event`, and `Exclusive -> OnDemand` focus-out with `160ms` pointer-entry verification |
| **Linux Wayland (`zwlr_layer_shell_v1` v1–v3)** | Popup on `Layer::Top` (`exclusive_zone = 0`), `set_keyboard_interactivity(true)`; no outside-click backdrops mapped | `IconAnchored` / `FallbackMonitorCorner` + `WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE` | `Escape`, repeated tray click, and `main` window clicks dismiss; external-window/empty-workspace outside clicks surfaced as limited capability |
| **Linux Wayland without `zwlr_layer_shell_v1`** (GNOME / Mutter) | Standard frameless `xdg_toplevel`; no `gtk_window_move`, `set_position`, or layer backdrops issued | `CompositorManaged` (`WAYLAND_COMPOSITOR_PLACEMENT_NOTICE` or `WAYLAND_MISSING_COORDS_NOTICE`) | `Escape`, focus-out (`WindowEvent::Focused(false)`), `main` window click, repeated tray click |
| **Linux X11 / XWayland (`GdkX11Display`)** | Utility `GtkApplicationWindow` clamped to target `gdk::Monitor::workarea()`, positioned via `gtk_win.move_(gdk_x, gdk_y)`; no Wayland backdrops | `IconAnchored` with valid coordinates; `FallbackMonitorCorner` when coordinates are `(0, 0)` / absent | `Escape`, focus-out (`WindowEvent::Focused(false)`), `main` window click, repeated tray click |
| **macOS (`#[cfg(not(target_os = "linux"))]`)** | Conditional preparation only: `resolve_tray_anchor_for_monitors` + `PhysicalPosition` from `TrayIcon::rect()` / click | `IconAnchored` or `FallbackMonitorCorner` | `Escape`, `WindowEvent::Focused(false)`, `main` `Focused(true)`, repeated tray click (untested on macOS hardware in this milestone) |

## 3. Core Behavioral Invariants

### 3.1 Pre-Realization Layer-Shell Initialization
`gtk_layer_shell::LayerShell::init_layer_shell` must execute before the underlying
`gtk::ApplicationWindow` is realized. `ensure_or_create_tray_popover_window` builds the hidden
`tray` window with `.visible(false).focused(false)` (where Tao's hidden GTK path hides the
window instead of calling `show_all()`) and immediately invokes
`tray_linux::initialize_linux_popover_window`:
1. Checks `gtk_layer_shell::is_supported()` on `GdkWaylandDisplay`.
2. Checks `!gtk_win.is_layer_window() && !gtk_win.is_realized() && !gtk_win.is_mapped()` before
   calling `gtk_win.init_layer_shell()`. It **never** calls `gtk_win.unrealize()` on an
   already-realized Tauri/Wry-owned window (preserving backing native and WebKit handles) and
   never invokes `init_layer_shell()` on a realized or mapped window.
3. Verifies `gtk_win.is_layer_window()`. If a toolkit step realized the window before our safe
   hook, it reports `WAYLAND_REALIZED_FALLBACK_NOTICE` (`CompositorManaged`) with zero fake
   `set_position` calls and retains full `main` window reachability. **Note**: This fallback is
   a safety guard only and is **not** Sway acceptance; live Sway VM qualification must prove
   that normal hidden window construction takes the genuine `zwlr_layer_shell_v1`
   `IconAnchored` route (otherwise window/webview construction order must be adjusted).
4. Before `popover.show()`, the actual `GtkBin` content subtree (`gtk_win.child()`, excluding Tao's
   non-content CSD `GtkEventBox` titlebar slot) is explicitly marked visible via `show_all()`
   because Wry only runs `show_all()` on its X11 build branch when the parent Tauri window starts
   hidden.

### 3.2 Compact 3-Row Fixed Sizing (`< 500px`)
- The compact tray popover uses fixed logical dimensions `360x320` (`TRAY_POPOVER_WIDTH` x
  `TRAY_POPOVER_HEIGHT`), sized for up to `TRAY_MAX_CONTRACT_ROWS = 3` concise two-line status
  rows, header, optional notice banner, and footer without growing the native window for legacy
  dense cards.
- Both `WebviewWindowBuilder` (`min_inner_size(280.0, 240.0)`, `max_inner_size(360.0, 320.0)`)
  and GTK (`set_size_request`, `set_default_size`, `resize`) enforce this compact size
  independently of `main`'s `500px` minimum height (`MAIN_WINDOW_MIN_HEIGHT`, checked at
  compile time and in unit tests).
- On constrained monitor workareas, dimensions clamp to `workarea - 24px` without exceeding
  the monitor bounds.

### 3.3 Non-Exclusive, Non-Stretching Wayland Anchoring and Multi-Monitor Scale Fidelity
- `WaylandLayerShellPlacement` always sets `exclusive_zone = 0` (never reserving panel struts
  or shifting tiled windows) and anchors **strictly one vertical edge (`Top` xor `Bottom`) and
  one horizontal edge (`Left` xor `Right`)** (`is_compact_non_exclusive() == true`), preventing
  the compositor from stretching the surface across output dimensions.
- Coordinates from `StatusNotifierItem::Activate(x, y)` and `TrayRect` are matched against each
  `gdk::Monitor`'s own logical geometry and physical span (`derive_physical_spans`), never
  converted upfront using `monitors[0].scale_factor`.
- Reopening on a different monitor or after `WindowEvent::ScaleFactorChanged` recomputes the
  target `gdk::Monitor`, updates `LayerShell::set_monitor`, clears all four `Edge` anchors
  before applying the new perpendicular pair, and updates margins cleanly.

### 3.4 Pointer-Follow-Focus (`focus_follows_mouse yes`), Empty-Workspace Root Cause, and Native Outside-Click Dismissal
Without relying on Sway WM rules (`for_window [all] floating enable` or `focus_follows_mouse no`),
new WebKit/Tauri webviews, global input hooks, root privileges, or extra tray backends:
- **Zero panel-edge gap & no panel-rectangle fiction**: `StatusNotifierItem::Activate(x, y)`
  provides only a point `(x, y)` with no generic panel bounds or user input serial. Threadlane
  never invents hardcoded `32px` strips or guessed whole-panel rectangles. On Wayland layer-shell,
  both the popup and transient outside-click backdrops set `exclusive_zone = 0` so the compositor
  itself excludes real panel exclusive zones, and the popup edge anchored toward a panel uses
  `panel_gap = 0` so pointer travel from the tray icon into the popup does not cross an exposed
  strip of tiled workspace window.
- **Transient hover-unfocus protection**: On protocol version `>= 4`
  (`WaylandKeyboardPolicy::ExclusiveThenOnDemand`), the popup layer surface opens with
  `KeyboardMode::Exclusive` (ensuring immediate `Escape` focus and initial hover stability) and
  schedules the `WAYLAND_ON_DEMAND_TRANSITION_DELAY` (`250ms`) transition to
  `KeyboardMode::OnDemand` upon pointer entry or confirmed native GTK layer mapping, **independent**
  of backdrop admission success (so mapped popups reporting RGBA, surface, or deadline capability
  notices never remain stuck in `Exclusive`, while legacy `< v4`
  `ExclusiveLimitedExternalOutsideClick` never transitions to `OnDemand`). Timer callbacks are
  fenced by **both** `generation` and `attempt_ticket` (`should_apply_wayland_on_demand_transition`)
  so an older same-generation attempt cannot alter newer geometry or focus.
  When `WindowEvent::Focused(false)` fires after `POPOVER_BLUR_GRACE_PERIOD` (`180ms`),
  `handle_linux_popover_unfocused` verifies focus loss after `WAYLAND_FOCUS_LOSS_VERIFY_DELAY`
  (`160ms`) using generation-scoped `WaylandLayerFocusTracker`, keeping the popup open whenever
  the pointer has entered the popup (`gdk::NotifyType::Inferior` child webview crossings are
  preserved as inside).
- **Settled empty-workspace Wayland root cause**: On `zwlr_layer_shell_v1` v4+ (such as Sway /
  wlroots), a surface committed with `KeyboardMode::OnDemand` relinquishes keyboard focus only
  when another focusable surface claims focus. Clicking an **empty workspace background** has no
  focusable client surface beneath the pointer, so the compositor emits `wl_pointer.leave` without
  any `wl_keyboard.leave` (leaving GTK `is-active` / focus `true`). A focus timer or cached focus
  flag cannot synthesize a missing compositor signal.
- **Bounded per-monitor outside-click input surfaces & standard first-click dismissal semantics**:
  - Strictly while the Wayland `zwlr_layer_shell_v1` v4+ popup is visible, Threadlane maps at
    most one owned, lightweight GTK3 `gtk::Window` (`LINUX_TRAY_BACKDROP_WINDOW_ROLE`) per
    enumerated `gdk::Monitor` (bounded by `MAX_WAYLAND_OUTSIDE_CLICK_MONITORS = 16`), configured
    on `Layer::Top` with `KeyboardMode::None`, `exclusive_zone = 0`, 4-edge monitor-local
    anchoring (`(0, 0, width, height)` in surface-local coordinates even when global monitor
    origins are negative across mixed `1x`/`2x` outputs), and transparent RGBA drawing
    (`cairo::Operator::Source` `(0, 0, 0, 0)`).
  - **Pre-map explicit empty input region & post-configure bounded enablement**:
    1. Before `show()`, `create_realized_empty_surface` sets a `1x1` pre-configure placeholder
       default size, calls `window.realize()`, and applies an **explicit empty
       `gtk::cairo::Region`** (`&empty_region`, `0` rectangles via
       `GdkWindow::input_shape_combine_region(&empty_region, 0, 0)` in `gdk 0.18.2` — whereas an
       unshaped/null GDK input region defaults to the full surface input region and is never
       fail-open) (`BackdropLifecyclePhase::CreatedEmpty`).
    2. `drive_sync_for_attempt` registers main-thread ownership on the pre-reserved
       `attempt_ticket` in `LINUX_OUTSIDE_CLICK_STATE` before calling `window.show()`
       (`BackdropLifecyclePhase::MappedEmpty`), keeping every partial surface's input region
       explicitly empty until all target monitors complete admission.
    3. Only after `connect_size_allocate` / configured allocation passes
       `WaylandOutsideClickBackdropSpec::classify_allocation` via
       `drive_surface_configured_with_ticket` (`drive_async_surface_configured`), live popup
       visibility and active `(generation, admission_seq)` ownership are confirmed, and main-thread
       ownership is verified does `enable_bounded_input_region` apply the surface-local
       `(0, 0, width, height)` `cairo::Region` via
       `GdkWindow::input_shape_combine_region(&bounded_region, 0, 0)`
       (`BackdropLifecyclePhase::ActiveBounded`) and queue a frame commit.
    4. If any mapped surface has not received a valid compositor configuration on open,
       `WAYLAND_BACKDROP_CONFIGURE_TIMEOUT` (`500ms`) runs `drive_async_configure_timeout`, tearing
       down all surfaces for that admission fail-open and re-sampling epoch and native popup
       visibility **after** teardown before publishing
       `WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE`.
  - **Explicit relative layer ordering**: On protocol `>= 4`, the interactive `tray` popup maps
    on `Layer::Overlay` strictly above all `Layer::Top` outside-click backdrops
    (`WaylandShellLayer::Overlay > WaylandShellLayer::Top`), never depending on same-layer map
    order.
  - **Standard first-click dismissal**:
    1. Clicks inside the `360x320` popup hit the `Layer::Overlay` popup directly above `Layer::Top`
       backdrops and operate popup controls normally.
    2. Pointer motion and crossings (`MotionNotify`, `EnterNotify`, `LeaveNotify`, and non-initial
       button release events) anywhere on any monitor **never** dismiss the popup.
    3. The first genuine outside mouse button press (`gdk::EventType::ButtonPress`, `button >= 1`)
       on an empty workspace, uncovered desktop region, or secondary monitor hits that monitor's
       `Layer::Top` backdrop, validates exact `(surface_id, generation, admission_seq,
       attempt_ticket)` ownership, live window/display identity, and seat master-pointer identity,
       **releases that master pointer's implicit GDK grab once via `gdk_device_ungrab(...,
       event.time())` before any backdrop teardown**, revalidates the exact token, **releases all
       backdrops up to that attempt fail-open**, revalidates the exact token again, hides the popup
       via `hide_popover_window_if_linux_epoch_current`, and **consumes only that single dismissing
       click** (`glib::Propagation::Stop`), preserving the running application and keeping `main`
       reachable. Subsequent clicks — including clicks inside a reopened popup — are never trapped
       by a stale implicit grab on a destroyed backdrop and pass directly to their live target
       surface with zero remaining input surfaces.
- **Strict GTK main-thread ownership, reentrancy safety, & fail-open cleanup**:
  - Non-`Send` GTK backdrop handles live exclusively on the GTK main thread inside
    `LINUX_OUTSIDE_CLICK_STATE` (`thread_local! { RefCell<OutsideClickCoordinatorState<...>> }`)
    and `LINUX_DISPLAY_MONITOR_GUARD_ATTACHED` (`thread_local! { Cell<bool> }`), guarded by
    `gtk::is_initialized_main_thread()`, while cross-thread Tauri/`ksni`/single-instance callbacks
    marshal onto `AppHandle::run_on_main_thread`.
  - `LINUX_OUTSIDE_CLICK_STATE` is borrowed **only** across leaf in-memory vector/field updates and
    is **never** held across `window.realize()`, `window.show()`, `gdk_device_ungrab`,
    `input_shape_combine_region`, `hide_popover_window`,
    `hide_popover_window_if_linux_epoch_current`, or backend callbacks. Synchronous GTK reentrancy
    during `gdk_device_ungrab`, fail-open surface release, pre-teardown, remap/`show()`, or
    backdrop mapping (`hide`, `unmap`, `monitor-removed`, same-generation nested `Refresh`, or a
    newer open generation) never encounters a borrowed `RefCell`, never triggers an invalid
    global-zero assertion (post-teardown emptiness assertions are scoped to `drained_epoch_ticket`),
    and leaves every retired surface explicitly empty and closed while preserving any newer owner's
    surfaces, mapped state, and notice.
  - Generation- and ticket-scoped coordination (`latest_attempt_ticket` kept distinct from
    `active_admission_seq` so a failed current attempt clears active surface ownership while still
    identifying itself for truthful notice reporting) guarantees finite per-open signal attachment
    and releases every surface across repeated `show`, `hide`, `toggle`, `blur`, `close`, `Escape`,
    `show_main`, `quit`, `ScaleFactorChanged`, `monitor-removed`, partial multi-monitor failures,
    pending-map deadlines, and configure timeouts.
  - **Asynchronous `popover.show()` pending-map admission, controlled remaps, & fenced cancellation**:
    On Linux Tauri/Tao (`tauri-runtime-wry` -> `tao::window::Window::set_visible`), `popover.show()`
    dispatches `WindowRequest::Visible(true)` over a GLib channel so `window.show_all()` runs
    asynchronously on a subsequent main-loop turn, and calling `LayerShell::set_monitor` on a mapped
    layer window transiently unmaps and remaps that `GtkWindow`. Both
    `open_and_present_linux_popover` and `refresh_linux_popover_size_on_scale_change` (including
    `monitor-added` while live or pending map via `has_live_or_pending_open`) execute through the
    single shared `drive_execute_popup_map_attempt` path:
    1. Reserves `(generation, attempt_ticket)` upfront (`begin_open_attempt` /
       `begin_refresh_attempt` + `reserve_attempt_ticket`) before pre-teardown, controlled remap,
       or `popover.show()`, and arms `PendingPopupMapAdmission` with finite absolute deadline
       `entry_now + WAYLAND_POPUP_PENDING_MAP_TIMEOUT` (`1000ms`).
    2. Guards actual webview/GTK size, `LayerShell::set_monitor`, anchor/margin, initial
       `KeyboardMode::Exclusive`, pre-show notice, and `popover.show()` writes with shared
       production epoch checkpoints (`drive_present_layer_popup_with_checkpoints` and
       `drive_present_non_layer_popup_with_checkpoints` via `is_presentation_epoch_current`) so
       synchronous reentry inside `set_monitor` or `show` halts stale outer frames immediately
       without overwriting newer geometry or focus.
    3. Samples fresh monotonic post-presentation time (`now_fn().max(entry_now)`) before evaluating
       `ImmediatePostConfigure` so presentation latency neither extends the `1000ms` window nor
       admits an already-mapped refresh/open past the absolute deadline. If
       `gtk_win.is_visible() && gtk_win.is_mapped() && gtk_win.is_layer_window()` is not yet true
       within the deadline, retains `PendingPopupMapAdmission` with `0` backdrop surfaces and
       schedules the remaining duration up to `WAYLAND_POPUP_PENDING_MAP_TIMEOUT`.
    4. Explicitly armed `connect_map` (`PendingMapTrigger::MapSignal`) **always** defers admission
       and any nested GTK backdrop creation to at most one `(generation, attempt_ticket)`-fenced
       `glib::idle_add_local_once` step (`PendingMapTrigger::IdleProbe`) **after** the popup
       `::map` signal stack unwinds — even when native `visible + mapped + layer` flags are already
       `true` inside `::map`. Once `IdleProbe` (or an already-mapped `Refresh` on
       `ImmediatePostConfigure`) confirms native `visible + mapped + layer` state outside `::map`
       within the `1000ms` deadline, it completes `complete_attempt_map(generation, attempt_ticket,
       true)` + `drive_sync_for_attempt` on the pre-reserved `attempt_ticket`, schedules the fenced
       v4 `Exclusive -> OnDemand` transition, and requests a bounded epoch-guarded `queue_draw`
       (`drive_request_settled_popup_redraw` / `drive_settled_popup_redraw_signal` /
       `drive_settled_popup_redraw_idle`) across the mapped popup root and its qualified
       `GtkBin::child()` content subtree (excluding Tao's empty CSD `GtkEventBox` titlebar via
       `gtk_win.child()` + `gtk_win.titlebar()` identity, never ignoring `EventBox` by widget
       class) once root, content container, and real `WebKitWebView` are visible, mapped, and
       allocated beyond `1x1` — retaining the matching pending wait with zero spin loops if
       allocation transiently unsettles before the single queued idle runs (`gtk-layer-shell`
       detaches `wl_surface` buffers on layer remap and calls `gdk_window_invalidate_rect(...,
       FALSE)` without invalidating child `GdkWindow`s; live Sway VM execution remains the sole
       verification gate for native reopen rendering and input).
    5. On `connect_unmap`, `on_transient_unmap_preserving_epoch` + `drive_popup_transient_unmap`
       releases active backdrops fail-open while preserving `PendingPopupMapAdmission` and any
       pending settled-redraw epoch **only** when a matching or newer `(generation, attempt_ticket)`
       controlled remap is armed; unsolicited unmaps, `connect_hide` (paired with synchronous
       `hide_linux_popover_gtk_window_on_main_thread` in `hide_popover_window`,
       `show_and_focus_main_window`, and `execute_explicit_quit`), `explicit_quit`, and
       `monitor-removed` cancel and fence permanently.
    6. If `WAYLAND_POPUP_PENDING_MAP_TIMEOUT` (`1000ms`) elapses before map completion,
       `drive_pending_map_deadline` disarms `(generation, attempt_ticket)` and records
       `ExpiredPopupMapAttempt`: any late `connect_map` (`drive_expired_map_late_progress`) defers
       out of `::map` to a single fenced `IdleProbe`, never resurrects `tracker.mapped` or admits
       outside-click surfaces (`0` input surfaces), and once native mapping settles schedules
       `Exclusive -> OnDemand` + settled popup redraw while truthfully publishing
       `WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE` when visible.
    7. `cleanup_and_classify_presentation_err` samples cancellation and supersession **before**
       calling own `abort_tracker_attempt` (which sets `cancelled_generation`), so expected
       `hide`/quit cancellation before a presentation `Err` returns
       `Ok(PopupAttemptExecutionOutcome::SupersededOrCancelled)` (`NoPublication`, without forcing
       `main` recovery) while genuine current presentation errors propagate `Err`
       (`RecoverMainOnCurrentFailure`). `open_and_present_linux_popover` returns `LinuxOpenOutcome`
       (`Presented`, `SupersededSameGeneration`, `SupersededOrCancelled`) consumed by
       `TrayState::apply_linux_open_outcome`.
  - Every backdrop release is **fail-open** and idempotent across handle clones:
    `release_gtk_backdrop_surface_fail_open` first applies an **explicit empty
    `gtk::cairo::Region`** (`gtk::cairo::Region::create()`, zero rectangles) via
    `GdkWindow::input_shape_combine_region(&empty_region, 0, 0)` (`gdk 0.18.2` takes
    `&cairo::Region` directly; an unshaped/null GDK input region defaults to full-surface input and
    is **never** fail-open), then synchronously disconnects `button-press-event`, `size-allocate`,
    and `draw` handlers, calls `hide()`, marks the window deletable, and calls `close()`.
- **All dismissal routes**:
  1. First outside mouse button press on any monitor's transient `Layer::Top` backdrop (Wayland
     `zwlr_layer_shell_v1` v4+, including empty workspaces and multi-monitor layouts).
  2. `Escape` key: handled natively via GTK `connect_key_press_event` across the `tray` window
     widget tree in addition to the webview keydown bridge.
  3. Focus-out (`WindowEvent::Focused(false)`): dismisses on X11, Wayland `xdg_toplevel`, and
     Wayland `zwlr_layer_shell_v1` v4+ (`OnDemand`) if another focusable surface claims keyboard
     focus without hitting a backdrop.
  4. Outside click on Threadlane's `main` window: `attach_linux_main_window_outside_click_guard`
     listens to `button-press-event` (actual mouse click, never hover-focus) to hide the popover
     immediately.
  5. Repeated tray icon click: `TrayState::evaluate_tray_click_toggle` hides a visible popover
     (`HideVisiblePopover`), suppresses accidental reopen within `250ms` of an auto-blur hide
     (`SuppressReopenAfterBlur`), and debounces duplicate D-Bus activations within `90ms`.

## 4. Truthful Protocol and Host Limitations

1. **Missing `StatusNotifierItem` coordinates (`(0, 0)` or context-menu `Open status`)**:
   `tray-icon 0.25.1`'s Linux `ksni` backend forwards `(x, y)` on `StatusNotifierItem::Activate`
   but always returns `None` from `TrayIcon::rect()`, and `MenuOpenStatus` carries no click
   point. Threadlane never reuses stale click coordinates from an earlier monitor click and
   never reports fallback placement as `IconAnchored`; it returns
   `PopoverPlacementOutcome::FallbackMonitorCorner` with `MISSING_TRAY_COORDINATES_FALLBACK_NOTICE`.
   On Wayland without an active `main`-window monitor hint, initial fallback leaves
   `monitor_index: None` so the compositor selects the active output rather than blindly
   forcing `monitors[0]`.
2. **Wayland compositors without `zwlr_layer_shell_v1` (GNOME / Mutter)**:
   Standard Wayland `xdg_toplevel` clients cannot position themselves in global output
   coordinates. Threadlane reports `CompositorManaged` (`WAYLAND_COMPOSITOR_PLACEMENT_NOTICE`),
   maps zero layer-shell backdrops, and preserves full `main` window reachability.
3. **Legacy `zwlr_layer_shell_v1` versions `1..=3`**:
   `KeyboardMode::OnDemand` was introduced in `zwlr_layer_shell_v1` version 4. On compositors
   exposing versions `1..=3`, Threadlane keeps the popup on `Layer::Top` without outside-click
   backdrops and surfaces `WAYLAND_LEGACY_LAYER_PROTOCOL_NOTICE`, while `Escape`, repeated tray
   clicks, and clicks on Threadlane's `main` window still dismiss.
4. **Unavailable RGBA visual, native outside-click surface failure, active-visible pending-map/admission failure, or configure timeout on Wayland v4+**:
   If the active GDK screen lacks an RGBA visual, creating/realizing/mapping/enabling a
   per-monitor `Layer::Top` backdrop fails on any output, a pending popup map attempt expires at
   `WAYLAND_POPUP_PENDING_MAP_TIMEOUT` (`1000ms`) or admission returns `CancelledFailOpen` while the
   attempt remains current, or compositor backdrop configuration does not arrive within
   `WAYLAND_BACKDROP_CONFIGURE_TIMEOUT` (`500ms`), `OutsideClickCoordinatorState` immediately tears
   down that attempt's partial or sibling surfaces fail-open (`drive_release_up_to_attempt`, never
   leaving an opaque window, invisible input trap, or clobbering a newer reentrant attempt),
   re-samples current attempt ownership and native popup visibility after teardown, surfaces
   `WAYLAND_TRANSPARENT_VISUAL_UNAVAILABLE_NOTICE` or
   `WAYLAND_OUTSIDE_CLICK_SURFACE_UNAVAILABLE_NOTICE` when the failed attempt is still current and
   visibly open, keeps `main` reachable, and never falls back to host or global input hooks.
5. **Clicks on inert external `NONE`-interactivity panel regions reserving an exclusive zone**:
   Wayland isolates pointer input per client, and `exclusive_zone = 0` backdrops intentionally
   respect the compositor's panel exclusive zones rather than guessing panel bounds. Clicking
   Threadlane's own SNI icon toggles via D-Bus `Activate`, whereas clicking inert empty space on
   an external `NONE`-interactivity bar surface inside its reserved exclusive zone produces no
   focus change or client event.
6. **Secondary-monitor SNI hosts emitting surface-local coordinates**:
   The D-Bus `org.kde.StatusNotifierItem.Activate(int x, int y)` method carries no `wl_output`
   identifier. Panels that emit compositor-global logical or physical coordinates map to the
   exact target monitor; if a third-party panel on a secondary output emits bar-local `(x, y)`
   offsets starting near `(0, 0)`, those values are indistinguishable in SNI from primary-output
   coordinates.

## 5. Sway VM Harness Verification Notes

- `zwlr_layer_shell_v1` surfaces are **not** regular `xdg_toplevel` containers and will **not**
  appear as floating/tiled views inside `swaymsg -t get_tree` container lists.
- Live qualification must run with persistent guest pointer **and** keyboard seat capabilities
  (`seat` capabilities `3`, valid XKB keymap, no synthetic setup keypress), `for_window [all] floating enable`
  removed, and `focus_follows_mouse yes` active, verifying from rendered output (`grim`) and
  Wayland/compositor layer-surface state:
  1. Genuine `zwlr_layer_shell_v1` `IconAnchored` placement on `Layer::Overlay` (`360x320`
     compact bounds on target monitor, including `2x` scale) with per-monitor
     `threadlane-tray-outside-click` surfaces mapped on `Layer::Top` (`KeyboardMode::None`,
     `exclusive_zone = 0`) strictly while open.
  2. Pointer transit from the Waybar SNI icon into the popup on `HEADLESS-2` (`scale 2.0`),
     sustained no-click hover dwell of at least `9.5s` (`MIN_POINTER_DWELL_MS = 9500ms`, spanning
     at least two `4000ms` UI refresh intervals plus margin), and pointer motion keep the popup
     visibly painted with `0` leaked workspace pixels on `HEADLESS-1`; inside-popup clicks reach
     the `Layer::Overlay` popup.
  3. Empty-workspace outside button press consumes that single click and immediately dismisses
     both the popup and **all** `threadlane-tray-outside-click` layer surfaces while preserving
     the running app and keeping `main` reachable across repeated open/dismiss/reopen cycles.
  4. Sustained mixed-scale reopen paint survival (`refresh_paint_survival` in
     `vm-acceptance-report.json`): on `HEADLESS-1` (`scale 1.0`) after `Escape` dismissal and
     reopen — **before** clicking `Open Threadlane` — `10` bounded immediate `grim`/Pillow
     captures targeting `1s` intervals (`0.90s..2.5s` actual measured inter-capture time including
     screenshot/analysis overhead; never exceeding `2.5s` per step) over a `>= 9.0s` monotonic
     window (with no retry loop hiding blank or `13x13` frames) must each satisfy compact
     `360x320` logical bounds, interior/footer paint density (`fill_ratio >= 0.70`), and measured
     `HEADLESS-2` cross-output cleanliness (`other_output_diff_pixels == 0`), validated fail-closed
     via `verify_vm_acceptance_report` (`_verify_rendered_popup_measurement`) in both the runner
     CLI and the outer NixOS `testScript`.
