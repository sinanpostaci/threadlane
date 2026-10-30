# Idle session control contract

Baseline486f3ca is already reviewed and built. This package adds explicit operator
stop-idle/restart-stopped controls and an independent native metadata UI fix.
No live chats, product inference, credentials, global config, OS services or PID
signaling are used. UI closure still leaves hosts alive. All prose is English.

## Parent decisions and actual APIs

The separate633-line proposal is advisory pseudocode, NOT an API reference. Real
names are `SupervisorClient`, `threadlane::Error`, `ApiError::new`,
`Core::open/create_binding/execute`, `Uuid::new_v4().simple()` and `state.db`.
There is no RuntimeClient, MaterializedApprovalAuthority, Core::default/apply,
ApiError::conflict, rand dependency or optional EventBatch.batch_id. Read actual
signatures before tests. No dependency versions or persisted Core schema change.

Use existing authenticated loopback HTTP with optional host-control installation
only on the FINAL host supervisor. Legacy `Supervisor::start` and bootstrap keep
control disabled. No stop/restart ScopedRequest/MCP tool; paired tools remain29.
Keep SupervisorConfig fields unchanged: add an entry point instead of requiring
legacy config literal migrations. Use the existing graceful owner shutdown path.

## A: Core/runtime/host exact shared interfaces

Core exports `RuntimeIdleInspection {non_terminal_runs:usize,
non_terminal_workers:usize,in_flight_wakeups:usize,pending_wakeups:usize}` and
`Core::inspect_runtime_idle(&self)->Result<RuntimeIdleInspection,CoreError>`.
Use one consistent read query (aggregate subqueries) across ALL runtime bindings;
no recent-summary truncation. Existing tables/status enums define terminalness.

Runtime exports serde deny_unknown_fields types:
- `HostControlConfig {approval_id:String,subject_digest:String,
  control_binding_id:String,thread_uuid:String,session_binding_id:String,
  instance_id:String}` (validated trusted startup input).
- `HostControlOwnerScope` same owner fields but `expected_instance_id:String`
  instead of instance_id; aliases `HostControlInfoRequest`, `StopIdleHostRequest`.
- `HostControlRuntimeState::{Ready,Stopping}` serialized snake_case.
- `HostControlBusyCounts {non_terminal_runs:usize,non_terminal_workers:usize,
  live_worker_tasks:usize,in_flight_wakeups:usize,pending_enabled_wakeups:usize}`.
- `HostControlStatusResponse {approval_id:String,subject_digest:String,
  control_binding_id:String,thread_uuid:String,session_binding_id:String,
  instance_id:String,state:HostControlRuntimeState,idle:bool,busy:HostControlBusyCounts}`.

`Supervisor::start_with_host_control(config:SupervisorConfig,control:HostControlConfig)
 ->Result<SupervisorHandle,threadlane::Error>` is additive. Existing start stays off.
`SupervisorHandle::wait_for_stop_request(&self)` subscribes to a monotonic latch
and waits; legacy-disabled controls wait forever. MUST borrow &self, not &mutself,
so host CLI can select against wait_for_fatal simultaneously.
`SupervisorClient::host_control_info(&self,&HostControlInfoRequest)` and
`stop_idle_host(&self,&StopIdleHostRequest)` return the typed response or runtime Error.
POST `/v1/host-control/info` and `/v1/host-control/stop` use existing bearer auth,
strict bounded JSON and full owner+instance comparison. Disabled control fails
with `HostControlNotConfigured`; no model-supplied identity establishes authority.
Add stable ErrorCode variants HostControlNotConfigured, HostNotIdle,
HostInstanceMismatch, HostScopeMismatch, HostStopping, with explicit status/error
mapping. Never parse English error text to select behavior.

Idle stop atomically checks under SharedState::active: zero nonterminal runs and
workers, zero live active_workers and zero InFlight wakeups. Pending wakeups block
when a sender is enabled (when off they may legitimately remain Pending). Core
InFlight already covers invocation until result/reap; no duplicate atomic sender
counter unless source evidence proves it necessary. Lock order active -> short
active_workers leaf; never await while holding that std lock.

On success latch stopping BEFORE HTTP acknowledgement, reject all new mutating
requests including scoped commands/create-binding/retry and background claims.
Read-only status/control-info may remain available until owner drains. Do NOT reuse
retired in a way that drops Core/locks early; internal shutdown cleanup must still
acquire Core. Repeated same-instance stop is idempotent. A dispatcher winning the
mutex makes stop busy; stop winning the mutex prevents dispatch. No force mode.

HostPhase adds Stopping (never Ready). New manifests/summaries add serde(default)
`instance_id:Option<String>`; legacy missing ID is readable but lifecycle actions
are unsupported. Each start creates `hst_`+32 lowercase hex using existing Uuid;
bootstrap/final/failure/stopped records preserve this one ID; restart rotates it.
Final host verifies authenticated control info agrees with all frozen owner fields
before writing Ready. Bootstrap cannot send lifecycle/notification effects.
`ApprovedHostHandle::instance_id(&self)->&str` and `wait_for_stop_request(&self)`.
CLI owner selects stop request, signal and fatal, then uses normal shutdown.
Shutdown writes Stopping (gates false), closes/reaps runtime, writes Stopped only
on successful completion/persistence, then releases host.lock. Failure/transport
loss is never converted into Stopped; preserve Failed/uncertain diagnostics.

HostClient exports:
`stop_idle_session(&self,approval_id:&str,expected_instance_id:&str)
 ->Result<HostSessionSummary,HostError>` and
`host_control_status(&self,approval_id:&str)
 ->Result<Option<HostControlStatusResponse>,HostError>` (None for legacy IDs).
Validate original approval/control-binding/thread first; authenticate live owner.
Stop returns success ONLY after matching-instance durable Stopped+released lock,
including duplicate completion. Bounded confirmation5s/25ms after request; current
HTTP client timeout also remains bounded. Changed instance, failed shutdown, a
missing descriptor or mere EOF is not stop proof. Lost response may be reconciled
by the durable proof; otherwise report StopOutcomeUncertain, no automatic resend.

Add HostError variants and exact codes:
- HostNotIdle{approval_id:String,detail:String} -> host_not_idle
- InstanceMismatch{approval_id:String,expected_instance_id:String,
  actual_instance_id:Option<String>} -> host_instance_mismatch
- StopOutcomeUncertain{approval_id:String,detail:String} -> host_stop_outcome_uncertain
- RestartNotAllowed{approval_id:String,phase:Option<HostPhase>,detail:String}
  -> host_restart_not_allowed
Safe UI/native mappings must not echo raw detail/path/token.

`ApprovedHost::restart_stopped(config:ApprovedHostConfig,expected_prior_instance_id:&str)
 ->Result<ApprovedHostHandle,HostError>` checks clean Stopped, lock and expected ID
UNDER the acquired host lock before startup writes. Revalidates current approval,
publication, registry and receipt identity; no replay. Config owns notifications,
default off. CLI serve adds optional `--restart-from-instance-id` selecting this
path. Existing trusted library start/recovery API remains compatible (not a new
untrusted/MCP route); desktop ordinary Start must reject Stopped and direct operator
to explicit Restart. Desktop restart never treats Ready/Failed/unknown as Stopped.

## C: native desktop and session panel

Tauri commands (operator only, NOT MCP):
`desktop_stop_approved_session(approval_id:String,expected_instance_id:String)`;
`desktop_restart_approved_session(approval_id:String,expected_prior_instance_id:String,
 notify_codex_tool_id:Option<String>)` -> HostSessionSummary.
JS accepts only these IDs. No PID, paths, tokens, owner fields, arbitrary args or
timeouts. Verify connected original chat+approval, same per-approval async mutex as
Start; recheck binding and trusted selected notification tool before effects.
Restart CLI adds --restart-from-instance-id; A rechecks under host lock. Allow
existing receipts only for explicit Restart, never replay tasks or restore opt-in.
No automatic stop+start chain. Reap only already-owned child handles; no signals.
Stop must work with an empty/reopened Hub via authenticated control, never PID.

Existing approved_session read observes authenticated lifecycle Stopping and
projects that phase with readiness gates false. Offline or stale Ready manifests
never prove readiness. Legacy summaries without instance IDs keep read-only views
and show lifecycle controls unavailable. Update exhaustive HostPhase matches.

Frontend HostSessionSummary: `instance_id?:string|null`, HostPhase includes stopping.
New stop/restart wrappers mirror exact camelCase args. Three actions share one
in-flight guard and epoch/context checks; clear task cache on instance change,
drop stale responses, don't mutate a newly selected chat. Stop idle session is
explicit and may be refused if hidden active work exists; server is authoritative.
Busy errors direct operator to existing task cancellation. Start is disabled for
Stopped; Restart is explicit only from Stopped+validID. Stopping/uncertain is never
shown as successful stop, restart or a fresh Ready session. Notification choices
reset per instance/context and remain editable only before a NEW explicit start or
restart, unchecked by default. Hidden/DEV/unbound controls stay disabled. Preserve
narrow quiet layout, system theme/accent and focused input nodes.

## Ownership and tests

A ArgonHigh: all threadlane-core, runtime, threadlane-host code/focused tests only.
No native MCP, desktop or root lock edits. C GoMax: Tauri host_control.rs/main.rs,
frontend src/host/** only. B GoMax: native metadata panel/new logic+tests and one
main.ts navigation hook; independent. Parent owns docs, root locks and exact
native-MCP exhaustive error-map seams, review and test orchestration. Never copy
old sibling crates for validation. No new deps without actual need/review.

Tests use actual APIs and fake executables only: off/legacy/bootstrap; idle read
counts and queued/waiting/cancel-requested/live-task/sender busy; latch races vs
dispatch/claim; owner/instance/unknown-field/auth; duplicate and lost stop; owner
never drains -> uncertain; persistence fatal -> Failed; clean Stopped/lockfree;
new instance on restart, stale ID, wrong phase, changed approval/registry/binding,
receipts retained/no replay/default-off; real CLI process exit and Hub reopen;
frontend in-flight/context/instance reset and unchanged29MCPtools. Parent executes
fmt, meaningful focused regressions, Clippy and final combined package gates.
