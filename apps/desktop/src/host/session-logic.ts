import type { ApprovalSummary } from '../approval/types.ts';
import type { RegistrySnapshot } from '../registry/types.ts';
import type { BrandKey } from '../types.ts';
import type {
  DependencyInputSummary,
  HostCodexToolChoice,
  HostExecutionGates,
  HostPanelContext,
  HostSessionSummary,
  IntegrationStrategy,
  TaskEvidenceVerificationStatus,
  TaskExecutionStatus,
  TaskIntegrationLifecycleStatus,
  TaskLifecycleProjection,
  TaskReviewLifecycleStatus,
  TaskStateSnapshot,
  VerifiedIntegrationReceiptSummary,
  VerifiedReviewReceiptSummary,
  WorkerSnapshot,
} from './types.ts';

export const HOST_STATUS_POLL_MS = 5_000;
export const MAX_VISIBLE_APPROVED_TASKS = 20;
export const MAX_APPROVED_TASK_IDS = 64;

export interface HostRequestTicket {
  readonly epoch: number;
  readonly key: string;
}

export interface StartLocalSessionOptions {
  nativeBridgeAvailable: boolean;
  previewFixtureMode?: boolean;
  busy: boolean;
  uncertain?: boolean;
  summary?: HostSessionSummary | null;
}

/** One explicit lifecycle action; all three share the panel's single guard. */
export type HostLifecycleAction = 'start' | 'stop' | 'restart';

export interface HostLifecycleEligibilityOptions {
  nativeBridgeAvailable: boolean;
  previewFixtureMode?: boolean;
  busy: boolean;
  uncertain?: boolean;
  summary?: HostSessionSummary | null;
}

/** Exact `hst_` instance identity accepted from the host summary. */
export function isValidHostInstanceId(raw: string | null | undefined): boolean {
  return typeof raw === 'string' && /^hst_[0-9a-f]{32}$/.test(raw);
}

/** Stable identity key for one host instance, used to clear per-instance UI state. */
export function hostInstanceId(
  summary: HostSessionSummary | null | undefined
): string | null {
  const raw = summary?.instance_id?.trim() ?? '';
  return raw ? raw : null;
}

export function hostInstanceChanged(
  previous: HostSessionSummary | null | undefined,
  next: HostSessionSummary | null | undefined
): boolean {
  return hostInstanceId(previous) !== hostInstanceId(next);
}

export function formatHostLifecycleActionLabel(action: HostLifecycleAction): string {
  switch (action) {
    case 'start':
      return 'Starting';
    case 'stop':
      return 'Stopping';
    case 'restart':
      return 'Restarting';
    default:
      return 'Working';
  }
}

/** Quiet operator-facing explanation for the explicit startup opt-in. */
export const HOST_NOTIFICATION_UNVERIFIED_NOTE =
  'Queues a fixed wake-up for this approved chat. It may resume the chat and consume tokens; live routing is not verified.';

/** In-memory only notification choice for one approval/context. */
export interface HostNotificationUiState {
  optIn: boolean;
  toolId: string;
}

export interface HostNotificationAvailabilityOptions {
  nativeBridgeAvailable: boolean;
  previewFixtureMode: boolean;
  contextMatches: boolean;
  running: boolean;
  busy: boolean;
  toolCount: number;
}

export function emptyHostNotificationState(): HostNotificationUiState {
  return { optIn: false, toolId: '' };
}

/** Codex CLI choices come from the trusted registry; all other adapters are excluded. */
export function codexToolChoicesFromRegistry(
  registry: RegistrySnapshot | null | undefined
): HostCodexToolChoice[] {
  if (!registry) {
    return [];
  }
  const choices: HostCodexToolChoice[] = [];
  const seen = new Set<string>();
  for (const tool of registry.tools) {
    if (tool.adapter !== 'codex_cli') {
      continue;
    }
    const id = tool.id.trim();
    if (!id || seen.has(id)) {
      continue;
    }
    seen.add(id);
    const label = tool.label.trim();
    choices.push({ id, label: label || id, enabled: tool.enabled });
  }
  return choices;
}

/** Only enabled, de-duplicated Codex CLI tools are selectable. */
export function enabledHostCodexTools(
  context: HostPanelContext | null | undefined
): HostCodexToolChoice[] {
  const tools: HostCodexToolChoice[] = [];
  const seen = new Set<string>();
  for (const tool of context?.codexTools ?? []) {
    const id = tool.id.trim();
    if (!id || !tool.enabled || seen.has(id)) {
      continue;
    }
    seen.add(id);
    const label = tool.label.trim();
    tools.push({ id, label: label || id, enabled: true });
  }
  return tools;
}

function defaultHostNotificationToolId(tools: readonly HostCodexToolChoice[]): string {
  return tools.length === 1 ? tools[0]!.id : '';
}

/**
 * Reconciles the explicit checkbox with the current context. A context change
 * always clears the opt-in. A removed or disabled selected tool also clears it.
 * A single available tool may be preselected, but it is never auto-enabled.
 */
export function resolveHostNotificationState(
  current: HostNotificationUiState,
  context: HostPanelContext | null | undefined,
  contextChanged: boolean
): HostNotificationUiState {
  const tools = enabledHostCodexTools(context);
  if (contextChanged) {
    return { optIn: false, toolId: defaultHostNotificationToolId(tools) };
  }
  const selected = tools.some((tool) => tool.id === current.toolId)
    ? current.toolId
    : defaultHostNotificationToolId(tools);
  if (!current.optIn) {
    return { optIn: false, toolId: selected };
  }
  const eligible = tools.find((tool) => tool.id === selected);
  if (!eligible) {
    return emptyHostNotificationState();
  }
  return { optIn: true, toolId: eligible.id };
}

export function canConfigureHostNotifications(
  options: HostNotificationAvailabilityOptions
): boolean {
  return (
    options.nativeBridgeAvailable &&
    !options.previewFixtureMode &&
    options.contextMatches &&
    !options.running &&
    !options.busy &&
    options.toolCount > 0
  );
}

/** Returns only an eligible tool ID, or null for off. Never returns a path. */
export function notificationToolIdForStart(
  state: HostNotificationUiState,
  context: HostPanelContext | null | undefined,
  running: boolean
): string | null {
  if (running || !state.optIn) {
    return null;
  }
  const selected = enabledHostCodexTools(context).find(
    (tool) => tool.id === state.toolId
  );
  return selected?.id ?? null;
}

function safeNotificationDigestLabel(raw: string | null | undefined): string {
  const digest = raw?.trim() ?? '';
  if (!/^[0-9a-f]{12,64}$/i.test(digest)) {
    return 'sender digest unavailable';
  }
  return `sender ${digest.slice(0, 12)}…`;
}

/**
 * Displays the actual startup policy from the host summary. A missing field
 * means an older summary, which was always notification-off. This never
 * claims that a wake-up was delivered.
 */
export function hostNotificationPolicyText(
  summary: HostSessionSummary | null | undefined
): string {
  if (!summary) {
    return 'Notifications: no startup policy reported.';
  }
  const notifications = summary.notifications ?? {
    enabled: false,
    executable_digest: null,
    timeout_ms: null,
  };
  if (!notifications.enabled) {
    return 'Notifications: off at startup.';
  }
  const timeout =
    notifications.timeout_ms !== null && Number.isSafeInteger(notifications.timeout_ms)
      ? ` · timeout ${notifications.timeout_ms} ms`
      : '';
  return `Notifications: on at startup · ${safeNotificationDigestLabel(
    notifications.executable_digest
  )}${timeout} · delivery not verified.`;
}

export interface HostWorkerTelemetry {
  requestedModel: string | null;
  requestedEffort: string | null;
  observedModel: string | null;
  observedEffort: string | null;
}

export interface HostTelemetryLabels {
  requested: string;
  observed: string;
}

export type HostErrorKind =
  | 'not_started'
  | 'not_ready'
  | 'bridge_disconnected'
  | 'invalid_input'
  | 'not_idle'
  | 'instance_mismatch'
  | 'uncertain'
  | 'restart_not_allowed'
  | 'control_unavailable'
  | 'other';

export interface HostErrorState {
  kind: HostErrorKind;
  message: string | null;
}

/** Errors after which start, stop and restart stay disabled until refresh. */
export function isUncertainHostErrorKind(kind: HostErrorKind | null): boolean {
  return kind === 'uncertain' || kind === 'instance_mismatch';
}

function nonEmptyText(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? '';
  return trimmed ? trimmed : null;
}

/**
 * Epoch key for approved-host reads. It deliberately includes the frozen
 * approval identity, the exact control binding/thread context, the connection
 * state, and the registry revision/tool choices so stale responses and stale
 * notification opt-ins cannot survive a context change.
 */
export function buildHostRequestKey(
  approval: ApprovalSummary,
  context: HostPanelContext
): string {
  return JSON.stringify([
    approval.approval_id,
    approval.subject_digest,
    context.bindingId ?? '',
    context.threadUuid ?? '',
    context.connected,
    context.registryRevision ?? null,
    (context.codexTools ?? []).map((tool) => [tool.id, tool.label, tool.enabled]),
  ]);
}

/**
 * Lifecycle-action ticket key. Action and expected instance are part of the
 * key so a late stop/restart response cannot mutate a newly selected chat or a
 * new host instance.
 */
export function buildHostLifecycleRequestKey(
  approval: ApprovalSummary,
  context: HostPanelContext,
  action: HostLifecycleAction,
  expectedInstanceId: string | null | undefined
): string {
  return JSON.stringify([
    buildHostRequestKey(approval, context),
    action,
    expectedInstanceId ?? '',
  ]);
}

export function isCurrentHostRequest(
  ticket: HostRequestTicket,
  epoch: number,
  key: string
): boolean {
  return ticket.epoch === epoch && ticket.key === key;
}

/** The panel context must match the approval's authoritative control binding and thread. */
export function approvalContextMatches(
  approval: ApprovalSummary,
  context: HostPanelContext
): boolean {
  return (
    context.connected &&
    Boolean(context.bindingId) &&
    Boolean(context.threadUuid) &&
    context.bindingId === approval.binding_id &&
    context.threadUuid === approval.thread_uuid
  );
}

/**
 * A backend summary is only usable for the current approval when every frozen
 * identity field matches. The runtime `session_binding_id` is intentionally
 * not part of this comparison; it is host-owned and distinct from the control
 * binding in `context`.
 */
export function hostSummaryMatchesApproval(
  summary: HostSessionSummary | null | undefined,
  approval: ApprovalSummary
): boolean {
  return (
    summary != null &&
    summary.approval_id === approval.approval_id &&
    summary.subject_digest === approval.subject_digest &&
    summary.binding_id === approval.binding_id &&
    summary.thread_uuid === approval.thread_uuid &&
    summary.plan_id === approval.plan_id &&
    summary.plan_revision === approval.plan_revision
  );
}

export function hostSummaryMatchesContext(
  summary: HostSessionSummary | null | undefined,
  approval: ApprovalSummary,
  context: HostPanelContext
): boolean {
  return (
    hostSummaryMatchesApproval(summary, approval) &&
    approvalContextMatches(approval, context)
  );
}

export function hostTaskMatchesContext(
  task: TaskStateSnapshot | null | undefined,
  approval: ApprovalSummary,
  context: HostPanelContext
): boolean {
  return (
    task != null &&
    approvalContextMatches(approval, context) &&
    task.approval_id === approval.approval_id &&
    task.subject_digest === approval.subject_digest &&
    task.binding_id === approval.binding_id &&
    task.thread_uuid === approval.thread_uuid
  );
}

/**
 * Ready is derived only from the backend phase plus the authenticated
 * supervisor gate. No PID, manifest, session binding, or absence of an error
 * can establish readiness.
 */
export function isHostSummaryReady(
  summary: HostSessionSummary | null | undefined
): boolean {
  return (
    summary != null &&
    summary.phase === 'ready' &&
    summary.gates?.supervisor_authenticated_ready === true
  );
}

export function formatHostStatusLabel(
  summary: HostSessionSummary | null | undefined
): string {
  if (!summary) {
    return 'Not started';
  }
  if (isHostSummaryReady(summary)) {
    return 'Ready';
  }
  switch (summary.phase) {
    case 'bootstrapping':
      return 'Bootstrapping';
    case 'stopping':
      return 'Stopping';
    case 'failed':
      return 'Failed';
    case 'stopped':
      return 'Stopped';
    case 'ready':
      return 'Not ready (gate unverified)';
    default:
      return 'Unknown';
  }
}

export function formatHostStatusTone(
  summary: HostSessionSummary | null | undefined
): 'default' | 'active' | 'warn' {
  if (isHostSummaryReady(summary)) {
    return 'active';
  }
  if (summary?.phase === 'stopping') {
    return 'active';
  }
  if (summary?.phase === 'failed' || summary?.phase === 'ready') {
    return 'warn';
  }
  return 'default';
}

/**
 * Starting is available only from an actual native bridge, a connected exact
 * approval context, and no in-flight action. A matching ready/bootstrapping/
 * stopping/failed/stopped session must not be started again from this panel; a
 * stopped session requires the explicit Restart action.
 */
export function canStartLocalSession(
  approval: ApprovalSummary,
  context: HostPanelContext,
  options: StartLocalSessionOptions
): boolean {
  if (
    !options.nativeBridgeAvailable ||
    options.previewFixtureMode === true ||
    options.uncertain === true ||
    options.busy
  ) {
    return false;
  }
  if (!approval.approval_id.trim() || !approval.subject_digest.trim()) {
    return false;
  }
  if (!approvalContextMatches(approval, context)) {
    return false;
  }
  if (options.summary && hostSummaryMatchesContext(options.summary, approval, context)) {
    if (
      options.summary.phase === 'ready' ||
      options.summary.phase === 'bootstrapping' ||
      options.summary.phase === 'stopping' ||
      options.summary.phase === 'failed' ||
      options.summary.phase === 'stopped'
    ) {
      return false;
    }
  }
  return true;
}

function lifecycleControlsEligible(
  approval: ApprovalSummary,
  context: HostPanelContext,
  options: HostLifecycleEligibilityOptions
): boolean {
  return (
    options.nativeBridgeAvailable &&
    options.previewFixtureMode !== true &&
    options.uncertain !== true &&
    !options.busy &&
    approvalContextMatches(approval, context) &&
    Boolean(approval.approval_id.trim()) &&
    Boolean(approval.subject_digest.trim()) &&
    options.summary != null &&
    hostSummaryMatchesContext(options.summary, approval, context)
  );
}

/** Stop is explicit and only offered for an authenticated Ready instance. */
export function canStopLocalSession(
  approval: ApprovalSummary,
  context: HostPanelContext,
  options: HostLifecycleEligibilityOptions
): boolean {
  if (!lifecycleControlsEligible(approval, context, options)) {
    return false;
  }
  const summary = options.summary ?? null;
  return isHostSummaryReady(summary) && isValidHostInstanceId(hostInstanceId(summary));
}

/**
 * Restart is only offered from a clean Stopped manifest with a valid prior
 * instance. Ready, Failed, Stopping and unknown phases never qualify.
 */
export function canRestartLocalSession(
  approval: ApprovalSummary,
  context: HostPanelContext,
  options: HostLifecycleEligibilityOptions
): boolean {
  if (!lifecycleControlsEligible(approval, context, options)) {
    return false;
  }
  const summary = options.summary ?? null;
  return (
    summary?.phase === 'stopped' &&
    isValidHostInstanceId(hostInstanceId(summary))
  );
}

/** Bounded operator hint for the one available lifecycle action. */
export function hostLifecycleHint(
  summary: HostSessionSummary | null | undefined,
  uncertain: boolean
): string {
  if (uncertain) {
    return 'The last lifecycle outcome is uncertain. Refresh status before starting, stopping, or restarting.';
  }
  if (!summary) {
    return '';
  }
  switch (summary.phase) {
    case 'ready':
      return 'Stop idle session is explicit. The server may refuse it while any task or wake-up is active; cancel existing approved tasks first.';
    case 'stopping':
      return 'Stopping is in progress. Start, stop and restart are disabled until the host reports Stopped or Failed.';
    case 'stopped':
      return 'Restart creates a new host instance. Existing task receipts are not replayed and notification opt-in stays off unless selected again.';
    default:
      return '';
  }
}

export function normalizeApprovedTaskIds(taskIds: readonly string[]): string[] {
  const normalized: string[] = [];
  for (const raw of taskIds) {
    const taskId = raw.trim();
    if (!taskId || normalized.includes(taskId)) {
      continue;
    }
    normalized.push(taskId);
    if (normalized.length >= MAX_APPROVED_TASK_IDS) {
      break;
    }
  }
  return normalized;
}

export function nextVisibleApprovedTaskCount(current: number, total: number): number {
  if (total <= 0) {
    return 0;
  }
  const safeCurrent = Math.min(Math.max(0, Math.trunc(current)), total);
  return Math.min(safeCurrent + MAX_VISIBLE_APPROVED_TASKS, total);
}

const TASK_STATUS_LABELS: Readonly<Record<TaskExecutionStatus, string>> = {
  not_started: 'Not started',
  dependency_inputs_unverified: 'Blocked: dependencies unverified',
  dependency_inputs_ready: 'Ready: dependencies verified',
  native_route_required: 'Native route required',
  queued: 'Queued',
  claimed: 'Claimed',
  running: 'Running',
  cancel_requested: 'Cancel requested',
  ready_for_review: 'Ready for review',
  failed: 'Failed',
  cancelled: 'Cancelled',
  timed_out: 'Timed out',
  unknown: 'Unknown (uncertain)',
};

export function formatTaskStatusLabel(status: TaskExecutionStatus): string {
  return TASK_STATUS_LABELS[status] ?? 'Unknown (uncertain)';
}

export function formatTaskStatusTone(
  status: TaskExecutionStatus
): 'default' | 'active' | 'warn' {
  switch (status) {
    case 'queued':
    case 'claimed':
    case 'running':
    case 'cancel_requested':
      return 'active';
    case 'dependency_inputs_unverified':
    case 'native_route_required':
    case 'failed':
    case 'timed_out':
    case 'unknown':
      return 'warn';
    case 'dependency_inputs_ready':
      return 'default';
    default:
      return 'default';
  }
}

export function isTerminalTaskStatus(status: TaskExecutionStatus): boolean {
  return (
    status === 'ready_for_review' ||
    status === 'failed' ||
    status === 'cancelled' ||
    status === 'timed_out' ||
    status === 'unknown'
  );
}

/**
 * Tasks in `ready_for_review` have finished worker execution (`isTerminalTaskStatus`
 * remains true for execution controls), but their review, integration, and
 * evidence verification lifecycle can still advance or be invalidated on disk
 * during active polling.
 */
export function shouldPollTaskSnapshot(
  task: Pick<TaskStateSnapshot, 'status' | 'lifecycle'>
): boolean {
  return (
    !isTerminalTaskStatus(task.status) ||
    task.status === 'ready_for_review' ||
    Boolean(task.lifecycle && task.lifecycle.review_status !== 'not_ready')
  );
}

export type TaskReadMode = 'missing' | 'active' | 'all';

export function selectTaskIdsForRead(
  approvedTaskIds: readonly string[],
  visibleTaskCount: number,
  mode: TaskReadMode,
  snapshots: ReadonlyMap<
    string,
    Pick<TaskStateSnapshot, 'status' | 'lifecycle'>
  >,
  pendingTaskIds?: ReadonlySet<string>
): string[] {
  const visibleIds = normalizeApprovedTaskIds(approvedTaskIds).slice(
    0,
    Math.max(0, visibleTaskCount)
  );
  const selected: string[] = [];
  for (const taskId of visibleIds) {
    if (pendingTaskIds?.has(taskId)) {
      continue;
    }
    const snapshot = snapshots.get(taskId);
    const shouldRead =
      mode === 'all' ||
      !snapshot ||
      (mode === 'active' && shouldPollTaskSnapshot(snapshot));
    if (shouldRead) {
      selected.push(taskId);
    }
  }
  return selected;
}

export type TaskLifecycleBadgeTone = 'default' | 'active' | 'warn';

export interface TaskLifecycleStageBadge {
  label: string;
  tone: TaskLifecycleBadgeTone;
}

export interface TaskLifecycleDisplay {
  visible: boolean;
  execution: TaskLifecycleStageBadge;
  review: TaskLifecycleStageBadge;
  integration: TaskLifecycleStageBadge;
  evidence: TaskLifecycleStageBadge;
  receiptSummary: string | null;
  detailNote: string;
  detailTone: TaskLifecycleBadgeTone;
}

export function formatTaskReviewLifecycleStatus(
  status: TaskReviewLifecycleStatus
): TaskLifecycleStageBadge {
  switch (status) {
    case 'not_ready':
      return { label: 'Not ready', tone: 'default' };
    case 'pending_review':
      return { label: 'Review pending', tone: 'default' };
    case 'accepted':
      return { label: 'Review accepted', tone: 'active' };
    case 'historical_accepted':
      return { label: 'Accepted (history)', tone: 'default' };
    case 'invalidated':
      return { label: 'Review invalidated', tone: 'warn' };
    default:
      return { label: 'Review pending', tone: 'default' };
  }
}

export function formatTaskIntegrationLifecycleStatus(
  status: TaskIntegrationLifecycleStatus
): TaskLifecycleStageBadge {
  switch (status) {
    case 'not_integrated':
      return { label: 'Not integrated', tone: 'default' };
    case 'ready_to_integrate':
      return { label: 'Ready to integrate', tone: 'default' };
    case 'integrated':
      return { label: 'Integrated', tone: 'active' };
    case 'historical_integrated':
      return { label: 'Integrated (history)', tone: 'default' };
    case 'invalidated':
      return { label: 'Integration invalidated', tone: 'warn' };
    default:
      return { label: 'Not integrated', tone: 'default' };
  }
}

export function formatTaskEvidenceVerificationStatus(
  status: TaskEvidenceVerificationStatus
): TaskLifecycleStageBadge {
  switch (status) {
    case 'not_applicable':
      return { label: 'Evidence N/A', tone: 'default' };
    case 'pending_review':
      return { label: 'Awaiting review receipt', tone: 'default' };
    case 'verified_live':
      return { label: 'Verified live', tone: 'active' };
    case 'recorded_historical_only':
      return {
        label: 'Recorded history (checkout cleaned up)',
        tone: 'default',
      };
    case 'invalidated':
      return { label: 'Evidence invalidated', tone: 'warn' };
    default:
      return { label: 'Awaiting review receipt', tone: 'default' };
  }
}

export function formatIntegrationStrategyLabel(
  strategy: IntegrationStrategy
): string {
  switch (strategy) {
    case 'fast_forward_only':
      return 'fast-forward';
    case 'merge_no_ff':
      return 'merge --no-ff';
    default:
      return 'branch integration';
  }
}

export function resolveTaskLifecycleProjection(
  task: Pick<TaskStateSnapshot, 'status' | 'lifecycle'>
): TaskLifecycleProjection {
  if (task.lifecycle) {
    return {
      review_status: task.lifecycle.review_status,
      integration_status: task.lifecycle.integration_status,
      evidence_status: task.lifecycle.evidence_status,
      task_checkout_present: Boolean(task.lifecycle.task_checkout_present),
      integration_checkout_present: Boolean(
        task.lifecycle.integration_checkout_present
      ),
      review_receipt: task.lifecycle.review_receipt ?? null,
      integration_receipt: task.lifecycle.integration_receipt ?? null,
      verification_detail: task.lifecycle.verification_detail ?? null,
    };
  }
  if (task.status === 'ready_for_review') {
    return {
      review_status: 'pending_review',
      integration_status: 'not_integrated',
      evidence_status: 'pending_review',
      task_checkout_present: false,
      integration_checkout_present: false,
      review_receipt: null,
      integration_receipt: null,
      verification_detail: null,
    };
  }
  return {
    review_status: 'not_ready',
    integration_status: 'not_integrated',
    evidence_status: 'not_applicable',
    task_checkout_present: false,
    integration_checkout_present: false,
    review_receipt: null,
    integration_receipt: null,
    verification_detail: null,
  };
}

export function formatTaskLifecycleReceipts(
  lifecycle: TaskLifecycleProjection | null | undefined
): string | null {
  if (
    !lifecycle ||
    lifecycle.evidence_status === 'invalidated' ||
    lifecycle.review_status === 'invalidated' ||
    lifecycle.integration_status === 'invalidated'
  ) {
    return null;
  }
  const parts: string[] = [];
  const review: VerifiedReviewReceiptSummary | null =
    lifecycle.review_receipt ?? null;
  if (
    review &&
    (lifecycle.review_status === 'accepted' ||
      lifecycle.review_status === 'historical_accepted')
  ) {
    const receiptId = nonEmptyText(review.receipt_id) ?? 'receipt';
    const commit = abbreviatedDependencySha(review.task_head_commit);
    const reviewer = nonEmptyText(review.reviewer_id);
    const meta = [
      commit ? `commit ${commit}` : null,
      reviewer ? `reviewer ${reviewer}` : null,
    ]
      .filter((item): item is string => item !== null)
      .join(', ');
    parts.push(
      meta ? `Review ${receiptId} (${meta})` : `Review ${receiptId}`
    );
  }
  const integration: VerifiedIntegrationReceiptSummary | null =
    lifecycle.integration_receipt ?? null;
  if (
    integration &&
    (lifecycle.integration_status === 'integrated' ||
      lifecycle.integration_status === 'historical_integrated')
  ) {
    const receiptId = nonEmptyText(integration.receipt_id) ?? 'receipt';
    const target = abbreviatedDependencySha(
      integration.integrated_target_commit
    );
    const operator = nonEmptyText(integration.operator_id);
    const strategy = formatIntegrationStrategyLabel(integration.strategy);
    const meta = [
      target ? `target ${target}` : null,
      strategy,
      operator ? `operator ${operator}` : null,
    ]
      .filter((item): item is string => item !== null)
      .join(', ');
    parts.push(
      meta
        ? `Integration ${receiptId} (${meta})`
        : `Integration ${receiptId}`
    );
  }
  return parts.length > 0 ? parts.join(' · ') : null;
}

/**
 * Review and lifecycle wording for a task snapshot. The frontend never turns
 * worker completion, a worker `final_summary`, or a backend `accepted` flag
 * into acceptance, merging, or plan application; only host-verified lifecycle
 * projections can report reviewed, integrated, historical, or invalidated state.
 */
export function taskReviewNote(
  task: Pick<TaskStateSnapshot, 'status' | 'lifecycle'>
): string {
  if (task.status === 'unknown') {
    return 'Unknown (uncertain); not replayed or accepted.';
  }
  const lifecycle = task.lifecycle ?? null;
  if (lifecycle) {
    const detail = nonEmptyText(lifecycle.verification_detail);
    if (
      lifecycle.evidence_status === 'invalidated' ||
      lifecycle.review_status === 'invalidated' ||
      lifecycle.integration_status === 'invalidated'
    ) {
      return detail
        ? `Lifecycle evidence invalidated; not treated as reviewed or integrated. ${detail}`
        : 'Lifecycle evidence invalidated; not treated as reviewed or integrated.';
    }
    if (
      lifecycle.evidence_status === 'recorded_historical_only' ||
      lifecycle.review_status === 'historical_accepted' ||
      lifecycle.integration_status === 'historical_integrated'
    ) {
      return detail
        ? `Recorded history only (task checkout cleaned up; live checkout diff re-verification unavailable). ${detail}`
        : 'Recorded history only (task checkout cleaned up; live checkout diff re-verification unavailable).';
    }
    if (
      lifecycle.review_status === 'accepted' &&
      lifecycle.integration_status === 'integrated' &&
      lifecycle.evidence_status === 'verified_live'
    ) {
      return detail
        ? `Reviewed and integrated on the run branch (verified live). ${detail}`
        : 'Reviewed and integrated on the run branch (verified live).';
    }
    if (
      lifecycle.review_status === 'accepted' &&
      lifecycle.evidence_status === 'verified_live'
    ) {
      return detail
        ? `Review receipt verified live; ready to integrate onto the run branch (not yet integrated). ${detail}`
        : 'Review receipt verified live; ready to integrate onto the run branch (not yet integrated).';
    }
    if (lifecycle.review_status === 'pending_review') {
      return 'Review pending; not accepted, merged, or applied.';
    }
  }
  if (task.status === 'ready_for_review') {
    return 'Review pending; not accepted, merged, or applied.';
  }
  return '';
}

export function formatTaskLifecycleDisplay(
  task: Pick<TaskStateSnapshot, 'status' | 'lifecycle'>
): TaskLifecycleDisplay {
  const lifecycle = resolveTaskLifecycleProjection(task);
  const visible =
    task.status === 'ready_for_review' ||
    Boolean(
      task.lifecycle &&
        (task.lifecycle.review_status !== 'not_ready' ||
          task.lifecycle.integration_status !== 'not_integrated' ||
          task.lifecycle.evidence_status !== 'not_applicable')
    );
  const execution: TaskLifecycleStageBadge = {
    label: formatTaskStatusLabel(task.status),
    tone: formatTaskStatusTone(task.status),
  };
  const review = formatTaskReviewLifecycleStatus(lifecycle.review_status);
  const integration = formatTaskIntegrationLifecycleStatus(
    lifecycle.integration_status
  );
  const evidence = formatTaskEvidenceVerificationStatus(
    lifecycle.evidence_status
  );
  const receiptSummary = formatTaskLifecycleReceipts(lifecycle);
  const detailNote = taskReviewNote(task);
  const detailTone: TaskLifecycleBadgeTone =
    review.tone === 'warn' ||
    integration.tone === 'warn' ||
    evidence.tone === 'warn' ||
    task.status === 'unknown'
      ? 'warn'
      : review.tone === 'active' ||
          integration.tone === 'active' ||
          evidence.tone === 'active'
        ? 'active'
        : 'default';

  return {
    visible,
    execution,
    review,
    integration,
    evidence,
    receiptSummary,
    detailNote,
    detailTone,
  };
}

export interface TaskLifecycleElementSink {
  textContent: string | null;
  hidden: HTMLElement['hidden'];
  setAttribute(name: string, value: string): void;
}

export interface TaskLifecycleDomTargets {
  lifecycleGroup: Pick<TaskLifecycleElementSink, 'hidden'>;
  executionPill: Pick<TaskLifecycleElementSink, 'textContent' | 'setAttribute'>;
  reviewPill: Pick<TaskLifecycleElementSink, 'textContent' | 'setAttribute'>;
  integrationPill: Pick<TaskLifecycleElementSink, 'textContent' | 'setAttribute'>;
  evidencePill: Pick<TaskLifecycleElementSink, 'textContent' | 'setAttribute'>;
  lifecycleReceipts: Pick<TaskLifecycleElementSink, 'textContent' | 'hidden'>;
  lifecycleNote: TaskLifecycleElementSink;
  reviewDetail: TaskLifecycleElementSink;
}

function writeSinkText(
  target: { textContent: string | null },
  text: string
): void {
  if (target.textContent !== text) {
    target.textContent = text;
  }
}

/**
 * Synchronizes the lifecycle stage pills, receipt summary, and review/verification
 * notes in place through `textContent` and `data-status` attributes without
 * replacing DOM nodes or parsing HTML.
 */
export function syncTaskLifecycleTargets(
  targets: TaskLifecycleDomTargets,
  task: Pick<TaskStateSnapshot, 'status' | 'lifecycle'> | null,
  receiptDisplayLimit: number = 600
): TaskLifecycleDisplay | null {
  if (!task) {
    targets.lifecycleGroup.hidden = true;
    writeSinkText(targets.executionPill, 'Waiting');
    targets.executionPill.setAttribute('data-status', 'default');
    writeSinkText(targets.reviewPill, 'Not ready');
    targets.reviewPill.setAttribute('data-status', 'default');
    writeSinkText(targets.integrationPill, 'Not integrated');
    targets.integrationPill.setAttribute('data-status', 'default');
    writeSinkText(targets.evidencePill, 'Evidence N/A');
    targets.evidencePill.setAttribute('data-status', 'default');
    writeSinkText(targets.lifecycleReceipts, '');
    targets.lifecycleReceipts.hidden = true;
    writeSinkText(targets.lifecycleNote, '');
    targets.lifecycleNote.setAttribute('data-status', 'default');
    targets.lifecycleNote.hidden = true;
    writeSinkText(targets.reviewDetail, '');
    targets.reviewDetail.setAttribute('data-status', 'default');
    targets.reviewDetail.hidden = true;
    return null;
  }

  const display = formatTaskLifecycleDisplay(task);
  targets.lifecycleGroup.hidden = !display.visible;
  writeSinkText(targets.executionPill, display.execution.label);
  targets.executionPill.setAttribute('data-status', display.execution.tone);
  writeSinkText(targets.reviewPill, display.review.label);
  targets.reviewPill.setAttribute('data-status', display.review.tone);
  writeSinkText(targets.integrationPill, display.integration.label);
  targets.integrationPill.setAttribute('data-status', display.integration.tone);
  writeSinkText(targets.evidencePill, display.evidence.label);
  targets.evidencePill.setAttribute('data-status', display.evidence.tone);

  const receiptText = display.receiptSummary
    ? compactDisplayText(display.receiptSummary, receiptDisplayLimit)
    : '';
  writeSinkText(targets.lifecycleReceipts, receiptText);
  targets.lifecycleReceipts.hidden = receiptText.length === 0;

  writeSinkText(targets.lifecycleNote, display.detailNote);
  targets.lifecycleNote.setAttribute('data-status', display.detailTone);
  targets.lifecycleNote.hidden =
    !display.visible || display.detailNote.length === 0;

  writeSinkText(targets.reviewDetail, display.detailNote);
  targets.reviewDetail.setAttribute('data-status', display.detailTone);
  targets.reviewDetail.hidden = display.detailNote.length === 0;

  return display;
}

/** Operator guidance for dependent inputs that the host has not verified. */
export const TASK_DEPENDENCY_UNVERIFIED_NOTE =
  'Waiting for reviewed prerequisite code. Prepare dependency inputs before starting this task.';

export interface TaskDependencyDetail {
  text: string;
  tone: 'default' | 'warn';
}

function abbreviatedDependencySha(raw: string): string | null {
  const value = typeof raw === 'string' ? raw.trim() : '';
  if (!value) {
    return null;
  }
  return value.length <= 12 ? value : `${value.slice(0, 12)}…`;
}

/**
 * Read-only dependency text. It never turns a status or parent list into a
 * prepared summary: the host must report `dependency_input` explicitly.
 */
export function formatTaskDependencyDetail(
  task: Pick<TaskStateSnapshot, 'status' | 'dependency_input'>
): TaskDependencyDetail | null {
  const input: DependencyInputSummary | null = task.dependency_input ?? null;
  const parts: string[] = [];
  if (input) {
    const base = abbreviatedDependencySha(input.base_commit);
    const predecessors = Array.isArray(input.predecessor_task_ids)
      ? input.predecessor_task_ids
          .map((id) => (typeof id === 'string' ? id.trim() : ''))
          .filter((id) => id.length > 0)
      : [];
    const predecessorText =
      predecessors.length > 0
        ? `Prerequisites: ${predecessors.join(', ')}`
        : 'Prerequisites not reported';
    parts.push(
      `${
        base ? `Base ${base}` : 'Base not reported'
      } · ${predecessorText}.`
    );
  }
  if (task.status === 'dependency_inputs_unverified') {
    parts.push(TASK_DEPENDENCY_UNVERIFIED_NOTE);
  } else if (task.status === 'dependency_inputs_ready' && !input) {
    parts.push(
      'Prepared input details are unavailable.'
    );
  } else if (task.status === 'dependency_inputs_ready') {
    parts.push('Ready to start.');
  }
  if (parts.length === 0) {
    return null;
  }
  return {
    text: parts.join(' '),
    tone: task.status === 'dependency_inputs_unverified' ? 'warn' : 'default',
  };
}

/** Writes dependency details through `textContent`, never HTML parsing. */
export function writeTaskDependencyText(
  target: { textContent: string | null },
  task: Pick<TaskStateSnapshot, 'status' | 'dependency_input'>
): TaskDependencyDetail | null {
  const detail = formatTaskDependencyDetail(task);
  const text = detail?.text ?? '';
  if (target.textContent !== text) {
    target.textContent = text;
  }
  return detail;
}

/** Honest gate wording for legacy hosts that never report input support. */
export function formatDependencyInputsGate(
  gates: Pick<HostExecutionGates, 'dependency_inputs_required'> | null | undefined
): string {
  if (gates?.dependency_inputs_required === true) {
    return 'Reviewed prerequisite code is required before starting dependent tasks.';
  }
  if (gates?.dependency_inputs_required === false) {
    return 'Not supported by this host.';
  }
  return 'Legacy host: dependency preparation is unavailable.';
}

/** Dependent tasks are not described as unconditionally blocked. */
export function formatDependentTasksGate(
  gates: Pick<
    HostExecutionGates,
    'dependent_tasks_blocked' | 'dependency_inputs_required'
  >
): string {
  if (gates.dependency_inputs_required === true) {
    return gates.dependent_tasks_blocked
      ? 'Waiting for reviewed prerequisite code.'
      : 'Available after prerequisite code is reviewed and prepared.';
  }
  if (gates.dependent_tasks_blocked) {
    return gates.dependency_inputs_required === false
      ? 'Blocked by host policy; no verified-input requirement reported.'
      : 'Blocked; verified-input support not reported (legacy).';
  }
  return gates.dependency_inputs_required === false
    ? 'No global block reported.'
    : 'No global block reported; verified-input support not reported (legacy).';
}

export function workerTelemetry(worker: WorkerSnapshot | null | undefined): HostWorkerTelemetry {
  if (!worker) {
    return {
      requestedModel: null,
      requestedEffort: null,
      observedModel: null,
      observedEffort: null,
    };
  }
  return {
    requestedModel: nonEmptyText(worker.requested_model),
    requestedEffort: nonEmptyText(worker.requested_effort),
    observedModel: nonEmptyText(worker.observed_model),
    observedEffort: nonEmptyText(worker.observed_effort),
  };
}

export function formatTaskTelemetry(
  task: Pick<TaskStateSnapshot, 'worker'>
): HostTelemetryLabels {
  const telemetry = workerTelemetry(task.worker);
  return {
    requested: formatTelemetryPart(
      telemetry.requestedModel,
      telemetry.requestedEffort
    ),
    observed: formatTelemetryPart(telemetry.observedModel, telemetry.observedEffort),
  };
}

function formatTelemetryPart(
  model: string | null,
  effort: string | null
): string {
  if (model === null && effort === null) {
    return 'Not reported';
  }
  return `${model ?? 'model not reported'} · effort ${effort ?? 'not reported'}`;
}

/** Shows only actual worker progress text or the actual last event kind. */
export function formatTaskProgress(
  task: Pick<TaskStateSnapshot, 'worker'>
): string {
  if (!task.worker) {
    return 'No worker snapshot reported.';
  }
  const summary = nonEmptyText(task.worker.progress.last_summary);
  if (summary) {
    return summary;
  }
  const eventKind = nonEmptyText(task.worker.progress.last_event_kind);
  if (eventKind) {
    return `Last event: ${eventKind}`;
  }
  return 'No live progress reported.';
}

export function formatTaskInstruction(
  task: Pick<TaskStateSnapshot, 'worker'>
): string {
  return nonEmptyText(task.worker?.instruction) ?? 'Instruction not reported yet.';
}

/** Uses the real adapter/provider/model fields; absent values remain absent. */
export function resolveHostTaskBrand(
  task: Pick<TaskStateSnapshot, 'adapter' | 'provider' | 'worker'> | null | undefined
): BrandKey {
  if (!task) {
    return 'generic';
  }
  const provider = (task.worker?.provider ?? task.provider ?? '').toLowerCase();
  const model = `${task.worker?.observed_model ?? ''} ${
    task.worker?.requested_model ?? ''
  }`.toLowerCase();

  if (task.adapter === 'opencode_v2' || provider.includes('opencode')) {
    return model.includes('deepseek') ? 'deepseek' : 'opencode';
  }
  if (task.adapter === 'antigravity' || provider.includes('antigravity')) {
    return model.includes('gemini') || model.includes('argon') ? 'gemini' : 'antigravity';
  }
  if (task.adapter === 'codex_cli') {
    return 'openai';
  }
  if (model.includes('deepseek')) {
    return 'deepseek';
  }
  if (model.includes('gemini') || model.includes('argon')) {
    return 'gemini';
  }
  if (model.includes('gpt') || model.includes('astra') || model.includes('luna')) {
    return 'openai';
  }
  return 'generic';
}

export function formatAdapterLabel(adapter: TaskStateSnapshot['adapter']): string {
  switch (adapter) {
    case 'codex_cli':
      return 'Native Codex';
    case 'opencode_v2':
      return 'OpenCode v2';
    case 'antigravity':
      return 'Antigravity';
    case 'jev_api':
      return 'Jev API';
    case 'openai_decisions_api':
      return 'OpenAI decisions API';
    case 'git':
      return 'Git';
    case 'tmux':
      return 'tmux';
    case 'codegraph':
      return 'CodeGraph';
    default:
      return 'Unknown adapter';
  }
}

export function compactDisplayText(raw: string, maxLength: number): string {
  const collapsed = raw.replace(/\s+/g, ' ').trim();
  if (collapsed.length <= maxLength) {
    return collapsed;
  }
  return `${collapsed.slice(0, Math.max(0, maxLength - 1))}…`;
}

function readErrorCode(err: unknown): string | null {
  if (!err || typeof err !== 'object') {
    return null;
  }
  const code = (err as { code?: unknown }).code;
  return typeof code === 'string' ? code.trim() : null;
}

/**
 * Converts expected backend states into bounded UI notices. A transport or
 * host error never becomes a running/ready state.
 */
export function classifyHostError(err: unknown): HostErrorState {
  const code = readErrorCode(err);
  switch (code) {
    case 'not_started':
      return { kind: 'not_started', message: null };
    case 'host_not_ready':
      return {
        kind: 'not_ready',
        message: 'The approved host is not ready yet.',
      };
    case 'bridge_disconnected':
      return {
        kind: 'bridge_disconnected',
        message: 'The native desktop bridge is disconnected.',
      };
    case 'invalid_input':
      return {
        kind: 'invalid_input',
        message: 'The approved session request was not valid.',
      };
    case 'host_not_idle':
      return {
        kind: 'not_idle',
        message:
          'The host reports active or pending work and refused the idle stop. Use the existing approved task cancellation controls, then retry.',
      };
    case 'host_instance_mismatch':
      return {
        kind: 'instance_mismatch',
        message:
          'The host instance changed or no longer matches this approval. Refresh status before retrying.',
      };
    case 'host_stop_outcome_uncertain':
      return {
        kind: 'uncertain',
        message:
          'Shutdown could not be confirmed. The session is not reported stopped; refresh status before another action.',
      };
    case 'host_restart_not_allowed':
      return {
        kind: 'restart_not_allowed',
        message:
          'Only a cleanly stopped session with the same instance can be restarted explicitly.',
      };
    case 'host_control_unavailable':
      return {
        kind: 'control_unavailable',
        message:
          'This host session does not expose authenticated lifecycle controls.',
      };
    case 'lock_busy':
      return {
        kind: 'other',
        message:
          'The host session is locked by an active host process. Refresh status before retrying.',
      };
    case 'notification_policy_change_requires_restart':
      return {
        kind: 'other',
        message:
          'The running host was started with a different notification policy. Stop and restart it to change the policy; the live host was not changed.',
      };
    default:
      return {
        kind: 'other',
        message: 'Session status is unavailable right now.',
      };
  }
}
