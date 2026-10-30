import type { ApprovalSummary } from '../approval/types.ts';
import type { RegistrySnapshot } from '../registry/types.ts';
import type { BrandKey } from '../types.ts';
import type {
  HostCodexToolChoice,
  HostPanelContext,
  HostSessionSummary,
  TaskExecutionStatus,
  TaskStateSnapshot,
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
  busy: boolean;
  summary?: HostSessionSummary | null;
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
  | 'other';

export interface HostErrorState {
  kind: HostErrorKind;
  message: string | null;
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
  if (summary?.phase === 'failed' || summary?.phase === 'ready') {
    return 'warn';
  }
  return 'default';
}

/**
 * Starting is available only from an actual native bridge, a connected exact
 * approval context, and no in-flight action. A matching ready/bootstrapping
 * session must not be started again from this panel.
 */
export function canStartLocalSession(
  approval: ApprovalSummary,
  context: HostPanelContext,
  options: StartLocalSessionOptions
): boolean {
  if (!options.nativeBridgeAvailable || options.busy) {
    return false;
  }
  if (!approval.approval_id.trim() || !approval.subject_digest.trim()) {
    return false;
  }
  if (!approvalContextMatches(approval, context)) {
    return false;
  }
  if (options.summary && hostSummaryMatchesContext(options.summary, approval, context)) {
    if (options.summary.phase === 'ready' || options.summary.phase === 'bootstrapping') {
      return false;
    }
  }
  return true;
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
 * Review wording for a task snapshot. The frontend never turns worker
 * completion or a backend `accepted` flag into acceptance, merging, or plan
 * application.
 */
export function taskReviewNote(task: Pick<TaskStateSnapshot, 'status'>): string {
  if (task.status === 'ready_for_review') {
    return 'Review pending; not accepted, merged, or applied.';
  }
  if (task.status === 'unknown') {
    return 'Unknown (uncertain); not replayed or accepted.';
  }
  return '';
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
