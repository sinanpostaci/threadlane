/**
 * Pure projection and formatting helpers for the read-only Activity request feed.
 *
 * This module intentionally has no DOM, Tauri, shell, or provider imports so the
 * feed's ordering, scope, evidence, and fallback behavior can be tested with
 * node:test without a DOM package.
 */
import type { BrandKey } from '../types.ts';
import type {
  ObservationActor,
  ObservationCall,
  ObservationDelivery,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationUsage,
} from '../observation/types.ts';

export interface ActivityFeedScope {
  source_id: string;
  job_id: string;
}

export type ObservationLifecycleState = ObservationRequest['state'];
export type DeliveryStage = ObservationDelivery['stage'];
export type RequesterEvidenceLabel =
  | 'Configured'
  | 'Observed'
  | 'Operator supplied'
  | 'Unknown';

export type LifecycleTone =
  | 'neutral'
  | 'active'
  | 'waiting'
  | 'ok'
  | 'review'
  | 'danger'
  | 'cancelled'
  | 'unknown';

export type ActivityDurationKind = 'recorded' | 'derived' | 'elapsed' | 'unknown';

export interface ActivityDurationView {
  kind: ActivityDurationKind;
  text: string;
  note: string;
}

export interface UsageView {
  reported: boolean;
  input: number | null;
  output: number | null;
  cacheRead: number | null;
  total: number | null;
  provenance: string | null;
  summary: string;
  detail: string;
}

export interface ActivityChildView {
  call: ObservationCall;
  title: string;
  stateLabel: string;
  stateTone: LifecycleTone;
  showSpinner: boolean;
  executorSummary: string;
  requestedModel: string;
  observedModel: string;
  requestedEffort: string;
  observedEffort: string;
  modelSummary: string;
  effortSummary: string;
  usage: UsageView;
  duration: ActivityDurationView;
  durationSummary: string;
  durationDetail: string;
  startedAtValue: string;
  finishedAtValue: string;
  summary: string;
}

export interface ActivityCalledExecutorView {
  callCount: number;
  recordedCount: number;
  unknownCount: number;
  toolCount: number;
  providerCount: number;
  modelCount: number;
  effortCount: number;
  summary: string;
  detail: string;
  brand: BrandKey;
}

export interface ActivityDeliveryView {
  record: ObservationDelivery;
  stage: DeliveryStage;
  label: string;
  note: string;
  atMs: number | null;
  atValue: string;
  recipient: string;
  summary: string;
}

export interface ActivityRequesterLineView {
  value: string;
  evidence: RequesterEvidenceLabel | null;
  evidenceKey: string;
  detail: string;
}

export interface RecordedModelView {
  value: string;
  detail: string;
}

export type ActivityTimelineKind = 'request' | 'requester' | 'delivery' | 'child';

export interface ActivityTimelineEventView {
  key: string;
  kind: ActivityTimelineKind;
  atMs: number | null;
  atValue: string;
  label: string;
  stateLabel: string | null;
  tone: LifecycleTone;
  detail: string;
  lines: string[];
  note: string;
  stage: DeliveryStage | null;
}

export interface ActivityCompactEvidence {
  value: string;
  detail: string;
}

export interface ActivityExecutorDisplay {
  role: string;
  model: string | null;
  detail: string;
}

export interface ActivityTimelineDisplay {
  label: string;
  detail: string;
  lines: string[];
}

export interface ActivityVisibleTimelineEvent {
  event: ActivityTimelineEventView;
  display: ActivityTimelineDisplay;
  recordCount: number;
}

export interface ActivityRequestView {
  key: string;
  domKey: string;
  request: ObservationRequest;
  source: ObservationSource | null;
  job: ObservationJob | null;
  sourceLabel: string;
  sourceDetails: string;
  projectLabel: string;
  jobLabel: string;
  jobLinked: boolean;
  jobUnavailable: boolean;
  title: string;
  operation: string;
  state: ObservationLifecycleState;
  stateLabel: string;
  stateTone: LifecycleTone;
  showSpinner: boolean;
  callsRunning: boolean;
  runningChildCount: number;
  meaningfulAtMs: number | null;
  updatedAtSummary: string;
  updatedAtValue: string;
  startedAtValue: string;
  finishedAtValue: string;
  requesterLabel: string;
  requesterEvidence: RequesterEvidenceLabel;
  requesterEvidenceKey: string;
  requesterSummary: string;
  requesterLine: ActivityRequesterLineView;
  actorRole: string;
  actorSource: string;
  actorModel: string;
  actorEffort: string;
  conversationDetails: string;
  executorIdentity: string;
  executorSummary: string;
  executorTool: string;
  executorProvider: string;
  executorRole: string;
  executorRoleLabel: string;
  requestedModel: string;
  observedModel: string;
  recordedModelValue: string;
  recordedModelDetail: string;
  requestedEffort: string;
  observedEffort: string;
  modelSummary: string;
  effortSummary: string;
  calledExecutor: ActivityCalledExecutorView | null;
  usage: UsageView;
  duration: ActivityDurationView;
  durationSummary: string;
  durationDetail: string;
  timeline: ActivityTimelineEventView[];
  delivery: ObservationDelivery[];
  deliveries: ActivityDeliveryView[];
  deliveryLabel: string;
  deliveryNote: string;
  deliveryAtValue: string;
  deliveryRecipient: string;
  deliverySummary: string;
  latestReturn: string;
  children: ActivityChildView[];
  childrenReturned: number;
  childrenCount: number;
  childCoverage: string;
  parentRequestId: string | null;
  summary: string;
}

export interface ActivityScopeView {
  scoped: boolean;
  scope: ActivityFeedScope | null;
  source: ObservationSource | null;
  job: ObservationJob | null;
  sourceLabel: string;
  projectLabel: string;
  jobLabel: string;
  jobUnavailable: boolean;
  summary: string;
  ids: string;
}

export interface UnavailableSourceView {
  source: ObservationSource;
  label: string;
  message: string;
}

export interface ActivityFeedProjection {
  scope: ActivityScopeView;
  rows: ActivityRequestView[];
  totalRecorded: number;
  filteredOutCount: number;
  hasMore: boolean;
  jobsHasMore: boolean;
  warnings: string[];
  unavailableSources: UnavailableSourceView[];
  generatedAtMs: number | null;
  snapshotTimeLabel: string;
  nextCursor: string | null;
}

export interface ActivityFeedProjectionOptions {
  scope?: ActivityFeedScope | null;
  now_ms?: number | null;
}

const LIFECYCLE_LABELS: Record<ObservationLifecycleState, string> = {
  queued: 'Queued',
  running: 'Running',
  waiting_for_user: 'Waiting for user',
  completed: 'Completed',
  ready_for_review: 'Ready for review',
  failed: 'Failed',
  cancelled: 'Cancelled',
  unknown: 'Unknown',
};

const DELIVERY_STAGE_LABELS: Record<DeliveryStage, string> = {
  result_ready: 'Result ready',
  notification_submitted: 'Notification submitted',
  batch_delivered: 'Batch delivered',
  batch_acknowledged: 'Batch acknowledged',
  response_returned: 'Response returned',
  unknown: 'Unknown',
  failed: 'Delivery failed',
};

const DELIVERY_STAGE_NOTES: Record<DeliveryStage, string> = {
  result_ready: 'Result is ready; delivery is not yet proven.',
  notification_submitted:
    'Notification was submitted; submission is not proof of delivery or reading.',
  batch_delivered: 'Batch was delivered; reading is not proven.',
  batch_acknowledged:
    'Batch acknowledgement is not proof that a reader opened or read the item.',
  response_returned: 'A response was returned to the origin.',
  unknown: 'Delivery state is unknown; no return is proven.',
  failed: 'Delivery failed.',
};

const REQUESTER_EVIDENCE_LABELS: Record<ObservationActor['evidence'], RequesterEvidenceLabel> = {
  configured: 'Configured',
  observed: 'Observed',
  operator: 'Operator supplied',
  unknown: 'Unknown',
};

const REQUESTER_EVIDENCE_KEYS: Record<ObservationActor['evidence'], string> = {
  configured: 'configured',
  observed: 'observed',
  operator: 'operator',
  unknown: 'unknown',
};

function trimOrNull(value: string | null | undefined): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function finiteTimestamp(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : null;
}

function finiteCount(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function nonNegativeInteger(
  value: number | null | undefined,
  fallback: number
): number {
  const count = finiteCount(value);
  return count === null ? fallback : Math.max(0, Math.trunc(count));
}

function compareText(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

function padTwo(value: number): string {
  return String(value).padStart(2, '0');
}

export function requestIdentityKey(sourceId: string, requestId: string): string {
  return `${sourceId}\u0000${requestId}`;
}

export function requestDomKey(sourceId: string, requestId: string): string {
  return `${encodeURIComponent(sourceId)}::${encodeURIComponent(requestId)}`;
}

export function compactFeedText(
  value: string | null | undefined,
  maxLength: number = 260
): string {
  const collapsed = (value ?? '').replace(/\s+/g, ' ').trim();
  if (!collapsed) {
    return '';
  }
  const limit =
    Number.isFinite(maxLength) && maxLength >= 1 ? Math.trunc(maxLength) : 260;
  if (collapsed.length <= limit) {
    return collapsed;
  }
  return `${collapsed.slice(0, limit - 1)}…`;
}

export function formatFeedTimestamp(timestampMs: number | null | undefined): string {
  const ms = finiteTimestamp(timestampMs);
  if (ms === null) {
    return 'Not reported';
  }
  const date = new Date(ms);
  return `${date.getFullYear()}-${padTwo(date.getMonth() + 1)}-${padTwo(
    date.getDate()
  )} ${padTwo(date.getHours())}:${padTwo(date.getMinutes())}`;
}

export function formatOptionalField(value: string | null | undefined): string {
  return trimOrNull(value) ?? 'Not reported';
}

export function requestMeaningfulTimestampMs(request: ObservationRequest): number | null {
  const candidates: Array<number | null | undefined> = [
    request.updated_at_ms,
    request.finished_at_ms,
    request.started_at_ms,
    ...deliveryRecordList(request.delivery).map((delivery) => delivery.at_ms),
  ];
  for (const child of request.children ?? []) {
    candidates.push(child.finished_at_ms, child.started_at_ms);
  }

  let newest: number | null = null;
  for (const candidate of candidates) {
    const ms = finiteTimestamp(candidate);
    if (ms !== null && (newest === null || ms > newest)) {
      newest = ms;
    }
  }
  return newest;
}

export function formatLifecycleLabel(
  state: ObservationLifecycleState | null | undefined
): string {
  if (!state) {
    return 'Unknown';
  }
  return LIFECYCLE_LABELS[state] ?? 'Unknown';
}

export function resolveLifecycleTone(
  state: ObservationLifecycleState | null | undefined
): LifecycleTone {
  switch (state) {
    case 'queued':
      return 'neutral';
    case 'running':
      return 'active';
    case 'waiting_for_user':
      return 'waiting';
    case 'completed':
      return 'ok';
    case 'ready_for_review':
      return 'review';
    case 'failed':
      return 'danger';
    case 'cancelled':
      return 'cancelled';
    case 'unknown':
    default:
      return 'unknown';
  }
}

export function shouldShowSpinner(
  state: ObservationLifecycleState | null | undefined
): boolean {
  return state === 'running';
}

export function formatRequesterEvidence(
  evidence: ObservationActor['evidence'] | null | undefined
): RequesterEvidenceLabel {
  if (!evidence) {
    return 'Unknown';
  }
  return REQUESTER_EVIDENCE_LABELS[evidence] ?? 'Unknown';
}

export function requesterEvidenceKey(
  evidence: ObservationActor['evidence'] | null | undefined
): string {
  if (!evidence) {
    return 'unknown';
  }
  return REQUESTER_EVIDENCE_KEYS[evidence] ?? 'unknown';
}

export function formatRequesterIdentity(
  actor: ObservationActor | null | undefined
): string {
  return trimOrNull(actor?.label) ?? 'Unknown';
}

export function formatRequesterSummary(
  actor: ObservationActor | null | undefined
): string {
  return `Requester: ${formatRequesterIdentity(actor)} (${formatRequesterEvidence(
    actor?.evidence
  )})`;
}

/**
 * Collapsed-row requester contract. A missing label and missing evidence must not
 * turn into the repeated "Unknown Unknown" diagnostic copy.
 */
export function projectRequesterLine(
  actor: ObservationActor | null | undefined
): ActivityRequesterLineView {
  const value = formatRequesterIdentity(actor);
  const evidence = formatRequesterEvidence(actor?.evidence);
  const knownLabel = trimOrNull(actor?.label) !== null;
  const showEvidence = knownLabel || evidence !== 'Unknown';
  return {
    value,
    evidence: showEvidence ? evidence : null,
    evidenceKey: requesterEvidenceKey(actor?.evidence),
    detail: !knownLabel && evidence === 'Unknown'
      ? 'Requester identity and evidence not reported.'
      : `Requester ${value} · ${evidence}.`,
  };
}

function humanizeRecordedLabel(value: string): string {
  return value.replace(/_/g, ' ').replace(/\s+/g, ' ').trim();
}

export function formatExecutorRoleLabel(
  role: string | null | undefined
): string {
  const value = trimOrNull(role);
  return value === null || value === 'Not reported'
    ? 'Role not reported'
    : humanizeRecordedLabel(value);
}

export function formatRecordedOperation(
  operation: string | null | undefined
): string {
  const value = trimOrNull(operation);
  return value === null ? 'Operation not reported' : humanizeRecordedLabel(value);
}

function capitalizeLabel(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

/** Display names shorten familiar model identifiers; exact receipts stay in detail. */
export function formatFriendlyModel(value: string): string {
  const model = value.trim().split('/').at(-1) ?? value.trim();
  if (/^deepseek(?:-|$)/i.test(model)) return 'DeepSeek';
  if (/(?:^|-)argon(?:-|$)/i.test(model)) return 'Argon';
  if (/(?:^|-)sol(?:-|$)/i.test(model)) return 'Sol';
  if (/(?:^|-)astra(?:-|$)/i.test(model)) return 'Astra';
  if (/^gpt-/i.test(model)) return model.replace(/^gpt-/i, 'GPT-');
  if (/^gemini-/i.test(model)) return model.replace(/^gemini-/i, 'Gemini ');
  if (/^jev-/i.test(model)) return `Jev · ${model}`;
  return model;
}

export function isLocalMcpExecutor(
  executor: ObservationExecutor | null | undefined
): boolean {
  if (!executor) {
    return false;
  }
  const tool = trimOrNull(executor.tool);
  if (tool === null || !/^threadlane_mcp$/i.test(tool) || trimOrNull(executor.provider) !== 'local') {
    return false;
  }
  return [
    executor.role,
    executor.requested_model,
    executor.observed_model,
    executor.requested_effort,
    executor.observed_effort,
  ].every((value) => trimOrNull(value) === null);
}

export function formatFriendlyTool(executor: ObservationExecutor | null): string {
  const tool = trimOrNull(executor?.tool) ?? trimOrNull(executor?.provider);
  if (!tool) return 'Tool not recorded';
  if (/^threadlane_mcp$/i.test(tool)) return 'Threadlane MCP';
  if (/^opencode(?:2|-go|_go)?$/i.test(tool)) {
    return /go/i.test(tool) ? 'OpenCode Go' : 'OpenCode';
  }
  if (/^(?:agy|antigravity)$/i.test(tool)) return 'Antigravity';
  if (/^codex$/i.test(tool)) return 'Codex';
  if (/^jev$/i.test(tool)) return 'Jev';
  return capitalizeLabel(humanizeRecordedLabel(tool));
}

/** Choose one evidence source for the compact value; never combine mismatched receipts. */
export function projectExecutorDisplay(
  executor: ObservationExecutor | null | undefined
): ActivityExecutorDisplay {
  if (isLocalMcpExecutor(executor)) {
    return {
      role: 'Threadlane MCP',
      model: 'Local operation',
      detail:
        'Executor: Threadlane MCP · local. Local MCP operation; model and effort are not applicable.',
    };
  }
  const observed = trimOrNull(executor?.observed_model);
  const requested = trimOrNull(executor?.requested_model);
  const model = observed ?? requested;
  const effort = observed !== null
    ? trimOrNull(executor?.observed_effort)
    : trimOrNull(executor?.requested_effort);
  const detail = `${formatExecutorSummary(executor)}. Model ${formatRequestedObserved(
    executor?.requested_model, executor?.observed_model
  )}. Effort ${formatRequestedObserved(
    executor?.requested_effort, executor?.observed_effort
  )}.`;
  return {
    role: capitalizeLabel(formatExecutorRoleLabel(executor?.role)),
    model: model === null
      ? 'Model not reported'
      : [formatFriendlyModel(model), effort ? capitalizeLabel(effort) : null]
          .filter(Boolean).join(' · '),
    detail,
  };
}

/** Child attribution is explicit and cannot become an inferred root executor. */
export function projectRequestExecutorDisplay(view: ActivityRequestView): ActivityExecutorDisplay {
  const root = view.request.executor;
  const hasRootExecutionEvidence = [root?.role, root?.requested_model, root?.observed_model]
    .some((value) => trimOrNull(value) !== null);
  if (hasRootExecutionEvidence) {
    return projectExecutorDisplay(root);
  }
  const called = projectCalledExecutors(view.request.children, view.childrenCount);
  if (called) {
    if (view.children.length === 1 && view.childrenCount <= 1) {
      const display = projectExecutorDisplay(view.children[0]!.call.executor);
      const role = trimOrNull(view.children[0]!.call.executor?.role)
        ? `Called ${display.role}` : 'Called tool';
      return { ...display, role, detail: called.detail + ' ' + display.detail };
    }
    return {
      role: 'Called tools',
      model: view.childCoverage,
      detail: called.detail,
    };
  }
  if (isLocalMcpExecutor(root)) {
    return projectExecutorDisplay(root);
  }
  if (hasExecutorRecord(root)) {
    return { role: formatFriendlyTool(root ?? null), model: null, detail: projectExecutorDisplay(root).detail };
  }
  return { role: 'Executor not recorded', model: null, detail: 'No executor was recorded for this request.' };
}

/** Quiet causal copy for the timeline; full recorded fields remain secondary. */
export function projectTimelineDisplay(
  view: ActivityRequestView,
  event: ActivityTimelineEventView
): ActivityTimelineDisplay {
  if (event.kind === 'requester') {
    const actor = view.request.actor;
    const detail = [
      view.requesterLabel,
      trimOrNull(actor.role) ? capitalizeLabel(humanizeRecordedLabel(actor.role!)) : null,
      trimOrNull(actor.source),
    ].filter(Boolean).join(' · ');
    const conversation = trimOrNull(view.request.thread_label);
    return {
      label: 'Requester',
      detail,
      lines: [
        conversation ? `Conversation: ${conversation}` : '',
        !trimOrNull(actor.label) && trimOrNull(actor.source)
          ? 'Source is known; caller identity is unavailable.' : '',
      ].filter(Boolean),
    };
  }
  if (event.kind === 'request') {
    if (view.request.operation === 'worker.historical') {
      return {
        label: 'Recovered from worker history',
        detail: 'Worker execution was recovered; the original request was not recorded.',
        lines: [],
      };
    }
    const origin = trimOrNull(view.request.actor.source);
    return {
      label: origin ? `${origin} → Threadlane` : 'Request recorded',
      detail: capitalizeLabel(formatRecordedOperation(view.request.operation)),
      lines: [compactFeedText(view.request.summary, 320)].filter(Boolean),
    };
  }
  if (event.kind === 'child') {
    const child = view.children.find((item, index) => event.key === `child:${index}:${item.call.id}`);
    if (child) {
      const executor = projectExecutorDisplay(child.call.executor);
      return {
        label: `Threadlane → ${formatFriendlyTool(child.call.executor)}`,
        detail: `${child.title} · ${child.stateLabel}`,
        lines: [
          hasExecutorRecord(child.call.executor) ? executor.model : null,
          child.summary,
        ].filter((line): line is string => Boolean(line)),
      };
    }
  }
  return {
    label: event.label,
    detail: event.lines[0] || event.detail,
    lines: [event.lines[1], event.detail !== 'Recipient: Not reported' ? event.detail : '']
      .filter((line): line is string => Boolean(line)),
  };
}

/**
 * Delivery DTOs can repeat one transport stage for every covered event. Group
 * their actual records by stage and exact recipient, without interpreting prose
 * or claiming a number of distinct batches. The raw timeline is left untouched.
 */
export function projectVisibleTimeline(view: ActivityRequestView): ActivityVisibleTimelineEvent[] {
  const deliveryByKey = new Map<string, ActivityDeliveryView>(
    view.deliveries.map((delivery, index) => [`delivery:${index}`, delivery] as const)
  );
  const recipientLabels = new Map<string, string>();
  const groups = new Map<string, {
    entry: ActivityVisibleTimelineEvent;
    index: number;
  }>();
  const visible: Array<{ entry: ActivityVisibleTimelineEvent; index: number }> = [];
  const stageLabels: Record<DeliveryStage, string> = {
    result_ready: 'Result ready',
    notification_submitted: 'Notification submitted',
    batch_delivered: 'Events delivered',
    batch_acknowledged: 'Delivery acknowledged',
    response_returned: 'Response returned',
    unknown: 'Delivery status unknown',
    failed: 'Delivery failed',
  };

  for (const [index, event] of view.timeline.entries()) {
    const delivery = event.kind === 'delivery' ? deliveryByKey.get(event.key) : null;
    if (!delivery) {
      visible.push({
        entry: { event, display: projectTimelineDisplay(view, event), recordCount: 1 },
        index,
      });
      continue;
    }
    const recipient = trimOrNull(delivery.record.recipient);
    const key = JSON.stringify([delivery.stage, recipient]);
    let group = groups.get(key);
    if (!group) {
      let recipientLabel = 'Recipient not recorded';
      if (recipient !== null) {
        if (!recipientLabels.has(recipient)) {
          const known: Record<string, string> = {
            follower: 'Follower', origin: 'Origin', operator: 'Operator',
            cli: 'CLI', codex: 'Codex',
          };
          recipientLabels.set(recipient,
            recipient === view.request.binding_id
              ? 'Linked conversation'
              : Object.hasOwn(known, recipient.toLowerCase())
                ? known[recipient.toLowerCase()]!
                : `Recipient ${recipientLabels.size + 1}`
          );
        }
        recipientLabel = recipientLabels.get(recipient)!;
      }
      group = {
        entry: {
          event,
          display: { label: stageLabels[delivery.stage], detail: recipientLabel, lines: [] },
          recordCount: 0,
        },
        index,
      };
      groups.set(key, group);
      visible.push(group);
    }
    group.entry.recordCount += 1;
    // Place the summary at its latest recorded occurrence so later transport
    // evidence does not appear to precede an intervening causal call.
    group.entry.event = event;
    group.index = index;
  }

  for (const group of groups.values()) {
    const count = group.entry.recordCount;
    group.entry.display.detail = `${count} ${count === 1 ? 'record' : 'records'} · ${group.entry.display.detail}`;
  }
  visible.sort((left, right) => left.index - right.index);
  const lastDelivery = [...visible].reverse().find((item) => item.entry.event.kind === 'delivery');
  if (lastDelivery) {
    lastDelivery.entry.display.lines.push(
      'Delivery and acknowledgement do not establish human reading or review acceptance.'
    );
  }
  return visible.map(({ entry }) => entry);
}

/**
 * Compact model evidence keeps requested and observed models distinct. Equal
 * values may collapse to one model; otherwise both recorded values remain
 * visible and the full sentence stays available as the accessible detail.
 */
export function projectRecordedModel(
  requested: string | null | undefined,
  observed: string | null | undefined
): RecordedModelView {
  const requestedValue = trimOrNull(requested);
  const observedValue = trimOrNull(observed);
  if (requestedValue === null && observedValue === null) {
    return {
      value: 'Not reported',
      detail: 'Model requested and observed not reported.',
    };
  }
  if (requestedValue === null) {
    return {
      value: `observed ${observedValue}`,
      detail: `Model requested not reported · observed ${observedValue}.`,
    };
  }
  if (observedValue === null) {
    return {
      value: `requested ${requestedValue}`,
      detail: `Model requested ${requestedValue} · observed not reported.`,
    };
  }
  if (requestedValue === observedValue) {
    return {
      value: requestedValue,
      detail: `Model requested ${requestedValue} · observed ${observedValue}.`,
    };
  }
  return {
    value: `req ${requestedValue} · obs ${observedValue}`,
    detail: `Model requested ${requestedValue} · observed ${observedValue}.`,
  };
}

export function hasExecutorRecord(
  executor: ObservationExecutor | null | undefined
): boolean {
  if (!executor) {
    return false;
  }
  return [
    executor.tool,
    executor.provider,
    executor.role,
    executor.requested_model,
    executor.observed_model,
    executor.requested_effort,
    executor.observed_effort,
  ].some((value) => trimOrNull(value) !== null);
}

export function formatExecutorIdentity(
  executor: ObservationExecutor | null | undefined
): string {
  if (!hasExecutorRecord(executor)) {
    return 'Executor not recorded';
  }
  if (isLocalMcpExecutor(executor)) {
    return 'Threadlane MCP · local';
  }
  const parts = [
    trimOrNull(executor?.role),
    trimOrNull(executor?.tool),
    trimOrNull(executor?.provider),
  ].filter((value): value is string => value !== null);
  return parts.length > 0 ? parts.join(' · ') : 'Executor not identified';
}

export function formatExecutorSummary(
  executor: ObservationExecutor | null | undefined
): string {
  const identity = formatExecutorIdentity(executor);
  return identity === 'Executor not recorded' || identity === 'Executor not identified'
    ? identity
    : `Executor: ${identity}`;
}

export function formatRequestedObserved(
  requested: string | null | undefined,
  observed: string | null | undefined
): string {
  return `requested ${trimOrNull(requested) ?? 'Not reported'} · observed ${
    trimOrNull(observed) ?? 'Not reported'
  }`;
}

export function resolveExecutorBrand(
  executor: ObservationExecutor | null | undefined
): BrandKey {
  const provider = `${executor?.tool ?? ''} ${executor?.provider ?? ''}`.toLowerCase();
  if (!provider.trim()) {
    return 'generic';
  }
  if (provider.includes('opencode')) {
    return 'opencode';
  }
  if (provider.includes('antigravity') || /(^|\s)agy(\s|$)/.test(provider)) {
    return 'antigravity';
  }
  if (provider.includes('codex') || provider.includes('openai')) {
    return 'openai';
  }
  if (provider.includes('gemini')) {
    return 'gemini';
  }
  if (provider.includes('deepseek')) {
    return 'deepseek';
  }
  if (provider.includes('demo')) {
    return 'demo';
  }
  return 'generic';
}

function distinctRecordedValues(
  values: ReadonlyArray<string | null | undefined>
): string[] {
  const distinct: string[] = [];
  for (const value of values) {
    const trimmed = trimOrNull(value);
    if (trimmed !== null && !distinct.includes(trimmed)) {
      distinct.push(trimmed);
    }
  }
  return distinct;
}

function pluralizedCount(count: number, singular: string, plural: string): string {
  return `${count} ${count === 1 ? singular : plural}`;
}

/**
 * Summarizes only child call executor records for the collapsed row. Root
 * request executor data is never consulted, and unknown child records do not
 * contribute an inferred tool, provider, model, or effort.
 *
 * `reportedCount` is the request's child call total. When it exceeds the
 * returned calls, the compact summary names the returned-call coverage and
 * never presents the returned subset as complete.
 */
export function projectCalledExecutors(
  children: readonly ObservationCall[] | null | undefined,
  reportedCount: number | null | undefined = null
): ActivityCalledExecutorView | null {
  const calls = children ?? [];
  const returnedCount = calls.length;
  const totalReported = Math.max(
    nonNegativeInteger(reportedCount, returnedCount),
    returnedCount
  );
  const callsIncomplete = totalReported > returnedCount;
  const recorded: Array<{ call: ObservationCall; executor: ObservationExecutor }> = [];
  for (const call of calls) {
    const executor = call.executor ?? null;
    if (executor !== null && hasExecutorRecord(executor)) {
      recorded.push({ call, executor });
    }
  }
  if (recorded.length === 0) {
    return null;
  }

  const tools = distinctRecordedValues(recorded.map((entry) => entry.executor.tool));
  const providers = distinctRecordedValues(
    recorded.map((entry) => entry.executor.provider)
  );
  const models = distinctRecordedValues(
    recorded.flatMap((entry) => [
      entry.executor.requested_model,
      entry.executor.observed_model,
    ])
  );
  const efforts = distinctRecordedValues(
    recorded.flatMap((entry) => [
      entry.executor.requested_effort,
      entry.executor.observed_effort,
    ])
  );
  const unknownCount = Math.max(0, returnedCount - recorded.length);
  const coverageParts: string[] = [];
  if (callsIncomplete) {
    coverageParts.push(
      `${returnedCount} of ${totalReported} reported child calls returned`
    );
  }
  if (unknownCount > 0) {
    coverageParts.push(
      `${recorded.length} of ${returnedCount} returned child calls ${
        recorded.length === 1 ? 'has' : 'have'
      } executor details`
    );
  }
  const coverage =
    coverageParts.length > 0 ? `${coverageParts.join(' · ')} · ` : '';
  const detailCoverage = callsIncomplete
    ? `Returned ${returnedCount} of ${totalReported} reported child calls. `
    : '';
  const unknownNote =
    unknownCount > 0
      ? ` ${pluralizedCount(
          unknownCount,
          'returned child call has',
          'returned child calls have'
        )} no executor record.`
      : '';
  const brands = recorded.map((entry) => resolveExecutorBrand(entry.executor));
  // A partial returned-call set must not lend its child provider an official brand.
  const brand: BrandKey =
    !callsIncomplete &&
    unknownCount === 0 &&
    brands.every((candidate) => candidate === brands[0])
      ? brands[0]!
      : 'generic';

  const singleEntry = recorded.length === 1 ? recorded[0]! : null;
  const singleFields = singleEntry
    ? [
        `tool ${trimOrNull(singleEntry.executor.tool) ?? 'Not reported'}`,
        `provider ${trimOrNull(singleEntry.executor.provider) ?? 'Not reported'}`,
        `model requested ${
          trimOrNull(singleEntry.executor.requested_model) ?? 'Not reported'
        }`,
        `observed ${
          trimOrNull(singleEntry.executor.observed_model) ?? 'Not reported'
        }`,
        `effort requested ${
          trimOrNull(singleEntry.executor.requested_effort) ?? 'Not reported'
        }`,
        `observed ${
          trimOrNull(singleEntry.executor.observed_effort) ?? 'Not reported'
        }`,
      ]
    : null;

  if (singleEntry !== null && !callsIncomplete) {
    const entry = singleEntry;
    const fields = singleFields!;
    return {
      callCount: returnedCount,
      recordedCount: recorded.length,
      unknownCount,
      toolCount: tools.length,
      providerCount: providers.length,
      modelCount: models.length,
      effortCount: efforts.length,
      summary: `${coverage}${fields.join(' · ')}`,
      detail: `Child call ${entry.call.id} recorded ${fields.join(' · ')}.${unknownNote}`,
      brand,
    };
  }

  const countSummary = `${pluralizedCount(
    tools.length,
    'tool',
    'tools'
  )} / ${pluralizedCount(models.length, 'model', 'models')}`;
  const exactReturnedDetail =
    singleEntry && singleFields
      ? ` Child call ${singleEntry.call.id} recorded ${singleFields.join(' · ')}.`
      : '';

  return {
    callCount: returnedCount,
    recordedCount: recorded.length,
    unknownCount,
    toolCount: tools.length,
    providerCount: providers.length,
    modelCount: models.length,
    effortCount: efforts.length,
    summary: `${coverage}${countSummary}`,
    detail: `${detailCoverage}${pluralizedCount(
      recorded.length,
      'child executor recorded',
      'child executors recorded'
    )}: ${pluralizedCount(tools.length, 'tool', 'tools')}, ${pluralizedCount(
      providers.length,
      'provider',
      'providers'
    )}, ${pluralizedCount(models.length, 'model', 'models')}, ${pluralizedCount(
      efforts.length,
      'effort',
      'efforts'
    )}.${exactReturnedDetail}${unknownNote}`,
    brand,
  };
}

export function projectUsage(
  usage: ObservationUsage | null | undefined,
  executor: ObservationExecutor | null | undefined = null,
  hasChildWork: boolean = false
): UsageView {
  const input = finiteCount(usage?.input_tokens);
  const output = finiteCount(usage?.output_tokens);
  const cacheRead = finiteCount(usage?.cache_read_tokens);
  const total = finiteCount(usage?.total_tokens);
  const provenance = trimOrNull(usage?.provenance);
  const reported = [input, output, cacheRead, total].some((value) => value !== null);

  if (!reported) {
    if (isLocalMcpExecutor(executor) && !hasChildWork) {
      return {
        reported: false,
        input,
        output,
        cacheRead,
        total,
        provenance,
        summary: 'Local operation',
        detail: 'Local Threadlane MCP handler; caller model usage is not attributed to this operation.',
      };
    }
    return {
      reported: false,
      input,
      output,
      cacheRead,
      total,
      provenance,
      summary: 'Usage not reported',
      detail: 'Usage not reported.',
    };
  }

  const parts: string[] = [];
  if (input !== null) {
    parts.push(`in ${input.toLocaleString('en-US')}`);
  }
  if (output !== null) {
    parts.push(`out ${output.toLocaleString('en-US')}`);
  }
  if (cacheRead !== null) {
    parts.push(`cache read ${cacheRead.toLocaleString('en-US')}`);
  }
  if (total !== null) {
    parts.push(`total ${total.toLocaleString('en-US')}`);
  }

  const tokenText = `Tokens: ${parts.join(' · ')}`;
  return {
    reported: true,
    input,
    output,
    cacheRead,
    total,
    provenance,
    summary: tokenText,
    detail: `${tokenText}. Provenance: ${provenance ?? 'Not reported'}.`,
  };
}

export function formatDurationMs(durationMs: number): string {
  const safe = Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : 0;
  if (safe < 1000) {
    return `${Math.round(safe)}ms`;
  }
  const totalSeconds = safe / 1000;
  if (totalSeconds < 60) {
    const rounded =
      totalSeconds < 10 ? Math.round(totalSeconds * 10) / 10 : Math.round(totalSeconds);
    return `${rounded}s`;
  }
  const wholeMinutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds % 60);
  if (wholeMinutes < 60) {
    return seconds > 0 ? `${wholeMinutes}m ${seconds}s` : `${wholeMinutes}m`;
  }
  const hours = Math.floor(wholeMinutes / 60);
  const remainingMinutes = wholeMinutes % 60;
  return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
}

export function projectDuration(
  request: Pick<
    ObservationRequest,
    'state' | 'started_at_ms' | 'finished_at_ms' | 'duration_ms'
  >,
  nowMs: number | null | undefined = null
): ActivityDurationView {
  const recorded = finiteCount(request.duration_ms);
  if (recorded !== null && recorded >= 0) {
    return {
      kind: 'recorded',
      text: formatDurationMs(recorded),
      note: 'Recorded duration.',
    };
  }

  const started = finiteTimestamp(request.started_at_ms);
  const finished = finiteTimestamp(request.finished_at_ms);
  if (started !== null && finished !== null && finished >= started) {
    return {
      kind: 'derived',
      text: formatDurationMs(finished - started),
      note: 'Derived from recorded start and end timestamps.',
    };
  }

  const now = finiteTimestamp(nowMs);
  if (request.state === 'running' && started !== null && now !== null) {
    return {
      kind: 'elapsed',
      text: formatDurationMs(Math.max(0, now - started)),
      note: 'Elapsed from the recorded start timestamp.',
    };
  }

  return {
    kind: 'unknown',
    text: 'Duration not reported',
    note: 'Duration not reported.',
  };
}

export function formatDurationSummary(duration: ActivityDurationView): string {
  if (duration.kind === 'unknown') {
    return 'Duration not reported';
  }
  if (duration.kind === 'elapsed') {
    return `Elapsed ${duration.text}`;
  }
  return `Duration ${duration.text}`;
}

export function formatDurationDetail(duration: ActivityDurationView): string {
  if (duration.kind === 'unknown') {
    return 'Duration not reported.';
  }
  return `${formatDurationSummary(duration)}. ${duration.note}`;
}

export function formatCompactDuration(
  duration: ActivityDurationView,
  detail: string
): ActivityCompactEvidence {
  if (duration.kind === 'unknown') {
    return { value: 'Duration not reported', detail };
  }
  return { value: duration.text, detail };
}

/**
 * Collapsed usage is a single recorded total at most. Input/output/cache
 * breakdowns stay in the expanded detail and are never added together here.
 */
export function formatCompactUsage(
  usage: UsageView
): ActivityCompactEvidence {
  if (!usage.reported) {
    return {
      value:
        usage.summary === 'Local operation'
          ? 'Local operation'
          : 'Usage not reported',
      detail: usage.detail,
    };
  }
  if (usage.total === null) {
    return { value: 'Partial usage', detail: usage.detail };
  }
  const compact = new Intl.NumberFormat('en-US', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(usage.total);
  return {
    value: `${compact} tokens`,
    detail: usage.detail,
  };
}

export function countRunningRequests(
  rows: readonly ActivityRequestView[]
): number {
  return rows.filter(
    (row) => row.request.state === 'running' || row.callsRunning
  ).length;
}

export function formatRunningCountLabel(count: number): string {
  const safe = Number.isFinite(count) ? Math.max(0, Math.trunc(count)) : 0;
  if (safe === 0) {
    return '';
  }
  return safe === 1 ? '1 call running' : `${safe} calls running`;
}

export function formatRelativeTime(
  timestampMs: number | null | undefined,
  nowMs: number | null | undefined = Date.now()
): string {
  const timestamp = finiteTimestamp(timestampMs);
  if (timestamp === null) {
    return 'Time not reported';
  }
  const now = finiteTimestamp(nowMs) ?? Date.now();
  const elapsedSeconds = Math.max(0, Math.floor((now - timestamp) / 1000));
  if (elapsedSeconds < 45) {
    return 'Just now';
  }
  const minutes = Math.floor(elapsedSeconds / 60);
  if (minutes < 60) {
    return `${Math.max(1, minutes)}m ago`;
  }
  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }
  const days = Math.floor(hours / 24);
  if (days < 30) {
    return `${days}d ago`;
  }
  return formatFeedTimestamp(timestamp);
}

export function describeDeliveryStage(stage: DeliveryStage | null | undefined): {
  label: string;
  note: string;
} {
  if (!stage) {
    return {
      label: 'Delivery not reported',
      note: 'No delivery stage was recorded.',
    };
  }
  return {
    label: DELIVERY_STAGE_LABELS[stage] ?? 'Unknown',
    note: DELIVERY_STAGE_NOTES[stage] ?? DELIVERY_STAGE_NOTES.unknown,
  };
}

export function formatDeliveryStageLabel(
  stage: DeliveryStage | null | undefined
): string {
  return describeDeliveryStage(stage).label;
}

function deliveryRecordList(
  value: ObservationDelivery | readonly ObservationDelivery[] | null | undefined
): ObservationDelivery[] {
  if (value === null || value === undefined) {
    return [];
  }
  if (Array.isArray(value)) {
    return [...(value as readonly ObservationDelivery[])];
  }
  return [value as ObservationDelivery];
}

function sortDeliveryRecords(
  value: ObservationDelivery | readonly ObservationDelivery[] | null | undefined
): ObservationDelivery[] {
  return deliveryRecordList(value)
    .map((record, index) => ({ record, index }))
    .sort((left, right) => {
      const leftMs = finiteTimestamp(left.record.at_ms);
      const rightMs = finiteTimestamp(right.record.at_ms);
      if (leftMs === null && rightMs === null) {
        return left.index - right.index;
      }
      if (leftMs === null) {
        return 1;
      }
      if (rightMs === null) {
        return -1;
      }
      if (leftMs !== rightMs) {
        return leftMs - rightMs;
      }
      return left.index - right.index;
    })
    .map((entry) => entry.record);
}

function latestDeliveryRecord(
  records: readonly ObservationDelivery[]
): ObservationDelivery | null {
  let latest: ObservationDelivery | null = null;
  let latestMs: number | null = null;
  for (const record of records) {
    const ms = finiteTimestamp(record.at_ms);
    if (ms !== null && (latestMs === null || ms >= latestMs)) {
      latest = record;
      latestMs = ms;
    }
  }
  if (latest !== null) {
    return latest;
  }
  return records.length > 0 ? records[records.length - 1]! : null;
}

export function formatLatestProvenReturnStage(
  delivery: ObservationDelivery | readonly ObservationDelivery[] | null | undefined
): string {
  const records = deliveryRecordList(delivery);
  if (records.length === 0) {
    return 'No return stage recorded.';
  }
  const latest = latestDeliveryRecord(records)!;
  if (latest.stage === 'response_returned') {
    return 'Latest proven return stage: Response returned.';
  }
  const responseNote = records.some((record) => record.stage === 'response_returned')
    ? 'An earlier synchronous response was returned; this does not prove final result delivery.'
    : 'No response return recorded.';
  return `Latest recorded stage: ${describeDeliveryStage(latest.stage).label}. ${responseNote}`;
}

function buildDeliveryView(record: ObservationDelivery): ActivityDeliveryView {
  const description = describeDeliveryStage(record.stage);
  return {
    record,
    stage: record.stage,
    label: description.label,
    note: description.note,
    atMs: finiteTimestamp(record.at_ms),
    atValue: formatFeedTimestamp(record.at_ms),
    recipient: formatOptionalField(record.recipient),
    summary:
      compactFeedText(record.summary, 320) || 'Delivery summary not reported.',
  };
}

function summarizeDeliveryLabel(records: readonly ObservationDelivery[]): string {
  if (records.length === 0) {
    return describeDeliveryStage(null).label;
  }
  if (records.length === 1) {
    return describeDeliveryStage(records[0]?.stage).label;
  }
  return `${records.length} delivery records`;
}

function summarizeDeliveryNote(records: readonly ObservationDelivery[]): string {
  if (records.length === 0) {
    return describeDeliveryStage(null).note;
  }
  if (records.length === 1) {
    return describeDeliveryStage(records[0]?.stage).note;
  }
  return 'Multiple delivery records are recorded; each stage is listed separately.';
}

function summarizeDeliveryRecipients(records: readonly ObservationDelivery[]): string {
  const recipients: string[] = [];
  for (const record of records) {
    const recipient = trimOrNull(record.recipient);
    if (recipient !== null && !recipients.includes(recipient)) {
      recipients.push(recipient);
    }
  }
  return recipients.length > 0 ? recipients.join(' · ') : 'Not reported';
}

function summarizeDeliverySummary(records: readonly ObservationDelivery[]): string {
  const summaries = records
    .map((record) => compactFeedText(record.summary, 320))
    .filter((summary) => summary.length > 0);
  if (summaries.length === 0) {
    return 'Delivery summary not reported.';
  }
  return compactFeedText(summaries.join(' · '), 640);
}

export function formatChildCoverage(returned: number, reported: number): string {
  const safeReturned = Number.isFinite(returned) ? Math.max(0, Math.trunc(returned)) : 0;
  const safeReported = Number.isFinite(reported) ? Math.max(0, Math.trunc(reported)) : 0;
  if (safeReported > safeReturned) {
    return `Additional calls are not included: showing ${safeReturned} of ${safeReported}.`;
  }
  if (safeReported < safeReturned) {
    return `Showing ${safeReturned} returned child calls; the reported count is ${safeReported}.`;
  }
  if (safeReturned === 0) {
    return 'No child calls returned.';
  }
  return `${safeReturned} child ${safeReturned === 1 ? 'call' : 'calls'} returned.`;
}

export function resolveRequestJob(
  snapshot: Pick<ObservationSnapshot, 'jobs'>,
  request: Pick<ObservationRequest, 'source_id' | 'job_id'>
): {
  job: ObservationJob | null;
  label: string;
  linked: boolean;
  unavailable: boolean;
} {
  const jobId = trimOrNull(request.job_id);
  if (!jobId) {
    return {
      job: null,
      label: 'No linked job',
      linked: false,
      unavailable: false,
    };
  }

  const job =
    snapshot.jobs.find(
      (candidate) => candidate.source_id === request.source_id && candidate.id === jobId
    ) ?? null;
  if (!job) {
    return {
      job: null,
      label: 'Job details unavailable',
      linked: true,
      unavailable: true,
    };
  }

  const title = trimOrNull(job.title);
  if (!title) {
    return {
      job,
      label: 'Job details unavailable',
      linked: true,
      unavailable: true,
    };
  }

  return {
    job,
    label: title,
    linked: true,
    unavailable: false,
  };
}

export function formatSourceDetails(
  source: ObservationSource | null | undefined,
  sourceId: string
): string {
  if (!source) {
    return `Source details unavailable · ${sourceId}`;
  }
  const label = trimOrNull(source.label) ?? sourceId;
  return `${label} · ${sourceId}`;
}

export function formatConversationDetails(
  request: Pick<ObservationRequest, 'binding_id' | 'thread_uuid' | 'thread_label'>
): string {
  const parts: string[] = [];
  const label = trimOrNull(request.thread_label);
  if (label) {
    parts.push(`Thread: ${label}`);
  }
  const binding = trimOrNull(request.binding_id);
  if (binding) {
    parts.push(`Binding: ${binding}`);
  }
  const uuid = trimOrNull(request.thread_uuid);
  if (uuid) {
    parts.push(`Thread ID: ${uuid}`);
  }
  return parts.length > 0 ? parts.join(' · ') : 'Conversation details not reported.';
}

export function describeActivityScope(
  snapshot: Pick<ObservationSnapshot, 'sources' | 'jobs'>,
  scope: ActivityFeedScope | null | undefined
): ActivityScopeView {
  if (!scope) {
    return {
      scoped: false,
      scope: null,
      source: null,
      job: null,
      sourceLabel: '',
      projectLabel: '',
      jobLabel: '',
      jobUnavailable: false,
      summary: 'Showing all recorded activity across available sources.',
      ids: '',
    };
  }

  const source =
    snapshot.sources.find((candidate) => candidate.id === scope.source_id) ?? null;
  const job =
    snapshot.jobs.find(
      (candidate) =>
        candidate.source_id === scope.source_id && candidate.id === scope.job_id
    ) ?? null;
  const sourceLabel = source ? trimOrNull(source.label) ?? source.id : 'Source details unavailable';
  const projectLabel = job ? trimOrNull(job.project_label) ?? 'Project not reported' : 'Job details unavailable';
  const jobLabel = job ? trimOrNull(job.title) ?? 'Job details unavailable' : 'Job details unavailable';
  const jobUnavailable = job === null || trimOrNull(job.title) === null;
  const summary = job
    ? `Scoped to ${sourceLabel} · ${projectLabel} · ${jobLabel}`
    : `Scoped to ${sourceLabel} · ${scope.job_id}. Job details unavailable.`;

  return {
    scoped: true,
    scope,
    source,
    job,
    sourceLabel,
    projectLabel,
    jobLabel,
    jobUnavailable,
    summary,
    ids: `Source ${scope.source_id} · Job ${scope.job_id}`,
  };
}

export function formatActivityScopeLabel(scope: ActivityScopeView): string {
  if (!scope.scoped) {
    return 'All recorded activity · Available sources';
  }
  const project = scope.projectLabel.trim();
  const job = scope.jobLabel.trim();
  if (project && job && project !== job) {
    return `${project} · ${job}`;
  }
  return project || job || 'Job details unavailable';
}

function buildChildView(call: ObservationCall, nowMs: number | null): ActivityChildView {
  const executor = call.executor ?? null;
  const usage = projectUsage(call.usage);
  const duration = projectDuration(call, nowMs);
  return {
    call,
    title: trimOrNull(call.title) ?? 'Child call title not reported',
    stateLabel: formatLifecycleLabel(call.state),
    stateTone: resolveLifecycleTone(call.state),
    showSpinner: shouldShowSpinner(call.state),
    executorSummary: formatExecutorSummary(executor),
    requestedModel: formatOptionalField(executor?.requested_model),
    observedModel: formatOptionalField(executor?.observed_model),
    requestedEffort: formatOptionalField(executor?.requested_effort),
    observedEffort: formatOptionalField(executor?.observed_effort),
    modelSummary: formatRequestedObserved(
      executor?.requested_model,
      executor?.observed_model
    ),
    effortSummary: formatRequestedObserved(
      executor?.requested_effort,
      executor?.observed_effort
    ),
    usage,
    duration,
    durationSummary: formatDurationSummary(duration),
    durationDetail: formatDurationDetail(duration),
    startedAtValue: formatFeedTimestamp(call.started_at_ms),
    finishedAtValue: formatFeedTimestamp(call.finished_at_ms),
    summary: compactFeedText(call.summary, 320),
  };
}

function buildTimelineEvents(view: ActivityRequestView): ActivityTimelineEventView[] {
  const jobDetail = view.jobLinked
    ? view.jobUnavailable
      ? `unavailable · Job ${view.request.job_id}`
      : `${view.jobLabel} · Job ${view.request.job_id}`
    : 'No linked job';
  const sourceDetail =
    view.source?.availability === 'unavailable'
      ? `${view.sourceDetails} · Unavailable`
      : view.sourceDetails;
  const requestLines = [
    `Title: ${view.title}`,
    `Source: ${sourceDetail}`,
    `Request: ${view.request.source_id} · ${view.request.id}`,
    `Project: ${view.projectLabel}`,
    `Job: ${jobDetail}`,
    `Conversation: ${view.conversationDetails}`,
    `Root executor: ${view.executorRoleLabel} · tool ${view.executorTool} · provider ${view.executorProvider}`,
    `Root model: requested ${view.requestedModel} · observed ${view.observedModel}`,
    `Root effort: requested ${view.requestedEffort} · observed ${view.observedEffort}`,
    view.calledExecutor
      ? `Called tools/models: ${view.calledExecutor.detail}`
      : '',
    `Usage: ${view.usage.detail}`,
    view.durationDetail,
    `Delivery: ${view.latestReturn}`,
    `Child calls: ${view.childCoverage}`,
    view.parentRequestId ? `Parent request: ${view.parentRequestId}` : '',
    `Summary: ${view.summary}`,
  ].filter((line) => line.length > 0);

  const requestEvent: ActivityTimelineEventView = {
    key: 'request',
    kind: 'request',
    atMs:
      finiteTimestamp(view.request.started_at_ms) ??
      finiteTimestamp(view.request.updated_at_ms),
    atValue:
      view.startedAtValue !== 'Not reported'
        ? view.startedAtValue
        : view.updatedAtValue,
    label: 'Request recorded',
    stateLabel: view.stateLabel,
    tone: view.stateTone,
    detail: `Operation: ${formatRecordedOperation(view.operation)}.`,
    lines: requestLines,
    note: '',
    stage: null,
  };

  const requesterEvidence = view.requesterLine.evidence ?? 'not reported';
  const requesterEvent: ActivityTimelineEventView = {
    key: 'requester',
    kind: 'requester',
    atMs: requestEvent.atMs,
    atValue: requestEvent.atValue,
    label: 'Requester',
    stateLabel: null,
    tone: 'neutral',
    detail: view.requesterLine.value,
    lines: [
      `Evidence: ${requesterEvidence}`,
      `Source: ${view.actorSource} · role ${view.actorRole}`,
      `Model: ${view.actorModel} · effort ${view.actorEffort}`,
    ],
    note: '',
    stage: null,
  };

  const deliveryEvents: ActivityTimelineEventView[] = view.deliveries.map(
    (delivery, index) => ({
      key: `delivery:${index}`,
      kind: 'delivery',
      atMs: delivery.atMs,
      atValue: delivery.atValue,
      label: delivery.label,
      stateLabel: null,
      tone: 'neutral',
      detail: `Recipient: ${delivery.recipient}`,
      lines: [delivery.summary, delivery.note].filter((line) => line.length > 0),
      note: '',
      stage: delivery.stage,
    })
  );

  const childEvents: ActivityTimelineEventView[] = view.children.map(
    (child, index) => ({
      key: `child:${index}:${child.call.id}`,
      kind: 'child',
      atMs:
        finiteTimestamp(child.call.started_at_ms) ??
        finiteTimestamp(child.call.finished_at_ms),
      atValue:
        child.startedAtValue !== 'Not reported'
          ? child.startedAtValue
          : child.finishedAtValue,
      label: 'Child call',
      stateLabel: child.stateLabel,
      tone: child.stateTone,
      detail: child.title,
      lines: [
        `Call: ${child.call.id}`,
        child.executorSummary,
        `Model: ${child.modelSummary}`,
        `Effort: ${child.effortSummary}`,
        child.durationSummary,
        child.usage.summary,
        child.summary,
      ].filter((line) => line.length > 0),
      note: '',
      stage: null,
    })
  );

  const ordered = [...deliveryEvents, ...childEvents]
    .map((event, index) => ({ event, index }))
    .sort((left, right) => {
      const leftMs = left.event.atMs;
      const rightMs = right.event.atMs;
      if (leftMs === null && rightMs === null) {
        return left.index - right.index;
      }
      if (leftMs === null) {
        return 1;
      }
      if (rightMs === null) {
        return -1;
      }
      if (leftMs !== rightMs) {
        return leftMs - rightMs;
      }
      return left.index - right.index;
    })
    .map((entry) => entry.event);

  return [requesterEvent, requestEvent, ...ordered];
}

export function buildActivityRequestView(
  snapshot: Pick<ObservationSnapshot, 'sources' | 'jobs'>,
  request: ObservationRequest,
  nowMs: number | null | undefined = null
): ActivityRequestView {
  const source =
    snapshot.sources.find((candidate) => candidate.id === request.source_id) ?? null;
  const jobResolution = resolveRequestJob(snapshot, request);
  const actor = request.actor ?? null;
  const executor = request.executor ?? null;
  const childCalls = request.children ?? [];
  const childrenReturned = childCalls.length;
  const childrenCount = nonNegativeInteger(
    request.children_count,
    childrenReturned
  );
  const calledExecutor =
    hasExecutorRecord(executor) && !isLocalMcpExecutor(executor)
      ? null
      : projectCalledExecutors(childCalls, childrenCount);
  const hasChildWork =
    childrenReturned > 0 || childrenCount > 0 || calledExecutor !== null;
  const usage = projectUsage(request.usage, executor, hasChildWork);
  const duration = projectDuration(request, nowMs);
  const delivery = sortDeliveryRecords(request.delivery);
  const deliveries = delivery.map((record) => buildDeliveryView(record));
  const latestDelivery = latestDeliveryRecord(delivery);
  const children = childCalls.map((call) => buildChildView(call, nowMs));
  const updatedAtValue = formatFeedTimestamp(request.updated_at_ms);
  const parentRequestId = trimOrNull(request.parent_request_id);
  const runningChildCount = childCalls.filter(
    (call) => call.state === 'running'
  ).length;
  // A synchronous root completion can still have verified child work in flight.
  const callsRunning = request.state === 'completed' && runningChildCount > 0;
  // The root lifecycle stays the recorded request state. Child progress is
  // surfaced separately and never changes the root spinner decision.
  const stateLabel = formatLifecycleLabel(request.state);
  const stateTone = resolveLifecycleTone(request.state);
  const recordedModel = projectRecordedModel(
    executor?.requested_model,
    executor?.observed_model
  );

  const view: ActivityRequestView = {
    key: requestIdentityKey(request.source_id, request.id),
    domKey: requestDomKey(request.source_id, request.id),
    request,
    source,
    job: jobResolution.job,
    sourceLabel: source ? trimOrNull(source.label) ?? source.id : 'Source details unavailable',
    sourceDetails: formatSourceDetails(source, request.source_id),
    projectLabel:
      trimOrNull(request.project_label) ??
      trimOrNull(jobResolution.job?.project_label) ??
      'Project not reported',
    jobLabel: jobResolution.label,
    jobLinked: jobResolution.linked,
    jobUnavailable: jobResolution.unavailable,
    title: trimOrNull(request.title) ?? 'Request title not reported',
    operation: trimOrNull(request.operation) ?? 'Operation not reported',
    state: request.state,
    stateLabel,
    stateTone,
    showSpinner: shouldShowSpinner(request.state),
    callsRunning,
    runningChildCount,
    meaningfulAtMs: requestMeaningfulTimestampMs(request),
    updatedAtSummary:
      updatedAtValue === 'Not reported'
        ? 'Updated time not reported'
        : `Updated ${updatedAtValue}`,
    updatedAtValue,
    startedAtValue: formatFeedTimestamp(request.started_at_ms),
    finishedAtValue: formatFeedTimestamp(request.finished_at_ms),
    requesterLabel: formatRequesterIdentity(actor),
    requesterEvidence: formatRequesterEvidence(actor?.evidence),
    requesterEvidenceKey: requesterEvidenceKey(actor?.evidence),
    requesterSummary: formatRequesterSummary(actor),
    requesterLine: projectRequesterLine(actor),
    actorRole: formatOptionalField(actor?.role),
    actorSource: formatOptionalField(actor?.source),
    actorModel: formatOptionalField(actor?.model),
    actorEffort: formatOptionalField(actor?.effort),
    conversationDetails: formatConversationDetails(request),
    executorIdentity: formatExecutorIdentity(executor),
    executorSummary: formatExecutorSummary(executor),
    executorTool: formatOptionalField(executor?.tool),
    executorProvider: formatOptionalField(executor?.provider),
    executorRole: formatOptionalField(executor?.role),
    executorRoleLabel: formatExecutorRoleLabel(executor?.role),
    requestedModel: formatOptionalField(executor?.requested_model),
    observedModel: formatOptionalField(executor?.observed_model),
    recordedModelValue: recordedModel.value,
    recordedModelDetail: recordedModel.detail,
    requestedEffort: formatOptionalField(executor?.requested_effort),
    observedEffort: formatOptionalField(executor?.observed_effort),
    modelSummary: formatRequestedObserved(
      executor?.requested_model,
      executor?.observed_model
    ),
    effortSummary: formatRequestedObserved(
      executor?.requested_effort,
      executor?.observed_effort
    ),
    calledExecutor,
    usage,
    duration,
    durationSummary: formatDurationSummary(duration),
    durationDetail: formatDurationDetail(duration),
    timeline: [],
    delivery,
    deliveries,
    deliveryLabel: summarizeDeliveryLabel(delivery),
    deliveryNote: summarizeDeliveryNote(delivery),
    deliveryAtValue: formatFeedTimestamp(latestDelivery?.at_ms),
    deliveryRecipient: summarizeDeliveryRecipients(delivery),
    deliverySummary: summarizeDeliverySummary(delivery),
    latestReturn: formatLatestProvenReturnStage(delivery),
    children,
    childrenReturned,
    childrenCount,
    childCoverage: formatChildCoverage(childrenReturned, childrenCount),
    parentRequestId,
    summary: compactFeedText(request.summary, 640) || 'Summary not reported.',
  };

  view.timeline = buildTimelineEvents(view);

  return view;
}

export function projectActivityFeed(
  snapshot: ObservationSnapshot,
  options: ActivityFeedProjectionOptions = {}
): ActivityFeedProjection {
  const scope = options.scope ?? null;
  const nowMs =
    finiteTimestamp(options.now_ms) ??
    finiteTimestamp(snapshot.generated_at_ms) ??
    null;
  const byIdentity = new Map<string, ObservationRequest>();

  for (const request of snapshot.requests ?? []) {
    const key = requestIdentityKey(request.source_id, request.id);
    const existing = byIdentity.get(key);
    if (!existing) {
      byIdentity.set(key, request);
      continue;
    }
    const existingMs = requestMeaningfulTimestampMs(existing) ?? -1;
    const nextMs = requestMeaningfulTimestampMs(request) ?? -1;
    if (nextMs > existingMs) {
      byIdentity.set(key, request);
    }
  }

  const uniqueRequests = Array.from(byIdentity.values());
  const scopedRequests = scope
    ? uniqueRequests.filter(
        (request) =>
          request.source_id === scope.source_id && request.job_id === scope.job_id
      )
    : uniqueRequests;

  const rows = scopedRequests
    .map((request) => buildActivityRequestView(snapshot, request, nowMs))
    .sort((left, right) => {
      const leftMs = left.meaningfulAtMs;
      const rightMs = right.meaningfulAtMs;
      if (leftMs !== rightMs) {
        if (leftMs === null) {
          return 1;
        }
        if (rightMs === null) {
          return -1;
        }
        return rightMs - leftMs;
      }
      const source = compareText(left.request.source_id, right.request.source_id);
      if (source !== 0) {
        return source;
      }
      return compareText(left.request.id, right.request.id);
    });

  const unavailableSources = (snapshot.sources ?? [])
    .filter((source) => source.availability === 'unavailable')
    .map((source) => ({
      source,
      label: trimOrNull(source.label) ?? source.id,
      message: trimOrNull(source.message) ?? 'No details provided.',
    }));

  const warnings = (snapshot.warnings ?? [])
    .map((warning) => compactFeedText(warning, 320))
    .filter((warning) => warning.length > 0);

  const generatedAtMs = finiteTimestamp(snapshot.generated_at_ms);

  return {
    scope: describeActivityScope(snapshot, scope),
    rows,
    totalRecorded: uniqueRequests.length,
    filteredOutCount: uniqueRequests.length - rows.length,
    hasMore: snapshot.has_more === true,
    jobsHasMore: snapshot.jobs_has_more === true,
    warnings,
    unavailableSources,
    generatedAtMs,
    snapshotTimeLabel:
      generatedAtMs === null
        ? 'Snapshot time not reported'
        : `Snapshot ${formatFeedTimestamp(generatedAtMs)}`,
    nextCursor: trimOrNull(snapshot.next_cursor),
  };
}
