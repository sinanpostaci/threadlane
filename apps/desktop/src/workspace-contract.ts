import {
  DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
  MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW,
  MAX_OBSERVATION_JOBS,
  MAX_OBSERVATION_REQUEST_PAGE_LIMIT,
  MAX_OBSERVATION_SUMMARY_CHARS,
  MAX_WORKSPACE_ATTEMPTS_PER_TASK,
  MAX_WORKSPACE_CANDIDATES_PER_TASK,
  MAX_WORKSPACE_CANONICAL_PATH_BYTES,
  MAX_WORKSPACE_CHAT_WORK_EVENTS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_EVENTS_TOTAL,
  MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_ITEMS_TOTAL,
  MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_PRESENCE_TOTAL,
  MAX_WORKSPACE_BACKLOG_PRESENCE_SAMPLE_AGE_MS,
  MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB,
  MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
  MAX_WORKSPACE_JOBS_PER_BINDING,
  MAX_WORKSPACE_LABEL_BYTES,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE,
  MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
  MAX_WORKSPACE_OUTBOX_PER_JOB,
  MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  MAX_WORKSPACE_RUNS_PER_JOB,
  MAX_WORKSPACE_TASKS_PER_JOB,
  OBSERVATION_SCHEMA_VERSION,
  WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
  WORKSPACE_METADATA_TIMEOUT_MS,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  WORKSPACE_SAMPLING_INTERVAL_MS,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
} from './observation/types.ts';

import type {
  BacklogItem,
  ObservationDeliveryStage,
  ObservationExecutor,
  ObservationJob,
  ObservationJobState,
  ObservationProcessInstance,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationWarningContext,
  WorkspaceAcceptanceProof,
  WorkspaceAcceptanceStatus,
  WorkspaceActivityState,
  WorkspaceAttentionKind,
  WorkspaceAttentionState,
  WorkspaceAvailabilityState,
  WorkspaceBacklogEvent,
  WorkspaceBacklogItemKind,
  WorkspaceBacklogOutcome,
  WorkspaceBacklogPresenceAvailability,
  WorkspaceBacklogPresenceFreshness,
  WorkspaceBacklogStatus,
  WorkspaceBacklogWorkerPresence,
  WorkspaceCapabilityState,
  WorkspaceChatIdentity,
  WorkspaceChatSummary,
  WorkspaceChatWork,
  WorkspaceChatWorkBounds,
  WorkspaceCombinedCandidate,
  WorkspaceConfiguredSelection,
  WorkspaceConnectionState,
  WorkspaceCoveredAttention,
  WorkspaceEvaluatedNativeSample,
  WorkspaceExternalResolution,
  WorkspaceExternalResolutionApplicability,
  WorkspaceExternalResolutionOutcome,
  WorkspaceExternalResolutionScope,
  WorkspaceFreshnessState,
  WorkspaceHistoricalFailure,
  WorkspaceJobHistory,
  WorkspaceJobHistoryTruncation,
  WorkspaceJobResult,
  WorkspaceJobSummary,
  WorkspaceMessageCompleteness,
  WorkspaceMessageItem,
  WorkspaceMessageScope,
  WorkspaceMessageSourceKind,
  WorkspaceMessageSourceOrder,
  WorkspaceMessagesWindow,
  WorkspaceNativeCapabilities,
  WorkspaceNativeSample,
  WorkspaceOutboxRecord,
  WorkspaceRecordLinks,
  WorkspaceRunRecord,
  WorkspaceSafeMessageSourceKind,
  WorkspaceSampleConnection,
  WorkspaceSamplingPolicy,
  WorkspaceSnapshot,
  WorkspaceSnapshotBounds,
  WorkspaceTaskAttempt,
  WorkspaceTaskCandidate,
  WorkspaceTaskHistory,
} from './observation/types.ts';

export {
  DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
  MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW,
  MAX_OBSERVATION_JOBS,
  MAX_OBSERVATION_REQUEST_PAGE_LIMIT,
  MAX_OBSERVATION_SUMMARY_CHARS,
  MAX_WORKSPACE_ATTEMPTS_PER_TASK,
  MAX_WORKSPACE_CANDIDATES_PER_TASK,
  MAX_WORKSPACE_CANONICAL_PATH_BYTES,
  MAX_WORKSPACE_CHAT_WORK_EVENTS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_EVENTS_TOTAL,
  MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_ITEMS_TOTAL,
  MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT,
  MAX_WORKSPACE_CHAT_WORK_PRESENCE_TOTAL,
  MAX_WORKSPACE_BACKLOG_PRESENCE_SAMPLE_AGE_MS,
  MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB,
  MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
  MAX_WORKSPACE_JOBS_PER_BINDING,
  MAX_WORKSPACE_LABEL_BYTES,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE,
  MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES,
  MAX_WORKSPACE_OUTBOX_PER_JOB,
  MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
  MAX_WORKSPACE_RUNS_PER_JOB,
  MAX_WORKSPACE_TASKS_PER_JOB,
  OBSERVATION_SCHEMA_VERSION,
  WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
  WORKSPACE_METADATA_TIMEOUT_MS,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  WORKSPACE_SAMPLING_INTERVAL_MS,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
};

export type {
  BacklogItem,
  ObservationProcessInstance,
  WorkspaceAcceptanceProof,
  WorkspaceAcceptanceStatus,
  WorkspaceActivityState,
  WorkspaceAttentionKind,
  WorkspaceAttentionState,
  WorkspaceAvailabilityState,
  WorkspaceBacklogEvent,
  WorkspaceBacklogItemKind,
  WorkspaceBacklogOutcome,
  WorkspaceBacklogPresenceAvailability,
  WorkspaceBacklogPresenceFreshness,
  WorkspaceBacklogStatus,
  WorkspaceBacklogWorkerPresence,
  WorkspaceCapabilityState,
  WorkspaceChatIdentity,
  WorkspaceChatSummary,
  WorkspaceChatWork,
  WorkspaceChatWorkBounds,
  WorkspaceCombinedCandidate,
  WorkspaceConfiguredSelection,
  WorkspaceConnectionState,
  WorkspaceCoveredAttention,
  WorkspaceEvaluatedNativeSample,
  WorkspaceExternalResolution,
  WorkspaceExternalResolutionApplicability,
  WorkspaceExternalResolutionOutcome,
  WorkspaceExternalResolutionScope,
  WorkspaceFreshnessState,
  WorkspaceHistoricalFailure,
  WorkspaceJobHistory,
  WorkspaceJobHistoryTruncation,
  WorkspaceJobResult,
  WorkspaceJobSummary,
  WorkspaceMessageCompleteness,
  WorkspaceMessageItem,
  WorkspaceMessageScope,
  WorkspaceMessageSourceKind,
  WorkspaceMessageSourceOrder,
  WorkspaceMessagesWindow,
  WorkspaceNativeCapabilities,
  WorkspaceNativeSample,
  WorkspaceOutboxRecord,
  WorkspaceRecordLinks,
  WorkspaceRunRecord,
  WorkspaceSafeMessageSourceKind,
  WorkspaceSampleConnection,
  WorkspaceSamplingPolicy,
  WorkspaceSnapshot,
  WorkspaceSnapshotBounds,
  WorkspaceTaskAttempt,
  WorkspaceTaskCandidate,
  WorkspaceTaskHistory,
};

// ---------------------------------------------------------------------------
// Shared Frontend Surface, Per-Chat Archive, and Compact Native Tray Contracts
// ---------------------------------------------------------------------------

/** Chat-first workspace tabs with `messages` pinned as default and `jobs` separated from `activity`. */
export const WORKSPACE_CHAT_SURFACE_TABS = ['messages', 'jobs', 'activity'] as const;
export type WorkspaceChatSurfaceTab = (typeof WORKSPACE_CHAT_SURFACE_TABS)[number];
export const DEFAULT_WORKSPACE_CHAT_TAB: WorkspaceChatSurfaceTab = 'messages';

export interface WorkspaceSurfaceSeparationContract {
  readonly default_chat_tab: 'messages';
  readonly messages_surface: {
    readonly kind: 'safe_summaries_results_questions';
    readonly requires_transcript: false;
    readonly allows_synthetic_user_messages: false;
    readonly max_items: typeof MAX_WORKSPACE_MESSAGES_WINDOW;
    readonly max_summary_chars: typeof MAX_OBSERVATION_SUMMARY_CHARS;
  };
  readonly jobs_surface: {
    readonly kind: 'durable_job_task_attempt_candidate_ledger';
    readonly max_jobs_total: typeof MAX_OBSERVATION_JOBS;
    readonly max_jobs_per_binding: typeof MAX_WORKSPACE_JOBS_PER_BINDING;
    readonly max_tasks_per_job: typeof MAX_WORKSPACE_TASKS_PER_JOB;
    readonly max_attempts_per_task: typeof MAX_WORKSPACE_ATTEMPTS_PER_TASK;
    readonly max_candidates_per_task: typeof MAX_WORKSPACE_CANDIDATES_PER_TASK;
    readonly max_runs_per_job: typeof MAX_WORKSPACE_RUNS_PER_JOB;
    readonly max_outbox_per_job: typeof MAX_WORKSPACE_OUTBOX_PER_JOB;
    readonly max_combined_candidates_per_job: typeof MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB;
  };
  readonly activity_surface: {
    readonly kind: 'paginated_request_call_delivery_audit_feed';
    readonly default_page_limit: typeof DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT;
    readonly max_page_limit: typeof MAX_OBSERVATION_REQUEST_PAGE_LIMIT;
    readonly max_frontend_window_requests: typeof MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW;
    readonly max_summary_chars: typeof MAX_OBSERVATION_SUMMARY_CHARS;
  };
}

export const WORKSPACE_SURFACE_SEPARATION: WorkspaceSurfaceSeparationContract = {
  default_chat_tab: DEFAULT_WORKSPACE_CHAT_TAB,
  messages_surface: {
    kind: 'safe_summaries_results_questions',
    requires_transcript: false,
    allows_synthetic_user_messages: false,
    max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
    max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
  },
  jobs_surface: {
    kind: 'durable_job_task_attempt_candidate_ledger',
    max_jobs_total: MAX_OBSERVATION_JOBS,
    max_jobs_per_binding: MAX_WORKSPACE_JOBS_PER_BINDING,
    max_tasks_per_job: MAX_WORKSPACE_TASKS_PER_JOB,
    max_attempts_per_task: MAX_WORKSPACE_ATTEMPTS_PER_TASK,
    max_candidates_per_task: MAX_WORKSPACE_CANDIDATES_PER_TASK,
    max_runs_per_job: MAX_WORKSPACE_RUNS_PER_JOB,
    max_outbox_per_job: MAX_WORKSPACE_OUTBOX_PER_JOB,
    max_combined_candidates_per_job: MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB,
  },
  activity_surface: {
    kind: 'paginated_request_call_delivery_audit_feed',
    default_page_limit: DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
    max_page_limit: MAX_OBSERVATION_REQUEST_PAGE_LIMIT,
    max_frontend_window_requests: MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW,
    max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
  },
};

export function normalizeWorkspaceChatTab(value: unknown): WorkspaceChatSurfaceTab {
  if (value === 'messages' || value === 'jobs' || value === 'activity') {
    return value;
  }
  return DEFAULT_WORKSPACE_CHAT_TAB;
}

/**
 * Ledger-independent desktop boundary contract (`workspace_external_resolution_view_v1`)
 * for projecting backend-verified external resolution receipts across Messages, Jobs,
 * Activity, app-model, and compact native tray surfaces.
 */
export const WORKSPACE_EXTERNAL_RESOLUTION_OUTCOMES: readonly WorkspaceExternalResolutionOutcome[] =
  ['resolved_externally'] as const;
export const WORKSPACE_EXTERNAL_RESOLUTION_SCOPES: readonly WorkspaceExternalResolutionScope[] = [
  'job',
  'tasks',
] as const;
export const WORKSPACE_EXTERNAL_RESOLUTION_APPLICABILITIES: readonly WorkspaceExternalResolutionApplicability[] =
  ['applicable', 'stale', 'unverified'] as const;

/**
 * Maximum covered-attention rows retained per external resolution (`65`): one explicit
 * job-level row plus up to `MAX_WORKSPACE_TASKS_PER_JOB` task rows.
 */
export const MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION =
  MAX_WORKSPACE_TASKS_PER_JOB + 1;

export interface WorkspaceExternalResolutionViewContract {
  readonly version: typeof WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION;
  readonly max_external_resolutions_per_job: typeof MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB;
  readonly max_task_ids_per_resolution: typeof MAX_WORKSPACE_TASKS_PER_JOB;
  readonly max_evidence_refs_per_resolution: typeof MAX_WORKSPACE_RECORD_LINKS_PER_LIST;
  readonly max_covered_attention_per_resolution:
    typeof MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION;
  readonly max_summary_chars: typeof MAX_OBSERVATION_SUMMARY_CHARS;
  readonly max_id_bytes: typeof MAX_WORKSPACE_LABEL_BYTES;
  readonly requires_store_verified_applicability: true;
  readonly forbids_frontend_proof_synthesis: true;
  readonly forbids_synthetic_provider_calls: true;
  readonly forbids_synthetic_user_messages: true;
  readonly forbids_synthetic_delivery_or_closure_events: true;
  readonly preserves_covered_attention_rows: true;
  readonly full_job_label: 'Resolved externally';
  readonly partial_tasks_label: 'Partially resolved externally';
  readonly stale_receipt_label: 'Stale external receipt';
  readonly unverified_receipt_label: 'Unverified external receipt';
}

export const WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT: WorkspaceExternalResolutionViewContract =
  {
    version: WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION,
    max_external_resolutions_per_job: MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB,
    max_task_ids_per_resolution: MAX_WORKSPACE_TASKS_PER_JOB,
    max_evidence_refs_per_resolution: MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
    max_covered_attention_per_resolution: MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION,
    max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
    max_id_bytes: MAX_WORKSPACE_LABEL_BYTES,
    requires_store_verified_applicability: true,
    forbids_frontend_proof_synthesis: true,
    forbids_synthetic_provider_calls: true,
    forbids_synthetic_user_messages: true,
    forbids_synthetic_delivery_or_closure_events: true,
    preserves_covered_attention_rows: true,
    full_job_label: 'Resolved externally',
    partial_tasks_label: 'Partially resolved externally',
    stale_receipt_label: 'Stale external receipt',
    unverified_receipt_label: 'Unverified external receipt',
  };

/** Per-chat Active / Archive lifecycle preferences keyed strictly by canonical `WorkspaceChatIdentity`. */
export const WORKSPACE_CHAT_LIFECYCLE_FILTERS = ['active', 'archived'] as const;
export type WorkspaceChatLifecycle = (typeof WORKSPACE_CHAT_LIFECYCLE_FILTERS)[number];
export const DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER: WorkspaceChatLifecycle = 'active';
export const WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY =
  'threadlane.desktop.workspace.chatArchive.v1' as const;
export const MAX_WORKSPACE_ARCHIVED_CHAT_PREFERENCES = 512 as const;

export interface WorkspaceChatArchiveEntry {
  chat_key: string;
  identity: WorkspaceChatIdentity;
  lifecycle: WorkspaceChatLifecycle;
  updated_at_ms: number;
}

export type WorkspaceChatArchivePreferenceMap = Readonly<Record<string, WorkspaceChatArchiveEntry>>;

export interface WorkspaceChatPreferenceStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

/** Compact native tray content bounds and actions. */
export const WORKSPACE_TRAY_ROW_KINDS = ['needs_human', 'working', 'result'] as const;
export type WorkspaceTrayRowKind = (typeof WORKSPACE_TRAY_ROW_KINDS)[number];

export const WORKSPACE_TRAY_ACTIONS = ['show_main', 'hide', 'quit'] as const;
export type WorkspaceTrayAction = (typeof WORKSPACE_TRAY_ACTIONS)[number];

export const WORKSPACE_MAIN_CLOSE_BEHAVIORS = ['hide_to_tray', 'quit'] as const;
export type WorkspaceMainCloseBehavior = (typeof WORKSPACE_MAIN_CLOSE_BEHAVIORS)[number];

export const WORKSPACE_TRAY_MAX_ROWS = 3 as const;
export const WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS = 1 as const;
export const WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT = 50 as const;
export const WORKSPACE_TRAY_REFRESH_INTERVAL_MS = 4_000 as const;
export const WORKSPACE_TRAY_MAX_TITLE_CHARS = 80 as const;
export const WORKSPACE_TRAY_MAX_DETAIL_CHARS = 160 as const;
export const WORKSPACE_TRAY_MAX_ERROR_CHARS = 200 as const;

export interface WorkspaceTrayContentBounds {
  readonly max_rows: typeof WORKSPACE_TRAY_MAX_ROWS;
  readonly max_terminal_result_rows: typeof WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS;
  readonly snapshot_page_limit: typeof WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT;
  readonly refresh_interval_ms: typeof WORKSPACE_TRAY_REFRESH_INTERVAL_MS;
  readonly max_title_chars: typeof WORKSPACE_TRAY_MAX_TITLE_CHARS;
  readonly max_detail_chars: typeof WORKSPACE_TRAY_MAX_DETAIL_CHARS;
  readonly max_error_chars: typeof WORKSPACE_TRAY_MAX_ERROR_CHARS;
  readonly priority_order: readonly ['needs_human', 'working', 'result'];
  readonly allowed_actions: readonly ['show_main', 'hide', 'quit'];
  readonly allowed_close_behaviors: readonly ['hide_to_tray', 'quit'];
}

export const WORKSPACE_TRAY_CONTENT_BOUNDS: WorkspaceTrayContentBounds = {
  max_rows: WORKSPACE_TRAY_MAX_ROWS,
  max_terminal_result_rows: WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS,
  snapshot_page_limit: WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
  refresh_interval_ms: WORKSPACE_TRAY_REFRESH_INTERVAL_MS,
  max_title_chars: WORKSPACE_TRAY_MAX_TITLE_CHARS,
  max_detail_chars: WORKSPACE_TRAY_MAX_DETAIL_CHARS,
  max_error_chars: WORKSPACE_TRAY_MAX_ERROR_CHARS,
  priority_order: WORKSPACE_TRAY_ROW_KINDS,
  allowed_actions: WORKSPACE_TRAY_ACTIONS,
  allowed_close_behaviors: WORKSPACE_MAIN_CLOSE_BEHAVIORS,
};

export interface WorkspaceCompactTrayRow {
  id: string;
  kind: WorkspaceTrayRowKind;
  title: string;
  detail: string;
  updated_at_ms: number | null;
  chat_identity: WorkspaceChatIdentity | null;
  job_id: string | null;
}

// ---------------------------------------------------------------------------
// Errors & Wire Constants
// ---------------------------------------------------------------------------

export type WorkspaceContractErrorCode =
  | 'invalid_chat_identity'
  | 'invalid_native_sample'
  | 'sidecar_too_large'
  | 'forbidden_message_source'
  | 'invalid_json';

export class WorkspaceContractError extends Error {
  readonly code: WorkspaceContractErrorCode;
  readonly field: string | null;

  constructor(code: WorkspaceContractErrorCode, message: string, field: string | null = null) {
    super(message);
    this.name = 'WorkspaceContractError';
    this.code = code;
    this.field = field;
  }
}

export const DEFAULT_WORKSPACE_SAMPLING_POLICY: Readonly<WorkspaceSamplingPolicy> = {
  interval_ms: WORKSPACE_SAMPLING_INTERVAL_MS,
  metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
  stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  snapshot_resets_sample_age: false,
};

export const WORKSPACE_SAFE_MESSAGE_SOURCE_KINDS: readonly WorkspaceSafeMessageSourceKind[] = [
  'work_brief_summary',
  'plan_summary',
  'question_prompt',
  'question_answer',
  'task_result_summary',
  'review_verdict_summary',
  'closure_summary',
  'job_result_summary',
  'attention_summary',
  'journal_summary',
] as const;

const SAFE_MESSAGE_SOURCE_SET: ReadonlySet<string> = new Set(WORKSPACE_SAFE_MESSAGE_SOURCE_KINDS);
const NIL_UUID_HYPHENATED = '00000000-0000-0000-0000-000000000000';
const UTF8_ENCODER = new TextEncoder();
const UTF8_DECODER = new TextDecoder('utf-8', { fatal: true });

// ---------------------------------------------------------------------------
// UTF-8, Unicode, and Secret-Safe Sanitization Helpers
// ---------------------------------------------------------------------------

export function utf8ByteLength(value: string): number {
  return UTF8_ENCODER.encode(value).byteLength;
}

function compareStringsUtf8(left: string, right: string): number {
  if (left === right) {
    return 0;
  }
  const leftBytes = UTF8_ENCODER.encode(left);
  const rightBytes = UTF8_ENCODER.encode(right);
  const minLen = Math.min(leftBytes.byteLength, rightBytes.byteLength);
  for (let i = 0; i < minLen; i += 1) {
    const diff = (leftBytes[i] ?? 0) - (rightBytes[i] ?? 0);
    if (diff !== 0) {
      return diff;
    }
  }
  return leftBytes.byteLength - rightBytes.byteLength;
}

function isUnicodeControlCodePoint(cp: number): boolean {
  return (cp >= 0x00 && cp <= 0x1f) || (cp >= 0x7f && cp <= 0x9f);
}

function containsSkPrefixMarker(lower: string): boolean {
  return /(?:^|[^a-z0-9])sk-/.test(lower);
}

function containsSensitiveMarker(lower: string): boolean {
  return (
    containsSkPrefixMarker(lower) ||
    lower.includes('bearer') ||
    lower.includes('api_key') ||
    lower.includes('apikey') ||
    lower.includes('authorization') ||
    lower.includes('secret') ||
    lower.includes('password') ||
    lower.includes('token') ||
    lower.includes('prompt') ||
    lower.includes('instruction')
  );
}

export function validateCorrelationId(raw: unknown): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  const trimmed = raw.trim();
  if (trimmed.length === 0 || trimmed.length > MAX_WORKSPACE_LABEL_BYTES) {
    return null;
  }
  if (!/^[A-Za-z0-9_.:-]+$/.test(trimmed)) {
    return null;
  }
  if (containsSensitiveMarker(trimmed.toLowerCase())) {
    return null;
  }
  return trimmed;
}

function sanitizeStatusToken(raw: unknown): string {
  if (typeof raw !== 'string') {
    return 'unknown';
  }
  const trimmed = raw.trim();
  if (
    trimmed.length === 0 ||
    trimmed.length > 64 ||
    !/^[A-Za-z0-9_.-]+$/.test(trimmed) ||
    containsSensitiveMarker(trimmed.toLowerCase())
  ) {
    return 'unknown';
  }
  return trimmed;
}

export function sanitizeWorkspaceLabel(raw: unknown, fallback: string): string {
  if (typeof raw !== 'string') {
    return fallback;
  }
  let out = '';
  let usedBytes = 0;
  for (const ch of raw) {
    const cp = ch.codePointAt(0) ?? 0;
    if (isUnicodeControlCodePoint(cp)) {
      continue;
    }
    const chBytes = utf8ByteLength(ch);
    if (usedBytes + chBytes <= MAX_WORKSPACE_LABEL_BYTES) {
      out += ch;
      usedBytes += chBytes;
    }
  }
  const trimmed = out.trim();
  return trimmed.length === 0 ? fallback : trimmed;
}

export function sanitizeWorkspaceSelectionToken(raw: unknown): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  let out = '';
  let usedBytes = 0;
  for (const ch of raw) {
    const cp = ch.codePointAt(0) ?? 0;
    if (isUnicodeControlCodePoint(cp)) {
      continue;
    }
    const chBytes = utf8ByteLength(ch);
    if (usedBytes + chBytes <= MAX_WORKSPACE_LABEL_BYTES) {
      out += ch;
      usedBytes += chBytes;
    }
  }
  const trimmed = out.trim();
  if (trimmed.length === 0 || containsSensitiveMarker(trimmed.toLowerCase())) {
    return null;
  }
  return trimmed;
}

export interface WorkspaceSanitizedSummary {
  summary: string;
  truncated: boolean;
  original_char_count: number | null;
  redacted: boolean;
}

export function sanitizeWorkspaceSummaryWithMeta(raw: unknown): WorkspaceSanitizedSummary {
  const text = typeof raw === 'string' ? raw : '';
  const mappedChars: string[] = [];
  for (const ch of text) {
    const cp = ch.codePointAt(0) ?? 0;
    mappedChars.push(isUnicodeControlCodePoint(cp) ? ' ' : ch);
  }
  const collapsed = mappedChars.join('').trim().replace(/\s+/gu, ' ');
  const lower = collapsed.toLowerCase();
  if (
    containsSkPrefixMarker(lower) ||
    lower.includes('bearer ') ||
    lower.includes('api_key') ||
    lower.includes('apikey') ||
    lower.includes('authorization:') ||
    lower.includes('secret') ||
    lower.includes('password')
  ) {
    return {
      summary: '[redacted sensitive metadata]',
      truncated: false,
      original_char_count: null,
      redacted: true,
    };
  }

  const codePoints = Array.from(collapsed);
  const charCount = codePoints.length;
  if (charCount <= MAX_OBSERVATION_SUMMARY_CHARS) {
    return {
      summary: collapsed,
      truncated: false,
      original_char_count: charCount,
      redacted: false,
    };
  }

  const keep = Math.max(0, MAX_OBSERVATION_SUMMARY_CHARS - 3);
  return {
    summary: `${codePoints.slice(0, keep).join('')}...`,
    truncated: true,
    original_char_count: charCount,
    redacted: false,
  };
}

export function sanitizeWorkspaceSummary(raw: unknown): string {
  return sanitizeWorkspaceSummaryWithMeta(raw).summary;
}

function truncateUnicodeChars(raw: string, maxChars: number): string {
  const sanitized = sanitizeWorkspaceSummary(raw);
  const codePoints = Array.from(sanitized);
  if (codePoints.length <= maxChars) {
    return sanitized;
  }
  if (maxChars <= 3) {
    return codePoints.slice(0, Math.max(0, maxChars)).join('');
  }
  return `${codePoints.slice(0, maxChars - 3).join('')}...`;
}

function cleanOptionalSummary(raw: unknown): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  const cleaned = sanitizeWorkspaceSummary(raw);
  return cleaned.length > 0 ? cleaned : null;
}

function cleanPositiveIntOrNull(raw: unknown): number | null {
  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    return null;
  }
  const floored = Math.trunc(raw);
  return floored > 0 ? floored : null;
}

function cleanNonNegativeInt(raw: unknown, fallback = 0): number {
  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    return fallback;
  }
  const floored = Math.trunc(raw);
  return floored >= 0 ? floored : fallback;
}

function cleanNonNegativeIntOrNull(raw: unknown): number | null {
  if (typeof raw !== 'number' || !Number.isFinite(raw)) {
    return null;
  }
  const floored = Math.trunc(raw);
  return floored >= 0 ? floored : null;
}

function cleanIdList(raw: unknown, maxItems: number): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  const out: string[] = [];
  for (const item of raw) {
    if (out.length >= maxItems) {
      break;
    }
    const clean = validateCorrelationId(item);
    if (clean !== null) {
      out.push(clean);
    }
  }
  return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function capTail<T>(items: T[], maxLen: number): { items: T[]; omitted: number } {
  if (items.length <= maxLen) {
    return { items, omitted: 0 };
  }
  const omitted = items.length - maxLen;
  return { items: items.slice(omitted), omitted };
}

// ---------------------------------------------------------------------------
// Authoritative Chat Identity (`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`)
// ---------------------------------------------------------------------------

function isForbiddenChatIdentitySubstituteToken(raw: string): boolean {
  const lower = raw.toLowerCase();
  return (
    lower.startsWith('job_') ||
    lower.startsWith('job-') ||
    lower.startsWith('job:') ||
    lower.startsWith('plan_') ||
    lower.startsWith('plan-') ||
    lower.startsWith('plan:') ||
    lower.startsWith('sess_') ||
    lower.startsWith('sess-') ||
    lower.startsWith('sess:') ||
    lower.startsWith('session_') ||
    lower.startsWith('session-') ||
    lower.startsWith('session:') ||
    lower.startsWith('sessionid')
  );
}

function validateWorkspaceSourceId(raw: unknown): string {
  if (
    typeof raw !== 'string' ||
    raw.length === 0 ||
    utf8ByteLength(raw) > MAX_WORKSPACE_LABEL_BYTES ||
    raw.trim() !== raw
  ) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'source_id must be a non-empty trimmed identifier up to 128 bytes',
      'source_id',
    );
  }
  if (!/^[A-Za-z0-9_.-]+$/.test(raw)) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'source_id must contain only ASCII [A-Za-z0-9_.-] characters',
      'source_id',
    );
  }
  if (containsSensitiveMarker(raw.toLowerCase())) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'source_id must not contain sensitive markers',
      'source_id',
    );
  }
  if (isForbiddenChatIdentitySubstituteToken(raw)) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'job_id, plan_id, or sessionId cannot replace source_id',
      'source_id',
    );
  }
  return raw;
}

function validateWorkspaceBindingId(raw: unknown): string {
  if (typeof raw !== 'string') {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'binding_id must be a string',
      'binding_id',
    );
  }
  const clean = validateCorrelationId(raw);
  if (clean === null) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'binding_id must be a valid non-empty ASCII identifier up to 128 bytes',
      'binding_id',
    );
  }
  if (clean !== raw) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'binding_id must not contain leading or trailing whitespace',
      'binding_id',
    );
  }
  if (isForbiddenChatIdentitySubstituteToken(clean)) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'job_id, plan_id, or sessionId cannot replace binding_id',
      'binding_id',
    );
  }
  return clean;
}

function isCanonicalLowercaseNonNilUuid(raw: string): boolean {
  if (raw.length !== 36 || raw === NIL_UUID_HYPHENATED) {
    return false;
  }
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(raw);
}

function validateWorkspaceThreadUuid(raw: unknown): string {
  if (typeof raw !== 'string' || !isCanonicalLowercaseNonNilUuid(raw)) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'thread_uuid must be a canonical lowercase hyphenated non-nil UUID',
      'thread_uuid',
    );
  }
  return raw;
}

function validateCanonicalProjectScope(raw: unknown): string {
  if (typeof raw !== 'string' || raw.length === 0 || raw.trim() !== raw || !raw.startsWith('/')) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'canonical_project_scope must be a trimmed absolute path up to 512 UTF-8 bytes',
      'canonical_project_scope',
    );
  }
  if (utf8ByteLength(raw) > MAX_WORKSPACE_CANONICAL_PATH_BYTES) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'canonical_project_scope exceeds 512 UTF-8 bytes',
      'canonical_project_scope',
    );
  }
  for (const ch of raw) {
    if (isUnicodeControlCodePoint(ch.codePointAt(0) ?? 0)) {
      throw new WorkspaceContractError(
        'invalid_chat_identity',
        'canonical_project_scope must not contain control characters',
        'canonical_project_scope',
      );
    }
  }
  if (
    raw.includes('//') ||
    raw === '/.' ||
    raw === '/..' ||
    raw.includes('/./') ||
    raw.includes('/../') ||
    raw.endsWith('/.') ||
    raw.endsWith('/..') ||
    (raw.length > 1 && raw.endsWith('/'))
  ) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      "canonical_project_scope must not contain '//', '.', '..', or trailing slashes",
      'canonical_project_scope',
    );
  }
  if (containsSkPrefixMarker(raw.toLowerCase())) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'canonical_project_scope must not contain secret-like tokens',
      'canonical_project_scope',
    );
  }
  return raw;
}

export function validateWorkspaceChatIdentity(input: unknown): WorkspaceChatIdentity {
  if (!isRecord(input)) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'chat identity must be an object with source_id, binding_id, thread_uuid, and canonical_project_scope',
    );
  }
  return {
    source_id: validateWorkspaceSourceId(input.source_id),
    binding_id: validateWorkspaceBindingId(input.binding_id),
    thread_uuid: validateWorkspaceThreadUuid(input.thread_uuid),
    canonical_project_scope: validateCanonicalProjectScope(input.canonical_project_scope),
  };
}

export function isValidWorkspaceChatIdentity(input: unknown): input is WorkspaceChatIdentity {
  try {
    validateWorkspaceChatIdentity(input);
    return true;
  } catch {
    return false;
  }
}

export function formatWorkspaceChatIdentityKey(identity: WorkspaceChatIdentity): string | null {
  try {
    const valid = validateWorkspaceChatIdentity(identity);
    return [
      'wchat:v1',
      `${utf8ByteLength(valid.source_id)}:${valid.source_id}`,
      `${utf8ByteLength(valid.binding_id)}:${valid.binding_id}`,
      `${utf8ByteLength(valid.thread_uuid)}:${valid.thread_uuid}`,
      `${utf8ByteLength(valid.canonical_project_scope)}:${valid.canonical_project_scope}`,
    ].join(':');
  } catch {
    return null;
  }
}

export function workspaceChatIdentitiesEqual(
  left: WorkspaceChatIdentity,
  right: WorkspaceChatIdentity,
): boolean {
  return (
    left.source_id === right.source_id &&
    left.binding_id === right.binding_id &&
    left.thread_uuid === right.thread_uuid &&
    left.canonical_project_scope === right.canonical_project_scope
  );
}

// ---------------------------------------------------------------------------
// Sampling Policy, Availability/Activity Separation, and Native Samples
// ---------------------------------------------------------------------------

function parseAvailabilityState(raw: unknown): WorkspaceAvailabilityState {
  if (
    raw === 'available' ||
    raw === 'disconnected' ||
    raw === 'stale' ||
    raw === 'unavailable'
  ) {
    return raw;
  }
  return 'unavailable';
}

function parseActivityState(raw: unknown): WorkspaceActivityState {
  if (
    raw === 'working' ||
    raw === 'idle' ||
    raw === 'waiting_for_approval' ||
    raw === 'waiting_for_input' ||
    raw === 'unknown'
  ) {
    return raw;
  }
  return 'unknown';
}

function parseConnectionState(raw: unknown): WorkspaceConnectionState {
  if (
    raw === 'connected' ||
    raw === 'disconnected' ||
    raw === 'degraded' ||
    raw === 'unavailable'
  ) {
    return raw;
  }
  return 'unavailable';
}

function parseFreshnessState(raw: unknown): WorkspaceFreshnessState {
  if (raw === 'fresh' || raw === 'stale' || raw === 'never_succeeded' || raw === 'unknown') {
    return raw;
  }
  return 'unknown';
}

function parseCapabilityState(raw: unknown): WorkspaceCapabilityState {
  if (raw === 'supported' || raw === 'unsupported' || raw === 'unknown') {
    return raw;
  }
  return 'unknown';
}

export function normalizeWorkspaceSamplingPolicy(raw: unknown): WorkspaceSamplingPolicy {
  const obj = isRecord(raw) ? raw : {};
  const intervalMs = cleanPositiveIntOrNull(obj.interval_ms) ?? WORKSPACE_SAMPLING_INTERVAL_MS;
  const timeoutMs =
    cleanPositiveIntOrNull(obj.metadata_timeout_ms) ?? WORKSPACE_METADATA_TIMEOUT_MS;
  const staleMs =
    cleanPositiveIntOrNull(obj.stale_threshold_ms) ?? WORKSPACE_SAMPLE_STALE_THRESHOLD_MS;
  return {
    interval_ms: intervalMs,
    metadata_timeout_ms: timeoutMs,
    stale_threshold_ms: staleMs,
    snapshot_resets_sample_age: false,
  };
}

export function createUnavailableConfiguredSelection(): WorkspaceConfiguredSelection {
  return {
    selected_model: null,
    selected_effort: null,
    evidence: 'unavailable',
    counts_as_runtime_telemetry: false,
  };
}

export function createVerifiedConfiguredSelection(
  selectedModel?: string | null,
  selectedEffort?: string | null,
): WorkspaceConfiguredSelection {
  const cleanModel = sanitizeWorkspaceSelectionToken(selectedModel);
  const cleanEffort = sanitizeWorkspaceSelectionToken(selectedEffort);
  if (cleanModel === null && cleanEffort === null) {
    return createUnavailableConfiguredSelection();
  }
  return {
    selected_model: cleanModel,
    selected_effort: cleanEffort,
    evidence: 'verified_configuration',
    counts_as_runtime_telemetry: false,
  };
}

export function normalizeWorkspaceConfiguredSelection(raw: unknown): WorkspaceConfiguredSelection {
  if (!isRecord(raw) || raw.evidence !== 'verified_configuration') {
    return createUnavailableConfiguredSelection();
  }
  return createVerifiedConfiguredSelection(
    typeof raw.selected_model === 'string' ? raw.selected_model : null,
    typeof raw.selected_effort === 'string' ? raw.selected_effort : null,
  );
}

function normalizeProcessInstance(raw: unknown): ObservationProcessInstance | null {
  if (!isRecord(raw)) {
    return null;
  }
  const pid = cleanPositiveIntOrNull(raw.pid);
  const bootId = typeof raw.boot_id === 'string' ? raw.boot_id : '';
  const startTicks = cleanNonNegativeIntOrNull(raw.start_ticks);
  if (
    pid === null ||
    startTicks === null ||
    bootId.length === 0 ||
    utf8ByteLength(bootId) > MAX_WORKSPACE_LABEL_BYTES ||
    bootId.trim() !== bootId ||
    !/^[\x21-\x7e]+$/.test(bootId)
  ) {
    return null;
  }
  return {
    pid,
    boot_id: bootId,
    start_ticks: startTicks,
  };
}

function processInstancesEqual(
  left: ObservationProcessInstance | null,
  right: ObservationProcessInstance | null,
): boolean {
  if (left === null && right === null) {
    return true;
  }
  if (left === null || right === null) {
    return false;
  }
  return (
    left.pid === right.pid &&
    left.boot_id === right.boot_id &&
    left.start_ticks === right.start_ticks
  );
}

function normalizeNativeCapabilities(raw: unknown): WorkspaceNativeCapabilities {
  const obj = isRecord(raw) ? raw : {};
  return {
    exact_thread_read: parseCapabilityState(obj.exact_thread_read),
    activity_detail: parseCapabilityState(obj.activity_detail),
    configured_selection: parseCapabilityState(obj.configured_selection),
    wakeup_delivery: parseCapabilityState(obj.wakeup_delivery),
    per_call_identity: parseCapabilityState(obj.per_call_identity),
  };
}

function parseRawIdentityFields(raw: unknown): WorkspaceChatIdentity {
  const obj = isRecord(raw) ? raw : {};
  return {
    source_id: typeof obj.source_id === 'string' ? obj.source_id : '',
    binding_id: typeof obj.binding_id === 'string' ? obj.binding_id : '',
    thread_uuid: typeof obj.thread_uuid === 'string' ? obj.thread_uuid : '',
    canonical_project_scope:
      typeof obj.canonical_project_scope === 'string' ? obj.canonical_project_scope : '',
  };
}

export function validateWorkspaceNativeSample(sample: WorkspaceNativeSample): void {
  validateWorkspaceChatIdentity(sample.chat_identity);
  const cleanEpoch = validateCorrelationId(sample.producer_epoch);
  if (cleanEpoch === null || cleanEpoch !== sample.producer_epoch) {
    throw new WorkspaceContractError(
      'invalid_native_sample',
      'producer_epoch must be a valid trimmed ASCII token up to 128 bytes',
      'producer_epoch',
    );
  }
  if (sample.backend_process_instance !== null) {
    if (normalizeProcessInstance(sample.backend_process_instance) === null) {
      throw new WorkspaceContractError(
        'invalid_native_sample',
        'backend_process_instance requires non-zero pid and graphic ASCII boot_id',
        'backend_process_instance',
      );
    }
  }
  if (!Number.isInteger(sample.sequence) || sample.sequence < 0) {
    throw new WorkspaceContractError(
      'invalid_native_sample',
      'sequence must be a non-negative integer',
      'sequence',
    );
  }
  if (
    sample.observed_at_ms !== null &&
    sample.last_success_at_ms !== null &&
    sample.last_success_at_ms > sample.observed_at_ms
  ) {
    throw new WorkspaceContractError(
      'invalid_native_sample',
      'last_success_at_ms must not exceed observed_at_ms',
      'last_success_at_ms',
    );
  }
}

export function normalizeWorkspaceNativeSample(raw: unknown): WorkspaceNativeSample {
  const obj = isRecord(raw) ? raw : {};
  const chatIdentity = parseRawIdentityFields(obj.chat_identity);
  const identityOk = isValidWorkspaceChatIdentity(chatIdentity);
  const rawEpoch = typeof obj.producer_epoch === 'string' ? obj.producer_epoch : '';
  const cleanEpoch = validateCorrelationId(rawEpoch);
  const validEpoch = cleanEpoch !== null && cleanEpoch === rawEpoch ? cleanEpoch : '';
  const validProcess = normalizeProcessInstance(obj.backend_process_instance);
  const rawObserved = obj.observed_at_ms ?? obj.observed_at;
  const rawLastSuccess = obj.last_success_at_ms ?? obj.last_success_at;
  const observedAtMs = cleanPositiveIntOrNull(rawObserved);
  const rawLastSuccessMs = cleanPositiveIntOrNull(rawLastSuccess);

  let timestampsConsistent = true;
  if (observedAtMs !== null && rawLastSuccessMs !== null) {
    timestampsConsistent = rawLastSuccessMs <= observedAtMs;
  } else if (observedAtMs === null && rawLastSuccessMs !== null) {
    timestampsConsistent = false;
  }

  let availability = parseAvailabilityState(obj.availability);
  if (!identityOk || validEpoch.length === 0 || !timestampsConsistent) {
    availability = 'unavailable';
  } else if (
    availability === 'available' &&
    (observedAtMs === null || rawLastSuccessMs === null)
  ) {
    availability = 'unavailable';
  }

  const activity: WorkspaceActivityState =
    availability === 'available' ? parseActivityState(obj.activity) : 'unknown';

  const configuredSelection =
    availability === 'available'
      ? normalizeWorkspaceConfiguredSelection(obj.configured_selection)
      : createUnavailableConfiguredSelection();

  const rawConn = isRecord(obj.connection) ? obj.connection : {};
  let connState = parseConnectionState(rawConn.state);
  if (availability === 'disconnected') {
    connState = 'disconnected';
  } else if (availability === 'unavailable' && connState === 'connected') {
    connState = 'unavailable';
  }

  return {
    chat_identity: chatIdentity,
    producer_epoch: validEpoch,
    backend_process_instance: validProcess,
    sequence: cleanNonNegativeInt(obj.sequence, 0),
    observed_at_ms: observedAtMs,
    last_success_at_ms: timestampsConsistent ? rawLastSuccessMs : null,
    availability,
    activity,
    connection: {
      state: connState,
      freshness: parseFreshnessState(rawConn.freshness),
      route: validateCorrelationId(rawConn.route),
      diagnostic: cleanOptionalSummary(rawConn.diagnostic),
    },
    capabilities: normalizeNativeCapabilities(obj.capabilities),
    configured_selection: configuredSelection,
  };
}

export function evaluateWorkspaceNativeSampleAt(
  sample: WorkspaceNativeSample,
  referenceNowMs: number,
  staleThresholdMs: number = WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
): WorkspaceEvaluatedNativeSample {
  const thresholdMs =
    staleThresholdMs > 0 ? staleThresholdMs : WORKSPACE_SAMPLE_STALE_THRESHOLD_MS;
  const norm = normalizeWorkspaceNativeSample(sample);
  try {
    validateWorkspaceNativeSample(sample);
  } catch {
    return {
      sample_age_ms: null,
      freshness: 'unknown',
      effective_availability: 'unavailable',
      effective_activity: 'unknown',
      effective_configured_selection: createUnavailableConfiguredSelection(),
    };
  }

  if (norm.producer_epoch.length === 0) {
    return {
      sample_age_ms: null,
      freshness: 'unknown',
      effective_availability: 'unavailable',
      effective_activity: 'unknown',
      effective_configured_selection: createUnavailableConfiguredSelection(),
    };
  }

  if (norm.last_success_at_ms === null) {
    const freshness: WorkspaceFreshnessState =
      norm.observed_at_ms !== null ? 'never_succeeded' : 'unknown';
    const effectiveAvailability: WorkspaceAvailabilityState =
      norm.availability === 'disconnected' ? 'disconnected' : 'unavailable';
    return {
      sample_age_ms: null,
      freshness,
      effective_availability: effectiveAvailability,
      effective_activity: 'unknown',
      effective_configured_selection: createUnavailableConfiguredSelection(),
    };
  }

  const deltaMs = Math.trunc(referenceNowMs) - norm.last_success_at_ms;
  if (deltaMs < -thresholdMs) {
    return {
      sample_age_ms: null,
      freshness: 'unknown',
      effective_availability: 'unavailable',
      effective_activity: 'unknown',
      effective_configured_selection: createUnavailableConfiguredSelection(),
    };
  }

  const clampedAgeMs = Math.max(0, deltaMs);
  const isStale = clampedAgeMs > thresholdMs;
  const freshness: WorkspaceFreshnessState = isStale ? 'stale' : 'fresh';

  let effectiveAvailability: WorkspaceAvailabilityState;
  if (norm.availability === 'available') {
    effectiveAvailability = isStale ? 'stale' : 'available';
  } else {
    effectiveAvailability = norm.availability;
  }

  const effectiveActivity: WorkspaceActivityState =
    effectiveAvailability === 'available' ? norm.activity : 'unknown';

  const effectiveSelection =
    effectiveAvailability === 'available'
      ? norm.configured_selection
      : createUnavailableConfiguredSelection();

  return {
    sample_age_ms: clampedAgeMs,
    freshness,
    effective_availability: effectiveAvailability,
    effective_activity: effectiveActivity,
    effective_configured_selection: effectiveSelection,
  };
}

export type WorkspaceSampleReconcileDecision =
  | 'accepted_initial'
  | 'accepted_next_sequence'
  | 'accepted_new_epoch'
  | 'rejected_invalid_sample'
  | 'rejected_mismatched_identity'
  | 'rejected_duplicate_sequence'
  | 'rejected_stale_sequence'
  | 'rejected_stale_epoch'
  | 'rejected_epoch_process_mismatch';

export function isSampleReconcileAccepted(decision: WorkspaceSampleReconcileDecision): boolean {
  return (
    decision === 'accepted_initial' ||
    decision === 'accepted_next_sequence' ||
    decision === 'accepted_new_epoch'
  );
}

export function reconcileWorkspaceNativeSample(
  current: WorkspaceNativeSample | null,
  incoming: WorkspaceNativeSample,
): WorkspaceSampleReconcileDecision {
  try {
    validateWorkspaceNativeSample(incoming);
  } catch {
    return 'rejected_invalid_sample';
  }
  if (current === null) {
    return 'accepted_initial';
  }
  try {
    validateWorkspaceNativeSample(current);
  } catch {
    return 'accepted_initial';
  }
  if (!workspaceChatIdentitiesEqual(current.chat_identity, incoming.chat_identity)) {
    return 'rejected_mismatched_identity';
  }
  if (current.producer_epoch === incoming.producer_epoch) {
    if (!processInstancesEqual(current.backend_process_instance, incoming.backend_process_instance)) {
      return 'rejected_epoch_process_mismatch';
    }
    if (incoming.sequence > current.sequence) {
      return 'accepted_next_sequence';
    }
    if (incoming.sequence === current.sequence) {
      return 'rejected_duplicate_sequence';
    }
    return 'rejected_stale_sequence';
  }
  if (incoming.observed_at_ms === null) {
    return 'rejected_stale_epoch';
  }
  if (current.observed_at_ms !== null && incoming.observed_at_ms < current.observed_at_ms) {
    return 'rejected_stale_epoch';
  }
  return 'accepted_new_epoch';
}

export function serializeWorkspaceNativeSampleSidecar(sample: WorkspaceNativeSample): Uint8Array {
  validateWorkspaceNativeSample(sample);
  const normalized = normalizeWorkspaceNativeSample(sample);
  const bytes = UTF8_ENCODER.encode(JSON.stringify(normalized));
  if (bytes.byteLength > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES) {
    throw new WorkspaceContractError(
      'sidecar_too_large',
      `native sample sidecar size (${bytes.byteLength} bytes) exceeds ${MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES}-byte limit`,
    );
  }
  return bytes;
}

export function parseWorkspaceNativeSampleSidecar(
  input: Uint8Array | string,
): WorkspaceNativeSample {
  const bytes = typeof input === 'string' ? UTF8_ENCODER.encode(input) : input;
  if (bytes.byteLength > MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES) {
    throw new WorkspaceContractError(
      'sidecar_too_large',
      `native sample sidecar size (${bytes.byteLength} bytes) exceeds ${MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES}-byte limit`,
    );
  }
  let jsonText: string;
  try {
    jsonText = typeof input === 'string' ? input : UTF8_DECODER.decode(bytes);
  } catch (err) {
    throw new WorkspaceContractError(
      'invalid_json',
      `invalid UTF-8 in native sample sidecar: ${String(err)}`,
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonText);
  } catch (err) {
    throw new WorkspaceContractError(
      'invalid_json',
      `invalid JSON in native sample sidecar: ${String(err)}`,
    );
  }
  return normalizeWorkspaceNativeSample(parsed);
}

export function boundWorkspaceNativeSamplesPerSource(
  samples: readonly WorkspaceNativeSample[],
): { samples: WorkspaceNativeSample[]; omittedCount: number } {
  let omittedCount = 0;
  const latestByChat = new Map<string, WorkspaceNativeSample>();

  for (const sample of samples) {
    const chatKey = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (chatKey === null) {
      omittedCount += 1;
      continue;
    }
    try {
      validateWorkspaceNativeSample(sample);
    } catch {
      omittedCount += 1;
      continue;
    }
    const normalized = normalizeWorkspaceNativeSample(sample);
    const existing = latestByChat.get(chatKey) ?? null;
    if (existing === null || isSampleReconcileAccepted(reconcileWorkspaceNativeSample(existing, normalized))) {
      latestByChat.set(chatKey, normalized);
    }
  }

  const bySource = new Map<string, Array<{ key: string; sample: WorkspaceNativeSample }>>();
  const sortedChatKeys = Array.from(latestByChat.keys()).sort(compareStringsUtf8);
  for (const key of sortedChatKeys) {
    const sample = latestByChat.get(key);
    if (!sample) {
      continue;
    }
    const bucket = bySource.get(sample.chat_identity.source_id) ?? [];
    bucket.push({ key, sample });
    bySource.set(sample.chat_identity.source_id, bucket);
  }

  const retained: WorkspaceNativeSample[] = [];
  const sortedSources = Array.from(bySource.keys()).sort(compareStringsUtf8);
  for (const sourceId of sortedSources) {
    const group = bySource.get(sourceId) ?? [];
    group.sort((left, right) => {
      const leftObs = left.sample.observed_at_ms ?? Number.MIN_SAFE_INTEGER;
      const rightObs = right.sample.observed_at_ms ?? Number.MIN_SAFE_INTEGER;
      if (rightObs !== leftObs) {
        return rightObs - leftObs;
      }
      if (right.sample.sequence !== left.sample.sequence) {
        return right.sample.sequence - left.sample.sequence;
      }
      return compareStringsUtf8(left.key, right.key);
    });
    if (group.length > MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE) {
      omittedCount += group.length - MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE;
      group.length = MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE;
    }
    for (const entry of group) {
      retained.push(entry.sample);
    }
  }

  return { samples: retained, omittedCount };
}

export function normalizeWorkspaceChatSummary(raw: unknown): WorkspaceChatSummary {
  const obj = isRecord(raw) ? raw : {};
  const identity = parseRawIdentityFields(obj.identity);
  const identityOk = isValidWorkspaceChatIdentity(identity);
  const availability: WorkspaceAvailabilityState = identityOk
    ? parseAvailabilityState(obj.availability)
    : 'unavailable';
  const activity: WorkspaceActivityState =
    availability === 'available' ? parseActivityState(obj.activity) : 'unknown';
  const configuredSelection =
    availability === 'available'
      ? normalizeWorkspaceConfiguredSelection(obj.configured_selection)
      : createUnavailableConfiguredSelection();
  return {
    identity,
    thread_label: sanitizeWorkspaceLabel(obj.thread_label, 'Codex chat'),
    project_label: sanitizeWorkspaceLabel(obj.project_label, 'Project'),
    availability,
    activity,
    configured_selection: configuredSelection,
    active_job_count: cleanNonNegativeInt(obj.active_job_count, 0),
    attention_count: cleanNonNegativeInt(obj.attention_count, 0),
    updated_at_ms: cleanPositiveIntOrNull(obj.updated_at_ms),
  };
}

// ---------------------------------------------------------------------------
// Structured Job History, Failure vs Attention, and Verified Acceptance Proof
// ---------------------------------------------------------------------------

function parseAcceptanceStatus(raw: unknown): WorkspaceAcceptanceStatus {
  if (
    raw === 'verified_accepted' ||
    raw === 'pending_review' ||
    raw === 'rejected' ||
    raw === 'not_accepted' ||
    raw === 'unknown'
  ) {
    return raw;
  }
  return 'unknown';
}

function parseAttentionKind(raw: unknown): WorkspaceAttentionKind {
  if (
    raw === 'needs_approval' ||
    raw === 'needs_user_input' ||
    raw === 'on_hold' ||
    raw === 'unresolved_effect' ||
    raw === 'blocked_failure' ||
    raw === 'review_required' ||
    raw === 'none' ||
    raw === 'unknown'
  ) {
    return raw;
  }
  return 'unknown';
}

function parseObservationJobState(raw: unknown): ObservationJobState {
  if (
    raw === 'planning' ||
    raw === 'ready' ||
    raw === 'running' ||
    raw === 'reviewing' ||
    raw === 'needs_user' ||
    raw === 'completed' ||
    raw === 'failed' ||
    raw === 'unknown'
  ) {
    return raw;
  }
  return 'unknown';
}

function parseObservationDeliveryStage(raw: unknown): ObservationDeliveryStage {
  if (
    raw === 'result_ready' ||
    raw === 'notification_submitted' ||
    raw === 'batch_delivered' ||
    raw === 'batch_acknowledged' ||
    raw === 'response_returned' ||
    raw === 'unknown' ||
    raw === 'failed'
  ) {
    return raw;
  }
  return 'unknown';
}

function cleanAcceptanceProofBase(raw: unknown): WorkspaceAcceptanceProof {
  const obj = isRecord(raw) ? raw : {};
  return {
    status: parseAcceptanceStatus(obj.status),
    accepted_candidate_id: validateCorrelationId(obj.accepted_candidate_id),
    combined_candidate_id: validateCorrelationId(obj.combined_candidate_id),
    review_verdict_id: validateCorrelationId(obj.review_verdict_id),
    review_receipt_id: validateCorrelationId(obj.review_receipt_id),
    integration_receipt_id: validateCorrelationId(obj.integration_receipt_id),
    closure_reservation_id: validateCorrelationId(obj.closure_reservation_id),
    closure_verdict_id: validateCorrelationId(obj.closure_verdict_id),
    verified_at_ms: cleanPositiveIntOrNull(obj.verified_at_ms),
  };
}

export function normalizeWorkspaceAcceptanceProofForTask(raw: unknown): WorkspaceAcceptanceProof {
  const out = cleanAcceptanceProofBase(raw);
  if (out.status === 'verified_accepted') {
    const hasCandidate = out.accepted_candidate_id !== null;
    const hasReviewProof = out.review_verdict_id !== null || out.review_receipt_id !== null;
    if (!hasCandidate || !hasReviewProof) {
      out.status = 'unknown';
    }
  }
  return out;
}

export function normalizeWorkspaceAcceptanceProofForJob(raw: unknown): WorkspaceAcceptanceProof {
  const out = cleanAcceptanceProofBase(raw);
  if (
    out.status === 'verified_accepted' &&
    (out.combined_candidate_id === null || out.closure_verdict_id === null)
  ) {
    out.status = 'unknown';
  }
  return out;
}

function normalizeHistoricalFailure(raw: unknown): WorkspaceHistoricalFailure | null {
  if (!isRecord(raw)) {
    return null;
  }
  const cleanReason = sanitizeWorkspaceSummary(raw.reason);
  return {
    failure_id: validateCorrelationId(raw.failure_id),
    stage: sanitizeStatusToken(raw.stage),
    failed_at_ms: cleanPositiveIntOrNull(raw.failed_at_ms),
    task_id: validateCorrelationId(raw.task_id),
    attempt_no: cleanNonNegativeIntOrNull(raw.attempt_no),
    run_id: validateCorrelationId(raw.run_id),
    outbox_id: validateCorrelationId(raw.outbox_id),
    reason: cleanReason.length > 0 ? cleanReason : 'Historical failure recorded',
    superseded_or_resolved: Boolean(raw.superseded_or_resolved),
  };
}

function normalizeAttentionState(raw: unknown): WorkspaceAttentionState | null {
  if (!isRecord(raw)) {
    return null;
  }
  return {
    kind: parseAttentionKind(raw.kind),
    reason: sanitizeWorkspaceSummary(raw.reason),
    next_step: cleanOptionalSummary(raw.next_step),
    blocking_hold_ids: cleanIdList(raw.blocking_hold_ids, MAX_WORKSPACE_RECORD_LINKS_PER_LIST),
    blocking_outbox_ids: cleanIdList(raw.blocking_outbox_ids, MAX_WORKSPACE_RECORD_LINKS_PER_LIST),
    question_id: validateCorrelationId(raw.question_id),
    updated_at_ms: cleanPositiveIntOrNull(raw.updated_at_ms),
  };
}

function cleanDeduplicatedIdList(raw: unknown, maxItems: number): string[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  const seen = new Set<string>();
  const out: string[] = [];
  for (const item of raw) {
    if (out.length >= maxItems) {
      break;
    }
    const clean = validateCorrelationId(item);
    if (clean !== null && clean !== 'unknown' && !seen.has(clean)) {
      seen.add(clean);
      out.push(clean);
    }
  }
  return out;
}

function parseExternalResolutionApplicability(
  raw: unknown,
): WorkspaceExternalResolutionApplicability {
  if (raw === 'applicable' || raw === 'stale' || raw === 'unverified') {
    return raw;
  }
  return 'unverified';
}

function normalizeCoveredAttentionList(
  raw: unknown,
  declaredTaskIds: ReadonlySet<string>,
): WorkspaceCoveredAttention[] {
  if (!Array.isArray(raw)) {
    return [];
  }
  const normalized: WorkspaceCoveredAttention[] = [];
  for (const entry of raw) {
    if (!isRecord(entry)) {
      continue;
    }
    const attention = normalizeAttentionState(entry.attention);
    if (attention === null) {
      continue;
    }
    if (entry.task_id === null) {
      normalized.push({ task_id: null, attention });
      continue;
    }
    const cleanTaskId = validateCorrelationId(entry.task_id);
    if (
      cleanTaskId === null ||
      cleanTaskId === 'unknown' ||
      !declaredTaskIds.has(cleanTaskId)
    ) {
      // Invalid or undeclared task identities are quarantined rather than
      // widened into job-level coverage.
      continue;
    }
    normalized.push({ task_id: cleanTaskId, attention });
  }
  return capTail(normalized, MAX_WORKSPACE_COVERED_ATTENTION_PER_RESOLUTION).items;
}

/**
 * Normalizes a single `WorkspaceExternalResolution` record from the backend's
 * store-verified projection. Never synthesizes proof or applicability from prose,
 * commit SHAs, or lineage links.
 */
export function normalizeWorkspaceExternalResolution(
  raw: unknown,
): WorkspaceExternalResolution | null {
  if (!isRecord(raw)) {
    return null;
  }
  const receiptId = validateCorrelationId(raw.receipt_id);
  if (receiptId === null || receiptId === 'unknown') {
    return null;
  }
  if (raw.outcome !== 'resolved_externally') {
    return null;
  }
  if (raw.scope !== 'job' && raw.scope !== 'tasks') {
    return null;
  }
  const taskIds = cleanDeduplicatedIdList(raw.task_ids, MAX_WORKSPACE_TASKS_PER_JOB);
  let applicability = parseExternalResolutionApplicability(raw.applicability);
  if (raw.scope === 'tasks' && taskIds.length === 0 && applicability === 'applicable') {
    applicability = 'unverified';
  }
  const cleanRequestKey = validateCorrelationId(raw.request_key);
  const cleanPreviewDigest = validateCorrelationId(raw.preview_digest);
  return {
    receipt_id: receiptId,
    request_key:
      cleanRequestKey !== null && cleanRequestKey !== 'unknown' ? cleanRequestKey : null,
    preview_digest:
      cleanPreviewDigest !== null && cleanPreviewDigest !== 'unknown' ? cleanPreviewDigest : null,
    outcome: 'resolved_externally',
    scope: raw.scope,
    task_ids: taskIds,
    applicability,
    summary: sanitizeWorkspaceSummary(raw.summary),
    evidence_refs: cleanDeduplicatedIdList(raw.evidence_refs, MAX_WORKSPACE_RECORD_LINKS_PER_LIST),
    recorded_at_ms: cleanPositiveIntOrNull(raw.recorded_at_ms),
    covered_attention: normalizeCoveredAttentionList(
      raw.covered_attention,
      new Set(taskIds),
    ),
  };
}

function normalizeExternalResolutionListWithOmissions(raw: unknown): {
  items: WorkspaceExternalResolution[];
  omitted: number;
} {
  if (!Array.isArray(raw)) {
    return { items: [], omitted: 0 };
  }
  const parsed: WorkspaceExternalResolution[] = [];
  let invalidCount = 0;
  for (const item of raw) {
    const clean = normalizeWorkspaceExternalResolution(item);
    if (clean !== null) {
      parsed.push(clean);
    } else {
      invalidCount += 1;
    }
  }
  const deduplicatedRev: WorkspaceExternalResolution[] = [];
  const seenReceipts = new Set<string>();
  for (let idx = parsed.length - 1; idx >= 0; idx -= 1) {
    const entry = parsed[idx]!;
    if (!seenReceipts.has(entry.receipt_id)) {
      seenReceipts.add(entry.receipt_id);
      deduplicatedRev.push(entry);
    }
  }
  deduplicatedRev.reverse();
  const capped = capTail(deduplicatedRev, MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB);
  return {
    items: capped.items,
    omitted: invalidCount + (parsed.length - deduplicatedRev.length) + capped.omitted,
  };
}

export function normalizeWorkspaceExternalResolutions(
  raw: unknown,
): WorkspaceExternalResolution[] {
  return normalizeExternalResolutionListWithOmissions(raw).items;
}

function normalizeExecutor(raw: unknown): ObservationExecutor | null {
  if (!isRecord(raw)) {
    return null;
  }
  return {
    tool: typeof raw.tool === 'string' ? raw.tool : null,
    provider: typeof raw.provider === 'string' ? raw.provider : null,
    role: typeof raw.role === 'string' ? raw.role : null,
    requested_model: typeof raw.requested_model === 'string' ? raw.requested_model : null,
    observed_model: typeof raw.observed_model === 'string' ? raw.observed_model : null,
    requested_effort: typeof raw.requested_effort === 'string' ? raw.requested_effort : null,
    observed_effort: typeof raw.observed_effort === 'string' ? raw.observed_effort : null,
  };
}

function normalizeTaskAttempt(raw: unknown): WorkspaceTaskAttempt {
  const obj = isRecord(raw) ? raw : {};
  return {
    attempt_no: cleanNonNegativeInt(obj.attempt_no, 0),
    status: sanitizeStatusToken(obj.status),
    run_id: validateCorrelationId(obj.run_id),
    worker_id: validateCorrelationId(obj.worker_id),
    outbox_id: validateCorrelationId(obj.outbox_id),
    executor: normalizeExecutor(obj.executor),
    started_at_ms: cleanPositiveIntOrNull(obj.started_at_ms),
    finished_at_ms: cleanPositiveIntOrNull(obj.finished_at_ms),
    summary: cleanOptionalSummary(obj.summary),
  };
}

function normalizeTaskCandidate(raw: unknown): WorkspaceTaskCandidate {
  const obj = isRecord(raw) ? raw : {};
  return {
    candidate_id: validateCorrelationId(obj.candidate_id) ?? 'unknown',
    attempt_no: cleanNonNegativeInt(obj.attempt_no, 0),
    worker_id: validateCorrelationId(obj.worker_id),
    head_commit_sha: validateCorrelationId(obj.head_commit_sha),
    tree_sha: validateCorrelationId(obj.tree_sha),
    verification_passed:
      typeof obj.verification_passed === 'boolean' ? obj.verification_passed : null,
    review_status: sanitizeStatusToken(obj.review_status),
    review_verdict_id: validateCorrelationId(obj.review_verdict_id),
    review_receipt_id: validateCorrelationId(obj.review_receipt_id),
    produced_at_ms: cleanPositiveIntOrNull(obj.produced_at_ms),
    summary: cleanOptionalSummary(obj.summary),
  };
}

function normalizeTaskHistoryWithOmissions(raw: unknown): {
  task: WorkspaceTaskHistory;
  attemptsOmitted: number;
  candidatesOmitted: number;
} {
  const obj = isRecord(raw) ? raw : {};
  const rawAttempts = Array.isArray(obj.attempts) ? obj.attempts.map(normalizeTaskAttempt) : [];
  const cappedAttempts = capTail(rawAttempts, MAX_WORKSPACE_ATTEMPTS_PER_TASK);

  const rawCandidates = Array.isArray(obj.candidates)
    ? obj.candidates.map(normalizeTaskCandidate)
    : [];
  const cappedCandidates = capTail(rawCandidates, MAX_WORKSPACE_CANDIDATES_PER_TASK);

  return {
    task: {
      task_id: validateCorrelationId(obj.task_id) ?? 'unknown',
      title: sanitizeWorkspaceLabel(obj.title, 'Task'),
      role: validateCorrelationId(obj.role),
      difficulty: validateCorrelationId(obj.difficulty),
      stage: typeof obj.stage === 'string' ? sanitizeStatusToken(obj.stage) : null,
      status: sanitizeStatusToken(obj.status),
      depends_on: cleanIdList(obj.depends_on, MAX_WORKSPACE_TASKS_PER_JOB),
      current_attempt_no: cleanNonNegativeInt(obj.current_attempt_no, 0),
      attempts: cappedAttempts.items,
      candidates: cappedCandidates.items,
      active_candidate_id: validateCorrelationId(obj.active_candidate_id),
      accepted_candidate_id: validateCorrelationId(obj.accepted_candidate_id),
      historical_failure: normalizeHistoricalFailure(obj.historical_failure),
      current_attention: normalizeAttentionState(obj.current_attention),
      acceptance_proof: normalizeWorkspaceAcceptanceProofForTask(obj.acceptance_proof),
    },
    attemptsOmitted: cappedAttempts.omitted,
    candidatesOmitted: cappedCandidates.omitted,
  };
}

function normalizeRunRecord(raw: unknown): WorkspaceRunRecord {
  const obj = isRecord(raw) ? raw : {};
  return {
    run_id: validateCorrelationId(obj.run_id) ?? 'unknown',
    worker_id: validateCorrelationId(obj.worker_id),
    binding_id: validateCorrelationId(obj.binding_id) ?? 'unknown',
    task_id: validateCorrelationId(obj.task_id),
    attempt_no: cleanNonNegativeIntOrNull(obj.attempt_no),
    role: validateCorrelationId(obj.role),
    status: sanitizeStatusToken(obj.status),
    executor: normalizeExecutor(obj.executor),
    started_at_ms: cleanPositiveIntOrNull(obj.started_at_ms),
    finished_at_ms: cleanPositiveIntOrNull(obj.finished_at_ms),
    updated_at_ms: cleanPositiveIntOrNull(obj.updated_at_ms),
  };
}

function normalizeOutboxRecord(raw: unknown): WorkspaceOutboxRecord {
  const obj = isRecord(raw) ? raw : {};
  return {
    outbox_id: validateCorrelationId(obj.outbox_id) ?? 'unknown',
    request_key: validateCorrelationId(obj.request_key),
    kind: sanitizeStatusToken(obj.kind),
    status: sanitizeStatusToken(obj.status),
    task_id: validateCorrelationId(obj.task_id),
    attempt_no: cleanNonNegativeIntOrNull(obj.attempt_no),
    role: validateCorrelationId(obj.role),
    run_id: validateCorrelationId(obj.run_id),
    worker_id: validateCorrelationId(obj.worker_id),
    receipt_id: validateCorrelationId(obj.receipt_id),
    reserved_at_job_revision: cleanNonNegativeIntOrNull(obj.reserved_at_job_revision),
    updated_at_job_revision: cleanNonNegativeIntOrNull(obj.updated_at_job_revision),
    diagnostic: cleanOptionalSummary(obj.diagnostic),
  };
}

function normalizeCombinedCandidate(raw: unknown): WorkspaceCombinedCandidate {
  const obj = isRecord(raw) ? raw : {};
  return {
    combined_candidate_id: validateCorrelationId(obj.combined_candidate_id) ?? 'unknown',
    job_revision: cleanNonNegativeIntOrNull(obj.job_revision),
    status: sanitizeStatusToken(obj.status),
    final_commit_sha: validateCorrelationId(obj.final_commit_sha),
    final_tree_sha: validateCorrelationId(obj.final_tree_sha),
    candidate_digest: validateCorrelationId(obj.candidate_digest),
    closure_reservation_id: validateCorrelationId(obj.closure_reservation_id),
    check_count: cleanNonNegativeInt(obj.check_count, 0),
    produced_at_ms: cleanPositiveIntOrNull(obj.produced_at_ms),
  };
}

function normalizeJobResult(raw: unknown): WorkspaceJobResult | null {
  if (!isRecord(raw)) {
    return null;
  }
  return {
    outcome: sanitizeStatusToken(raw.outcome),
    summary: sanitizeWorkspaceSummary(raw.summary),
    produced_at_ms: cleanPositiveIntOrNull(raw.produced_at_ms),
    delivery_stage: parseObservationDeliveryStage(raw.delivery_stage),
    closure_verdict_id: validateCorrelationId(raw.closure_verdict_id),
  };
}

function normalizeRecordLinks(raw: unknown): WorkspaceRecordLinks {
  const obj = isRecord(raw) ? raw : {};
  return {
    source_plan_id: validateCorrelationId(obj.source_plan_id),
    source_plan_revision: cleanNonNegativeIntOrNull(obj.source_plan_revision),
    source_approval_id: validateCorrelationId(obj.source_approval_id),
    source_work_brief_id: validateCorrelationId(obj.source_work_brief_id),
    planning_grant_id: validateCorrelationId(obj.planning_grant_id),
    planning_reservation_id: validateCorrelationId(obj.planning_reservation_id),
    supersedes_job_id: validateCorrelationId(obj.supersedes_job_id),
    superseded_by_job_id: validateCorrelationId(obj.superseded_by_job_id),
    parent_job_id: validateCorrelationId(obj.parent_job_id),
    follow_up_job_ids: cleanIdList(obj.follow_up_job_ids, MAX_WORKSPACE_RECORD_LINKS_PER_LIST),
    architect_advice_ids: cleanIdList(
      obj.architect_advice_ids,
      MAX_WORKSPACE_RECORD_LINKS_PER_LIST,
    ),
    source_request_ids: cleanIdList(obj.source_request_ids, MAX_WORKSPACE_RECORD_LINKS_PER_LIST),
  };
}

function normalizeJobSummary(raw: unknown): WorkspaceJobSummary {
  const obj = isRecord(raw) ? raw : {};
  let validSource = 'unknown';
  try {
    validSource = validateWorkspaceSourceId(obj.source_id);
  } catch {
    validSource = 'unknown';
  }
  let validBinding: string | null = null;
  if (typeof obj.binding_id === 'string') {
    try {
      validBinding = validateWorkspaceBindingId(obj.binding_id);
    } catch {
      validBinding = null;
    }
  }
  let validChat: WorkspaceChatIdentity | null = null;
  if (isValidWorkspaceChatIdentity(obj.chat_identity)) {
    const checked = validateWorkspaceChatIdentity(obj.chat_identity);
    if (checked.source_id === validSource) {
      validChat = checked;
    }
  }
  return {
    source_id: validSource,
    job_id: validateCorrelationId(obj.job_id) ?? 'unknown',
    binding_id: validBinding,
    chat_identity: validChat,
    title: sanitizeWorkspaceLabel(obj.title, 'Job'),
    job_revision: cleanNonNegativeIntOrNull(obj.job_revision),
    state: parseObservationJobState(obj.state),
    phase: typeof obj.phase === 'string' ? sanitizeStatusToken(obj.phase) : null,
    approval_status: sanitizeStatusToken(obj.approval_status),
    execution_status: sanitizeStatusToken(obj.execution_status),
    review_state:
      typeof obj.review_state === 'string' ? sanitizeStatusToken(obj.review_state) : null,
    created_at_ms: cleanPositiveIntOrNull(obj.created_at_ms),
    updated_at_ms: cleanPositiveIntOrNull(obj.updated_at_ms),
    summary: cleanOptionalSummary(obj.summary),
  };
}

export function normalizeWorkspaceJobHistory(raw: unknown): WorkspaceJobHistory {
  const obj = isRecord(raw) ? raw : {};
  const rawTasks = Array.isArray(obj.tasks) ? obj.tasks : [];
  const tasksOmittedExtra = Math.max(0, rawTasks.length - MAX_WORKSPACE_TASKS_PER_JOB);
  const tasks: WorkspaceTaskHistory[] = [];
  let attemptsOmittedExtra = 0;
  let candidatesOmittedExtra = 0;
  for (const rawTask of rawTasks.slice(0, MAX_WORKSPACE_TASKS_PER_JOB)) {
    const res = normalizeTaskHistoryWithOmissions(rawTask);
    attemptsOmittedExtra += res.attemptsOmitted;
    candidatesOmittedExtra += res.candidatesOmitted;
    tasks.push(res.task);
  }

  const rawRuns = Array.isArray(obj.runs) ? obj.runs.map(normalizeRunRecord) : [];
  const cappedRuns = capTail(rawRuns, MAX_WORKSPACE_RUNS_PER_JOB);

  const rawOutbox = Array.isArray(obj.outbox) ? obj.outbox.map(normalizeOutboxRecord) : [];
  const cappedOutbox = capTail(rawOutbox, MAX_WORKSPACE_OUTBOX_PER_JOB);

  const rawCombined = Array.isArray(obj.combined_candidates)
    ? obj.combined_candidates.map(normalizeCombinedCandidate)
    : [];
  const cappedCombined = capTail(rawCombined, MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB);

  const cappedExternalResolutions = normalizeExternalResolutionListWithOmissions(
    obj.external_resolutions,
  );

  const rawTrunc = isRecord(obj.truncation) ? obj.truncation : {};
  const externalResolutionsOmitted =
    cleanNonNegativeInt(rawTrunc.external_resolutions_omitted, 0) +
    cappedExternalResolutions.omitted;
  const truncation: WorkspaceJobHistoryTruncation = {
    tasks_omitted: cleanNonNegativeInt(rawTrunc.tasks_omitted, 0) + tasksOmittedExtra,
    attempts_omitted: cleanNonNegativeInt(rawTrunc.attempts_omitted, 0) + attemptsOmittedExtra,
    candidates_omitted:
      cleanNonNegativeInt(rawTrunc.candidates_omitted, 0) + candidatesOmittedExtra,
    runs_omitted: cleanNonNegativeInt(rawTrunc.runs_omitted, 0) + cappedRuns.omitted,
    outbox_omitted: cleanNonNegativeInt(rawTrunc.outbox_omitted, 0) + cappedOutbox.omitted,
    combined_candidates_omitted:
      cleanNonNegativeInt(rawTrunc.combined_candidates_omitted, 0) + cappedCombined.omitted,
    ...(externalResolutionsOmitted > 0
      ? { external_resolutions_omitted: externalResolutionsOmitted }
      : {}),
  };

  return {
    job: normalizeJobSummary(obj.job),
    tasks,
    runs: cappedRuns.items,
    outbox: cappedOutbox.items,
    combined_candidates: cappedCombined.items,
    result: normalizeJobResult(obj.result),
    links: normalizeRecordLinks(obj.links),
    historical_failure: normalizeHistoricalFailure(obj.historical_failure),
    current_attention: normalizeAttentionState(obj.current_attention),
    acceptance_proof: normalizeWorkspaceAcceptanceProofForJob(obj.acceptance_proof),
    external_resolutions: cappedExternalResolutions.items,
    truncation,
  };
}

export function boundWorkspaceJobHistories(
  histories: readonly WorkspaceJobHistory[],
): { histories: WorkspaceJobHistory[]; omittedCount: number } {
  const normalizedAll = histories.map((h) => normalizeWorkspaceJobHistory(h));
  normalizedAll.sort((left, right) => {
    const leftTs = left.job.updated_at_ms ?? Number.MIN_SAFE_INTEGER;
    const rightTs = right.job.updated_at_ms ?? Number.MIN_SAFE_INTEGER;
    if (rightTs !== leftTs) {
      return rightTs - leftTs;
    }
    const sourceCmp = compareStringsUtf8(left.job.source_id, right.job.source_id);
    if (sourceCmp !== 0) {
      return sourceCmp;
    }
    return compareStringsUtf8(left.job.job_id, right.job.job_id);
  });

  const perBindingCounts = new Map<string, number>();
  const retained: WorkspaceJobHistory[] = [];
  let omittedCount = 0;

  for (const item of normalizedAll) {
    if (item.job.binding_id !== null) {
      const key = `${item.job.source_id}\u0000${item.job.binding_id}`;
      const count = perBindingCounts.get(key) ?? 0;
      if (count >= MAX_WORKSPACE_JOBS_PER_BINDING) {
        omittedCount += 1;
        continue;
      }
      perBindingCounts.set(key, count + 1);
    }
    if (retained.length >= MAX_OBSERVATION_JOBS) {
      omittedCount += 1;
      continue;
    }
    retained.push(item);
  }

  return { histories: retained, omittedCount };
}

// ---------------------------------------------------------------------------
// Safe `Messages` Projection (`<= 1,000` items; No Transcripts or Synthetic Turns)
// ---------------------------------------------------------------------------

export function validateSafeWorkspaceMessageSource(rawKind: unknown): WorkspaceSafeMessageSourceKind {
  const trimmed = typeof rawKind === 'string' ? rawKind.trim() : '';
  if (SAFE_MESSAGE_SOURCE_SET.has(trimmed)) {
    return trimmed as WorkspaceSafeMessageSourceKind;
  }
  throw new WorkspaceContractError(
    'forbidden_message_source',
    `forbidden or unrecognized workspace message source kind \`${trimmed}\``,
    'source_kind',
  );
}

export function formatWorkspaceMessageItemId(
  sourceId: string,
  sourceKind: WorkspaceSafeMessageSourceKind,
  sourceRecordId: string,
  subIndex: number,
): string {
  return [
    'wmsg:v1',
    `${utf8ByteLength(sourceId)}:${sourceId}`,
    `${utf8ByteLength(sourceKind)}:${sourceKind}`,
    `${utf8ByteLength(sourceRecordId)}:${sourceRecordId}`,
    `${Math.max(0, Math.trunc(subIndex))}`,
  ].join(':');
}

function normalizeMessageScope(raw: unknown): {
  scope: WorkspaceMessageScope;
  missingFields: string[];
} {
  const obj = isRecord(raw) ? raw : {};
  const missingFields: string[] = [];

  let sourceId = 'unknown';
  try {
    sourceId = validateWorkspaceSourceId(obj.source_id);
  } catch {
    missingFields.push('source_id');
  }

  let bindingId: string | null = null;
  if (typeof obj.binding_id === 'string') {
    try {
      bindingId = validateWorkspaceBindingId(obj.binding_id);
    } catch {
      missingFields.push('binding_id');
    }
  } else {
    missingFields.push('binding_id');
  }

  let threadUuid: string | null = null;
  if (typeof obj.thread_uuid === 'string') {
    try {
      threadUuid = validateWorkspaceThreadUuid(obj.thread_uuid);
    } catch {
      missingFields.push('thread_uuid');
    }
  } else {
    missingFields.push('thread_uuid');
  }

  let canonicalProjectScope: string | null = null;
  if (typeof obj.canonical_project_scope === 'string') {
    try {
      canonicalProjectScope = validateCanonicalProjectScope(obj.canonical_project_scope);
    } catch {
      missingFields.push('canonical_project_scope');
    }
  } else {
    missingFields.push('canonical_project_scope');
  }

  let chatIdentity: WorkspaceChatIdentity | null = null;
  if (isValidWorkspaceChatIdentity(obj.chat_identity)) {
    const candidate = validateWorkspaceChatIdentity(obj.chat_identity);
    if (
      candidate.source_id === sourceId &&
      candidate.binding_id === bindingId &&
      candidate.thread_uuid === threadUuid &&
      candidate.canonical_project_scope === canonicalProjectScope
    ) {
      chatIdentity = candidate;
    }
  }
  if (
    chatIdentity === null &&
    sourceId !== 'unknown' &&
    bindingId !== null &&
    threadUuid !== null &&
    canonicalProjectScope !== null
  ) {
    chatIdentity = {
      source_id: sourceId,
      binding_id: bindingId,
      thread_uuid: threadUuid,
      canonical_project_scope: canonicalProjectScope,
    };
  }

  return {
    scope: {
      source_id: sourceId,
      binding_id: bindingId,
      thread_uuid: threadUuid,
      canonical_project_scope: canonicalProjectScope,
      chat_identity: chatIdentity,
      job_id: validateCorrelationId(obj.job_id),
      task_id: validateCorrelationId(obj.task_id),
      run_id: validateCorrelationId(obj.run_id),
      attempt_no: cleanNonNegativeIntOrNull(obj.attempt_no),
    },
    missingFields,
  };
}

export interface WorkspaceMessageCandidateInput {
  source_kind: string;
  source_record_id: string;
  source_order: WorkspaceMessageSourceOrder;
  scope: Partial<WorkspaceMessageScope> & { source_id: string };
  actor_role?: string | null;
  raw_summary: string;
  upstream_truncated?: boolean;
  incomplete_reason?: string | null;
}

export function createSafeWorkspaceMessageItem(
  input: WorkspaceMessageCandidateInput,
): WorkspaceMessageItem {
  const kind = validateSafeWorkspaceMessageSource(input.source_kind);
  validateWorkspaceSourceId(input.scope.source_id);
  const cleanRecordId = validateCorrelationId(input.source_record_id);
  if (cleanRecordId === null) {
    throw new WorkspaceContractError(
      'invalid_native_sample',
      'source_record_id must be a valid non-empty ASCII identifier',
      'source_record_id',
    );
  }
  const { scope, missingFields } = normalizeMessageScope(input.scope);
  const recordedAtMs = cleanPositiveIntOrNull(input.source_order.recorded_at_ms);
  if (recordedAtMs === null) {
    missingFields.push('recorded_at_ms');
  }
  const order: WorkspaceMessageSourceOrder = {
    recorded_at_ms: recordedAtMs,
    source_seq: cleanNonNegativeInt(input.source_order.source_seq, 0),
    sub_index: cleanNonNegativeInt(input.source_order.sub_index, 0),
  };
  const sanitized = sanitizeWorkspaceSummaryWithMeta(input.raw_summary);
  const itemId = formatWorkspaceMessageItemId(
    scope.source_id,
    kind,
    cleanRecordId,
    order.sub_index,
  );
  return {
    item_id: itemId,
    source_kind: kind,
    source_record_id: cleanRecordId,
    source_order: order,
    scope,
    actor_role: validateCorrelationId(input.actor_role ?? null),
    summary: sanitized.summary,
    completeness: {
      summary_truncated: sanitized.truncated || Boolean(input.upstream_truncated),
      original_char_count: sanitized.original_char_count,
      max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
      redacted: sanitized.redacted,
      missing_fields: missingFields,
      incomplete_reason: cleanOptionalSummary(input.incomplete_reason),
    },
  };
}

function normalizeSafeMessageItem(raw: unknown): WorkspaceMessageItem | null {
  if (!isRecord(raw)) {
    return null;
  }
  let kind: WorkspaceSafeMessageSourceKind;
  try {
    kind = validateSafeWorkspaceMessageSource(raw.source_kind);
  } catch {
    return null;
  }
  const rawScope = isRecord(raw.scope) ? raw.scope : {};
  try {
    validateWorkspaceSourceId(rawScope.source_id);
  } catch {
    return null;
  }
  const cleanRecordId = validateCorrelationId(raw.source_record_id);
  if (cleanRecordId === null) {
    return null;
  }

  const { scope, missingFields } = normalizeMessageScope(rawScope);
  const rawComp = isRecord(raw.completeness) ? raw.completeness : {};
  if (Array.isArray(rawComp.missing_fields)) {
    for (const existing of rawComp.missing_fields) {
      const cleanName = validateCorrelationId(existing);
      if (cleanName !== null && !missingFields.includes(cleanName)) {
        missingFields.push(cleanName);
      }
    }
  }

  const rawOrder = isRecord(raw.source_order) ? raw.source_order : {};
  const recordedAtMs = cleanPositiveIntOrNull(rawOrder.recorded_at_ms);
  if (recordedAtMs === null && !missingFields.includes('recorded_at_ms')) {
    missingFields.push('recorded_at_ms');
  }

  const order: WorkspaceMessageSourceOrder = {
    recorded_at_ms: recordedAtMs,
    source_seq: cleanNonNegativeInt(rawOrder.source_seq, 0),
    sub_index: cleanNonNegativeInt(rawOrder.sub_index, 0),
  };

  const sanitized = sanitizeWorkspaceSummaryWithMeta(raw.summary);
  const prevOrigCount = cleanNonNegativeIntOrNull(rawComp.original_char_count);
  let originalCharCount: number | null = sanitized.original_char_count;
  if (originalCharCount !== null && prevOrigCount !== null) {
    originalCharCount = Math.max(prevOrigCount, originalCharCount);
  }

  return {
    item_id: formatWorkspaceMessageItemId(scope.source_id, kind, cleanRecordId, order.sub_index),
    source_kind: kind,
    source_record_id: cleanRecordId,
    source_order: order,
    scope,
    actor_role: validateCorrelationId(raw.actor_role),
    summary: sanitized.summary,
    completeness: {
      summary_truncated: Boolean(rawComp.summary_truncated) || sanitized.truncated,
      original_char_count: originalCharCount,
      max_summary_chars: MAX_OBSERVATION_SUMMARY_CHARS,
      redacted: Boolean(rawComp.redacted) || sanitized.redacted,
      missing_fields: missingFields,
      incomplete_reason: cleanOptionalSummary(rawComp.incomplete_reason),
    },
  };
}

function compareWorkspaceMessages(left: WorkspaceMessageItem, right: WorkspaceMessageItem): number {
  const leftHasTs = left.source_order.recorded_at_ms !== null ? 1 : 0;
  const rightHasTs = right.source_order.recorded_at_ms !== null ? 1 : 0;
  if (leftHasTs !== rightHasTs) {
    return leftHasTs - rightHasTs;
  }
  const leftTs = left.source_order.recorded_at_ms ?? 0;
  const rightTs = right.source_order.recorded_at_ms ?? 0;
  if (leftTs !== rightTs) {
    return leftTs - rightTs;
  }
  if (left.source_order.source_seq !== right.source_order.source_seq) {
    return left.source_order.source_seq - right.source_order.source_seq;
  }
  if (left.source_order.sub_index !== right.source_order.sub_index) {
    return left.source_order.sub_index - right.source_order.sub_index;
  }
  const sourceCmp = compareStringsUtf8(left.scope.source_id, right.scope.source_id);
  if (sourceCmp !== 0) {
    return sourceCmp;
  }
  const kindCmp = compareStringsUtf8(left.source_kind, right.source_kind);
  if (kindCmp !== 0) {
    return kindCmp;
  }
  const recordCmp = compareStringsUtf8(left.source_record_id, right.source_record_id);
  if (recordCmp !== 0) {
    return recordCmp;
  }
  return compareStringsUtf8(left.item_id, right.item_id);
}

export function normalizeWorkspaceMessagesWindow(raw: unknown): WorkspaceMessagesWindow {
  const obj = isRecord(raw) ? raw : {};
  const rawItems = Array.isArray(obj.items) ? obj.items : [];
  let droppedInvalid = 0;
  const byId = new Map<string, WorkspaceMessageItem>();

  for (const rawItem of rawItems) {
    const clean = normalizeSafeMessageItem(rawItem);
    if (clean === null) {
      droppedInvalid += 1;
    } else {
      byId.set(clean.item_id, clean);
    }
  }

  const ordered = Array.from(byId.values()).sort(compareWorkspaceMessages);
  const capped = capTail(ordered, MAX_WORKSPACE_MESSAGES_WINDOW);
  const baseOmitted = cleanNonNegativeInt(obj.omitted_count, 0);
  const omittedCount = baseOmitted + droppedInvalid + capped.omitted;
  const hasMore = Boolean(obj.has_more) || capped.omitted > 0;
  const truncated = Boolean(obj.truncated) || omittedCount > 0;

  return {
    availability: parseAvailabilityState(obj.availability),
    items: capped.items,
    max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
    omitted_count: omittedCount,
    has_more: hasMore,
    truncated,
    requires_transcript: false,
    allows_synthetic_user_messages: false,
  };
}

export function projectSafeWorkspaceMessages(
  inputs: readonly WorkspaceMessageCandidateInput[],
): WorkspaceMessagesWindow {
  const items = inputs.map((input) => createSafeWorkspaceMessageItem(input));
  return normalizeWorkspaceMessagesWindow({
    availability: 'available',
    items,
    max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
    omitted_count: 0,
    has_more: false,
    truncated: false,
    requires_transcript: false,
    allows_synthetic_user_messages: false,
  });
}

// ---------------------------------------------------------------------------
// `WorkspaceSnapshot` Envelope Normalization, Legacy Upgrade, and Regeneration
// ---------------------------------------------------------------------------

function normalizeLegacyObservationSnapshot(raw: unknown, fallbackGeneratedAtMs: number): {
  observation: ObservationSnapshot;
  requestsCapped: boolean;
  jobsCapped: boolean;
} {
  const obj = isRecord(raw) ? raw : {};
  const sources: ObservationSource[] = Array.isArray(obj.sources)
    ? (obj.sources as ObservationSource[])
    : [];
  const rawJobs: ObservationJob[] = Array.isArray(obj.jobs) ? (obj.jobs as ObservationJob[]) : [];
  const rawRequests: ObservationRequest[] = Array.isArray(obj.requests)
    ? (obj.requests as ObservationRequest[])
    : [];

  let jobs = rawJobs.slice();
  let jobsHasMore = Boolean(obj.jobs_has_more);
  let jobsCapped = false;
  if (jobs.length > MAX_OBSERVATION_JOBS) {
    jobs = jobs.slice(0, MAX_OBSERVATION_JOBS);
    jobsHasMore = true;
    jobsCapped = true;
  }

  let requests = rawRequests.slice();
  let hasMore = Boolean(obj.has_more);
  let requestsCapped = false;
  if (requests.length > MAX_OBSERVATION_REQUEST_PAGE_LIMIT) {
    requests = requests.slice(0, MAX_OBSERVATION_REQUEST_PAGE_LIMIT);
    hasMore = true;
    requestsCapped = true;
  }

  const warnings: string[] = Array.isArray(obj.warnings)
    ? obj.warnings.filter((w): w is string => typeof w === 'string')
    : [];
  const warningContexts: ObservationWarningContext[] | undefined = Array.isArray(
    obj.warning_contexts,
  )
    ? (obj.warning_contexts as ObservationWarningContext[])
    : undefined;

  const observation: ObservationSnapshot = {
    schema_version: OBSERVATION_SCHEMA_VERSION,
    generated_at_ms:
      typeof obj.generated_at_ms === 'number' && Number.isFinite(obj.generated_at_ms)
        ? Math.trunc(obj.generated_at_ms)
        : fallbackGeneratedAtMs,
    sources,
    jobs,
    requests,
    next_cursor: typeof obj.next_cursor === 'string' ? obj.next_cursor : null,
    has_more: hasMore,
    jobs_has_more: jobsHasMore,
    warnings,
    ...(warningContexts && warningContexts.length > 0 ? { warning_contexts: warningContexts } : {}),
  };

  return { observation, requestsCapped, jobsCapped };
}

export function upgradeLegacyObservationSnapshot(observation: ObservationSnapshot): WorkspaceSnapshot {
  const sourcePaths = new Map<string, string>();
  for (const src of observation.sources) {
    if (typeof src.project_path === 'string') {
      try {
        sourcePaths.set(src.id, validateCanonicalProjectScope(src.project_path));
      } catch {
        // Ignore non-canonical source paths during read-only legacy upgrade.
      }
    }
  }

  const chatsByKey = new Map<string, WorkspaceChatSummary>();
  const histories: WorkspaceJobHistory[] = [];

  for (const job of observation.jobs) {
    let scopePath: string | null = null;
    if (typeof job.project_path === 'string') {
      try {
        scopePath = validateCanonicalProjectScope(job.project_path);
      } catch {
        scopePath = null;
      }
    }
    if (scopePath === null) {
      scopePath = sourcePaths.get(job.source_id) ?? null;
    }

    let maybeIdentity: WorkspaceChatIdentity | null = null;
    if (job.binding_id !== null && job.thread_uuid !== null && scopePath !== null) {
      try {
        maybeIdentity = validateWorkspaceChatIdentity({
          source_id: job.source_id,
          binding_id: job.binding_id,
          thread_uuid: job.thread_uuid,
          canonical_project_scope: scopePath,
        });
      } catch {
        maybeIdentity = null;
      }
    }

    if (maybeIdentity !== null) {
      const chatKey = formatWorkspaceChatIdentityKey(maybeIdentity);
      if (chatKey !== null) {
        const isActive =
          job.state === 'planning' ||
          job.state === 'ready' ||
          job.state === 'running' ||
          job.state === 'reviewing' ||
          job.state === 'needs_user' ||
          job.active_call_count > 0;
        const needsAttention = job.state === 'needs_user';
        let entry = chatsByKey.get(chatKey);
        if (!entry) {
          entry = {
            identity: maybeIdentity,
            thread_label: sanitizeWorkspaceLabel(job.thread_label ?? 'Codex chat', 'Codex chat'),
            project_label: sanitizeWorkspaceLabel(job.project_label, 'Project'),
            availability: 'unavailable',
            activity: 'unknown',
            configured_selection: createUnavailableConfiguredSelection(),
            active_job_count: 0,
            attention_count: 0,
            updated_at_ms: job.updated_at_ms,
          };
          chatsByKey.set(chatKey, entry);
        }
        if (isActive) {
          entry.active_job_count += 1;
        }
        if (needsAttention) {
          entry.attention_count += 1;
        }
        if (
          job.updated_at_ms !== null &&
          (entry.updated_at_ms === null || job.updated_at_ms > entry.updated_at_ms)
        ) {
          entry.updated_at_ms = job.updated_at_ms;
        }
      }
    }

    histories.push(
      normalizeWorkspaceJobHistory({
        job: {
          source_id: job.source_id,
          job_id: job.id,
          binding_id: job.binding_id,
          chat_identity: maybeIdentity,
          title: job.title,
          job_revision: null,
          state: job.state,
          phase: null,
          approval_status: job.approval_status,
          execution_status: job.execution_status,
          review_state: job.review_state,
          created_at_ms: null,
          updated_at_ms: job.updated_at_ms,
          summary: job.summary,
        },
        tasks: job.tasks.map((t) => ({
          task_id: t.id,
          title: t.title,
          status: t.status,
          depends_on: t.depends_on,
          current_attempt_no: 0,
          attempts: [],
          candidates: [],
        })),
        links: {
          source_plan_id: job.plan_id,
          source_plan_revision: job.plan_revision,
        },
        acceptance_proof: { status: 'unknown' },
      }),
    );
  }

  return normalizeWorkspaceSnapshot({
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: observation.generated_at_ms,
    sampling_policy: DEFAULT_WORKSPACE_SAMPLING_POLICY,
    observation,
    chats: Array.from(chatsByKey.values()),
    native_samples: [],
    job_histories: histories,
    messages: {
      availability: 'unavailable',
      items: [],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    },
    chat_work: [],
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
  });
}

const WORKSPACE_BACKLOG_STATUSES: readonly WorkspaceBacklogStatus[] = [
  'todo',
  'doing',
  'done',
  'blocked',
] as const;
const WORKSPACE_BACKLOG_ITEM_KINDS: readonly WorkspaceBacklogItemKind[] = [
  'worker',
  'coworker',
] as const;
const WORKSPACE_BACKLOG_OUTCOMES: readonly WorkspaceBacklogOutcome[] = [
  'success',
  'failure',
  'unknown',
] as const;
const WORKSPACE_BACKLOG_PRESENCE_AVAILABILITIES: readonly WorkspaceBacklogPresenceAvailability[] = [
  'live',
  'non_live',
  'unavailable',
] as const;
const WORKSPACE_BACKLOG_PRESENCE_FRESHNESSES: readonly WorkspaceBacklogPresenceFreshness[] = [
  'fresh',
  'stale',
  'not_applicable',
] as const;

function parseBacklogStatus(raw: unknown): WorkspaceBacklogStatus | null {
  return typeof raw === 'string' && (WORKSPACE_BACKLOG_STATUSES as readonly string[]).includes(raw)
    ? (raw as WorkspaceBacklogStatus)
    : null;
}

function parseBacklogItemKind(raw: unknown): WorkspaceBacklogItemKind | null {
  return typeof raw === 'string' && (WORKSPACE_BACKLOG_ITEM_KINDS as readonly string[]).includes(raw)
    ? (raw as WorkspaceBacklogItemKind)
    : null;
}

function parseBacklogOutcome(raw: unknown): WorkspaceBacklogOutcome | null {
  return typeof raw === 'string' && (WORKSPACE_BACKLOG_OUTCOMES as readonly string[]).includes(raw)
    ? (raw as WorkspaceBacklogOutcome)
    : null;
}

function parseBacklogPresenceAvailability(
  raw: unknown,
): WorkspaceBacklogPresenceAvailability {
  return typeof raw === 'string' &&
    (WORKSPACE_BACKLOG_PRESENCE_AVAILABILITIES as readonly string[]).includes(raw)
    ? (raw as WorkspaceBacklogPresenceAvailability)
    : 'unavailable';
}

function parseBacklogPresenceFreshness(raw: unknown): WorkspaceBacklogPresenceFreshness {
  return typeof raw === 'string' &&
    (WORKSPACE_BACKLOG_PRESENCE_FRESHNESSES as readonly string[]).includes(raw)
    ? (raw as WorkspaceBacklogPresenceFreshness)
    : 'not_applicable';
}

function cleanBacklogToken(raw: unknown, maxBytes: number): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  if (
    raw.length === 0 ||
    raw.trim() !== raw ||
    utf8ByteLength(raw) > maxBytes ||
    !/^[A-Za-z0-9_.:-]+$/.test(raw) ||
    containsSensitiveMarker(raw.toLowerCase())
  ) {
    return null;
  }
  return raw;
}

function cleanBacklogText(raw: unknown, maxBytes: number, allowEmpty = false): string | null {
  if (typeof raw !== 'string') {
    return null;
  }
  if (utf8ByteLength(raw) > maxBytes || raw.includes('\0')) {
    return null;
  }
  if (!allowEmpty && raw.trim().length === 0) {
    return null;
  }
  return raw;
}

function cleanOptionalBacklogToken(raw: unknown, maxBytes: number): string | null | undefined {
  if (raw === null || raw === undefined) {
    return null;
  }
  const clean = cleanBacklogToken(raw, maxBytes);
  return clean === null ? undefined : clean;
}

function cleanOptionalBacklogText(raw: unknown, maxBytes: number): string | null | undefined {
  if (raw === null || raw === undefined) {
    return null;
  }
  const clean = cleanBacklogText(raw, maxBytes);
  return clean === null ? undefined : clean;
}

function normalizeBacklogItem(raw: unknown, bindingId: string): BacklogItem | null {
  if (!isRecord(raw)) {
    return null;
  }
  const itemId = cleanBacklogToken(raw.item_id, MAX_WORKSPACE_LABEL_BYTES);
  const itemBinding = cleanBacklogToken(raw.binding_id, MAX_WORKSPACE_LABEL_BYTES);
  const revision = cleanPositiveIntOrNull(raw.revision);
  const kind = parseBacklogItemKind(raw.kind);
  const status = parseBacklogStatus(raw.status);
  const title = cleanBacklogText(raw.title, 256);
  const acceptanceCondition = cleanBacklogText(raw.acceptance_condition, 2_048);
  const deliveryDescription = cleanBacklogText(raw.delivery_description, 2_048);
  if (
    itemId === null ||
    itemBinding === null ||
    itemBinding !== bindingId ||
    revision === null ||
    kind === null ||
    status === null ||
    title === null ||
    acceptanceCondition === null ||
    deliveryDescription === null
  ) {
    return null;
  }

  const blockedReason = cleanOptionalBacklogText(raw.blocked_reason, 1_024);
  const currentExecutionRef = cleanOptionalBacklogToken(raw.current_execution_ref, 512);
  const completionNote = cleanOptionalBacklogText(raw.completion_note, 2_048);
  const completionEvidence = cleanOptionalBacklogText(raw.completion_evidence, 2_048);
  const completedAtMs = cleanPositiveIntOrNull(raw.completed_at_ms);
  const createdAtMs = cleanPositiveIntOrNull(raw.created_at_ms);
  const updatedAtMs = cleanPositiveIntOrNull(raw.updated_at_ms);
  if (
    blockedReason === undefined ||
    currentExecutionRef === undefined ||
    completionNote === undefined ||
    completionEvidence === undefined ||
    createdAtMs === null ||
    updatedAtMs === null
  ) {
    return null;
  }
  if (status === 'blocked') {
    if (blockedReason === null) {
      return null;
    }
  } else if (blockedReason !== null) {
    return null;
  }
  if (status === 'done') {
    if (completionNote === null || completionEvidence === null || completedAtMs === null) {
      return null;
    }
  } else if (
    completionNote !== null ||
    completionEvidence !== null ||
    completedAtMs !== null
  ) {
    return null;
  }

  return {
    item_id: itemId,
    binding_id: itemBinding,
    revision,
    title,
    acceptance_condition: acceptanceCondition,
    delivery_description: deliveryDescription,
    kind,
    status,
    blocked_reason: blockedReason,
    current_execution_ref: currentExecutionRef,
    completion_note: completionNote,
    completion_evidence: completionEvidence,
    completed_at_ms: completedAtMs,
    created_at_ms: createdAtMs,
    updated_at_ms: updatedAtMs,
  };
}

function normalizeBacklogEvent(raw: unknown): WorkspaceBacklogEvent | null {
  if (!isRecord(raw)) {
    return null;
  }
  const eventId = cleanBacklogToken(raw.event_id, 512);
  const sequence = cleanPositiveIntOrNull(raw.sequence);
  const itemId = cleanBacklogToken(raw.item_id, MAX_WORKSPACE_LABEL_BYTES);
  const revision = cleanPositiveIntOrNull(raw.revision);
  const action = cleanBacklogToken(raw.action, 64);
  const recordedAtMs = cleanPositiveIntOrNull(raw.recorded_at_ms);
  const title = cleanBacklogText(raw.title, 256);
  const status = parseBacklogStatus(raw.status);
  const executionRef = cleanOptionalBacklogToken(raw.execution_ref, 512);
  const note = cleanOptionalBacklogText(raw.note, 2_048);
  let outcome: WorkspaceBacklogOutcome | null = null;
  if (raw.outcome !== null && raw.outcome !== undefined) {
    outcome = parseBacklogOutcome(raw.outcome);
    if (outcome === null) {
      return null;
    }
  }
  if (
    eventId === null ||
    sequence === null ||
    itemId === null ||
    revision === null ||
    action === null ||
    recordedAtMs === null ||
    title === null ||
    status === null ||
    executionRef === undefined ||
    note === undefined
  ) {
    return null;
  }
  return {
    event_id: eventId,
    sequence,
    item_id: itemId,
    revision,
    action,
    recorded_at_ms: recordedAtMs,
    title,
    status,
    execution_ref: executionRef,
    outcome,
    note,
  };
}

function normalizeBacklogPresence(raw: unknown): WorkspaceBacklogWorkerPresence | null {
  if (!isRecord(raw)) {
    return null;
  }
  const itemId = cleanBacklogToken(raw.item_id, MAX_WORKSPACE_LABEL_BYTES);
  const executionRef = cleanBacklogToken(raw.execution_ref, 512);
  const approvalId = cleanBacklogToken(raw.approval_id, MAX_WORKSPACE_LABEL_BYTES);
  const taskId = cleanBacklogToken(raw.task_id, MAX_WORKSPACE_LABEL_BYTES);
  const runId = cleanBacklogToken(raw.run_id, MAX_WORKSPACE_LABEL_BYTES);
  const workerId = cleanBacklogToken(raw.worker_id, MAX_WORKSPACE_LABEL_BYTES);
  const claimId = cleanOptionalBacklogToken(raw.claim_id, MAX_WORKSPACE_LABEL_BYTES);
  const hostInstanceId = cleanOptionalBacklogToken(raw.host_instance_id, MAX_WORKSPACE_LABEL_BYTES);
  const invocationId = cleanOptionalBacklogToken(raw.invocation_id, MAX_WORKSPACE_LABEL_BYTES);
  const sequence = cleanNonNegativeIntOrNull(raw.sequence);
  const availability = parseBacklogPresenceAvailability(raw.availability);
  const freshness = parseBacklogPresenceFreshness(raw.freshness);
  const sampledAtMs = cleanPositiveIntOrNull(raw.sampled_at_ms);
  const sampleAgeMs = cleanNonNegativeIntOrNull(raw.sample_age_ms);
  const maxSampleAgeMs =
    cleanNonNegativeIntOrNull(raw.max_sample_age_ms) ??
    MAX_WORKSPACE_BACKLOG_PRESENCE_SAMPLE_AGE_MS;
  const expiresAtMs = cleanPositiveIntOrNull(raw.expires_at_ms);
  if (
    itemId === null ||
    executionRef === null ||
    approvalId === null ||
    taskId === null ||
    runId === null ||
    workerId === null ||
    claimId === undefined ||
    hostInstanceId === undefined ||
    invocationId === undefined
  ) {
    return null;
  }
  return {
    item_id: itemId,
    execution_ref: executionRef,
    approval_id: approvalId,
    task_id: taskId,
    run_id: runId,
    worker_id: workerId,
    claim_id: claimId,
    host_instance_id: hostInstanceId,
    invocation_id: invocationId,
    sequence,
    availability,
    freshness,
    live:
      raw.live === true && availability === 'live' && freshness === 'fresh',
    sampled_at_ms: sampledAtMs,
    sample_age_ms: sampleAgeMs,
    max_sample_age_ms: maxSampleAgeMs,
    expires_at_ms: expiresAtMs,
  };
}

function normalizeWorkspaceChatWorkEntry(raw: unknown): WorkspaceChatWork | null {
  if (!isRecord(raw)) {
    return null;
  }
  let identity: WorkspaceChatIdentity;
  try {
    identity = validateWorkspaceChatIdentity(raw.identity);
  } catch {
    return null;
  }
  const availability = raw.availability === 'available' ? 'available' : 'unavailable';
  const rawBounds = isRecord(raw.bounds) ? raw.bounds : {};
  const bounds: WorkspaceChatWorkBounds = {
    items_omitted: cleanNonNegativeInt(rawBounds.items_omitted, 0),
    events_omitted: cleanNonNegativeInt(rawBounds.events_omitted, 0),
    presence_omitted: cleanNonNegativeInt(rawBounds.presence_omitted, 0),
  };

  let itemsOmitted = 0;
  const itemsById = new Map<string, BacklogItem>();
  if (Array.isArray(raw.items)) {
    for (const entry of raw.items) {
      const item = normalizeBacklogItem(entry, identity.binding_id);
      if (item === null) {
        itemsOmitted += 1;
        continue;
      }
      const existing = itemsById.get(item.item_id);
      if (existing === undefined || item.revision > existing.revision) {
        if (existing !== undefined) {
          itemsOmitted += 1;
        }
        itemsById.set(item.item_id, item);
      } else {
        itemsOmitted += 1;
      }
    }
  }
  let items = Array.from(itemsById.values()).sort(
    (left, right) =>
      left.created_at_ms - right.created_at_ms ||
      compareStringsUtf8(left.item_id, right.item_id),
  );
  if (items.length > MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT) {
    const overflow = items.length - MAX_WORKSPACE_CHAT_WORK_ITEMS_PER_CHAT;
    items = items.slice(overflow);
    itemsOmitted += overflow;
  }
  const itemById = new Map(items.map((item) => [item.item_id, item] as const));

  let eventsOmitted = 0;
  const eventsById = new Map<string, WorkspaceBacklogEvent>();
  if (Array.isArray(raw.events)) {
    for (const entry of raw.events) {
      const event = normalizeBacklogEvent(entry);
      if (event === null || event.event_id !== `${event.item_id}:${event.revision}`) {
        eventsOmitted += 1;
        continue;
      }
      const existing = eventsById.get(event.event_id);
      if (
        existing === undefined ||
        event.sequence < existing.sequence ||
        (event.sequence === existing.sequence && event.recorded_at_ms > existing.recorded_at_ms)
      ) {
        if (existing !== undefined) {
          eventsOmitted += 1;
        }
        eventsById.set(event.event_id, event);
      } else {
        eventsOmitted += 1;
      }
    }
  }
  let events = Array.from(eventsById.values()).sort(
    (left, right) =>
      left.sequence - right.sequence || compareStringsUtf8(left.event_id, right.event_id),
  );
  if (events.length > MAX_WORKSPACE_CHAT_WORK_EVENTS_PER_CHAT) {
    const overflow = events.length - MAX_WORKSPACE_CHAT_WORK_EVENTS_PER_CHAT;
    events = events.slice(overflow);
    eventsOmitted += overflow;
  }

  let presenceOmitted = 0;
  const presenceByKey = new Map<string, WorkspaceBacklogWorkerPresence>();
  if (Array.isArray(raw.worker_presence)) {
    for (const entry of raw.worker_presence) {
      const presence = normalizeBacklogPresence(entry);
      const ownerItem = presence === null ? undefined : itemById.get(presence.item_id);
      if (
        presence === null ||
        ownerItem === undefined ||
        ownerItem.kind !== 'worker' ||
        (ownerItem.status !== 'doing' && ownerItem.status !== 'blocked') ||
        ownerItem.current_execution_ref !== presence.execution_ref
      ) {
        presenceOmitted += 1;
        continue;
      }
      const key = `${presence.item_id}\u0000${presence.execution_ref}`;
      const existing = presenceByKey.get(key);
      if (existing === undefined || compareBacklogPresence(presence, existing) < 0) {
        if (existing !== undefined) {
          presenceOmitted += 1;
        }
        presenceByKey.set(key, presence);
      } else {
        presenceOmitted += 1;
      }
    }
  }
  let workerPresence = Array.from(presenceByKey.values()).sort(compareBacklogPresence);
  if (workerPresence.length > MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT) {
    const overflow = workerPresence.length - MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT;
    workerPresence = workerPresence.slice(0, MAX_WORKSPACE_CHAT_WORK_PRESENCE_PER_CHAT);
    presenceOmitted += overflow;
  }

  return {
    identity,
    availability,
    items,
    events,
    worker_presence: workerPresence,
    bounds: {
      items_omitted: bounds.items_omitted + itemsOmitted,
      events_omitted: bounds.events_omitted + eventsOmitted,
      presence_omitted: bounds.presence_omitted + presenceOmitted,
    },
  };
}

function compareBacklogPresence(
  left: WorkspaceBacklogWorkerPresence,
  right: WorkspaceBacklogWorkerPresence,
): number {
  if (left.live !== right.live) {
    return left.live ? -1 : 1;
  }
  const leftSampled = left.sampled_at_ms ?? Number.MIN_SAFE_INTEGER;
  const rightSampled = right.sampled_at_ms ?? Number.MIN_SAFE_INTEGER;
  if (leftSampled !== rightSampled) {
    return rightSampled - leftSampled;
  }
  const leftSequence = left.sequence ?? 0;
  const rightSequence = right.sequence ?? 0;
  if (leftSequence !== rightSequence) {
    return rightSequence - leftSequence;
  }
  return (
    compareStringsUtf8(left.item_id, right.item_id) ||
    compareStringsUtf8(left.execution_ref, right.execution_ref)
  );
}

function normalizeWorkspaceChatWorkList(raw: unknown): WorkspaceChatWork[] {
  const merged = new Map<
    string,
    {
      identity: WorkspaceChatIdentity;
      availability: 'available' | 'unavailable';
      items: BacklogItem[];
      events: WorkspaceBacklogEvent[];
      worker_presence: WorkspaceBacklogWorkerPresence[];
      bounds: WorkspaceChatWorkBounds;
    }
  >();
  if (Array.isArray(raw)) {
    for (const entry of raw) {
      const work = normalizeWorkspaceChatWorkEntry(entry);
      if (work === null) {
        continue;
      }
      const key = formatWorkspaceChatIdentityKey(work.identity);
      if (key === null) {
        continue;
      }
      const existing = merged.get(key);
      if (existing === undefined) {
        merged.set(key, {
          identity: work.identity,
          availability: work.availability,
          items: work.items.slice(),
          events: work.events.slice(),
          worker_presence: work.worker_presence.slice(),
          bounds: { ...work.bounds },
        });
      } else {
        if (work.availability === 'available') {
          existing.availability = 'available';
        }
        existing.items.push(...work.items);
        existing.events.push(...work.events);
        existing.worker_presence.push(...work.worker_presence);
        existing.bounds.items_omitted += work.bounds.items_omitted;
        existing.bounds.events_omitted += work.bounds.events_omitted;
        existing.bounds.presence_omitted += work.bounds.presence_omitted;
      }
    }
  }

  const sortedKeys = Array.from(merged.keys()).sort(compareStringsUtf8);
  let chatWork: WorkspaceChatWork[] = [];
  for (const key of sortedKeys) {
    const entry = merged.get(key);
    if (!entry) {
      continue;
    }
    const normalized = normalizeWorkspaceChatWork(entry);
    if (normalized !== null) {
      chatWork.push(normalized);
    }
  }

  let totalItems = chatWork.reduce((sum, work) => sum + work.items.length, 0);
  let totalEvents = chatWork.reduce((sum, work) => sum + work.events.length, 0);
  let totalPresence = chatWork.reduce((sum, work) => sum + work.worker_presence.length, 0);
  if (totalItems > MAX_WORKSPACE_CHAT_WORK_ITEMS_TOTAL) {
    for (let index = chatWork.length - 1; index >= 0; index -= 1) {
      if (totalItems <= MAX_WORKSPACE_CHAT_WORK_ITEMS_TOTAL) {
        break;
      }
      const work = chatWork[index]!;
      const overflow = totalItems - MAX_WORKSPACE_CHAT_WORK_ITEMS_TOTAL;
      const remove = Math.min(overflow, work.items.length);
      if (remove > 0) {
        work.items = work.items.slice(remove);
        work.bounds.items_omitted += remove;
        totalItems -= remove;
        const remainingItemIds = new Set(work.items.map((item) => item.item_id));
        const presenceBefore = work.worker_presence.length;
        work.worker_presence = work.worker_presence.filter((presence) =>
          remainingItemIds.has(presence.item_id),
        );
        const removedPresence = presenceBefore - work.worker_presence.length;
        if (removedPresence > 0) {
          work.bounds.presence_omitted += removedPresence;
          totalPresence -= removedPresence;
        }
      }
    }
  }

  if (totalEvents > MAX_WORKSPACE_CHAT_WORK_EVENTS_TOTAL) {
    for (let index = chatWork.length - 1; index >= 0; index -= 1) {
      if (totalEvents <= MAX_WORKSPACE_CHAT_WORK_EVENTS_TOTAL) {
        break;
      }
      const work = chatWork[index]!;
      const overflow = totalEvents - MAX_WORKSPACE_CHAT_WORK_EVENTS_TOTAL;
      const remove = Math.min(overflow, work.events.length);
      if (remove > 0) {
        work.events = work.events.slice(remove);
        work.bounds.events_omitted += remove;
        totalEvents -= remove;
      }
    }
  }

  if (totalPresence > MAX_WORKSPACE_CHAT_WORK_PRESENCE_TOTAL) {
    for (let index = chatWork.length - 1; index >= 0; index -= 1) {
      if (totalPresence <= MAX_WORKSPACE_CHAT_WORK_PRESENCE_TOTAL) {
        break;
      }
      const work = chatWork[index]!;
      const overflow = totalPresence - MAX_WORKSPACE_CHAT_WORK_PRESENCE_TOTAL;
      const remove = Math.min(overflow, work.worker_presence.length);
      if (remove > 0) {
        work.worker_presence = work.worker_presence.slice(
          0,
          work.worker_presence.length - remove,
        );
        work.bounds.presence_omitted += remove;
        totalPresence -= remove;
      }
    }
  }

  return chatWork;
}

export function normalizeWorkspaceChatWork(raw: unknown): WorkspaceChatWork | null {
  return normalizeWorkspaceChatWorkEntry(raw);
}

export function normalizeWorkspaceSnapshot(raw: unknown): WorkspaceSnapshot {
  const obj = isRecord(raw) ? raw : {};
  const generatedAtMs =
    typeof obj.generated_at_ms === 'number' && Number.isFinite(obj.generated_at_ms)
      ? Math.trunc(obj.generated_at_ms)
      : 0;
  const samplingPolicy = normalizeWorkspaceSamplingPolicy(obj.sampling_policy);
  const { observation, requestsCapped, jobsCapped } = normalizeLegacyObservationSnapshot(
    obj.observation,
    0,
  );

  const rawSamples = Array.isArray(obj.native_samples)
    ? obj.native_samples.map((s) => (isRecord(s) ? normalizeWorkspaceNativeSample(s) : null))
    : [];
  const preValidSamples: WorkspaceNativeSample[] = [];
  let invalidSamplesCount = 0;
  for (const s of rawSamples) {
    if (s === null) {
      invalidSamplesCount += 1;
    } else {
      preValidSamples.push(s);
    }
  }
  const boundedSamples = boundWorkspaceNativeSamplesPerSource(preValidSamples);

  const evaluatedByChat = new Map<
    string,
    { sample: WorkspaceNativeSample; eval: WorkspaceEvaluatedNativeSample }
  >();
  for (const sample of boundedSamples.samples) {
    const chatKey = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (chatKey !== null) {
      evaluatedByChat.set(chatKey, {
        sample,
        eval: evaluateWorkspaceNativeSampleAt(sample, generatedAtMs, samplingPolicy.stale_threshold_ms),
      });
    }
  }

  const chatsByKey = new Map<string, WorkspaceChatSummary>();
  if (Array.isArray(obj.chats)) {
    for (const rawChat of obj.chats) {
      const normChat = normalizeWorkspaceChatSummary(rawChat);
      const chatKey = formatWorkspaceChatIdentityKey(normChat.identity);
      if (chatKey !== null) {
        chatsByKey.set(chatKey, normChat);
      }
    }
  }

  for (const [chatKey, { sample, eval: evaluated }] of evaluatedByChat.entries()) {
    let entry = chatsByKey.get(chatKey);
    if (!entry) {
      entry = {
        identity: sample.chat_identity,
        thread_label: 'Codex chat',
        project_label: 'Project',
        availability: evaluated.effective_availability,
        activity: evaluated.effective_activity,
        configured_selection: evaluated.effective_configured_selection,
        active_job_count: 0,
        attention_count: 0,
        updated_at_ms: sample.observed_at_ms,
      };
      chatsByKey.set(chatKey, entry);
    } else {
      entry.availability = evaluated.effective_availability;
      entry.activity = evaluated.effective_activity;
      entry.configured_selection = evaluated.effective_configured_selection;
      if (
        sample.observed_at_ms !== null &&
        (entry.updated_at_ms === null || sample.observed_at_ms > entry.updated_at_ms)
      ) {
        entry.updated_at_ms = sample.observed_at_ms;
      }
    }
  }

  const sortedChatKeys = Array.from(chatsByKey.keys()).sort(compareStringsUtf8);
  const chats: WorkspaceChatSummary[] = [];
  for (const key of sortedChatKeys) {
    const item = chatsByKey.get(key);
    if (item) {
      chats.push(item);
    }
  }

  const rawHistories = Array.isArray(obj.job_histories)
    ? obj.job_histories.map(normalizeWorkspaceJobHistory)
    : [];
  const boundedHistories = boundWorkspaceJobHistories(rawHistories);
  const messages = normalizeWorkspaceMessagesWindow(obj.messages);
  const chatWork = normalizeWorkspaceChatWorkList(obj.chat_work);
  const rawBounds = isRecord(obj.bounds) ? obj.bounds : {};

  return {
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: generatedAtMs,
    sampling_policy: samplingPolicy,
    observation,
    chats,
    native_samples: boundedSamples.samples,
    job_histories: boundedHistories.histories,
    messages,
    chat_work: chatWork,
    bounds: {
      native_samples_omitted:
        cleanNonNegativeInt(rawBounds.native_samples_omitted, 0) +
        invalidSamplesCount +
        boundedSamples.omittedCount,
      job_histories_omitted:
        cleanNonNegativeInt(rawBounds.job_histories_omitted, 0) + boundedHistories.omittedCount,
      messages_omitted: messages.omitted_count,
      requests_capped: Boolean(rawBounds.requests_capped) || requestsCapped,
      jobs_capped: Boolean(rawBounds.jobs_capped) || jobsCapped,
    },
  };
}

export function parseWorkspaceSnapshot(input: unknown): WorkspaceSnapshot {
  let value: unknown = input;
  if (typeof input === 'string') {
    try {
      value = JSON.parse(input);
    } catch (err) {
      throw new WorkspaceContractError(
        'invalid_json',
        `failed to parse WorkspaceSnapshot JSON: ${String(err)}`,
      );
    }
  }
  if (!isRecord(value)) {
    throw new WorkspaceContractError(
      'invalid_json',
      'WorkspaceSnapshot payload must be a JSON object',
    );
  }

  const hasWorkspaceObservation = isRecord(value.observation);
  const looksLikeLegacyObservation =
    !hasWorkspaceObservation &&
    ('sources' in value ||
      'requests' in value ||
      ('jobs' in value && !('job_histories' in value)));

  if (looksLikeLegacyObservation) {
    const { observation } = normalizeLegacyObservationSnapshot(value, 0);
    return upgradeLegacyObservationSnapshot(observation);
  }

  return normalizeWorkspaceSnapshot(value);
}

/**
 * Re-assembles a `WorkspaceSnapshot` at `nextGeneratedAtMs` without resetting
 * `observed_at_ms` or `last_success_at_ms` on any `WorkspaceNativeSample`.
 */
export function regenerateWorkspaceSnapshotAt(
  snapshot: WorkspaceSnapshot,
  nextGeneratedAtMs: number,
): WorkspaceSnapshot {
  const ts = Math.trunc(nextGeneratedAtMs);
  return normalizeWorkspaceSnapshot({
    ...snapshot,
    generated_at_ms: ts,
    observation: {
      ...snapshot.observation,
      generated_at_ms: ts,
    },
  });
}

// ---------------------------------------------------------------------------
// Per-Chat Active / Archive Preference Persistence Helpers
// ---------------------------------------------------------------------------

export function readWorkspaceChatArchivePreferences(
  storage: Pick<WorkspaceChatPreferenceStorage, 'getItem'> | null | undefined,
): WorkspaceChatArchivePreferenceMap {
  if (!storage) {
    return {};
  }
  let raw: string | null = null;
  try {
    raw = storage.getItem(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY);
  } catch {
    return {};
  }
  if (typeof raw !== 'string' || raw.trim().length === 0) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (!isRecord(parsed)) {
    return {};
  }

  const entries: WorkspaceChatArchiveEntry[] = [];
  for (const [storedKey, rawEntry] of Object.entries(parsed)) {
    if (!isRecord(rawEntry)) {
      continue;
    }
    if (!isValidWorkspaceChatIdentity(rawEntry.identity)) {
      continue;
    }
    const validIdentity = validateWorkspaceChatIdentity(rawEntry.identity);
    const canonicalKey = formatWorkspaceChatIdentityKey(validIdentity);
    if (canonicalKey === null || canonicalKey !== storedKey) {
      continue;
    }
    if (rawEntry.lifecycle !== 'archived') {
      continue;
    }
    entries.push({
      chat_key: canonicalKey,
      identity: validIdentity,
      lifecycle: 'archived',
      updated_at_ms: cleanPositiveIntOrNull(rawEntry.updated_at_ms) ?? 1,
    });
  }

  entries.sort(
    (a, b) => b.updated_at_ms - a.updated_at_ms || compareStringsUtf8(a.chat_key, b.chat_key),
  );
  const out: Record<string, WorkspaceChatArchiveEntry> = {};
  for (const entry of entries.slice(0, MAX_WORKSPACE_ARCHIVED_CHAT_PREFERENCES)) {
    out[entry.chat_key] = entry;
  }
  return out;
}

export function setWorkspaceChatArchivePreference(
  current: WorkspaceChatArchivePreferenceMap,
  identity: WorkspaceChatIdentity,
  lifecycle: WorkspaceChatLifecycle,
  updatedAtMs: number,
): WorkspaceChatArchivePreferenceMap {
  const validIdentity = validateWorkspaceChatIdentity(identity);
  const canonicalKey = formatWorkspaceChatIdentityKey(validIdentity);
  if (canonicalKey === null) {
    throw new WorkspaceContractError(
      'invalid_chat_identity',
      'cannot compute canonical chat key for archive preference',
    );
  }

  const entries: WorkspaceChatArchiveEntry[] = [];
  for (const existing of Object.values(current)) {
    if (existing.chat_key !== canonicalKey && existing.lifecycle === 'archived') {
      entries.push(existing);
    }
  }

  if (lifecycle === 'archived') {
    entries.push({
      chat_key: canonicalKey,
      identity: validIdentity,
      lifecycle: 'archived',
      updated_at_ms: cleanPositiveIntOrNull(updatedAtMs) ?? 1,
    });
  }

  entries.sort(
    (a, b) => b.updated_at_ms - a.updated_at_ms || compareStringsUtf8(a.chat_key, b.chat_key),
  );
  const out: Record<string, WorkspaceChatArchiveEntry> = {};
  for (const entry of entries.slice(0, MAX_WORKSPACE_ARCHIVED_CHAT_PREFERENCES)) {
    out[entry.chat_key] = entry;
  }
  return out;
}

export function writeWorkspaceChatArchivePreferences(
  storage: Pick<WorkspaceChatPreferenceStorage, 'setItem'> | null | undefined,
  preferences: WorkspaceChatArchivePreferenceMap,
): boolean {
  if (!storage) {
    return false;
  }
  try {
    storage.setItem(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY, JSON.stringify(preferences));
    return true;
  } catch {
    return false;
  }
}

export function resolveWorkspaceChatLifecycle(
  preferences: WorkspaceChatArchivePreferenceMap,
  identity: WorkspaceChatIdentity,
): WorkspaceChatLifecycle {
  const canonicalKey = formatWorkspaceChatIdentityKey(identity);
  if (canonicalKey === null) {
    return 'active';
  }
  const entry = preferences[canonicalKey];
  return entry?.lifecycle === 'archived' ? 'archived' : 'active';
}

// ---------------------------------------------------------------------------
// Compact Native Tray Content Bounds Enforcement
// ---------------------------------------------------------------------------

function trayRowKindPriority(kind: WorkspaceTrayRowKind): number {
  if (kind === 'needs_human') {
    return 0;
  }
  if (kind === 'working') {
    return 1;
  }
  return 2;
}

export function boundWorkspaceCompactTrayRows(
  rows: readonly WorkspaceCompactTrayRow[],
): WorkspaceCompactTrayRow[] {
  const sanitized: WorkspaceCompactTrayRow[] = rows.map((row) => {
    const validIdentity =
      row.chat_identity !== null && isValidWorkspaceChatIdentity(row.chat_identity)
        ? validateWorkspaceChatIdentity(row.chat_identity)
        : null;
    return {
      id: validateCorrelationId(row.id) ?? 'tray_row',
      kind:
        row.kind === 'needs_human' || row.kind === 'working' || row.kind === 'result'
          ? row.kind
          : 'result',
      title: truncateUnicodeChars(
        sanitizeWorkspaceLabel(row.title, 'Codex chat'),
        WORKSPACE_TRAY_MAX_TITLE_CHARS,
      ),
      detail: truncateUnicodeChars(row.detail, WORKSPACE_TRAY_MAX_DETAIL_CHARS),
      updated_at_ms: cleanPositiveIntOrNull(row.updated_at_ms),
      chat_identity: validIdentity,
      job_id: validateCorrelationId(row.job_id),
    };
  });

  sanitized.sort((left, right) => {
    const prioDiff = trayRowKindPriority(left.kind) - trayRowKindPriority(right.kind);
    if (prioDiff !== 0) {
      return prioDiff;
    }
    const leftTs = left.updated_at_ms ?? Number.MIN_SAFE_INTEGER;
    const rightTs = right.updated_at_ms ?? Number.MIN_SAFE_INTEGER;
    if (rightTs !== leftTs) {
      return rightTs - leftTs;
    }
    return compareStringsUtf8(left.id, right.id);
  });

  const out: WorkspaceCompactTrayRow[] = [];
  let terminalResultsCount = 0;
  for (const row of sanitized) {
    if (out.length >= WORKSPACE_TRAY_MAX_ROWS) {
      break;
    }
    if (row.kind === 'result') {
      if (terminalResultsCount >= WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS) {
        continue;
      }
      terminalResultsCount += 1;
    }
    out.push(row);
  }
  return out;
}
