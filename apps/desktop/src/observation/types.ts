export type ObservationLifecycleState =
  | 'queued'
  | 'running'
  | 'waiting_for_user'
  | 'completed'
  | 'ready_for_review'
  | 'failed'
  | 'cancelled'
  | 'unknown';

export type ObservationEvidence = 'unknown' | 'configured' | 'observed' | 'operator';

export type ObservationDeliveryStage =
  | 'result_ready'
  | 'notification_submitted'
  | 'batch_delivered'
  | 'batch_acknowledged'
  | 'response_returned'
  | 'unknown'
  | 'failed';

export type ObservationJobState =
  | 'planning'
  | 'ready'
  | 'running'
  | 'reviewing'
  | 'needs_user'
  | 'completed'
  | 'failed'
  | 'unknown';

export interface ObservationSource {
  id: string;
  label: string;
  project_path: string | null;
  availability: 'available' | 'unavailable';
  message: string | null;
}

export interface ObservationActor {
  label: string | null;
  role: string | null;
  source: string | null;
  model: string | null;
  effort: string | null;
  selected_model?: string | null;
  selected_effort?: string | null;
  evidence: ObservationEvidence;
}

export interface ObservationExecutor {
  tool: string | null;
  provider: string | null;
  role: string | null;
  requested_model: string | null;
  observed_model: string | null;
  requested_effort: string | null;
  observed_effort: string | null;
}

export interface ObservationUsage {
  input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  total_tokens: number | null;
  provenance: string;
}

export interface ObservationDelivery {
  stage: ObservationDeliveryStage;
  at_ms: number | null;
  summary: string;
  recipient: string | null;
}

export interface ObservationCall {
  id: string;
  title: string;
  state: ObservationLifecycleState;
  executor: ObservationExecutor | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  duration_ms: number | null;
  usage: ObservationUsage | null;
  summary: string | null;
}

export interface ObservationRequest {
  source_id: string;
  id: string;
  job_id: string | null;
  parent_request_id: string | null;
  title: string;
  operation: string;
  project_label: string;
  binding_id: string | null;
  thread_uuid: string | null;
  thread_label: string | null;
  state: ObservationLifecycleState;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  updated_at_ms: number;
  duration_ms: number | null;
  actor: ObservationActor;
  executor: ObservationExecutor | null;
  usage: ObservationUsage | null;
  delivery: ObservationDelivery[];
  children: ObservationCall[];
  children_count: number;
  summary: string | null;
}

export interface ObservationTask {
  id: string;
  title: string;
  status: string;
  depends_on: string[];
}

export interface ObservationJob {
  source_id: string;
  id: string;
  project_label: string;
  project_path: string | null;
  title: string;
  thread_uuid: string | null;
  thread_label: string | null;
  binding_id: string | null;
  plan_id: string | null;
  plan_revision: number | null;
  state: ObservationJobState;
  approval_status: string;
  execution_status: string;
  review_state: string | null;
  updated_at_ms: number | null;
  active_call_count: number;
  tasks: ObservationTask[];
  summary: string | null;
}

export type ObservationWarningOrigin =
  | 'registry'
  | 'jobs'
  | 'journal'
  | 'requests'
  | 'snapshot'
  | (string & {});

export interface ObservationWarningContext {
  warning: string;
  source_id: string | null;
  origin: ObservationWarningOrigin;
}

export interface ObservationSnapshot {
  schema_version: 1;
  generated_at_ms: number;
  sources: ObservationSource[];
  jobs: ObservationJob[];
  requests: ObservationRequest[];
  next_cursor: string | null;
  has_more: boolean;
  jobs_has_more: boolean;
  warnings: string[];
  warning_contexts?: ObservationWarningContext[];
}

export interface ObservationSnapshotRequest {
  source_id?: string | null;
  job_id?: string | null;
  cursor?: string | null;
  limit?: number;
}

export const OBSERVATION_SCHEMA_VERSION = 1 as const;
export const WORKSPACE_SNAPSHOT_SCHEMA_VERSION = 1 as const;

export const DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT = 50 as const;
export const MAX_OBSERVATION_REQUEST_PAGE_LIMIT = 200 as const;
export const MAX_OBSERVATION_JOBS = 500 as const;
export const MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW = 1_000 as const;
export const MAX_OBSERVATION_SUMMARY_CHARS = 240 as const;

export const MAX_WORKSPACE_LABEL_BYTES = 128 as const;
export const MAX_WORKSPACE_CANONICAL_PATH_BYTES = 512 as const;
export const MAX_WORKSPACE_NATIVE_SAMPLE_SIDECAR_BYTES = 65_536 as const;
export const MAX_WORKSPACE_NATIVE_SAMPLES_PER_SOURCE = 128 as const;
export const MAX_WORKSPACE_MESSAGES_WINDOW = 1_000 as const;

export const MAX_WORKSPACE_JOBS_PER_BINDING = 64 as const;
export const MAX_WORKSPACE_TASKS_PER_JOB = 64 as const;
export const MAX_WORKSPACE_ATTEMPTS_PER_TASK = 16 as const;
export const MAX_WORKSPACE_CANDIDATES_PER_TASK = 16 as const;
export const MAX_WORKSPACE_RUNS_PER_JOB = 256 as const;
export const MAX_WORKSPACE_OUTBOX_PER_JOB = 256 as const;
export const MAX_WORKSPACE_COMBINED_CANDIDATES_PER_JOB = 32 as const;
export const MAX_WORKSPACE_RECORD_LINKS_PER_LIST = 64 as const;
export const MAX_WORKSPACE_EXTERNAL_RESOLUTIONS_PER_JOB =
  MAX_WORKSPACE_RECORD_LINKS_PER_LIST;
export const WORKSPACE_EXTERNAL_RESOLUTION_VIEW_CONTRACT_VERSION =
  'workspace_external_resolution_view_v1' as const;

export const WORKSPACE_SAMPLING_INTERVAL_MS = 2_000 as const;
export const WORKSPACE_METADATA_TIMEOUT_MS = 750 as const;
export const WORKSPACE_SAMPLE_STALE_THRESHOLD_MS = 10_000 as const;

export interface ObservationProcessInstance {
  pid: number;
  boot_id: string;
  start_ticks: number;
}

export type WorkspaceAvailabilityState = 'available' | 'disconnected' | 'stale' | 'unavailable';

export type WorkspaceActivityState =
  | 'working'
  | 'idle'
  | 'waiting_for_approval'
  | 'waiting_for_input'
  | 'unknown';

export type WorkspaceConnectionState = 'connected' | 'disconnected' | 'degraded' | 'unavailable';

export type WorkspaceFreshnessState = 'fresh' | 'stale' | 'never_succeeded' | 'unknown';

export type WorkspaceCapabilityState = 'supported' | 'unsupported' | 'unknown';

export type WorkspaceSelectionEvidence = 'verified_configuration' | 'unavailable';

export type WorkspaceAcceptanceStatus =
  | 'verified_accepted'
  | 'pending_review'
  | 'rejected'
  | 'not_accepted'
  | 'unknown';

export type WorkspaceAttentionKind =
  | 'needs_approval'
  | 'needs_user_input'
  | 'on_hold'
  | 'unresolved_effect'
  | 'blocked_failure'
  | 'review_required'
  | 'none'
  | 'unknown';

export type WorkspaceExternalResolutionOutcome = 'resolved_externally';

export type WorkspaceExternalResolutionScope = 'job' | 'tasks';

export type WorkspaceExternalResolutionApplicability =
  | 'applicable'
  | 'stale'
  | 'unverified';

export type WorkspaceSafeMessageSourceKind =
  | 'work_brief_summary'
  | 'plan_summary'
  | 'question_prompt'
  | 'question_answer'
  | 'task_result_summary'
  | 'review_verdict_summary'
  | 'closure_summary'
  | 'job_result_summary'
  | 'attention_summary'
  | 'journal_summary';

export type WorkspaceMessageSourceKind = WorkspaceSafeMessageSourceKind | 'unknown';

export interface WorkspaceChatIdentity {
  source_id: string;
  binding_id: string;
  thread_uuid: string;
  canonical_project_scope: string;
}

export interface WorkspaceSamplingPolicy {
  interval_ms: number;
  metadata_timeout_ms: number;
  stale_threshold_ms: number;
  snapshot_resets_sample_age: false;
}

export interface WorkspaceConfiguredSelection {
  selected_model: string | null;
  selected_effort: string | null;
  evidence: WorkspaceSelectionEvidence;
  counts_as_runtime_telemetry: false;
}

export interface WorkspaceSampleConnection {
  state: WorkspaceConnectionState;
  freshness: WorkspaceFreshnessState;
  route: string | null;
  diagnostic: string | null;
}

export interface WorkspaceNativeCapabilities {
  exact_thread_read: WorkspaceCapabilityState;
  activity_detail: WorkspaceCapabilityState;
  configured_selection: WorkspaceCapabilityState;
  wakeup_delivery: WorkspaceCapabilityState;
  per_call_identity: WorkspaceCapabilityState;
}

export interface WorkspaceNativeSample {
  chat_identity: WorkspaceChatIdentity;
  producer_epoch: string;
  backend_process_instance: ObservationProcessInstance | null;
  sequence: number;
  observed_at_ms: number | null;
  last_success_at_ms: number | null;
  availability: WorkspaceAvailabilityState;
  activity: WorkspaceActivityState;
  connection: WorkspaceSampleConnection;
  capabilities: WorkspaceNativeCapabilities;
  configured_selection: WorkspaceConfiguredSelection;
}

export interface WorkspaceEvaluatedNativeSample {
  sample_age_ms: number | null;
  freshness: WorkspaceFreshnessState;
  effective_availability: WorkspaceAvailabilityState;
  effective_activity: WorkspaceActivityState;
  effective_configured_selection: WorkspaceConfiguredSelection;
}

export interface WorkspaceChatSummary {
  identity: WorkspaceChatIdentity;
  thread_label: string;
  project_label: string;
  availability: WorkspaceAvailabilityState;
  activity: WorkspaceActivityState;
  configured_selection: WorkspaceConfiguredSelection;
  active_job_count: number;
  attention_count: number;
  updated_at_ms: number | null;
}

export interface WorkspaceAcceptanceProof {
  status: WorkspaceAcceptanceStatus;
  accepted_candidate_id: string | null;
  combined_candidate_id: string | null;
  review_verdict_id: string | null;
  review_receipt_id: string | null;
  integration_receipt_id: string | null;
  closure_reservation_id: string | null;
  closure_verdict_id: string | null;
  verified_at_ms: number | null;
}

export interface WorkspaceHistoricalFailure {
  failure_id: string | null;
  stage: string;
  failed_at_ms: number | null;
  task_id: string | null;
  attempt_no: number | null;
  run_id: string | null;
  outbox_id: string | null;
  reason: string;
  superseded_or_resolved: boolean;
}

export interface WorkspaceAttentionState {
  kind: WorkspaceAttentionKind;
  reason: string;
  next_step: string | null;
  blocking_hold_ids: string[];
  blocking_outbox_ids: string[];
  question_id: string | null;
  updated_at_ms: number | null;
}

export interface WorkspaceTaskAttempt {
  attempt_no: number;
  status: string;
  run_id: string | null;
  worker_id: string | null;
  outbox_id: string | null;
  executor: ObservationExecutor | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  summary: string | null;
}

export interface WorkspaceTaskCandidate {
  candidate_id: string;
  attempt_no: number;
  worker_id: string | null;
  head_commit_sha: string | null;
  tree_sha: string | null;
  verification_passed: boolean | null;
  review_status: string;
  review_verdict_id: string | null;
  review_receipt_id: string | null;
  produced_at_ms: number | null;
  summary: string | null;
}

export interface WorkspaceTaskHistory {
  task_id: string;
  title: string;
  role: string | null;
  difficulty: string | null;
  stage: string | null;
  status: string;
  depends_on: string[];
  current_attempt_no: number;
  attempts: WorkspaceTaskAttempt[];
  candidates: WorkspaceTaskCandidate[];
  active_candidate_id: string | null;
  accepted_candidate_id: string | null;
  historical_failure: WorkspaceHistoricalFailure | null;
  current_attention: WorkspaceAttentionState | null;
  acceptance_proof: WorkspaceAcceptanceProof;
}

export interface WorkspaceRunRecord {
  run_id: string;
  worker_id: string | null;
  binding_id: string;
  task_id: string | null;
  attempt_no: number | null;
  role: string | null;
  status: string;
  executor: ObservationExecutor | null;
  started_at_ms: number | null;
  finished_at_ms: number | null;
  updated_at_ms: number | null;
}

export interface WorkspaceOutboxRecord {
  outbox_id: string;
  request_key: string | null;
  kind: string;
  status: string;
  task_id: string | null;
  attempt_no: number | null;
  role: string | null;
  run_id: string | null;
  worker_id: string | null;
  receipt_id: string | null;
  reserved_at_job_revision: number | null;
  updated_at_job_revision: number | null;
  diagnostic: string | null;
}

export interface WorkspaceCombinedCandidate {
  combined_candidate_id: string;
  job_revision: number | null;
  status: string;
  final_commit_sha: string | null;
  final_tree_sha: string | null;
  candidate_digest: string | null;
  closure_reservation_id: string | null;
  check_count: number;
  produced_at_ms: number | null;
}

export interface WorkspaceJobResult {
  outcome: string;
  summary: string;
  produced_at_ms: number | null;
  delivery_stage: ObservationDeliveryStage;
  closure_verdict_id: string | null;
}

export interface WorkspaceRecordLinks {
  source_plan_id: string | null;
  source_plan_revision: number | null;
  source_approval_id: string | null;
  source_work_brief_id: string | null;
  planning_grant_id: string | null;
  planning_reservation_id: string | null;
  supersedes_job_id: string | null;
  superseded_by_job_id: string | null;
  parent_job_id: string | null;
  follow_up_job_ids: string[];
  architect_advice_ids: string[];
  source_request_ids: string[];
}

export interface WorkspaceJobSummary {
  source_id: string;
  job_id: string;
  binding_id: string | null;
  chat_identity: WorkspaceChatIdentity | null;
  title: string;
  job_revision: number | null;
  state: ObservationJobState;
  phase: string | null;
  approval_status: string;
  execution_status: string;
  review_state: string | null;
  created_at_ms: number | null;
  updated_at_ms: number | null;
  summary: string | null;
}

export interface WorkspaceCoveredAttention {
  task_id: string | null;
  attention: WorkspaceAttentionState;
}

export interface WorkspaceExternalResolution {
  receipt_id: string;
  request_key: string | null;
  preview_digest: string | null;
  outcome: WorkspaceExternalResolutionOutcome;
  scope: WorkspaceExternalResolutionScope;
  task_ids: string[];
  applicability: WorkspaceExternalResolutionApplicability;
  summary: string;
  evidence_refs: string[];
  recorded_at_ms: number | null;
  covered_attention: WorkspaceCoveredAttention[];
}

export interface WorkspaceJobHistoryTruncation {
  tasks_omitted: number;
  attempts_omitted: number;
  candidates_omitted: number;
  runs_omitted: number;
  outbox_omitted: number;
  combined_candidates_omitted: number;
  external_resolutions_omitted?: number;
}

export interface WorkspaceJobHistory {
  job: WorkspaceJobSummary;
  tasks: WorkspaceTaskHistory[];
  runs: WorkspaceRunRecord[];
  outbox: WorkspaceOutboxRecord[];
  combined_candidates: WorkspaceCombinedCandidate[];
  result: WorkspaceJobResult | null;
  links: WorkspaceRecordLinks;
  historical_failure: WorkspaceHistoricalFailure | null;
  current_attention: WorkspaceAttentionState | null;
  acceptance_proof: WorkspaceAcceptanceProof;
  external_resolutions?: WorkspaceExternalResolution[];
  truncation: WorkspaceJobHistoryTruncation;
}

export interface WorkspaceMessageSourceOrder {
  recorded_at_ms: number | null;
  source_seq: number;
  sub_index: number;
}

export interface WorkspaceMessageScope {
  source_id: string;
  binding_id: string | null;
  thread_uuid: string | null;
  canonical_project_scope: string | null;
  chat_identity: WorkspaceChatIdentity | null;
  job_id: string | null;
  task_id: string | null;
  run_id: string | null;
  attempt_no: number | null;
}

export interface WorkspaceMessageCompleteness {
  summary_truncated: boolean;
  original_char_count: number | null;
  max_summary_chars: number;
  redacted: boolean;
  missing_fields: string[];
  incomplete_reason: string | null;
}

export interface WorkspaceMessageItem {
  item_id: string;
  source_kind: WorkspaceSafeMessageSourceKind;
  source_record_id: string;
  source_order: WorkspaceMessageSourceOrder;
  scope: WorkspaceMessageScope;
  actor_role: string | null;
  summary: string;
  completeness: WorkspaceMessageCompleteness;
}

export interface WorkspaceMessagesWindow {
  availability: WorkspaceAvailabilityState;
  items: WorkspaceMessageItem[];
  max_items: number;
  omitted_count: number;
  has_more: boolean;
  truncated: boolean;
  requires_transcript: false;
  allows_synthetic_user_messages: false;
}

export interface WorkspaceSnapshotBounds {
  native_samples_omitted: number;
  job_histories_omitted: number;
  messages_omitted: number;
  requests_capped: boolean;
  jobs_capped: boolean;
}

export interface WorkspaceSnapshot {
  schema_version: 1;
  generated_at_ms: number;
  sampling_policy: WorkspaceSamplingPolicy;
  observation: ObservationSnapshot;
  chats: WorkspaceChatSummary[];
  native_samples: WorkspaceNativeSample[];
  job_histories: WorkspaceJobHistory[];
  messages: WorkspaceMessagesWindow;
  bounds: WorkspaceSnapshotBounds;
}
