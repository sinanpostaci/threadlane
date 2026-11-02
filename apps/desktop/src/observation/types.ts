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
}

export interface ObservationSnapshotRequest {
  source_id?: string | null;
  job_id?: string | null;
  cursor?: string | null;
  limit?: number;
}
