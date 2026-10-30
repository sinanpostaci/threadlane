import type { ToolAdapter } from '../registry/types.ts';

/** Lifecycle phase serialized by `threadlane_host::HostPhase`. */
export type HostPhase = 'bootstrapping' | 'ready' | 'failed' | 'stopped';

/**
 * Execution gates from `threadlane_host::HostExecutionGates`.
 *
 * These are the complete fields exposed by the host summary. There is no
 * frontend-invented process or manifest readiness field.
 */
export interface HostExecutionGates {
  supervisor_authenticated_ready: boolean;
  workers_auto_started_on_boot: boolean;
  root_tasks_only: boolean;
  dependent_tasks_blocked: boolean;
  native_routes_manual_only: boolean;
  worker_completed_means_ready_for_review: boolean;
  auto_accept_or_merge_enabled: boolean;
}

/**
 * Safe startup notification policy from `threadlane_host::HostNotificationSummary`.
 *
 * The executable digest is a one-way identity, never a filesystem path. These
 * fields describe startup configuration only; they do not prove delivery.
 */
export interface HostNotificationSummary {
  enabled: boolean;
  executable_digest: string | null;
  timeout_ms: number | null;
}

/** Safe, token-free, path-free `threadlane_host::HostSessionSummary` DTO. */
export interface HostSessionSummary {
  approval_id: string;
  subject_digest: string;
  /** Authoritative control-plane conversation binding (not the host runtime session binding). */
  binding_id: string;
  thread_uuid: string;
  plan_id: string;
  plan_revision: number;
  /** Host-owned runtime session binding, distinct from `binding_id`. */
  session_binding_id: string | null;
  phase: HostPhase;
  worker_profile_count: number;
  native_route_count: number;
  root_worker_task_ids: string[];
  dependent_task_ids: string[];
  native_route_task_ids: string[];
  gates: HostExecutionGates;
  last_error: string | null;
  /**
   * Actual startup policy reported by the host. Optional for compatibility
   * with older summaries; a missing value means notifications were off.
   */
  notifications?: HostNotificationSummary;
}

/** Canonical provider values serialized by `threadlane_workers::WorkerProvider`. */
export type WorkerProvider = 'antigravity' | 'opencode_v2';

/** Canonical worker lifecycle values from the core `WorkerStatus` enum. */
export type WorkerStatus =
  | 'queued'
  | 'claimed'
  | 'running'
  | 'cancel_requested'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'timed_out'
  | 'unknown';

/** Canonical review state from the core `WorkerAcceptanceState` enum. */
export type WorkerAcceptanceState =
  | 'pending_review'
  | 'ready_for_review'
  | 'rejected_by_diff_gate'
  | 'execution_failed'
  | 'unknown';

/** Compact owned worktree references from the core `WorkerWorktreeSummary`. */
export interface WorkerWorktreeSummary {
  run_worktree_id: string | null;
  run_worktree_path: string | null;
  run_branch_name: string | null;
  task_worktree_id: string | null;
  task_worktree_path: string | null;
  task_branch_name: string | null;
  base_commit: string;
  head_commit: string | null;
}

/** Bounded live progress counters from the core `WorkerProgressSummary`. */
export interface WorkerProgressSummary {
  durable_progress_events: number;
  coalesced_progress_events: number;
  dropped_live_events: number;
  last_event_kind: string | null;
  last_summary: string | null;
  last_event_at_ms: number | null;
}

/** Compact outcome evidence from the core `WorkerOutcomeSummary`. */
export interface WorkerOutcomeSummary {
  exit_code: number | null;
  signal: number | null;
  duration_ms: number | null;
  timed_out: boolean;
  cancelled: boolean;
  cancel_requested_before_completion: boolean;
  missing_model_evidence: boolean;
  missing_effort_evidence: boolean;
  provider_error: boolean;
  incomplete_cleanup: boolean;
  rejected_by_diff_gate: boolean;
  stdout_truncated: boolean;
  stderr_truncated: boolean;
  acceptance_state: WorkerAcceptanceState;
  changed_paths_count: number;
  changed_paths_preview: string[];
  violation_count: number;
  violation_summaries: string[];
  final_summary: string | null;
  failure_reason: string | null;
  report_artifact_id: string | null;
}

/** Compact runtime `WorkerSnapshot` DTO from `threadlane-core`. */
export interface WorkerSnapshot {
  untrusted_data: boolean;
  worker_id: string;
  run_id: string;
  binding_id: string;
  revision: number;
  status: WorkerStatus;
  profile_id: string;
  profile_digest: string;
  provider: string;
  requested_model: string;
  requested_effort: string;
  observed_model: string | null;
  observed_effort: string | null;
  instruction: string;
  claim_id: string | null;
  attempt_count: number;
  worktree: WorkerWorktreeSummary;
  progress: WorkerProgressSummary;
  outcome: WorkerOutcomeSummary | null;
  created_at_ms: number;
  updated_at_ms: number;
  started_at_ms: number | null;
  completed_at_ms: number | null;
  cancel_requested_at_ms: number | null;
  cancel_reason: string | null;
}

/** Canonical task execution status from `threadlane_host::TaskExecutionStatus`. */
export type TaskExecutionStatus =
  | 'not_started'
  | 'dependency_inputs_unverified'
  | 'native_route_required'
  | 'queued'
  | 'claimed'
  | 'running'
  | 'cancel_requested'
  | 'ready_for_review'
  | 'failed'
  | 'cancelled'
  | 'timed_out'
  | 'unknown';

/** Safe `threadlane_host::TaskStateSnapshot` DTO. */
export interface TaskStateSnapshot {
  approval_id: string;
  subject_digest: string;
  /** Authoritative control-plane binding, not the host runtime session binding. */
  binding_id: string;
  thread_uuid: string;
  /** Host-owned runtime session binding. */
  session_binding_id: string;
  task_id: string;
  task_digest: string;
  adapter: ToolAdapter;
  provider: WorkerProvider | null;
  depends_on: string[];
  profile_id: string | null;
  command_key: string | null;
  status: TaskExecutionStatus;
  run_id: string | null;
  worker_id: string | null;
  worker: WorkerSnapshot | null;
  /** Always false in the backend contract; completion is never acceptance. */
  accepted: boolean;
}

/** Enabled or disabled Codex CLI choice resolved from the trusted registry. */
export interface HostCodexToolChoice {
  id: string;
  label: string;
  enabled: boolean;
}

/**
 * Current desktop conversation context supplied by the Plans integration.
 *
 * `codexTools` and `registryRevision` are optional so older callers remain
 * compatible. Without them the notification opt-in stays disabled.
 */
export interface HostPanelContext {
  bindingId: string | null;
  threadUuid: string | null;
  connected: boolean;
  codexTools?: readonly HostCodexToolChoice[];
  registryRevision?: number;
}

/** Arguments for the frozen `desktop_approved_session` command. */
export interface DesktopApprovedSessionArgs {
  approvalId: string;
}

/**
 * Arguments for the frozen `desktop_start_approved_session` command.
 *
 * Only a trusted registry tool identifier may cross the bridge. Omitted or
 * null means notification opt-in is off; raw paths, threads, argv, and
 * timeouts are intentionally not accepted from JavaScript.
 */
export interface DesktopStartApprovedSessionArgs {
  approvalId: string;
  notifyCodexToolId?: string | null;
}

/** Arguments for the frozen `desktop_approved_task` command. */
export interface DesktopApprovedTaskArgs {
  approvalId: string;
  taskId: string;
}
