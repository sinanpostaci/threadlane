export type AttachmentMode = 'demo' | 'runtime' | 'unconfigured';
export type ConnectionState = 'connected' | 'disconnected' | 'blocked' | 'unconfigured';

export interface Attachment {
  mode: AttachmentMode;
  connection: ConnectionState;
  state_dir: string | null;
  message: string | null;
  updated_at_ms: number;
}

export interface BindingSummary {
  binding_id: string;
  thread_uuid: string;
  label: string;
  is_local_demo: boolean;
}

export type TaskKind = 'worker' | 'demo' | 'unknown';

export interface TaskSummary {
  run_id: string;
  binding_id: string;
  revision: number;
  status: string;
  title: string;
  provider: string | null;
  requested_model: string | null;
  requested_effort: string | null;
  observed_model: string | null;
  observed_effort: string | null;
  last_summary: string | null;
  updated_at_ms: number;
  needs_evidence: boolean;
  kind: TaskKind;
  profile_id: string | null;
}

export interface DemoControls {
  available: boolean;
  run_id: string | null;
  binding_id: string | null;
  can_answer: boolean;
  can_cancel: boolean;
  revision: number | null;
}

export interface DesktopSnapshot {
  attachment: Attachment;
  bindings: BindingSummary[];
  selected_binding_id: string | null;
  tasks: TaskSummary[];
  total_runs: number;
  active_runs: number;
  has_more: boolean;
  demo_controls: DemoControls;
}

export interface RunQuestion {
  question_id: string;
  prompt: string;
}

export interface RunEventCounts {
  durable_progress_events: number;
  coalesced_progress_events: number;
  dropped_live_events: number;
}

export interface RunDetail {
  task: TaskSummary;
  instruction: string;
  outcome_summary: string | null;
  failure_reason: string | null;
  acceptance_state: string | null;
  exit_code: number | null;
  branch_name: string | null;
  worktree_path: string | null;
  latest_activity: string | null;
  question: RunQuestion | null;
  counts: RunEventCounts;
  missing_model_evidence: boolean;
  missing_effort_evidence: boolean;
  demo_controls: DemoControls;
}

export type ToolAvailability = 'found' | 'not_found' | 'not_configured';

export interface ToolStatus {
  id: string;
  label: string;
  executable_path: string | null;
  availability: ToolAvailability;
  note: string;
}

export type WindowAction = 'close' | 'minimize';

export interface DesktopSnapshotArgs {
  bindingId?: string | null;
}

export interface DesktopAttachArgs {
  stateDir: string;
}

export interface DesktopRunDetailArgs {
  bindingId: string;
  runId: string;
}

export type EventKind =
  | 'run_started'
  | 'run_progress'
  | 'question_asked'
  | 'question_answered'
  | 'question_superseded'
  | 'run_steered'
  | 'run_completed'
  | 'run_cancelled'
  | 'worker_queued'
  | 'worker_started'
  | 'worker_progress'
  | 'worker_cancel_requested'
  | 'worker_completed'
  | 'worker_failed'
  | 'worker_cancelled'
  | 'worker_timed_out'
  | 'worker_unknown';

export const EVENT_KINDS: readonly EventKind[] = [
  'run_started',
  'run_progress',
  'question_asked',
  'question_answered',
  'question_superseded',
  'run_steered',
  'run_completed',
  'run_cancelled',
  'worker_queued',
  'worker_started',
  'worker_progress',
  'worker_cancel_requested',
  'worker_completed',
  'worker_failed',
  'worker_cancelled',
  'worker_timed_out',
  'worker_unknown',
] as const;

export interface EventPayload {
  untrusted_data: boolean;
  summary: string;
  question_id: string | null;
  question_prompt: string | null;
  answer: string | null;
  instruction: string | null;
  cancel_reason: string | null;
}

export interface ActivityEvent {
  event_id: string;
  binding_id: string;
  seq: number;
  run_id: string;
  revision: number;
  step_seq: number;
  kind: EventKind;
  important: boolean;
  payload: EventPayload;
  created_at_ms: number;
}

export interface ActivityPage {
  binding_id: string;
  events: ActivityEvent[];
  next_cursor: string | null;
  has_more: boolean;
  high_water_mark_seq: number;
  after_seq: number;
}

export interface DesktopActivityHistoryArgs {
  bindingId: string;
  afterSeq?: number | null;
  cursor?: string | null;
  limit?: number | null;
  runId?: string | null;
  kind?: EventKind | null;
}

export interface DesktopAnswerDemoArgs {
  expectedRevision: number;
}

export interface DesktopCancelDemoArgs {
  expectedRevision: number;
}

export interface DesktopWindowActionArgs {
  action: WindowAction;
}

export type NavPage = 'activity' | 'chats' | 'plans' | 'tools' | 'profiles' | 'settings';
export type DetailTab = 'overview' | 'prompt' | 'evidence';
export type TaskProviderFilter = 'all' | 'opencode' | 'antigravity' | 'codex' | 'demo';

export type ThemeMode = 'system' | 'light' | 'dark';
export type SurfaceMode = 'glass' | 'solid';
export type AccentName = 'sage' | 'blue' | 'violet' | 'sand';

export interface AppearancePreferences {
  theme: ThemeMode;
  surface: SurfaceMode;
  opacity: number;
  accent: AccentName;
  rememberStateDir: boolean;
  savedStateDir: string;
}

export interface ExecutionPreferences {
  maxConcurrentWorkers: number;
  maxTasksPerPlan: number;
  workerTimeoutSecs: number;
  decisionBudgetTokens: number;
  redactPathsInLogs: boolean;
  requireExplicitPublish: boolean;
}

export const EXECUTION_PREFERENCE_BOUNDS = {
  maxConcurrentWorkers: { min: 1, max: 8 },
  maxTasksPerPlan: { min: 1, max: 32 },
  workerTimeoutSecs: { min: 30, max: 3600 },
  decisionBudgetTokens: { min: 128, max: 8192 },
} as const;

export const DEFAULT_EXECUTION_PREFERENCES: ExecutionPreferences = {
  maxConcurrentWorkers: 2,
  maxTasksPerPlan: 8,
  workerTimeoutSecs: 600,
  decisionBudgetTokens: 1024,
  redactPathsInLogs: true,
  requireExplicitPublish: true,
};

export type BrandKey = 'openai' | 'opencode' | 'antigravity' | 'gemini' | 'deepseek' | 'demo' | 'generic';

export type QueueBucket = 'waiting' | 'active' | 'completed';

export interface RecordedProfileGroup {
  profileId: string;
  isUnassigned: boolean;
  runCount: number;
  activeCount: number;
  needsEvidenceCount: number;
  models: string[];
  providers: string[];
  tasks: TaskSummary[];
}

export interface DetailTarget {
  bindingId: string;
  runId: string;
}

export type PendingMutation = 'start_demo' | 'answer_demo' | 'cancel_demo' | 'attach' | null;

export const ACCENT_PALETTES: Record<AccentName, readonly [light: string, dark: string]> = {
  sage: ['#4a6657', '#aac9b8'],
  blue: ['#405f7d', '#aac4dd'],
  violet: ['#67547c', '#c5b5da'],
  sand: ['#786146', '#d5c0a2'],
};

export const DEFAULT_APPEARANCE: AppearancePreferences = {
  theme: 'system',
  surface: 'glass',
  opacity: 88,
  accent: 'sage',
  rememberStateDir: false,
  savedStateDir: '',
};
