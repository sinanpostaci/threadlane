import {
  ACCENT_PALETTES,
  DEFAULT_APPEARANCE,
  DEFAULT_EXECUTION_PREFERENCES,
  EXECUTION_PREFERENCE_BOUNDS,
  type AccentName,
  type AppearancePreferences,
  type Attachment,
  type BindingSummary,
  type BrandKey,
  type ExecutionPreferences,
  type QueueBucket,
  type RecordedProfileGroup,
  type SurfaceMode,
  type TaskProviderFilter,
  type TaskSummary,
  type ThemeMode,
  type ToolAvailability,
} from './types.ts';

const EXACT_STATUS_BUCKETS: Readonly<Record<string, QueueBucket>> = {
  queued: 'active',
  claimed: 'active',
  running: 'active',
  waiting_input: 'waiting',
  completing: 'active',
  cancel_requested: 'active',
  completed: 'completed',
  failed: 'completed',
  cancelled: 'completed',
  canceled: 'completed',
  timed_out: 'completed',
  timedout: 'completed',
  unknown: 'completed',
  uncertain: 'completed',
  waiting_for_answer: 'waiting',
  execution_failed: 'completed',
};

const EXACT_STATUS_LABELS: Readonly<Record<string, string>> = {
  queued: 'Queued',
  claimed: 'Claimed',
  running: 'Running',
  waiting_input: 'Waiting for input',
  completing: 'Completing',
  cancel_requested: 'Cancel requested',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  canceled: 'Cancelled',
  timed_out: 'Timed out',
  timedout: 'Timed out',
  unknown: 'Unknown (uncertain)',
  uncertain: 'Unknown (uncertain)',
  waiting_for_answer: 'Waiting for answer',
  execution_failed: 'Execution failed',
};

const WAITING_STATUS_PATTERNS = [
  'waiting',
  'needs_input',
  'needsinput',
  'question',
  'awaiting',
  'blocked_on_user',
  'action_required',
];

const TERMINAL_STATUS_PATTERNS = [
  'complete',
  'completed',
  'handled',
  'stopped',
  'cancelled',
  'canceled',
  'failed',
  'execution_failed',
  'executionfailed',
  'timed_out',
  'timedout',
  'timeout',
  'unknown',
  'uncertain',
  'rejected',
  'aborted',
  'terminated',
  'done',
];

export function normalizeStatusKey(status: string): string {
  return status
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

export function classifyTaskBucket(task: TaskSummary): QueueBucket {
  const normalized = normalizeStatusKey(task.status);
  if (!normalized) {
    return 'completed';
  }
  const exact = EXACT_STATUS_BUCKETS[normalized];
  if (exact) {
    return exact;
  }
  if (WAITING_STATUS_PATTERNS.some((p) => normalized === p || normalized.includes(p))) {
    return 'waiting';
  }
  if (TERMINAL_STATUS_PATTERNS.some((p) => normalized === p || normalized.includes(p))) {
    return 'completed';
  }
  return 'active';
}

export function isTaskActive(task: TaskSummary): boolean {
  return classifyTaskBucket(task) !== 'completed';
}

export function formatStatusLabel(status: string): string {
  const normalized = normalizeStatusKey(status);
  if (!normalized) {
    return 'Unknown (uncertain)';
  }
  const exact = EXACT_STATUS_LABELS[normalized];
  if (exact) {
    return exact;
  }
  const withSpaces = normalized.replace(/_+/g, ' ');
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1);
}

export function resolveTaskBrand(task: TaskSummary): BrandKey {
  const provider = (task.provider ?? '').toLowerCase();
  const model = `${task.observed_model ?? ''} ${task.requested_model ?? ''}`.toLowerCase();
  if (task.kind === 'demo' && !provider) {
    return 'demo';
  }
  if (provider.includes('antigravity') || provider === 'agy') {
    if (model.includes('gemini') || model.includes('argon')) {
      return 'gemini';
    }
    return 'antigravity';
  }
  if (provider.includes('opencode')) {
    if (model.includes('deepseek')) {
      return 'deepseek';
    }
    return 'opencode';
  }
  if (provider.includes('codex') || provider.includes('openai')) {
    return 'openai';
  }
  if (model.includes('deepseek')) {
    return 'deepseek';
  }
  if (model.includes('gemini') || model.includes('argon')) {
    return 'gemini';
  }
  if (model.includes('sol') || model.includes('luna') || model.includes('gpt') || model.includes('astra')) {
    return 'openai';
  }
  if (task.kind === 'demo') {
    return 'demo';
  }
  return 'generic';
}

export function resolveToolBrand(toolId: string, label: string): BrandKey {
  const combined = `${toolId} ${label}`.toLowerCase();
  if (combined.includes('opencode')) {
    return 'opencode';
  }
  if (combined.includes('antigravity') || combined.includes('agy')) {
    return 'antigravity';
  }
  if (combined.includes('codex') || combined.includes('openai')) {
    return 'openai';
  }
  return 'generic';
}

export function matchesProviderFilter(task: TaskSummary, filter: TaskProviderFilter): boolean {
  if (filter === 'all') {
    return true;
  }
  if (filter === 'demo') {
    return task.kind === 'demo';
  }
  const provider = (task.provider ?? '').toLowerCase();
  const brand = resolveTaskBrand(task);
  if (filter === 'opencode') {
    return provider.includes('opencode') || brand === 'opencode' || brand === 'deepseek';
  }
  if (filter === 'antigravity') {
    return (
      provider.includes('antigravity') ||
      provider === 'agy' ||
      brand === 'antigravity' ||
      brand === 'gemini'
    );
  }
  if (filter === 'codex') {
    return provider.includes('codex') || provider.includes('openai') || brand === 'openai';
  }
  return true;
}

export function formatProviderName(task: TaskSummary): string {
  if (task.provider && task.provider.trim().length > 0) {
    const raw = task.provider.trim();
    const lower = raw.toLowerCase();
    if (lower === 'agy' || lower === 'antigravity') return 'Antigravity';
    if (lower === 'opencode') return 'OpenCode';
    if (lower === 'codex') return 'Codex';
    if (lower === 'demo' || lower === 'local_demo') return 'Local demo';
    return raw;
  }
  if (task.kind === 'demo') {
    return 'Local demo';
  }
  if (task.kind === 'worker') {
    return 'Worker';
  }
  return 'Unknown provider';
}

export function formatModelAndEffort(task: TaskSummary): string {
  const observedModel = task.observed_model?.trim() || null;
  const observedEffort = task.observed_effort?.trim() || null;
  const requestedModel = task.requested_model?.trim() || null;
  const requestedEffort = task.requested_effort?.trim() || null;

  if (observedModel || observedEffort) {
    const modelPart = observedModel ?? requestedModel ?? 'Model unverified';
    const effortPart = observedEffort
      ? observedEffort
      : requestedEffort
        ? `${requestedEffort} (req)`
        : 'Effort unverified';
    return `${modelPart} · ${effortPart}`;
  }

  if (requestedModel || requestedEffort) {
    const parts = [requestedModel ?? 'Unspecified model', requestedEffort ?? 'Unspecified effort'];
    return `Requested: ${parts.join(' · ')}`;
  }

  if (task.kind === 'demo') {
    return 'Deterministic local state machine · No model call';
  }

  return 'No model or effort telemetry recorded';
}

export function formatRelativeTimestamp(updatedAtMs: number, nowMs: number = Date.now()): string {
  if (!Number.isFinite(updatedAtMs) || updatedAtMs <= 0) {
    return '—';
  }
  const diffSec = Math.max(0, Math.floor((nowMs - updatedAtMs) / 1000));
  if (diffSec < 5) {
    return 'just now';
  }
  if (diffSec < 60) {
    return `${diffSec}s ago`;
  }
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) {
    return `${diffMin}m ago`;
  }
  const diffHours = Math.floor(diffMin / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

export function formatBindingLabel(binding: BindingSummary): string {
  const base = binding.label.trim() || binding.binding_id;
  if (binding.is_local_demo && !base.toLowerCase().includes('demo')) {
    return `${base} (Local demo)`;
  }
  return base;
}

export function formatAttachmentBadge(
  attachment: Attachment,
  isDevPreviewFixture: boolean = false
): string {
  if (isDevPreviewFixture) {
    return 'Design preview';
  }
  if (attachment.connection === 'blocked') {
    return 'Blocked';
  }
  if (attachment.connection === 'disconnected') {
    return 'Offline';
  }
  if (attachment.connection === 'unconfigured' || attachment.mode === 'unconfigured') {
    return 'Unconfigured';
  }
  if (attachment.mode === 'demo') {
    return 'Local demo';
  }
  return 'Live observer';
}

export function formatFooterStatus(
  attachment: Attachment,
  isDevPreviewFixture: boolean = false
): { left: string; right: string } {
  if (isDevPreviewFixture) {
    return {
      left: 'DEV preview fixture',
      right: 'No live connection',
    };
  }
  if (attachment.connection === 'blocked') {
    return {
      left: 'Runtime access blocked',
      right: attachment.message || 'Check state directory permissions',
    };
  }
  if (attachment.connection === 'disconnected') {
    return {
      left: attachment.mode === 'demo' ? 'Demo runtime offline' : 'Observer offline',
      right: attachment.message || 'No live connection',
    };
  }
  if (attachment.connection === 'unconfigured' || attachment.mode === 'unconfigured') {
    return {
      left: 'Unconfigured observer',
      right: 'Attach runtime or run local demo',
    };
  }
  if (attachment.mode === 'demo') {
    return {
      left: 'Local demo runtime',
      right: 'Connected · No paid calls',
    };
  }
  return {
    left: 'Runtime observer',
    right: 'Connected · Read-only',
  };
}

export function formatToolAvailability(availability: ToolAvailability): string {
  switch (availability) {
    case 'found':
      return 'Found on PATH';
    case 'not_found':
      return 'Not found on PATH';
    case 'not_configured':
      return 'Not configured';
  }
}

export function formatEvidenceSummary(
  task: Pick<
    TaskSummary,
    'kind' | 'status' | 'observed_model' | 'observed_effort' | 'needs_evidence'
  >,
  missingModelEvidence: boolean = false,
  missingEffortEvidence: boolean = false
): string {
  if (missingModelEvidence && missingEffortEvidence) {
    return 'Missing model & effort evidence';
  }
  if (missingModelEvidence) {
    return 'Missing model evidence';
  }
  if (missingEffortEvidence) {
    return 'Missing effort evidence';
  }
  if (task.needs_evidence) {
    return 'Needs provider evidence';
  }
  if (task.kind === 'demo') {
    return 'Not applicable (local demo)';
  }
  if (task.kind !== 'worker') {
    return 'Unverified';
  }

  const hasObservedModel = Boolean(task.observed_model && task.observed_model.trim().length > 0);
  const hasObservedEffort = Boolean(
    task.observed_effort && task.observed_effort.trim().length > 0
  );

  if (hasObservedModel && hasObservedEffort) {
    return 'Model & effort recorded';
  }

  const active = classifyTaskBucket(task as TaskSummary) !== 'completed';
  if (hasObservedModel && !hasObservedEffort) {
    return active ? 'Pending (effort unverified)' : 'Unverified (effort not observed)';
  }
  if (!hasObservedModel && hasObservedEffort) {
    return active ? 'Pending (model unverified)' : 'Unverified (model not observed)';
  }
  return active ? 'Pending (unverified)' : 'Unverified (not observed)';
}

export function formatEvidenceFieldCheck(
  task: Pick<TaskSummary, 'kind' | 'status'>,
  observedValue: string | null,
  missingFlag: boolean,
  fieldName: 'model' | 'effort'
): string {
  if (missingFlag) {
    return fieldName === 'model' ? 'Missing model evidence' : 'Missing effort evidence';
  }
  if (task.kind === 'demo') {
    return 'Not applicable (local demo)';
  }
  if (task.kind !== 'worker') {
    return 'Unverified';
  }
  if (Boolean(observedValue && observedValue.trim().length > 0)) {
    return 'Observed';
  }
  return classifyTaskBucket(task as TaskSummary) !== 'completed'
    ? 'Pending (unverified)'
    : 'Unverified';
}

export function deriveRecordedProfiles(tasks: readonly TaskSummary[]): RecordedProfileGroup[] {
  const groups = new Map<string, RecordedProfileGroup>();

  for (const task of tasks) {
    const rawId = task.profile_id?.trim() || '';
    const isUnassigned = rawId.length === 0;
    const key = isUnassigned ? '__unassigned__' : rawId;
    let group = groups.get(key);
    if (!group) {
      group = {
        profileId: isUnassigned ? 'No profile ID recorded' : rawId,
        isUnassigned,
        runCount: 0,
        activeCount: 0,
        needsEvidenceCount: 0,
        models: [],
        providers: [],
        tasks: [],
      };
      groups.set(key, group);
    }

    group.runCount += 1;
    if (isTaskActive(task)) {
      group.activeCount += 1;
    }
    if (task.needs_evidence) {
      group.needsEvidenceCount += 1;
    }

    const providerLabel = formatProviderName(task);
    if (!group.providers.includes(providerLabel)) {
      group.providers.push(providerLabel);
    }

    const modelSummary = formatModelAndEffort(task);
    if (!group.models.includes(modelSummary)) {
      group.models.push(modelSummary);
    }

    group.tasks.push(task);
  }

  return [...groups.values()].sort((a, b) => {
    if (a.isUnassigned !== b.isUnassigned) {
      return a.isUnassigned ? 1 : -1;
    }
    return a.profileId.localeCompare(b.profileId);
  });
}

export function sanitizeAppearancePreferences(raw: unknown): AppearancePreferences {
  const result: AppearancePreferences = { ...DEFAULT_APPEARANCE };
  if (!raw || typeof raw !== 'object') {
    return result;
  }
  const obj = raw as Record<string, unknown>;

  const validThemes: readonly ThemeMode[] = ['system', 'light', 'dark'];
  if (typeof obj.theme === 'string' && (validThemes as readonly string[]).includes(obj.theme)) {
    result.theme = obj.theme as ThemeMode;
  }

  const validSurfaces: readonly SurfaceMode[] = ['glass', 'solid'];
  if (typeof obj.surface === 'string' && (validSurfaces as readonly string[]).includes(obj.surface)) {
    result.surface = obj.surface as SurfaceMode;
  }

  if (typeof obj.opacity === 'number' && Number.isFinite(obj.opacity)) {
    result.opacity = Math.round(Math.min(100, Math.max(65, obj.opacity)));
  }

  const validAccents = Object.keys(ACCENT_PALETTES) as AccentName[];
  if (typeof obj.accent === 'string' && (validAccents as readonly string[]).includes(obj.accent)) {
    result.accent = obj.accent as AccentName;
  }

  if (typeof obj.rememberStateDir === 'boolean') {
    result.rememberStateDir = obj.rememberStateDir;
  }

  if (result.rememberStateDir && typeof obj.savedStateDir === 'string') {
    result.savedStateDir = obj.savedStateDir.trim().slice(0, 512);
  } else {
    result.savedStateDir = '';
  }

  return result;
}

const ALLOWED_EXECUTION_PREF_KEYS = new Set<string>([
  'maxConcurrentWorkers',
  'maxTasksPerPlan',
  'workerTimeoutSecs',
  'decisionBudgetTokens',
  'redactPathsInLogs',
  'requireExplicitPublish',
]);

function clampInt(value: unknown, min: number, max: number, fallback: number): number {
  const num = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : NaN;
  if (!Number.isFinite(num)) {
    return fallback;
  }
  return Math.round(Math.min(max, Math.max(min, num)));
}

export function sanitizeExecutionPreferences(raw: unknown): ExecutionPreferences {
  const result: ExecutionPreferences = { ...DEFAULT_EXECUTION_PREFERENCES };
  if (!raw || typeof raw !== 'object') {
    return result;
  }
  const obj = raw as Record<string, unknown>;

  result.maxConcurrentWorkers = clampInt(
    obj.maxConcurrentWorkers,
    EXECUTION_PREFERENCE_BOUNDS.maxConcurrentWorkers.min,
    EXECUTION_PREFERENCE_BOUNDS.maxConcurrentWorkers.max,
    DEFAULT_EXECUTION_PREFERENCES.maxConcurrentWorkers
  );
  result.maxTasksPerPlan = clampInt(
    obj.maxTasksPerPlan,
    EXECUTION_PREFERENCE_BOUNDS.maxTasksPerPlan.min,
    EXECUTION_PREFERENCE_BOUNDS.maxTasksPerPlan.max,
    DEFAULT_EXECUTION_PREFERENCES.maxTasksPerPlan
  );
  result.workerTimeoutSecs = clampInt(
    obj.workerTimeoutSecs,
    EXECUTION_PREFERENCE_BOUNDS.workerTimeoutSecs.min,
    EXECUTION_PREFERENCE_BOUNDS.workerTimeoutSecs.max,
    DEFAULT_EXECUTION_PREFERENCES.workerTimeoutSecs
  );
  result.decisionBudgetTokens = clampInt(
    obj.decisionBudgetTokens,
    EXECUTION_PREFERENCE_BOUNDS.decisionBudgetTokens.min,
    EXECUTION_PREFERENCE_BOUNDS.decisionBudgetTokens.max,
    DEFAULT_EXECUTION_PREFERENCES.decisionBudgetTokens
  );

  if (typeof obj.redactPathsInLogs === 'boolean') {
    result.redactPathsInLogs = obj.redactPathsInLogs;
  }
  if (typeof obj.requireExplicitPublish === 'boolean') {
    result.requireExplicitPublish = obj.requireExplicitPublish;
  }

  return result;
}

export function validateExecutionPreferencesInput(raw: unknown): {
  prefs: ExecutionPreferences;
  rejectedKeys: string[];
} {
  const prefs = sanitizeExecutionPreferences(raw);
  if (!raw || typeof raw !== 'object') {
    return { prefs, rejectedKeys: [] };
  }
  const rejectedKeys = Object.keys(raw as Record<string, unknown>).filter(
    (key) => !ALLOWED_EXECUTION_PREF_KEYS.has(key)
  );
  return { prefs, rejectedKeys };
}
