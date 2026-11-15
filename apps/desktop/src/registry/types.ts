export const MAX_SAFE_REVISION = 9007199254740991;

export type ToolAdapter =
  | 'codex_cli'
  | 'opencode_v2'
  | 'antigravity'
  | 'jev_api'
  | 'openai_decisions_api'
  | 'git'
  | 'tmux'
  | 'codegraph';

export const TOOL_ADAPTERS: readonly ToolAdapter[] = [
  'codex_cli',
  'opencode_v2',
  'antigravity',
  'jev_api',
  'openai_decisions_api',
  'git',
  'tmux',
  'codegraph',
] as const;

export function isApiToolAdapter(adapter: ToolAdapter): boolean {
  return adapter === 'jev_api' || adapter === 'openai_decisions_api';
}

export interface ToolDefinition {
  id: string;
  label: string;
  adapter: ToolAdapter;
  /** null means automatic system resolution; a nonnull path is a custom override. */
  executable_path: string | null;
  enabled: boolean;
}

export interface ModelDefinition {
  id: string;
  tool_id: string;
  label: string;
  model_arg: string;
  efforts: string[];
  allow_no_effort: boolean;
}

export interface ModelChoice {
  model_id: string;
  effort: string | null;
}

/**
 * Canonical workflow roles. New configuration, drafts and writes use `advisor`.
 * `architect` is retained only as a readable legacy input spelling for stored
 * profiles, plans, policies and receipts; it is never emitted for new writes.
 */
export type CanonicalWorkflowRole =
  | 'follower'
  | 'orchestrator'
  | 'advisor'
  | 'coder'
  | 'reviewer'
  | 'closure';

/** Frozen legacy spelling accepted from older snapshots and command payloads. */
export type LegacyWorkflowRole = 'architect';

/** Workflow role accepted at configuration and record-reading boundaries. */
export type WorkflowRole = CanonicalWorkflowRole | LegacyWorkflowRole;

/**
 * Current role list for new role selectors and drafts. Advisor appears exactly once.
 */
export const CANONICAL_WORKFLOW_ROLES: readonly CanonicalWorkflowRole[] = [
  'follower',
  'orchestrator',
  'advisor',
  'coder',
  'reviewer',
  'closure',
] as const;

/**
 * Current workflow role order for new selectors, profile editors and writes.
 * Advisor appears once; the frozen `architect` spelling is not a current role.
 */
export const WORKFLOW_ROLES: readonly CanonicalWorkflowRole[] = [
  ...CANONICAL_WORKFLOW_ROLES,
];

/**
 * Frozen historical workflow role order. Use this only to read raw legacy
 * records and tests that model old snapshots; never key a current editor table
 * or emit a new write from it.
 */
export const LEGACY_WORKFLOW_ROLES: readonly WorkflowRole[] = [
  'follower',
  'orchestrator',
  'architect',
  'coder',
  'reviewer',
  'closure',
] as const;

export type CanonicalExecutionWorkflowRole = Exclude<CanonicalWorkflowRole, 'follower'>;

export type LegacyExecutionWorkflowRole = LegacyWorkflowRole;

export type ExecutionWorkflowRole =
  | CanonicalExecutionWorkflowRole
  | LegacyExecutionWorkflowRole;

export type ExecutionRole = ExecutionWorkflowRole;

/** Current five configurable execution roles. Advisor appears exactly once. */
export const CANONICAL_EXECUTION_ROLES: readonly CanonicalExecutionWorkflowRole[] = [
  'orchestrator',
  'advisor',
  'coder',
  'reviewer',
  'closure',
] as const;

/**
 * Legacy execution role order for explicit raw-record compatibility. It is not
 * the current editor key list and must never be used for new writes.
 */
export const LEGACY_EXECUTION_ROLES: readonly ExecutionWorkflowRole[] = [
  'orchestrator',
  'architect',
  'coder',
  'reviewer',
  'closure',
] as const;

/** Current five configurable execution roles. Advisor appears exactly once. */
export const EXECUTION_ROLES: readonly CanonicalExecutionWorkflowRole[] = [
  ...CANONICAL_EXECUTION_ROLES,
];

/** Current execution roles for new role lists. */
export const EXECUTION_WORKFLOW_ROLES = CANONICAL_EXECUTION_ROLES;

const WORKFLOW_ROLE_INPUTS: readonly string[] = [
  ...CANONICAL_WORKFLOW_ROLES,
  ...LEGACY_WORKFLOW_ROLES,
];

export function isWorkflowRole(value: string): value is WorkflowRole {
  return WORKFLOW_ROLE_INPUTS.includes(value);
}

export function isExecutionRole(value: string): value is ExecutionWorkflowRole {
  return isWorkflowRole(value) && value !== 'follower';
}

export const isExecutionWorkflowRole = isExecutionRole;

/** Canonicalizes both accepted spellings to the current Advisor identity. */
export function canonicalWorkflowRole(role: WorkflowRole): CanonicalWorkflowRole {
  return role === 'architect' ? 'advisor' : role;
}

/** Canonical execution key, or null for the non-execution follower role. */
export function canonicalExecutionWorkflowRole(
  role: WorkflowRole
): CanonicalExecutionWorkflowRole | null {
  const canonical = canonicalWorkflowRole(role);
  return canonical === 'follower' ? null : canonical;
}

/** Configuration key emitted for new profile/default/override writes. */
export function workflowRoleConfigValue(role: WorkflowRole): CanonicalWorkflowRole {
  return canonicalWorkflowRole(role);
}

/**
 * Returns the semantic duplicate key for a role/difficulty slot after alias canonicalization.
 * Both `architect` and `advisor` select the same slot and are never last-write-wins aliases.
 */
export function workflowRoleSlotKey(
  role: WorkflowRole,
  difficulty: Difficulty | null
): string {
  return `${canonicalWorkflowRole(role)}:${difficulty ?? 'default'}`;
}

export interface DuplicateWorkflowAssignment {
  role: CanonicalWorkflowRole;
  difficulty: Difficulty | null;
}

/** Finds a duplicate semantic role/difficulty slot across both configuration aliases. */
export function findDuplicateWorkflowAssignment(
  assignments: readonly Pick<RoleAssignment, 'role' | 'difficulty'>[]
): DuplicateWorkflowAssignment | null {
  const seen = new Set<string>();
  for (const assignment of assignments) {
    const key = workflowRoleSlotKey(assignment.role, assignment.difficulty);
    if (seen.has(key)) {
      return {
        role: canonicalWorkflowRole(assignment.role),
        difficulty: assignment.difficulty,
      };
    }
    seen.add(key);
  }
  return null;
}

/**
 * Presentation-only labels for workflow roles. Both accepted spellings display as Advisor;
 * these labels never change persisted workflow role values, IPC payloads, digests, or validation.
 */
const WORKFLOW_ROLE_PRESENTATION_LABELS: Record<WorkflowRole, string> = {
  follower: 'Follower',
  orchestrator: 'Orchestrator',
  architect: 'Advisor',
  advisor: 'Advisor',
  coder: 'Worker',
  reviewer: 'Reviewer',
  closure: 'Closure',
};

export function workflowRoleLabel(role: WorkflowRole): string {
  return WORKFLOW_ROLE_PRESENTATION_LABELS[role];
}

export type Difficulty = 'easy' | 'medium' | 'hard' | 'extreme';

export const DIFFICULTIES: readonly Difficulty[] = [
  'easy',
  'medium',
  'hard',
  'extreme',
] as const;

export interface RoleAssignment {
  /** Configuration accepts both `advisor` and legacy `architect`; resolve by canonical key. */
  role: WorkflowRole;
  difficulty: Difficulty | null;
  choice: ModelChoice;
}

export type DecisionAdvisorMode = 'shadow';

export interface DecisionAdvisor {
  model_id: string;
  mode: DecisionAdvisorMode;
}

export interface WorkflowProfile {
  id: string;
  name: string;
  assignments: RoleAssignment[];
  decision_advisor?: DecisionAdvisor | null;
}

export interface RegistrySnapshot {
  schema_version: number;
  revision: number;
  tools: ToolDefinition[];
  models: ModelDefinition[];
  profiles: WorkflowProfile[];
  default_profile_id: string | null;
}

export type RegistryChange =
  | { op: 'upsert_tool'; tool: ToolDefinition }
  | { op: 'delete_tool'; tool_id: string }
  | { op: 'upsert_model'; model: ModelDefinition }
  | { op: 'upsert_models'; models: ModelDefinition[] }
  | { op: 'delete_model'; model_id: string }
  | { op: 'upsert_profile'; profile: WorkflowProfile }
  | { op: 'delete_profile'; profile_id: string }
  | { op: 'set_default_profile'; profile_id: string | null };

export interface ToolProbe {
  tool_id: string;
  availability: string;
  version: string | null;
  auth_status: string;
  models: ModelDefinition[];
  message: string;
}

export type ToolVersionAvailability = 'found' | 'not_found' | 'api_only' | 'disabled';

export type ToolSelectionMode = 'system' | 'custom';

/**
 * Version-only startup inspection for one saved tool. These fields are read-only
 * observations; no model catalog, login, installation, or update command is used.
 */
export interface ToolVersionInspection {
  tool_id: string;
  availability: ToolVersionAvailability;
  selection_mode: ToolSelectionMode;
  configured_path: string | null;
  resolved_path: string | null;
  system_path: string | null;
  version: string | null;
  system_version: string | null;
  system_path_differs: boolean;
  message: string;
  error_code: string | null;
}

export interface DesktopToolVersionsSnapshot {
  registry_revision: number;
  tools: ToolVersionInspection[];
}

export type DecisionProvider = 'jev' | 'openai';

export const DECISION_PROVIDERS: readonly DecisionProvider[] = ['jev', 'openai'] as const;

export interface DecisionRequest {
  tool_id: string;
  provider: DecisionProvider;
  model: string;
  task_summary: string;
}

export interface DecisionAdvice {
  provider: DecisionProvider;
  model: string;
  difficulty: Difficulty | null;
  confidence: number | null;
  needs_review: boolean;
  reason_code: string;
  input_tokens: number | null;
  output_tokens: number | null;
}

export interface PlanTask {
  id: string;
  title: string;
  instruction: string;
  /**
   * New draft input accepts `advisor` and legacy `architect` and persists `advisor`.
   * Stored historical plan revisions retain their original raw spelling.
   */
  role: WorkflowRole;
  difficulty: Difficulty;
  depends_on: string[];
  allowed_write_paths: string[];
}

export type PlanStatus = 'draft' | 'published' | 'received' | 'applied' | 'superseded';

export const PLAN_STATUSES: readonly PlanStatus[] = [
  'draft',
  'published',
  'received',
  'applied',
  'superseded',
] as const;

export interface PlanRevision {
  plan_id: string;
  binding_id: string;
  revision: number;
  profile_id: string;
  profile_revision: number;
  summary: string;
  tasks: PlanTask[];
  status: PlanStatus;
  created_at_ms: number;
}

export interface DeliveryReceipt {
  plan_id: string;
  revision: number;
  state: PlanStatus;
}

export type PlanChange =
  | {
      op: 'save_draft';
      plan_id: string;
      binding_id: string;
      profile_id: string;
      profile_revision: number;
      summary: string;
      tasks: PlanTask[];
    }
  | { op: 'publish'; plan_id: string; revision: number }
  | { op: 'receive'; plan_id: string; revision: number }
  | { op: 'apply'; plan_id: string; revision: number };

export interface NativeThread {
  id: string;
  project_id: string | null;
  title: string | null;
  cwd: string;
  status: string;
  model: string | null;
  effort: string | null;
}

export interface NativeThreadPage {
  threads: NativeThread[];
  next_cursor: string | null;
}

export interface AppError {
  code: string;
  message: string;
}

export interface DesktopRegistryApplyArgs {
  expectedRevision: number;
  change: RegistryChange;
}

export interface DesktopProbeToolArgs {
  toolId: string;
}

export interface DesktopSecretStatusArgs {
  toolId: string;
}

export interface DesktopSecretSetArgs {
  toolId: string;
  secret: string;
}

export interface DesktopSecretDeleteArgs {
  toolId: string;
}

export interface DesktopDecisionModelsArgs {
  toolId: string;
  provider: DecisionProvider;
}

export interface DesktopTestDecisionArgs {
  toolId: string;
  provider: DecisionProvider;
  model: string;
}

export interface DesktopPlanChangeArgs {
  expectedRevision: number;
  change: PlanChange;
}

export interface DesktopNativeThreadsArgs {
  toolId: string;
  projectId?: string | null;
  cursor?: string | null;
}

export function createEmptyRegistrySnapshot(): RegistrySnapshot {
  return {
    schema_version: 1,
    revision: 0,
    tools: [],
    models: [],
    profiles: [],
    default_profile_id: null,
  };
}

/**
 * Resolves a role/difficulty assignment from a profile in the snapshot.
 * Exact difficulty match takes precedence, followed by explicit role default (`difficulty: null`).
 * Both `advisor` and legacy `architect` select the same semantic slot. Ambiguous
 * duplicate slots fail closed instead of choosing an arbitrary assignment. Never
 * substitutes another role or unconfigured model.
 */
export function resolve_choice(
  snapshot: RegistrySnapshot,
  profileId: string,
  role: WorkflowRole,
  difficulty: Difficulty | null = null
): ModelChoice {
  const profile = snapshot.profiles.find((p) => p.id === profileId);
  if (!profile) {
    throw {
      code: 'profile_not_found',
      message: `Profile "${profileId}" was not found in the registry.`,
    } satisfies AppError;
  }

  const canonicalRole = canonicalWorkflowRole(role);
  const matchingRole = profile.assignments.filter(
    (assignment) => canonicalWorkflowRole(assignment.role) === canonicalRole
  );
  const seenDifficulties = new Set<Difficulty | null>();
  for (const assignment of matchingRole) {
    if (seenDifficulties.has(assignment.difficulty)) {
      const diffLabel = assignment.difficulty ?? 'default';
      throw {
        code: 'duplicate_assignment',
        message: `Profile "${profile.name}" has ambiguous duplicate assignments for role "${canonicalRole}" (${diffLabel}).`,
      } satisfies AppError;
    }
    seenDifficulties.add(assignment.difficulty);
  }

  let matched: RoleAssignment | undefined;
  if (difficulty !== null) {
    matched = matchingRole.find((assignment) => assignment.difficulty === difficulty);
  }
  if (!matched) {
    matched = matchingRole.find((assignment) => assignment.difficulty === null);
  }
  if (!matched) {
    const diffLabel = difficulty ? ` (${difficulty})` : '';
    throw {
      code: 'missing_assignment',
      message: `Profile "${profile.name}" has no model assignment for role "${canonicalRole}"${diffLabel}.`,
    } satisfies AppError;
  }

  const model = snapshot.models.find((m) => m.id === matched.choice.model_id);
  if (!model) {
    throw {
      code: 'model_not_found',
      message: `Assigned model "${matched.choice.model_id}" does not exist in the registry.`,
    } satisfies AppError;
  }

  const tool = snapshot.tools.find((t) => t.id === model.tool_id);
  if (!tool || !tool.enabled) {
    throw {
      code: 'tool_disabled',
      message: `Tool "${model.tool_id}" for model "${model.label}" is missing or disabled.`,
    } satisfies AppError;
  }

  return {
    model_id: matched.choice.model_id,
    effort: matched.choice.effort,
  };
}

export const resolveChoice = resolve_choice;
