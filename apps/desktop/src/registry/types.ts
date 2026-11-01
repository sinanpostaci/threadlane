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

export type WorkflowRole =
  | 'follower'
  | 'orchestrator'
  | 'architect'
  | 'coder'
  | 'reviewer'
  | 'closure';

export const WORKFLOW_ROLES: readonly WorkflowRole[] = [
  'follower',
  'orchestrator',
  'architect',
  'coder',
  'reviewer',
  'closure',
] as const;

export type Difficulty = 'easy' | 'medium' | 'hard' | 'extreme';

export const DIFFICULTIES: readonly Difficulty[] = [
  'easy',
  'medium',
  'hard',
  'extreme',
] as const;

export interface RoleAssignment {
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
 * Never substitutes another role or unconfigured model.
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

  let matched: RoleAssignment | undefined;
  if (difficulty !== null) {
    matched = profile.assignments.find((a) => a.role === role && a.difficulty === difficulty);
  }
  if (!matched) {
    matched = profile.assignments.find((a) => a.role === role && a.difficulty === null);
  }
  if (!matched) {
    const diffLabel = difficulty ? ` (${difficulty})` : '';
    throw {
      code: 'missing_assignment',
      message: `Profile "${profile.name}" has no model assignment for role "${role}"${diffLabel}.`,
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
