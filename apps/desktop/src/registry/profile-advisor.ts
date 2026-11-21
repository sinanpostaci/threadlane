import {
  isApiToolAdapter,
  type DecisionAdvisor,
  type DecisionAdvisorMode,
  type ModelDefinition,
  type ToolAdapter,
  type ToolDefinition,
  type WorkflowProfile,
} from './types.ts';

export const DECISION_ADVISOR_MODE: DecisionAdvisorMode = 'shadow';

const MAX_DECISION_MODEL_BYTES = 128;
const JEV_DECISION_MODEL_PREFIX = 'jev-';
const DEFAULT_OPENAI_DECISION_MODEL = 'gpt-6-luna';
const OPENAI_DECISION_MODEL_PREFIX = 'gpt-6-luna-';
const DECISION_MODEL_TOKEN_PATTERN = /^[A-Za-z0-9._:-]+$/;
const MAX_ISSUE_TOKEN_CHARS = 44;

export interface DecisionAdvisorRegistry {
  readonly tools: readonly ToolDefinition[];
  readonly models: readonly ModelDefinition[];
}

export interface DecisionAdvisorChoice {
  tool: ToolDefinition;
  model: ModelDefinition;
}

export type DecisionAdvisorIssueCode =
  | 'missing_model'
  | 'missing_tool'
  | 'disabled_tool'
  | 'wrong_adapter'
  | 'effort_required'
  | 'unsupported_model';

export interface DecisionAdvisorIssue {
  code: DecisionAdvisorIssueCode;
  message: string;
}

export type DecisionAdvisorValidation =
  | {
      state: 'off';
      advisor: null;
      tool: null;
      model: null;
      issue: null;
    }
  | {
      state: 'ready';
      advisor: DecisionAdvisor;
      tool: ToolDefinition;
      model: ModelDefinition;
      issue: null;
    }
  | {
      state: 'invalid';
      advisor: DecisionAdvisor;
      tool: ToolDefinition | null;
      model: ModelDefinition | null;
      issue: DecisionAdvisorIssue;
    };

function cleanModelId(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

/**
 * Treats a missing or null saved advisor as off. Only the frozen shadow mode is
 * accepted; no effort or provider fields belong to an advisor selection.
 */
export function normalizeDecisionAdvisor(value: unknown): DecisionAdvisor | null {
  if (value === null || value === undefined || typeof value !== 'object') {
    return null;
  }
  const candidate = value as { model_id?: unknown; mode?: unknown };
  const modelId = cleanModelId(candidate.model_id);
  if (!modelId || candidate.mode !== DECISION_ADVISOR_MODE) {
    return null;
  }
  return { model_id: modelId, mode: DECISION_ADVISOR_MODE };
}

export function decisionAdvisorFromProfile(
  profile: Pick<WorkflowProfile, 'decision_advisor'>
): DecisionAdvisor | null {
  return normalizeDecisionAdvisor(profile.decision_advisor);
}

export function cloneDecisionAdvisor(value: unknown): DecisionAdvisor | null {
  const advisor = normalizeDecisionAdvisor(value);
  return advisor ? { ...advisor } : null;
}

/**
 * Stable advisor part of a profile signature. Legacy missing, null, and other
 * off representations share the same value so they cannot look like changes.
 */
export function decisionAdvisorSignature(value: unknown): string {
  const advisor = normalizeDecisionAdvisor(value);
  return advisor ? `${advisor.mode}:${advisor.model_id}` : 'off';
}

/**
 * Profile signature with canonical advisor handling. Callers pass already
 * ordered assignments; legacy off profiles and null advisors share one value.
 */
export function profileSignatureWithDecisionAdvisor(
  profile: Pick<WorkflowProfile, 'name' | 'decision_advisor'>,
  assignments: readonly unknown[]
): string {
  return JSON.stringify({
    name: profile.name,
    assignments,
    decision_advisor: decisionAdvisorSignature(profile.decision_advisor),
  });
}

/**
 * Serializes an advisor only when one is configured. An off profile omits the
 * field entirely instead of sending null or a default.
 */
export function profileWithDecisionAdvisor(
  profile: WorkflowProfile,
  value: unknown
): WorkflowProfile {
  const advisor = normalizeDecisionAdvisor(value);
  if (!advisor) {
    const next = { ...profile };
    delete next.decision_advisor;
    return next;
  }
  return { ...profile, decision_advisor: advisor };
}

function findModel(
  models: readonly ModelDefinition[],
  modelId: string
): ModelDefinition | null {
  return (
    models.find((model) => model.id === modelId) ??
    models.find((model) => model.id.trim() === modelId) ??
    null
  );
}

function findTool(
  tools: readonly ToolDefinition[],
  toolId: string
): ToolDefinition | null {
  return (
    tools.find((tool) => tool.id === toolId) ??
    tools.find((tool) => tool.id.trim() === toolId) ??
    null
  );
}

function isValidDecisionModelToken(value: string): boolean {
  return (
    value.length > 0 &&
    value.length <= MAX_DECISION_MODEL_BYTES &&
    DECISION_MODEL_TOKEN_PATTERN.test(value)
  );
}

/**
 * Matches `crates/threadlane-decisions/src/schema.rs` (`is_valid_model_token`,
 * `is_supported_jev_model`, `is_supported_openai_decision_model`) against the
 * exact native `model_arg` without trimming invalid whitespace.
 */
export function isSupportedDecisionAdvisorModel(
  adapter: ToolAdapter,
  modelArg: unknown
): boolean {
  if (typeof modelArg !== 'string' || !isValidDecisionModelToken(modelArg)) {
    return false;
  }
  if (adapter === 'jev_api') {
    return (
      modelArg.startsWith(JEV_DECISION_MODEL_PREFIX) &&
      modelArg.length > JEV_DECISION_MODEL_PREFIX.length
    );
  }
  if (adapter === 'openai_decisions_api') {
    return (
      modelArg === DEFAULT_OPENAI_DECISION_MODEL ||
      modelArg.startsWith(OPENAI_DECISION_MODEL_PREFIX)
    );
  }
  return false;
}

function formatBoundedIssueToken(value: unknown, fallback: string): string {
  if (typeof value !== 'string') {
    return fallback;
  }
  const sanitized = value.replace(/[\u0000-\u001f\u007f]+/g, ' ').trim();
  if (!sanitized) {
    return fallback;
  }
  const chars = Array.from(sanitized);
  if (chars.length <= MAX_ISSUE_TOKEN_CHARS) {
    return sanitized;
  }
  return `${chars.slice(0, MAX_ISSUE_TOKEN_CHARS - 3).join('')}...`;
}

function unsupportedModelMessage(
  tool: ToolDefinition,
  model: ModelDefinition
): string {
  const safeLabel = formatBoundedIssueToken(
    model.label || model.id,
    'selected model'
  );
  const safeTool = formatBoundedIssueToken(
    tool.label || tool.id,
    'this API tool'
  );
  const requirement =
    tool.adapter === 'jev_api'
      ? 'a jev-... model argument'
      : 'gpt-6-luna or a gpt-6-luna-... model argument';
  return (
    `Advisor model "${safeLabel}" is not supported by "${safeTool}" ` +
    `(requires ${requirement}). Choose a supported API model or clear the advisor.`
  );
}

/**
 * Enabled Jev/OpenAI decision models that permit no effort and carry a
 * runtime-supported native `model_arg`. These are the only valid advisor
 * choices and are never offered as workflow-role models.
 */
export function eligibleDecisionAdvisors(
  registry: DecisionAdvisorRegistry
): DecisionAdvisorChoice[] {
  const choices: DecisionAdvisorChoice[] = [];
  const seen = new Set<string>();

  for (const model of registry.models) {
    const modelId = model.id.trim();
    if (!modelId || seen.has(modelId) || !model.allow_no_effort) {
      continue;
    }
    const tool = findTool(registry.tools, model.tool_id);
    if (
      !tool ||
      !tool.enabled ||
      !isApiToolAdapter(tool.adapter) ||
      !isSupportedDecisionAdvisorModel(tool.adapter, model.model_arg)
    ) {
      continue;
    }
    seen.add(modelId);
    choices.push({ tool, model });
  }

  return choices;
}

export function validateDecisionAdvisor(
  registry: DecisionAdvisorRegistry,
  value: unknown
): DecisionAdvisorValidation {
  const advisor = normalizeDecisionAdvisor(value);
  if (!advisor) {
    return { state: 'off', advisor: null, tool: null, model: null, issue: null };
  }

  const model = findModel(registry.models, advisor.model_id);
  if (!model) {
    return {
      state: 'invalid',
      advisor,
      tool: null,
      model: null,
      issue: {
        code: 'missing_model',
        message:
          `Advisor model "${advisor.model_id}" is not in the registry. ` +
          'Choose another API model or clear the advisor.',
      },
    };
  }

  const modelLabel = model.label || model.id;
  const tool = findTool(registry.tools, model.tool_id);
  if (!tool) {
    return {
      state: 'invalid',
      advisor,
      tool: null,
      model,
      issue: {
        code: 'missing_tool',
        message:
          `Advisor model "${modelLabel}" refers to missing tool "${model.tool_id}". ` +
          'Choose another API model or clear the advisor.',
      },
    };
  }

  if (!tool.enabled) {
    return {
      state: 'invalid',
      advisor,
      tool,
      model,
      issue: {
        code: 'disabled_tool',
        message:
          `Advisor model "${modelLabel}" belongs to disabled tool "${tool.label}". ` +
          'Enable the tool in Tools, choose another API model, or clear the advisor.',
      },
    };
  }

  if (!isApiToolAdapter(tool.adapter)) {
    return {
      state: 'invalid',
      advisor,
      tool,
      model,
      issue: {
        code: 'wrong_adapter',
        message:
          `Model "${modelLabel}" belongs to "${tool.label}", which is not a Jev or ` +
          'OpenAI decisions API. Choose an API advisor model or clear the advisor.',
      },
    };
  }

  if (!model.allow_no_effort) {
    return {
      state: 'invalid',
      advisor,
      tool,
      model,
      issue: {
        code: 'effort_required',
        message:
          `Advisor model "${modelLabel}" requires an effort, but decision advisors ` +
          'have no effort setting. Choose a model that allows no effort.',
      },
    };
  }

  if (!isSupportedDecisionAdvisorModel(tool.adapter, model.model_arg)) {
    return {
      state: 'invalid',
      advisor,
      tool,
      model,
      issue: {
        code: 'unsupported_model',
        message: unsupportedModelMessage(tool, model),
      },
    };
  }

  return { state: 'ready', advisor, tool, model, issue: null };
}
