import {
  isApiToolAdapter,
  type DecisionAdvisor,
  type DecisionAdvisorMode,
  type ModelDefinition,
  type ToolDefinition,
  type WorkflowProfile,
} from './types.ts';

export const DECISION_ADVISOR_MODE: DecisionAdvisorMode = 'shadow';

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
  | 'effort_required';

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

/**
 * Enabled Jev/OpenAI decision models that permit no effort. These are the only
 * valid advisor choices and are never offered as workflow-role models.
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
    if (!tool || !tool.enabled || !isApiToolAdapter(tool.adapter)) {
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

  return { state: 'ready', advisor, tool, model, issue: null };
}
