import type { ModelChoice, ModelDefinition, ToolDefinition } from './types.ts';

export const ENCODED_EFFORT_ORDER = [
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
  'ultra',
] as const;

export type EncodedEffort = (typeof ENCODED_EFFORT_ORDER)[number];

const ENCODED_EFFORT_RANK = new Map<string, number>(
  ENCODED_EFFORT_ORDER.map((effort, index) => [effort, index])
);

export interface ModelFamily {
  key: string;
  label: string;
  tool_id: string;
  members: ModelDefinition[];
  variants: Array<{ effort: string; model: ModelDefinition }>;
  encodedEfforts: boolean;
}

interface ParsedEncodedCandidate {
  model: ModelDefinition;
  toolId: string;
  baseLabel: string;
  baseModelArg: string;
  effort: EncodedEffort;
  groupIdentity: string;
}

function isRecognizedEncodedEffort(value: string): value is EncodedEffort {
  return ENCODED_EFFORT_RANK.has(value);
}

function parseParenthesizedLabelEffort(
  rawLabel: string
): { baseLabel: string; effort: EncodedEffort } | null {
  const trimmed = rawLabel.trim();
  if (!trimmed.endsWith(')')) {
    return null;
  }
  const openIndex = trimmed.lastIndexOf('(');
  if (openIndex <= 0) {
    return null;
  }
  const beforeParen = trimmed.slice(0, openIndex);
  if (!/\s$/.test(beforeParen)) {
    return null;
  }
  const baseLabel = beforeParen.trim();
  if (!baseLabel) {
    return null;
  }
  const inner = trimmed.slice(openIndex + 1, -1).trim();
  if (!inner || inner.includes('(') || inner.includes(')')) {
    return null;
  }
  // Require TitleCase or uppercase/lowercase recognized token; reject arbitrary casing or extra words.
  const lower = inner.toLowerCase();
  if (!isRecognizedEncodedEffort(lower)) {
    return null;
  }
  return { baseLabel, effort: lower };
}

function parseModelArgEffort(
  rawModelArg: string
): { baseModelArg: string; effort: EncodedEffort } | null {
  const trimmed = rawModelArg.trim();
  const dashIndex = trimmed.lastIndexOf('-');
  if (dashIndex <= 0 || dashIndex === trimmed.length - 1) {
    return null;
  }
  const baseModelArg = trimmed.slice(0, dashIndex);
  const suffix = trimmed.slice(dashIndex + 1);
  if (
    !baseModelArg ||
    baseModelArg.endsWith('-') ||
    /\s/.test(baseModelArg) ||
    baseModelArg.includes('/')
  ) {
    return null;
  }
  if (!isRecognizedEncodedEffort(suffix)) {
    return null;
  }
  return { baseModelArg, effort: suffix };
}

function parseEncodedCandidate(
  model: ModelDefinition,
  antigravityToolIds: ReadonlySet<string>
): ParsedEncodedCandidate | null {
  const toolId = model.tool_id?.trim() ?? '';
  const modelId = model.id?.trim() ?? '';
  if (!toolId || !modelId || !antigravityToolIds.has(toolId)) {
    return null;
  }
  if (model.allow_no_effort !== true) {
    return null;
  }
  if (!Array.isArray(model.efforts) || model.efforts.some((entry) => entry.trim() !== '')) {
    return null;
  }

  const labelParts = parseParenthesizedLabelEffort(model.label ?? '');
  if (!labelParts) {
    return null;
  }
  const argParts = parseModelArgEffort(model.model_arg ?? '');
  if (!argParts) {
    return null;
  }
  if (labelParts.effort !== argParts.effort) {
    return null;
  }

  const groupIdentity = `${toolId}\u0000${argParts.baseModelArg}\u0000${labelParts.baseLabel}`;
  return {
    model,
    toolId,
    baseLabel: labelParts.baseLabel,
    baseModelArg: argParts.baseModelArg,
    effort: argParts.effort,
    groupIdentity,
  };
}

function buildOrdinaryFamily(
  model: ModelDefinition,
  usedKeys: Set<string>
): ModelFamily {
  const baseKey = `ordinary:${model.tool_id}:${model.id}`;
  let key = baseKey;
  let suffix = 2;
  while (usedKeys.has(key)) {
    key = `${baseKey}#${suffix}`;
    suffix += 1;
  }
  usedKeys.add(key);

  return {
    key,
    label: model.label || model.id,
    tool_id: model.tool_id,
    members: [model],
    variants: [],
    encodedEfforts: false,
  };
}

export function groupModelFamilies(
  models: readonly ModelDefinition[],
  tools: readonly ToolDefinition[]
): ModelFamily[] {
  const antigravityToolIds = new Set<string>();
  for (const tool of tools) {
    if (tool && tool.adapter === 'antigravity' && tool.id?.trim()) {
      antigravityToolIds.add(tool.id.trim());
    }
  }

  const parsedByIndex = new Map<number, ParsedEncodedCandidate>();
  const candidatesByGroup = new Map<string, ParsedEncodedCandidate[]>();
  const candidatesByToolAndArg = new Map<string, ParsedEncodedCandidate[]>();
  const candidatesByToolAndLabel = new Map<string, ParsedEncodedCandidate[]>();

  models.forEach((model, index) => {
    const candidate = parseEncodedCandidate(model, antigravityToolIds);
    if (!candidate) {
      return;
    }
    parsedByIndex.set(index, candidate);

    const groupList = candidatesByGroup.get(candidate.groupIdentity) ?? [];
    groupList.push(candidate);
    candidatesByGroup.set(candidate.groupIdentity, groupList);

    const argKey = `${candidate.toolId}\u0000${candidate.baseModelArg}`;
    const argList = candidatesByToolAndArg.get(argKey) ?? [];
    argList.push(candidate);
    candidatesByToolAndArg.set(argKey, argList);

    const labelKey = `${candidate.toolId}\u0000${candidate.baseLabel}`;
    const labelList = candidatesByToolAndLabel.get(labelKey) ?? [];
    labelList.push(candidate);
    candidatesByToolAndLabel.set(labelKey, labelList);
  });

  const unsafeGroupIdentities = new Set<string>();

  for (const [groupIdentity, groupCandidates] of candidatesByGroup.entries()) {
    const seenEfforts = new Set<string>();
    const seenModelIds = new Set<string>();
    for (const candidate of groupCandidates) {
      const id = candidate.model.id.trim();
      if (seenEfforts.has(candidate.effort) || seenModelIds.has(id)) {
        unsafeGroupIdentities.add(groupIdentity);
        break;
      }
      seenEfforts.add(candidate.effort);
      seenModelIds.add(id);
    }
  }

  // Also guard against duplicate effort collisions across the same (tool_id, baseModelArg)
  // or same (tool_id, baseLabel) so ambiguous candidates are never merged.
  for (const list of candidatesByToolAndArg.values()) {
    const seenEfforts = new Set<string>();
    let duplicate = false;
    for (const candidate of list) {
      if (seenEfforts.has(candidate.effort)) {
        duplicate = true;
        break;
      }
      seenEfforts.add(candidate.effort);
    }
    if (duplicate) {
      for (const candidate of list) {
        unsafeGroupIdentities.add(candidate.groupIdentity);
      }
    }
  }

  for (const list of candidatesByToolAndLabel.values()) {
    const seenEfforts = new Set<string>();
    let duplicate = false;
    for (const candidate of list) {
      if (seenEfforts.has(candidate.effort)) {
        duplicate = true;
        break;
      }
      seenEfforts.add(candidate.effort);
    }
    if (duplicate) {
      for (const candidate of list) {
        unsafeGroupIdentities.add(candidate.groupIdentity);
      }
    }
  }

  const usedKeys = new Set<string>(models.map((model) => model.id));
  const emittedGroups = new Set<string>();
  const families: ModelFamily[] = [];

  models.forEach((model, index) => {
    const candidate = parsedByIndex.get(index);
    if (!candidate || unsafeGroupIdentities.has(candidate.groupIdentity)) {
      families.push(buildOrdinaryFamily(model, usedKeys));
      return;
    }

    if (emittedGroups.has(candidate.groupIdentity)) {
      return;
    }
    emittedGroups.add(candidate.groupIdentity);

    const groupCandidates = candidatesByGroup.get(candidate.groupIdentity) ?? [candidate];
    const sortedCandidates = [...groupCandidates].sort((left, right) => {
      const leftRank = ENCODED_EFFORT_RANK.get(left.effort) ?? 999;
      const rightRank = ENCODED_EFFORT_RANK.get(right.effort) ?? 999;
      return leftRank - rightRank;
    });

    const baseKey = `family:${candidate.toolId}:${candidate.baseModelArg}`;
    let key = baseKey;
    let suffix = 2;
    while (usedKeys.has(key)) {
      key = `${baseKey}#${suffix}`;
      suffix += 1;
    }
    usedKeys.add(key);

    families.push({
      key,
      label: candidate.baseLabel,
      tool_id: candidate.toolId,
      members: sortedCandidates.map((entry) => entry.model),
      variants: sortedCandidates.map((entry) => ({
        effort: entry.effort,
        model: entry.model,
      })),
      encodedEfforts: true,
    });
  });

  return families;
}

export function visibleModelCount(
  models: readonly ModelDefinition[],
  tools: readonly ToolDefinition[]
): number {
  return groupModelFamilies(models, tools).length;
}

export function findModelFamily(
  families: readonly ModelFamily[],
  modelId: string
): ModelFamily | undefined {
  const trimmed = modelId.trim();
  if (!trimmed) {
    return undefined;
  }
  return families.find((family) =>
    family.members.some((member) => member.id === trimmed || member.id.trim() === trimmed)
  );
}

export function selectedFamilyEffort(
  family: ModelFamily,
  choice: ModelChoice
): string | null {
  const targetId = choice.model_id?.trim() ?? '';
  if (!targetId) {
    return null;
  }

  if (!family.encodedEfforts) {
    const member = family.members.find(
      (entry) => entry.id === targetId || entry.id.trim() === targetId
    );
    if (!member) {
      return null;
    }
    return choice.effort ?? null;
  }

  const variant = family.variants.find(
    (entry) => entry.model.id === targetId || entry.model.id.trim() === targetId
  );
  if (!variant) {
    return null;
  }

  const rawEffort = choice.effort?.trim() || null;
  if (rawEffort !== null) {
    return null;
  }

  return variant.effort;
}

export function modelChoiceForFamilyEffort(
  family: ModelFamily,
  effort: string | null
): ModelChoice | null {
  const normalizedEffort = effort?.trim() || null;

  if (!family.encodedEfforts) {
    const model = family.members[0];
    if (!model) {
      return null;
    }
    if (normalizedEffort === null) {
      if (!model.allow_no_effort) {
        return null;
      }
      return { model_id: model.id, effort: null };
    }
    if (!model.efforts.includes(normalizedEffort)) {
      return null;
    }
    return { model_id: model.id, effort: normalizedEffort };
  }

  if (normalizedEffort === null) {
    return null;
  }
  const lowerEffort = normalizedEffort.toLowerCase();
  const variant = family.variants.find((entry) => entry.effort === lowerEffort);
  if (!variant) {
    return null;
  }
  return {
    model_id: variant.model.id,
    effort: null,
  };
}
