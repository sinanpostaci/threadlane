import { isApiToolAdapter, type ModelDefinition, type ToolDefinition } from './types.ts';

export interface ToolDisplayProjection {
  tools: ToolDefinition[];
  duplicates: Array<{ primary: ToolDefinition; duplicates: ToolDefinition[] }>;
}

function normalizeToolLabel(label: string): string {
  return label.trim().toLowerCase();
}

function normalizeExecutablePath(executablePath: string | null): string | null {
  if (executablePath === null) {
    return null;
  }
  const trimmed = executablePath.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function buildEquivalenceKey(tool: ToolDefinition): string | null {
  if (isApiToolAdapter(tool.adapter)) {
    return null;
  }
  const normalizedLabel = normalizeToolLabel(tool.label);
  if (normalizedLabel.length === 0) {
    return null;
  }
  const normalizedPath = normalizeExecutablePath(tool.executable_path);
  const pathKey = normalizedPath === null ? 'system' : `custom:${normalizedPath}`;
  return `${tool.adapter}\u0000${tool.enabled ? '1' : '0'}\u0000${normalizedLabel}\u0000${pathKey}`;
}

export function findEquivalentTool(
  candidate: ToolDefinition,
  tools: readonly ToolDefinition[],
): ToolDefinition | null {
  const candidateKey = buildEquivalenceKey(candidate);
  if (candidateKey === null) {
    return null;
  }
  for (const existing of tools) {
    if (existing.id === candidate.id) {
      continue;
    }
    if (buildEquivalenceKey(existing) === candidateKey) {
      return existing;
    }
  }
  return null;
}

export function projectToolRegistrations(
  tools: readonly ToolDefinition[],
  models: readonly ModelDefinition[],
): ToolDisplayProjection {
  const populatedToolIds = new Set<string>();
  for (const model of models) {
    populatedToolIds.add(model.tool_id);
  }

  const groupsByKey = new Map<string, ToolDefinition[]>();
  for (const tool of tools) {
    const key = buildEquivalenceKey(tool);
    if (key === null) {
      continue;
    }
    const group = groupsByKey.get(key);
    if (group) {
      group.push(tool);
    } else {
      groupsByKey.set(key, [tool]);
    }
  }

  const hiddenTools = new Set<ToolDefinition>();
  const duplicates: Array<{ primary: ToolDefinition; duplicates: ToolDefinition[] }> = [];

  for (const group of groupsByKey.values()) {
    if (group.length < 2) {
      continue;
    }
    const firstTool = group[0];
    if (!firstTool) {
      continue;
    }
    const populatedTools = group.filter((tool) => populatedToolIds.has(tool.id));
    if (populatedTools.length > 1) {
      continue;
    }
    const primary = populatedTools[0] ?? firstTool;
    const duplicateGroup = group.filter((tool) => tool !== primary);
    if (duplicateGroup.length === 0) {
      continue;
    }
    for (const duplicate of duplicateGroup) {
      hiddenTools.add(duplicate);
    }
    duplicates.push({
      primary,
      duplicates: duplicateGroup,
    });
  }

  return {
    tools: tools.filter((tool) => !hiddenTools.has(tool)),
    duplicates,
  };
}
