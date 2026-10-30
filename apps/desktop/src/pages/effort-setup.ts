import type { ModelDefinition } from '../registry/types.ts';

/** Mirrors the registry's per-model effort bound. */
export const MAX_EFFORT_OPTIONS = 32;
/** Mirrors the registry's per-effort character bound. */
export const MAX_EFFORT_OPTION_LENGTH = 64;

const CONTROL_CHARACTERS = /\p{Cc}/u;

function hasSavedEffort(value: unknown): boolean {
  return typeof value === 'string' && value.trim().length > 0;
}

function isUsableEffort(value: string): boolean {
  return (
    value.length > 0 &&
    Array.from(value).length <= MAX_EFFORT_OPTION_LENGTH &&
    !CONTROL_CHARACTERS.test(value)
  );
}

/**
 * Trims, validates, and deduplicates catalog effort tokens in display order.
 * Invalid tokens are dropped rather than written to the registry.
 */
export function normalizeDetectedEfforts(
  values: readonly unknown[] | null | undefined
): string[] {
  if (!Array.isArray(values)) return [];
  const efforts: string[] = [];
  const seen = new Set<string>();
  for (const raw of values) {
    if (typeof raw !== 'string') continue;
    const effort = raw.trim();
    if (!isUsableEffort(effort) || seen.has(effort)) continue;
    seen.add(effort);
    efforts.push(effort);
    if (efforts.length >= MAX_EFFORT_OPTIONS) break;
  }
  return efforts;
}

export interface DetectedEffortUpdatePlan {
  updates: ModelDefinition[];
}

/**
 * Builds metadata-only updates for saved models that have no effort options yet.
 *
 * A saved model is eligible only when it belongs to the same tool, its trimmed ID
 * and exact model argument match a catalog entry for that tool, and the catalog
 * provides at least one valid effort token. Saved labels, model arguments, and
 * allow-no-effort choices are always preserved. Nonempty saved effort lists are
 * never overwritten.
 */
export function planDetectedEffortUpdates(
  savedModels: readonly ModelDefinition[],
  catalogModels: readonly ModelDefinition[],
  toolId: string
): DetectedEffortUpdatePlan {
  const tool = toolId.trim();
  if (!tool) return { updates: [] };

  const detectedById = new Map<string, ModelDefinition[]>();
  for (const catalogModel of catalogModels) {
    if (!catalogModel || typeof catalogModel !== 'object') continue;
    const id = typeof catalogModel.id === 'string' ? catalogModel.id.trim() : '';
    const catalogToolId =
      typeof catalogModel.tool_id === 'string' ? catalogModel.tool_id.trim() : '';
    if (!id || catalogToolId !== tool) continue;
    const matches = detectedById.get(id);
    if (matches) matches.push(catalogModel);
    else detectedById.set(id, [catalogModel]);
  }

  const updates: ModelDefinition[] = [];
  for (const saved of savedModels) {
    if (!saved || typeof saved !== 'object') continue;
    if (saved.tool_id !== tool) continue;
    const id = typeof saved.id === 'string' ? saved.id.trim() : '';
    if (!id) continue;
    if (typeof saved.model_arg !== 'string' || typeof saved.label !== 'string') continue;
    const detected = detectedById
      .get(id)
      ?.find((catalogModel) => catalogModel.model_arg === saved.model_arg);
    if (!detected || typeof detected.model_arg !== 'string') continue;
    if (!saved.model_arg.trim() || !detected.model_arg.trim()) continue;
    const savedEfforts = Array.isArray(saved.efforts) ? saved.efforts : [];
    if (savedEfforts.some(hasSavedEffort)) continue;
    const efforts = normalizeDetectedEfforts(detected.efforts);
    if (efforts.length === 0) continue;
    updates.push({
      id: saved.id,
      tool_id: saved.tool_id,
      label: saved.label,
      model_arg: saved.model_arg,
      efforts,
      allow_no_effort: saved.allow_no_effort,
    });
  }
  return { updates };
}
