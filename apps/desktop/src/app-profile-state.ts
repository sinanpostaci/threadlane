import {
  findModelFamily,
  groupModelFamilies,
  modelChoiceForFamilyEffort,
  selectedFamilyEffort,
  type ModelFamily,
} from './registry/model-families.ts';
import {
  cloneDecisionAdvisor,
  decisionAdvisorFromProfile,
  eligibleDecisionAdvisors,
  profileSignatureWithDecisionAdvisor,
  profileWithDecisionAdvisor,
  validateDecisionAdvisor,
  type DecisionAdvisorChoice,
  type DecisionAdvisorValidation,
} from './registry/profile-advisor.ts';
import {
  DIFFICULTIES as REGISTRY_DIFFICULTIES,
  EXECUTION_ROLES,
  WORKFLOW_ROLES,
  isApiToolAdapter,
  workflowRoleLabel,
  type DecisionAdvisor,
  type Difficulty,
  type ExecutionWorkflowRole,
  type ModelDefinition,
  type RegistrySnapshot,
  type RoleAssignment,
  type ToolDefinition,
  type WorkflowProfile,
  type WorkflowRole,
} from './registry/types.ts';

export const PROFILE_EXECUTION_ROLES: readonly ExecutionWorkflowRole[] = [
  ...EXECUTION_ROLES,
];

export const DIFFICULTIES: readonly Difficulty[] = [...REGISTRY_DIFFICULTIES];

export const DIFFICULTY_LABELS: Record<Difficulty, string> = {
  easy: 'Easy',
  medium: 'Medium',
  hard: 'Hard',
  extreme: 'Extreme',
};

export interface RoleMetadata {
  readonly id: ExecutionWorkflowRole;
  readonly label: string;
  readonly summary: string;
}

export const ROLE_METADATA: readonly RoleMetadata[] = [
  {
    id: 'orchestrator',
    label: workflowRoleLabel('orchestrator'),
    summary: 'Structures multi-task plans and dependency boundaries.',
  },
  {
    id: 'architect',
    label: workflowRoleLabel('architect'),
    summary: 'Read-only structural analysis before plan lock.',
  },
  {
    id: 'coder',
    label: workflowRoleLabel('coder'),
    summary: 'Scoped worktree implementation across difficulty tiers.',
  },
  {
    id: 'reviewer',
    label: workflowRoleLabel('reviewer'),
    summary: 'Validates patch scope, tests, and provider telemetry.',
  },
  {
    id: 'closure',
    label: workflowRoleLabel('closure'),
    summary: 'Single final acceptance check before returning to Codex.',
  },
];

export function getRoleMetadata(role: WorkflowRole): RoleMetadata {
  return (
    ROLE_METADATA.find((item) => item.id === role) ?? ROLE_METADATA[0]!
  );
}

export type Slot = 'default' | Difficulty;

export const SLOTS: readonly Slot[] = ['default', ...DIFFICULTIES];

export interface Choice {
  model_id: string;
  effort: string | null;
  pending_family?: string | null;
}

export type AssignmentTable = Record<WorkflowRole, Record<Slot, Choice>>;

export interface AssignmentIssue {
  role: WorkflowRole;
  slot: Slot;
  message: string;
}

export interface DraftAnalysis {
  errors: string[];
  issues: AssignmentIssue[];
  advisor: DecisionAdvisorValidation;
  missingDefaults: WorkflowRole[];
  unassignedRoles: WorkflowRole[];
  assignmentCount: number;
  defaultCount: number;
  overrideCount: number;
}

export interface ProfileSummary {
  assignmentCount: number;
  defaultCount: number;
  overrideCount: number;
  missingDefaults: WorkflowRole[];
  issues: AssignmentIssue[];
  advisor: DecisionAdvisorValidation;
}

export interface ProfileDraft {
  mode: 'create' | 'edit';
  id: string;
  name: string;
  nameTouched?: boolean;
  sourceId: string | null;
  sourceRevision: number;
  sourceSignature: string | null;
  cloneOfName: string | null;
  assignments: AssignmentTable;
  decisionAdvisor: DecisionAdvisor | null;
  externalChanged: boolean;
  externalDeleted: boolean;
  overwriteAcknowledged: boolean;
  acknowledgedSignature: string | null;
}

export interface ModelGroup {
  tool: ToolDefinition;
  models: ModelDefinition[];
  families: ModelFamily[];
}

export interface RegistryIndex {
  tools: Map<string, ToolDefinition>;
  models: Map<string, ModelDefinition>;
  toolList: readonly ToolDefinition[];
  modelList: readonly ModelDefinition[];
  allFamilies: ModelFamily[];
  familiesByKey: Map<string, ModelFamily>;
  enabledFamiliesByKey: Map<string, ModelFamily>;
  groups: ModelGroup[];
  advisorChoices: DecisionAdvisorChoice[];
}

export interface SafeError {
  code: string | null;
  message: string;
}

export type OperationKind = 'save' | 'delete' | 'default' | 'reload';

export interface PendingOperation {
  id: number;
  kind: OperationKind;
  targetId: string | null;
  expectedRevision: number | null;
}

export interface PanelNotice {
  tone: 'info' | 'success' | 'warn';
  text: string;
}

export type PendingSwitchIntent =
  | { kind: 'select'; profileId: string }
  | { kind: 'create' }
  | { kind: 'clone'; sourceProfileId: string };

export interface ProfilesPanelState {
  snapshot: RegistrySnapshot | null;
  selectedProfileId: string | null;
  selectedRole: ExecutionWorkflowRole;
  openDifficulty: Difficulty | null;
  compactStep: 'list' | 'editor';
  draftsById: Record<string, ProfileDraft>;
  createDraft: ProfileDraft | null;
  confirmDeleteId: string | null;
  pendingSwitch: PendingSwitchIntent | null;
  pending: PendingOperation | null;
  nextOperationId: number;
  notice: PanelNotice | null;
  error: SafeError | null;
  conflict: boolean;
}

export const MAX_PROFILE_ID_LENGTH = 64;
export const MAX_PROFILE_NAME_LENGTH = 120;
export const DELETED_SIGNATURE = '__deleted__';

export function createEmptyTable(): AssignmentTable {
  const table = {} as AssignmentTable;
  for (const role of PROFILE_EXECUTION_ROLES) {
    const slots = {} as Record<Slot, Choice>;
    for (const slot of SLOTS) {
      slots[slot] = { model_id: '', effort: null, pending_family: null };
    }
    table[role] = slots;
  }
  return table;
}

export function cloneTable(table: AssignmentTable): AssignmentTable {
  const next = createEmptyTable();
  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      const source = table[role]?.[slot];
      next[role][slot] = {
        model_id: source?.model_id ?? '',
        effort: source?.effort ?? null,
        pending_family: source?.pending_family ?? null,
      };
    }
  }
  return next;
}

export function cloneDraft(draft: ProfileDraft): ProfileDraft {
  return {
    ...draft,
    assignments: cloneTable(draft.assignments),
    decisionAdvisor: cloneDecisionAdvisor(draft.decisionAdvisor),
  };
}

export function tableFromProfile(profile: WorkflowProfile): AssignmentTable {
  const table = createEmptyTable();
  for (const assignment of profile.assignments) {
    if (!(PROFILE_EXECUTION_ROLES as readonly WorkflowRole[]).includes(assignment.role)) {
      continue;
    }
    const slot: Slot = assignment.difficulty ?? 'default';
    if (!SLOTS.includes(slot)) {
      continue;
    }
    table[assignment.role][slot] = {
      model_id: assignment.choice.model_id,
      effort: assignment.choice.effort ?? null,
      pending_family: null,
    };
  }
  return table;
}

export function assignmentsFromTable(table: AssignmentTable): RoleAssignment[] {
  const assignments: RoleAssignment[] = [];
  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      const cell = table[role]?.[slot];
      if (!cell || !cell.model_id || cell.pending_family) {
        continue;
      }
      assignments.push({
        role,
        difficulty: slot === 'default' ? null : slot,
        choice: { model_id: cell.model_id, effort: cell.effort },
      });
    }
  }
  return assignments;
}

export function profileFromDraft(draft: ProfileDraft): WorkflowProfile {
  return profileWithDecisionAdvisor(
    {
      id: draft.id,
      name: draft.name.trim(),
      assignments: assignmentsFromTable(draft.assignments),
    },
    draft.decisionAdvisor
  );
}

export function profileSignature(profile: WorkflowProfile): string {
  const assignments = profile.assignments
    .filter((assignment) =>
      (PROFILE_EXECUTION_ROLES as readonly WorkflowRole[]).includes(assignment.role)
    )
    .map((assignment) => ({
      role: assignment.role,
      difficulty: assignment.difficulty ?? null,
      model_id: assignment.choice.model_id,
      effort: assignment.choice.effort ?? null,
    }))
    .sort((left, right) => {
      const roleDelta =
        WORKFLOW_ROLES.indexOf(left.role) - WORKFLOW_ROLES.indexOf(right.role);
      if (roleDelta !== 0) {
        return roleDelta;
      }
      const leftSlot: Slot = left.difficulty ?? 'default';
      const rightSlot: Slot = right.difficulty ?? 'default';
      return SLOTS.indexOf(leftSlot) - SLOTS.indexOf(rightSlot);
    });

  return profileSignatureWithDecisionAdvisor(profile, assignments);
}

export function hasDifficultyOverride(cell: Choice | undefined | null): boolean {
  if (!cell) {
    return false;
  }
  return Boolean(cell.model_id || cell.pending_family || cell.effort !== null);
}

export function countRoleDifficultyOverrides(
  table: AssignmentTable,
  role: WorkflowRole
): number {
  const roleSlots = table[role];
  if (!roleSlots) {
    return 0;
  }
  return DIFFICULTIES.filter((diff) => hasDifficultyOverride(roleSlots[diff])).length;
}

export function countProfileDifficultyOverrides(table: AssignmentTable): number {
  return PROFILE_EXECUTION_ROLES.reduce(
    (sum, role) => sum + countRoleDifficultyOverrides(table, role),
    0
  );
}

export function hasPendingFamilyInTable(table: AssignmentTable): boolean {
  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      if (table[role]?.[slot]?.pending_family) {
        return true;
      }
    }
  }
  return false;
}

export function isDraftDirty(draft: ProfileDraft): boolean {
  if (draft.mode === 'create') {
    const hasAnyAssignment = PROFILE_EXECUTION_ROLES.some((role) =>
      SLOTS.some((slot) => hasDifficultyOverride(draft.assignments[role]?.[slot]))
    );
    return (
      draft.name.trim().length > 0 ||
      hasAnyAssignment ||
      draft.decisionAdvisor !== null ||
      Boolean(draft.nameTouched)
    );
  }
  if (
    draft.externalChanged ||
    draft.externalDeleted ||
    hasPendingFamilyInTable(draft.assignments)
  ) {
    return true;
  }
  if (draft.name !== draft.name.trim() && draft.nameTouched) {
    return true;
  }
  const currentSignature = profileSignature(profileFromDraft(draft));
  return currentSignature !== draft.sourceSignature;
}

export function needsAcknowledgement(draft: ProfileDraft): boolean {
  return draft.externalChanged && !draft.overwriteAcknowledged;
}

export function buildRegistryIndex(snapshot: RegistrySnapshot): RegistryIndex {
  const tools = new Map(snapshot.tools.map((tool) => [tool.id, tool]));
  const models = new Map(snapshot.models.map((model) => [model.id, model]));
  const allFamilies = groupModelFamilies(snapshot.models, snapshot.tools);
  const familiesByKey = new Map(allFamilies.map((family) => [family.key, family]));
  const enabledFamiliesByKey = new Map<string, ModelFamily>();
  const groups: ModelGroup[] = [];

  for (const tool of snapshot.tools) {
    if (!tool.enabled || isApiToolAdapter(tool.adapter)) {
      continue;
    }
    const toolModels = snapshot.models.filter((model) => model.tool_id === tool.id);
    const toolFamilies = allFamilies.filter((family) => family.tool_id === tool.id);
    for (const family of toolFamilies) {
      enabledFamiliesByKey.set(family.key, family);
    }
    groups.push({
      tool,
      models: toolModels,
      families: toolFamilies,
    });
  }

  const advisorChoices = eligibleDecisionAdvisors({
    tools: snapshot.tools,
    models: snapshot.models,
  });

  return {
    tools,
    models,
    toolList: snapshot.tools,
    modelList: snapshot.models,
    allFamilies,
    familiesByKey,
    enabledFamiliesByKey,
    groups,
    advisorChoices,
  };
}

export function resolveFamilyForCell(
  index: RegistryIndex,
  cell: Choice
): ModelFamily | undefined {
  if (cell.pending_family) {
    return (
      index.enabledFamiliesByKey.get(cell.pending_family) ??
      index.familiesByKey.get(cell.pending_family)
    );
  }
  if (!cell.model_id) {
    return undefined;
  }
  return findModelFamily(index.allFamilies, cell.model_id);
}

export function effectiveCellEffort(
  index: RegistryIndex,
  cell: Choice
): string | null {
  if (!cell.model_id) {
    return cell.effort;
  }
  const family = resolveFamilyForCell(index, cell);
  if (family && family.encodedEfforts) {
    return selectedFamilyEffort(family, cell) ?? cell.effort;
  }
  return cell.effort;
}

function normalizeChoiceForFamily(
  family: ModelFamily,
  preferredEffort: string | null,
  explicitModelId?: string | null
): Choice {
  if (family.encodedEfforts) {
    if (explicitModelId) {
      const explicitVariant = family.variants.find(
        (variant) => variant.model.id === explicitModelId
      );
      if (explicitVariant) {
        const mapped = modelChoiceForFamilyEffort(family, explicitVariant.effort);
        if (mapped) {
          return {
            model_id: mapped.model_id,
            effort: mapped.effort,
            pending_family: null,
          };
        }
      }
    }

    if (preferredEffort !== null) {
      const mapped = modelChoiceForFamilyEffort(family, preferredEffort);
      if (mapped) {
        return {
          model_id: mapped.model_id,
          effort: mapped.effort,
          pending_family: null,
        };
      }
    }

    const firstVariant = family.variants[0];
    if (firstVariant) {
      const mapped = modelChoiceForFamilyEffort(family, firstVariant.effort);
      if (mapped) {
        return {
          model_id: mapped.model_id,
          effort: mapped.effort,
          pending_family: null,
        };
      }
    }

    return {
      model_id: '',
      effort: null,
      pending_family: family.key,
    };
  }

  const model = family.members[0];
  if (!model) {
    return { model_id: '', effort: null, pending_family: null };
  }
  return normalizeChoiceForModel(model, preferredEffort);
}

function normalizeChoiceForModel(
  model: ModelDefinition,
  preferredEffort: string | null
): Choice {
  if (preferredEffort !== null && model.efforts.includes(preferredEffort)) {
    return {
      model_id: model.id,
      effort: preferredEffort,
      pending_family: null,
    };
  }
  if (preferredEffort === null && model.allow_no_effort && model.efforts.length === 0) {
    return {
      model_id: model.id,
      effort: null,
      pending_family: null,
    };
  }
  if (model.efforts.length > 0) {
    return {
      model_id: model.id,
      effort: model.efforts[0]!,
      pending_family: null,
    };
  }
  if (model.allow_no_effort) {
    return {
      model_id: model.id,
      effort: null,
      pending_family: null,
    };
  }
  return {
    model_id: model.id,
    effort: null,
    pending_family: null,
  };
}

/**
 * Pure transition when a user selects a model or family (or clears/inherits)
 * for a role's default or difficulty override slot.
 *
 * - Selecting `""` resets that single slot (`default` -> unassigned, difficulty -> inherit)
 *   without touching other slots or roles.
 * - Selecting a model or family resolves a concrete `model_id` + compatible `effort`,
 *   normalizing unsupported efforts truthfully while preserving existing compatible efforts.
 */
export function selectRoleModel(
  snapshot: RegistrySnapshot,
  table: AssignmentTable,
  role: WorkflowRole,
  slot: Slot,
  selectedValue: string
): AssignmentTable {
  const next = cloneTable(table);
  const currentCell = next[role][slot];
  const trimmed = selectedValue.trim();

  if (!trimmed) {
    next[role][slot] = { model_id: '', effort: null, pending_family: null };
    return next;
  }

  const index = buildRegistryIndex(snapshot);
  const currentEffort = effectiveCellEffort(index, currentCell);
  const defaultEffort = effectiveCellEffort(index, next[role].default);
  const preferredEffort =
    currentCell.model_id || currentCell.pending_family || currentEffort !== null
      ? currentEffort
      : slot !== 'default'
        ? defaultEffort
        : null;

  const byFamilyKey =
    index.enabledFamiliesByKey.get(trimmed) ?? index.familiesByKey.get(trimmed);
  if (byFamilyKey) {
    next[role][slot] = normalizeChoiceForFamily(
      byFamilyKey,
      preferredEffort,
      null
    );
    return next;
  }

  const familyByMember = findModelFamily(index.allFamilies, trimmed);
  if (familyByMember) {
    const isSpecificMember =
      familyByMember.encodedEfforts &&
      familyByMember.members.some((member) => member.id === trimmed);
    next[role][slot] = normalizeChoiceForFamily(
      familyByMember,
      preferredEffort,
      isSpecificMember ? trimmed : null
    );
    return next;
  }

  const directModel = index.models.get(trimmed);
  if (directModel) {
    next[role][slot] = normalizeChoiceForModel(directModel, preferredEffort);
    return next;
  }

  // Retain unknown model ID as-is without dropping existing effort if present
  next[role][slot] = {
    model_id: trimmed,
    effort: preferredEffort,
    pending_family: null,
  };
  return next;
}

/**
 * Pure transition when a user selects a reasoning effort for a role default
 * or difficulty override slot.
 *
 * - Never unexpectedly discards a concrete model.
 * - For encoded-effort model families, maps the chosen effort to the exact
 *   per-effort variant `model_id` + `effort`.
 * - For an inheriting difficulty slot where the user directly selects an effort,
 *   promotes the role default's model/family with that effort if valid.
 */
export function selectRoleEffort(
  snapshot: RegistrySnapshot,
  table: AssignmentTable,
  role: WorkflowRole,
  slot: Slot,
  rawEffortValue: string | null
): AssignmentTable {
  const next = cloneTable(table);
  const index = buildRegistryIndex(snapshot);
  const currentCell = next[role][slot];
  const nextEffort =
    rawEffortValue === null || rawEffortValue.trim() === ''
      ? null
      : rawEffortValue.trim();

  // If editing a difficulty override that is currently inheriting, promote from role default
  const targetSourceCell =
    !currentCell.model_id && !currentCell.pending_family && slot !== 'default'
      ? next[role].default
      : currentCell;

  const family = resolveFamilyForCell(index, targetSourceCell);
  if (family && family.encodedEfforts) {
    const mapped = modelChoiceForFamilyEffort(family, nextEffort);
    if (!mapped) {
      return next;
    }
    next[role][slot] = {
      model_id: mapped.model_id,
      effort: mapped.effort,
      pending_family: null,
    };
    return next;
  }

  if (!targetSourceCell.model_id) {
    return next;
  }

  if (family) {
    const mapped = modelChoiceForFamilyEffort(family, nextEffort);
    if (!mapped) {
      return next;
    }
    next[role][slot] = {
      model_id: mapped.model_id,
      effort: mapped.effort,
      pending_family: null,
    };
    return next;
  }

  const currentModel = index.models.get(targetSourceCell.model_id);
  if (!currentModel) {
    return next;
  }
  if (nextEffort === null && !currentModel.allow_no_effort) {
    return next;
  }
  if (nextEffort !== null && !currentModel.efforts.includes(nextEffort)) {
    return next;
  }
  next[role][slot] = {
    model_id: currentModel.id,
    effort: nextEffort,
    pending_family: null,
  };
  return next;
}

/**
 * Clears only the four difficulty overrides for `role`, preserving `role` default,
 * all other roles' defaults and overrides, and any other profile fields.
 */
export function clearRoleDifficultyOverrides(
  table: AssignmentTable,
  role: WorkflowRole
): AssignmentTable {
  const next = cloneTable(table);
  for (const difficulty of DIFFICULTIES) {
    next[role][difficulty] = {
      model_id: '',
      effort: null,
      pending_family: null,
    };
  }
  return next;
}

/**
 * Clears all difficulty overrides across all five execution roles for a single
 * profile table, preserving all five role defaults.
 */
export function clearAllProfileOverrides(
  table: AssignmentTable
): AssignmentTable {
  const next = cloneTable(table);
  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const difficulty of DIFFICULTIES) {
      next[role][difficulty] = {
        model_id: '',
        effort: null,
        pending_family: null,
      };
    }
  }
  return next;
}

export function clearDraftRoleOverrides(
  draft: ProfileDraft,
  role: WorkflowRole
): ProfileDraft {
  return {
    ...cloneDraft(draft),
    assignments: clearRoleDifficultyOverrides(draft.assignments, role),
  };
}

export function clearDraftProfileOverrides(draft: ProfileDraft): ProfileDraft {
  return {
    ...cloneDraft(draft),
    assignments: clearAllProfileOverrides(draft.assignments),
  };
}

function slotLabel(slot: Slot): string {
  return slot === 'default' ? 'role default' : `${DIFFICULTY_LABELS[slot]} override`;
}

export function validateTableWithIndex(
  index: RegistryIndex,
  table: AssignmentTable
): AssignmentIssue[] {
  const issues: AssignmentIssue[] = [];

  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      const cell = table[role][slot];
      if (cell.pending_family) {
        const pendingFamily = index.familiesByKey.get(cell.pending_family);
        const familyLabel = pendingFamily?.label || cell.pending_family;
        if (!pendingFamily) {
          issues.push({
            role,
            slot,
            message: `Model family "${familyLabel}" is not in the registry.`,
          });
          continue;
        }
        const tool = index.tools.get(pendingFamily.tool_id);
        if (!tool) {
          issues.push({
            role,
            slot,
            message: `Model family "${familyLabel}" refers to a missing tool.`,
          });
        } else if (!tool.enabled) {
          issues.push({
            role,
            slot,
            message: `Model family "${familyLabel}" belongs to disabled tool "${tool.label}".`,
          });
        }
        issues.push({
          role,
          slot,
          message: `Model family "${familyLabel}" requires an explicit effort.`,
        });
        continue;
      }

      if (!cell.model_id) {
        if (cell.effort !== null) {
          issues.push({ role, slot, message: 'An effort is set without a model.' });
        }
        continue;
      }

      const model = index.models.get(cell.model_id);
      if (!model) {
        issues.push({
          role,
          slot,
          message: `Model "${cell.model_id}" is not in the registry.`,
        });
        continue;
      }

      const modelLabel = model.label || model.id;
      const tool = index.tools.get(model.tool_id);
      if (!tool) {
        issues.push({
          role,
          slot,
          message: `Model "${modelLabel}" refers to a missing tool.`,
        });
      } else if (!tool.enabled) {
        issues.push({
          role,
          slot,
          message: `Model "${modelLabel}" belongs to disabled tool "${tool.label}".`,
        });
      } else if (isApiToolAdapter(tool.adapter)) {
        issues.push({
          role,
          slot,
          message:
            `Model "${modelLabel}" uses "${tool.label}", a decisions API available only ` +
            'as a Decision advisor, not a workflow role. Clear this role assignment.',
        });
        continue;
      }

      const family = resolveFamilyForCell(index, cell);
      if (family && family.encodedEfforts) {
        const derived = selectedFamilyEffort(family, cell);
        if (derived === null) {
          issues.push({
            role,
            slot,
            message: `Effort "${cell.effort ?? ''}" is not supported by model "${modelLabel}".`,
          });
        }
        continue;
      }

      if (cell.effort === null) {
        if (!model.allow_no_effort) {
          issues.push({
            role,
            slot,
            message: `Model "${modelLabel}" requires an explicit effort.`,
          });
        }
      } else if (!model.efforts.includes(cell.effort)) {
        issues.push({
          role,
          slot,
          message: `Effort "${cell.effort}" is not supported by model "${modelLabel}".`,
        });
      }
    }
  }

  return issues;
}

export function analyzeDraft(
  snapshot: RegistrySnapshot,
  draft: ProfileDraft
): DraftAnalysis {
  const index = buildRegistryIndex(snapshot);
  const issues = validateTableWithIndex(index, draft.assignments);
  const advisor = validateDecisionAdvisor(
    { tools: index.toolList, models: index.modelList },
    draft.decisionAdvisor
  );
  const errors = issues.map(
    (issue) =>
      `${workflowRoleLabel(issue.role)} ${slotLabel(issue.slot)}: ${issue.message}`
  );
  if (advisor.state === 'invalid') {
    errors.push(`Decision advisor: ${advisor.issue.message}`);
  }
  const trimmedName = draft.name.trim();

  if (!trimmedName) {
    errors.unshift('Profile name is required.');
  } else if (Array.from(trimmedName).length > MAX_PROFILE_NAME_LENGTH) {
    errors.unshift(
      `Profile name must be ${MAX_PROFILE_NAME_LENGTH} characters or fewer.`
    );
  }
  if (!draft.id.trim()) {
    errors.unshift('Profile identifier is missing.');
  } else if (draft.id.length > MAX_PROFILE_ID_LENGTH) {
    errors.unshift(
      `Profile identifier must be ${MAX_PROFILE_ID_LENGTH} characters or fewer.`
    );
  }

  const missingDefaults = PROFILE_EXECUTION_ROLES.filter(
    (role) => !draft.assignments[role].default.model_id
  );
  const unassignedRoles = PROFILE_EXECUTION_ROLES.filter((role) =>
    SLOTS.every((slot) => !hasDifficultyOverride(draft.assignments[role][slot]))
  );
  let assignmentCount = 0;
  let overrideCount = 0;

  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      if (draft.assignments[role][slot].model_id) {
        assignmentCount += 1;
      }
      if (
        slot !== 'default' &&
        hasDifficultyOverride(draft.assignments[role][slot])
      ) {
        overrideCount += 1;
      }
    }
  }

  return {
    errors,
    issues,
    advisor,
    missingDefaults,
    unassignedRoles,
    assignmentCount,
    defaultCount: PROFILE_EXECUTION_ROLES.length - missingDefaults.length,
    overrideCount,
  };
}

export function summarizeProfile(
  snapshot: RegistrySnapshot,
  profile: WorkflowProfile
): ProfileSummary {
  const index = buildRegistryIndex(snapshot);
  const table = tableFromProfile(profile);
  const missingDefaults = PROFILE_EXECUTION_ROLES.filter(
    (role) => !table[role].default.model_id
  );
  let assignmentCount = 0;
  let overrideCount = 0;

  for (const role of PROFILE_EXECUTION_ROLES) {
    for (const slot of SLOTS) {
      if (table[role][slot].model_id) {
        assignmentCount += 1;
      }
      if (slot !== 'default' && hasDifficultyOverride(table[role][slot])) {
        overrideCount += 1;
      }
    }
  }

  return {
    assignmentCount,
    defaultCount: PROFILE_EXECUTION_ROLES.length - missingDefaults.length,
    overrideCount,
    missingDefaults,
    issues: validateTableWithIndex(index, table),
    advisor: validateDecisionAdvisor(
      { tools: snapshot.tools, models: snapshot.models },
      profile.decision_advisor
    ),
  };
}

export function normalizeError(error: unknown): SafeError {
  const objectError =
    error !== null && typeof error === 'object'
      ? (error as { code?: unknown; message?: unknown })
      : null;
  const code =
    objectError && typeof objectError.code === 'string'
      ? objectError.code.trim().slice(0, 64)
      : null;
  let message =
    objectError && typeof objectError.message === 'string'
      ? objectError.message
      : null;

  if (!message && error instanceof Error) {
    message = error.message;
  }
  if (!message && typeof error === 'string') {
    message = error;
  }

  return {
    code: code || null,
    message:
      (message ?? 'Registry request failed.')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 240) || 'Registry request failed.',
  };
}

export function isRevisionConflict(error: SafeError): boolean {
  const text = `${error.code ?? ''} ${error.message}`.toLowerCase();
  return (
    text.includes('revision') ||
    text.includes('conflict') ||
    text.includes('stale')
  );
}

export function allocateProfileId(
  snapshot: RegistrySnapshot,
  extraExcludedIds: Iterable<string> = []
): string {
  const existing = new Set(snapshot.profiles.map((profile) => profile.id));
  for (const extra of extraExcludedIds) {
    existing.add(extra);
  }
  if (typeof globalThis.crypto?.randomUUID === 'function') {
    for (let attempt = 0; attempt < 16; attempt += 1) {
      const candidate = `profile-${globalThis.crypto.randomUUID()}`.slice(
        0,
        MAX_PROFILE_ID_LENGTH
      );
      if (candidate.length > 0 && !existing.has(candidate)) {
        return candidate;
      }
    }
  }
  for (let suffix = 1; suffix <= 10_000; suffix += 1) {
    const candidate = `profile-r${snapshot.revision}-${suffix}`;
    if (
      candidate.length <= MAX_PROFILE_ID_LENGTH &&
      !existing.has(candidate)
    ) {
      return candidate;
    }
  }
  throw new Error('Could not allocate a unique profile identifier.');
}

export function copyProfileName(name: string): string {
  const characters = Array.from(`${name.trim()} copy`);
  return characters.slice(0, MAX_PROFILE_NAME_LENGTH).join('');
}

export function createEditDraftFromProfile(
  snapshot: RegistrySnapshot,
  profile: WorkflowProfile
): ProfileDraft {
  return {
    mode: 'edit',
    id: profile.id,
    name: profile.name,
    nameTouched: false,
    sourceId: profile.id,
    sourceRevision: snapshot.revision,
    sourceSignature: profileSignature(profile),
    cloneOfName: null,
    assignments: tableFromProfile(profile),
    decisionAdvisor: decisionAdvisorFromProfile(profile),
    externalChanged: false,
    externalDeleted: false,
    overwriteAcknowledged: false,
    acknowledgedSignature: null,
  };
}

function chooseInitialProfileId(snapshot: RegistrySnapshot | null): string | null {
  if (!snapshot || snapshot.profiles.length === 0) {
    return null;
  }
  if (
    snapshot.default_profile_id &&
    snapshot.profiles.some((p) => p.id === snapshot.default_profile_id)
  ) {
    return snapshot.default_profile_id;
  }
  return snapshot.profiles[0]!.id;
}

export function createProfilesPanelState(
  snapshot: RegistrySnapshot | null = null
): ProfilesPanelState {
  const draftsById: Record<string, ProfileDraft> = {};
  if (snapshot) {
    for (const profile of snapshot.profiles) {
      draftsById[profile.id] = createEditDraftFromProfile(snapshot, profile);
    }
  }
  return {
    snapshot,
    selectedProfileId: chooseInitialProfileId(snapshot),
    selectedRole: 'coder',
    openDifficulty: 'easy',
    compactStep: 'list',
    draftsById,
    createDraft: null,
    confirmDeleteId: null,
    pendingSwitch: null,
    pending: null,
    nextOperationId: 1,
    notice: null,
    error: null,
    conflict: false,
  };
}

export function getActiveDraft(state: ProfilesPanelState): ProfileDraft | null {
  if (!state.selectedProfileId) {
    return null;
  }
  if (state.createDraft && state.selectedProfileId === state.createDraft.id) {
    return state.createDraft;
  }
  return state.draftsById[state.selectedProfileId] ?? null;
}

/**
 * Reconciles an incoming `RegistrySnapshot` with existing drafts.
 *
 * - Never resets a dirty edit draft or an active create/clone draft.
 * - Distinguishes unrelated registry changes (e.g. another profile or tool changed)
 *   from actual drift or external deletion of a dirty draft's source profile.
 * - Clean drafts are refreshed seamlessly to match the latest snapshot.
 */
export function syncRegistrySnapshot(
  state: ProfilesPanelState,
  snapshot: RegistrySnapshot | null
): ProfilesPanelState {
  if (!snapshot) {
    return {
      ...state,
      snapshot: null,
    };
  }
  if (state.snapshot && snapshot.revision < state.snapshot.revision) {
    return state;
  }

  const nextDraftsById: Record<string, ProfileDraft> = {};
  const profileMap = new Map(snapshot.profiles.map((p) => [p.id, p]));

  // Reconcile existing tracked drafts
  for (const [profileId, existingDraft] of Object.entries(state.draftsById)) {
    const latestProfile = profileMap.get(profileId);
    const dirty = isDraftDirty(existingDraft);

    if (!dirty) {
      if (latestProfile) {
        nextDraftsById[profileId] = createEditDraftFromProfile(
          snapshot,
          latestProfile
        );
      }
      continue;
    }

    // Dirty draft: preserve user edits and detect drift or deletion
    const preserved = cloneDraft(existingDraft);
    if (!latestProfile) {
      preserved.externalChanged = true;
      preserved.externalDeleted = true;
      if (preserved.acknowledgedSignature !== DELETED_SIGNATURE) {
        preserved.overwriteAcknowledged = false;
        preserved.acknowledgedSignature = null;
      }
      nextDraftsById[profileId] = preserved;
      continue;
    }

    const latestSig = profileSignature(latestProfile);
    if (latestSig !== preserved.sourceSignature) {
      preserved.externalChanged = true;
      preserved.externalDeleted = false;
      if (preserved.acknowledgedSignature !== latestSig) {
        preserved.overwriteAcknowledged = false;
        preserved.acknowledgedSignature = null;
      }
    } else {
      // Unrelated registry revision change: source profile is unchanged
      preserved.externalChanged = false;
      preserved.externalDeleted = false;
      preserved.overwriteAcknowledged = false;
      preserved.acknowledgedSignature = null;
      preserved.sourceRevision = snapshot.revision;
    }
    nextDraftsById[profileId] = preserved;
  }

  // Add clean drafts for any newly appeared profiles
  for (const profile of snapshot.profiles) {
    if (!nextDraftsById[profile.id]) {
      nextDraftsById[profile.id] = createEditDraftFromProfile(snapshot, profile);
    }
  }

  // Ensure createDraft ID never collides with a newly arrived profile ID
  let nextCreateDraft = state.createDraft ? cloneDraft(state.createDraft) : null;
  let nextSelectedId = state.selectedProfileId;

  const savingCreateDraft =
    state.pending?.kind === 'save' &&
    nextCreateDraft !== null &&
    (state.pending.targetId === nextCreateDraft.id ||
      (state.pending.targetId === null &&
        state.selectedProfileId === nextCreateDraft.id));

  if (
    nextCreateDraft &&
    profileMap.has(nextCreateDraft.id) &&
    !savingCreateDraft
  ) {
    const oldCreateId = nextCreateDraft.id;
    const safeId = allocateProfileId(snapshot, Object.keys(nextDraftsById));
    nextCreateDraft.id = safeId;
    if (nextSelectedId === oldCreateId) {
      nextSelectedId = safeId;
    }
  }

  const selectionStillValid =
    nextSelectedId !== null &&
    ((nextCreateDraft && nextSelectedId === nextCreateDraft.id) ||
      Boolean(nextDraftsById[nextSelectedId]));

  if (!selectionStillValid) {
    nextSelectedId = chooseInitialProfileId(snapshot);
  }

  return {
    ...state,
    snapshot,
    draftsById: nextDraftsById,
    createDraft: nextCreateDraft,
    selectedProfileId: nextSelectedId,
  };
}

function applySwitchIntent(
  state: ProfilesPanelState,
  intent: PendingSwitchIntent,
  compact: boolean,
  explicitNewId?: string
): ProfilesPanelState {
  if (intent.kind === 'select') {
    const isCreate =
      state.createDraft && state.createDraft.id === intent.profileId;
    if (!isCreate && !state.draftsById[intent.profileId]) {
      return state;
    }
    return {
      ...state,
      selectedProfileId: intent.profileId,
      compactStep: compact ? 'editor' : state.compactStep,
      pendingSwitch: null,
      confirmDeleteId: null,
      notice: null,
    };
  }

  if (!state.snapshot) {
    return state;
  }

  const excluded = [
    ...Object.keys(state.draftsById),
    ...(state.createDraft ? [state.createDraft.id] : []),
  ];
  const id =
    explicitNewId &&
    explicitNewId.trim().length > 0 &&
    explicitNewId.trim().length <= MAX_PROFILE_ID_LENGTH &&
    !excluded.includes(explicitNewId.trim()) &&
    !state.snapshot.profiles.some((p) => p.id === explicitNewId.trim())
      ? explicitNewId.trim()
      : allocateProfileId(state.snapshot, excluded);

  if (intent.kind === 'create') {
    const draft: ProfileDraft = {
      mode: 'create',
      id,
      name: '',
      nameTouched: false,
      sourceId: null,
      sourceRevision: state.snapshot.revision,
      sourceSignature: null,
      cloneOfName: null,
      assignments: createEmptyTable(),
      decisionAdvisor: null,
      externalChanged: false,
      externalDeleted: false,
      overwriteAcknowledged: false,
      acknowledgedSignature: null,
    };
    return {
      ...state,
      createDraft: draft,
      selectedProfileId: draft.id,
      compactStep: compact ? 'editor' : state.compactStep,
      pendingSwitch: null,
      confirmDeleteId: null,
      notice: null,
      error: null,
      conflict: false,
    };
  }

  const sourceProfile = state.snapshot.profiles.find(
    (p) => p.id === intent.sourceProfileId
  );
  if (!sourceProfile) {
    return state;
  }

  const draft: ProfileDraft = {
    mode: 'create',
    id,
    name: copyProfileName(sourceProfile.name),
    nameTouched: false,
    sourceId: null,
    sourceRevision: state.snapshot.revision,
    sourceSignature: null,
    cloneOfName: sourceProfile.name,
    assignments: cloneTable(tableFromProfile(sourceProfile)),
    decisionAdvisor: cloneDecisionAdvisor(sourceProfile.decision_advisor),
    externalChanged: false,
    externalDeleted: false,
    overwriteAcknowledged: false,
    acknowledgedSignature: null,
  };

  return {
    ...state,
    createDraft: draft,
    selectedProfileId: draft.id,
    compactStep: compact ? 'editor' : state.compactStep,
    pendingSwitch: null,
    confirmDeleteId: null,
    notice: null,
    error: null,
    conflict: false,
  };
}

/**
 * Selects a profile to inspect or edit.
 *
 * - If the current draft is clean, switches immediately.
 * - If the current draft is dirty:
 *   - Per-profile edit drafts remain preserved in `draftsById`, and `pendingSwitch`
 *     is recorded so the UI can also offer explicit Keep draft & switch / Discard & switch / Cancel,
 *     or `keepDraftAndSwitch` can be used directly without losing any edits.
 */
export function selectProfile(
  state: ProfilesPanelState,
  profileId: string,
  compact: boolean = false
): ProfilesPanelState {
  if (state.pending !== null) {
    return state;
  }
  if (state.selectedProfileId === profileId) {
    return {
      ...state,
      compactStep: compact ? 'editor' : state.compactStep,
      pendingSwitch: null,
    };
  }

  const active = getActiveDraft(state);
  if (active && isDraftDirty(active)) {
    return {
      ...state,
      pendingSwitch: { kind: 'select', profileId },
    };
  }

  return applySwitchIntent(
    {
      ...state,
      createDraft:
        state.createDraft && !isDraftDirty(state.createDraft)
          ? null
          : state.createDraft,
    },
    { kind: 'select', profileId },
    compact
  );
}

export function beginCreateProfile(
  state: ProfilesPanelState,
  compact: boolean = false,
  explicitNewId?: string
): ProfilesPanelState {
  if (!state.snapshot || state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (
    (active && isDraftDirty(active)) ||
    (state.createDraft && isDraftDirty(state.createDraft))
  ) {
    return {
      ...state,
      pendingSwitch: { kind: 'create' },
    };
  }
  return applySwitchIntent(state, { kind: 'create' }, compact, explicitNewId);
}

export function beginCloneProfile(
  state: ProfilesPanelState,
  sourceProfileId: string,
  compact: boolean = false,
  explicitNewId?: string
): ProfilesPanelState {
  if (!state.snapshot || state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (
    (active && isDraftDirty(active)) ||
    (state.createDraft && isDraftDirty(state.createDraft))
  ) {
    return {
      ...state,
      pendingSwitch: { kind: 'clone', sourceProfileId },
    };
  }
  return applySwitchIntent(
    state,
    { kind: 'clone', sourceProfileId },
    compact,
    explicitNewId
  );
}

/**
 * Keeps the current dirty draft intact in `draftsById` (or `createDraft` when switching
 * to an existing profile) and completes the pending switch.
 */
export function keepDraftAndSwitch(
  state: ProfilesPanelState,
  compact: boolean = false,
  explicitNewId?: string
): ProfilesPanelState {
  if (!state.pendingSwitch || state.pending !== null) {
    return state;
  }
  return applySwitchIntent(state, state.pendingSwitch, compact, explicitNewId);
}

/**
 * Discards the current dirty draft and completes the pending switch.
 */
export function discardDraftAndSwitch(
  state: ProfilesPanelState,
  compact: boolean = false,
  explicitNewId?: string
): ProfilesPanelState {
  if (!state.pendingSwitch || state.pending !== null) {
    return state;
  }
  const discarded = discardActiveDraft(state);
  return applySwitchIntent(
    discarded,
    state.pendingSwitch,
    compact,
    explicitNewId
  );
}

export function cancelPendingSwitch(
  state: ProfilesPanelState
): ProfilesPanelState {
  if (!state.pendingSwitch) {
    return state;
  }
  return {
    ...state,
    pendingSwitch: null,
  };
}

export function selectWorkflowRole(
  state: ProfilesPanelState,
  role: ExecutionWorkflowRole
): ProfilesPanelState {
  if (!PROFILE_EXECUTION_ROLES.includes(role)) {
    return state;
  }
  return {
    ...state,
    selectedRole: role,
  };
}

export function toggleDifficultyAccordion(
  state: ProfilesPanelState,
  difficulty: Difficulty
): ProfilesPanelState {
  if (!DIFFICULTIES.includes(difficulty)) {
    return state;
  }
  return {
    ...state,
    openDifficulty: state.openDifficulty === difficulty ? null : difficulty,
  };
}

export function setCompactStep(
  state: ProfilesPanelState,
  step: 'list' | 'editor'
): ProfilesPanelState {
  return {
    ...state,
    compactStep: step,
  };
}

export function updateActiveDraft(
  state: ProfilesPanelState,
  updater: (draft: ProfileDraft) => ProfileDraft
): ProfilesPanelState {
  if (state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (!active) {
    return state;
  }
  const updated = updater(cloneDraft(active));
  if (state.createDraft && active.id === state.createDraft.id) {
    return {
      ...state,
      createDraft: updated,
      selectedProfileId: updated.id,
    };
  }
  return {
    ...state,
    draftsById: {
      ...state.draftsById,
      [active.sourceId ?? active.id]: updated,
    },
  };
}

/**
 * Discards unsaved changes on the active draft.
 * - For an edit draft whose profile still exists in the snapshot, resets it to
 *   the clean snapshot state.
 * - For an externally deleted draft or a create/clone draft, closes the draft
 *   and selects an existing profile.
 */
export function discardActiveDraft(
  state: ProfilesPanelState
): ProfilesPanelState {
  if (state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (!active) {
    return state;
  }

  if (state.createDraft && active.id === state.createDraft.id) {
    return {
      ...state,
      createDraft: null,
      selectedProfileId: chooseInitialProfileId(state.snapshot),
      pendingSwitch: null,
      notice: null,
      error: null,
      conflict: false,
    };
  }

  const key = active.sourceId ?? active.id;
  const savedProfile = state.snapshot?.profiles.find((p) => p.id === key);
  const nextDraftsById = { ...state.draftsById };

  if (savedProfile && state.snapshot) {
    nextDraftsById[key] = createEditDraftFromProfile(
      state.snapshot,
      savedProfile
    );
    return {
      ...state,
      draftsById: nextDraftsById,
      pendingSwitch: null,
      notice: null,
      error: null,
      conflict: false,
    };
  }

  delete nextDraftsById[key];
  return {
    ...state,
    draftsById: nextDraftsById,
    selectedProfileId: chooseInitialProfileId(state.snapshot),
    pendingSwitch: null,
    notice: null,
    error: null,
    conflict: false,
  };
}

/**
 * Replaces the active draft with the latest saved version from `state.snapshot`,
 * or closes it if the profile was externally deleted.
 */
export function loadLatestIntoActiveDraft(
  state: ProfilesPanelState
): ProfilesPanelState {
  if (!state.snapshot || state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (!active || active.mode !== 'edit' || !active.sourceId) {
    return state;
  }

  const key = active.sourceId;
  const latest = state.snapshot.profiles.find((p) => p.id === key);
  const nextDraftsById = { ...state.draftsById };

  if (!latest) {
    delete nextDraftsById[key];
    return {
      ...state,
      draftsById: nextDraftsById,
      selectedProfileId: chooseInitialProfileId(state.snapshot),
      notice: {
        tone: 'warn',
        text: 'The saved profile no longer exists in the latest registry. The draft was closed.',
      },
      error: null,
      conflict: false,
    };
  }

  nextDraftsById[key] = createEditDraftFromProfile(state.snapshot, latest);
  return {
    ...state,
    draftsById: nextDraftsById,
    notice: {
      tone: 'info',
      text: 'Loaded the latest saved version into the editor.',
    },
    error: null,
    conflict: false,
  };
}

/**
 * Explicitly acknowledges overwriting a changed profile or recreating an externally
 * deleted profile with a safe new ID.
 */
export function acknowledgeActiveDraftOverwrite(
  state: ProfilesPanelState,
  explicitNewId?: string
): ProfilesPanelState {
  if (!state.snapshot || state.pending !== null) {
    return state;
  }
  const active = getActiveDraft(state);
  if (!active || active.mode !== 'edit' || !active.sourceId) {
    return state;
  }

  const key = active.sourceId;
  const latest = state.snapshot.profiles.find((p) => p.id === key) ?? null;
  const updated = cloneDraft(active);

  if (!latest) {
    const excluded = new Set<string>([
      key,
      ...Object.keys(state.draftsById),
      ...(state.createDraft ? [state.createDraft.id] : []),
    ]);
    const trimmedExplicit = explicitNewId?.trim() ?? '';
    const safeNewId =
      trimmedExplicit.length > 0 &&
      trimmedExplicit.length <= MAX_PROFILE_ID_LENGTH &&
      !excluded.has(trimmedExplicit) &&
      !state.snapshot.profiles.some((p) => p.id === trimmedExplicit)
        ? trimmedExplicit
        : allocateProfileId(state.snapshot, excluded);
    updated.id = safeNewId;
    updated.overwriteAcknowledged = true;
    updated.acknowledgedSignature = DELETED_SIGNATURE;
  } else {
    updated.overwriteAcknowledged = true;
    updated.acknowledgedSignature = profileSignature(latest);
  }

  return {
    ...state,
    draftsById: {
      ...state.draftsById,
      [key]: updated,
    },
    notice: null,
    error: null,
    conflict: false,
  };
}

export function requestDeleteProfile(
  state: ProfilesPanelState,
  profileId: string
): ProfilesPanelState {
  if (state.pending !== null) {
    return state;
  }
  return {
    ...state,
    confirmDeleteId: profileId,
    notice: null,
    error: null,
    conflict: false,
  };
}

export function cancelDeleteProfile(
  state: ProfilesPanelState
): ProfilesPanelState {
  return {
    ...state,
    confirmDeleteId: null,
  };
}

export function beginAsyncOperation(
  state: ProfilesPanelState,
  kind: OperationKind,
  targetId: string | null = null
): { state: ProfilesPanelState; operationId: number | null } {
  if (state.pending !== null) {
    return { state, operationId: null };
  }
  if (kind !== 'reload' && !state.snapshot) {
    return { state, operationId: null };
  }
  const operationId = state.nextOperationId;
  return {
    operationId,
    state: {
      ...state,
      pending: {
        id: operationId,
        kind,
        targetId,
        expectedRevision: state.snapshot?.revision ?? null,
      },
      nextOperationId: operationId + 1,
      notice: null,
      error: null,
      conflict: false,
    },
  };
}

/**
 * Resolves a completed mutation (`save`, `delete`, `default`) using the actual
 * snapshot returned by `desktop_registry_apply`.
 * Ignores stale async results whose `operationId` does not match `state.pending`,
 * and retains the highest-revision authoritative snapshot so delayed responses
 * never roll back newer registry state.
 */
export function resolveOperationSuccess(
  state: ProfilesPanelState,
  operationId: number,
  updatedSnapshot: RegistrySnapshot,
  successText: string
): { state: ProfilesPanelState; accepted: boolean } {
  if (!state.pending || state.pending.id !== operationId) {
    return { state, accepted: false };
  }

  const pending = state.pending;
  const authoritativeSnapshot =
    state.snapshot && state.snapshot.revision > updatedSnapshot.revision
      ? state.snapshot
      : updatedSnapshot;

  let working: ProfilesPanelState = {
    ...state,
    pending: null,
    error: null,
    conflict: false,
    confirmDeleteId:
      pending.kind === 'delete' ? null : state.confirmDeleteId,
  };

  if (pending.kind === 'save') {
    const active = getActiveDraft(state);
    const savedId = pending.targetId ?? active?.id ?? null;
    const isCreateSave =
      state.createDraft !== null &&
      (savedId === state.createDraft.id ||
        (pending.targetId === null &&
          active !== null &&
          active.id === state.createDraft.id));

    if (isCreateSave && state.createDraft) {
      const createId = state.createDraft.id;
      const targetSavedId = savedId ?? createId;
      const nextDrafts = { ...working.draftsById };
      delete nextDrafts[createId];
      delete nextDrafts[targetSavedId];
      working = {
        ...working,
        draftsById: nextDrafts,
        createDraft: null,
        selectedProfileId:
          working.selectedProfileId === createId ||
          working.selectedProfileId === targetSavedId
            ? targetSavedId
            : working.selectedProfileId,
      };
    } else {
      let sourceKey: string | null = null;
      if (savedId !== null && working.draftsById[savedId]) {
        sourceKey = savedId;
      } else if (savedId !== null) {
        for (const [key, draft] of Object.entries(working.draftsById)) {
          if (draft.id === savedId || draft.sourceId === savedId) {
            sourceKey = key;
            break;
          }
        }
      }
      if (sourceKey === null && active && active.mode === 'edit') {
        sourceKey = active.sourceId ?? active.id;
      }

      const nextDrafts = { ...working.draftsById };
      if (sourceKey !== null) {
        delete nextDrafts[sourceKey];
      }
      if (savedId !== null) {
        delete nextDrafts[savedId];
      }
      const shouldSelectSaved =
        savedId !== null &&
        (working.selectedProfileId === sourceKey ||
          working.selectedProfileId === savedId ||
          working.selectedProfileId === null);
      working = {
        ...working,
        draftsById: nextDrafts,
        selectedProfileId: shouldSelectSaved
          ? savedId
          : working.selectedProfileId,
      };
    }
  } else if (pending.kind === 'delete' && pending.targetId) {
    const nextDrafts = { ...working.draftsById };
    delete nextDrafts[pending.targetId];
    working = {
      ...working,
      draftsById: nextDrafts,
      selectedProfileId:
        working.selectedProfileId === pending.targetId
          ? null
          : working.selectedProfileId,
    };
  }

  const synced = syncRegistrySnapshot(working, authoritativeSnapshot);
  return {
    accepted: true,
    state: {
      ...synced,
      notice: { tone: 'success', text: successText },
      error: null,
      conflict: false,
    },
  };
}

/**
 * Resolves a failed async operation (`save`, `delete`, `default`, `reload`).
 * Preserves the user's draft and marks `conflict: true` on `stale_revision` /
 * `revision_conflict` errors without retrying automatically.
 */
export function resolveOperationError(
  state: ProfilesPanelState,
  operationId: number,
  rawError: unknown
): { state: ProfilesPanelState; accepted: boolean } {
  if (!state.pending || state.pending.id !== operationId) {
    return { state, accepted: false };
  }
  const pendingKind = state.pending.kind;
  const safeError = normalizeError(rawError);
  const conflict =
    pendingKind !== 'reload' && isRevisionConflict(safeError);

  let cleared: ProfilesPanelState = {
    ...state,
    pending: null,
    error: safeError,
    conflict,
    notice: null,
  };
  if (
    cleared.createDraft &&
    cleared.snapshot &&
    cleared.snapshot.profiles.some((p) => p.id === cleared.createDraft!.id)
  ) {
    cleared = {
      ...syncRegistrySnapshot(cleared, cleared.snapshot),
      error: safeError,
      conflict,
      notice: null,
    };
  }

  return {
    accepted: true,
    state: cleared,
  };
}

/**
 * Resolves an explicit `Reload latest` (`desktop_registry_snapshot`) call.
 * Preserves any dirty drafts and reconciles them against the newest authoritative
 * snapshot without rolling back if a higher revision already arrived.
 */
export function resolveReloadSuccess(
  state: ProfilesPanelState,
  operationId: number,
  latestSnapshot: RegistrySnapshot
): { state: ProfilesPanelState; accepted: boolean } {
  if (
    !state.pending ||
    state.pending.id !== operationId ||
    state.pending.kind !== 'reload'
  ) {
    return { state, accepted: false };
  }

  const authoritativeSnapshot =
    state.snapshot && state.snapshot.revision > latestSnapshot.revision
      ? state.snapshot
      : latestSnapshot;

  const activeBefore = getActiveDraft(state);
  const hadDirtyDraft = activeBefore ? isDraftDirty(activeBefore) : false;

  const synced = syncRegistrySnapshot(
    {
      ...state,
      pending: null,
      error: null,
      conflict: false,
    },
    authoritativeSnapshot
  );

  return {
    accepted: true,
    state: {
      ...synced,
      notice: {
        tone: 'info',
        text: hadDirtyDraft
          ? 'Loaded the latest registry. Your draft was preserved.'
          : 'Loaded the latest registry.',
      },
      error: null,
      conflict: false,
    },
  };
}
