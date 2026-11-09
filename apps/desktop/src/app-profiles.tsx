import './app-profiles.css';

import {
  Badge,
  Box,
  Button,
  Flex,
  Select,
  Separator,
  Text,
} from '@radix-ui/themes';
import {
  AlertCircle,
  ArrowLeft,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Copy,
  Plus,
  RefreshCw,
  RotateCcw,
  Trash2,
  X,
} from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

import { ToolBrand } from './app-brand.tsx';
import type { ProfilesPanelProps } from './app-contract.ts';
import {
  acknowledgeActiveDraftOverwrite,
  beginAsyncOperation,
  beginCloneProfile,
  beginCreateProfile,
  buildRegistryIndex,
  cancelDeleteProfile,
  cancelPendingSwitch,
  clearDraftProfileOverrides,
  clearDraftRoleOverrides,
  copyProfileName,
  countProfileDifficultyOverrides,
  countRoleDifficultyOverrides,
  createProfilesPanelState,
  DIFFICULTIES,
  DIFFICULTY_LABELS,
  discardActiveDraft,
  discardDraftAndSwitch,
  effectiveCellEffort,
  getActiveDraft,
  getRoleMetadata,
  hasDifficultyOverride,
  isDraftDirty,
  keepDraftAndSwitch,
  loadLatestIntoActiveDraft,
  MAX_PROFILE_NAME_LENGTH,
  needsAcknowledgement,
  PROFILE_EXECUTION_ROLES,
  requestDeleteProfile,
  resolveFamilyForCell,
  resolveOperationError,
  resolveOperationSuccess,
  resolveReloadSuccess,
  ROLE_METADATA,
  selectProfile,
  selectRoleEffort,
  selectRoleModel,
  selectWorkflowRole,
  setCompactStep,
  syncRegistrySnapshot,
  toggleDifficultyAccordion,
  updateActiveDraft,
  type AssignmentTable,
  type Choice,
  type ProfilesPanelState,
  type RegistryIndex,
  type Slot,
} from './app-profile-state.ts';
import {
  analyzeDraft,
  profileFromDraft,
  profileSignature,
  summarizeProfile,
  tableFromProfile,
} from './pages/profiles.ts';
import {
  desktop_registry_apply,
  desktop_registry_snapshot,
} from './registry/ipc.ts';
import {
  findModelFamily,
  selectedFamilyEffort,
} from './registry/model-families.ts';
import type {
  DecisionAdvisorChoice,
  DecisionAdvisorValidation,
} from './registry/profile-advisor.ts';
import {
  isApiToolAdapter,
   isExecutionRole,
  workflowRoleLabel,
  type Difficulty,
  type ModelDefinition,
  type RegistryChange,
  type ToolAdapter,
  type ToolDefinition,
  type WorkflowProfile,
  type WorkflowRole,
} from './registry/types.ts';

const UNASSIGNED_VALUE = '__unassigned__';
const INHERIT_VALUE = '__inherit__';
const NO_EFFORT_VALUE = '__no_effort__';
const UNSET_EFFORT_VALUE = '__unset_effort__';

function resolveCellToolAdapter(
  index: RegistryIndex | null,
  cell: Choice
): ToolAdapter | null {
  if (!index) {
    return null;
  }
  const family = resolveFamilyForCell(index, cell);
  if (family) {
    return index.tools.get(family.tool_id)?.adapter ?? null;
  }
  if (cell.model_id) {
    const model = index.models.get(cell.model_id);
    if (model) {
      return index.tools.get(model.tool_id)?.adapter ?? null;
    }
  }
  return null;
}

function resolveRoleToolAdapter(
  index: RegistryIndex | null,
  table: AssignmentTable,
  role: WorkflowRole
): ToolAdapter | null {
  const defaultAdapter = resolveCellToolAdapter(index, table[role].default);
  if (defaultAdapter) {
    return defaultAdapter;
  }
  for (const difficulty of DIFFICULTIES) {
    const overrideAdapter = resolveCellToolAdapter(index, table[role][difficulty]);
    if (overrideAdapter) {
      return overrideAdapter;
    }
  }
  return null;
}

function formatCellChoiceLabel(
  index: RegistryIndex | null,
  cell: Choice
): string {
  if (!cell.model_id && !cell.pending_family) {
    return 'Not assigned';
  }
  if (!index) {
    return cell.effort
      ? `${cell.model_id || cell.pending_family} · ${cell.effort}`
      : cell.model_id || cell.pending_family || 'Not assigned';
  }
  if (cell.pending_family) {
    const family = index.familiesByKey.get(cell.pending_family);
    return `${family?.label || cell.pending_family} · select effort`;
  }
  const family = findModelFamily(index.allFamilies, cell.model_id);
  const model = index.models.get(cell.model_id);
  const label = family?.encodedEfforts
    ? family.label
    : model?.label || cell.model_id;
  const effort = effectiveCellEffort(index, cell);
  return effort ? `${label} · ${effort}` : label;
}

function formatAdvisorSummary(validation: DecisionAdvisorValidation): string {
  if (validation.state === 'off') {
    return 'Off';
  }
  if (validation.state === 'invalid') {
    return 'Needs repair';
  }
  return `${validation.tool.label} · ${validation.model.label}`;
}

function groupAdvisorChoices(
  choices: readonly DecisionAdvisorChoice[]
): Array<{ tool: ToolDefinition; models: ModelDefinition[] }> {
  const groups: Array<{ tool: ToolDefinition; models: ModelDefinition[] }> = [];
  for (const choice of choices) {
    let group = groups.find((entry) => entry.tool.id === choice.tool.id);
    if (!group) {
      group = { tool: choice.tool, models: [] };
      groups.push(group);
    }
    group.models.push(choice.model);
  }
  return groups;
}

function resolveModelSelectState(
  index: RegistryIndex | null,
  cell: Choice,
  slot: Slot
): {
  selectedValue: string;
  retainedUnavailable: { value: string; label: string } | null;
} {
  const emptySentinel = slot === 'default' ? UNASSIGNED_VALUE : INHERIT_VALUE;
  if (!cell.model_id && !cell.pending_family) {
    return { selectedValue: emptySentinel, retainedUnavailable: null };
  }

  if (index) {
    for (const group of index.groups) {
      for (const family of group.families) {
        const optionValue = family.encodedEfforts
          ? family.key
          : family.members[0]?.id ?? family.key;
        const isMatch = cell.pending_family
          ? cell.pending_family === family.key
          : Boolean(cell.model_id) &&
            family.members.some((member) => member.id === cell.model_id);
        if (isMatch) {
          return { selectedValue: optionValue, retainedUnavailable: null };
        }
      }
    }

    if (cell.pending_family) {
      const family = index.familiesByKey.get(cell.pending_family);
      return {
        selectedValue: cell.pending_family,
        retainedUnavailable: {
          value: cell.pending_family,
          label: `${family?.label || cell.pending_family} (unavailable)`,
        },
      };
    }

    const model = index.models.get(cell.model_id);
    const tool = model ? index.tools.get(model.tool_id) : null;
    const reason =
      tool && isApiToolAdapter(tool.adapter)
        ? 'decision advisor only'
        : tool && !tool.enabled
          ? `disabled — ${tool.label}`
          : 'unavailable';
    return {
      selectedValue: cell.model_id,
      retainedUnavailable: {
        value: cell.model_id,
        label: `${model?.label || cell.model_id} (${reason})`,
      },
    };
  }

  const fallbackVal = cell.pending_family || cell.model_id;
  return {
    selectedValue: fallbackVal,
    retainedUnavailable: {
      value: fallbackVal,
      label: `${fallbackVal} (unavailable)`,
    },
  };
}

interface EffortSelectConfig {
  selectedValue: string;
  disabled: boolean;
  items: Array<{ value: string; label: string; disabled?: boolean }>;
}

function resolveEffortSelectState(
  index: RegistryIndex | null,
  table: AssignmentTable,
  role: WorkflowRole,
  slot: Slot,
  busy: boolean
): EffortSelectConfig {
  const cell = table[role][slot];
  const defaultCell = table[role].default;

  // Inheriting difficulty override
  if (slot !== 'default' && !hasDifficultyOverride(cell)) {
    const defaultEffort = index
      ? effectiveCellEffort(index, defaultCell)
      : defaultCell.effort;
    const inheritLabel = defaultEffort
      ? `Use role default (${defaultEffort})`
      : 'Use role default';
    const items: Array<{ value: string; label: string; disabled?: boolean }> = [
      { value: INHERIT_VALUE, label: inheritLabel },
    ];

    if (index && (defaultCell.model_id || defaultCell.pending_family)) {
      const defaultFamily = resolveFamilyForCell(index, defaultCell);
      if (defaultFamily && defaultFamily.encodedEfforts) {
        for (const variant of defaultFamily.variants) {
          items.push({ value: variant.effort, label: variant.effort });
        }
      } else if (defaultCell.model_id) {
        const defaultModel = index.models.get(defaultCell.model_id);
        if (defaultModel) {
          for (const effort of defaultModel.efforts) {
            items.push({ value: effort, label: effort });
          }
        }
      }
    }

    return {
      selectedValue: INHERIT_VALUE,
      disabled: busy || items.length <= 1,
      items,
    };
  }

  if (!cell.model_id && !cell.pending_family) {
    return {
      selectedValue: UNSET_EFFORT_VALUE,
      disabled: true,
      items: [
        {
          value: UNSET_EFFORT_VALUE,
          label: 'Select a model first',
          disabled: true,
        },
      ],
    };
  }

  const family = index ? resolveFamilyForCell(index, cell) : undefined;
  if (family && family.encodedEfforts) {
    const derivedEffort = cell.pending_family
      ? null
      : selectedFamilyEffort(family, cell);
    const hasInvalidRawEffort =
      !cell.pending_family && Boolean(cell.model_id) && cell.effort !== null;
    const items: Array<{ value: string; label: string; disabled?: boolean }> =
      [];

    if (derivedEffort === null && !hasInvalidRawEffort) {
      items.push({
        value: UNSET_EFFORT_VALUE,
        label: 'Select an effort',
        disabled: true,
      });
    }

    for (const variant of family.variants) {
      items.push({ value: variant.effort, label: variant.effort });
    }

    if (
      hasInvalidRawEffort &&
      cell.effort !== null &&
      !items.some((item) => item.value === cell.effort)
    ) {
      items.push({
        value: cell.effort,
        label: `${cell.effort} (unsupported)`,
        disabled: true,
      });
    }

    const selectedValue =
      derivedEffort ??
      (hasInvalidRawEffort && cell.effort !== null
        ? cell.effort
        : UNSET_EFFORT_VALUE);

    return {
      selectedValue,
      disabled: busy,
      items,
    };
  }

  const model =
    cell.model_id && index ? index.models.get(cell.model_id) ?? null : null;
  if (!model) {
    const retainedValue = cell.effort ?? NO_EFFORT_VALUE;
    const retainedLabel = cell.effort
      ? `${cell.effort} (retained)`
      : 'No effort (retained)';
    return {
      selectedValue: retainedValue,
      disabled: true,
      items: [{ value: retainedValue, label: retainedLabel, disabled: true }],
    };
  }

  const items: Array<{ value: string; label: string; disabled?: boolean }> = [];
  let selectedValue = UNSET_EFFORT_VALUE;

  if (model.allow_no_effort) {
    items.push({ value: NO_EFFORT_VALUE, label: 'No effort' });
    if (cell.effort === null) {
      selectedValue = NO_EFFORT_VALUE;
    }
  } else if (cell.effort === null) {
    items.push({
      value: UNSET_EFFORT_VALUE,
      label: 'Select an effort',
      disabled: true,
    });
    selectedValue = UNSET_EFFORT_VALUE;
  }

  for (const effort of model.efforts) {
    items.push({ value: effort, label: effort });
    if (cell.effort === effort) {
      selectedValue = effort;
    }
  }

  if (
    cell.effort !== null &&
    !items.some((item) => item.value === cell.effort)
  ) {
    items.push({
      value: cell.effort,
      label: `${cell.effort} (unsupported)`,
      disabled: true,
    });
    selectedValue = cell.effort;
  }

  return {
    selectedValue,
    disabled: busy,
    items,
  };
}

export function ProfilesPanel(props: ProfilesPanelProps) {
  const {
    registry,
    loading,
    error: externalError,
    compact,
    onRegistryChanged,
    onOpenTools,
  } = props;

  const [state, setState] = useState<ProfilesPanelState>(() =>
    createProfilesPanelState(registry)
  );
  const stateRef = useRef<ProfilesPanelState>(state);
  stateRef.current = state;

  const mountedRef = useRef<boolean>(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    setState((prev) => {
      if (prev.snapshot === registry) {
        return prev;
      }
      return syncRegistrySnapshot(prev, registry);
    });
  }, [registry]);

  const snapshot = state.snapshot;
  const index = useMemo(
    () => (snapshot ? buildRegistryIndex(snapshot) : null),
    [snapshot]
  );

  const activeDraft = useMemo(() => getActiveDraft(state), [state]);
  const analysis = useMemo(
    () => (snapshot && activeDraft ? analyzeDraft(snapshot, activeDraft) : null),
    [snapshot, activeDraft]
  );

  const busy = loading || state.pending !== null;
  const hasAssignableModels = useMemo(
    () => index?.groups.some((group) => group.families.length > 0) ?? false,
    [index]
  );

  const selectedRole = state.selectedRole;
  const selectedRoleMeta = useMemo(
    () => getRoleMetadata(selectedRole),
    [selectedRole]
  );

  const selectedRoleOverrideCount = useMemo(
    () =>
      activeDraft
        ? countRoleDifficultyOverrides(activeDraft.assignments, selectedRole)
        : 0,
    [activeDraft, selectedRole]
  );

  const selectedProfileOverrideCount = useMemo(
    () =>
      activeDraft ? countProfileDifficultyOverrides(activeDraft.assignments) : 0,
    [activeDraft]
  );

  const runRegistryChange = async (
    change: RegistryChange,
    kind: 'save' | 'delete' | 'default',
    targetId: string | null,
    successText: string
  ) => {
    const current = stateRef.current;
    if (!current.snapshot || current.pending !== null) {
      return;
    }
    const expectedRevision = current.snapshot.revision;
    const started = beginAsyncOperation(current, kind, targetId);
    if (started.operationId === null) {
      return;
    }
    const opId = started.operationId;
    stateRef.current = started.state;
    setState(started.state);

    try {
      const updatedSnapshot = await desktop_registry_apply({
        expectedRevision,
        change,
      });
      if (!mountedRef.current) {
        return;
      }
      const outcome = resolveOperationSuccess(
        stateRef.current,
        opId,
        updatedSnapshot,
        successText
      );
      if (!outcome.accepted) {
        return;
      }
      stateRef.current = outcome.state;
      setState(outcome.state);
      onRegistryChanged(updatedSnapshot);
    } catch (err) {
      if (!mountedRef.current) {
        return;
      }
      const outcome = resolveOperationError(stateRef.current, opId, err);
      if (!outcome.accepted) {
        return;
      }
      stateRef.current = outcome.state;
      setState(outcome.state);
    }
  };

  const handleReloadLatest = async () => {
    const current = stateRef.current;
    if (current.pending !== null) {
      return;
    }
    const started = beginAsyncOperation(current, 'reload', null);
    if (started.operationId === null) {
      return;
    }
    const opId = started.operationId;
    stateRef.current = started.state;
    setState(started.state);

    try {
      const latestSnapshot = await desktop_registry_snapshot();
      if (!mountedRef.current) {
        return;
      }
      const outcome = resolveReloadSuccess(
        stateRef.current,
        opId,
        latestSnapshot
      );
      if (!outcome.accepted) {
        return;
      }
      stateRef.current = outcome.state;
      setState(outcome.state);
      onRegistryChanged(latestSnapshot);
    } catch (err) {
      if (!mountedRef.current) {
        return;
      }
      const outcome = resolveOperationError(stateRef.current, opId, err);
      if (!outcome.accepted) {
        return;
      }
      stateRef.current = outcome.state;
      setState(outcome.state);
    }
  };

  const handleSaveDraft = async () => {
    const current = stateRef.current;
    const draft = getActiveDraft(current);
    if (!current.snapshot || !draft || current.pending !== null) {
      return;
    }
    if (needsAcknowledgement(draft)) {
      return;
    }
    const currentAnalysis = analyzeDraft(current.snapshot, draft);
    if (currentAnalysis.errors.length > 0) {
      setState((prev) =>
        updateActiveDraft(prev, (d) => ({ ...d, nameTouched: true }))
      );
      return;
    }
    const profile = profileFromDraft(draft);
    await runRegistryChange(
      { op: 'upsert_profile', profile },
      'save',
      profile.id,
      `"${profile.name}" saved.`
    );
  };

  const handleSaveAndSwitch = async () => {
    const current = stateRef.current;
    const switchIntent = current.pendingSwitch;
    const draft = getActiveDraft(current);
    if (
      !current.snapshot ||
      !draft ||
      !switchIntent ||
      current.pending !== null
    ) {
      return;
    }
    if (needsAcknowledgement(draft)) {
      return;
    }
    const currentAnalysis = analyzeDraft(current.snapshot, draft);
    if (currentAnalysis.errors.length > 0) {
      setState((prev) =>
        updateActiveDraft(prev, (d) => ({ ...d, nameTouched: true }))
      );
      return;
    }
    const profile = profileFromDraft(draft);
    await runRegistryChange(
      { op: 'upsert_profile', profile },
      'save',
      profile.id,
      `"${profile.name}" saved.`
    );
    if (!mountedRef.current) {
      return;
    }
    if (!stateRef.current.error && stateRef.current.pendingSwitch) {
      const nextState = keepDraftAndSwitch(stateRef.current, compact);
      stateRef.current = nextState;
      setState(nextState);
    }
  };

  const handleDeleteProfile = async (profile: WorkflowProfile) => {
    await runRegistryChange(
      { op: 'delete_profile', profile_id: profile.id },
      'delete',
      profile.id,
      `"${profile.name}" deleted.`
    );
  };

  const handleSetDefault = async (profile: WorkflowProfile) => {
    await runRegistryChange(
      { op: 'set_default_profile', profile_id: profile.id },
      'default',
      profile.id,
      `Default set to "${profile.name}".`
    );
  };

  const handleClearDefault = async () => {
    await runRegistryChange(
      { op: 'set_default_profile', profile_id: null },
      'default',
      null,
      'Default profile cleared.'
    );
  };

  const handleSelectProfile = (profileId: string) => {
    setState((prev) => selectProfile(prev, profileId, compact));
  };

  const handleWorkflowRoleChange = (rawRole: string) => {
    if (!isExecutionRole(rawRole)) {
      return;
    }
    setState((prev) => selectWorkflowRole(prev, rawRole));
  };

  const handleSlotModelChange = (
    role: WorkflowRole,
    slot: Slot,
    rawValue: string
  ) => {
    if (!snapshot) {
      return;
    }
    const normalizedValue =
      rawValue === UNASSIGNED_VALUE || rawValue === INHERIT_VALUE
        ? ''
        : rawValue;
    setState((prev) =>
      updateActiveDraft(prev, (draft) => ({
        ...draft,
        assignments: selectRoleModel(
          snapshot,
          draft.assignments,
          role,
          slot,
          normalizedValue
        ),
      }))
    );
  };

  const handleSlotEffortChange = (
    role: WorkflowRole,
    slot: Slot,
    rawValue: string
  ) => {
    if (!snapshot || rawValue === INHERIT_VALUE || rawValue === UNSET_EFFORT_VALUE) {
      return;
    }
    const normalizedEffort = rawValue === NO_EFFORT_VALUE ? null : rawValue;
    setState((prev) =>
      updateActiveDraft(prev, (draft) => ({
        ...draft,
        assignments: selectRoleEffort(
          snapshot,
          draft.assignments,
          role,
          slot,
          normalizedEffort
        ),
      }))
    );
  };

  const handleClearRoleOverrides = () => {
    setState((prev) =>
      updateActiveDraft(prev, (draft) =>
        clearDraftRoleOverrides(draft, selectedRole)
      )
    );
  };

  const handleClearProfileOverrides = () => {
    setState((prev) =>
      updateActiveDraft(prev, (draft) => clearDraftProfileOverrides(draft))
    );
  };

  const handleDecisionAdvisorChange = (rawValue: string) => {
    const nextModelId =
      rawValue === UNASSIGNED_VALUE ? '' : rawValue.trim();
    setState((prev) =>
      updateActiveDraft(prev, (draft) => ({
        ...draft,
        decisionAdvisor: nextModelId
          ? { model_id: nextModelId, mode: 'shadow' }
          : null,
      }))
    );
  };

  const renderModelSetupCallout = (inEditor: boolean) => {
    if (!snapshot) {
      return null;
    }
    const enabledTools = snapshot.tools.filter(
      (tool) => tool.enabled && !isApiToolAdapter(tool.adapter)
    );
    const enabledToolIds = new Set(enabledTools.map((tool) => tool.id));
    const enabledModels = snapshot.models.filter((model) =>
      enabledToolIds.has(model.tool_id)
    );
    if (enabledModels.length > 0) {
      return null;
    }

    let title = 'No models chosen for profiles yet';
    let detail =
      'Choose models for your enabled tools in Tools so they appear in profile role selectors.';
    if (snapshot.tools.length === 0) {
      title = 'No tools or models added yet';
      detail =
        'Add a CLI tool and choose its models in Tools to assign workflow roles here.';
    } else if (enabledTools.length === 0) {
      title = 'All registered workflow tools are disabled';
      detail =
        'Enable a workflow tool in Tools or choose models for an enabled tool.';
    } else if (snapshot.models.length > 0) {
      title = 'No models available from enabled tools';
      detail =
        'Saved workflow models currently belong to disabled tools. Enable those tools or choose models in Tools.';
    }

    const extra = inEditor
      ? ' Your current profile draft stays intact while you choose models, or you can save a named partial profile now.'
      : '';

    return (
      <div className="tl-profiles-callout" role="status">
        <div className="tl-profiles-callout-copy">
          <Text size="2" weight="bold">
            {title}
          </Text>
          <Text size="1" color="gray">
            {detail}
            {extra}
          </Text>
        </div>
        <div className="tl-profiles-callout-actions">
          <Button
            size="2"
            variant="solid"
            color="gray"
            highContrast
            className="tl-profiles-primary-btn"
            disabled={busy}
            onClick={onOpenTools}
          >
            Choose models
          </Button>
        </div>
      </div>
    );
  };

  const renderProfileListSection = () => {
    if (!snapshot) {
      return null;
    }
    const defaultProfile = snapshot.default_profile_id
      ? snapshot.profiles.find((p) => p.id === snapshot.default_profile_id) ??
        null
      : null;

    return (
      <section aria-label="Profile selection">
        <div className="tl-profiles-section-bar" style={{ marginBottom: 8 }}>
          <div>
            <Text size="2" weight="bold" as="div">
              Profiles
            </Text>
          </div>

          <div className="tl-profiles-toolbar">
            <Button
              size="2"
              variant="solid"
              color="gray"
              highContrast
              className="tl-profiles-primary-btn"
              disabled={busy}
              onClick={() =>
                setState((prev) => beginCreateProfile(prev, compact))
              }
            >
              <Plus size={15} aria-hidden="true" />
              New profile
            </Button>

            {defaultProfile ? (
              <Button
                size="2"
                variant="soft"
                color="gray"
                className="tl-profiles-touch-btn"
                disabled={busy}
                onClick={() => void handleClearDefault()}
              >
                {state.pending?.kind === 'default' && !state.pending.targetId
                  ? 'Clearing...'
                  : 'Clear default'}
              </Button>
            ) : null}

            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              onClick={() => void handleReloadLatest()}
            >
              <RefreshCw size={14} aria-hidden="true" />
              {state.pending?.kind === 'reload' ? 'Reloading...' : 'Reload'}
            </Button>

            {!compact && activeDraft ? (
              <Button
                size="2"
                variant="soft"
                color="gray"
                className="tl-profiles-touch-btn"
                disabled={busy || selectedProfileOverrideCount === 0}
                onClick={handleClearProfileOverrides}
              >
                <RotateCcw size={14} aria-hidden="true" />
                Clear all overrides
              </Button>
            ) : null}
          </div>
        </div>

        {snapshot.profiles.length === 0 && !state.createDraft ? (
          <div className="tl-profiles-empty-box">
            <Text size="2" weight="bold">
              No profiles saved yet
            </Text>
            <Text size="1" color="gray">
              Create a profile to assign models and reasoning efforts across the
              five execution roles.
            </Text>
          </div>
        ) : (
          <div
            className="tl-profiles-list-table"
            role="group"
            aria-label="Profiles"
          >
            {state.createDraft ? (
              <button
                type="button"
                aria-pressed={state.selectedProfileId === state.createDraft.id}
                className="tl-profiles-role-row-btn"
                disabled={busy}
                onClick={() => handleSelectProfile(state.createDraft!.id)}
              >
                <div style={{ minWidth: 0, flex: '1 1 200px' }}>
                  <Flex align="center" gap="2" wrap="wrap">
                    <Text size="2" weight="bold">
                      {state.createDraft.name.trim() ||
                        (state.createDraft.cloneOfName
                          ? copyProfileName(state.createDraft.cloneOfName)
                          : 'New profile')}
                    </Text>
                    <span
                      className="tl-profiles-pill"
                      data-tone="attention"
                    >
                      Unsaved new profile
                    </span>
                  </Flex>
                  <Text size="1" color="gray" as="div" style={{ marginTop: 2 }}>
                    {state.createDraft.cloneOfName
                      ? `Cloned from "${state.createDraft.cloneOfName}"`
                      : 'Unsaved draft'}
                  </Text>
                </div>

                <Flex align="center" gap="2" wrap="wrap">
                  <ChevronRight size={16} aria-hidden="true" />
                </Flex>
              </button>
            ) : null}

            {snapshot.profiles.map((profile) => {
              const trackedDraft = state.draftsById[profile.id];
              const dirty = trackedDraft ? isDraftDirty(trackedDraft) : false;
              const summary = summarizeProfile(snapshot, profile);
              const table = trackedDraft
                ? trackedDraft.assignments
                : tableFromProfile(profile);
              const workerDefault = formatCellChoiceLabel(
                index,
                table.coder.default
              );
              const drifted = Boolean(
                trackedDraft &&
                  trackedDraft.sourceSignature !== null &&
                  profileSignature(profile) !== trackedDraft.sourceSignature
              );
              const isSelected = state.selectedProfileId === profile.id;
              const isDefault = snapshot.default_profile_id === profile.id;
              const hasIssues =
                summary.issues.length > 0 ||
                summary.advisor.state === 'invalid';
              const displayName =
                (trackedDraft?.name.trim() || profile.name) || 'Unnamed profile';

              const subtitleParts: string[] = [
                `${workflowRoleLabel('coder')}: ${workerDefault}`,
              ];
              if (summary.missingDefaults.length > 0) {
                subtitleParts.push(
                  `Missing defaults: ${summary.missingDefaults
                    .map((role) => workflowRoleLabel(role))
                    .join(', ')}`
                );
              } else {
                subtitleParts.push('All 5 role defaults set');
              }
              if (summary.advisor.state !== 'off') {
                subtitleParts.push(
                  `Advisor: ${formatAdvisorSummary(summary.advisor)}`
                );
              }

              return (
                <button
                  key={profile.id}
                  type="button"
                  aria-pressed={isSelected}
                  className="tl-profiles-role-row-btn"
                  disabled={busy}
                  onClick={() => handleSelectProfile(profile.id)}
                >
                  <div style={{ minWidth: 0, flex: '1 1 200px' }}>
                    <Flex align="center" gap="2" wrap="wrap">
                      <Text size="2" weight="bold">
                        {displayName}
                      </Text>
                      {isDefault ? (
                        <Badge color="gray" variant="solid" highContrast>
                          Default
                        </Badge>
                      ) : null}
                      {dirty ? (
                        <span
                          className="tl-profiles-pill"
                          data-tone="attention"
                        >
                          Unsaved
                        </span>
                      ) : null}
                      {drifted ? (
                        <span
                          className="tl-profiles-pill"
                          data-tone="attention"
                        >
                          Updated externally
                        </span>
                      ) : null}
                      {hasIssues ? (
                        <span className="tl-profiles-pill" data-tone="danger">
                          Needs attention
                        </span>
                      ) : null}
                    </Flex>
                    <Text
                      size="1"
                      color="gray"
                      as="div"
                      style={{ marginTop: 2 }}
                    >
                      {subtitleParts.join(' · ')}
                    </Text>
                  </div>

                  <Flex align="center" gap="2" wrap="wrap">
                    <span className="tl-profiles-pill">
                      {summary.defaultCount}/{PROFILE_EXECUTION_ROLES.length}{' '}
                      role defaults · {summary.overrideCount}{' '}
                      {summary.overrideCount === 1 ? 'override' : 'overrides'}
                    </span>
                    <ChevronRight size={16} aria-hidden="true" />
                  </Flex>
                </button>
              );
            })}
          </div>
        )}
      </section>
    );
  };

  const renderRoleSummariesAndEditor = () => {
    if (!snapshot || !activeDraft || !analysis) {
      return null;
    }

    const savedSourceProfile =
      activeDraft.mode === 'edit' && activeDraft.sourceId
        ? snapshot.profiles.find((p) => p.id === activeDraft.sourceId) ?? null
        : null;
    const isDefault =
      savedSourceProfile !== null &&
      snapshot.default_profile_id === savedSourceProfile.id;
    const isConfirmingDelete =
      savedSourceProfile !== null &&
      state.confirmDeleteId === savedSourceProfile.id;
    const draftDirty = isDraftDirty(activeDraft);

    const suppressInitialBlankNameError =
      activeDraft.mode === 'create' &&
      !activeDraft.nameTouched &&
      activeDraft.name.trim() === '';
    const visibleErrors = suppressInitialBlankNameError
      ? analysis.errors.filter(
          (message) => message !== 'Profile name is required.'
        )
      : analysis.errors;

    const selectedRoleAdapter = resolveRoleToolAdapter(
      index,
      activeDraft.assignments,
      selectedRole
    );
    const defaultCell = activeDraft.assignments[selectedRole].default;
    const defaultModelSelect = resolveModelSelectState(
      index,
      defaultCell,
      'default'
    );
    const defaultEffortSelect = resolveEffortSelectState(
      index,
      activeDraft.assignments,
      selectedRole,
      'default',
      busy
    );

    const advisorValidation = analysis.advisor;
    const advisorChoices = index?.advisorChoices ?? [];
    const advisorGroups = groupAdvisorChoices(advisorChoices);
    const currentAdvisorId = activeDraft.decisionAdvisor?.model_id ?? '';
    const isAdvisorInEligibleList = advisorChoices.some(
      (choice) => choice.model.id === currentAdvisorId
    );
    const advisorSelectValue = currentAdvisorId
      ? currentAdvisorId
      : UNASSIGNED_VALUE;

    return (
      <>
        {compact ? (
          <div className="tl-profiles-section-bar">
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-primary-btn"
              disabled={busy}
              onClick={() => setState((prev) => setCompactStep(prev, 'list'))}
            >
              <ArrowLeft size={16} aria-hidden="true" />
              Back to profiles
            </Button>
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy || selectedProfileOverrideCount === 0}
              onClick={handleClearProfileOverrides}
            >
              <RotateCcw size={14} aria-hidden="true" />
              Clear all overrides
            </Button>
          </div>
        ) : null}

        {/* Active profile management & name bar */}
        <section
          className="tl-profiles-editor-box"
          aria-label="Selected profile settings"
        >
          <div className="tl-profiles-section-bar">
            <Flex align="center" gap="2" wrap="wrap">
              <Text size="2" weight="bold">
                {activeDraft.name.trim() ||
                  (activeDraft.mode === 'create'
                    ? 'New profile'
                    : 'Unnamed profile')}
              </Text>
              {isDefault ? (
                <Badge color="gray" variant="solid" highContrast>
                  Default
                </Badge>
              ) : null}
              {draftDirty ? (
                <span className="tl-profiles-pill" data-tone="attention">
                  Unsaved changes
                </span>
              ) : null}
              <Text size="1" color="gray">
                {analysis.defaultCount}/{PROFILE_EXECUTION_ROLES.length} role
                defaults · {analysis.overrideCount}{' '}
                {analysis.overrideCount === 1 ? 'override' : 'overrides'}
              </Text>
            </Flex>

            {savedSourceProfile ? (
              <div className="tl-profiles-toolbar">
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  className="tl-profiles-touch-btn"
                  disabled={busy}
                  aria-label={`Clone ${savedSourceProfile.name}`}
                  onClick={() =>
                    setState((prev) =>
                      beginCloneProfile(prev, savedSourceProfile.id, compact)
                    )
                  }
                >
                  <Copy size={14} aria-hidden="true" />
                  Clone
                </Button>

                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  className="tl-profiles-touch-btn"
                  disabled={busy || isDefault}
                  aria-label={`Set ${savedSourceProfile.name} as default`}
                  onClick={() => void handleSetDefault(savedSourceProfile)}
                >
                  {state.pending?.kind === 'default' &&
                  state.pending.targetId === savedSourceProfile.id
                    ? 'Setting default...'
                    : 'Set default'}
                </Button>

                <Button
                  size="2"
                  variant="soft"
                  color="red"
                  className="tl-profiles-touch-btn"
                  disabled={busy}
                  aria-label={`Delete ${savedSourceProfile.name}`}
                  onClick={() =>
                    setState((prev) =>
                      requestDeleteProfile(prev, savedSourceProfile.id)
                    )
                  }
                >
                  <Trash2 size={14} aria-hidden="true" />
                  Delete
                </Button>
              </div>
            ) : null}
          </div>

          {isConfirmingDelete && savedSourceProfile ? (
            <div
              className="tl-profiles-callout"
              data-tone="danger"
              role="alertdialog"
              aria-label={`Confirm deletion of ${savedSourceProfile.name}`}
            >
              <div className="tl-profiles-callout-copy">
                <Text size="2" weight="bold">
                  Delete &ldquo;{savedSourceProfile.name}&rdquo; permanently?
                </Text>
                <Text size="1" color="gray">
                  Cannot be undone. Active tasks keep their current models.
                  {isDefault
                    ? ' This profile is currently the default.'
                    : ''}
                </Text>
              </div>
              <div className="tl-profiles-callout-actions">
                <Button
                  size="2"
                  variant="solid"
                  color="red"
                  className="tl-profiles-primary-btn"
                  disabled={busy}
                  onClick={() => void handleDeleteProfile(savedSourceProfile)}
                >
                  {state.pending?.kind === 'delete' &&
                  state.pending.targetId === savedSourceProfile.id
                    ? 'Deleting...'
                    : 'Delete profile'}
                </Button>
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  className="tl-profiles-touch-btn"
                  disabled={busy}
                  onClick={() => setState((prev) => cancelDeleteProfile(prev))}
                >
                  Cancel
                </Button>
              </div>
            </div>
          ) : null}

          {activeDraft.externalChanged ? (
            <div
              className="tl-profiles-callout"
              data-tone="warn"
              role="alert"
            >
              <div className="tl-profiles-callout-copy">
                <Text size="2" weight="bold">
                  {activeDraft.externalDeleted
                    ? 'This profile was removed from the latest registry'
                    : activeDraft.overwriteAcknowledged
                      ? 'Your draft will replace the newer saved version'
                      : 'This profile changed after your draft started'}
                </Text>
                <Text size="1" color="gray">
                  {activeDraft.externalDeleted
                    ? 'Your draft is preserved. Discard the draft, or keep it and recreate the profile with a new identifier.'
                    : 'Your draft is preserved. Load the latest saved version, or keep your draft and save explicitly.'}
                </Text>
              </div>
              <div className="tl-profiles-callout-actions">
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  className="tl-profiles-touch-btn"
                  disabled={busy}
                  onClick={() =>
                    setState((prev) =>
                      activeDraft.externalDeleted
                        ? discardActiveDraft(prev)
                        : loadLatestIntoActiveDraft(prev)
                    )
                  }
                >
                  {activeDraft.externalDeleted
                    ? 'Discard draft and close'
                    : 'Load latest into editor'}
                </Button>
                {!activeDraft.overwriteAcknowledged ? (
                  <Button
                    size="2"
                    variant="solid"
                    color="gray"
                    highContrast
                    className="tl-profiles-primary-btn"
                    disabled={busy}
                    onClick={() =>
                      setState((prev) => acknowledgeActiveDraftOverwrite(prev))
                    }
                  >
                    {activeDraft.externalDeleted
                      ? 'Keep draft and recreate'
                      : 'Keep my draft'}
                  </Button>
                ) : null}
              </div>
            </div>
          ) : null}

          <div>
            <label
              className="tl-profiles-field-label"
              htmlFor="tl-profile-name-input"
            >
              Profile name
            </label>
            <input
              id="tl-profile-name-input"
              type="text"
              className="tl-profiles-text-input"
              maxLength={MAX_PROFILE_NAME_LENGTH}
              autoComplete="off"
              spellCheck={false}
              disabled={busy}
              value={activeDraft.name}
              onChange={(e) => {
                const nextName = e.target.value;
                setState((prev) =>
                  updateActiveDraft(prev, (draft) => ({
                    ...draft,
                    name: nextName,
                    nameTouched: true,
                  }))
                );
              }}
            />
          </div>
        </section>

        {renderModelSetupCallout(true)}

        {/* Role Selector: 5 compact summary rows on desktop, 1 Workflow role Select on compact */}
        {!compact ? (
          <div
            className="tl-profiles-list-table"
            role="group"
            aria-label="Execution roles"
          >
            {PROFILE_EXECUTION_ROLES.map((roleId) => {
              const meta = getRoleMetadata(roleId);
              const roleAdapter = resolveRoleToolAdapter(
                index,
                activeDraft.assignments,
                roleId
              );
              const roleOverrides = countRoleDifficultyOverrides(
                activeDraft.assignments,
                roleId
              );
              const roleIssues = analysis.issues.filter(
                (issue) => issue.role === roleId
              ).length;
              const isSelected = roleId === selectedRole;
              const defaultLabel = formatCellChoiceLabel(
                index,
                activeDraft.assignments[roleId].default
              );

              return (
                <button
                  key={roleId}
                  type="button"
                  aria-pressed={isSelected}
                  className="tl-profiles-role-row-btn"
                  onClick={() =>
                    setState((prev) => selectWorkflowRole(prev, roleId))
                  }
                >
                  <Flex
                    align="center"
                    gap="3"
                    style={{ minWidth: 0, flex: '1 1 180px' }}
                  >
                    <ToolBrand adapter={roleAdapter} size={18} />
                    <div style={{ minWidth: 0 }}>
                      <Text size="2" weight="bold" as="div">
                        {meta.label}
                      </Text>
                      <Text size="1" color="gray" as="div">
                        {meta.summary}
                      </Text>
                    </div>
                  </Flex>

                  <Flex align="center" gap="2" wrap="wrap">
                    <span className="tl-profiles-pill">{defaultLabel}</span>
                    {roleIssues > 0 ? (
                      <span className="tl-profiles-pill" data-tone="danger">
                        {roleIssues} {roleIssues === 1 ? 'issue' : 'issues'}
                      </span>
                    ) : (
                      <span
                        className="tl-profiles-pill"
                        data-tone={roleOverrides > 0 ? 'working' : undefined}
                      >
                        {roleOverrides > 0
                          ? `${roleOverrides} ${
                              roleOverrides === 1 ? 'override' : 'overrides'
                            }`
                          : 'Default only'}
                      </span>
                    )}
                    <ChevronRight size={16} aria-hidden="true" />
                  </Flex>
                </button>
              );
            })}
          </div>
        ) : (
          <div className="tl-profiles-role-select-field">
            <span className="tl-profiles-field-label">Workflow role</span>
            <Select.Root
              value={selectedRole}
              disabled={busy}
              onValueChange={handleWorkflowRoleChange}
            >
              <Select.Trigger aria-label="Workflow role" />
              <Select.Content>
                {ROLE_METADATA.map((meta) => (
                  <Select.Item key={meta.id} value={meta.id}>
                    {meta.label}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
          </div>
        )}

        {/* Single Selected Role Editor */}
        <section
          className="tl-profiles-editor-box"
          aria-label={`Selected role editor: ${selectedRoleMeta.label}`}
        >
          <div className="tl-profiles-section-bar">
            <Flex align="center" gap="2" wrap="wrap">
              <ToolBrand adapter={selectedRoleAdapter} size={18} />
              <Text size="2" weight="bold">
                {selectedRoleMeta.label} defaults
              </Text>
            </Flex>
            <Text size="1" color="gray">
              {selectedRoleMeta.summary}
            </Text>
          </div>

          <Separator size="4" />

          <div className="tl-profiles-default-grid">
            <Box style={{ minWidth: 0 }}>
              <span className="tl-profiles-field-label">Default model</span>
              <Select.Root
                value={defaultModelSelect.selectedValue}
                disabled={busy}
                onValueChange={(value) =>
                  handleSlotModelChange(selectedRole, 'default', value)
                }
              >
                <Select.Trigger aria-label="Default model" />
                <Select.Content>
                  <Select.Item value={UNASSIGNED_VALUE}>
                    {hasAssignableModels
                      ? 'Not assigned'
                      : 'No enabled-tool models available'}
                  </Select.Item>
                  {index?.groups.map((group) =>
                    group.families.length > 0 ? (
                      <Select.Group key={group.tool.id}>
                        <Select.Label>{group.tool.label}</Select.Label>
                        {group.families.map((family) => {
                          const optionValue = family.encodedEfforts
                            ? family.key
                            : family.members[0]?.id ?? family.key;
                          return (
                            <Select.Item key={optionValue} value={optionValue}>
                              {family.label}
                            </Select.Item>
                          );
                        })}
                      </Select.Group>
                    ) : null
                  )}
                  {defaultModelSelect.retainedUnavailable ? (
                    <Select.Item
                      value={defaultModelSelect.retainedUnavailable.value}
                      disabled
                    >
                      {defaultModelSelect.retainedUnavailable.label}
                    </Select.Item>
                  ) : null}
                </Select.Content>
              </Select.Root>
            </Box>

            <Box style={{ minWidth: 0 }}>
              <span className="tl-profiles-field-label">Reasoning effort</span>
              <Select.Root
                value={defaultEffortSelect.selectedValue}
                disabled={defaultEffortSelect.disabled}
                onValueChange={(value) =>
                  handleSlotEffortChange(selectedRole, 'default', value)
                }
              >
                <Select.Trigger aria-label="Reasoning effort" />
                <Select.Content>
                  {defaultEffortSelect.items.map((item) => (
                    <Select.Item
                      key={item.value}
                      value={item.value}
                      disabled={item.disabled}
                    >
                      {item.label}
                    </Select.Item>
                  ))}
                </Select.Content>
              </Select.Root>
            </Box>
          </div>

          <Separator size="4" />

          <div className="tl-profiles-section-bar">
            <div>
              <Flex align="center" gap="2" wrap="wrap">
                <Text size="2" weight="bold">
                  Difficulty overrides
                </Text>
                <span
                  className="tl-profiles-pill"
                  data-tone={selectedRoleOverrideCount > 0 ? 'working' : undefined}
                >
                  {selectedRoleOverrideCount}{' '}
                  {selectedRoleOverrideCount === 1 ? 'override' : 'overrides'}
                </span>
              </Flex>
              <Text size="1" color="gray" as="div" style={{ marginTop: 2 }}>
                Levels without an override use the role default.
              </Text>
            </div>

            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy || selectedRoleOverrideCount === 0}
              aria-label={`Clear difficulty overrides for ${selectedRoleMeta.label}`}
              onClick={handleClearRoleOverrides}
            >
              <RotateCcw size={14} aria-hidden="true" />
              Clear difficulty overrides
            </Button>
          </div>

          <div className="tl-profiles-override-list">
            {DIFFICULTIES.map((difficulty: Difficulty) => {
              const diffLabel = DIFFICULTY_LABELS[difficulty];
              const overrideCell =
                activeDraft.assignments[selectedRole][difficulty];
              const isOverridden = hasDifficultyOverride(overrideCell);
              const overrideModelSelect = resolveModelSelectState(
                index,
                overrideCell,
                difficulty
              );
              const overrideEffortSelect = resolveEffortSelectState(
                index,
                activeDraft.assignments,
                selectedRole,
                difficulty,
                busy
              );
              const defaultSummary = formatCellChoiceLabel(index, defaultCell);
              const overrideSummary = formatCellChoiceLabel(
                index,
                overrideCell
              );
              const editorId = `tl-difficulty-editor-${difficulty}`;
              const isOpen = state.openDifficulty === difficulty;

              const overrideControls = (
                <div className="tl-profiles-override-controls">
                  <Box style={{ minWidth: 0 }}>
                    <span className="tl-profiles-field-label">
                      {diffLabel} model
                    </span>
                    <Select.Root
                      value={overrideModelSelect.selectedValue}
                      disabled={busy}
                      onValueChange={(value) =>
                        handleSlotModelChange(selectedRole, difficulty, value)
                      }
                    >
                      <Select.Trigger
                        aria-label={`${diffLabel} override model`}
                      />
                      <Select.Content>
                        <Select.Item value={INHERIT_VALUE}>
                          Use role default
                        </Select.Item>
                        {index?.groups.map((group) =>
                          group.families.length > 0 ? (
                            <Select.Group key={group.tool.id}>
                              <Select.Label>{group.tool.label}</Select.Label>
                              {group.families.map((family) => {
                                const optionValue = family.encodedEfforts
                                  ? family.key
                                  : family.members[0]?.id ?? family.key;
                                return (
                                  <Select.Item
                                    key={optionValue}
                                    value={optionValue}
                                  >
                                    {family.label}
                                  </Select.Item>
                                );
                              })}
                            </Select.Group>
                          ) : null
                        )}
                        {overrideModelSelect.retainedUnavailable ? (
                          <Select.Item
                            value={
                              overrideModelSelect.retainedUnavailable.value
                            }
                            disabled
                          >
                            {overrideModelSelect.retainedUnavailable.label}
                          </Select.Item>
                        ) : null}
                      </Select.Content>
                    </Select.Root>
                  </Box>

                  <Box style={{ minWidth: 0 }}>
                    <span className="tl-profiles-field-label">
                      {diffLabel} effort
                    </span>
                    <Select.Root
                      value={overrideEffortSelect.selectedValue}
                      disabled={overrideEffortSelect.disabled}
                      onValueChange={(value) =>
                        handleSlotEffortChange(selectedRole, difficulty, value)
                      }
                    >
                      <Select.Trigger
                        aria-label={`${diffLabel} override effort`}
                      />
                      <Select.Content>
                        {overrideEffortSelect.items.map((item) => (
                          <Select.Item
                            key={item.value}
                            value={item.value}
                            disabled={item.disabled}
                          >
                            {item.label}
                          </Select.Item>
                        ))}
                      </Select.Content>
                    </Select.Root>
                  </Box>
                </div>
              );

              if (compact) {
                return (
                  <div
                    key={difficulty}
                    className="tl-profiles-override-card tl-profiles-override-accordion"
                    data-overridden={String(isOverridden)}
                  >
                    <button
                      type="button"
                      className="tl-profiles-override-accordion-trigger"
                      aria-label={`${diffLabel} difficulty`}
                      aria-expanded={isOpen}
                      aria-controls={editorId}
                      onClick={() =>
                        setState((prev) =>
                          toggleDifficultyAccordion(prev, difficulty)
                        )
                      }
                    >
                      <span className="tl-profiles-override-accordion-top">
                        <Flex align="center" gap="2" wrap="wrap">
                          <Text size="2" weight="bold">
                            {diffLabel}
                          </Text>
                          <span
                            className="tl-profiles-pill"
                            data-tone={isOverridden ? 'working' : undefined}
                          >
                            {isOverridden
                              ? 'Override active'
                              : 'Use role default'}
                          </span>
                        </Flex>
                        {isOpen ? (
                          <ChevronDown size={16} aria-hidden="true" />
                        ) : (
                          <ChevronRight size={16} aria-hidden="true" />
                        )}
                      </span>
                      <Text
                        size="1"
                        color="gray"
                        className="tl-profiles-override-summary"
                      >
                        {isOverridden
                          ? overrideSummary
                          : `Use role default · inherits ${defaultSummary}`}
                      </Text>
                    </button>
                    <div id={editorId} hidden={!isOpen}>
                      {isOpen ? (
                        <div className="tl-profiles-override-accordion-panel">
                          <Text size="1" color="gray" as="p" style={{ margin: 0 }}>
                            {isOverridden
                              ? `Effective: ${overrideSummary} (default: ${defaultSummary})`
                              : `Inherits role default: ${defaultSummary}`}
                          </Text>
                          {overrideControls}
                        </div>
                      ) : null}
                    </div>
                  </div>
                );
              }

              return (
                <div
                  key={difficulty}
                  className="tl-profiles-override-card"
                  data-overridden={String(isOverridden)}
                >
                  <div className="tl-profiles-override-head">
                    <Flex align="center" gap="2" wrap="wrap">
                      <Text size="2" weight="bold">
                        {diffLabel}
                      </Text>
                      <span
                        className="tl-profiles-pill"
                        data-tone={isOverridden ? 'working' : undefined}
                      >
                        {isOverridden ? 'Override active' : 'Role default'}
                      </span>
                    </Flex>

                    <Text size="1" color="gray">
                      {isOverridden
                        ? `Effective: ${overrideSummary} (default: ${defaultSummary})`
                        : `Inherits role default: ${defaultSummary}`}
                    </Text>
                  </div>

                  {overrideControls}
                </div>
              );
            })}
          </div>
        </section>

        {/* Optional Shadow Decision Advisor */}
        <section
          className="tl-profiles-editor-box"
          aria-label="Decision advisor"
        >
          <div className="tl-profiles-section-bar">
            <div>
              <Flex align="center" gap="2" wrap="wrap">
                <Text size="2" weight="bold">
                  Decision advisor
                </Text>
                <span className="tl-profiles-pill">Optional · Shadow mode</span>
              </Flex>
              <Text size="1" color="gray" as="div" style={{ marginTop: 2 }}>
                Suggests task difficulty without changing plans or active tasks.
              </Text>
            </div>

            {activeDraft.decisionAdvisor ? (
              <Button
                size="2"
                variant="soft"
                color="gray"
                className="tl-profiles-touch-btn"
                disabled={busy}
                aria-label="Clear the Decision advisor"
                onClick={() => handleDecisionAdvisorChange(UNASSIGNED_VALUE)}
              >
                Clear advisor
              </Button>
            ) : null}
          </div>

          <Box style={{ minWidth: 0 }}>
            <span className="tl-profiles-field-label">API model</span>
            <Select.Root
              value={advisorSelectValue}
              disabled={busy}
              onValueChange={handleDecisionAdvisorChange}
            >
              <Select.Trigger aria-label="Decision advisor API model" />
              <Select.Content>
                <Select.Item value={UNASSIGNED_VALUE}>
                  Off — no advisor
                </Select.Item>
                {advisorGroups.map((group) => (
                  <Select.Group key={group.tool.id}>
                    <Select.Label>{group.tool.label}</Select.Label>
                    {group.models.map((model) => (
                      <Select.Item key={model.id} value={model.id}>
                        {model.label || model.id}
                      </Select.Item>
                    ))}
                  </Select.Group>
                ))}
                {currentAdvisorId && !isAdvisorInEligibleList ? (
                  <Select.Item value={currentAdvisorId} disabled>
                    {`${
                      advisorValidation.model?.label ||
                      index?.models.get(currentAdvisorId)?.label ||
                      currentAdvisorId
                    } (unavailable)`}
                  </Select.Item>
                ) : null}
              </Select.Content>
            </Select.Root>
          </Box>

          {advisorValidation.state === 'invalid' ? (
            <div
              className="tl-profiles-callout"
              data-tone="danger"
              role="alert"
            >
              <div className="tl-profiles-callout-copy">
                <Text size="2" weight="bold">
                  Advisor needs repair
                </Text>
                <Text size="1" color="gray">
                  {advisorValidation.issue.message}
                </Text>
              </div>
              <div className="tl-profiles-callout-actions">
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  className="tl-profiles-touch-btn"
                  disabled={busy}
                  onClick={onOpenTools}
                >
                  Choose in Tools
                </Button>
              </div>
            </div>
          ) : null}

          {advisorChoices.length === 0 &&
          advisorValidation.state !== 'invalid' ? (
            <Flex align="center" justify="between" gap="2" wrap="wrap">
              <Text size="1" color="gray">
                No eligible decision API model is enabled in Tools.
              </Text>
              <Button
                size="2"
                variant="soft"
                color="gray"
                className="tl-profiles-touch-btn"
                disabled={busy}
                onClick={onOpenTools}
              >
                Choose in Tools
              </Button>
            </Flex>
          ) : null}
        </section>

        {/* Validation issues & explicit Save / Cancel bar */}
        {visibleErrors.length > 0 ? (
          <div className="tl-profiles-callout" data-tone="danger" role="alert">
            <div className="tl-profiles-callout-copy">
              <Text size="2" weight="bold">
                Fix before saving
              </Text>
              <ul className="tl-profiles-error-list">
                {visibleErrors.map((errText) => (
                  <li key={errText}>{errText}</li>
                ))}
              </ul>
            </div>
          </div>
        ) : null}

        <div className="tl-profiles-section-bar">
          <div className="tl-profiles-toolbar">
            <Button
              size="2"
              variant="solid"
              color="gray"
              highContrast
              className="tl-profiles-primary-btn"
              disabled={
                busy ||
                !isDraftDirty(activeDraft) ||
                state.conflict ||
                analysis.errors.length > 0 ||
                needsAcknowledgement(activeDraft)
              }
              onClick={() => void handleSaveDraft()}
            >
              {state.pending?.kind === 'save'
                ? 'Saving...'
                : activeDraft.mode === 'create'
                  ? 'Create profile'
                  : activeDraft.externalDeleted &&
                      activeDraft.overwriteAcknowledged
                    ? 'Recreate profile'
                    : 'Save changes'}
            </Button>

            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={
                busy || (activeDraft.mode === 'edit' && !draftDirty)
              }
              onClick={() => setState((prev) => discardActiveDraft(prev))}
            >
              Cancel
            </Button>
          </div>
        </div>
      </>
    );
  };

  return (
    <div
      className="tl-profiles"
      aria-busy={busy ? 'true' : undefined}
    >
      {/* Top-level status/error banners */}
      {externalError ? (
        <div className="tl-profiles-callout" data-tone="danger" role="alert">
          <div className="tl-profiles-callout-copy">
            <Flex align="center" gap="2">
              <AlertCircle size={16} aria-hidden="true" />
              <Text size="2" weight="bold">
                Registry unavailable
              </Text>
            </Flex>
            <Text size="1" color="gray">
              {externalError}
            </Text>
          </div>
          <div className="tl-profiles-callout-actions">
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              onClick={() => void handleReloadLatest()}
            >
              <RefreshCw size={14} aria-hidden="true" />
              Reload latest
            </Button>
          </div>
        </div>
      ) : null}

      {state.error ? (
        <div
          className="tl-profiles-callout"
          data-tone={state.conflict ? 'warn' : 'danger'}
          role="alert"
        >
          <div className="tl-profiles-callout-copy">
            <Flex align="center" gap="2">
              <AlertCircle size={16} aria-hidden="true" />
              <Text size="2" weight="bold">
                {state.conflict
                  ? 'Registry changed before this action completed'
                  : 'Registry action failed'}
              </Text>
            </Flex>
            <Text size="1" color="gray">
              {state.error.code
                ? `${state.error.code}: ${state.error.message}`
                : state.error.message}
            </Text>
          </div>
          <div className="tl-profiles-callout-actions">
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              onClick={() => void handleReloadLatest()}
            >
              <RefreshCw size={14} aria-hidden="true" />
              Reload latest
            </Button>
            <Button
              size="2"
              variant="ghost"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              aria-label="Dismiss error"
              onClick={() =>
                setState((prev) => ({
                  ...prev,
                  error: null,
                  conflict: false,
                }))
              }
            >
              <X size={14} aria-hidden="true" />
              Dismiss
            </Button>
          </div>
        </div>
      ) : null}

      {state.notice ? (
        <div
          className="tl-profiles-callout"
          data-tone={state.notice.tone === 'success' ? 'success' : undefined}
          role="status"
        >
          <div className="tl-profiles-callout-copy">
            <Flex align="center" gap="2">
              <CheckCircle2 size={16} aria-hidden="true" />
              <Text size="2" weight="bold">
                {state.notice.text}
              </Text>
            </Flex>
          </div>
          <div className="tl-profiles-callout-actions">
            <Button
              size="2"
              variant="ghost"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              aria-label="Dismiss notice"
              onClick={() => setState((prev) => ({ ...prev, notice: null }))}
            >
              <X size={14} aria-hidden="true" />
              Dismiss
            </Button>
          </div>
        </div>
      ) : null}

      {state.pendingSwitch ? (
        <div
          className="tl-profiles-callout"
          data-tone="warn"
          role="alertdialog"
          aria-label="Unsaved profile changes"
        >
          <div className="tl-profiles-callout-copy">
            <Text size="2" weight="bold">
              Unsaved changes in{' '}
              {activeDraft?.name.trim() || 'current profile draft'}
            </Text>
            <Text size="1" color="gray">
              Keep your draft in memory while switching, discard your unsaved
              edits, or stay on this profile.
            </Text>
          </div>
          <div className="tl-profiles-callout-actions">
            {activeDraft &&
            analysis &&
            analysis.errors.length === 0 &&
            !needsAcknowledgement(activeDraft) &&
            !state.conflict ? (
              <Button
                size="2"
                variant="solid"
                color="gray"
                highContrast
                className="tl-profiles-primary-btn"
                disabled={busy}
                onClick={() => void handleSaveAndSwitch()}
              >
                Save &amp; switch
              </Button>
            ) : null}
            {state.pendingSwitch.kind === 'select' ||
            activeDraft?.mode === 'edit' ? (
              <Button
                size="2"
                variant="soft"
                color="gray"
                className="tl-profiles-touch-btn"
                disabled={busy}
                onClick={() =>
                  setState((prev) => keepDraftAndSwitch(prev, compact))
                }
              >
                Keep draft &amp; switch
              </Button>
            ) : null}
            <Button
              size="2"
              variant="soft"
              color="red"
              className="tl-profiles-touch-btn"
              disabled={busy}
              onClick={() =>
                setState((prev) => discardDraftAndSwitch(prev, compact))
              }
            >
              Discard changes
            </Button>
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-touch-btn"
              disabled={busy}
              onClick={() => setState((prev) => cancelPendingSwitch(prev))}
            >
              Stay here
            </Button>
          </div>
        </div>
      ) : null}

      {!snapshot ? (
        <div className="tl-profiles-empty-box" role="status">
          <Text size="2" weight="bold">
            {loading ? 'Loading profiles...' : 'Profile registry is unavailable'}
          </Text>
          <Text size="1" color="gray">
            {loading
              ? 'Reading tool, model, and role profile definitions.'
              : 'Reload the registry when the local runtime is available.'}
          </Text>
          <div>
            <Button
              size="2"
              variant="soft"
              color="gray"
              className="tl-profiles-primary-btn"
              disabled={busy}
              onClick={() => void handleReloadLatest()}
            >
              <RefreshCw size={14} aria-hidden="true" />
              Reload latest
            </Button>
          </div>
        </div>
      ) : compact ? (
        state.compactStep === 'list' || !activeDraft ? (
          <>
            {renderProfileListSection()}
            {!activeDraft ? renderModelSetupCallout(false) : null}
          </>
        ) : (
          renderRoleSummariesAndEditor()
        )
      ) : (
        <>
          {renderProfileListSection()}
          {!activeDraft ? renderModelSetupCallout(false) : null}
          {renderRoleSummariesAndEditor()}
        </>
      )}
    </div>
  );
}
