import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  acknowledgeActiveDraftOverwrite,
  analyzeDraft,
  beginAsyncOperation,
  beginCloneProfile,
  beginCreateProfile,
  cancelPendingSwitch,
  clearDraftProfileOverrides,
  clearDraftRoleOverrides,
  countProfileDifficultyOverrides,
  countRoleDifficultyOverrides,
  createProfilesPanelState,
  discardActiveDraft,
  discardDraftAndSwitch,
  getActiveDraft,
  isDraftDirty,
  keepDraftAndSwitch,
  loadLatestIntoActiveDraft,
  needsAcknowledgement,
  profileFromDraft,
  profileSignature,
  resolveOperationError,
  resolveOperationSuccess,
  resolveReloadSuccess,
  selectProfile,
  selectRoleEffort,
  selectRoleModel,
  syncRegistrySnapshot,
  tableFromProfile,
  updateActiveDraft,
} from './app-profile-state.ts';
import {
  createEmptyRegistrySnapshot,
  EXECUTION_ROLES,
  type ModelDefinition,
  type RegistrySnapshot,
  type RoleAssignment,
  type ToolDefinition,
  type WorkflowProfile,
} from './registry/types.ts';

function makeTools(): ToolDefinition[] {
  return [
    {
      id: 'codex-main',
      label: 'Codex CLI',
      adapter: 'codex_cli',
      executable_path: '/usr/bin/codex',
      enabled: true,
    },
    {
      id: 'agy-main',
      label: 'Antigravity',
      adapter: 'antigravity',
      executable_path: '/usr/bin/agy',
      enabled: true,
    },
    {
      id: 'opencode-main',
      label: 'OpenCode Go',
      adapter: 'opencode_v2',
      executable_path: '/usr/bin/opencode',
      enabled: true,
    },
    {
      id: 'jev-main',
      label: 'Jev Decisions API',
      adapter: 'jev_api',
      executable_path: null,
      enabled: true,
    },
  ];
}

function makeModels(): ModelDefinition[] {
  return [
    {
      id: 'gpt-5.4',
      tool_id: 'codex-main',
      label: 'GPT-5.4',
      model_arg: 'gpt-5.4',
      efforts: ['low', 'medium', 'high', 'xhigh'],
      allow_no_effort: false,
    },
    {
      id: 'gpt-6-astra',
      tool_id: 'codex-main',
      label: 'GPT-6 Astra',
      model_arg: 'gpt-6-astra',
      efforts: ['high', 'xhigh'],
      allow_no_effort: false,
    },
    {
      id: 'gemini-4-argon-low',
      tool_id: 'agy-main',
      label: 'Gemini 4 Argon (Low)',
      model_arg: 'gemini-4-argon-low',
      efforts: ['low'],
      allow_no_effort: true,
    },
    {
      id: 'gemini-4-argon-medium',
      tool_id: 'agy-main',
      label: 'Gemini 4 Argon (Medium)',
      model_arg: 'gemini-4-argon-medium',
      efforts: ['medium'],
      allow_no_effort: true,
    },
    {
      id: 'gemini-4-argon-high',
      tool_id: 'agy-main',
      label: 'Gemini 4 Argon (High)',
      model_arg: 'gemini-4-argon-high',
      efforts: ['high'],
      allow_no_effort: true,
    },
    {
      id: 'deepseek-v4.1-flash',
      tool_id: 'opencode-main',
      label: 'DeepSeek v4.1 Flash',
      model_arg: 'opencode-go/deepseek-v4.1-flash',
      efforts: ['max'],
      allow_no_effort: false,
    },
    {
      id: 'jev-1.14.0',
      tool_id: 'jev-main',
      label: 'Jev 1.14.0',
      model_arg: 'jev-1.14.0',
      efforts: [],
      allow_no_effort: true,
    },
  ];
}

function makeDefaultAssignments(
  modelId: string = 'gpt-5.4',
  effort: string | null = 'high'
): RoleAssignment[] {
  return EXECUTION_ROLES.map((role) => ({
    role,
    difficulty: null,
    choice: { model_id: modelId, effort },
  }));
}

function makeProfile(
  id: string,
  name: string,
  extraAssignments: RoleAssignment[] = [],
  advisorModelId?: string
): WorkflowProfile {
  const base: WorkflowProfile = {
    id,
    name,
    assignments: [...makeDefaultAssignments(), ...extraAssignments],
  };
  if (advisorModelId) {
    base.decision_advisor = { model_id: advisorModelId, mode: 'shadow' };
  }
  return base;
}

function makeSnapshot(
  profiles: WorkflowProfile[],
  revision: number = 3
): RegistrySnapshot {
  return {
    ...createEmptyRegistrySnapshot(),
    revision,
    tools: makeTools(),
    models: makeModels(),
    profiles,
    default_profile_id: profiles[0]?.id ?? null,
  };
}

describe('app-profile-state transitions and invariants', () => {
  it('ignores stale async save results, blocks duplicate submissions, and preserves dirty drafts on stale_revision conflict', () => {
    const profileA = makeProfile('profile-a', 'Primary Profile');
    const snapshot = makeSnapshot([profileA], 5);

    let state = createProfilesPanelState(snapshot);
    state = updateActiveDraft(state, (draft) => ({
      ...draft,
      name: 'Primary Profile Edited',
      nameTouched: true,
    }));

    const firstStart = beginAsyncOperation(state, 'save', profileA.id);
    assert.ok(firstStart.operationId !== null);
    state = firstStart.state;
    assert.equal(state.pending?.expectedRevision, 5);

    // Duplicate submission while pending is blocked
    const duplicateStart = beginAsyncOperation(state, 'save', profileA.id);
    assert.equal(duplicateStart.operationId, null);
    assert.equal(duplicateStart.state, state);

    // Stale operation ID from an earlier generation is ignored
    const staleSuccess = resolveOperationSuccess(
      state,
      firstStart.operationId - 1,
      { ...snapshot, revision: 6 },
      'Saved.'
    );
    assert.equal(staleSuccess.accepted, false);
    assert.equal(staleSuccess.state.pending?.id, firstStart.operationId);

    // Revision conflict preserves the dirty draft and records conflict
    const conflictOutcome = resolveOperationError(state, firstStart.operationId, {
      code: 'stale_revision',
      message: 'Registry revision conflict: expected 5, current is 6.',
    });
    assert.equal(conflictOutcome.accepted, true);
    state = conflictOutcome.state;
    assert.equal(state.pending, null);
    assert.equal(state.conflict, true);
    assert.equal(state.error?.code, 'stale_revision');

    const activeAfterConflict = getActiveDraft(state);
    assert.ok(activeAfterConflict);
    assert.equal(activeAfterConflict.name, 'Primary Profile Edited');
    assert.equal(isDraftDirty(activeAfterConflict), true);

    // Reloading latest where another profile was added clears conflict and keeps draft ready to save
    const reloadStart = beginAsyncOperation(state, 'reload');
    assert.ok(reloadStart.operationId !== null);
    state = reloadStart.state;

    // Late completion of the first failed save op is rejected
    const lateSave = resolveOperationSuccess(
      state,
      firstStart.operationId,
      { ...snapshot, revision: 6 },
      'Saved.'
    );
    assert.equal(lateSave.accepted, false);

    const profileB = makeProfile('profile-b', 'Second Profile');
    const latestSnapshot = makeSnapshot([profileA, profileB], 6);
    const reloadOutcome = resolveReloadSuccess(
      state,
      reloadStart.operationId,
      latestSnapshot
    );
    assert.equal(reloadOutcome.accepted, true);
    state = reloadOutcome.state;
    assert.equal(state.conflict, false);
    assert.equal(state.error, null);
    const activeAfterReload = getActiveDraft(state);
    assert.ok(activeAfterReload);
    assert.equal(activeAfterReload.name, 'Primary Profile Edited');
    assert.equal(activeAfterReload.sourceRevision, 6);
    assert.equal(activeAfterReload.externalChanged, false);
  });

  it('distinguishes unrelated registry revisions from external profile drift and external deletion with safe ID recreation', () => {
    const profileA = makeProfile('profile-a', 'Alpha');
    const profileB = makeProfile('profile-b', 'Beta');
    const snapshotV1 = makeSnapshot([profileA, profileB], 1);

    let state = createProfilesPanelState(snapshotV1);
    state = updateActiveDraft(state, (draft) => ({
      ...draft,
      assignments: selectRoleModel(
        snapshotV1,
        draft.assignments,
        'coder',
        'easy',
        'deepseek-v4.1-flash'
      ),
    }));

    // 1. Unrelated revision change (profileB renamed, profileA untouched)
    const snapshotV2 = makeSnapshot(
      [profileA, { ...profileB, name: 'Beta Updated' }],
      2
    );
    state = syncRegistrySnapshot(state, snapshotV2);
    let active = getActiveDraft(state)!;
    assert.equal(active.externalChanged, false);
    assert.equal(active.externalDeleted, false);
    assert.equal(active.sourceRevision, 2);
    assert.equal(active.assignments.coder.easy.model_id, 'deepseek-v4.1-flash');
    assert.equal(state.draftsById['profile-b']?.name, 'Beta Updated');

    // 2. External modification of profileA
    const modifiedA = makeProfile('profile-a', 'Alpha Server Rename');
    const snapshotV3 = makeSnapshot([modifiedA, profileB], 3);
    state = syncRegistrySnapshot(state, snapshotV3);
    active = getActiveDraft(state)!;
    assert.equal(active.externalChanged, true);
    assert.equal(active.externalDeleted, false);
    assert.equal(needsAcknowledgement(active), true);
    assert.equal(active.assignments.coder.easy.model_id, 'deepseek-v4.1-flash');

    // Acknowledge overwrite for V3
    state = acknowledgeActiveDraftOverwrite(state);
    active = getActiveDraft(state)!;
    assert.equal(needsAcknowledgement(active), false);
    assert.equal(active.acknowledgedSignature, profileSignature(modifiedA));

    // If profileA changes again in V4, overwriteAcknowledged must reset
    const modifiedA2 = makeProfile('profile-a', 'Alpha Second Server Rename');
    const snapshotV4 = makeSnapshot([modifiedA2, profileB], 4);
    state = syncRegistrySnapshot(state, snapshotV4);
    active = getActiveDraft(state)!;
    assert.equal(active.externalChanged, true);
    assert.equal(needsAcknowledgement(active), true);

    // Load latest into active draft
    state = loadLatestIntoActiveDraft(state);
    active = getActiveDraft(state)!;
    assert.equal(active.name, 'Alpha Second Server Rename');
    assert.equal(active.externalChanged, false);
    assert.equal(isDraftDirty(active), false);

    // 3. Re-dirty profileA, then external deletion in V5
    state = updateActiveDraft(state, (draft) => ({
      ...draft,
      name: 'Alpha Local Draft',
      nameTouched: true,
    }));
    const snapshotV5 = makeSnapshot([profileB], 5);
    state = syncRegistrySnapshot(state, snapshotV5);
    active = getActiveDraft(state)!;
    assert.equal(active.externalChanged, true);
    assert.equal(active.externalDeleted, true);
    assert.equal(needsAcknowledgement(active), true);
    assert.equal(active.name, 'Alpha Local Draft');

    // Explicit recreation assigns a safe new ID distinct from deleted profile-a and existing profile-b
    state = acknowledgeActiveDraftOverwrite(state);
    active = getActiveDraft(state)!;
    assert.equal(needsAcknowledgement(active), false);
    assert.notEqual(active.id, 'profile-a');
    assert.notEqual(active.id, 'profile-b');
    assert.equal(active.id.length > 0 && active.id.length <= 64, true);

    const recreatedProfile = profileFromDraft(active);
    assert.equal(recreatedProfile.id, active.id);
    assert.equal(recreatedProfile.name, 'Alpha Local Draft');

    // Completing save of the recreated profile removes old 'profile-a' draft entry and selects new ID
    const recreateSaveStart = beginAsyncOperation(
      state,
      'save',
      recreatedProfile.id
    );
    assert.ok(recreateSaveStart.operationId !== null);
    const snapshotV6 = makeSnapshot([profileB, recreatedProfile], 6);
    const recreateSaved = resolveOperationSuccess(
      recreateSaveStart.state,
      recreateSaveStart.operationId,
      snapshotV6,
      'Recreated.'
    );
    assert.equal(recreateSaved.accepted, true);
    assert.equal(recreateSaved.state.selectedProfileId, recreatedProfile.id);
    assert.equal(recreateSaved.state.draftsById['profile-a'], undefined);
    assert.equal(
      isDraftDirty(recreateSaved.state.draftsById[recreatedProfile.id]!),
      false
    );

    // Alternatively, discarding the deleted draft closes it and selects remaining profile-b
    state = discardActiveDraft(state);
    assert.equal(state.selectedProfileId, 'profile-b');
    assert.equal(state.draftsById['profile-a'], undefined);
  });

  it('isolates role difficulty override reset and profile-wide override reset without touching defaults, advisor, or other profiles', () => {
    const profileOne = makeProfile(
      'profile-1',
      'Profile One',
      [
        {
          role: 'coder',
          difficulty: 'easy',
          choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
        },
        {
          role: 'coder',
          difficulty: 'medium',
          choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
        },
        {
          role: 'coder',
          difficulty: 'hard',
          choice: { model_id: 'gemini-4-argon-high', effort: 'high' },
        },
        {
          role: 'reviewer',
          difficulty: 'extreme',
          choice: { model_id: 'gpt-6-astra', effort: 'xhigh' },
        },
      ],
      'jev-1.14.0'
    );
    const profileTwo = makeProfile('profile-2', 'Profile Two', [
      {
        role: 'architect',
        difficulty: 'hard',
        choice: { model_id: 'gpt-6-astra', effort: 'high' },
      },
    ]);
    const snapshot = makeSnapshot([profileOne, profileTwo]);
    let state = createProfilesPanelState(snapshot);

    let draftOne = getActiveDraft(state)!;
    assert.equal(countRoleDifficultyOverrides(draftOne.assignments, 'coder'), 3);
    assert.equal(
      countRoleDifficultyOverrides(draftOne.assignments, 'reviewer'),
      1
    );
    assert.equal(countProfileDifficultyOverrides(draftOne.assignments), 4);

    // Clear only coder difficulty overrides
    state = updateActiveDraft(state, (d) => clearDraftRoleOverrides(d, 'coder'));
    draftOne = getActiveDraft(state)!;
    assert.equal(countRoleDifficultyOverrides(draftOne.assignments, 'coder'), 0);
    assert.deepEqual(draftOne.assignments.coder.default, {
      model_id: 'gpt-5.4',
      effort: 'high',
      pending_family: null,
    });
    assert.equal(
      countRoleDifficultyOverrides(draftOne.assignments, 'reviewer'),
      1
    );
    assert.deepEqual(draftOne.assignments.reviewer.extreme, {
      model_id: 'gpt-6-astra',
      effort: 'xhigh',
      pending_family: null,
    });
    assert.deepEqual(draftOne.decisionAdvisor, {
      model_id: 'jev-1.14.0',
      mode: 'shadow',
    });

    // Restore coder override and clear all profile overrides for profile-1
    state = updateActiveDraft(state, (d) => ({
      ...d,
      assignments: selectRoleModel(
        snapshot,
        d.assignments,
        'coder',
        'easy',
        'deepseek-v4.1-flash'
      ),
    }));
    state = updateActiveDraft(state, (d) => clearDraftProfileOverrides(d));
    draftOne = getActiveDraft(state)!;
    assert.equal(countProfileDifficultyOverrides(draftOne.assignments), 0);
    for (const role of EXECUTION_ROLES) {
      assert.deepEqual(draftOne.assignments[role].default, {
        model_id: 'gpt-5.4',
        effort: 'high',
        pending_family: null,
      });
    }
    assert.deepEqual(draftOne.decisionAdvisor, {
      model_id: 'jev-1.14.0',
      mode: 'shadow',
    });

    // Other profile's overrides remain untouched
    const draftTwo = state.draftsById['profile-2']!;
    assert.equal(
      countRoleDifficultyOverrides(draftTwo.assignments, 'architect'),
      1
    );
  });

  it('resolves encoded-effort model families, normalizes unsupported efforts on model change, and preserves concrete overrides and disabled selections', () => {
    const legacyAndDisabledProfile: WorkflowProfile = {
      id: 'profile-legacy',
      name: 'Legacy Profile',
      assignments: [
        {
          role: 'follower',
          difficulty: null,
          choice: { model_id: 'gpt-5.4', effort: 'high' },
        },
        ...makeDefaultAssignments('gpt-5.4', 'high'),
        {
          role: 'coder',
          difficulty: 'easy',
          choice: { model_id: 'deepseek-v4.1-flash', effort: 'max' },
        },
        {
          role: 'reviewer',
          difficulty: 'hard',
          choice: { model_id: 'retired-model-id', effort: 'high' },
        },
      ],
    };
    const snapshot = makeSnapshot([legacyAndDisabledProfile]);
    let table = tableFromProfile(legacyAndDisabledProfile);

    // Obsolete follower is ignored while retired model assignment is retained
    assert.equal('follower' in table, false);
    assert.equal(table.reviewer.hard.model_id, 'retired-model-id');
    assert.equal(table.reviewer.hard.effort, 'high');

    // Selecting encoded Antigravity family resolves concrete variant matching current effort ('high')
    table = selectRoleModel(
      snapshot,
      table,
      'coder',
      'default',
      'family:agy-main:gemini-4-argon'
    );
    assert.equal(table.coder.default.model_id, 'gemini-4-argon-high');
    assert.equal(table.coder.default.effort, 'high');
    assert.equal(table.coder.default.pending_family, null);

    // Changing effort on encoded family maps to the corresponding variant model_id
    table = selectRoleEffort(snapshot, table, 'coder', 'default', 'low');
    assert.equal(table.coder.default.model_id, 'gemini-4-argon-low');
    assert.equal(table.coder.default.effort, 'low');

    // Changing role default model to deepseek-v4.1-flash normalizes unsupported 'low' effort to 'max'
    // without touching independent concrete overrides
    table = selectRoleModel(
      snapshot,
      table,
      'coder',
      'default',
      'deepseek-v4.1-flash'
    );
    assert.equal(table.coder.default.model_id, 'deepseek-v4.1-flash');
    assert.equal(table.coder.default.effort, 'max');
    assert.equal(table.reviewer.hard.model_id, 'retired-model-id');

    // Passing an unsupported effort to selectRoleEffort never discards the concrete model
    table = selectRoleEffort(snapshot, table, 'coder', 'easy', 'unsupported');
    assert.equal(table.coder.easy.model_id, 'deepseek-v4.1-flash');
    assert.equal(table.coder.easy.effort, 'max');

    // Switching difficulty override model to "" ("Use role default") resets the override pair
    table = selectRoleModel(snapshot, table, 'coder', 'easy', '');
    assert.equal(table.coder.easy.model_id, '');
    assert.equal(table.coder.easy.effort, null);

    // Repairing the retired model on reviewer:hard allows clean validation and omits follower
    table = selectRoleModel(snapshot, table, 'reviewer', 'hard', '');
    const draft = {
      ...createProfilesPanelState(snapshot).draftsById['profile-legacy']!,
      assignments: table,
    };
    const analysis = analyzeDraft(snapshot, draft);
    assert.deepEqual(analysis.errors, []);
    const serialized = profileFromDraft(draft);
    assert.equal(
      serialized.assignments.some((a) => a.role === 'follower'),
      false
    );
    assert.equal(
      serialized.assignments.some(
        (a) => a.role === 'coder' && a.difficulty === 'easy'
      ),
      false
    );
  });

  it('guards unsaved profile switches and supports keep-draft, discard-draft, and cancel-switch flows', () => {
    const profileA = makeProfile('profile-a', 'Profile A');
    const profileB = makeProfile('profile-b', 'Profile B');
    const snapshot = makeSnapshot([profileA, profileB]);

    let state = createProfilesPanelState(snapshot);
    assert.equal(state.selectedProfileId, 'profile-a');

    // Clean switch happens immediately without pendingSwitch prompt
    state = selectProfile(state, 'profile-b', true);
    assert.equal(state.selectedProfileId, 'profile-b');
    assert.equal(state.compactStep, 'editor');
    assert.equal(state.pendingSwitch, null);

    // Edit profile-b so it becomes dirty
    state = updateActiveDraft(state, (draft) => ({
      ...draft,
      name: 'Profile B Unsaved Edit',
      nameTouched: true,
    }));
    assert.equal(isDraftDirty(getActiveDraft(state)!), true);

    // Attempting to select profile-a records pendingSwitch instead of losing edits
    state = selectProfile(state, 'profile-a');
    assert.equal(state.selectedProfileId, 'profile-b');
    assert.deepEqual(state.pendingSwitch, {
      kind: 'select',
      profileId: 'profile-a',
    });

    // Canceling switch stays on profile-b
    state = cancelPendingSwitch(state);
    assert.equal(state.pendingSwitch, null);
    assert.equal(state.selectedProfileId, 'profile-b');
    assert.equal(getActiveDraft(state)?.name, 'Profile B Unsaved Edit');

    // Keeping draft and switching moves to profile-a while preserving profile-b's dirty draft
    state = selectProfile(state, 'profile-a');
    state = keepDraftAndSwitch(state);
    assert.equal(state.selectedProfileId, 'profile-a');
    assert.equal(state.pendingSwitch, null);
    assert.equal(state.draftsById['profile-b']?.name, 'Profile B Unsaved Edit');

    // Switching back to profile-b (since profile-a is clean) immediately shows preserved dirty draft
    state = selectProfile(state, 'profile-b');
    assert.equal(state.selectedProfileId, 'profile-b');
    assert.equal(getActiveDraft(state)?.name, 'Profile B Unsaved Edit');

    // Starting New or Clone while dirty also sets pendingSwitch; discardDraftAndSwitch resets profile-b and opens clone
    state = beginCloneProfile(state, 'profile-a');
    assert.deepEqual(state.pendingSwitch, {
      kind: 'clone',
      sourceProfileId: 'profile-a',
    });
    state = discardDraftAndSwitch(state);
    assert.equal(state.draftsById['profile-b']?.name, 'Profile B');
    assert.equal(isDraftDirty(state.draftsById['profile-b']!), false);
    assert.equal(getActiveDraft(state)?.mode, 'create');
    assert.equal(getActiveDraft(state)?.name, 'Profile A copy');

    // Beginning create from clean state works directly
    state = discardActiveDraft(state);
    state = beginCreateProfile(state);
    assert.equal(getActiveDraft(state)?.mode, 'create');
  });

  it('enforces snapshot revision monotonicity across delayed default, save, delete, reload, and background sync', () => {
    const profileA = makeProfile('profile-a', 'Alpha');
    const profileB = makeProfile('profile-b', 'Beta');
    const profileOld = makeProfile('profile-old', 'To Be Deleted In Rev12');
    const snapshotRev10 = makeSnapshot([profileA, profileB, profileOld], 10);

    // 1. Delayed 'default' completion after rev 12 arrives via syncRegistrySnapshot
    let state = createProfilesPanelState(snapshotRev10);
    state = updateActiveDraft(state, (draft) => ({
      ...draft,
      name: 'Alpha Unsaved Dirty Draft',
      nameTouched: true,
    }));

    const defaultOp = beginAsyncOperation(state, 'default', profileB.id);
    assert.ok(defaultOp.operationId !== null);
    state = defaultOp.state;
    assert.equal(state.pending?.expectedRevision, 10);

    const snapshotRev11Default: RegistrySnapshot = {
      ...snapshotRev10,
      revision: 11,
      default_profile_id: profileB.id,
    };
    const profileC = makeProfile('profile-c', 'Gamma Added At 12');
    const updatedToolsAt12: ToolDefinition[] = makeTools().map((tool) =>
      tool.id === 'opencode-main' ? { ...tool, label: 'OpenCode Go Rev12' } : tool
    );
    const snapshotRev12: RegistrySnapshot = {
      ...snapshotRev10,
      revision: 12,
      tools: updatedToolsAt12,
      profiles: [profileA, { ...profileB, name: 'Beta Rev12' }, profileC],
      default_profile_id: profileC.id,
    };

    // Background sync advances state to rev 12 while 'default' op (from rev 10 -> 11) is still pending
    state = syncRegistrySnapshot(state, snapshotRev12);
    assert.equal(state.snapshot?.revision, 12);

    // Older operationId is ignored
    const staleIdDefault = resolveOperationSuccess(
      state,
      defaultOp.operationId - 1,
      snapshotRev11Default,
      'Default set.'
    );
    assert.equal(staleIdDefault.accepted, false);
    assert.equal(staleIdDefault.state.pending?.id, defaultOp.operationId);

    // Legitimate pending operation resolves with rev 11: completes once, retains rev 12 state
    const resolvedDefault = resolveOperationSuccess(
      state,
      defaultOp.operationId,
      snapshotRev11Default,
      'Default set.'
    );
    assert.equal(resolvedDefault.accepted, true);
    state = resolvedDefault.state;
    assert.equal(state.pending, null);
    assert.equal(state.snapshot?.revision, 12);
    assert.equal(state.snapshot?.default_profile_id, profileC.id);
    assert.equal(
      state.snapshot?.tools.find((t) => t.id === 'opencode-main')?.label,
      'OpenCode Go Rev12'
    );
    assert.equal(
      state.snapshot?.profiles.some((p) => p.id === profileOld.id),
      false
    );
    assert.equal(state.draftsById[profileOld.id], undefined);
    assert.equal(state.draftsById[profileB.id]?.name, 'Beta Rev12');
    assert.equal(state.draftsById[profileA.id]?.name, 'Alpha Unsaved Dirty Draft');
    assert.equal(isDraftDirty(state.draftsById[profileA.id]!), true);

    // Duplicate resolution of the same operationId is ignored
    const duplicateDefault = resolveOperationSuccess(
      state,
      defaultOp.operationId,
      snapshotRev11Default,
      'Default set.'
    );
    assert.equal(duplicateDefault.accepted, false);

    // Older background snapshot arriving via syncRegistrySnapshot is ignored
    const afterStaleBgSync = syncRegistrySnapshot(state, snapshotRev11Default);
    assert.equal(afterStaleBgSync, state);
    assert.equal(afterStaleBgSync.snapshot?.revision, 12);

    // 2. Delayed 'save' completion after higher revision (normal progression, external drift, and external deletion)
    let saveState = createProfilesPanelState(snapshotRev10);
    // Make profile-b a dirty unrelated draft, then switch to profile-a and edit/save profile-a
    saveState = selectProfile(saveState, profileB.id);
    saveState = updateActiveDraft(saveState, (draft) => ({
      ...draft,
      name: 'Beta Dirty Unrelated Draft',
      nameTouched: true,
    }));
    saveState = selectProfile(saveState, profileA.id);
    saveState = keepDraftAndSwitch(saveState);
    saveState = updateActiveDraft(saveState, (draft) => ({
      ...draft,
      name: 'Alpha Saved At 11',
      nameTouched: true,
    }));

    const savedProfileA11 = profileFromDraft(getActiveDraft(saveState)!);
    const saveOp = beginAsyncOperation(saveState, 'save', profileA.id);
    assert.ok(saveOp.operationId !== null);
    saveState = saveOp.state;

    const snapshotRev11Save = makeSnapshot(
      [savedProfileA11, profileB, profileOld],
      11
    );

    // 2a. Rev 12 keeps savedProfileA11, modifies profileB externally, deletes profileOld, and updates default
    const snapshotRev12AfterSave: RegistrySnapshot = {
      ...makeSnapshot(
        [savedProfileA11, { ...profileB, name: 'Beta Server 12' }, profileC],
        12
      ),
      default_profile_id: profileC.id,
    };
    const saveStateAt12 = syncRegistrySnapshot(saveState, snapshotRev12AfterSave);
    const saveResolvedA = resolveOperationSuccess(
      saveStateAt12,
      saveOp.operationId,
      snapshotRev11Save,
      'Saved.'
    );
    assert.equal(saveResolvedA.accepted, true);
    assert.equal(saveResolvedA.state.pending, null);
    assert.equal(saveResolvedA.state.snapshot?.revision, 12);
    assert.equal(saveResolvedA.state.snapshot?.default_profile_id, profileC.id);
    assert.equal(saveResolvedA.state.draftsById[profileOld.id], undefined);
    // Saved draft for profile-a is now clean and synced to rev 12
    const draftAAfterSave = saveResolvedA.state.draftsById[profileA.id]!;
    assert.equal(draftAAfterSave.name, 'Alpha Saved At 11');
    assert.equal(draftAAfterSave.sourceRevision, 12);
    assert.equal(draftAAfterSave.externalChanged, false);
    assert.equal(isDraftDirty(draftAAfterSave), false);
    // Dirty unrelated draft for profile-b stays dirty and reflects rev 12 drift
    const draftBAfterSave = saveResolvedA.state.draftsById[profileB.id]!;
    assert.equal(draftBAfterSave.name, 'Beta Dirty Unrelated Draft');
    assert.equal(isDraftDirty(draftBAfterSave), true);
    assert.equal(draftBAfterSave.externalChanged, true);

    // 2b. Rev 12 externally modifies profile-a itself after the rev 11 save
    const driftedProfileA12 = makeProfile('profile-a', 'Alpha Drifted At 12');
    const snapshotRev12Drifted = makeSnapshot([driftedProfileA12, profileB], 12);
    const saveStateDrifted12 = syncRegistrySnapshot(
      saveState,
      snapshotRev12Drifted
    );
    const saveResolvedDrifted = resolveOperationSuccess(
      saveStateDrifted12,
      saveOp.operationId,
      snapshotRev11Save,
      'Saved.'
    );
    assert.equal(saveResolvedDrifted.accepted, true);
    assert.equal(saveResolvedDrifted.state.snapshot?.revision, 12);
    const draftADrifted = saveResolvedDrifted.state.draftsById[profileA.id]!;
    assert.equal(draftADrifted.name, 'Alpha Drifted At 12');
    assert.equal(draftADrifted.sourceRevision, 12);
    assert.equal(draftADrifted.externalChanged, false);
    assert.equal(isDraftDirty(draftADrifted), false);
    assert.equal(
      saveResolvedDrifted.state.draftsById[profileB.id]?.name,
      'Beta Dirty Unrelated Draft'
    );

    // 2c. Rev 12 externally deletes profile-a after the rev 11 save -> never resurrects profile-a
    const snapshotRev12DeletedA = makeSnapshot([profileB], 12);
    const saveStateDeleted12 = syncRegistrySnapshot(
      saveState,
      snapshotRev12DeletedA
    );
    const saveResolvedDeleted = resolveOperationSuccess(
      saveStateDeleted12,
      saveOp.operationId,
      snapshotRev11Save,
      'Saved.'
    );
    assert.equal(saveResolvedDeleted.accepted, true);
    assert.equal(saveResolvedDeleted.state.snapshot?.revision, 12);
    assert.equal(saveResolvedDeleted.state.draftsById[profileA.id], undefined);
    assert.equal(saveResolvedDeleted.state.selectedProfileId, profileB.id);
    assert.equal(
      saveResolvedDeleted.state.draftsById[profileB.id]?.name,
      'Beta Dirty Unrelated Draft'
    );

    // 3. Delayed 'delete' completion after higher revision
    let delState = createProfilesPanelState(snapshotRev10);
    delState = selectProfile(delState, profileB.id);
    delState = updateActiveDraft(delState, (draft) => ({
      ...draft,
      name: 'Beta Dirty During Delete',
      nameTouched: true,
    }));
    const delOp = beginAsyncOperation(delState, 'delete', profileA.id);
    assert.ok(delOp.operationId !== null);
    delState = delOp.state;

    const snapshotRev11Delete = makeSnapshot([profileB, profileOld], 11);
    const snapshotRev12Delete = makeSnapshot(
      [{ ...profileB, name: 'Beta Server 12' }, profileC],
      12
    );
    delState = syncRegistrySnapshot(delState, snapshotRev12Delete);
    const delResolved = resolveOperationSuccess(
      delState,
      delOp.operationId,
      snapshotRev11Delete,
      'Deleted.'
    );
    assert.equal(delResolved.accepted, true);
    assert.equal(delResolved.state.pending, null);
    assert.equal(delResolved.state.snapshot?.revision, 12);
    assert.equal(delResolved.state.draftsById[profileA.id], undefined);
    assert.equal(delResolved.state.draftsById[profileOld.id], undefined);
    assert.equal(
      delResolved.state.draftsById[profileB.id]?.name,
      'Beta Dirty During Delete'
    );

    // 4. Delayed 'reload' completion after higher revision
    let reloadState = createProfilesPanelState(snapshotRev10);
    reloadState = updateActiveDraft(reloadState, (draft) => ({
      ...draft,
      name: 'Alpha Dirty During Reload',
      nameTouched: true,
    }));
    const reloadOp = beginAsyncOperation(reloadState, 'reload');
    assert.ok(reloadOp.operationId !== null);
    reloadState = reloadOp.state;
    reloadState = syncRegistrySnapshot(reloadState, snapshotRev12);

    const staleReload = resolveReloadSuccess(
      reloadState,
      reloadOp.operationId - 1,
      snapshotRev11Default
    );
    assert.equal(staleReload.accepted, false);

    const reloadResolved = resolveReloadSuccess(
      reloadState,
      reloadOp.operationId,
      snapshotRev11Default
    );
    assert.equal(reloadResolved.accepted, true);
    assert.equal(reloadResolved.state.pending, null);
    assert.equal(reloadResolved.state.snapshot?.revision, 12);
    assert.equal(reloadResolved.state.snapshot?.default_profile_id, profileC.id);
    assert.equal(
      reloadResolved.state.draftsById[profileA.id]?.name,
      'Alpha Dirty During Reload'
    );
    assert.equal(
      resolveReloadSuccess(
        reloadResolved.state,
        reloadOp.operationId,
        snapshotRev11Default
      ).accepted,
      false
    );
  });
});
