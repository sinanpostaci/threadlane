import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  applySnapshotPollError,
  applySnapshotPollSuccess,
  beginMutation,
  beginPlansRefresh,
  beginRegistryRefresh,
  beginSnapshotPoll,
  createInitialSnapshot,
  createInitialState,
  hasPlansSnapshotChanged,
  hasRegistrySnapshotChanged,
  navigateToPage,
  openRunDetail,
  plansSnapshotFingerprint,
  registrySnapshotFingerprint,
  selectBinding,
  shouldPollActivePlansStore,
} from '../state.ts';
import {
  applyPlanChangeLocally,
  applyRegistryChangeLocally,
  createEmptyRegistrySnapshot,
  type PlanRevision,
  type RegistrySnapshot,
} from '../registry/ipc.ts';
import {
  applyLoadedExecutionPreview,
  canSubmitApprovalDraft,
  compileSamplePreviewForDevMode,
  createInitialApprovalFormDraft,
  setApprovalReviewConfirmed,
  syncApprovalDraftWithContext,
  updateApprovalDraftField,
} from './ipc.ts';
import { isEditablePlanFieldTagName } from '../pages/plans.ts';
import type { Attachment, BindingSummary, DesktopSnapshot, TaskSummary } from '../types.ts';

const NOW_MS = 1_730_000_000_000;
const VALID_THREAD_UUID = '550e8400-e29b-41d4-a716-446655440000';
const ALT_THREAD_UUID = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
const BINDING_ID = 'bind-alpha';
const REPOSITORY_PATH = '/home/operator/threadlane';
const BASE_COMMIT = 'a1b2c3d4e5f60718293a4b5c6d7e8f9012345678';

function makeTask(overrides: Partial<TaskSummary> = {}): TaskSummary {
  return {
    run_id: 'run-alpha',
    binding_id: BINDING_ID,
    revision: 2,
    status: 'Running',
    title: 'Inspect live plans refresh',
    provider: 'antigravity',
    requested_model: 'gemini-4-argon-high',
    requested_effort: 'high',
    observed_model: 'gemini-4-argon-high',
    observed_effort: 'high',
    last_summary: 'Watching local plan ledger',
    updated_at_ms: NOW_MS,
    needs_evidence: false,
    kind: 'worker',
    profile_id: 'profile-live',
    ...overrides,
  };
}

function makeAttachment(overrides: Partial<Attachment> = {}): Attachment {
  return {
    mode: 'runtime',
    connection: 'connected',
    state_dir: '/tmp/threadlane-live-state',
    message: null,
    updated_at_ms: NOW_MS,
    ...overrides,
  };
}

function makeBindings(threadUuid: string = VALID_THREAD_UUID): BindingSummary[] {
  return [
    {
      binding_id: BINDING_ID,
      thread_uuid: threadUuid,
      label: 'Live plan binding',
      is_local_demo: false,
    },
    {
      binding_id: 'bind-beta',
      thread_uuid: ALT_THREAD_UUID,
      label: 'Second binding',
      is_local_demo: false,
    },
  ];
}

function makeDesktopSnapshot(
  attachment: Attachment = makeAttachment()
): DesktopSnapshot {
  const base = createInitialSnapshot(NOW_MS);
  return {
    ...base,
    attachment,
    bindings: makeBindings(),
    selected_binding_id: BINDING_ID,
    tasks: [makeTask()],
    total_runs: 1,
    active_runs: 1,
  };
}

function makeRegistry(): RegistrySnapshot {
  let snapshot = createEmptyRegistrySnapshot();
  snapshot = applyRegistryChangeLocally(snapshot, 0, {
    op: 'upsert_tool',
    tool: {
      id: 'tool-agy',
      label: 'Antigravity CLI',
      adapter: 'antigravity',
      executable_path: '/run/current-system/sw/bin/agy',
      enabled: true,
    },
  });
  snapshot = applyRegistryChangeLocally(snapshot, 1, {
    op: 'upsert_tool',
    tool: {
      id: 'tool-extra',
      label: 'Unused local tool',
      adapter: 'git',
      executable_path: '/run/current-system/sw/bin/git',
      enabled: false,
    },
  });
  snapshot = applyRegistryChangeLocally(snapshot, 2, {
    op: 'upsert_model',
    model: {
      id: 'model-argon',
      tool_id: 'tool-agy',
      label: 'Gemini 4 Argon High',
      model_arg: 'gemini-4-argon-high',
      efforts: ['high'],
      allow_no_effort: false,
    },
  });
  snapshot = applyRegistryChangeLocally(snapshot, 3, {
    op: 'upsert_profile',
    profile: {
      id: 'profile-live',
      name: 'Live Plan Profile',
      assignments: [
        {
          role: 'coder',
          difficulty: 'hard',
          choice: { model_id: 'model-argon', effort: 'high' },
        },
      ],
    },
  });
  return snapshot;
}

function makePublishedPlan(registry: RegistrySnapshot): PlanRevision {
  const saved = applyPlanChangeLocally(
    [],
    0,
    {
      op: 'save_draft',
      plan_id: 'plan-live',
      binding_id: BINDING_ID,
      profile_id: 'profile-live',
      profile_revision: registry.revision,
      summary: 'Bounded live plan refresh',
      tasks: [
        {
          id: 'task-live',
          title: 'Refresh local plan context',
          instruction: 'Compare persisted plan content before rebuilding the page.',
          role: 'coder',
          difficulty: 'hard',
          depends_on: [],
          allowed_write_paths: ['apps/desktop/src/main.ts'],
        },
      ],
    },
    NOW_MS
  );
  return applyPlanChangeLocally(
    saved.revisions,
    saved.updated.revision,
    {
      op: 'publish',
      plan_id: saved.updated.plan_id,
      revision: saved.updated.revision,
    },
    NOW_MS + 1
  ).updated;
}

function makeConfirmedApprovalDraft(
  registry: RegistrySnapshot,
  plan: PlanRevision,
  bindings: BindingSummary[],
  attachment: Attachment
) {
  let draft = applyLoadedExecutionPreview(
    createInitialApprovalFormDraft(plan.plan_id, plan.revision, plan.binding_id),
    compileSamplePreviewForDevMode(plan, registry),
    false
  );
  draft = updateApprovalDraftField(draft, 'repositoryPath', REPOSITORY_PATH);
  draft = updateApprovalDraftField(draft, 'baseCommit', BASE_COMMIT);
  return setApprovalReviewConfirmed(draft, true, bindings, attachment, BINDING_ID);
}

describe('live plan and registry store refresh', () => {
  it('uses revision plus content fingerprints to distinguish changed from unchanged local data', () => {
    const registry = makeRegistry();
    const plan = makePublishedPlan(registry);

    const sameRegistry = structuredClone(registry);
    assert.equal(hasRegistrySnapshotChanged(registry, sameRegistry), false);
    assert.equal(
      registrySnapshotFingerprint(registry),
      registrySnapshotFingerprint(sameRegistry)
    );

    const reorderedRegistry: RegistrySnapshot = {
      ...registry,
      tools: [...registry.tools].reverse(),
      models: [...registry.models].reverse(),
      profiles: [...registry.profiles].reverse(),
    };
    assert.equal(hasRegistrySnapshotChanged(registry, reorderedRegistry), false);

    const newerRegistry = applyRegistryChangeLocally(registry, registry.revision, {
      op: 'set_default_profile',
      profile_id: 'profile-live',
    });
    assert.equal(hasRegistrySnapshotChanged(registry, newerRegistry), true);
    assert.notEqual(
      registrySnapshotFingerprint(registry),
      registrySnapshotFingerprint(newerRegistry)
    );

    const samePlans = [structuredClone(plan)];
    assert.equal(hasPlansSnapshotChanged([plan], samePlans), false);
    assert.equal(
      plansSnapshotFingerprint([plan]),
      plansSnapshotFingerprint(samePlans)
    );

    const received = applyPlanChangeLocally(
      [plan],
      plan.revision,
      {
        op: 'receive',
        plan_id: plan.plan_id,
        revision: plan.revision,
      },
      NOW_MS + 2
    );
    assert.equal(hasPlansSnapshotChanged([plan], received.revisions), true);
    assert.notEqual(
      plansSnapshotFingerprint([plan]),
      plansSnapshotFingerprint(received.revisions)
    );
  });

  it('polls only an active, visible, idle Plans page', () => {
    const state = navigateToPage(createInitialState(), 'plans');
    assert.equal(shouldPollActivePlansStore(state, false), true);
    assert.equal(shouldPollActivePlansStore(state, true), false);

    assert.equal(
      shouldPollActivePlansStore(navigateToPage(state, 'activity'), false),
      false
    );

    const plansInFlight = beginPlansRefresh(state, true);
    assert.ok(plansInFlight.ticket !== null);
    assert.equal(shouldPollActivePlansStore(plansInFlight.state, false), false);

    const registryInFlight = beginRegistryRefresh(state, true);
    assert.ok(registryInFlight.ticket !== null);
    assert.equal(shouldPollActivePlansStore(registryInFlight.state, false), false);

    const mutating = beginMutation(state, 'attach');
    assert.ok(mutating.ticket !== null);
    assert.equal(shouldPollActivePlansStore(mutating.state, false), false);

    const detailOpen = openRunDetail(state, BINDING_ID, 'run-alpha').state;
    assert.equal(shouldPollActivePlansStore(detailOpen, false), false);
  });

  it('marks only the current failed observer poll disconnected while preserving cached runs and bindings', () => {
    let state = createInitialState();
    const first = beginSnapshotPoll(state);
    assert.ok(first.ticket !== null);
    state = applySnapshotPollSuccess(
      first.state,
      first.ticket,
      makeDesktopSnapshot()
    );
    assert.equal(state.snapshot.attachment.connection, 'connected');
    assert.equal(state.snapshot.tasks[0]?.run_id, 'run-alpha');

    const failed = beginSnapshotPoll(state);
    assert.ok(failed.ticket !== null);
    const disconnected = applySnapshotPollError(
      failed.state,
      failed.ticket,
      'Runtime observer disconnected.'
    );
    assert.equal(disconnected.snapshot.attachment.connection, 'disconnected');
    assert.deepEqual(disconnected.snapshot.bindings, makeBindings());
    assert.equal(disconnected.snapshot.tasks[0]?.run_id, 'run-alpha');
    assert.equal(disconnected.selectedBindingId, BINDING_ID);
    assert.equal(disconnected.pollInFlight, false);
    assert.equal(disconnected.actionError, 'Runtime observer disconnected.');

    const retry = beginSnapshotPoll(disconnected);
    assert.ok(retry.ticket !== null);
    const reconnected = applySnapshotPollSuccess(
      retry.state,
      retry.ticket,
      makeDesktopSnapshot()
    );
    assert.equal(reconnected.snapshot.attachment.connection, 'connected');

    const stale = beginSnapshotPoll(reconnected);
    assert.ok(stale.ticket !== null);
    const switched = selectBinding(stale.state, 'bind-beta', true);
    assert.ok(switched.ticket !== null);
    const afterStaleError = applySnapshotPollError(
      switched.state,
      stale.ticket,
      'Stale observer failure.'
    );
    assert.equal(afterStaleError.snapshot.attachment.connection, 'connected');
    assert.equal(afterStaleError.actionError, null);
  });

  it('clears stale approval confirmation from the disconnected observer snapshot without losing draft fields', () => {
    const registry = makeRegistry();
    const plan = makePublishedPlan(registry);
    const bindings = makeBindings();
    const attachment = makeAttachment();
    const confirmed = makeConfirmedApprovalDraft(registry, plan, bindings, attachment);
    assert.equal(
      canSubmitApprovalDraft(confirmed, bindings, attachment, BINDING_ID),
      true
    );

    let state = createInitialState();
    const poll = beginSnapshotPoll(state);
    assert.ok(poll.ticket !== null);
    state = applySnapshotPollSuccess(
      poll.state,
      poll.ticket,
      makeDesktopSnapshot(attachment)
    );
    const failedPoll = beginSnapshotPoll(state);
    assert.ok(failedPoll.ticket !== null);
    state = applySnapshotPollError(
      failedPoll.state,
      failedPoll.ticket,
      'Observer fetch failed.'
    );

    const synced = syncApprovalDraftWithContext(
      confirmed,
      plan,
      registry,
      bindings,
      state.snapshot.attachment,
      BINDING_ID
    );
    assert.equal(synced.reviewConfirmed, false);
    assert.equal(synced.confirmedSubjectSignature, null);
    assert.equal(synced.repositoryPath, REPOSITORY_PATH);
    assert.equal(synced.baseCommit, BASE_COMMIT);
    assert.equal(
      canSubmitApprovalDraft(synced, bindings, state.snapshot.attachment, BINDING_ID),
      false
    );
  });

  it('clears stale approval preview and confirmation on external registry or plan changes while retaining operator draft input', () => {
    const registry = makeRegistry();
    const plan = makePublishedPlan(registry);
    const bindings = makeBindings();
    const attachment = makeAttachment();
    const confirmed = makeConfirmedApprovalDraft(registry, plan, bindings, attachment);

    const newerRegistry = applyRegistryChangeLocally(registry, registry.revision, {
      op: 'set_default_profile',
      profile_id: 'profile-live',
    });
    const afterRegistryChange = syncApprovalDraftWithContext(
      confirmed,
      plan,
      newerRegistry,
      bindings,
      attachment,
      BINDING_ID
    );
    assert.equal(afterRegistryChange.preview, null);
    assert.equal(afterRegistryChange.reviewConfirmed, false);
    assert.equal(afterRegistryChange.confirmedSubjectSignature, null);
    assert.equal(afterRegistryChange.repositoryPath, REPOSITORY_PATH);
    assert.equal(afterRegistryChange.baseCommit, BASE_COMMIT);
    assert.equal(
      canSubmitApprovalDraft(
        afterRegistryChange,
        bindings,
        attachment,
        BINDING_ID
      ),
      false
    );

    const supersededPlan: PlanRevision = {
      ...plan,
      status: 'superseded',
    };
    const afterPlanChange = syncApprovalDraftWithContext(
      confirmed,
      supersededPlan,
      registry,
      bindings,
      attachment,
      BINDING_ID
    );
    assert.equal(afterPlanChange.preview, null);
    assert.equal(afterPlanChange.reviewConfirmed, false);
    assert.equal(afterPlanChange.confirmedSubjectSignature, null);
    assert.equal(afterPlanChange.repositoryPath, REPOSITORY_PATH);
    assert.equal(afterPlanChange.baseCommit, BASE_COMMIT);
  });

  it('preserves focus only for editable Plans controls during background updates', () => {
    assert.equal(isEditablePlanFieldTagName('INPUT'), true);
    assert.equal(isEditablePlanFieldTagName('TEXTAREA'), true);
    assert.equal(isEditablePlanFieldTagName('SELECT'), true);
    assert.equal(isEditablePlanFieldTagName('BUTTON'), false);
    assert.equal(isEditablePlanFieldTagName(null), false);
  });
});
