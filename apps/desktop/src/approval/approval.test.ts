import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { Attachment, BindingSummary } from '../types.ts';
import {
  applyPlanChangeLocally,
  applyRegistryChangeLocally,
  createEmptyRegistrySnapshot,
  validatePlanTasksDag,
  type PlanRevision,
  type RegistrySnapshot,
} from '../registry/ipc.ts';
import {
  applyLoadedExecutionPreview,
  buildOperatorApprovalRequest,
  canSubmitApprovalDraft,
  classifyTaskApprovalDisposition,
  compileSamplePreviewForDevMode,
   computeInitialSelectedTaskIds,
  createInitialApprovalFormDraft,
  DEFAULT_EXECUTION_BUDGETS,
  desktop_approve_plan,
  desktop_plan_approvals,
  desktop_plan_execution_preview,
  isTaskWriteScopeContainedInAllowlist,
  resolveAuthoritativeBindingForApproval,
  setApprovalReviewConfirmed,
  syncApprovalDraftWithContext,
  toggleApprovalTaskSelection,
  updateApprovalDraftField,
  type PlanExecutionPreview,
} from './ipc.ts';

const VALID_THREAD_UUID = '550e8400-e29b-41d4-a716-446655440000';
const ALT_THREAD_UUID = '6ba7b810-9dad-11d1-80b4-00c04fd430c8';
const VALID_COMMIT_SHA = 'A1B2C3D4E5F60718293A4B5C6D7E8F9012345678';

function makeFixtureRegistry(): RegistrySnapshot {
  let snap = createEmptyRegistrySnapshot();
  snap = applyRegistryChangeLocally(snap, 0, {
    op: 'upsert_tool',
    tool: {
      id: 'tool-agy',
      label: 'Antigravity CLI',
      adapter: 'antigravity',
      executable_path: '/run/current-system/sw/bin/agy',
      enabled: true,
    },
  });
  snap = applyRegistryChangeLocally(snap, 1, {
    op: 'upsert_tool',
    tool: {
      id: 'tool-oc',
      label: 'OpenCode v2',
      adapter: 'opencode_v2',
      executable_path: '/run/current-system/sw/bin/opencode',
      enabled: true,
    },
  });
  snap = applyRegistryChangeLocally(snap, 2, {
    op: 'upsert_tool',
    tool: {
      id: 'tool-codex',
      label: 'Native Codex',
      adapter: 'codex_cli',
      executable_path: '/run/current-system/sw/bin/codex',
      enabled: true,
    },
  });
  snap = applyRegistryChangeLocally(snap, 3, {
    op: 'upsert_models',
    models: [
      {
        id: 'model-argon',
        tool_id: 'tool-agy',
        label: 'Gemini 4 Argon High',
        model_arg: 'gemini-4-argon-high',
        efforts: ['high'],
        allow_no_effort: false,
      },
      {
        id: 'model-deepseek',
        tool_id: 'tool-oc',
        label: 'DeepSeek v4.1 Flash',
        model_arg: 'opencode-go/deepseek-v4.1-flash',
        efforts: ['max'],
        allow_no_effort: false,
      },
      {
        id: 'model-codex',
        tool_id: 'tool-codex',
        label: 'GPT-6 Astra',
        model_arg: 'gpt-6-astra',
        efforts: ['high', 'max'],
        allow_no_effort: false,
      },
    ],
  });
  snap = applyRegistryChangeLocally(snap, 4, {
    op: 'upsert_profile',
    profile: {
      id: 'profile-m4',
      name: 'M4 Workflow Profile',
      assignments: [
        {
          role: 'coder',
          difficulty: 'hard',
          choice: { model_id: 'model-argon', effort: 'high' },
        },
        {
          role: 'coder',
          difficulty: 'easy',
          choice: { model_id: 'model-deepseek', effort: 'max' },
        },
        {
          role: 'reviewer',
          difficulty: null,
          choice: { model_id: 'model-codex', effort: 'high' },
        },
      ],
    },
  });
  return snap;
}

function makePublishedPlanAndPreview(registry: RegistrySnapshot): {
  plan: PlanRevision;
  preview: PlanExecutionPreview;
} {
  const saved = applyPlanChangeLocally(
    [],
    0,
    {
      op: 'save_draft',
      plan_id: 'plan-alpha',
      binding_id: 'bnd_live_alpha',
      profile_id: 'profile-m4',
      profile_revision: registry.revision,
      summary: 'Implement operator approval slice',
      tasks: [
        {
          id: 'task-root-agy',
          title: 'Core authority slice',
          instruction: 'Implement authority wiring in src-tauri/src/main.rs',
          role: 'coder',
          difficulty: 'hard',
          depends_on: [],
          allowed_write_paths: ['apps/desktop/src-tauri/src/main.rs'],
        },
        {
          id: 'task-root-oc',
          title: 'UI styles',
          instruction: 'Add compact plans.css rules',
          role: 'coder',
          difficulty: 'easy',
          depends_on: [],
          allowed_write_paths: ['apps/desktop/src/pages/plans.css'],
        },
        {
          id: 'task-dep-agy',
          title: 'Follow-up dependent worker task',
          instruction: 'Refine approval summary rendering after root task',
          role: 'coder',
          difficulty: 'hard',
          depends_on: ['task-root-agy'],
          allowed_write_paths: ['apps/desktop/src/pages/plans.ts'],
        },
        {
          id: 'task-native-review',
          title: 'Native Codex review',
          instruction: 'Review completed implementation in native Codex',
          role: 'reviewer',
          difficulty: 'medium',
          depends_on: ['task-dep-agy'],
          allowed_write_paths: ['docs/ROADMAP.md'],
        },
      ],
    },
    1_700_000_000_000
  );

  const published = applyPlanChangeLocally(
    saved.revisions,
    saved.updated.revision,
    {
      op: 'publish',
      plan_id: 'plan-alpha',
      revision: saved.updated.revision,
    },
    1_700_000_005_000
  ).updated;

  const preview = compileSamplePreviewForDevMode(published, registry);
  return { plan: published, preview };
}

function makeLiveBindings(threadUuid: string = VALID_THREAD_UUID): BindingSummary[] {
  return [
    {
      binding_id: 'bnd_live_alpha',
      thread_uuid: threadUuid,
      label: 'Operator Thread Alpha',
      is_local_demo: false,
    },
    {
      binding_id: 'bnd_demo_local',
      thread_uuid: VALID_THREAD_UUID,
      label: 'Local demo',
      is_local_demo: true,
    },
  ];
}

function makeLiveAttachment(overrides: Partial<Attachment> = {}): Attachment {
  return {
    mode: 'runtime',
    connection: 'connected',
    state_dir: '/home/operator/.local/share/threadlane/runtime',
    message: null,
    updated_at_ms: 1_700_000_000_000,
    ...overrides,
  };
}

describe('operator plan approval controls', () => {
  it('selects only root headless tasks initially and classifies dependent and native routes honestly', () => {
    const registry = makeFixtureRegistry();
    const { plan, preview } = makePublishedPlanAndPreview(registry);

    // Preview DTO never leaks executable_path.
    const serializedPreview = JSON.stringify(preview);
    assert.equal(serializedPreview.includes('executable_path'), false);
    assert.equal(serializedPreview.includes('/run/current-system/sw/bin/agy'), false);

    const initialIds = computeInitialSelectedTaskIds(preview);
    assert.deepEqual(initialIds, ['task-root-agy', 'task-root-oc']);

    const draft = applyLoadedExecutionPreview(
      createInitialApprovalFormDraft(plan.plan_id, plan.revision, plan.binding_id),
      preview,
      false
    );
    assert.deepEqual(draft.selectedTaskIds, ['task-root-agy', 'task-root-oc']);
    assert.equal(
      draft.approvedWritePathsText,
      'apps/desktop/src-tauri/src/main.rs, apps/desktop/src/pages/plans.css'
    );

    const depDisposition = classifyTaskApprovalDisposition(preview.tasks[2]!);
    assert.equal(depDisposition.kind, 'dependent_headless');
    assert.equal(depDisposition.defaultSelected, false);
    assert.match(depDisposition.note, /Blocked pending upstream inputs/i);

    const nativeDisposition = classifyTaskApprovalDisposition(preview.tasks[3]!);
    assert.equal(nativeDisposition.kind, 'native_route');
    assert.equal(nativeDisposition.defaultSelected, false);
    assert.match(nativeDisposition.note, /Manual native Codex conversation route/i);
  });

  it('invalidates review confirmation checkbox on any form edit, binding thread change, registry change, or plan supersede', () => {
    const registry = makeFixtureRegistry();
    const { plan, preview } = makePublishedPlanAndPreview(registry);
    const bindings = makeLiveBindings();

    let draft = applyLoadedExecutionPreview(
      createInitialApprovalFormDraft(plan.plan_id, plan.revision, plan.binding_id),
      preview,
      false
    );
    draft = updateApprovalDraftField(draft, 'repositoryPath', '/home/operator/threadlane');
    draft = updateApprovalDraftField(draft, 'baseCommit', VALID_COMMIT_SHA);

    // Before confirming checkbox, submission is blocked.
    assert.equal(canSubmitApprovalDraft(draft, bindings), false);
    assert.throws(
      () => buildOperatorApprovalRequest(draft, bindings),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'review_confirmation_required'
    );

    // Confirm review checkbox for the exact preview + form subject + live runtime context.
    const liveAttachment = makeLiveAttachment();
    const confirmedDraft = setApprovalReviewConfirmed(
      draft,
      true,
      bindings,
      liveAttachment,
      'bnd_live_alpha'
    );
    assert.equal(confirmedDraft.reviewConfirmed, true);
    assert.equal(typeof confirmedDraft.confirmedSubjectSignature, 'string');
    assert.equal(
      canSubmitApprovalDraft(confirmedDraft, bindings, liveAttachment, 'bnd_live_alpha'),
      true
    );

    // 1. Editing any form field immediately clears reviewConfirmed and blocks submission.
    const fieldEdits: Array<[ Parameters<typeof updateApprovalDraftField>[1], string ]> = [
      ['repositoryPath', '/home/operator/other-repo'],
      ['baseCommit', '1111111111111111111111111111111111111111'],
      ['approvedWritePathsText', 'apps/desktop/'],
      ['maxParallelText', '4'],
      ['timeoutMsText', '120000'],
      ['maxLineBytesText', '131072'],
      ['maxStdoutBytesText', '2097152'],
      ['maxStderrBytesText', '131072'],
      ['maxEventsText', '512'],
    ];
    for (const [field, nextValue] of fieldEdits) {
      const edited = updateApprovalDraftField(confirmedDraft, field, nextValue);
      assert.equal(
        edited.reviewConfirmed,
        false,
        `editing ${field} must clear reviewConfirmed`
      );
      assert.equal(edited.confirmedSubjectSignature, null);
      assert.equal(
        canSubmitApprovalDraft(edited, bindings, liveAttachment, 'bnd_live_alpha'),
        false
      );
    }

    // 2. Toggling task selection immediately clears reviewConfirmed.
    const toggledTask = toggleApprovalTaskSelection(confirmedDraft, 'task-dep-agy', true);
    assert.equal(toggledTask.reviewConfirmed, false);
    assert.equal(toggledTask.confirmedSubjectSignature, null);
    assert.equal(
      canSubmitApprovalDraft(toggledTask, bindings, liveAttachment, 'bnd_live_alpha'),
      false
    );

    // 3. Changing authoritative thread UUID, removing the binding, or changing attachment/selection on background poll invalidates confirmation while preserving form draft inputs.
    const rotatedBindings = makeLiveBindings(ALT_THREAD_UUID);
    assert.equal(
      canSubmitApprovalDraft(confirmedDraft, rotatedBindings, liveAttachment, 'bnd_live_alpha'),
      false
    );
    const syncedAfterThreadChange = syncApprovalDraftWithContext(
      confirmedDraft,
      plan,
      registry,
      rotatedBindings,
      liveAttachment,
      'bnd_live_alpha'
    );
    assert.equal(syncedAfterThreadChange.reviewConfirmed, false);
    assert.equal(syncedAfterThreadChange.confirmedSubjectSignature, null);
    assert.equal(syncedAfterThreadChange.repositoryPath, '/home/operator/threadlane');
    assert.equal(syncedAfterThreadChange.baseCommit, VALID_COMMIT_SHA);

    const runtimeContextVariations: Array<{
      label: string;
      bindings: BindingSummary[];
      attachment: Attachment;
      selectedBindingId: string | null;
    }> = [
      {
        label: 'binding removed from snapshot',
        bindings: [],
        attachment: liveAttachment,
        selectedBindingId: 'bnd_live_alpha',
      },
      {
        label: 'runtime connection offline',
        bindings,
        attachment: makeLiveAttachment({ connection: 'disconnected' }),
        selectedBindingId: 'bnd_live_alpha',
      },
      {
        label: 'attachment switched to demo mode',
        bindings,
        attachment: makeLiveAttachment({ mode: 'demo' }),
        selectedBindingId: 'bnd_live_alpha',
      },
      {
        label: 'attachment switched to unconfigured mode',
        bindings,
        attachment: makeLiveAttachment({
          mode: 'unconfigured',
          connection: 'unconfigured',
          state_dir: null,
        }),
        selectedBindingId: 'bnd_live_alpha',
      },
      {
        label: 'attached runtime state_dir path changed',
        bindings,
        attachment: makeLiveAttachment({
          state_dir: '/home/operator/.local/share/threadlane/other-runtime',
        }),
        selectedBindingId: 'bnd_live_alpha',
      },
      {
        label: 'selected binding changed in snapshot',
        bindings,
        attachment: liveAttachment,
        selectedBindingId: 'bnd_other_selected',
      },
    ];

    for (const ctxCase of runtimeContextVariations) {
      assert.equal(
        canSubmitApprovalDraft(
          confirmedDraft,
          ctxCase.bindings,
          ctxCase.attachment,
          ctxCase.selectedBindingId
        ),
        false,
        `canSubmitApprovalDraft must be false when ${ctxCase.label}`
      );
      const syncedCtx = syncApprovalDraftWithContext(
        confirmedDraft,
        plan,
        registry,
        ctxCase.bindings,
        ctxCase.attachment,
        ctxCase.selectedBindingId
      );
      assert.equal(
        syncedCtx.reviewConfirmed,
        false,
        `syncApprovalDraftWithContext must clear reviewConfirmed when ${ctxCase.label}`
      );
      assert.equal(syncedCtx.confirmedSubjectSignature, null);
      assert.equal(syncedCtx.repositoryPath, '/home/operator/threadlane');
      assert.equal(syncedCtx.baseCommit, VALID_COMMIT_SHA);
    }

    // 4. Advancing registry revision invalidates and clears prior preview while keeping form draft inputs.
    const newerRegistry = applyRegistryChangeLocally(registry, registry.revision, {
      op: 'set_default_profile',
      profile_id: 'profile-m4',
    });
    const syncedAfterRegistryBump = syncApprovalDraftWithContext(
      confirmedDraft,
      plan,
      newerRegistry,
      bindings,
      liveAttachment,
      'bnd_live_alpha'
    );
    assert.equal(syncedAfterRegistryBump.preview, null);
    assert.equal(syncedAfterRegistryBump.reviewConfirmed, false);
    assert.equal(syncedAfterRegistryBump.repositoryPath, '/home/operator/threadlane');
    assert.equal(syncedAfterRegistryBump.baseCommit, VALID_COMMIT_SHA);
    assert.equal(
      canSubmitApprovalDraft(syncedAfterRegistryBump, bindings, liveAttachment, 'bnd_live_alpha'),
      false
    );

    // 5. Superseding the plan revision clears prior preview and disables confirmation.
    const supersededPlan: PlanRevision = {
      ...plan,
      status: 'superseded',
    };
    const syncedAfterSupersede = syncApprovalDraftWithContext(
      confirmedDraft,
      supersededPlan,
      registry,
      bindings,
      liveAttachment,
      'bnd_live_alpha'
    );
    assert.equal(syncedAfterSupersede.preview, null);
    assert.equal(syncedAfterSupersede.reviewConfirmed, false);
    assert.equal(
      canSubmitApprovalDraft(syncedAfterSupersede, bindings, liveAttachment, 'bnd_live_alpha'),
      false
    );
  });

  it('builds exact OperatorApprovalRequest with captured preview digest/revision and enforces write scope & binding rules', () => {
    const registry = makeFixtureRegistry();
    const { plan, preview } = makePublishedPlanAndPreview(registry);
    const bindings = makeLiveBindings();

    let draft = applyLoadedExecutionPreview(
      createInitialApprovalFormDraft(plan.plan_id, plan.revision, plan.binding_id),
      preview,
      false
    );
    draft = updateApprovalDraftField(draft, 'repositoryPath', '/home/operator/threadlane');
    draft = updateApprovalDraftField(draft, 'baseCommit', VALID_COMMIT_SHA);
    draft = updateApprovalDraftField(draft, 'approvedWritePathsText', 'apps/desktop/');
    draft = updateApprovalDraftField(draft, 'maxParallelText', '3');
    draft = setApprovalReviewConfirmed(draft, true, bindings);

    const request = buildOperatorApprovalRequest(draft, bindings);
    assert.deepEqual(request, {
      binding_id: 'bnd_live_alpha',
      thread_uuid: VALID_THREAD_UUID,
      plan_id: 'plan-alpha',
      plan_revision: 1,
      expected_registry_revision: preview.registry_revision,
      expected_plan_digest: preview.plan_digest,
      repository_path: '/home/operator/threadlane',
      base_commit: VALID_COMMIT_SHA.toLowerCase(),
      approved_task_ids: ['task-root-agy', 'task-root-oc'],
      approved_write_paths: ['apps/desktop/'],
      max_parallel: 3,
      budgets: { ...DEFAULT_EXECUTION_BUDGETS },
    });

    // Componentwise write-scope containment checks.
    assert.equal(
      isTaskWriteScopeContainedInAllowlist('apps/desktop/src/main.ts', ['apps/desktop/']),
      true
    );
    assert.equal(
      isTaskWriteScopeContainedInAllowlist('apps/desktop/src/main.ts', ['apps/desktop']),
      false
    );
    assert.equal(
      isTaskWriteScopeContainedInAllowlist('apps/desktop-other/file.ts', ['apps/desktop/']),
      false
    );

    // Narrowing write paths so a selected task is uncovered must fail validation.
    const uncoveredDraft = updateApprovalDraftField(
      draft,
      'approvedWritePathsText',
      'apps/desktop/src-tauri/src/main.rs'
    );
    assert.throws(
      () => setApprovalReviewConfirmed(uncoveredDraft, true, bindings),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'unapproved_task_write_scope'
    );

    // Demo bindings and missing bindings must be rejected by authoritative binding resolution.
    const demoRes = resolveAuthoritativeBindingForApproval(bindings, 'bnd_demo_local');
    assert.equal(demoRes.ok, false);
    if (!demoRes.ok) {
      assert.equal(demoRes.code, 'demo_binding_forbidden');
    }

    const missingRes = resolveAuthoritativeBindingForApproval(bindings, 'bnd_unknown');
    assert.equal(missingRes.ok, false);
    if (!missingRes.ok) {
      assert.equal(missingRes.code, 'binding_not_found');
    }

    // Aligned directory-scope contract across plan task validation and operator approval allowlists.
    for (const validScope of [
      'src/dir/',
      'crates/threadlane-plans/',
      'apps/desktop/src/approval/',
      'apps/desktop/src/pages/plans.ts',
    ]) {
      validatePlanTasksDag([
        {
          id: 't1',
          title: 'Valid scope task',
          instruction: 'Check scope alignment',
          role: 'coder',
          difficulty: 'easy',
          depends_on: [],
          allowed_write_paths: [validScope],
        },
      ]);
    }

    for (const badScope of [
      '/',
      'src//',
      'src//lib.rs',
      '/etc/nixos/configuration.nix',
      '../outside/file.rs',
      'src/../lib.rs',
      './src/lib.rs',
      'src/.',
      'src\\lib.rs',
      'src/*.rs',
      '.git/',
      '.git/config',
      'AGENTS.md',
      '.codex/',
      '.codex/config.toml',
    ]) {
      assert.throws(
        () =>
          validatePlanTasksDag([
            {
              id: 't1',
              title: 'Invalid scope task',
              instruction: 'Check scope rejection',
              role: 'coder',
              difficulty: 'easy',
              depends_on: [],
              allowed_write_paths: [badScope],
            },
          ]),
        (err: unknown) =>
          typeof err === 'object' &&
          err !== null &&
          (err as { code?: string }).code === 'invalid_scope',
        `expected invalid_scope for plan write path "${badScope}"`
      );

      const badOpDraft = updateApprovalDraftField(draft, 'approvedWritePathsText', badScope);
      assert.throws(
        () => setApprovalReviewConfirmed(badOpDraft, true, bindings),
        (err: unknown) =>
          typeof err === 'object' &&
          err !== null &&
          (err as { code?: string }).code === 'invalid_write_scope',
        `expected invalid_write_scope for operator write path "${badScope}"`
      );
    }
  });

  it('publishing a plan grants no execution approval and browser modes never fabricate approval artifacts', async () => {
    const registry = makeFixtureRegistry();
    const { plan, preview } = makePublishedPlanAndPreview(registry);
    const bindings = makeLiveBindings();

    // A freshly published plan has no approval preview or confirmation by default.
    const initialDraft = createInitialApprovalFormDraft(
      plan.plan_id,
      plan.revision,
      plan.binding_id
    );
    assert.equal(initialDraft.preview, null);
    assert.equal(initialDraft.reviewConfirmed, false);
    assert.equal(canSubmitApprovalDraft(initialDraft, bindings), false);

    // Sample preview fixtures (browser DEV mode) are strictly read-only and reject approval.
    let sampleDraft = applyLoadedExecutionPreview(initialDraft, preview, true);
    sampleDraft = updateApprovalDraftField(
      sampleDraft,
      'repositoryPath',
      '/home/operator/threadlane'
    );
    sampleDraft = updateApprovalDraftField(sampleDraft, 'baseCommit', VALID_COMMIT_SHA);
    assert.equal(canSubmitApprovalDraft(sampleDraft, bindings), false);
    assert.throws(
      () => setApprovalReviewConfirmed(sampleDraft, true, bindings),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'preview_read_only'
    );

    // Without Tauri runtime in ordinary environment, IPC calls fail with bridge_disconnected.
    await assert.rejects(
      () => desktop_plan_execution_preview({ planId: 'plan-alpha', revision: 1 }),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'bridge_disconnected'
    );
    await assert.rejects(
      () => desktop_plan_approvals({ bindingId: 'bnd_live_alpha' }),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'bridge_disconnected'
    );
    await assert.rejects(
      () =>
        desktop_approve_plan({
          request: {
            binding_id: 'bnd_live_alpha',
            thread_uuid: VALID_THREAD_UUID,
            plan_id: 'plan-alpha',
            plan_revision: 1,
            expected_registry_revision: registry.revision,
            expected_plan_digest: preview.plan_digest,
            repository_path: '/home/operator/threadlane',
            base_commit: VALID_COMMIT_SHA.toLowerCase(),
            approved_task_ids: ['task-root-agy'],
            approved_write_paths: ['apps/desktop/src-tauri/src/main.rs'],
            max_parallel: 1,
            budgets: { ...DEFAULT_EXECUTION_BUDGETS },
          },
        }),
      (err: unknown) =>
        typeof err === 'object' &&
        err !== null &&
        (err as { code?: string }).code === 'bridge_disconnected'
    );
  });
});
