import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { ApprovalSummary } from '../approval/types.ts';
import { AppIpcError } from '../registry/ipc.ts';
import type { RegistrySnapshot } from '../registry/types.ts';
import {
  desktop_approved_session,
  desktop_approved_task,
  desktop_start_approved_session,
} from './ipc.ts';
import {
  buildHostRequestKey,
  canConfigureHostNotifications,
  canStartLocalSession,
  classifyHostError,
  codexToolChoicesFromRegistry,
  emptyHostNotificationState,
  enabledHostCodexTools,
  formatHostStatusLabel,
  formatTaskStatusLabel,
  formatTaskTelemetry,
  hostNotificationPolicyText,
  hostSummaryMatchesContext,
  hostTaskMatchesContext,
  isCurrentHostRequest,
  isHostSummaryReady,
  MAX_VISIBLE_APPROVED_TASKS,
  nextVisibleApprovedTaskCount,
  normalizeApprovedTaskIds,
  notificationToolIdForStart,
  resolveHostNotificationState,
  taskReviewNote,
  workerTelemetry,
} from './session-logic.ts';
import type {
  HostExecutionGates,
  HostPanelContext,
  HostSessionSummary,
  TaskStateSnapshot,
  WorkerSnapshot,
} from './types.ts';

const APPROVAL: ApprovalSummary = {
  approval_id: 'apr_0123456789abcdef0123456789abcdef',
  subject_digest: 'a'.repeat(64),
  policy_version: 1,
  binding_id: 'bind-alpha',
  thread_uuid: '550e8400-e29b-41d4-a716-446655440000',
  plan_id: 'plan-alpha',
  plan_revision: 2,
  profile_id: 'profile-m4',
  registry_revision: 7,
  plan_digest: 'b'.repeat(64),
  repository_path: '/tmp/threadlane-repo',
  base_commit: 'c'.repeat(40),
  approved_task_ids: ['task-root', 'task-native'],
  approved_write_paths: ['apps/desktop/src/host/'],
  max_parallel: 2,
  worker_profile_count: 1,
  native_route_count: 1,
  created_at_ms: 1_730_000_000_000,
};

const CONTEXT: HostPanelContext = {
  bindingId: 'bind-alpha',
  threadUuid: '550e8400-e29b-41d4-a716-446655440000',
  connected: true,
  registryRevision: 7,
  codexTools: [{ id: 'tool-codex', label: 'Native Codex', enabled: true }],
};

const READY_GATES: HostExecutionGates = {
  supervisor_authenticated_ready: true,
  workers_auto_started_on_boot: false,
  root_tasks_only: true,
  dependent_tasks_blocked: true,
  native_routes_manual_only: true,
  worker_completed_means_ready_for_review: true,
  auto_accept_or_merge_enabled: false,
};

function makeSummary(
  overrides: Partial<HostSessionSummary> = {}
): HostSessionSummary {
  return {
    approval_id: APPROVAL.approval_id,
    subject_digest: APPROVAL.subject_digest,
    binding_id: APPROVAL.binding_id,
    thread_uuid: APPROVAL.thread_uuid,
    plan_id: APPROVAL.plan_id,
    plan_revision: APPROVAL.plan_revision,
    session_binding_id: 'host-session-1',
    phase: 'ready',
    worker_profile_count: 1,
    native_route_count: 1,
    root_worker_task_ids: ['task-root'],
    dependent_task_ids: [],
    native_route_task_ids: ['task-native'],
    gates: { ...READY_GATES },
    last_error: null,
    notifications: {
      enabled: false,
      executable_digest: null,
      timeout_ms: null,
    },
    ...overrides,
  };
}

function makeWorker(overrides: Partial<WorkerSnapshot> = {}): WorkerSnapshot {
  return {
    untrusted_data: true,
    worker_id: 'wrk_0123456789abcdef0123456789abcdef',
    run_id: 'run_0123456789abcdef0123456789abcdef',
    binding_id: APPROVAL.binding_id,
    revision: 2,
    status: 'completed',
    profile_id: 'profile-m4',
    profile_digest: 'd'.repeat(64),
    provider: 'opencode_v2',
    requested_model: 'opencode-go/deepseek-v4.1-flash',
    requested_effort: 'max',
    observed_model: 'opencode-go/deepseek-v4.1-flash',
    observed_effort: 'max',
    instruction: 'Implement the scoped host session panel.',
    claim_id: 'wclm_0123456789abcdef0123456789abcdef',
    attempt_count: 1,
    worktree: {
      run_worktree_id: 'run-worktree-1',
      run_worktree_path: '/tmp/threadlane-work/run-1',
      run_branch_name: 'threadlane/run-1',
      task_worktree_id: 'task-worktree-1',
      task_worktree_path: '/tmp/threadlane-work/task-1',
      task_branch_name: 'threadlane/task-1',
      base_commit: APPROVAL.base_commit,
      head_commit: null,
    },
    progress: {
      durable_progress_events: 3,
      coalesced_progress_events: 0,
      dropped_live_events: 0,
      last_event_kind: 'assistant_message',
      last_summary: 'Editing the approved session panel.',
      last_event_at_ms: 1_730_000_001_000,
    },
    outcome: null,
    created_at_ms: 1_730_000_000_000,
    updated_at_ms: 1_730_000_001_000,
    started_at_ms: 1_730_000_000_500,
    completed_at_ms: 1_730_000_001_000,
    cancel_requested_at_ms: null,
    cancel_reason: null,
    ...overrides,
  };
}

function makeTask(
  overrides: Partial<TaskStateSnapshot> = {}
): TaskStateSnapshot {
  return {
    approval_id: APPROVAL.approval_id,
    subject_digest: APPROVAL.subject_digest,
    binding_id: APPROVAL.binding_id,
    thread_uuid: APPROVAL.thread_uuid,
    session_binding_id: 'host-session-1',
    task_id: 'task-root',
    task_digest: 'e'.repeat(64),
    adapter: 'opencode_v2',
    provider: 'opencode_v2',
    depends_on: [],
    profile_id: 'profile-m4',
    command_key: 'command-key-root',
    status: 'ready_for_review',
    run_id: 'run_0123456789abcdef0123456789abcdef',
    worker_id: 'wrk_0123456789abcdef0123456789abcdef',
    worker: makeWorker(),
    accepted: false,
    ...overrides,
  };
}

describe('approved local-session readiness gate', () => {
  it('cannot fake Ready from phase or session binding alone', () => {
    assert.equal(isHostSummaryReady(makeSummary()), true);

    const gateFalse = makeSummary({
      gates: { ...READY_GATES, supervisor_authenticated_ready: false },
    });
    assert.equal(isHostSummaryReady(gateFalse), false);
    assert.notEqual(formatHostStatusLabel(gateFalse), 'Ready');

    const phaseFalse = makeSummary({
      phase: 'bootstrapping',
      gates: { ...READY_GATES, supervisor_authenticated_ready: true },
    });
    assert.equal(isHostSummaryReady(phaseFalse), false);
    assert.notEqual(formatHostStatusLabel(phaseFalse), 'Ready');

    const stoppedWithSessionBinding = makeSummary({
      phase: 'stopped',
      session_binding_id: 'host-session-1',
    });
    assert.equal(isHostSummaryReady(stoppedWithSessionBinding), false);
    assert.notEqual(formatHostStatusLabel(stoppedWithSessionBinding), 'Ready');
  });
});

describe('approved local-session context isolation', () => {
  it('ignores cross-binding, cross-thread, disconnected, and stale-epoch responses', () => {
    assert.equal(hostSummaryMatchesContext(makeSummary(), APPROVAL, CONTEXT), true);
    assert.equal(
      hostSummaryMatchesContext(
        makeSummary({ session_binding_id: 'different-host-session' }),
        APPROVAL,
        CONTEXT
      ),
      true
    );

    const crossBinding = makeSummary({
      binding_id: 'bind-beta',
      session_binding_id: 'bind-alpha',
    });
    assert.equal(hostSummaryMatchesContext(crossBinding, APPROVAL, CONTEXT), false);

    const crossThread = makeSummary({
      thread_uuid: '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
    });
    assert.equal(hostSummaryMatchesContext(crossThread, APPROVAL, CONTEXT), false);
    assert.equal(
      hostSummaryMatchesContext(makeSummary(), APPROVAL, {
        ...CONTEXT,
        connected: false,
      }),
      false
    );

    const key = buildHostRequestKey(APPROVAL, CONTEXT);
    const ticket = { epoch: 7, key };
    assert.equal(isCurrentHostRequest(ticket, 7, key), true);
    assert.equal(isCurrentHostRequest(ticket, 8, key), false);
    assert.notEqual(
      buildHostRequestKey(
        { ...APPROVAL, subject_digest: 'f'.repeat(64) },
        CONTEXT
      ),
      key
    );
    assert.notEqual(
      buildHostRequestKey(APPROVAL, { ...CONTEXT, connected: false }),
      key
    );
    assert.equal(
      isCurrentHostRequest(
        ticket,
        7,
        buildHostRequestKey(APPROVAL, { ...CONTEXT, bindingId: 'bind-beta' })
      ),
      false
    );

    const crossTask = makeTask({
      binding_id: 'bind-beta',
      session_binding_id: 'bind-alpha',
    });
    assert.equal(hostTaskMatchesContext(crossTask, APPROVAL, CONTEXT), false);
    assert.equal(
      hostTaskMatchesContext(
        makeTask({ session_binding_id: 'different-host-session' }),
        APPROVAL,
        CONTEXT
      ),
      true
    );
    assert.equal(
      hostTaskMatchesContext(makeTask(), APPROVAL, {
        ...CONTEXT,
        threadUuid: '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
      }),
      false
    );
  });
});

describe('approved local-session start authority', () => {
  it('denies browser and mismatched contexts before any native invocation', async () => {
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: false,
        busy: false,
      }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: true,
      }),
      false
    );
    assert.equal(
      canStartLocalSession(
        APPROVAL,
        { ...CONTEXT, connected: false },
        { nativeBridgeAvailable: true, busy: false }
      ),
      false
    );
    assert.equal(
      canStartLocalSession(
        APPROVAL,
        { ...CONTEXT, bindingId: 'bind-beta' },
        { nativeBridgeAvailable: true, busy: false }
      ),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: false,
        summary: makeSummary(),
      }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: false,
        summary: makeSummary({ phase: 'stopped' }),
      }),
      true
    );

    await assert.rejects(
      () => desktop_start_approved_session({ approvalId: APPROVAL.approval_id }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'bridge_disconnected'
    );
    await assert.rejects(
      () => desktop_approved_session({ approvalId: APPROVAL.approval_id }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'bridge_disconnected'
    );
    await assert.rejects(
      () =>
        desktop_approved_task({
          approvalId: APPROVAL.approval_id,
          taskId: 'task-root',
        }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'bridge_disconnected'
    );
  });
});

describe('approved task telemetry honesty', () => {
  it('does not invent requested or observed model/effort values', () => {
    assert.deepEqual(workerTelemetry(null), {
      requestedModel: null,
      requestedEffort: null,
      observedModel: null,
      observedEffort: null,
    });

    const noWorker = makeTask({ worker: null });
    assert.deepEqual(formatTaskTelemetry(noWorker), {
      requested: 'Not reported',
      observed: 'Not reported',
    });

    const requestedOnly = makeWorker({
      requested_model: 'requested-model-a',
      requested_effort: 'high',
      observed_model: null,
      observed_effort: null,
    });
    const requestedTask = makeTask({ worker: requestedOnly });
    assert.deepEqual(workerTelemetry(requestedOnly), {
      requestedModel: 'requested-model-a',
      requestedEffort: 'high',
      observedModel: null,
      observedEffort: null,
    });
    const labels = formatTaskTelemetry(requestedTask);
    assert.equal(labels.requested, 'requested-model-a · effort high');
    assert.equal(labels.observed, 'Not reported');
    assert.ok(!labels.observed.includes('requested-model-a'));

    const observedTask = makeTask({
      worker: makeWorker({
        observed_model: 'observed-model-b',
        observed_effort: 'max',
      }),
    });
    assert.equal(
      formatTaskTelemetry(observedTask).observed,
      'observed-model-b · effort max'
    );
  });
});

describe('approved task completion semantics', () => {
  it('keeps completion and unknown states review-only, never accepted', () => {
    const completion = makeTask({ status: 'ready_for_review', accepted: false });
    const completionLabel = formatTaskStatusLabel(completion.status);
    assert.equal(completionLabel, 'Ready for review');
    assert.equal(
      /accepted|merged|applied|succeeded/i.test(completionLabel),
      false
    );
    assert.equal(
      taskReviewNote(completion),
      'Review pending; not accepted, merged, or applied.'
    );

    const incorrectlyAccepted = makeTask({
      status: 'ready_for_review',
      accepted: true,
    });
    assert.equal(formatTaskStatusLabel(incorrectlyAccepted.status), 'Ready for review');
    assert.equal(
      taskReviewNote(incorrectlyAccepted),
      'Review pending; not accepted, merged, or applied.'
    );

    const unknown = makeTask({ status: 'unknown' });
    assert.equal(formatTaskStatusLabel(unknown.status), 'Unknown (uncertain)');
    assert.equal(
      taskReviewNote(unknown),
      'Unknown (uncertain); not replayed or accepted.'
    );
    assert.equal(/accepted$/i.test(formatTaskStatusLabel(unknown.status)), false);
  });

  it('keeps normal backend states out of running and bounds visible task batches', () => {
    assert.deepEqual(classifyHostError({ code: 'host_not_ready', message: 'x' }), {
      kind: 'not_ready',
      message: 'The approved host is not ready yet.',
    });
    assert.deepEqual(classifyHostError({ code: 'not_started', message: 'x' }), {
      kind: 'not_started',
      message: null,
    });
    assert.deepEqual(classifyHostError({ code: 'bridge_disconnected', message: 'x' }), {
      kind: 'bridge_disconnected',
      message: 'The native desktop bridge is disconnected.',
    });

    const rawIds = Array.from({ length: 45 }, (_, index) => ` task-${index} `);
    rawIds.push('task-0');
    const normalized = normalizeApprovedTaskIds(rawIds);
    assert.equal(normalized.length, 45);
    assert.equal(normalized[0], 'task-0');
    assert.equal(MAX_VISIBLE_APPROVED_TASKS, 20);
    assert.equal(
      nextVisibleApprovedTaskCount(MAX_VISIBLE_APPROVED_TASKS, 45),
      40
    );
    assert.equal(nextVisibleApprovedTaskCount(40, 45), 45);
    assert.equal(nextVisibleApprovedTaskCount(45, 45), 45);
  });
});

const TWO_CODEX_TOOLS: HostPanelContext = {
  ...CONTEXT,
  registryRevision: 8,
  codexTools: [
    { id: 'tool-codex-a', label: 'Codex A', enabled: true },
    { id: 'tool-codex-b', label: 'Codex B', enabled: true },
    { id: 'tool-codex-disabled', label: 'Disabled Codex', enabled: false },
  ],
};

describe('approved session notification opt-in', () => {
  it('defaults unchecked, auto-selects only a single tool, and resets on context changes', () => {
    assert.deepEqual(emptyHostNotificationState(), { optIn: false, toolId: '' });
    assert.deepEqual(
      resolveHostNotificationState(emptyHostNotificationState(), CONTEXT, true),
      { optIn: false, toolId: 'tool-codex' }
    );
    assert.deepEqual(
      resolveHostNotificationState(emptyHostNotificationState(), TWO_CODEX_TOOLS, true),
      { optIn: false, toolId: '' }
    );
    assert.deepEqual(
      resolveHostNotificationState(
        { optIn: true, toolId: 'tool-codex-b' },
        TWO_CODEX_TOOLS,
        false
      ),
      { optIn: true, toolId: 'tool-codex-b' }
    );
    assert.deepEqual(
      resolveHostNotificationState(
        { optIn: true, toolId: 'tool-codex-b' },
        TWO_CODEX_TOOLS,
        true
      ),
      { optIn: false, toolId: '' }
    );
    assert.deepEqual(
      resolveHostNotificationState(
        { optIn: true, toolId: 'tool-codex-disabled' },
        TWO_CODEX_TOOLS,
        false
      ),
      { optIn: false, toolId: '' }
    );
    assert.deepEqual(
      resolveHostNotificationState(
        { optIn: true, toolId: 'tool-codex-a' },
        { ...CONTEXT, codexTools: undefined },
        false
      ),
      { optIn: false, toolId: '' }
    );
  });

  it('disables opt-in without the native bridge, live context, tools, or while running', () => {
    const allowed = {
      nativeBridgeAvailable: true,
      previewFixtureMode: false,
      contextMatches: true,
      running: false,
      busy: false,
      toolCount: 1,
    };
    assert.equal(canConfigureHostNotifications(allowed), true);
    assert.equal(
      canConfigureHostNotifications({ ...allowed, nativeBridgeAvailable: false }),
      false
    );
    assert.equal(
      canConfigureHostNotifications({ ...allowed, previewFixtureMode: true }),
      false
    );
    assert.equal(
      canConfigureHostNotifications({ ...allowed, contextMatches: false }),
      false
    );
    assert.equal(
      canConfigureHostNotifications({ ...allowed, running: true }),
      false
    );
    assert.equal(canConfigureHostNotifications({ ...allowed, busy: true }), false);
    assert.equal(canConfigureHostNotifications({ ...allowed, toolCount: 0 }), false);
  });

  it('passes only an eligible tool ID to start, never while running or unchecked', () => {
    assert.equal(
      notificationToolIdForStart(
        { optIn: true, toolId: 'tool-codex-b' },
        TWO_CODEX_TOOLS,
        false
      ),
      'tool-codex-b'
    );
    assert.equal(
      notificationToolIdForStart(
        { optIn: true, toolId: 'tool-codex-b' },
        TWO_CODEX_TOOLS,
        true
      ),
      null
    );
    assert.equal(
      notificationToolIdForStart(
        { optIn: false, toolId: 'tool-codex-b' },
        TWO_CODEX_TOOLS,
        false
      ),
      null
    );
    assert.equal(
      notificationToolIdForStart(
        { optIn: true, toolId: 'tool-codex-disabled' },
        TWO_CODEX_TOOLS,
        false
      ),
      null
    );
    assert.equal(
      notificationToolIdForStart(
        { optIn: true, toolId: 'tool-agy' },
        TWO_CODEX_TOOLS,
        false
      ),
      null
    );
    assert.equal(
      notificationToolIdForStart(
        { optIn: true, toolId: '/usr/bin/codex' },
        TWO_CODEX_TOOLS,
        false
      ),
      null
    );
  });

  it('keeps registry and approval context invalidation separate per tool/revision', () => {
    const baseKey = buildHostRequestKey(APPROVAL, CONTEXT);
    assert.notEqual(
      baseKey,
      buildHostRequestKey(APPROVAL, { ...CONTEXT, registryRevision: 8 })
    );
    assert.notEqual(
      baseKey,
      buildHostRequestKey(APPROVAL, {
        ...CONTEXT,
        codexTools: [{ id: 'tool-codex', label: 'Renamed Codex', enabled: true }],
      })
    );
    assert.notEqual(
      baseKey,
      buildHostRequestKey(APPROVAL, {
        ...CONTEXT,
        codexTools: [{ id: 'tool-other', label: 'Native Codex', enabled: true }],
      })
    );
    const staleTicket = { epoch: 4, key: baseKey };
    assert.equal(
      isCurrentHostRequest(
        staleTicket,
        4,
        buildHostRequestKey(APPROVAL, { ...CONTEXT, registryRevision: 9 })
      ),
      false
    );
  });

  it('filters registry choices to codex_cli and enabled-only UI options', () => {
    const registry: RegistrySnapshot = {
      schema_version: 1,
      revision: 12,
      tools: [
        {
          id: 'tool-codex',
          label: 'Native Codex',
          adapter: 'codex_cli',
          executable_path: '/private/bin/codex',
          enabled: true,
        },
        {
          id: 'tool-codex-disabled',
          label: 'Disabled Codex',
          adapter: 'codex_cli',
          executable_path: null,
          enabled: false,
        },
        {
          id: 'tool-agy',
          label: 'Antigravity',
          adapter: 'antigravity',
          executable_path: '/private/bin/agy',
          enabled: true,
        },
      ],
      models: [],
      profiles: [],
      default_profile_id: null,
    };
    assert.deepEqual(codexToolChoicesFromRegistry(registry), [
      { id: 'tool-codex', label: 'Native Codex', enabled: true },
      { id: 'tool-codex-disabled', label: 'Disabled Codex', enabled: false },
    ]);
    assert.deepEqual(enabledHostCodexTools(CONTEXT), [
      { id: 'tool-codex', label: 'Native Codex', enabled: true },
    ]);
    assert.deepEqual(enabledHostCodexTools(TWO_CODEX_TOOLS), [
      { id: 'tool-codex-a', label: 'Codex A', enabled: true },
      { id: 'tool-codex-b', label: 'Codex B', enabled: true },
    ]);
  });

  it('shows actual startup policy with a digest only and claims no delivery', () => {
    const digest = 'a'.repeat(64);
    const enabled = makeSummary({
      notifications: {
        enabled: true,
        executable_digest: digest,
        timeout_ms: 10_000,
      },
    });
    const text = hostNotificationPolicyText(enabled);
    assert.ok(text.includes('on at startup'));
    assert.ok(text.includes(digest.slice(0, 12)));
    assert.ok(!text.includes(digest));
    assert.ok(!text.includes('/'));
    assert.ok(text.includes('delivery not verified'));

    assert.ok(hostNotificationPolicyText(makeSummary()).includes('off at startup'));
    assert.ok(
      hostNotificationPolicyText(makeSummary({ notifications: undefined })).includes(
        'off at startup'
      )
    );
    assert.ok(
      hostNotificationPolicyText(null).includes('no startup policy reported')
    );

    const unsafeDigest = makeSummary({
      notifications: {
        enabled: true,
        executable_digest: '/private/bin/codex',
        timeout_ms: 10_000,
      },
    });
    const unsafeText = hostNotificationPolicyText(unsafeDigest);
    assert.ok(!unsafeText.includes('/private/bin/codex'));
    assert.ok(unsafeText.includes('sender digest unavailable'));
  });
});
