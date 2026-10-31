import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { ApprovalSummary } from '../approval/types.ts';
import { AppIpcError } from '../registry/ipc.ts';
import type { RegistrySnapshot } from '../registry/types.ts';
import {
  desktop_approved_session,
  desktop_approved_task,
  desktop_restart_approved_session,
  desktop_start_approved_session,
  desktop_stop_approved_session,
} from './ipc.ts';
import {
  buildHostLifecycleRequestKey,
  buildHostRequestKey,
  canConfigureHostNotifications,
  canRestartLocalSession,
  canStartLocalSession,
  canStopLocalSession,
  classifyHostError,
  codexToolChoicesFromRegistry,
  emptyHostNotificationState,
  enabledHostCodexTools,
  formatDependencyInputsGate,
  formatDependentTasksGate,
  formatHostLifecycleActionLabel,
  formatHostStatusLabel,
  formatTaskDependencyDetail,
  formatTaskEvidenceVerificationStatus,
  formatTaskIntegrationLifecycleStatus,
  formatTaskLifecycleDisplay,
  formatTaskLifecycleReceipts,
  formatTaskReviewLifecycleStatus,
  formatTaskStatusLabel,
  formatTaskStatusTone,
  formatTaskTelemetry,
  hostInstanceChanged,
  hostInstanceId,
  hostLifecycleHint,
  hostNotificationPolicyText,
  hostSummaryMatchesContext,
  hostTaskMatchesContext,
  isCurrentHostRequest,
  isHostSummaryReady,
  isTerminalTaskStatus,
  isUncertainHostErrorKind,
  isValidHostInstanceId,
  MAX_VISIBLE_APPROVED_TASKS,
  nextVisibleApprovedTaskCount,
  normalizeApprovedTaskIds,
  notificationToolIdForStart,
  resolveHostNotificationState,
  resolveTaskLifecycleProjection,
  selectTaskIdsForRead,
  shouldPollTaskSnapshot,
  syncTaskLifecycleTargets,
  taskReviewNote,
  TASK_DEPENDENCY_UNVERIFIED_NOTE,
  workerTelemetry,
  writeTaskDependencyText,
} from './session-logic.ts';
import type {
  HostExecutionGates,
  HostPanelContext,
  HostSessionSummary,
  TaskLifecycleProjection,
  TaskStateSnapshot,
  VerifiedIntegrationReceiptSummary,
  VerifiedReviewReceiptSummary,
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
    instance_id: 'hst_0123456789abcdef0123456789abcdef',
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
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: false,
        summary: makeSummary({ phase: 'stopping' }),
      }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: false,
        previewFixtureMode: true,
      }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, {
        nativeBridgeAvailable: true,
        busy: false,
        uncertain: true,
      }),
      false
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

  it('denies ordinary start for failed, not-ready, stopped, and stopping summaries', () => {
    const options = {
      nativeBridgeAvailable: true,
      previewFixtureMode: false,
      busy: false,
      uncertain: false,
    };
    const failedNotReady = makeSummary({
      phase: 'failed',
      gates: { ...READY_GATES, supervisor_authenticated_ready: false },
    });

    assert.equal(canStartLocalSession(APPROVAL, CONTEXT, options), true);

    for (const summary of [
      failedNotReady,
      makeSummary({ phase: 'failed' }),
      makeSummary({
        phase: 'ready',
        gates: { ...READY_GATES, supervisor_authenticated_ready: false },
      }),
      makeSummary({
        phase: 'stopped',
        gates: { ...READY_GATES, supervisor_authenticated_ready: false },
      }),
      makeSummary({
        phase: 'stopping',
        gates: { ...READY_GATES, supervisor_authenticated_ready: false },
      }),
    ]) {
      assert.equal(
        canStartLocalSession(APPROVAL, CONTEXT, { ...options, summary }),
        false
      );
    }

    assert.equal(
      canRestartLocalSession(APPROVAL, CONTEXT, {
        ...options,
        summary: failedNotReady,
      }),
      false
    );
  });
});

describe('host lifecycle identity and eligibility', () => {
  it('accepts only exact hst identities and rejects wrappers without the native bridge', async () => {
    const instance = 'hst_0123456789abcdef0123456789abcdef';
    assert.equal(isValidHostInstanceId(instance), true);
    for (const invalid of [
      '',
      'hst_',
      'hst_0123456789abcdef0123456789abcde',
      'hst_0123456789abcdef0123456789abcdef0',
      'hst_0123456789ABCDEF0123456789abcdef',
      'pid_0123456789abcdef0123456789abcdef',
      ' hst_0123456789abcdef0123456789abcdef',
      'hst_0123456789abcdef0123456789abcdef ',
      '/tmp/hst_0123456789abcdef0123456789abcdef',
    ]) {
      assert.equal(isValidHostInstanceId(invalid), false);
    }
    assert.equal(hostInstanceId(makeSummary()), instance);
    assert.equal(hostInstanceId(makeSummary({ instance_id: null })), null);

    await assert.rejects(
      () =>
        desktop_stop_approved_session({
          approvalId: APPROVAL.approval_id,
          expectedInstanceId: 'hst_short',
        }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'invalid_input'
    );
    await assert.rejects(
      () =>
        desktop_restart_approved_session({
          approvalId: APPROVAL.approval_id,
          expectedPriorInstanceId: '/tmp/not-an-instance',
        }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'invalid_input'
    );
    await assert.rejects(
      () =>
        desktop_stop_approved_session({
          approvalId: APPROVAL.approval_id,
          expectedInstanceId: instance,
        }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'bridge_disconnected'
    );
    await assert.rejects(
      () =>
        desktop_restart_approved_session({
          approvalId: APPROVAL.approval_id,
          expectedPriorInstanceId: instance,
          notifyCodexToolId: 'tool-codex',
        }),
      (err: unknown) =>
        err instanceof AppIpcError && err.code === 'bridge_disconnected'
    );
  });

  it('offers stop only from authenticated Ready, restart only from Stopped, and start never from Stopped', () => {
    const options = {
      nativeBridgeAvailable: true,
      previewFixtureMode: false,
      busy: false,
      uncertain: false,
    };
    const ready = makeSummary();
    const stopped = makeSummary({
      phase: 'stopped',
      gates: { ...READY_GATES, supervisor_authenticated_ready: false },
    });
    const stopping = makeSummary({
      phase: 'stopping',
      gates: { ...READY_GATES, supervisor_authenticated_ready: false },
    });

    assert.equal(canStartLocalSession(APPROVAL, CONTEXT, options), true);

    assert.equal(canStopLocalSession(APPROVAL, CONTEXT, { ...options, summary: ready }), true);
    assert.equal(
      canRestartLocalSession(APPROVAL, CONTEXT, { ...options, summary: ready }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, { ...options, summary: ready }),
      false
    );

    assert.equal(
      canRestartLocalSession(APPROVAL, CONTEXT, { ...options, summary: stopped }),
      true
    );
    assert.equal(
      canStopLocalSession(APPROVAL, CONTEXT, { ...options, summary: stopped }),
      false
    );
    assert.equal(
      canStartLocalSession(APPROVAL, CONTEXT, { ...options, summary: stopped }),
      false
    );

    for (const summary of [stopping, makeSummary({ instance_id: null })]) {
      assert.equal(
        canStartLocalSession(APPROVAL, CONTEXT, { ...options, summary }),
        false
      );
      assert.equal(
        canStopLocalSession(APPROVAL, CONTEXT, { ...options, summary }),
        false
      );
      assert.equal(
        canRestartLocalSession(APPROVAL, CONTEXT, { ...options, summary }),
        false
      );
    }

    assert.equal(
      canStopLocalSession(APPROVAL, CONTEXT, {
        ...options,
        nativeBridgeAvailable: false,
        summary: ready,
      }),
      false
    );
    assert.equal(
      canStopLocalSession(APPROVAL, CONTEXT, {
        ...options,
        busy: true,
        summary: ready,
      }),
      false
    );
    assert.equal(
      canRestartLocalSession(APPROVAL, CONTEXT, {
        ...options,
        busy: true,
        summary: stopped,
      }),
      false
    );
    assert.equal(
      canStopLocalSession(APPROVAL, CONTEXT, {
        ...options,
        previewFixtureMode: true,
        summary: ready,
      }),
      false
    );
    assert.equal(
      canRestartLocalSession(APPROVAL, CONTEXT, {
        ...options,
        previewFixtureMode: true,
        summary: stopped,
      }),
      false
    );
    assert.equal(
      canStopLocalSession(APPROVAL, CONTEXT, {
        ...options,
        uncertain: true,
        summary: ready,
      }),
      false
    );
    assert.equal(
      canStopLocalSession(
        APPROVAL,
        { ...CONTEXT, connected: false },
        { ...options, summary: ready }
      ),
      false
    );
    assert.equal(
      canRestartLocalSession(
        APPROVAL,
        { ...CONTEXT, bindingId: 'bind-beta' },
        { ...options, summary: stopped }
      ),
      false
    );

    assert.equal(formatHostStatusLabel(stopping), 'Stopping');
    assert.notEqual(formatHostStatusLabel(stopping), 'Ready');
    assert.equal(formatHostLifecycleActionLabel('start'), 'Starting');
    assert.equal(formatHostLifecycleActionLabel('stop'), 'Stopping');
    assert.equal(formatHostLifecycleActionLabel('restart'), 'Restarting');
  });

  it('drops stale lifecycle tickets across action, instance, and context changes', () => {
    const instance = 'hst_0123456789abcdef0123456789abcdef';
    const otherInstance = 'hst_ffffffffffffffffffffffffffffffff';
    const key = buildHostLifecycleRequestKey(APPROVAL, CONTEXT, 'stop', instance);
    const ticket = { epoch: 3, key };
    assert.equal(isCurrentHostRequest(ticket, 3, key), true);
    assert.equal(isCurrentHostRequest(ticket, 4, key), false);
    assert.notEqual(
      key,
      buildHostLifecycleRequestKey(APPROVAL, CONTEXT, 'restart', instance)
    );
    assert.notEqual(
      key,
      buildHostLifecycleRequestKey(APPROVAL, CONTEXT, 'stop', otherInstance)
    );
    assert.notEqual(
      key,
      buildHostLifecycleRequestKey(
        APPROVAL,
        { ...CONTEXT, bindingId: 'bind-beta' },
        'stop',
        instance
      )
    );
    assert.equal(
      isCurrentHostRequest(
        ticket,
        3,
        buildHostLifecycleRequestKey(APPROVAL, CONTEXT, 'stop', otherInstance)
      ),
      false
    );

    assert.equal(
      hostInstanceChanged(makeSummary(), makeSummary({ instance_id: otherInstance })),
      true
    );
    assert.equal(
      hostInstanceChanged(makeSummary(), makeSummary({ instance_id: instance })),
      false
    );

    assert.deepEqual(
      resolveHostNotificationState(
        { optIn: true, toolId: 'tool-codex' },
        CONTEXT,
        true
      ),
      { optIn: false, toolId: 'tool-codex' }
    );
  });

  it('maps busy and uncertain errors without claiming stopped or ready', () => {
    const notIdle = classifyHostError({ code: 'host_not_idle', message: 'raw' });
    assert.equal(notIdle.kind, 'not_idle');
    assert.ok(notIdle.message?.includes('cancellation controls'));

    const uncertain = classifyHostError({
      code: 'host_stop_outcome_uncertain',
      message: 'raw',
    });
    assert.equal(uncertain.kind, 'uncertain');
    assert.ok(uncertain.message?.includes('not reported stopped'));
    assert.equal(isUncertainHostErrorKind(uncertain.kind), true);

    const mismatch = classifyHostError({
      code: 'host_instance_mismatch',
      message: 'raw',
    });
    assert.equal(mismatch.kind, 'instance_mismatch');
    assert.equal(isUncertainHostErrorKind(mismatch.kind), true);

    const restart = classifyHostError({
      code: 'host_restart_not_allowed',
      message: 'raw',
    });
    assert.equal(restart.kind, 'restart_not_allowed');
    assert.equal(isUncertainHostErrorKind(restart.kind), false);

    const unavailable = classifyHostError({
      code: 'host_control_unavailable',
      message: 'raw',
    });
    assert.equal(unavailable.kind, 'control_unavailable');

    assert.ok(hostLifecycleHint(makeSummary(), false).includes('cancel existing'));
    assert.ok(hostLifecycleHint(makeSummary({ phase: 'stopped' }), false).includes('not replayed'));
    assert.ok(
      hostLifecycleHint(makeSummary({ phase: 'stopping' }), false).includes('disabled')
    );
    assert.ok(hostLifecycleHint(null, true).includes('uncertain'));
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

describe('approved task prepared dependency inputs', () => {
  it('labels verified inputs as ready without started or terminal meaning', () => {
    const label = formatTaskStatusLabel('dependency_inputs_ready');
    assert.equal(label, 'Ready: dependencies verified');
    assert.equal(formatTaskStatusTone('dependency_inputs_ready'), 'default');
    assert.equal(isTerminalTaskStatus('dependency_inputs_ready'), false);
    assert.equal(/started|running|accepted|merged|applied/i.test(label), false);

    assert.equal(
      formatTaskStatusLabel('dependency_inputs_unverified'),
      'Blocked: dependencies unverified'
    );
    assert.equal(formatTaskStatusTone('dependency_inputs_unverified'), 'warn');
    assert.equal(isTerminalTaskStatus('dependency_inputs_unverified'), false);

    const unverified = formatTaskDependencyDetail(
      makeTask({ status: 'dependency_inputs_unverified' })
    );
    assert.equal(unverified?.tone, 'warn');
    assert.equal(unverified?.text, TASK_DEPENDENCY_UNVERIFIED_NOTE);
    assert.ok(unverified?.text.includes('reviewed prerequisite code'));
    assert.ok(unverified?.text.includes('before starting this task'));
  });

  it('shows only a host-reported prepared base and predecessors, even after start', () => {
    const base = '0123456789abcdef0123456789abcdef01234567';
    const dependency_input = {
      input_digest: 'f'.repeat(64),
      base_commit: base,
      tree_sha: 'e'.repeat(40),
      predecessor_task_ids: ['task-root', 'task-native'],
    };
    const prepared = makeTask({
      status: 'dependency_inputs_ready',
      dependency_input,
    });
    const detail = formatTaskDependencyDetail(prepared);
    assert.ok(detail);
    assert.equal(detail.tone, 'default');
    assert.ok(detail.text.includes(`Base ${base.slice(0, 12)}…`));
    assert.equal(detail.text.includes(base), false);
    assert.ok(detail.text.includes('Prerequisites: task-root, task-native'));
    assert.ok(detail.text.includes('Base '));
    assert.ok(detail.text.includes('Ready to start.'));
    assert.equal(taskReviewNote(prepared), '');
    assert.deepEqual(Object.keys(detail).sort(), ['text', 'tone']);

    const started = formatTaskDependencyDetail(
      makeTask({ status: 'running', dependency_input })
    );
    assert.ok(started?.text.includes(`Base ${base.slice(0, 12)}…`));
    assert.equal(started?.text.includes('Ready to start.'), false);

    const readyWithoutSummary = formatTaskDependencyDetail(
      makeTask({ status: 'dependency_inputs_ready', dependency_input: null })
    );
    assert.ok(readyWithoutSummary?.text.includes('Prepared input details are unavailable'));
    assert.equal(readyWithoutSummary?.text.includes(base.slice(0, 12)), false);

    const parentsOnly = makeTask({
      status: 'not_started',
      depends_on: ['task-root'],
      dependency_input: null,
    });
    assert.equal(formatTaskDependencyDetail(parentsOnly), null);
    const clearedSink: { textContent: string | null } = {
      textContent: 'stale prepared base',
    };
    assert.equal(writeTaskDependencyText(clearedSink, parentsOnly), null);
    assert.equal(clearedSink.textContent, '');
  });

  it('writes unsafe prepared text as text and reports legacy gate support honestly', () => {
    const unsafe = '<img src=x onerror=alert(1)>';
    const task = makeTask({
      status: 'running',
      dependency_input: {
        input_digest: 'a'.repeat(64),
        base_commit: 'b'.repeat(40),
        tree_sha: 'c'.repeat(40),
        predecessor_task_ids: [unsafe, ' task-ok '],
      },
    });
    const sink: { textContent: string | null } = { textContent: '' };
    const detail = writeTaskDependencyText(sink, task);
    const rendered = sink.textContent ?? '';
    assert.equal(rendered, detail?.text ?? '');
    assert.ok(rendered.includes(unsafe));
    assert.ok(rendered.includes('task-ok'));
    assert.equal(rendered.includes('&lt;img'), false);
    assert.deepEqual(Object.keys(detail!).sort(), ['text', 'tone']);

    assert.ok(formatDependencyInputsGate(READY_GATES).includes('Legacy host'));
    assert.ok(formatDependentTasksGate(READY_GATES).includes('legacy'));

    const fresh = {
      ...READY_GATES,
      root_tasks_only: false,
      dependent_tasks_blocked: false,
      dependency_inputs_required: true,
    };
    assert.ok(
      formatDependencyInputsGate(fresh).includes('required before starting dependent tasks')
    );
    assert.ok(formatDependentTasksGate(fresh).includes('prerequisite code is reviewed and prepared'));
    assert.equal(formatDependentTasksGate(fresh).includes('Blocked'), false);
    assert.ok(
      formatDependentTasksGate({
        ...fresh,
        dependent_tasks_blocked: true,
      }).includes('Waiting for reviewed prerequisite code')
    );
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

interface MockLifecycleSink {
  textContent: string | null;
  hidden: boolean;
  attrs: Record<string, string>;
  setAttribute(name: string, value: string): void;
}

function makeLifecycleSink(): MockLifecycleSink {
  return {
    textContent: '',
    hidden: false,
    attrs: {},
    setAttribute(name: string, value: string): void {
      this.attrs[name] = value;
    },
  };
}

function makeLifecycleTargets() {
  return {
    lifecycleGroup: makeLifecycleSink(),
    executionPill: makeLifecycleSink(),
    reviewPill: makeLifecycleSink(),
    integrationPill: makeLifecycleSink(),
    evidencePill: makeLifecycleSink(),
    lifecycleReceipts: makeLifecycleSink(),
    lifecycleNote: makeLifecycleSink(),
    reviewDetail: makeLifecycleSink(),
  };
}

function makeReviewReceiptSummary(
  overrides: Partial<VerifiedReviewReceiptSummary> = {}
): VerifiedReviewReceiptSummary {
  return {
    receipt_id: 'rev_0123456789abcdef',
    subject_digest: 'b'.repeat(64),
    receipt_digest: 'c'.repeat(64),
    task_worktree_id: 'task-worktree-1',
    base_commit: APPROVAL.base_commit,
    task_head_commit: '1111222233334444555566667777888899990000',
    task_tree_sha: 'aaaa222233334444555566667777888899990000',
    diff_digest: 'd'.repeat(64),
    reviewer_id: 'operator-native',
    approved_at_ms: 1_730_000_010_000,
    ...overrides,
  };
}

function makeIntegrationReceiptSummary(
  overrides: Partial<VerifiedIntegrationReceiptSummary> = {}
): VerifiedIntegrationReceiptSummary {
  return {
    receipt_id: 'int_fedcba9876543210',
    receipt_digest: 'e'.repeat(64),
    review_receipt_id: 'rev_0123456789abcdef',
    integration_worktree_id: 'run-worktree-1',
    strategy: 'fast_forward_only',
    accepted_task_head_commit: '1111222233334444555566667777888899990000',
    integrated_target_commit: '9999888877776666555544443333222211110000',
    integrated_tree_sha: 'aaaa222233334444555566667777888899990000',
    operator_id: 'operator-native',
    integrated_at_ms: 1_730_000_020_000,
    ...overrides,
  };
}

function makeLifecycleProjection(
  overrides: Partial<TaskLifecycleProjection> = {}
): TaskLifecycleProjection {
  return {
    review_status: 'pending_review',
    integration_status: 'not_integrated',
    evidence_status: 'pending_review',
    task_checkout_present: true,
    integration_checkout_present: true,
    review_receipt: null,
    integration_receipt: null,
    verification_detail: 'Worker completed; awaiting operator review receipt.',
    ...overrides,
  };
}

describe('approved task lifecycle projection and polling', () => {
  it('projects completion-only tasks as pending review and ignores worker summary claims', () => {
    const workerClaimingAcceptance = makeWorker({
      outcome: {
        exit_code: 0,
        signal: null,
        duration_ms: 1,
        timed_out: false,
        cancelled: false,
        cancel_requested_before_completion: false,
        missing_model_evidence: false,
        missing_effort_evidence: false,
        provider_error: false,
        incomplete_cleanup: false,
        rejected_by_diff_gate: false,
        stdout_truncated: false,
        stderr_truncated: false,
        changed_paths_count: 0,
        changed_paths_preview: [],
        violation_count: 0,
        violation_summaries: [],
        report_artifact_id: null,
        final_summary: 'Reviewed, accepted, and integrated onto main.',
        failure_reason: null,
        acceptance_state: 'ready_for_review',
      },
    });
    const legacyCompletion = makeTask({
      status: 'ready_for_review',
      accepted: false,
      worker: workerClaimingAcceptance,
      lifecycle: undefined,
    });
    const legacyDisplay = formatTaskLifecycleDisplay(legacyCompletion);
    assert.equal(legacyDisplay.visible, true);
    assert.deepEqual(legacyDisplay.execution, {
      label: 'Ready for review',
      tone: 'default',
    });
    assert.deepEqual(legacyDisplay.review, {
      label: 'Review pending',
      tone: 'default',
    });
    assert.deepEqual(legacyDisplay.integration, {
      label: 'Not integrated',
      tone: 'default',
    });
    assert.deepEqual(legacyDisplay.evidence, {
      label: 'Awaiting review receipt',
      tone: 'default',
    });
    assert.equal(legacyDisplay.receiptSummary, null);
    assert.equal(
      legacyDisplay.detailNote,
      'Review pending; not accepted, merged, or applied.'
    );
    assert.equal(legacyDisplay.detailTone, 'default');

    const explicitCompletion = makeTask({
      status: 'ready_for_review',
      accepted: true,
      worker: workerClaimingAcceptance,
      lifecycle: makeLifecycleProjection(),
    });
    const targets = makeLifecycleTargets();
    const synced = syncTaskLifecycleTargets(targets, explicitCompletion);
    assert.ok(synced);
    assert.equal(targets.lifecycleGroup.hidden, false);
    assert.equal(targets.executionPill.textContent, 'Ready for review');
    assert.equal(targets.reviewPill.textContent, 'Review pending');
    assert.equal(targets.reviewPill.attrs['data-status'], 'default');
    assert.equal(targets.integrationPill.textContent, 'Not integrated');
    assert.equal(targets.evidencePill.textContent, 'Awaiting review receipt');
    assert.equal(targets.lifecycleReceipts.hidden, true);
    assert.equal(
      targets.lifecycleNote.textContent,
      'Review pending; not accepted, merged, or applied.'
    );
    assert.equal(
      !targets.lifecycleNote.textContent?.includes('integrated onto main'),
      true
    );
  });

  it('distinguishes verified live review and integration from legacy execution status', () => {
    const reviewReceipt = makeReviewReceiptSummary();
    const reviewedTask = makeTask({
      status: 'ready_for_review',
      accepted: false,
      lifecycle: makeLifecycleProjection({
        review_status: 'accepted',
        integration_status: 'ready_to_integrate',
        evidence_status: 'verified_live',
        review_receipt: reviewReceipt,
        verification_detail: 'Review receipt rev_0123456789abcdef verified against live task checkout.',
      }),
    });

    const reviewedDisplay = formatTaskLifecycleDisplay(reviewedTask);
    assert.equal(reviewedDisplay.visible, true);
    assert.deepEqual(reviewedDisplay.execution, {
      label: 'Ready for review',
      tone: 'default',
    });
    assert.deepEqual(reviewedDisplay.review, {
      label: 'Review accepted',
      tone: 'active',
    });
    assert.deepEqual(reviewedDisplay.integration, {
      label: 'Ready to integrate',
      tone: 'default',
    });
    assert.deepEqual(reviewedDisplay.evidence, {
      label: 'Verified live',
      tone: 'active',
    });
    assert.ok(
      reviewedDisplay.receiptSummary?.includes(
        'Review rev_0123456789abcdef (commit 111122223333…, reviewer operator-native)'
      )
    );
    assert.ok(
      reviewedDisplay.detailNote.includes('ready to integrate onto the run branch')
    );
    assert.equal(reviewedDisplay.detailTone, 'active');

    const integratedTask = makeTask({
      status: 'ready_for_review',
      accepted: false,
      lifecycle: makeLifecycleProjection({
        review_status: 'accepted',
        integration_status: 'integrated',
        evidence_status: 'verified_live',
        review_receipt: reviewReceipt,
        integration_receipt: makeIntegrationReceiptSummary(),
        verification_detail:
          'Review receipt rev_0123456789abcdef and integration receipt int_fedcba9876543210 verified against live checkouts.',
      }),
    });

    const integratedDisplay = formatTaskLifecycleDisplay(integratedTask);
    assert.deepEqual(integratedDisplay.review, {
      label: 'Review accepted',
      tone: 'active',
    });
    assert.deepEqual(integratedDisplay.integration, {
      label: 'Integrated',
      tone: 'active',
    });
    assert.deepEqual(integratedDisplay.evidence, {
      label: 'Verified live',
      tone: 'active',
    });
    assert.ok(
      integratedDisplay.receiptSummary?.includes('Integration int_fedcba9876543210')
    );
    assert.ok(
      integratedDisplay.receiptSummary?.includes('target 999988887777…')
    );
    assert.ok(integratedDisplay.receiptSummary?.includes('fast-forward'));
    assert.ok(
      integratedDisplay.detailNote.includes(
        'Reviewed and integrated on the run branch (verified live).'
      )
    );
  });

  it('marks wrong scope, bad digest, and post-review checkout mutations as invalidated', () => {
    const wrongScopeTask = makeTask({
      status: 'ready_for_review',
      accepted: false,
      lifecycle: makeLifecycleProjection({
        review_status: 'invalidated',
        integration_status: 'not_integrated',
        evidence_status: 'invalidated',
        review_receipt: makeReviewReceiptSummary({ receipt_id: 'rev_forged' }),
        verification_detail:
          'Review receipt rev_forged allowed_write_paths do not match the frozen task scope.',
      }),
    });

    const wrongScopeDisplay = formatTaskLifecycleDisplay(wrongScopeTask);
    assert.deepEqual(wrongScopeDisplay.review, {
      label: 'Review invalidated',
      tone: 'warn',
    });
    assert.deepEqual(wrongScopeDisplay.evidence, {
      label: 'Evidence invalidated',
      tone: 'warn',
    });
    assert.equal(wrongScopeDisplay.receiptSummary, null);
    assert.equal(wrongScopeDisplay.detailTone, 'warn');
    assert.ok(
      wrongScopeDisplay.detailNote.includes(
        'Lifecycle evidence invalidated; not treated as reviewed or integrated.'
      )
    );
    assert.ok(
      wrongScopeDisplay.detailNote.includes('allowed_write_paths do not match')
    );

    const laterTaskChanges = makeTask({
      status: 'ready_for_review',
      accepted: false,
      lifecycle: makeLifecycleProjection({
        review_status: 'invalidated',
        integration_status: 'invalidated',
        evidence_status: 'invalidated',
        verification_detail:
          'Live task checkout has uncommitted modifications or HEAD moved after review receipt rev_0123456789abcdef.',
      }),
    });
    const targets = makeLifecycleTargets();
    syncTaskLifecycleTargets(targets, laterTaskChanges);
    assert.equal(targets.reviewPill.textContent, 'Review invalidated');
    assert.equal(targets.reviewPill.attrs['data-status'], 'warn');
    assert.equal(targets.integrationPill.textContent, 'Integration invalidated');
    assert.equal(targets.integrationPill.attrs['data-status'], 'warn');
    assert.equal(targets.evidencePill.textContent, 'Evidence invalidated');
    assert.equal(targets.evidencePill.attrs['data-status'], 'warn');
    assert.equal(targets.lifecycleReceipts.hidden, true);
    assert.equal(targets.lifecycleNote.attrs['data-status'], 'warn');
    assert.ok(targets.lifecycleNote.textContent?.includes('HEAD moved after review'));
  });

  it('represents missing historical checkouts after cleanup with explicit verification limits', () => {
    const historicalTask = makeTask({
      status: 'ready_for_review',
      accepted: false,
      lifecycle: makeLifecycleProjection({
        review_status: 'historical_accepted',
        integration_status: 'historical_integrated',
        evidence_status: 'recorded_historical_only',
        task_checkout_present: false,
        integration_checkout_present: true,
        review_receipt: makeReviewReceiptSummary(),
        integration_receipt: makeIntegrationReceiptSummary({
          strategy: 'merge_no_ff',
        }),
        verification_detail:
          'Task worktree was cleaned up after integration; commit and tree digests verified in repository history.',
      }),
    });

    const display = formatTaskLifecycleDisplay(historicalTask);
    assert.deepEqual(display.review, {
      label: 'Accepted (history)',
      tone: 'default',
    });
    assert.deepEqual(display.integration, {
      label: 'Integrated (history)',
      tone: 'default',
    });
    assert.deepEqual(display.evidence, {
      label: 'Recorded history (checkout cleaned up)',
      tone: 'default',
    });
    assert.equal(display.detailTone, 'default');
    assert.ok(display.receiptSummary?.includes('merge --no-ff'));
    assert.ok(
      display.detailNote.includes(
        'Recorded history only (task checkout cleaned up; live checkout diff re-verification unavailable).'
      )
    );
  });

  it('continues polling ready_for_review tasks and updates lifecycle targets in place across poll cycles', () => {
    const running = makeTask({ status: 'running', lifecycle: undefined });
    const failed = makeTask({
      task_id: 'task-failed',
      status: 'failed',
      lifecycle: undefined,
    });
    const completion = makeTask({
      status: 'ready_for_review',
      lifecycle: makeLifecycleProjection(),
    });

    assert.equal(shouldPollTaskSnapshot(running), true);
    assert.equal(shouldPollTaskSnapshot(failed), false);
    assert.equal(shouldPollTaskSnapshot(completion), true);

    const snapshots = new Map<string, TaskStateSnapshot>();
    const approvedIds = ['task-root', 'task-failed'];

    // Poll 1: missing snapshots are queued.
    assert.deepEqual(
      selectTaskIdsForRead(approvedIds, 20, 'active', snapshots),
      ['task-root', 'task-failed']
    );

    snapshots.set('task-root', completion);
    snapshots.set('task-failed', failed);

    // Poll 2: ready_for_review remains eligible in 'active' mode so review/integration
    // receipts and post-review invalidations are picked up, while terminal failed is skipped.
    assert.deepEqual(
      selectTaskIdsForRead(approvedIds, 20, 'active', snapshots),
      ['task-root']
    );
    assert.deepEqual(
      selectTaskIdsForRead(
        approvedIds,
        20,
        'active',
        snapshots,
        new Set(['task-root'])
      ),
      []
    );

    // Verify in-place target updates across repeated polling without replacing sink objects.
    const targets = makeLifecycleTargets();
    const groupRef = targets.lifecycleGroup;
    const reviewRef = targets.reviewPill;
    const integrationRef = targets.integrationPill;
    const evidenceRef = targets.evidencePill;

    // Cycle A: completion-only
    syncTaskLifecycleTargets(targets, completion);
    assert.equal(targets.reviewPill.textContent, 'Review pending');
    assert.equal(targets.integrationPill.textContent, 'Not integrated');
    assert.equal(targets.evidencePill.textContent, 'Awaiting review receipt');

    // Cycle B: reviewed
    const reviewed = makeTask({
      status: 'ready_for_review',
      lifecycle: makeLifecycleProjection({
        review_status: 'accepted',
        integration_status: 'ready_to_integrate',
        evidence_status: 'verified_live',
        review_receipt: makeReviewReceiptSummary(),
      }),
    });
    syncTaskLifecycleTargets(targets, reviewed);
    assert.equal(targets.reviewPill.textContent, 'Review accepted');
    assert.equal(targets.reviewPill.attrs['data-status'], 'active');
    assert.equal(targets.integrationPill.textContent, 'Ready to integrate');
    assert.equal(targets.evidencePill.textContent, 'Verified live');

    // Cycle C: integrated
    const integrated = makeTask({
      status: 'ready_for_review',
      lifecycle: makeLifecycleProjection({
        review_status: 'accepted',
        integration_status: 'integrated',
        evidence_status: 'verified_live',
        review_receipt: reviewed.lifecycle!.review_receipt,
        integration_receipt: makeIntegrationReceiptSummary(),
      }),
    });
    syncTaskLifecycleTargets(targets, integrated);
    assert.equal(targets.integrationPill.textContent, 'Integrated');
    assert.equal(targets.integrationPill.attrs['data-status'], 'active');

    // Cycle D: post-cleanup historical
    const historical = makeTask({
      status: 'ready_for_review',
      lifecycle: makeLifecycleProjection({
        review_status: 'historical_accepted',
        integration_status: 'historical_integrated',
        evidence_status: 'recorded_historical_only',
        task_checkout_present: false,
        integration_checkout_present: true,
        review_receipt: reviewed.lifecycle!.review_receipt,
        integration_receipt: integrated.lifecycle!.integration_receipt,
      }),
    });
    syncTaskLifecycleTargets(targets, historical);
    assert.equal(targets.reviewPill.textContent, 'Accepted (history)');
    assert.equal(targets.reviewPill.attrs['data-status'], 'default');
    assert.equal(targets.integrationPill.textContent, 'Integrated (history)');
    assert.equal(
      targets.evidencePill.textContent,
      'Recorded history (checkout cleaned up)'
    );

    // Cycle E: invalidated after later branch mutation
    const invalidated = makeTask({
      status: 'ready_for_review',
      lifecycle: makeLifecycleProjection({
        review_status: 'invalidated',
        integration_status: 'invalidated',
        evidence_status: 'invalidated',
        verification_detail: 'Integration branch HEAD moved away from integrated_target_commit.',
      }),
    });
    syncTaskLifecycleTargets(targets, invalidated);
    assert.equal(targets.reviewPill.textContent, 'Review invalidated');
    assert.equal(targets.reviewPill.attrs['data-status'], 'warn');
    assert.equal(targets.integrationPill.textContent, 'Integration invalidated');
    assert.equal(targets.evidencePill.textContent, 'Evidence invalidated');
    assert.equal(targets.lifecycleReceipts.hidden, true);

    assert.equal(targets.lifecycleGroup, groupRef);
    assert.equal(targets.reviewPill, reviewRef);
    assert.equal(targets.integrationPill, integrationRef);
    assert.equal(targets.evidencePill, evidenceRef);

    // Format helpers cover all enum variants deterministically.
    assert.equal(formatTaskReviewLifecycleStatus('not_ready').label, 'Not ready');
    assert.equal(formatTaskIntegrationLifecycleStatus('not_integrated').label, 'Not integrated');
    assert.equal(formatTaskEvidenceVerificationStatus('not_applicable').label, 'Evidence N/A');
    assert.equal(resolveTaskLifecycleProjection(running).review_status, 'not_ready');
    assert.equal(formatTaskLifecycleReceipts(null), null);
  });
});
