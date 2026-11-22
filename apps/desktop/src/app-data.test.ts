import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { CodexChatTarget } from './app-contract.ts';
import {
  DesktopAppCoordinator,
  mergeHealthyObservationSources,
  mergeScopedObservationJobs,
  mergeScopedObservationRequests,
  parseWorkspaceChatIdentityKey,
  REGISTRY_POLL_EVERY_N_TICKS,
  type DesktopCoordinatorState,
  type ToolDiscoveryEntry,
} from './app-data.ts';
import {
  createProfilesPanelState,
  getActiveDraft,
  isDraftDirty,
  syncRegistrySnapshot,
  updateActiveDraft,
} from './app-profile-state.ts';
import type {
  BacklogItem,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
  WorkspaceBacklogEvent,
  WorkspaceBacklogWorkerPresence,
  WorkspaceChatWork,
  WorkspaceWorkerExecution,
} from './observation/types.ts';
import {
  PROJECTS_ARCHIVE_STORAGE_KEY,
  writeProjectsArchiveCutoff,
} from './pages/projects-archive.ts';
import type {
  DesktopToolVersionsSnapshot,
  RegistrySnapshot,
} from './registry/types.ts';
import {
  createSafeWorkspaceMessageItem,
  createVerifiedConfiguredSelection,
  DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
  DEFAULT_WORKSPACE_SAMPLING_POLICY,
  formatWorkspaceChatIdentityKey,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  normalizeWorkspaceSnapshot,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  type WorkspaceChatIdentity,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceChatSummary,
  type WorkspaceJobHistory,
  type WorkspaceNativeSample,
  type WorkspaceSnapshot,
} from './workspace-contract.ts';

const NOW_MS = Date.parse('2026-11-09T10:00:00.000Z');
const UUID_A = '11111111-2222-4333-8444-555555555555';
const UUID_B = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const UUID_C = '99999999-8888-4777-8666-555555555555';

const IDENTITY_A: WorkspaceChatIdentity = {
  source_id: 'src-a',
  binding_id: 'bind-a',
  thread_uuid: UUID_A,
  canonical_project_scope: '/work/alpha',
};

const IDENTITY_B: WorkspaceChatIdentity = {
  source_id: 'src-a',
  binding_id: 'bind-b',
  thread_uuid: UUID_B,
  canonical_project_scope: '/work/alpha',
};

const IDENTITY_SRC_B: WorkspaceChatIdentity = {
  source_id: 'src-b',
  binding_id: 'bind-src-b',
  thread_uuid: UUID_C,
  canonical_project_scope: '/work/beta:service',
};

function makeSource(
  overrides: Partial<ObservationSource> = {},
): ObservationSource {
  return {
    id: 'src-a',
    label: 'Alpha Project',
    project_path: '/work/alpha',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-a',
    id: 'job-1',
    project_label: 'Alpha Project',
    project_path: '/work/alpha',
    title: 'Coordinator test job',
    thread_uuid: UUID_A,
    thread_label: 'Chat A',
    binding_id: 'bind-a',
    plan_id: 'plan-1',
    plan_revision: 1,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 5_000,
    active_call_count: 1,
    tasks: [],
    summary: 'Running job',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {},
): ObservationRequest {
  return {
    source_id: 'src-a',
    id: 'req-1',
    job_id: 'job-1',
    parent_request_id: null,
    title: 'Worker step',
    operation: 'flow.handoff',
    project_label: 'Alpha Project',
    binding_id: 'bind-a',
    thread_uuid: UUID_A,
    thread_label: 'Chat A',
    state: 'running',
    started_at_ms: NOW_MS - 10_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 2_000,
    duration_ms: null,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: {
      tool: 'agy',
      provider: 'antigravity',
      role: 'coder',
      requested_model: 'gemini-4-argon-high',
      observed_model: 'gemini-4-argon-high',
      requested_effort: 'high',
      observed_effort: 'high',
    },
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Step summary',
    ...overrides,
  };
}

function makeLegacyObservationSnapshot(
  overrides: Partial<ObservationSnapshot> = {},
): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [makeJob()],
    requests: [makeRequest()],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

function makeChatSummary(
  identity: WorkspaceChatIdentity,
  overrides: Partial<WorkspaceChatSummary> = {},
): WorkspaceChatSummary {
  return {
    identity,
    thread_label: 'Chat A',
    project_label: 'Alpha Project',
    availability: 'available',
    activity: 'idle',
    configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
    active_job_count: 0,
    attention_count: 0,
    updated_at_ms: NOW_MS - 2_000,
    ...overrides,
  };
}

function makeNativeSample(
  identity: WorkspaceChatIdentity,
  overrides: Partial<WorkspaceNativeSample> = {},
): WorkspaceNativeSample {
  return {
    chat_identity: identity,
    producer_epoch: 'epoch-prod-01',
    backend_process_instance: {
      pid: 4242,
      boot_id: 'boot-linux-01',
      start_ticks: 910_000,
    },
    sequence: 1,
    observed_at_ms: NOW_MS - 2_000,
    last_success_at_ms: NOW_MS - 2_000,
    availability: 'available',
    activity: 'working',
    connection: {
      state: 'connected',
      freshness: 'fresh',
      route: 'managed_linux_desktop',
      diagnostic: null,
    },
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'supported',
    },
    configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
    ...overrides,
  };
}

function makeJobHistory(
  identity: WorkspaceChatIdentity | null,
  overrides: Partial<WorkspaceJobHistory> = {},
): WorkspaceJobHistory {
  const base: WorkspaceJobHistory = {
    job: {
      source_id: identity?.source_id ?? 'src-a',
      job_id: 'job-1',
      binding_id: identity?.binding_id ?? null,
      chat_identity: identity,
      title: 'Coordinator test job',
      job_revision: 1,
      state: 'running',
      phase: 'execution',
      approval_status: 'approved',
      execution_status: 'running',
      review_state: null,
      created_at_ms: NOW_MS - 30_000,
      updated_at_ms: NOW_MS - 5_000,
      summary: 'Running job',
    },
    tasks: [],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: {
      source_plan_id: 'plan-1',
      source_plan_revision: 1,
      source_approval_id: 'apr-1',
      source_work_brief_id: 'wb-1',
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: [],
    },
    historical_failure: null,
    current_attention: null,
    acceptance_proof: {
      status: 'unknown',
      accepted_candidate_id: null,
      combined_candidate_id: null,
      review_verdict_id: null,
      review_receipt_id: null,
      integration_receipt_id: null,
      closure_reservation_id: null,
      closure_verdict_id: null,
      verified_at_ms: null,
    },
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
  };
  return {
    ...base,
    ...overrides,
    job: {
      ...base.job,
      ...(overrides.job ?? {}),
    },
  };
}

function makeWorkspaceSnapshot(
  overrides: Partial<WorkspaceSnapshot> = {},
): WorkspaceSnapshot {
  const defaultMessage = createSafeWorkspaceMessageItem({
    source_kind: 'plan_summary',
    source_record_id: 'plan-1',
    source_order: {
      recorded_at_ms: NOW_MS - 3_000,
      source_seq: 1,
      sub_index: 0,
    },
    scope: {
      source_id: IDENTITY_A.source_id,
      binding_id: IDENTITY_A.binding_id,
      thread_uuid: IDENTITY_A.thread_uuid,
      canonical_project_scope: IDENTITY_A.canonical_project_scope,
      chat_identity: IDENTITY_A,
      job_id: 'job-1',
    },
    raw_summary: 'Approved plan summary for Chat A.',
  });

  return normalizeWorkspaceSnapshot({
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: NOW_MS,
    sampling_policy: DEFAULT_WORKSPACE_SAMPLING_POLICY,
    observation: makeLegacyObservationSnapshot(),
    chats: [makeChatSummary(IDENTITY_A, { thread_label: 'Chat A' })],
    native_samples: [makeNativeSample(IDENTITY_A)],
    job_histories: [makeJobHistory(IDENTITY_A)],
    messages: {
      availability: 'available',
      items: [defaultMessage],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    },
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
    ...overrides,
  });
}

function makeRegistry(
  overrides: Partial<RegistrySnapshot> = {},
): RegistrySnapshot {
  return {
    revision: 1,
    tools: [
      {
        id: 'codex-cli',
        label: 'Codex CLI',
        adapter: 'codex_cli',
        executable_path: null,
        enabled: true,
      },
    ],
    models: [
      {
        id: 'codex-gpt-5-4',
        tool_id: 'codex-cli',
        label: 'GPT-5.4',
        model_arg: 'gpt-5.4',
        efforts: ['medium', 'high'],
        allow_no_effort: false,
      },
    ],
    profiles: [
      {
        id: 'default-profile',
        name: 'Default Delivery',
        assignments: [
          {
            role: 'orchestrator',
            difficulty: null,
            choice: { model_id: 'codex-gpt-5-4', effort: 'high' },
          },
          {
            role: 'architect',
            difficulty: null,
            choice: { model_id: 'codex-gpt-5-4', effort: 'high' },
          },
          {
            role: 'coder',
            difficulty: null,
            choice: { model_id: 'codex-gpt-5-4', effort: 'high' },
          },
          {
            role: 'reviewer',
            difficulty: null,
            choice: { model_id: 'codex-gpt-5-4', effort: 'high' },
          },
          {
            role: 'closure',
            difficulty: null,
            choice: { model_id: 'codex-gpt-5-4', effort: 'high' },
          },
        ],
        decision_advisor: null,
      },
    ],
    schema_version: 1,
    default_profile_id: 'default-profile',
    ...overrides,
  };
}

function createMemoryStorage(initialEntries: Record<string, string> = {}): {
  raw: Map<string, string>;
  storage: WorkspaceChatPreferenceStorage & {
    failWrites: boolean;
    writeAttempts: number;
  };
} {
  const raw = new Map<string, string>(Object.entries(initialEntries));
  const storage = {
    failWrites: false,
    writeAttempts: 0,
    getItem(key: string): string | null {
      return raw.get(key) ?? null;
    },
    setItem(key: string, value: string): void {
      storage.writeAttempts += 1;
      if (storage.failWrites) {
        throw new Error('QuotaExceededError: storage write rejected');
      }
      raw.set(key, value);
    },
  };
  return { raw, storage };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function flushMicrotasks(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 10));
}

describe('app-data DesktopAppCoordinator', () => {
  it('prefers fetchWorkspaceSnapshot, defaults to Messages + Active lifecycle, and keeps legacy observation compatibility non-fabricating', async () => {
    let workspaceCalls = 0;
    let legacyCalls = 0;
    const requestsReceived: ObservationSnapshotRequest[] = [];

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async (req) => {
        workspaceCalls += 1;
        requestsReceived.push(req ?? {});
        return makeWorkspaceSnapshot({ generated_at_ms: NOW_MS });
      },
      fetchObservationSnapshot: async () => {
        legacyCalls += 1;
        return makeLegacyObservationSnapshot();
      },
      now: () => NOW_MS,
    });

    assert.equal(coordinator.getState().selectedTab, 'messages');
    assert.equal(coordinator.getState().workspaceLifecycle, 'active');
    assert.deepEqual(coordinator.getState().archivePreferences, {});
    assert.equal(coordinator.getState().archiveError, null);
    assert.equal('windowHours' in coordinator.getState(), false);
    assert.equal('archiveBeforeMs' in coordinator.getState(), false);
    assert.equal('loadMoreHistory' in coordinator, false);
    assert.equal('setWindowHours' in coordinator, false);
    assert.equal('setArchiveBeforeMs' in coordinator, false);

    await coordinator.refreshWorkspace();

    assert.equal(workspaceCalls, 1);
    assert.equal(legacyCalls, 0);
    assert.deepEqual(requestsReceived[0], {
      source_id: null,
      job_id: null,
      cursor: null,
      limit: DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
    });

    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.ok(coordinator.getState().workspaceSnapshot);

    const projected = coordinator.projectWorkspace(NOW_MS);
    assert.equal(projected.status, 'ready');
    assert.equal(projected.defaultTab, 'messages');
    assert.equal(projected.selectedChat?.activeTab, 'messages');
    assert.equal(projected.selectedChat?.messages.items.length, 1);
    assert.equal(projected.selectedChat?.jobs.items.length, 1);
    assert.equal(projected.selectedChat?.activity.items.length, 1);

    // Explicit legacy compatibility port must never fabricate native samples, acceptance proofs, or safe messages.
    const legacyCoordinator = new DesktopAppCoordinator({
      fetchObservationSnapshot: async () =>
        makeLegacyObservationSnapshot({
          jobs: [
            makeJob({
              state: 'completed',
              execution_status: 'completed',
              active_call_count: 0,
            }),
          ],
        }),
      now: () => NOW_MS,
    });
    await legacyCoordinator.refreshObservation();

    const upgradedState = legacyCoordinator.getState();
    assert.equal(upgradedState.workspaceSnapshot?.native_samples.length, 0);
    assert.equal(
      upgradedState.workspaceSnapshot?.messages.availability,
      'unavailable',
    );
    assert.equal(upgradedState.workspaceSnapshot?.messages.items.length, 0);

    const upgradedProjection = legacyCoordinator.projectWorkspace(NOW_MS);
    assert.equal(
      upgradedProjection.selectedChat?.nativeState.availability,
      'unavailable',
    );
    assert.equal(
      upgradedProjection.selectedChat?.jobs.verifiedAcceptedJobCount,
      0,
    );
    assert.equal(
      upgradedProjection.selectedChat?.messages.availability,
      'unavailable',
    );

    const legacyProjection = legacyCoordinator.projectLegacyWorkspace(NOW_MS);
    assert.equal(legacyProjection.status, 'ready');
  });

  it('polls only while visible, coalesces visibility+focus wakeup to a single read, and surfaces bounded Activity window notice without paging', async () => {
    const wakeupRead = deferred<WorkspaceSnapshot>();
    const nextPollRead = deferred<WorkspaceSnapshot>();
    const calls: ObservationSnapshotRequest[] = [];
    const states: DesktopCoordinatorState[] = [];

    const coordinator = new DesktopAppCoordinator(
      {
        fetchWorkspaceSnapshot: (req) => {
          calls.push(req ?? {});
          return calls.length === 1 ? wakeupRead.promise : nextPollRead.promise;
        },
        now: () => NOW_MS,
      },
      (state) => {
        states.push(state);
      },
      { visible: false, focused: false },
    );

    assert.equal(coordinator.canPoll(), false);
    await coordinator.pollTick();
    await coordinator.pollTick();
    assert.equal(calls.length, 0);

    // Simultaneous/back-to-back visibilitychange + focus events while read #1 is in flight must trigger only ONE read.
    coordinator.setVisibility(true, false);
    coordinator.setVisibility(true, true);
    assert.equal(calls.length, 1);
    assert.equal(coordinator.isWorkspaceInFlight(), true);

    // Snapshot reports has_more + next_cursor + requests_capped; coordinator must NOT auto-page cursor-page-2.
    wakeupRead.resolve(
      makeWorkspaceSnapshot({
        generated_at_ms: NOW_MS + 1_000,
        observation: makeLegacyObservationSnapshot({
          generated_at_ms: NOW_MS + 1_000,
          requests: Array.from(
            { length: DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT },
            (_, idx) =>
              makeRequest({
                id: `req-${idx}`,
                title: `Step ${idx}`,
              }),
          ),
          has_more: true,
          next_cursor: 'cursor-page-2',
        }),
        bounds: {
          native_samples_omitted: 0,
          job_histories_omitted: 0,
          messages_omitted: 0,
          requests_capped: true,
          jobs_capped: false,
        },
      }),
    );
    await flushMicrotasks();

    assert.equal(calls.length, 1);
    assert.equal(calls[0]?.cursor, null);
    assert.equal(calls[0]?.limit, DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT);
    assert.ok(states.at(-1)?.workspaceSnapshot);

    coordinator.setWorkspaceTab('activity');
    const projected = coordinator.projectWorkspace(NOW_MS + 1_000);
    assert.equal(projected.selectedChat?.activeTab, 'activity');
    assert.equal(projected.selectedChat?.activity.hasMore, true);
    assert.equal(projected.selectedChat?.activity.requestsCapped, true);
    assert.equal(projected.selectedChat?.activity.nextCursor, 'cursor-page-2');
    assert.ok(
      projected.selectedChat?.activity.statusNotices.some((notice) =>
        notice.includes('Showing a bounded activity request window'),
      ),
    );

    // If a poll is in flight with a queued refresh and the window becomes hidden, the queued follow-up is cancelled.
    const pollPromise = coordinator.pollTick();
    void coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(calls.length, 2);

    coordinator.setVisibility(false, false);
    nextPollRead.resolve(
      makeWorkspaceSnapshot({ generated_at_ms: NOW_MS + 2_000 }),
    );
    await pollPromise;
    await flushMicrotasks();

    assert.equal(calls.length, 2);
    await coordinator.pollTick();
    assert.equal(calls.length, 2);
  });

  it('coalesces duplicate concurrent refreshes into one in-flight read plus one bounded pending read', async () => {
    const inflight1 = deferred<WorkspaceSnapshot>();
    const inflight2 = deferred<WorkspaceSnapshot>();
    let callCount = 0;

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: () => {
        callCount += 1;
        return callCount === 1 ? inflight1.promise : inflight2.promise;
      },
      now: () => NOW_MS,
    });

    const p1 = coordinator.refreshWorkspace();
    void coordinator.refreshWorkspace();
    void coordinator.refreshWorkspace();
    void coordinator.refreshObservation();
    void coordinator.refreshObservation();

    assert.equal(callCount, 1);

    inflight1.resolve(
      makeWorkspaceSnapshot({ generated_at_ms: NOW_MS + 1_000 }),
    );
    await p1;
    await flushMicrotasks();

    assert.equal(callCount, 2);

    inflight2.resolve(
      makeWorkspaceSnapshot({ generated_at_ms: NOW_MS + 2_000 }),
    );
    await flushMicrotasks();

    assert.equal(
      coordinator.getState().workspaceSnapshot?.generated_at_ms,
      NOW_MS + 2_000,
    );
  });

  it('prevents late responses and late errors across chat switches, scope switches, and out-of-order generations from polluting the new selection', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const keySrcB = formatWorkspaceChatIdentityKey(IDENTITY_SRC_B)!;

    const twoChatSnapshot = makeWorkspaceSnapshot({
      generated_at_ms: NOW_MS + 5_000,
      chats: [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          updated_at_ms: NOW_MS - 1_000,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_A, { sequence: 3 }),
        makeNativeSample(IDENTITY_B, { sequence: 2 }),
      ],
    });

    const staleRefresh = deferred<WorkspaceSnapshot>();
    const postSwitchRefresh = deferred<WorkspaceSnapshot>();
    const failingBeforeSwitch = deferred<WorkspaceSnapshot>();
    const cleanAfterSwitch = deferred<WorkspaceSnapshot>();
    const slowScopeARead = deferred<WorkspaceSnapshot>();
    const scopeBRead = deferred<WorkspaceSnapshot>();

    let step = 0;
    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: (req) => {
        step += 1;
        if (step === 1) {
          return Promise.resolve(twoChatSnapshot);
        }
        if (step === 2) {
          // Out-of-order older generated_at_ms snapshot
          return Promise.resolve(
            makeWorkspaceSnapshot({
              generated_at_ms: NOW_MS + 1_000,
              chats: [makeChatSummary(IDENTITY_A, { thread_label: 'Old A' })],
            }),
          );
        }
        if (step === 3) {
          return staleRefresh.promise;
        }
        if (step === 4) {
          return postSwitchRefresh.promise;
        }
        if (step === 5) {
          return failingBeforeSwitch.promise;
        }
        if (step === 6) {
          return cleanAfterSwitch.promise;
        }
        if (req?.source_id === 'src-a') {
          return slowScopeARead.promise;
        }
        return scopeBRead.promise;
      },
      now: () => NOW_MS + 5_000,
    });

    // 1. Initial load & out-of-order generated_at_ms rejection
    await coordinator.refreshWorkspace({ mode: 'replace' });
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    coordinator.selectChat(keyB);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    // Switching chat with no in-flight read does not trigger extra IPC reads.
    assert.equal(step, 1);

    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot?.generated_at_ms,
      NOW_MS + 5_000,
    );
    assert.equal(coordinator.getState().selectedChatKey, keyB);

    // 2. Start in-flight refresh on Chat B, switch back to Chat A, then switch to Chat B while in flight:
    // late response containing only Chat A must NOT overwrite Chat B selection.
    coordinator.selectChat(keyA);
    const pStale = coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(step, 3);

    coordinator.selectChat(keyB);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    // Still single in-flight read until step 3 finishes.
    assert.equal(step, 3);

    staleRefresh.resolve(
      makeWorkspaceSnapshot({
        generated_at_ms: NOW_MS + 6_000,
        chats: [makeChatSummary(IDENTITY_A, { thread_label: 'Only Chat A' })],
        native_samples: [makeNativeSample(IDENTITY_A, { sequence: 4 })],
      }),
    );
    await pStale;
    await flushMicrotasks();

    // Coalesced post-switch read is now in flight; Chat B selection remained intact.
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(step, 4);

    postSwitchRefresh.resolve(
      makeWorkspaceSnapshot({
        generated_at_ms: NOW_MS + 7_000,
        chats: [
          makeChatSummary(IDENTITY_A, { thread_label: 'Chat A' }),
          makeChatSummary(IDENTITY_B, { thread_label: 'Chat B' }),
        ],
        native_samples: [
          makeNativeSample(IDENTITY_A, { sequence: 4 }),
          makeNativeSample(IDENTITY_B, { sequence: 3 }),
        ],
      }),
    );
    await flushMicrotasks();
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.getState().observationError, null);

    // 3. Late failure from an in-flight read started on Chat B must NOT carry error onto newly selected Chat A.
    const pFailOldGen = coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(step, 5);
    coordinator.selectChat(keyA);
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    failingBeforeSwitch.reject(new Error('Stale generation read error'));
    await pFailOldGen;
    await flushMicrotasks();

    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(coordinator.getState().observationError, null);
    assert.equal(step, 6);

    cleanAfterSwitch.resolve(
      makeWorkspaceSnapshot({
        generated_at_ms: NOW_MS + 8_000,
        chats: [
          makeChatSummary(IDENTITY_A, { thread_label: 'Chat A' }),
          makeChatSummary(IDENTITY_B, { thread_label: 'Chat B' }),
        ],
      }),
    );
    await flushMicrotasks();
    assert.equal(coordinator.getState().observationError, null);

    // 4. Source/job scope change while src-a scoped read is in flight:
    // clears mismatched src-a chat selection, keeps single in-flight read, and ignores late src-a failure.
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-1' });
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(step, 7);

    coordinator.setObservationScope({ source_id: 'src-b', job_id: 'job-b1' });
    assert.equal(coordinator.getState().selectedChatKey, null);
    // Single read in flight: step 8 has not fired until step 7 settles!
    assert.equal(step, 7);

    slowScopeARead.reject(new Error('Old source src-a read failed'));
    await flushMicrotasks();

    assert.equal(coordinator.getState().observationError, null);
    assert.equal(step, 8);

    scopeBRead.resolve(
      makeWorkspaceSnapshot({
        generated_at_ms: NOW_MS + 9_000,
        observation: makeLegacyObservationSnapshot({
          generated_at_ms: NOW_MS + 9_000,
          sources: [
            makeSource({
              id: 'src-b',
              label: 'Beta Service',
              project_path: '/work/beta:service',
            }),
          ],
          jobs: [
            makeJob({
              source_id: 'src-b',
              id: 'job-b1',
              project_label: 'Beta Service',
              project_path: '/work/beta:service',
              binding_id: IDENTITY_SRC_B.binding_id,
              thread_uuid: IDENTITY_SRC_B.thread_uuid,
              thread_label: 'Beta Chat',
            }),
          ],
          requests: [],
        }),
        chats: [
          makeChatSummary(IDENTITY_SRC_B, {
            thread_label: 'Beta Chat',
            project_label: 'Beta Service',
          }),
        ],
        native_samples: [makeNativeSample(IDENTITY_SRC_B)],
        job_histories: [
          makeJobHistory(IDENTITY_SRC_B, {
            job: {
              source_id: 'src-b',
              job_id: 'job-b1',
              binding_id: IDENTITY_SRC_B.binding_id,
              chat_identity: IDENTITY_SRC_B,
              title: 'Beta deployment job',
              job_revision: 1,
              state: 'running',
              phase: 'execution',
              approval_status: 'approved',
              execution_status: 'running',
              review_state: null,
              created_at_ms: NOW_MS,
              updated_at_ms: NOW_MS + 8_500,
              summary: 'Running beta slice.',
            },
          }),
        ],
      }),
    );
    await flushMicrotasks();

    assert.equal(coordinator.getState().observationError, null);
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
  });

  it('never resets native sample age on failed reads, withholds stale working/model immediately, and preserves idle/historical-failure invariants', async () => {
    let clockMs = NOW_MS;
    let shouldFail = false;
    const sampleObservedMs = NOW_MS - 2_000;
    const sampleLastSuccessMs = NOW_MS - 2_000;

    // Include a running job with a historical failed attempt; configured selection is never runtime telemetry.
    const jobWithHistoricalAttemptFailure = makeJobHistory(IDENTITY_A, {
      job: {
        source_id: IDENTITY_A.source_id,
        job_id: 'job-running-1',
        binding_id: IDENTITY_A.binding_id,
        chat_identity: IDENTITY_A,
        title: 'Coordinator migration job',
        job_revision: 2,
        state: 'running',
        phase: 'execution',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        created_at_ms: NOW_MS - 40_000,
        updated_at_ms: NOW_MS - 3_000,
        summary: 'Retrying worker slice.',
      },
      tasks: [
        {
          task_id: 'task-1',
          title: 'Update coordinator',
          role: 'coder',
          difficulty: 'medium',
          stage: 'execution',
          status: 'running',
          depends_on: [],
          current_attempt_no: 2,
          attempts: [
            {
              attempt_no: 1,
              status: 'failed',
              run_id: 'run-1',
              worker_id: 'worker-1',
              outbox_id: null,
              executor: null,
              started_at_ms: NOW_MS - 35_000,
              finished_at_ms: NOW_MS - 25_000,
              summary: 'First attempt timed out.',
            },
          ],
          candidates: [],
          active_candidate_id: null,
          accepted_candidate_id: null,
          historical_failure: {
            failure_id: 'fail-1',
            stage: 'execution',
            failed_at_ms: NOW_MS - 25_000,
            task_id: 'task-1',
            attempt_no: 1,
            run_id: 'run-1',
            outbox_id: null,
            reason: 'Worker attempt 1 timed out.',
            superseded_or_resolved: true,
          },
          current_attention: null,
          acceptance_proof: {
            status: 'unknown',
            accepted_candidate_id: null,
            combined_candidate_id: null,
            review_verdict_id: null,
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: null,
            verified_at_ms: null,
          },
        },
      ],
      current_attention: null,
    });

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => {
        if (shouldFail) {
          throw new Error('Backend socket read failed');
        }
        return makeWorkspaceSnapshot({
          generated_at_ms: clockMs,
          chats: [
            makeChatSummary(IDENTITY_A, {
              availability: 'available',
              activity: 'working',
              configured_selection: createVerifiedConfiguredSelection(
                'gpt-5.4',
                'high',
              ),
              active_job_count: 1,
              updated_at_ms: sampleObservedMs,
            }),
          ],
          native_samples: [
            makeNativeSample(IDENTITY_A, {
              observed_at_ms: sampleObservedMs,
              last_success_at_ms: sampleLastSuccessMs,
              availability: 'available',
              activity: 'working',
              configured_selection: createVerifiedConfiguredSelection(
                'gpt-5.4',
                'high',
              ),
            }),
          ],
          job_histories: [jobWithHistoricalAttemptFailure],
        });
      },
      now: () => clockMs,
    });

    await coordinator.refreshWorkspace({ mode: 'replace' });
    const liveProj = coordinator.projectWorkspace();
    assert.equal(liveProj.isStale, false);
    assert.equal(liveProj.totalWorkingCoworkerCount, 1);
    assert.equal(liveProj.selectedChat?.nativeState.availability, 'available');
    assert.equal(liveProj.selectedChat?.nativeState.activity, 'working');
    assert.equal(liveProj.selectedChat?.nativeState.sampleAgeMs, 2_000);
    assert.equal(
      liveProj.selectedChat?.configuredSelection.selectedModel,
      'gpt-5.4',
    );
    assert.equal(
      liveProj.selectedChat?.configuredSelection.countsAsRuntimeTelemetry,
      false,
    );
    // Historical failed attempt does NOT fabricate current human-input blocker.
    assert.equal(liveProj.totalNeedsHumanCount, 0);
    assert.equal(liveProj.selectedChat?.hasCurrentBlocker, false);

    // Advance only 3s (total sample age 5s < 10s stale threshold) and fail the read:
    // producer timestamps MUST NOT reset, sample age MUST be 5_000ms, and working/model MUST be withheld immediately!
    clockMs = NOW_MS + 3_000;
    shouldFail = true;
    await coordinator.refreshWorkspace({ mode: 'refresh' });

    const retainedSample =
      coordinator.getState().workspaceSnapshot?.native_samples[0];
    assert.ok(retainedSample);
    assert.equal(retainedSample.observed_at_ms, sampleObservedMs);
    assert.equal(retainedSample.last_success_at_ms, sampleLastSuccessMs);

    const failedEarlyProj = coordinator.projectWorkspace();
    assert.equal(failedEarlyProj.isStale, true);
    assert.equal(failedEarlyProj.staleReason, 'Backend socket read failed');
    assert.equal(failedEarlyProj.totalWorkingCoworkerCount, 0);
    assert.equal(
      failedEarlyProj.selectedChat?.nativeState.availability,
      'stale',
    );
    assert.equal(
      failedEarlyProj.selectedChat?.nativeState.activity,
      'unknown',
    );
    assert.equal(
      failedEarlyProj.selectedChat?.nativeState.observedAtMs,
      sampleObservedMs,
    );
    assert.equal(
      failedEarlyProj.selectedChat?.nativeState.lastSuccessAtMs,
      sampleLastSuccessMs,
    );
    assert.equal(failedEarlyProj.selectedChat?.nativeState.sampleAgeMs, 5_000);
    assert.equal(
      failedEarlyProj.selectedChat?.configuredSelection.selectedModel,
      null,
    );
    assert.equal(
      failedEarlyProj.selectedChat?.configuredSelection.isStale,
      true,
    );

    // Advance past the 10s freshness window: age continues advancing truthfully without reset.
    clockMs = NOW_MS + 12_000;
    const failedLateProj = coordinator.projectWorkspace();
    assert.equal(failedLateProj.selectedChat?.nativeState.sampleAgeMs, 14_000);
    assert.equal(
      failedLateProj.selectedChat?.nativeState.availability,
      'stale',
    );

    // Recover with a fresh idle native sample while job is still running: native idle never means job Done.
    shouldFail = false;
    const coordinatorIdleCheck = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () =>
        makeWorkspaceSnapshot({
          generated_at_ms: clockMs,
          chats: [
            makeChatSummary(IDENTITY_A, {
              availability: 'available',
              activity: 'idle',
              active_job_count: 1,
              updated_at_ms: clockMs - 1_000,
            }),
          ],
          native_samples: [
            makeNativeSample(IDENTITY_A, {
              sequence: 2,
              observed_at_ms: clockMs - 1_000,
              last_success_at_ms: clockMs - 1_000,
              availability: 'available',
              activity: 'idle',
            }),
          ],
          job_histories: [jobWithHistoricalAttemptFailure],
        }),
      now: () => clockMs,
    });
    await coordinatorIdleCheck.refreshWorkspace();
    const idleWithRunningJob = coordinatorIdleCheck.projectWorkspace();
    assert.equal(idleWithRunningJob.selectedChat?.nativeState.activity, 'idle');
    assert.equal(idleWithRunningJob.selectedChat?.jobs.activeJobCount, 1);
    assert.equal(
      idleWithRunningJob.selectedChat?.jobs.items[0]?.stateRaw,
      'running',
    );
  });

  it('handles partial source loss truthfully and preserves scoped merge helpers', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keySrcB = formatWorkspaceChatIdentityKey(IDENTITY_SRC_B)!;
    let dropSrcBChats = false;

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () =>
        makeWorkspaceSnapshot({
          generated_at_ms: dropSrcBChats ? NOW_MS + 2_000 : NOW_MS + 1_000,
          observation: makeLegacyObservationSnapshot({
            generated_at_ms: dropSrcBChats ? NOW_MS + 2_000 : NOW_MS + 1_000,
            sources: [
              makeSource({ id: 'src-a', availability: 'available' }),
              makeSource({
                id: 'src-b',
                label: 'Beta Service',
                project_path: '/work/beta:service',
                availability: 'unavailable',
                message: 'Store SQLite lock busy',
              }),
            ],
            jobs: [makeJob({ id: 'job-1', source_id: 'src-a' })],
            requests: [makeRequest({ id: 'req-1', source_id: 'src-a' })],
          }),
          chats: dropSrcBChats
            ? [makeChatSummary(IDENTITY_A, { thread_label: 'Alpha Chat' })]
            : [
                makeChatSummary(IDENTITY_A, { thread_label: 'Alpha Chat' }),
                makeChatSummary(IDENTITY_SRC_B, {
                  thread_label: 'Beta Cached Chat',
                  project_label: 'Beta Service',
                  availability: 'available',
                  activity: 'working',
                }),
              ],
          native_samples: dropSrcBChats
            ? [makeNativeSample(IDENTITY_A)]
            : [
                makeNativeSample(IDENTITY_A),
                makeNativeSample(IDENTITY_SRC_B, {
                  observed_at_ms: NOW_MS - 3_000,
                  last_success_at_ms: NOW_MS - 3_000,
                  availability: 'available',
                  activity: 'working',
                }),
              ],
        }),
      now: () => NOW_MS + 1_000,
    });

    await coordinator.refreshWorkspace({ mode: 'replace' });
    coordinator.selectChat(keySrcB);

    const partialProj = coordinator.projectWorkspace(NOW_MS + 1_000);
    assert.equal(partialProj.status, 'ready');
    assert.equal(partialProj.unavailableSourceCount, 1);
    assert.equal(partialProj.unavailableSources[0]?.sourceId, 'src-b');
    // Cached sample on an unavailable source cannot keep claiming live working/model!
    assert.equal(partialProj.selectedChat?.nativeState.availability, 'stale');
    assert.equal(partialProj.selectedChat?.nativeState.activity, 'unknown');
    assert.equal(partialProj.selectedChat?.nativeState.sampleAgeMs, 4_000);
    assert.equal(
      partialProj.selectedChat?.configuredSelection.selectedModel,
      null,
    );
    // Healthy source src-a still counts its 1 live working coworker.
    assert.equal(partialProj.totalWorkingCoworkerCount, 1);

    // When src-b chats drop completely on the next refresh, explicit selection stays on keySrcB
    // as temporarily unavailable instead of silently switching to healthy src-a.
    dropSrcBChats = true;
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
    const missingSrcBProj = coordinator.projectWorkspace(NOW_MS + 2_000);
    assert.equal(missingSrcBProj.selectedChatKey, keySrcB);
    assert.equal(missingSrcBProj.selectedChat, null);
    assert.equal(
      missingSrcBProj.selectedChatUnavailable?.reason,
      'source_unavailable',
    );
    assert.equal(
      missingSrcBProj.selectedChatUnavailable?.threadLabel,
      'Beta Cached Chat',
    );

    // Manual selection of healthy src-a still switches immediately.
    coordinator.selectChat(keyA);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(
      coordinator.projectWorkspace(NOW_MS + 2_000).selectedChat?.threadLabel,
      'Alpha Chat',
    );

    // Legacy scoped merge helpers remain intact for non-production callers.
    const mergedReqs = mergeScopedObservationRequests(
      [makeRequest({ source_id: 'src-a', id: 'shared', title: 'From A' })],
      [makeRequest({ source_id: 'src-b', id: 'shared', title: 'From B' })],
    );
    assert.equal(mergedReqs.length, 2);
    const mergedJobs = mergeScopedObservationJobs(
      [makeJob({ source_id: 'src-a', id: 'shared', title: 'Job A' })],
      [makeJob({ source_id: 'src-b', id: 'shared', title: 'Job B' })],
    );
    assert.equal(mergedJobs.length, 2);
    const mergedSources = mergeHealthyObservationSources(
      [makeSource({ id: 'src-a', availability: 'available' })],
      [
        makeSource({
          id: 'src-a',
          availability: 'unavailable',
          message: 'Transient',
        }),
      ],
    );
    assert.equal(mergedSources[0]?.availability, 'available');
  });

  it('manages canonical chat selection, Messages/Jobs/Activity tabs, Active/Archive lifecycle, storage failure, reload persistence, and preserves unrelated preferences', async () => {
    const appearanceKey = 'threadlane.appearance.v2';
    const appearanceValue = JSON.stringify({
      theme: 'dark',
      accent: 'indigo',
    });
    const { raw, storage } = createMemoryStorage({
      [appearanceKey]: appearanceValue,
    });
    const legacyCutoffWrite = writeProjectsArchiveCutoff(
      storage,
      NOW_MS,
      NOW_MS,
    );
    assert.equal(legacyCutoffWrite.ok, true);
    const originalLegacyCutoffRaw = raw.get(PROJECTS_ARCHIVE_STORAGE_KEY);
    assert.ok(originalLegacyCutoffRaw);

    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const keySrcB = formatWorkspaceChatIdentityKey(IDENTITY_SRC_B)!;

    // Canonical key round-trip including ':' inside canonical_project_scope
    assert.deepEqual(parseWorkspaceChatIdentityKey(keySrcB), IDENTITY_SRC_B);
    assert.equal(parseWorkspaceChatIdentityKey('Chat A'), null);
    assert.equal(parseWorkspaceChatIdentityKey('job-1'), null);
    assert.equal(parseWorkspaceChatIdentityKey('sess_12345'), null);

    const twoChatsSnapshot = makeWorkspaceSnapshot({
      chats: [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          updated_at_ms: NOW_MS - 1_000,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_A),
        makeNativeSample(IDENTITY_B),
      ],
    });

    const coordinator = new DesktopAppCoordinator(
      {
        fetchWorkspaceSnapshot: async () => twoChatsSnapshot,
        preferenceStorage: storage,
        now: () => NOW_MS,
      },
      () => {},
    );

    await coordinator.refreshWorkspace();

    // Legacy bulk cutoff is NOT migrated or applied; both chats start Active on Messages tab.
    assert.equal(raw.has(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY), false);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(coordinator.getState().selectedTab, 'messages');
    assert.equal(coordinator.projectWorkspace().activeChatCount, 2);
    assert.equal(coordinator.projectWorkspace().archivedChatCount, 0);

    // Non-canonical chat keys are rejected without mutating selectedChatKey.
    coordinator.selectChat('Chat B');
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    coordinator.selectChat('job-1');
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    // Tab switching across Messages -> Jobs -> Activity
    coordinator.setWorkspaceTab('jobs');
    assert.equal(coordinator.projectWorkspace().selectedChat?.activeTab, 'jobs');
    coordinator.setWorkspaceTab('activity');
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.activeTab,
      'activity',
    );
    coordinator.setWorkspaceTab('messages');
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.activeTab,
      'messages',
    );

    // Storage write failure on archive: returns false, preserves state, exposes archiveError!
    storage.failWrites = true;
    const failedArchive = coordinator.setChatArchived(IDENTITY_A, true);
    assert.equal(failedArchive, false);
    assert.deepEqual(coordinator.getState().archivePreferences, {});
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.ok(coordinator.getState().archiveError);
    assert.equal(coordinator.projectWorkspace().activeChatCount, 2);
    assert.equal(coordinator.projectWorkspace().archivedChatCount, 0);

    // Null storage also fails cleanly without pretending archive succeeded.
    const statelessCoordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => twoChatsSnapshot,
      preferenceStorage: null,
      now: () => NOW_MS,
    });
    await statelessCoordinator.refreshWorkspace();
    assert.equal(statelessCoordinator.setChatArchived(IDENTITY_A, true), false);
    assert.ok(statelessCoordinator.getState().archiveError);

    // Storage write succeeds: Chat A moves to Archive, Active selection advances to Chat B, archiveError clears.
    storage.failWrites = false;
    const okArchive = coordinator.setChatArchived(IDENTITY_A, true);
    assert.equal(okArchive, true);
    assert.equal(coordinator.getState().archiveError, null);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.projectWorkspace().activeChatCount, 1);
    assert.equal(coordinator.projectWorkspace().archivedChatCount, 1);

    // Navigate to Archive lifecycle -> selects archived Chat A.
    coordinator.setWorkspaceLifecycle('archived');
    assert.equal(coordinator.getState().workspaceLifecycle, 'archived');
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.lifecycle,
      'archived',
    );

    // Reload: a fresh DesktopAppCoordinator reading the same storage restores Chat A as archived.
    const reloadedCoordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => twoChatsSnapshot,
      preferenceStorage: storage,
      now: () => NOW_MS + 5_000,
    });
    await reloadedCoordinator.refreshWorkspace();
    assert.equal(reloadedCoordinator.getState().selectedTab, 'messages');
    assert.equal(reloadedCoordinator.getState().workspaceLifecycle, 'active');
    assert.equal(reloadedCoordinator.getState().selectedChatKey, keyB);
    assert.equal(reloadedCoordinator.projectWorkspace().activeChatCount, 1);
    assert.equal(reloadedCoordinator.projectWorkspace().archivedChatCount, 1);

    // Storage failure during restore keeps Chat A archived and surfaces error.
    reloadedCoordinator.setWorkspaceLifecycle('archived');
    assert.equal(reloadedCoordinator.getState().selectedChatKey, keyA);
    storage.failWrites = true;
    assert.equal(reloadedCoordinator.setChatArchived(IDENTITY_A, false), false);
    assert.ok(reloadedCoordinator.getState().archiveError);
    assert.equal(reloadedCoordinator.projectWorkspace().archivedChatCount, 1);

    // Restore succeeds once storage recovers.
    storage.failWrites = false;
    assert.equal(reloadedCoordinator.setChatArchived(IDENTITY_A, false), true);
    assert.equal(reloadedCoordinator.getState().archiveError, null);
    assert.equal(reloadedCoordinator.getState().selectedChatKey, null);
    reloadedCoordinator.setWorkspaceLifecycle('active');
    assert.equal(reloadedCoordinator.getState().selectedChatKey, keyA);
    assert.equal(reloadedCoordinator.projectWorkspace().activeChatCount, 2);
    assert.equal(reloadedCoordinator.projectWorkspace().archivedChatCount, 0);

    // Legacy bulk archive key and unrelated appearance preferences were NEVER deleted or modified!
    assert.equal(
      raw.get(PROJECTS_ARCHIVE_STORAGE_KEY),
      originalLegacyCutoffRaw,
    );
    assert.equal(raw.get(appearanceKey), appearanceValue);
  });

  it('coalesces concurrent openCodexChat calls to a single verified send and isolates chat switches from late navigation results', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const targetsSent: CodexChatTarget[] = [];
    const navAttempt1 = deferred<void>();
    const navAttempt2 = deferred<void>();

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () =>
        makeWorkspaceSnapshot({
          chats: [
            makeChatSummary(IDENTITY_A, { thread_label: 'Chat A' }),
            makeChatSummary(IDENTITY_B, { thread_label: 'Chat B' }),
          ],
          native_samples: [
            makeNativeSample(IDENTITY_A),
            makeNativeSample(IDENTITY_B),
          ],
          job_histories: [
            makeJobHistory(IDENTITY_A, {
              job: {
                source_id: 'src-a',
                job_id: 'job-a',
                binding_id: 'bind-a',
                chat_identity: IDENTITY_A,
                title: 'Job A',
                job_revision: 1,
                state: 'running',
                phase: 'execution',
                approval_status: 'approved',
                execution_status: 'running',
                review_state: null,
                created_at_ms: NOW_MS - 10_000,
                updated_at_ms: NOW_MS - 1_000,
                summary: 'Running A',
              },
            }),
            makeJobHistory(IDENTITY_B, {
              job: {
                source_id: 'src-a',
                job_id: 'job-b',
                binding_id: 'bind-b',
                chat_identity: IDENTITY_B,
                title: 'Job B',
                job_revision: 1,
                state: 'running',
                phase: 'execution',
                approval_status: 'approved',
                execution_status: 'running',
                review_state: null,
                created_at_ms: NOW_MS - 10_000,
                updated_at_ms: NOW_MS - 1_000,
                summary: 'Running B',
              },
            }),
          ],
        }),
      openCodexChat: (target) => {
        targetsSent.push(target);
        return targetsSent.length === 1
          ? navAttempt1.promise
          : navAttempt2.promise;
      },
      now: () => NOW_MS,
    });

    await coordinator.refreshWorkspace();
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    // Reject invalid UUID, URL/executable injection, unverified thread, and cross-thread job mismatch.
    assert.equal(
      await coordinator.openCodexChat({
        source_id: 'src-a',
        thread_uuid: 'not-a-uuid',
        job_id: 'job-a',
      }),
      false,
    );
    assert.equal(
      await coordinator.openCodexChat({
        source_id: 'https://evil.example/open',
        thread_uuid: UUID_A,
        job_id: null,
      }),
      false,
    );
    assert.equal(
      await coordinator.openCodexChat({
        source_id: 'src-a',
        thread_uuid: UUID_C,
        job_id: null,
      }),
      false,
    );
    assert.equal(
      await coordinator.openCodexChat({
        source_id: 'src-a',
        thread_uuid: UUID_A,
        job_id: 'job-b',
      }),
      false,
    );
    assert.equal(targetsSent.length, 0);
    assert.ok(coordinator.getState().codexOpenError);

    coordinator.clearCodexOpenError();
    assert.equal(coordinator.getState().codexOpenError, null);

    // Launch verified navigation on Chat A (uppercase UUID is canonicalized to lowercase).
    const pOpenA = coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_A.toUpperCase(),
      job_id: 'job-a',
    });
    assert.equal(coordinator.getState().codexOpenInFlight, true);
    assert.equal(targetsSent.length, 1);
    assert.deepEqual(targetsSent[0], {
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: 'job-a',
    });

    // Duplicate concurrent clicks while in flight coalesce without sending a second IPC call!
    const pDuplicate1 = await coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: 'job-a',
    });
    const pDuplicate2 = await coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_A,
      job_id: null,
    });
    assert.equal(pDuplicate1, false);
    assert.equal(pDuplicate2, false);
    assert.equal(targetsSent.length, 1);

    // Switch to Chat B while Chat A's navigation is still pending:
    // Chat B starts clean and unlocked, and Chat A's late failure MUST NOT pollute Chat B!
    coordinator.selectChat(keyB);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.getState().codexOpenInFlight, false);
    assert.equal(coordinator.getState().codexOpenError, null);

    const pOpenB = coordinator.openCodexChat({
      source_id: 'src-a',
      thread_uuid: UUID_B,
      job_id: 'job-b',
    });
    assert.equal(coordinator.getState().codexOpenInFlight, true);
    assert.equal(targetsSent.length, 2);

    // Chat A's superseded navigation now rejects with an error -> ignored!
    navAttempt1.reject(new Error('Chat A Codex window failed'));
    assert.equal(await pOpenA, false);
    assert.equal(coordinator.getState().codexOpenInFlight, true);
    assert.equal(coordinator.getState().codexOpenError, null);

    // Chat B's active navigation succeeds cleanly.
    navAttempt2.resolve();
    assert.equal(await pOpenB, true);
    assert.equal(coordinator.getState().codexOpenInFlight, false);
    assert.equal(coordinator.getState().codexOpenError, null);
  });

  it('preserves registry cadence, tool version scans, tool discovery, and dirty profile drafts across polls', async () => {
    let wsCalls = 0;
    let regCalls = 0;
    let verCalls = 0;
    let discoveryCalls = 0;
    let currentReg = makeRegistry({ revision: 5 });

    const discoveredSample: ToolDiscoveryEntry[] = [
      {
        id: 'codex-cli',
        label: 'Codex CLI',
        availability: 'available',
        executable_path: '/usr/local/bin/codex',
      },
    ];

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => {
        wsCalls += 1;
        return makeWorkspaceSnapshot({
          generated_at_ms: NOW_MS + wsCalls * 1_000,
        });
      },
      fetchRegistrySnapshot: async () => {
        regCalls += 1;
        return currentReg;
      },
      fetchToolVersions: async (): Promise<DesktopToolVersionsSnapshot> => {
        verCalls += 1;
        return {
          registry_revision: currentReg.revision,
          tools: [],
        };
      },
      fetchToolDiscovery: async () => {
        discoveryCalls += 1;
        return discoveredSample;
      },
      now: () => NOW_MS,
    });

    await coordinator.initialize();
    await flushMicrotasks();

    assert.equal(wsCalls, 1);
    assert.equal(regCalls, 1);
    assert.equal(verCalls, 1);
    assert.equal(discoveryCalls, 1);
    assert.deepEqual(coordinator.getState().discoveredTools, discoveredSample);
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(
      coordinator.getState().toolVersions.inspectedRegistryRevision,
      5,
    );

    // Simulate user editing a profile draft (dirty draft) synchronized with coordinator.getState().registry.
    let profilePanelState = createProfilesPanelState(
      coordinator.getState().registry,
    );
    profilePanelState = updateActiveDraft(profilePanelState, (draft) => ({
      ...draft,
      name: 'Unsaved Custom Delivery Draft',
      nameTouched: true,
    }));
    assert.equal(isDraftDirty(getActiveDraft(profilePanelState)!), true);

    // Run 5 observation poll ticks (< REGISTRY_POLL_EVERY_N_TICKS)
    for (let i = 0; i < 5; i += 1) {
      await coordinator.pollTick();
    }
    assert.equal(wsCalls, 6);
    assert.equal(regCalls, 1);
    assert.equal(verCalls, 1);

    // Advance to REGISTRY_POLL_EVERY_N_TICKS with a newer registry revision:
    // registry refreshes once without rescanning tool versions, and dirty profile draft stays preserved!
    currentReg = makeRegistry({ revision: 6 });
    for (let i = 5; i < REGISTRY_POLL_EVERY_N_TICKS; i += 1) {
      await coordinator.pollTick();
    }
    assert.equal(regCalls, 2);
    assert.equal(verCalls, 1);
    assert.equal(coordinator.getState().registry?.revision, 6);

    profilePanelState = syncRegistrySnapshot(
      profilePanelState,
      coordinator.getState().registry,
    );
    assert.equal(isDraftDirty(getActiveDraft(profilePanelState)!), true);
    assert.equal(
      getActiveDraft(profilePanelState)?.name,
      'Unsaved Custom Delivery Draft',
    );

    // Reject stale external registry revision (revision 3 < 6)
    assert.equal(
      coordinator.applyExternalRegistrySnapshot(makeRegistry({ revision: 3 })),
      false,
    );
    assert.equal(coordinator.getState().registry?.revision, 6);

    // Accept newer external registry revision with modified tool executable path -> triggers version recheck
    currentReg = makeRegistry({
      revision: 8,
      tools: [
        {
          id: 'codex-cli',
          label: 'Codex CLI',
          adapter: 'codex_cli',
          executable_path: '/custom/bin/codex',
          enabled: true,
        },
      ],
    });
    assert.equal(coordinator.applyExternalRegistrySnapshot(currentReg), true);
    assert.equal(coordinator.getState().registry?.revision, 8);
    await flushMicrotasks();
    assert.equal(verCalls, 2);
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(
      coordinator.getState().toolVersions.inspectedRegistryRevision,
      8,
    );

    profilePanelState = syncRegistrySnapshot(
      profilePanelState,
      coordinator.getState().registry,
    );
    assert.equal(isDraftDirty(getActiveDraft(profilePanelState)!), true);
    assert.equal(
      getActiveDraft(profilePanelState)?.name,
      'Unsaved Custom Delivery Draft',
    );
  });

  it('settles a mismatched version response and recovers on an explicit recheck', async () => {
    let responseRevision = 3;
    let versionCalls = 0;
    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => makeWorkspaceSnapshot(),
      fetchRegistrySnapshot: async () => makeRegistry({ revision: 5 }),
      fetchToolVersions: async () => {
        versionCalls += 1;
        return { registry_revision: responseRevision, tools: [] };
      },
      now: () => NOW_MS,
    });

    await coordinator.initialize();
    await flushMicrotasks();
    assert.equal(coordinator.getState().toolVersions.status, 'stale');
    assert.equal(coordinator.getState().toolVersions.activeTicketId, null);
    assert.equal(versionCalls, 1);

    responseRevision = 5;
    coordinator.recheckToolVersions();
    await flushMicrotasks();
    assert.equal(coordinator.getState().toolVersions.status, 'ready');
    assert.equal(
      coordinator.getState().toolVersions.inspectedRegistryRevision,
      5,
    );
    assert.equal(versionCalls, 2);
    coordinator.dispose();
  });

  it('preserves explicit chat selection across incomplete refreshes, omitted sources, and reordering, then restores automatically until another chat is manually selected', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const keySrcB = formatWorkspaceChatIdentityKey(IDENTITY_SRC_B)!;
    const { raw, storage } = createMemoryStorage();
    const openCodexTargets: CodexChatTarget[] = [];
    const emptyMessages: WorkspaceSnapshot['messages'] = {
      availability: 'available',
      items: [],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    };

    let snapshotToServe: WorkspaceSnapshot = makeWorkspaceSnapshot({
      generated_at_ms: NOW_MS + 1_000,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: NOW_MS + 1_000,
        sources: [makeSource({ id: 'src-a', label: 'Alpha Project', project_path: '/work/alpha' })],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 1_000,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_A, { activity: 'idle', sequence: 1 }),
        makeNativeSample(IDENTITY_B, { activity: 'working', sequence: 1 }),
      ],
      job_histories: [],
      messages: emptyMessages,
    });

    let currentNowMs = NOW_MS + 1_000;
    const coordinator = new DesktopAppCoordinator(
      {
        fetchWorkspaceSnapshot: async () => snapshotToServe,
        openCodexChat: async (target) => {
          openCodexTargets.push(target);
        },
        preferenceStorage: storage,
        now: () => currentNowMs,
      },
    );

    // 1. Initial no-selection auto-selects first visible chat (Chat B is newer).
    await coordinator.refreshWorkspace({ mode: 'replace' });
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.projectWorkspace().selectedChat?.threadLabel, 'Chat B');

    // 2. User explicitly selects Chat A.
    coordinator.selectChat(keyA);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(coordinator.projectWorkspace().selectedChat?.threadLabel, 'Chat A');

    // 3. Successful incomplete snapshot returns only Chat B (bounded omission, Chat B newer).
    currentNowMs = NOW_MS + 2_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [makeSource({ id: 'src-a', label: 'Alpha Project', project_path: '/work/alpha' })],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_B, {
          activity: 'working',
          sequence: 2,
          observed_at_ms: currentNowMs - 100,
          last_success_at_ms: currentNowMs - 100,
        }),
      ],
      job_histories: [],
      messages: emptyMessages,
      bounds: {
        native_samples_omitted: 1,
        job_histories_omitted: 0,
        messages_omitted: 0,
        requests_capped: false,
        jobs_capped: false,
      },
    });
    await coordinator.refreshWorkspace({ mode: 'refresh' });

    assert.equal(coordinator.getState().selectedChatKey, keyA);
    const unavailableProj = coordinator.projectWorkspace();
    assert.equal(unavailableProj.selectedChatKey, keyA);
    assert.equal(unavailableProj.selectedChat, null);
    assert.equal(unavailableProj.selectedChatUnavailable?.reason, 'chat_absent');
    assert.equal(unavailableProj.selectedChatUnavailable?.threadLabel, 'Chat A');
    assert.equal(
      unavailableProj.selectedChatUnavailable?.projectLabel,
      'Alpha Project',
    );
    assert.equal(
      unavailableProj.selectedChatUnavailable?.statusLabel,
      'Temporarily unavailable',
    );
    assert.doesNotMatch(
      unavailableProj.selectedChatUnavailable?.message ?? '',
      /\b(?:archived?|deleted?|removed?)\b/i,
    );
    // Absence of evidence must never write archive preferences.
    assert.deepEqual(coordinator.getState().archivePreferences, {});
    assert.equal(raw.get(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY), undefined);
    // Opening Codex for the temporarily unavailable selected chat must be refused.
    assert.equal(
      await coordinator.openCodexChat({
        source_id: IDENTITY_A.source_id,
        thread_uuid: IDENTITY_A.thread_uuid,
        job_id: null,
      }),
      false,
    );
    assert.equal(openCodexTargets.length, 0);
    assert.match(
      coordinator.getState().codexOpenError ?? '',
      /temporarily unavailable/i,
    );
    coordinator.clearCodexOpenError();

    // 4. Snapshot omits src-a from observation.sources entirely (only src-b present).
    currentNowMs = NOW_MS + 3_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [
          makeSource({
            id: 'src-b',
            label: 'Beta Service',
            project_path: '/work/beta:service',
          }),
        ],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_SRC_B, {
          thread_label: 'Src B Chat',
          project_label: 'Beta Service',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      native_samples: [makeNativeSample(IDENTITY_SRC_B)],
      job_histories: [],
      messages: emptyMessages,
    });
    await coordinator.refreshWorkspace({ mode: 'refresh' });

    assert.equal(coordinator.getState().selectedChatKey, keyA);
    const omittedSourceProj = coordinator.projectWorkspace();
    assert.equal(omittedSourceProj.selectedChat, null);
    assert.equal(
      omittedSourceProj.selectedChatUnavailable?.reason,
      'source_omitted',
    );
    assert.equal(omittedSourceProj.selectedChatUnavailable?.threadLabel, 'Chat A');
    assert.equal(
      await coordinator.openCodexChat({
        source_id: IDENTITY_A.source_id,
        thread_uuid: IDENTITY_A.thread_uuid,
        job_id: null,
      }),
      false,
    );
    assert.equal(openCodexTargets.length, 0);
    coordinator.clearCodexOpenError();

    // 5. Snapshot restores A+B reordered (B is newer and ordered first) -> A restores automatically.
    currentNowMs = NOW_MS + 4_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [makeSource({ id: 'src-a', label: 'Alpha Project', project_path: '/work/alpha' })],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_B, { sequence: 3 }),
        makeNativeSample(IDENTITY_A, { sequence: 2 }),
      ],
      job_histories: [],
      messages: emptyMessages,
    });
    await coordinator.refreshWorkspace({ mode: 'refresh' });

    assert.equal(coordinator.getState().selectedChatKey, keyA);
    const restoredProj = coordinator.projectWorkspace();
    assert.equal(restoredProj.visibleProjects[0]?.chats[0]?.chatKey, keyB);
    assert.equal(restoredProj.selectedChat?.threadLabel, 'Chat A');
    assert.equal(restoredProj.selectedChatUnavailable, null);
    assert.equal(
      await coordinator.openCodexChat({
        source_id: IDENTITY_A.source_id,
        thread_uuid: IDENTITY_A.thread_uuid,
        job_id: null,
      }),
      true,
    );
    assert.equal(openCodexTargets.length, 1);

    // 6. A disappears again, user manually selects B, then A+B returns -> stays on B.
    currentNowMs = NOW_MS + 5_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [makeSource({ id: 'src-a', label: 'Alpha Project', project_path: '/work/alpha' })],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
      ],
      native_samples: [makeNativeSample(IDENTITY_B, { sequence: 4 })],
      job_histories: [],
      messages: emptyMessages,
    });
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    coordinator.selectChat(keyB);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.projectWorkspace().selectedChat?.threadLabel, 'Chat B');

    currentNowMs = NOW_MS + 6_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [makeSource({ id: 'src-a', label: 'Alpha Project', project_path: '/work/alpha' })],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      native_samples: [
        makeNativeSample(IDENTITY_A, { sequence: 3 }),
        makeNativeSample(IDENTITY_B, { sequence: 5 }),
      ],
      job_histories: [],
      messages: emptyMessages,
    });
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.projectWorkspace().selectedChat?.threadLabel, 'Chat B');

    // 7. Explicit archive/restore, lifecycle change, and scope change still work.
    assert.equal(coordinator.setChatArchived(IDENTITY_B, true), true);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    coordinator.setWorkspaceLifecycle('archived');
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.setChatArchived(IDENTITY_B, false), true);
    assert.equal(coordinator.getState().selectedChatKey, null);
    coordinator.setWorkspaceLifecycle('active');
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    currentNowMs = NOW_MS + 7_000;
    snapshotToServe = makeWorkspaceSnapshot({
      generated_at_ms: currentNowMs,
      observation: makeLegacyObservationSnapshot({
        generated_at_ms: currentNowMs,
        sources: [
          makeSource({
            id: 'src-b',
            label: 'Beta Service',
            project_path: '/work/beta:service',
          }),
        ],
        jobs: [],
        requests: [],
      }),
      chats: [
        makeChatSummary(IDENTITY_SRC_B, {
          thread_label: 'Src B Chat',
          project_label: 'Beta Service',
        }),
      ],
      native_samples: [makeNativeSample(IDENTITY_SRC_B)],
      job_histories: [],
      messages: emptyMessages,
    });
    coordinator.setObservationScope({ source_id: 'src-b', job_id: 'job-b1' });
    assert.equal(coordinator.getState().selectedChatKey, null);
    await flushMicrotasks();
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);

    coordinator.dispose();
  });

  it('preserves explicit chat selection across same-scope replace reads with unchanged source_id/job_id until another chat is manually selected or archived', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const { raw, storage } = createMemoryStorage();
    const requestsReceived: ObservationSnapshotRequest[] = [];
    const openCodexTargets: CodexChatTarget[] = [];
    const emptyMessages: WorkspaceSnapshot['messages'] = {
      availability: 'available',
      items: [],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    };

    const makeScopedSnapshot = (
      generatedAtMs: number,
      chats: WorkspaceChatSummary[],
      nativeSamples: WorkspaceNativeSample[],
    ): WorkspaceSnapshot =>
      makeWorkspaceSnapshot({
        generated_at_ms: generatedAtMs,
        observation: makeLegacyObservationSnapshot({
          generated_at_ms: generatedAtMs,
          sources: [
            makeSource({
              id: 'src-a',
              label: 'Alpha Project',
              project_path: '/work/alpha',
            }),
          ],
          jobs: [],
          requests: [],
        }),
        chats,
        native_samples: nativeSamples,
        job_histories: [],
        messages: emptyMessages,
      });

    let currentNowMs = NOW_MS + 1_000;
    let snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 1_000,
        }),
      ],
      [
        makeNativeSample(IDENTITY_A, { activity: 'idle', sequence: 1 }),
        makeNativeSample(IDENTITY_B, { activity: 'working', sequence: 1 }),
      ],
    );

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async (req) => {
        requestsReceived.push(req ?? {});
        return snapshotToServe;
      },
      openCodexChat: async (target) => {
        openCodexTargets.push(target);
      },
      preferenceStorage: storage,
      now: () => currentNowMs,
    });

    // 1. Enter scoped filter { source_id: 'src-a', job_id: 'job-1' } with two observed chats A+B.
    // Default navigation auto-selects the first visible chat (Chat B is newer).
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-1' });
    await flushMicrotasks();
    assert.equal(requestsReceived.length, 1);
    assert.deepEqual(requestsReceived[0], {
      source_id: 'src-a',
      job_id: 'job-1',
      cursor: null,
      limit: DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
    });
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat B',
    );

    // 2. User explicitly selects Chat A.
    coordinator.selectChat(keyA);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat A',
    );

    // Re-applying the exact same scope filter is a no-op and does not reset snapshot or selection.
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-1' });
    assert.equal(requestsReceived.length, 1);
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    // 3. Same-scope refreshWorkspace({ mode: 'replace' }) returns only Chat B (Chat A absent).
    // Must stay on Chat A (temporarily unavailable) instead of jumping to Chat B.
    currentNowMs = NOW_MS + 2_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      [
        makeNativeSample(IDENTITY_B, {
          activity: 'working',
          sequence: 2,
          observed_at_ms: currentNowMs - 100,
          last_success_at_ms: currentNowMs - 100,
        }),
      ],
    );
    await coordinator.refreshWorkspace({ mode: 'replace' });

    assert.equal(requestsReceived.length, 2);
    assert.equal(requestsReceived[1]?.source_id, 'src-a');
    assert.equal(requestsReceived[1]?.job_id, 'job-1');
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    const unavailableProj = coordinator.projectWorkspace();
    assert.equal(unavailableProj.selectedChatKey, keyA);
    assert.equal(unavailableProj.selectedChat, null);
    assert.equal(unavailableProj.selectedChatUnavailable?.reason, 'chat_absent');
    assert.equal(unavailableProj.selectedChatUnavailable?.threadLabel, 'Chat A');
    assert.equal(
      unavailableProj.selectedChatUnavailable?.projectLabel,
      'Alpha Project',
    );
    assert.deepEqual(coordinator.getState().archivePreferences, {});
    assert.equal(raw.get(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY), undefined);
    assert.equal(
      await coordinator.openCodexChat({
        source_id: IDENTITY_A.source_id,
        thread_uuid: IDENTITY_A.thread_uuid,
        job_id: null,
      }),
      false,
    );
    assert.equal(openCodexTargets.length, 0);
    coordinator.clearCodexOpenError();

    // 4. Same-scope refreshWorkspace({ mode: 'replace' }) restores A+B (B still newer and ordered first).
    // Selection remains Chat A (A -> A unavailable -> A, never A -> B -> B).
    currentNowMs = NOW_MS + 3_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      [
        makeNativeSample(IDENTITY_B, { sequence: 3 }),
        makeNativeSample(IDENTITY_A, { sequence: 2 }),
      ],
    );
    await coordinator.refreshWorkspace({ mode: 'replace' });

    assert.equal(coordinator.getState().selectedChatKey, keyA);
    const restoredProj = coordinator.projectWorkspace();
    assert.equal(restoredProj.visibleProjects[0]?.chats[0]?.chatKey, keyB);
    assert.equal(restoredProj.selectedChat?.threadLabel, 'Chat A');
    assert.equal(restoredProj.selectedChatUnavailable, null);
    assert.equal(
      await coordinator.openCodexChat({
        source_id: IDENTITY_A.source_id,
        thread_uuid: IDENTITY_A.thread_uuid,
        job_id: null,
      }),
      true,
    );
    assert.equal(openCodexTargets.length, 1);

    // 5. Explicit selection after absence still switches to Chat B and holds across A+B replace.
    currentNowMs = NOW_MS + 4_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
      ],
      [makeNativeSample(IDENTITY_B, { sequence: 4 })],
    );
    await coordinator.refreshWorkspace({ mode: 'replace' });
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    coordinator.selectChat(keyB);
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat B',
    );

    currentNowMs = NOW_MS + 5_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      [
        makeNativeSample(IDENTITY_A, { sequence: 3 }),
        makeNativeSample(IDENTITY_B, { sequence: 5 }),
      ],
    );
    await coordinator.refreshWorkspace({ mode: 'replace' });
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat B',
    );

    // 6. Archive and restore still update selection within the active scope.
    assert.equal(coordinator.setChatArchived(IDENTITY_B, true), true);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    coordinator.setWorkspaceLifecycle('archived');
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(coordinator.setChatArchived(IDENTITY_B, false), true);
    assert.equal(coordinator.getState().selectedChatKey, null);
    coordinator.setWorkspaceLifecycle('active');
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    coordinator.dispose();
  });

  it('resets absent selection on explicit setObservationScope navigation across job_id, failed/empty first reads, and source_id changes', async () => {
    const keyA = formatWorkspaceChatIdentityKey(IDENTITY_A)!;
    const keyB = formatWorkspaceChatIdentityKey(IDENTITY_B)!;
    const keySrcB = formatWorkspaceChatIdentityKey(IDENTITY_SRC_B)!;
    const requestsReceived: ObservationSnapshotRequest[] = [];
    const emptyMessages: WorkspaceSnapshot['messages'] = {
      availability: 'available',
      items: [],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    };

    const makeScopedSnapshot = (
      generatedAtMs: number,
      sources: ObservationSource[],
      chats: WorkspaceChatSummary[],
      nativeSamples: WorkspaceNativeSample[],
    ): WorkspaceSnapshot =>
      makeWorkspaceSnapshot({
        generated_at_ms: generatedAtMs,
        observation: makeLegacyObservationSnapshot({
          generated_at_ms: generatedAtMs,
          sources,
          jobs: [],
          requests: [],
        }),
        chats,
        native_samples: nativeSamples,
        job_histories: [],
        messages: emptyMessages,
      });

    const sourceA = makeSource({
      id: 'src-a',
      label: 'Alpha Project',
      project_path: '/work/alpha',
    });
    const sourceB = makeSource({
      id: 'src-b',
      label: 'Beta Service',
      project_path: '/work/beta:service',
    });

    let currentNowMs = NOW_MS + 1_000;
    let shouldFail = false;
    let snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA],
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 2_000,
        }),
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: NOW_MS - 1_000,
        }),
      ],
      [
        makeNativeSample(IDENTITY_A, { sequence: 1 }),
        makeNativeSample(IDENTITY_B, { sequence: 1 }),
      ],
    );

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async (req) => {
        requestsReceived.push(req ?? {});
        if (shouldFail) {
          throw new Error('Scoped snapshot unavailable');
        }
        return snapshotToServe;
      },
      now: () => currentNowMs,
    });

    // 1. Load initial filter { source_id: 'src-a', job_id: 'job-1' } with A+B and select A.
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-1' });
    await flushMicrotasks();
    coordinator.selectChat(keyA);
    assert.equal(coordinator.getState().selectedChatKey, keyA);

    // 2. Explicit job_id filter change within the SAME source_id -> snapshot only has Chat B.
    // Must switch selection from Chat A to Chat B rather than keeping stale Chat A.
    currentNowMs = NOW_MS + 2_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA],
      [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      [makeNativeSample(IDENTITY_B, { sequence: 2 })],
    );
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-2' });
    assert.equal(coordinator.getState().workspaceSnapshot, null);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    await flushMicrotasks();

    assert.equal(requestsReceived.at(-1)?.source_id, 'src-a');
    assert.equal(requestsReceived.at(-1)?.job_id, 'job-2');
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat B',
    );
    assert.equal(coordinator.projectWorkspace().selectedChatUnavailable, null);

    // 3. Explicit job_id filter change whose initial read fails, then succeeds on retry with only Chat A.
    // Because no snapshot committed for job-3 yet, retry must still switch from Chat B to Chat A.
    currentNowMs = NOW_MS + 3_000;
    shouldFail = true;
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-3' });
    await flushMicrotasks();
    assert.equal(coordinator.getState().workspaceSnapshot, null);
    assert.equal(
      coordinator.getState().observationError,
      'Scoped snapshot unavailable',
    );
    assert.equal(coordinator.getState().selectedChatKey, keyB);

    shouldFail = false;
    currentNowMs = NOW_MS + 4_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA],
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      [makeNativeSample(IDENTITY_A, { sequence: 2 })],
    );
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(coordinator.getState().observationError, null);
    assert.equal(coordinator.getState().selectedChatKey, keyA);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat A',
    );

    // 4. Explicit job_id filter change whose first snapshot is empty, then populates with Chat B.
    // Empty first snapshot clears carried-over Chat A so Chat B auto-selects when it arrives.
    currentNowMs = NOW_MS + 5_000;
    snapshotToServe = makeScopedSnapshot(currentNowMs, [sourceA], [], []);
    coordinator.setObservationScope({ source_id: 'src-a', job_id: 'job-4' });
    await flushMicrotasks();
    assert.equal(coordinator.getState().selectedChatKey, null);
    assert.equal(coordinator.projectWorkspace().selectedChatUnavailable, null);

    currentNowMs = NOW_MS + 6_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA],
      [
        makeChatSummary(IDENTITY_B, {
          thread_label: 'Chat B',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      [makeNativeSample(IDENTITY_B, { sequence: 3 })],
    );
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(coordinator.getState().selectedChatKey, keyB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Chat B',
    );

    // 5. Explicit filter change to a different source_id clears selection synchronously and selects src-b chat.
    currentNowMs = NOW_MS + 7_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceB],
      [
        makeChatSummary(IDENTITY_SRC_B, {
          thread_label: 'Beta Chat',
          project_label: 'Beta Service',
          updated_at_ms: currentNowMs - 100,
        }),
      ],
      [makeNativeSample(IDENTITY_SRC_B, { sequence: 1 })],
    );
    coordinator.setObservationScope({ source_id: 'src-b', job_id: 'job-b1' });
    assert.equal(coordinator.getState().selectedChatKey, null);
    await flushMicrotasks();
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Beta Chat',
    );

    // 6. Explicit filter reset to null keeps current selection and preserves it on subsequent replace absence.
    currentNowMs = NOW_MS + 8_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA, sourceB],
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
        makeChatSummary(IDENTITY_SRC_B, {
          thread_label: 'Beta Chat',
          project_label: 'Beta Service',
          updated_at_ms: NOW_MS - 2_000,
        }),
      ],
      [
        makeNativeSample(IDENTITY_A, { sequence: 3 }),
        makeNativeSample(IDENTITY_SRC_B, { sequence: 2 }),
      ],
    );
    coordinator.setObservationScope(null);
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
    await flushMicrotasks();
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
    assert.equal(
      coordinator.projectWorkspace().selectedChat?.threadLabel,
      'Beta Chat',
    );

    currentNowMs = NOW_MS + 9_000;
    snapshotToServe = makeScopedSnapshot(
      currentNowMs,
      [sourceA, sourceB],
      [
        makeChatSummary(IDENTITY_A, {
          thread_label: 'Chat A',
          project_label: 'Alpha Project',
          updated_at_ms: currentNowMs - 50,
        }),
      ],
      [makeNativeSample(IDENTITY_A, { sequence: 4 })],
    );
    await coordinator.refreshWorkspace({ mode: 'replace' });
    assert.equal(coordinator.getState().selectedChatKey, keySrcB);
    assert.equal(
      coordinator.projectWorkspace().selectedChatUnavailable?.reason,
      'chat_absent',
    );
    assert.equal(
      coordinator.projectWorkspace().selectedChatUnavailable?.threadLabel,
      'Beta Chat',
    );

    coordinator.dispose();
  });

  it('reconciles regressed Worker presence on newer refreshes only for the exact chat, item, execution, and trustworthy origin/epoch without extending expiry', async () => {
    const makeWorkerItem = (
      bindingId: string,
      overrides: Partial<BacklogItem> = {},
    ): BacklogItem => ({
      item_id: 'blg_worker_01',
      binding_id: bindingId,
      revision: 3,
      title: 'Fix Worker presence flicker',
      acceptance_condition: 'Single row stays stable across brief gaps.',
      delivery_description: 'Frontend reconciliation and continuity.',
      kind: 'worker',
      status: 'doing',
      blocked_reason: null,
      current_execution_ref: 'exec_worker_01',
      completion_note: null,
      completion_evidence: null,
      completed_at_ms: null,
      created_at_ms: NOW_MS - 40_000,
      updated_at_ms: NOW_MS - 15_000,
      ...overrides,
    });

    const makeWorkerEvent = (
      overrides: Partial<WorkspaceBacklogEvent> = {},
    ): WorkspaceBacklogEvent => ({
      event_id: 'blg_worker_01:3',
      sequence: 3,
      item_id: 'blg_worker_01',
      revision: 3,
      action: 'worker_execution_attached',
      recorded_at_ms: NOW_MS - 15_000,
      title: 'Worker execution attached',
      status: 'doing',
      execution_ref: 'exec_worker_01',
      outcome: null,
      note: null,
      ...overrides,
    });

    const makeWorkerExecution = (
      overrides: Partial<WorkspaceWorkerExecution> = {},
    ): WorkspaceWorkerExecution => ({
      item_id: 'blg_worker_01',
      execution_ref: 'exec_worker_01',
      approval_id: 'apr_worker_01',
      task_id: 'task_worker_01',
      session_binding_id: 'sbx_runtime_01',
      run_id: 'run_worker_01',
      worker_id: 'wrk_worker_01',
      provider: 'google',
      requested_model: 'gemini-4-argon-high',
      observed_model: 'gemini-4-argon-high',
      requested_effort: 'high',
      observed_effort: 'high',
      worker_status: 'running',
      started_at_ms: NOW_MS - 14_000,
      finished_at_ms: null,
      duration_ms: null,
      outcome: null,
      acceptance: null,
      ...overrides,
    });

    const makeWorkerPresence = (
      overrides: Partial<WorkspaceBacklogWorkerPresence> = {},
    ): WorkspaceBacklogWorkerPresence => ({
      item_id: 'blg_worker_01',
      execution_ref: 'exec_worker_01',
      approval_id: 'apr_worker_01',
      task_id: 'task_worker_01',
      run_id: 'run_worker_01',
      worker_id: 'wrk_worker_01',
      claim_id: 'claim_01',
      host_instance_id: 'host_01',
      invocation_id: 'inv_01',
      sequence: 12,
      availability: 'live',
      freshness: 'fresh',
      live: true,
      sampled_at_ms: NOW_MS - 1_000,
      sample_age_ms: 1_000,
      max_sample_age_ms: 5_000,
      expires_at_ms: NOW_MS + 4_000,
      ...overrides,
    });

    const makeChatWorkSnapshot = (
      generatedAtMs: number,
      chatWork: WorkspaceChatWork[],
      sources: ObservationSource[] = [makeSource()],
    ): WorkspaceSnapshot =>
      makeWorkspaceSnapshot({
        generated_at_ms: generatedAtMs,
        observation: makeLegacyObservationSnapshot({
          generated_at_ms: generatedAtMs,
          sources,
          jobs: [],
          requests: [],
        }),
        chats: [
          makeChatSummary(IDENTITY_A, { thread_label: 'Chat A' }),
          makeChatSummary(IDENTITY_B, { thread_label: 'Chat B' }),
        ],
        native_samples: [
          makeNativeSample(IDENTITY_A, {
            sequence: Math.max(1, Math.trunc((generatedAtMs - NOW_MS) / 500) + 1),
            observed_at_ms: generatedAtMs - 500,
            last_success_at_ms: generatedAtMs - 500,
            activity: 'idle',
          }),
          makeNativeSample(IDENTITY_B, {
            sequence: 1,
            observed_at_ms: generatedAtMs - 500,
            last_success_at_ms: generatedAtMs - 500,
            activity: 'idle',
          }),
        ],
        job_histories: [],
        chat_work: chatWork,
      });

    const makeAlphaWork = (
      overrides: Partial<WorkspaceChatWork> = {},
    ): WorkspaceChatWork => ({
      identity: IDENTITY_A,
      availability: 'available',
      items: [makeWorkerItem(IDENTITY_A.binding_id)],
      events: [makeWorkerEvent()],
      worker_presence: [makeWorkerPresence()],
      worker_executions: [makeWorkerExecution()],
      bounds: {
        items_omitted: 0,
        events_omitted: 0,
        presence_omitted: 0,
        executions_omitted: 0,
      },
      ...overrides,
    });

    const makeBetaWork = (
      overrides: Partial<WorkspaceChatWork> = {},
    ): WorkspaceChatWork => ({
      identity: IDENTITY_B,
      availability: 'available',
      items: [makeWorkerItem(IDENTITY_B.binding_id)],
      events: [makeWorkerEvent()],
      worker_presence: [],
      worker_executions: [
        makeWorkerExecution({ session_binding_id: 'sbx_runtime_beta_01' }),
      ],
      bounds: {
        items_omitted: 0,
        events_omitted: 0,
        presence_omitted: 0,
        executions_omitted: 0,
      },
      ...overrides,
    });

    let currentNowMs = NOW_MS;
    let snapshotToServe = makeChatWorkSnapshot(NOW_MS, [
      makeAlphaWork(),
      makeBetaWork(),
    ]);

    const coordinator = new DesktopAppCoordinator({
      fetchWorkspaceSnapshot: async () => snapshotToServe,
      now: () => currentNowMs,
    });

    await coordinator.initialize();
    const initialAlphaPresence =
      coordinator.getState().workspaceSnapshot?.chat_work[0]?.worker_presence[0];
    assert.equal(initialAlphaPresence?.sequence, 12);
    assert.equal(initialAlphaPresence?.expires_at_ms, NOW_MS + 4_000);

    // 1. Newer overall snapshot carrying an older sequence for the exact same
    // chat, item, execution, and host/invocation/claim keeps seq 12 verbatim
    // without extending its original expires_at_ms or inflating presence_omitted.
    currentNowMs = NOW_MS + 1_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 11,
            sampled_at_ms: NOW_MS - 3_000,
            sample_age_ms: 4_000,
            expires_at_ms: NOW_MS + 2_000,
          }),
        ],
      }),
      makeBetaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 1,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS - 3_000,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    const afterRegressedSeq = coordinator.getState().workspaceSnapshot!;
    assert.equal(afterRegressedSeq.generated_at_ms, NOW_MS + 1_000);
    const alphaAfterSeq = afterRegressedSeq.chat_work.find(
      (entry) =>
        formatWorkspaceChatIdentityKey(entry.identity) ===
        formatWorkspaceChatIdentityKey(IDENTITY_A),
    )!;
    const betaAfterSeq = afterRegressedSeq.chat_work.find(
      (entry) =>
        formatWorkspaceChatIdentityKey(entry.identity) ===
        formatWorkspaceChatIdentityKey(IDENTITY_B),
    )!;
    assert.equal(alphaAfterSeq.worker_presence.length, 1);
    assert.equal(alphaAfterSeq.worker_presence[0]!.sequence, 12);
    assert.equal(alphaAfterSeq.worker_presence[0]!.sampled_at_ms, NOW_MS - 1_000);
    assert.equal(alphaAfterSeq.worker_presence[0]!.expires_at_ms, NOW_MS + 4_000);
    assert.equal(alphaAfterSeq.bounds.presence_omitted, 0);
    // Sibling chat with identical item/execution/host tokens remains isolated.
    assert.equal(betaAfterSeq.worker_presence[0]!.availability, 'unavailable');
    assert.equal(betaAfterSeq.worker_presence[0]!.sequence, 1);

    // 2. Same sequence with regressed timestamp or stale degradation of the same
    // sample also retains the fresher sample without changing its original expiry.
    currentNowMs = NOW_MS + 1_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 12,
            availability: 'live',
            freshness: 'stale',
            live: false,
            sampled_at_ms: NOW_MS - 1_000,
            sample_age_ms: 2_500,
            expires_at_ms: NOW_MS + 4_000,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    const afterSameSeqStale =
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!;
    assert.equal(afterSameSeqStale.sequence, 12);
    assert.equal(afterSameSeqStale.freshness, 'fresh');
    assert.equal(afterSameSeqStale.live, true);
    assert.equal(afterSameSeqStale.expires_at_ms, NOW_MS + 4_000);

    // 3. Newer sequence unavailable sample is NOT suppressed by older live seq 12.
    currentNowMs = NOW_MS + 2_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 13,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 1_800,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .sequence,
      13,
    );

    // 4. Recovery to fresh seq 14, then explicit non_live at the same seq 14 wins
    // immediately, and a subsequent regressed live seq 13 cannot resurrect Working.
    currentNowMs = NOW_MS + 3_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 14,
            sampled_at_ms: NOW_MS + 2_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 7_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.live,
      true,
    );

    currentNowMs = NOW_MS + 3_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 14,
            availability: 'non_live',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 2_500,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'non_live',
    );

    currentNowMs = NOW_MS + 4_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            sequence: 13,
            sampled_at_ms: NOW_MS + 2_000,
            sample_age_ms: 2_000,
            expires_at_ms: NOW_MS + 7_000,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'non_live',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .sequence,
      14,
    );

    // 5. Restarted host, new invocation/claim epoch, or untrustworthy null origin
    // never carries previous epoch state forward.
    currentNowMs = NOW_MS + 5_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_02',
            invocation_id: 'inv_02',
            claim_id: 'claim_02',
            sequence: 1,
            sampled_at_ms: NOW_MS + 4_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 9_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .host_instance_id,
      'host_02',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.live,
      true,
    );

    currentNowMs = NOW_MS + 5_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 1,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 4_000,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .host_instance_id,
      'host_03',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );

    // Untrustworthy null origin/epoch never retains previous live presence.
    currentNowMs = NOW_MS + 5_700;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 4,
            sampled_at_ms: NOW_MS + 5_400,
            sample_age_ms: 300,
            expires_at_ms: NOW_MS + 10_400,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.live,
      true,
    );

    currentNowMs = NOW_MS + 5_850;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: null,
            invocation_id: null,
            claim_id: null,
            sequence: 1,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 4_000,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .host_instance_id,
      null,
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );

    // Re-seed live on host_03 seq 5, then test foreign run, omitted presence,
    // terminal execution, and unavailable source.
    currentNowMs = NOW_MS + 6_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 5,
            sampled_at_ms: NOW_MS + 5_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 10_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .sequence,
      5,
    );

    // Foreign run on same item/execution_ref never inherits host_03 seq 5.
    currentNowMs = NOW_MS + 6_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_executions: [
          makeWorkerExecution({ run_id: 'run_foreign_02', worker_id: 'wrk_foreign_02' }),
        ],
        worker_presence: [
          makeWorkerPresence({
            run_id: 'run_foreign_02',
            worker_id: 'wrk_foreign_02',
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 1,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 5_000,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.run_id,
      'run_foreign_02',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );

    // Re-seed live on original attempt, then verify omitted presence is NOT resurrected.
    currentNowMs = NOW_MS + 7_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 6,
            sampled_at_ms: NOW_MS + 6_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 11_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence.length,
      1,
    );

    currentNowMs = NOW_MS + 7_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({ worker_presence: [] }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.deepEqual(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence,
      [],
    );

    // Re-seed live, then verify terminal execution immediately wins over retained live.
    currentNowMs = NOW_MS + 8_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 7,
            sampled_at_ms: NOW_MS + 7_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 12_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.live,
      true,
    );

    currentNowMs = NOW_MS + 8_500;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_executions: [
          makeWorkerExecution({
            worker_status: 'failed',
            outcome: 'failure',
            finished_at_ms: NOW_MS + 8_400,
            duration_ms: 22_400,
          }),
        ],
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 6,
            availability: 'unavailable',
            freshness: 'not_applicable',
            live: false,
            sampled_at_ms: NOW_MS + 7_000,
            sample_age_ms: null,
            expires_at_ms: null,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_executions?.[0]!
        .worker_status,
      'failed',
    );

    // Re-seed live, then verify unavailable source never retains prior live presence.
    currentNowMs = NOW_MS + 9_000;
    snapshotToServe = makeChatWorkSnapshot(currentNowMs, [
      makeAlphaWork({
        worker_presence: [
          makeWorkerPresence({
            host_instance_id: 'host_03',
            invocation_id: 'inv_03',
            claim_id: 'claim_03',
            sequence: 8,
            sampled_at_ms: NOW_MS + 8_500,
            sample_age_ms: 500,
            expires_at_ms: NOW_MS + 13_500,
          }),
        ],
      }),
      makeBetaWork(),
    ]);
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!.live,
      true,
    );

    currentNowMs = NOW_MS + 9_500;
    snapshotToServe = makeChatWorkSnapshot(
      currentNowMs,
      [
        makeAlphaWork({
          worker_presence: [
            makeWorkerPresence({
              host_instance_id: 'host_03',
              invocation_id: 'inv_03',
              claim_id: 'claim_03',
              sequence: 7,
              availability: 'unavailable',
              freshness: 'not_applicable',
              live: false,
              sampled_at_ms: NOW_MS + 8_000,
              sample_age_ms: null,
              expires_at_ms: null,
            }),
          ],
        }),
        makeBetaWork(),
      ],
      [makeSource({ availability: 'unavailable', message: 'Source unreachable' })],
    );
    await coordinator.refreshWorkspace({ mode: 'refresh' });
    assert.equal(
      coordinator.getState().workspaceSnapshot!.chat_work[0]!.worker_presence[0]!
        .availability,
      'unavailable',
    );

    coordinator.dispose();
  });
});
