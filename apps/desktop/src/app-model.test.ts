import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  DEFAULT_WORKSPACE_CHAT_TAB,
  PROJECTS_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_SURFACE_TABS,
  formatWorkspaceChatIdentityKey,
  formatWorkspaceProjectGroupKey,
  isCanonicalUuid,
  normalizeWorkspaceSnapshot,
  projectDesktopWorkspace,
  projectWorkspaceSnapshot,
  readProjectsArchiveCutoff,
  readWorkspaceChatArchivePreferences,
  regenerateWorkspaceSnapshotAt,
  resolveObservationRequestChatIdentity,
  setWorkspaceChatArchivePreference,
  validateWorkspaceChatIdentity,
  writeProjectsArchiveCutoff,
  writeWorkspaceChatArchivePreferences,
  type ProjectsArchiveStorage,
  type WorkspaceChatIdentity,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceJobHistory,
  type WorkspaceNativeSample,
  type WorkspaceSnapshot,
} from './app-model.ts';
import type {
  ObservationActor,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from './observation/types.ts';
import {
  DEFAULT_WORKSPACE_SAMPLING_POLICY,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  createSafeWorkspaceMessageItem,
  createVerifiedConfiguredSelection,
} from './workspace-contract.ts';

const NOW_MS = Date.parse('2026-11-09T10:00:00.000Z');
const UUID_CHAT_A = '11111111-2222-4333-8444-555555555555';
const UUID_CHAT_B = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee';

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src-a',
    label: 'Alpha Repo',
    project_path: '/work/alpha',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeActor(
  overrides: Partial<ObservationActor> = {}
): ObservationActor {
  return {
    label: 'Operator',
    role: 'operator',
    source: 'codex',
    model: null,
    effort: null,
    evidence: 'observed',
    ...overrides,
  };
}

function makeExecutor(
  overrides: Partial<ObservationExecutor> = {}
): ObservationExecutor {
  return {
    tool: 'agy',
    provider: 'antigravity',
    role: 'coder',
    requested_model: 'gemini-4-argon-high',
    observed_model: 'gemini-4-argon-high',
    requested_effort: 'high',
    observed_effort: 'high',
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-a',
    id: 'job-1',
    project_label: 'Alpha Repo',
    project_path: '/work/alpha',
    title: 'Implement desktop shell',
    thread_uuid: UUID_CHAT_A,
    thread_label: 'Desktop UI migration',
    binding_id: 'bind-a',
    plan_id: 'plan-1',
    plan_revision: 2,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 10_000,
    active_call_count: 1,
    tasks: [
      {
        id: 'task-1',
        title: 'Coordinator state machine',
        status: 'running',
        depends_on: [],
      },
    ],
    summary: 'Implementing desktop coordinator.',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src-a',
    id: 'req-1',
    job_id: 'job-1',
    parent_request_id: null,
    title: 'Worker handoff',
    operation: 'flow.handoff',
    project_label: 'Alpha Repo',
    binding_id: 'bind-a',
    thread_uuid: UUID_CHAT_A,
    thread_label: 'Desktop UI migration',
    state: 'running',
    started_at_ms: NOW_MS - 30_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 5_000,
    duration_ms: null,
    actor: makeActor(),
    executor: makeExecutor(),
    usage: {
      input_tokens: 12_000,
      output_tokens: 2_400,
      cache_read_tokens: 0,
      total_tokens: 14_400,
      provenance: 'provider',
    },
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Executing worker slice.',
    ...overrides,
  };
}

function makeSnapshot(
  overrides: Partial<ObservationSnapshot> = {}
): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

describe('app-model workspace and journal projection', () => {
  it('shows a settled failure without inventing a pending human-input request', () => {
    const snapshot = makeSnapshot({
      jobs: [makeJob({
        state: 'failed', execution_status: 'failed', active_call_count: 0,
        approval_status: 'approved',
        tasks: [{ id: 'failed-task', title: 'Failed check', status: 'failed', depends_on: [] }],
      })],
    });
    const projected = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    const chat = projected.projects[0]!.chats[0]!;
    assert.equal(projected.totalNeedsHumanCount, 0);
    assert.equal(chat.hasNeedsHuman, false);
    assert.equal(chat.primaryDotTone, 'danger');
    assert.equal(chat.statusSummaryLabel, 'Failed');
    assert.equal(projected.selectedJournal?.jobs[0]?.statusLabel, 'Failed');
    assert.equal(projected.selectedJournal?.attentionBanner, null);
  });

  it('sorts projects and chats by active/recent timestamps and displays working and needs-human independently', () => {
    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'src-older',
          label: 'AAA Older Project',
          project_path: '/work/aaa',
        }),
        makeSource({
          id: 'src-active',
          label: 'ZZZ Active Project',
          project_path: '/work/zzz',
        }),
      ],
      jobs: [
        makeJob({
          source_id: 'src-older',
          id: 'job-settled',
          project_label: 'AAA Older Project',
          project_path: '/work/aaa',
          thread_uuid: UUID_CHAT_A,
          thread_label: 'Settled chat',
          state: 'completed',
          execution_status: 'completed',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 120_000,
          tasks: [
            { id: 't1', title: 'Done task', status: 'completed', depends_on: [] },
          ],
        }),
        makeJob({
          source_id: 'src-active',
          id: 'job-need-user',
          project_label: 'ZZZ Active Project',
          project_path: '/work/zzz',
          thread_uuid: UUID_CHAT_B,
          thread_label: 'Dual active & needs-you chat',
          state: 'needs_user',
          execution_status: 'held',
          active_call_count: 1,
          updated_at_ms: NOW_MS - 5_000,
        }),
      ],
      requests: [
        makeRequest({
          source_id: 'src-active',
          id: 'req-running',
          job_id: 'job-need-user',
          project_label: 'ZZZ Active Project',
          thread_uuid: UUID_CHAT_B,
          thread_label: 'Dual active & needs-you chat',
          state: 'running',
          updated_at_ms: NOW_MS - 3_000,
        }),
      ],
    });

    const projection = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    assert.equal(projection.status, 'ready');
    assert.equal(projection.projects.length, 2);

    // ZZZ Active Project must sort before AAA Older Project despite alphabetical order
    const firstProject = projection.projects[0]!;
    assert.equal(firstProject.label, 'ZZZ Active Project');
    assert.equal(firstProject.hasWorking, true);
    assert.equal(firstProject.hasNeedsHuman, true);
    assert.equal(firstProject.workingBadgeLabel, '1 working');
    assert.equal(firstProject.needsHumanBadgeLabel, '1 needs you');

    // Chat itself exposes both working AND needs-human independently
    const activeChat = firstProject.chats[0]!;
    assert.equal(activeChat.hasWorking, true);
    assert.equal(activeChat.hasNeedsHuman, true);
    assert.equal(activeChat.workingBadgeLabel, '1 working');
    assert.equal(activeChat.needsHumanBadgeLabel, '1 needs you');
    assert.equal(activeChat.primaryDotTone, 'attention');
    assert.equal(activeChat.secondaryDotTone, 'working');

    // Default selection picks the most active/recent chat
    assert.equal(projection.selectedChatKey, activeChat.key);
  });

  it('preserves user-selected chat across refreshes even when another chat becomes newer', () => {
    const snapshot1 = makeSnapshot({
      jobs: [
        makeJob({
          id: 'job-a',
          thread_uuid: UUID_CHAT_A,
          thread_label: 'Chat Alpha',
          state: 'completed',
          execution_status: 'completed',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 60_000,
        }),
        makeJob({
          id: 'job-b',
          thread_uuid: UUID_CHAT_B,
          thread_label: 'Chat Beta',
          state: 'completed',
          execution_status: 'completed',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 30_000,
        }),
      ],
    });

    const initial = projectDesktopWorkspace(snapshot1, { nowMs: NOW_MS });
    const alphaChat = initial.projects[0]!.chats.find(
      (c) => c.label === 'Chat Alpha'
    )!;
    assert.ok(alphaChat);

    // Refresh where Chat Beta becomes actively running and newer
    const snapshot2 = makeSnapshot({
      jobs: [
        ...snapshot1.jobs.filter((j) => j.id === 'job-a'),
        makeJob({
          id: 'job-b',
          thread_uuid: UUID_CHAT_B,
          thread_label: 'Chat Beta',
          state: 'running',
          execution_status: 'running',
          active_call_count: 1,
          updated_at_ms: NOW_MS,
        }),
      ],
    });

    const refreshed = projectDesktopWorkspace(snapshot2, {
      nowMs: NOW_MS,
      selectedChatKey: alphaChat.key,
    });
    assert.equal(refreshed.selectedChatKey, alphaChat.key);
    assert.equal(refreshed.selectedJournal?.chatLabel, 'Chat Alpha');
  });

  it('distinguishes finished Worker calls from authoritative Job Done completion', () => {
    const snapshot = makeSnapshot({
      jobs: [
        makeJob({
          id: 'job-reviewing',
          thread_uuid: UUID_CHAT_A,
          state: 'reviewing',
          execution_status: 'ready_for_review',
          review_state: 'pending',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 4_000,
          tasks: [
            {
              id: 't-worker',
              title: 'Write implementation',
              status: 'ready_for_review',
              depends_on: [],
            },
          ],
        }),
      ],
      requests: [
        makeRequest({
          id: 'req-worker-done',
          job_id: 'job-reviewing',
          thread_uuid: UUID_CHAT_A,
          state: 'completed',
          started_at_ms: NOW_MS - 45_000,
          finished_at_ms: NOW_MS - 5_000,
          updated_at_ms: NOW_MS - 5_000,
          duration_ms: 40_000,
          executor: makeExecutor({ role: 'coder' }),
        }),
      ],
    });

    const projection = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    const journal = projection.selectedJournal!;
    assert.ok(journal);

    const jobSection = journal.jobs[0]!;
    assert.equal(jobSection.isAuthoritativeJobDone, false);
    assert.equal(jobSection.workerFinishedPendingAcceptance, true);
    assert.equal(journal.workingCount, 0, 'waiting for review is not an active call');
    assert.equal(projection.totalWorkingCount, 0);

    const activeReview = projectDesktopWorkspace(
      makeSnapshot({
        ...snapshot,
        jobs: snapshot.jobs.map((job) => ({ ...job, active_call_count: 1 })),
        requests: [...snapshot.requests, makeRequest({
          id: 'reviewer-active', job_id: 'job-reviewing', thread_uuid: UUID_CHAT_A,
          state: 'running', executor: makeExecutor({ role: 'reviewer' }),
        })],
      }),
      { nowMs: NOW_MS }
    );
    assert.equal(activeReview.totalWorkingCount, 1);
    assert.notEqual(jobSection.statusLabel, 'Done');
    assert.notEqual(jobSection.tone, 'success');

    const workerCall = journal.calls[0]!;
    assert.equal(workerCall.isWorkerCallOnly, true);
    assert.equal(workerCall.statusLabel, 'Worker call finished');
    assert.equal(workerCall.tone, 'neutral');
    assert.ok(workerCall.workerPendingNote?.includes('authoritative job completion'));

    // Now verify when job.state is authoritatively 'completed'
    const completedSnapshot = makeSnapshot({
      jobs: [
        makeJob({
          id: 'job-reviewing',
          thread_uuid: UUID_CHAT_A,
          state: 'completed',
          execution_status: 'completed',
          review_state: 'approved',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 1_000,
        }),
      ],
      requests: snapshot.requests,
    });

    const completedProjection = projectDesktopWorkspace(completedSnapshot, {
      nowMs: NOW_MS,
    });
    const completedJob = completedProjection.selectedJournal!.jobs[0]!;
    assert.equal(completedJob.isAuthoritativeJobDone, true);
    assert.equal(completedJob.statusLabel, 'Done');
    assert.equal(completedJob.tone, 'success');
  });

  it('projects concrete task failure and not_started states alongside task labels without generic Unknown', () => {
    const snapshot = makeSnapshot({
      jobs: [
        makeJob({
          id: 'job-fail',
          thread_uuid: UUID_CHAT_A,
          state: 'failed',
          execution_status: 'failed',
          active_call_count: 0,
          tasks: [
            {
              id: 't-1',
              title: 'Run database migration',
              status: 'failed',
              depends_on: [],
            },
            {
              id: 't-2',
              title: 'Verify schema',
              status: 'not_started',
              depends_on: ['t-1'],
            },
            {
              id: 't-3',
              title: 'Timeout probe',
              status: 'worker_timeout_error',
              depends_on: [],
            },
          ],
        }),
      ],
    });

    const projection = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    const jobSection = projection.selectedJournal!.jobs[0]!;
    assert.equal(jobSection.tone, 'danger');
    assert.equal(jobSection.tasks.length, 3);

    assert.equal(jobSection.tasks[0]!.label, 'Run database migration');
    assert.equal(jobSection.tasks[0]!.statusLabel, 'Failed');
    assert.equal(jobSection.tasks[0]!.tone, 'danger');

    assert.equal(jobSection.tasks[1]!.label, 'Verify schema');
    assert.equal(jobSection.tasks[1]!.statusLabel, 'Not started');
    assert.equal(jobSection.tasks[1]!.tone, 'neutral');

    assert.equal(jobSection.tasks[2]!.label, 'Timeout probe');
    assert.equal(jobSection.tasks[2]!.statusLabel, 'Timed out');
    assert.equal(jobSection.tasks[2]!.tone, 'danger');
  });

  it('groups repeated low-level MCP reads behind collapsed Other activity', () => {
    const mcpRequests = Array.from({ length: 8 }, (_, idx) =>
      makeRequest({
        id: `mcp-read-${idx + 1}`,
        job_id: 'job-1',
        thread_uuid: UUID_CHAT_A,
        title: idx % 2 === 0 ? 'status' : 'get_plan',
        operation: idx % 2 === 0 ? 'status' : 'get_plan',
        state: 'completed',
        started_at_ms: NOW_MS - (idx + 1) * 2_000,
        finished_at_ms: NOW_MS - (idx + 1) * 2_000 + 50,
        updated_at_ms: NOW_MS - (idx + 1) * 2_000 + 50,
        executor: {
          tool: 'threadlane_mcp',
          provider: 'local',
          role: null,
          requested_model: null,
          observed_model: null,
          requested_effort: null,
          observed_effort: null,
        },
        usage: null,
      })
    );

    const snapshot = makeSnapshot({
      jobs: [makeJob({ id: 'job-1', thread_uuid: UUID_CHAT_A })],
      requests: [
        makeRequest({
          id: 'req-real-worker',
          job_id: 'job-1',
          thread_uuid: UUID_CHAT_A,
          title: 'Build journal projection',
          state: 'running',
          usage: null,
        }),
        ...mcpRequests,
      ],
    });

    const projection = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    const journal = projection.selectedJournal!;
    assert.equal(journal.calls.length, 1);
    assert.equal(journal.calls[0]!.usageLabel, 'Usage unknown');
    assert.ok(journal.otherActivity);
    assert.equal(journal.otherActivity.title, 'Other activity');
    assert.equal(journal.otherActivity.count, 8);
    assert.equal(journal.otherActivity.items.length, 8);
  });

  it('resolves canonical CodexChatTarget scoped to the same source and deduplicates Open in Codex action', () => {
    assert.equal(isCanonicalUuid(UUID_CHAT_A), true);
    assert.equal(isCanonicalUuid('not-a-uuid-in-prose'), false);

    // Cross-source collision test: src-a has non-UUID thread_uuid, src-b has UUID_CHAT_B on same job-1
    const snapshot = makeSnapshot({
      sources: [
        makeSource({ id: 'src-a', label: 'Project A', project_path: '/work/a' }),
        makeSource({ id: 'src-b', label: 'Project B', project_path: '/work/b' }),
      ],
      jobs: [
        makeJob({
          source_id: 'src-a',
          id: 'job-1',
          project_label: 'Project A',
          project_path: '/work/a',
          thread_uuid: `prose mentions ${UUID_CHAT_B} but is not canonical`,
          binding_id: 'bind-only-a',
          state: 'needs_user',
          execution_status: 'held',
          active_call_count: 0,
          updated_at_ms: NOW_MS - 1_000,
        }),
        makeJob({
          source_id: 'src-b',
          id: 'job-1',
          project_label: 'Project B',
          project_path: '/work/b',
          thread_uuid: UUID_CHAT_B,
          binding_id: 'bind-b',
          state: 'running',
          execution_status: 'running',
          active_call_count: 1,
          updated_at_ms: NOW_MS - 10_000,
        }),
      ],
    });

    const projection = projectDesktopWorkspace(snapshot, { nowMs: NOW_MS });
    const chatA = projection.projects
      .flatMap((p) => p.chats)
      .find((c) => c.sourceId === 'src-a')!;
    const chatB = projection.projects
      .flatMap((p) => p.chats)
      .find((c) => c.sourceId === 'src-b')!;

    // Select chatA: must NOT forge or steal src-b's thread_uuid despite sharing job-1!
    const projA = projectDesktopWorkspace(snapshot, {
      nowMs: NOW_MS,
      selectedChatKey: chatA.key,
    });
    assert.equal(projA.selectedJournal?.codexTarget, null);
    assert.equal(projA.selectedJournal?.codexActionPlacement, 'none');
    assert.equal(projA.selectedJournal?.showHeaderCodexAction, false);
    assert.equal(
      projA.selectedJournal?.attentionBanner?.showOpenInCodex,
      false
    );

    // Select chatB (no attention item, running): single action placed in header
    const projB = projectDesktopWorkspace(snapshot, {
      nowMs: NOW_MS,
      selectedChatKey: chatB.key,
    });
    assert.deepEqual(projB.selectedJournal?.codexTarget, {
      source_id: 'src-b',
      thread_uuid: UUID_CHAT_B,
      job_id: 'job-1',
    });
    assert.equal(projB.selectedJournal?.codexActionPlacement, 'header');
    assert.equal(projB.selectedJournal?.showHeaderCodexAction, true);

    // When chatB has an attention banner, single action moves to attention_banner (not duplicated in header)
    const attentionSnapshot = makeSnapshot({
      sources: [makeSource({ id: 'src-b', label: 'Project B', project_path: '/work/b' })],
      jobs: [
        makeJob({
          source_id: 'src-b',
          id: 'job-1',
          project_label: 'Project B',
          project_path: '/work/b',
          thread_uuid: UUID_CHAT_B,
          state: 'needs_user',
          execution_status: 'held',
          active_call_count: 0,
        }),
      ],
    });
    const projBAttention = projectDesktopWorkspace(attentionSnapshot, {
      nowMs: NOW_MS,
    });
    assert.equal(
      projBAttention.selectedJournal?.codexActionPlacement,
      'attention_banner'
    );
    assert.equal(projBAttention.selectedJournal?.showHeaderCodexAction, false);
    assert.equal(
      projBAttention.selectedJournal?.attentionBanner?.showOpenInCodex,
      true
    );
  });

  it('honors soft archive cutoff and retains older unresolved work with honest age', () => {
    const memory = new Map<string, string>();
    const storage: ProjectsArchiveStorage = {
      getItem: (key) => memory.get(key) ?? null,
      setItem: (key, val) => {
        memory.set(key, val);
      },
    };

    const cutoffMs = NOW_MS - 30 * 60 * 1_000;
    const writeRes = writeProjectsArchiveCutoff(storage, cutoffMs, NOW_MS);
    assert.equal(writeRes.ok, true);
    assert.equal(readProjectsArchiveCutoff(storage, NOW_MS), cutoffMs);

    const tenHoursAgo = NOW_MS - 10 * 60 * 60 * 1_000;
    const snapshot = makeSnapshot({
      jobs: [
        makeJob({
          id: 'job-old-settled',
          thread_uuid: UUID_CHAT_A,
          thread_label: 'Old Settled',
          state: 'completed',
          execution_status: 'completed',
          active_call_count: 0,
          updated_at_ms: cutoffMs - 60_000,
        }),
        makeJob({
          id: 'job-old-unresolved',
          thread_uuid: UUID_CHAT_B,
          thread_label: 'Old Unresolved Question',
          state: 'needs_user',
          execution_status: 'held',
          active_call_count: 0,
          updated_at_ms: tenHoursAgo,
        }),
      ],
    });

    // A time window retains older unresolved work; an explicit archive cutoff
    // intentionally hides older inactive records, matching projects-archive.ts.
    const projection = projectDesktopWorkspace(snapshot, {
      nowMs: NOW_MS,
      windowHours: 6,
    });
    const visibleChats = projection.projects.flatMap((p) => p.chats);
    assert.equal(visibleChats.length, 2);
    const unresolved = visibleChats.find((chat) => chat.label === 'Old Unresolved Question');
    assert.ok(unresolved);
    assert.equal(unresolved.outsideWindow, true);
    assert.ok(unresolved.outsideWindowLabel?.includes('Outside 6h window'));
    assert.equal(projection.canArchiveHistory, true);

    const archived = projectDesktopWorkspace(snapshot, {
      nowMs: NOW_MS,
      windowHours: 6,
      archiveBeforeMs: cutoffMs,
    });
    assert.equal(archived.projects.flatMap((p) => p.chats).length, 0);
    assert.equal(archived.canRestoreArchived, true);
    const restored = projectDesktopWorkspace(snapshot, {
      nowMs: NOW_MS,
      windowHours: 6,
      archiveBeforeMs: null,
    });
    assert.deepEqual(
      restored.projects.flatMap((p) => p.chats).map((chat) => chat.key),
      visibleChats.map((chat) => chat.key)
    );
  });

  it('surfaces missing or unavailable sources honestly rather than as a healthy empty list', () => {
    const missingProj = projectDesktopWorkspace(null, {
      nowMs: NOW_MS,
      loading: false,
      error: 'Observation bridge disconnected.',
    });
    assert.equal(missingProj.status, 'unavailable');
    assert.equal(missingProj.unavailableReason, 'Observation bridge disconnected.');

    const unavailableSourceProj = projectDesktopWorkspace(
      makeSnapshot({
        sources: [
          makeSource({
            availability: 'unavailable',
            message: 'State directory not mounted.',
          }),
        ],
      }),
      { nowMs: NOW_MS }
    );
    assert.equal(unavailableSourceProj.status, 'unavailable');
    assert.equal(
      unavailableSourceProj.unavailableReason,
      'State directory not mounted.'
    );
  });
});

// ---------------------------------------------------------------------------
// Production WorkspaceSnapshot Projection Tests (`projectWorkspaceSnapshot`)
// ---------------------------------------------------------------------------

const UUID_CHAT_C = 'cccccccc-dddd-4eee-8fff-1234567890ab';

function makeChatIdentity(
  overrides: Partial<WorkspaceChatIdentity> = {},
): WorkspaceChatIdentity {
  return validateWorkspaceChatIdentity({
    source_id: 'src-a',
    binding_id: 'bnd_alpha',
    thread_uuid: UUID_CHAT_A,
    canonical_project_scope: '/work/alpha',
    ...overrides,
  });
}

function makeLiveNativeSample(
  identity: WorkspaceChatIdentity,
  overrides: Partial<WorkspaceNativeSample> = {},
): WorkspaceNativeSample {
  return {
    chat_identity: identity,
    producer_epoch: 'epoch_prod_01',
    backend_process_instance: {
      pid: 4201,
      boot_id: 'boot-linux-prod-01',
      start_ticks: 910_000,
    },
    sequence: 1,
    observed_at_ms: NOW_MS - 1_500,
    last_success_at_ms: NOW_MS - 1_500,
    availability: 'available',
    activity: 'idle',
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

function makeWorkspaceJobHistoryRecord(
  identity: WorkspaceChatIdentity | null,
  overrides: Partial<WorkspaceJobHistory> = {},
): WorkspaceJobHistory {
  const base: WorkspaceJobHistory = {
    job: {
      source_id: identity?.source_id ?? 'src-a',
      job_id: 'job_ws_01',
      binding_id: identity?.binding_id ?? null,
      chat_identity: identity,
      title: 'Workspace paket uygulaması',
      job_revision: 3,
      state: 'running',
      phase: 'execution',
      approval_status: 'approved',
      execution_status: 'running',
      review_state: null,
      created_at_ms: NOW_MS - 60_000,
      updated_at_ms: NOW_MS - 5_000,
      summary: 'Aktif paket yürütülüyor.',
    },
    tasks: [],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: {
      source_plan_id: 'plan_ws_01',
      source_plan_revision: 1,
      source_approval_id: 'apr_ws_01',
      source_work_brief_id: 'wb_ws_01',
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

function makeWorkspaceSnapshotFixture(
  overrides: Partial<WorkspaceSnapshot> = {},
): WorkspaceSnapshot {
  return normalizeWorkspaceSnapshot({
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: NOW_MS,
    sampling_policy: DEFAULT_WORKSPACE_SAMPLING_POLICY,
    observation: makeSnapshot(),
    chats: [],
    native_samples: [],
    job_histories: [],
    messages: {
      availability: 'available',
      items: [],
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

describe('projectWorkspaceSnapshot production projection', () => {
  it('keys chats strictly by four-part WorkspaceChatIdentity, keeps same-named chats/projects separate, defaults to Messages, and withholds incomplete-identity records', () => {
    // Four distinct chats sharing identical thread_label AND identical project_label:
    const idBase = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_alpha',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha',
    });
    const idDifferentThread = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_alpha',
      thread_uuid: UUID_CHAT_B,
      canonical_project_scope: '/work/alpha',
    });
    const idDifferentBinding = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_beta',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha',
    });
    const idDifferentProjectScope = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_alpha',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha-worktree',
    });

    const sharedTitle = 'Aynı Başlıklı Mimari Sohbet';
    const sharedProjectLabel = 'Threadlane Masaüstü';

    const msgChat1 = createSafeWorkspaceMessageItem({
      source_kind: 'plan_summary',
      source_record_id: 'plan_chat_1',
      source_order: { recorded_at_ms: NOW_MS - 3_000, source_seq: 1, sub_index: 0 },
      scope: {
        source_id: idBase.source_id,
        binding_id: idBase.binding_id,
        thread_uuid: idBase.thread_uuid,
        canonical_project_scope: idBase.canonical_project_scope,
        chat_identity: idBase,
        job_id: 'job_shared_id',
      },
      raw_summary: 'Birinci sohbetin plan özeti hazır.',
    });
    const msgChat2 = createSafeWorkspaceMessageItem({
      source_kind: 'plan_summary',
      source_record_id: 'plan_chat_2',
      source_order: { recorded_at_ms: NOW_MS - 2_000, source_seq: 2, sub_index: 0 },
      scope: {
        source_id: idDifferentThread.source_id,
        binding_id: idDifferentThread.binding_id,
        thread_uuid: idDifferentThread.thread_uuid,
        canonical_project_scope: idDifferentThread.canonical_project_scope,
        chat_identity: idDifferentThread,
        job_id: 'job_other_id',
      },
      raw_summary: 'İkinci sohbetin plan özeti ayrı tutuldu.',
    });
    // Incomplete message item: missing binding_id even though thread_uuid, canonical_project_scope, and job_id match Chat 1!
    const msgMissingBinding = createSafeWorkspaceMessageItem({
      source_kind: 'question_prompt',
      source_record_id: 'q_unscoped_01',
      source_order: { recorded_at_ms: NOW_MS - 1_000, source_seq: 3, sub_index: 0 },
      scope: {
        source_id: idBase.source_id,
        binding_id: null,
        thread_uuid: idBase.thread_uuid,
        canonical_project_scope: idBase.canonical_project_scope,
        chat_identity: null,
        job_id: 'job_shared_id',
      },
      raw_summary: `${sharedTitle}: eksik kimlikli mesaj başlığa göre iliştirilmemeli.`,
    });

    const jobChat1 = makeWorkspaceJobHistoryRecord(idBase, {
      job: {
        source_id: idBase.source_id,
        job_id: 'job_shared_id',
        binding_id: idBase.binding_id,
        chat_identity: idBase,
        title: 'Chat 1 Job',
        job_revision: 1,
        state: 'running',
        phase: 'execution',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        created_at_ms: NOW_MS - 10_000,
        updated_at_ms: NOW_MS - 2_500,
        summary: 'Birinci sohbet işi.',
      },
    });
    const jobIncomplete = makeWorkspaceJobHistoryRecord(null, {
      job: {
        source_id: 'src-a',
        job_id: 'job_incomplete_01',
        binding_id: 'bnd_alpha',
        chat_identity: null,
        title: sharedTitle,
        job_revision: 1,
        state: 'running',
        phase: 'execution',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        created_at_ms: NOW_MS - 9_000,
        updated_at_ms: NOW_MS - 1_500,
        summary: 'Eksik kimlikli job.',
      },
    });

    // Valid scoped request linked to job_shared_id (unambiguously resolves /work/alpha)
    const reqScopedToChat1 = makeRequest({
      source_id: 'src-a',
      id: 'req_chat1_ok',
      job_id: 'job_shared_id',
      binding_id: idBase.binding_id,
      thread_uuid: idBase.thread_uuid,
      thread_label: sharedTitle,
      project_label: sharedProjectLabel,
      title: 'Doğrulanmış Activity kaydı',
      state: 'running',
    });
    // Incomplete request missing binding_id despite matching title, project_label, thread_uuid, and job_id
    const reqMissingBinding = makeRequest({
      source_id: 'src-a',
      id: 'req_missing_binding',
      job_id: 'job_shared_id',
      binding_id: null,
      thread_uuid: idBase.thread_uuid,
      thread_label: sharedTitle,
      project_label: sharedProjectLabel,
      title: 'Eksik binding kaydı',
      state: 'running',
    });
    // Incomplete request missing thread_uuid despite matching title, project_label, binding_id, and job_id
    const reqMissingThread = makeRequest({
      source_id: 'src-a',
      id: 'req_missing_thread',
      job_id: 'job_shared_id',
      binding_id: idBase.binding_id,
      thread_uuid: null,
      thread_label: sharedTitle,
      project_label: sharedProjectLabel,
      title: 'Eksik thread_uuid kaydı',
      state: 'running',
    });
    // Ambiguous request: idBase and idDifferentProjectScope share (src-a, bnd_alpha, UUID_CHAT_A) across TWO canonical_project_scope values, and job_id is null
    const reqAmbiguousScope = makeRequest({
      source_id: 'src-a',
      id: 'req_ambiguous_scope',
      job_id: null,
      binding_id: idBase.binding_id,
      thread_uuid: idBase.thread_uuid,
      thread_label: sharedTitle,
      project_label: sharedProjectLabel,
      title: 'Belirsiz kapsamlı kayıt',
      state: 'completed',
    });

    const ws = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [
          makeSource({
            id: 'src-a',
            label: sharedProjectLabel,
            project_path: '/work/alpha',
          }),
        ],
        requests: [
          reqScopedToChat1,
          reqMissingBinding,
          reqMissingThread,
          reqAmbiguousScope,
        ],
      }),
      chats: [
        idBase,
        idDifferentThread,
        idDifferentBinding,
        idDifferentProjectScope,
      ].map((identity, idx) => ({
        identity,
        thread_label: sharedTitle,
        project_label: sharedProjectLabel,
        availability: 'available' as const,
        activity: 'idle' as const,
        configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
        active_job_count: idx === 0 ? 1 : 0,
        attention_count: 0,
        updated_at_ms: NOW_MS - (idx + 1) * 1_000,
      })),
      native_samples: [
        makeLiveNativeSample(idBase),
        makeLiveNativeSample(idDifferentThread),
        makeLiveNativeSample(idDifferentBinding),
        makeLiveNativeSample(idDifferentProjectScope),
      ],
      job_histories: [jobChat1, jobIncomplete],
      messages: {
        availability: 'available',
        items: [msgChat1, msgChat2, msgMissingBinding],
        max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
        omitted_count: 0,
        has_more: false,
        truncated: false,
        requires_transcript: false,
        allows_synthetic_user_messages: false,
      },
    });

    assert.equal(resolveObservationRequestChatIdentity(reqMissingBinding, ws), null);
    assert.equal(resolveObservationRequestChatIdentity(reqMissingThread, ws), null);
    assert.equal(resolveObservationRequestChatIdentity(reqAmbiguousScope, ws), null);
    assert.deepEqual(resolveObservationRequestChatIdentity(reqScopedToChat1, ws), idBase);

    const keyChat1 = formatWorkspaceChatIdentityKey(idBase)!;
    const projected = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: keyChat1,
    });

    // Four chats remain strictly distinct despite identical thread_label and project_label.
    assert.equal(projected.status, 'ready');
    assert.equal(projected.activeChatCount, 4);
    assert.equal(projected.activeProjects.length, 2);
    assert.deepEqual(
      new Set(projected.activeProjects.map((p) => p.projectKey)),
      new Set([
        formatWorkspaceProjectGroupKey('src-a', '/work/alpha'),
        formatWorkspaceProjectGroupKey('src-a', '/work/alpha-worktree'),
      ]),
    );

    const allChatKeys = projected.activeProjects.flatMap((p) => p.chats.map((c) => c.chatKey));
    assert.equal(allChatKeys.length, 4);
    assert.equal(new Set(allChatKeys).size, 4);

    // Default tab is Messages; Jobs and Activity are separate scoped surfaces.
    assert.equal(projected.defaultTab, DEFAULT_WORKSPACE_CHAT_TAB);
    assert.equal(projected.defaultTab, 'messages');
    assert.deepEqual(projected.availableTabs, WORKSPACE_CHAT_SURFACE_TABS);
    assert.ok(projected.selectedChat);
    assert.equal(projected.selectedChat.activeTab, 'messages');
    assert.equal(
      projected.selectedChat.messages.kind,
      'safe_summaries_results_questions',
    );
    assert.equal(
      projected.selectedChat.jobs.kind,
      'durable_job_task_attempt_candidate_ledger',
    );
    assert.equal(
      projected.selectedChat.activity.kind,
      'paginated_request_call_delivery_audit_feed',
    );

    // Messages surface contains ONLY msgChat1 (never msgChat2 or incomplete msgMissingBinding).
    assert.deepEqual(
      projected.selectedChat.messages.items.map((m) => m.sourceRecordId),
      ['plan_chat_1'],
    );
    assert.equal(projected.selectedChat.messages.unscopedWithheldCount, 1);
    assert.ok(
      projected.selectedChat.messages.statusNotices.some((n) =>
        n.includes('withheld instead of matching by title or job ID'),
      ),
    );

    // Jobs surface contains ONLY jobChat1 (never jobIncomplete).
    assert.deepEqual(
      projected.selectedChat.jobs.items.map((j) => j.jobId),
      ['job_shared_id'],
    );
    assert.equal(projected.selectedChat.jobs.unscopedWithheldCount, 1);

    // Activity surface contains ONLY reqScopedToChat1 (never missing-binding, missing-thread, or ambiguous-scope requests).
    assert.deepEqual(
      projected.selectedChat.activity.items.map((a) => a.requestId),
      ['req_chat1_ok'],
    );
    assert.equal(projected.selectedChat.activity.unscopedWithheldCount, 3);
    assert.ok(
      projected.selectedChat.activity.statusNotices.some((n) =>
        n.includes('withheld instead of matching by title or job ID'),
      ),
    );
  });

  it('isolates multiple sources and converts failed sources into visible English status notices while keeping healthy sources working', () => {
    const idSourceA = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_shared',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/shared-name',
    });
    const idSourceB = makeChatIdentity({
      source_id: 'src-b',
      binding_id: 'bnd_shared',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/shared-name',
    });

    const ws = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [
          makeSource({
            id: 'src-a',
            label: 'Shared Service',
            project_path: '/work/shared-name',
            availability: 'available',
          }),
          makeSource({
            id: 'src-b',
            label: 'Shared Service',
            project_path: '/work/shared-name',
            availability: 'available',
          }),
          makeSource({
            id: 'src-broken',
            label: 'Broken Store',
            project_path: '/work/broken',
            availability: 'unavailable',
            message: 'SQLite WAL file permission denied',
          }),
        ],
        requests: [
          makeRequest({
            source_id: 'src-a',
            id: 'req-same',
            job_id: null,
            binding_id: 'bnd_shared',
            thread_uuid: UUID_CHAT_A,
            title: 'Source A Call',
            state: 'completed',
          }),
          makeRequest({
            source_id: 'src-b',
            id: 'req-same',
            job_id: null,
            binding_id: 'bnd_shared',
            thread_uuid: UUID_CHAT_A,
            title: 'Source B Call',
            state: 'running',
          }),
        ],
      }),
      native_samples: [
        makeLiveNativeSample(idSourceA, { activity: 'idle' }),
        makeLiveNativeSample(idSourceB, { activity: 'working' }),
      ],
    });

    const keyA = formatWorkspaceChatIdentityKey(idSourceA)!;
    const keyB = formatWorkspaceChatIdentityKey(idSourceB)!;
    assert.notEqual(keyA, keyB);

    const projectedA = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: keyA,
      selectedTab: 'activity',
    });

    assert.equal(projectedA.status, 'ready');
    assert.equal(projectedA.activeChatCount, 2);
    assert.equal(projectedA.activeProjects.length, 2);
    assert.equal(projectedA.unavailableSourceCount, 1);
    assert.equal(projectedA.unavailableSources[0]?.sourceId, 'src-broken');
    assert.ok(
      projectedA.unavailableSources[0]?.statusText.includes(
        'Observation source "Broken Store" (src-broken) is unavailable: SQLite WAL file permission denied',
      ),
    );
    assert.ok(
      projectedA.statusNotices.some((n) =>
        n.includes('Observation source "Broken Store" (src-broken) is unavailable'),
      ),
    );

    assert.equal(projectedA.selectedChat?.activeTab, 'activity');
    assert.deepEqual(
      projectedA.selectedChat?.activity.items.map((i) => i.title),
      ['Source A Call'],
    );
    assert.equal(projectedA.selectedChat?.workerSummary.activeWorkerCount, 0);

    const projectedB = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: keyB,
    });
    assert.equal(projectedB.selectedChat?.nativeState.activityLabel, 'Coworker working');
    assert.equal(projectedB.selectedChat?.workerSummary.activeWorkerCount, 1);
    assert.deepEqual(
      projectedB.selectedChat?.activity.items.map((i) => i.title),
      ['Source B Call'],
    );
  });

  it('manages Projects/Active and Projects/Archive per chat without age-based hiding, legacy cutoff migration, backend mutation, or blocker loss', () => {
    const rawStore = new Map<string, string>();
    const dualStorage: ProjectsArchiveStorage & WorkspaceChatPreferenceStorage = {
      getItem: (key) => rawStore.get(key) ?? null,
      setItem: (key, value) => {
        rawStore.set(key, value);
      },
    };

    // Write a legacy bulk cutoff at NOW_MS so every historical chat would have been hidden under the old flow.
    const legacyWrite = writeProjectsArchiveCutoff(dualStorage, NOW_MS, NOW_MS);
    assert.equal(legacyWrite.ok, true);
    assert.ok(rawStore.has(PROJECTS_ARCHIVE_STORAGE_KEY));

    const fourteenDaysAgoMs = NOW_MS - 14 * 24 * 60 * 60 * 1_000;
    const idOldActive = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_old_chat',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha',
    });
    const idBlockedChat = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_blocked_chat',
      thread_uuid: UUID_CHAT_B,
      canonical_project_scope: '/work/alpha',
    });

    const blockedJob = makeWorkspaceJobHistoryRecord(idBlockedChat, {
      job: {
        source_id: 'src-a',
        job_id: 'job_blocked_01',
        binding_id: 'bnd_blocked_chat',
        chat_identity: idBlockedChat,
        title: 'Kritik migrasyon paketi',
        job_revision: 5,
        state: 'needs_user',
        phase: 'approval',
        approval_status: 'pending',
        execution_status: 'held',
        review_state: null,
        created_at_ms: fourteenDaysAgoMs,
        updated_at_ms: NOW_MS - 4_000,
        summary: 'Onay bekleniyor.',
      },
      current_attention: {
        kind: 'needs_approval',
        reason: 'Veritabanı şema değişikliği için kullanıcı onayı gerekiyor.',
        next_step: 'Codex sohbetini açıp plan revizyonunu onaylayın.',
        blocking_hold_ids: ['hold_db_01'],
        blocking_outbox_ids: [],
        question_id: 'q_db_01',
        updated_at_ms: NOW_MS - 4_000,
      },
    });

    const ws = makeWorkspaceSnapshotFixture({
      chats: [
        {
          identity: idOldActive,
          thread_label: 'On Dört Günlük Sohbet',
          project_label: 'Alpha Repo',
          availability: 'unavailable',
          activity: 'unknown',
          configured_selection: createVerifiedConfiguredSelection(null, null),
          active_job_count: 0,
          attention_count: 0,
          updated_at_ms: fourteenDaysAgoMs,
        },
        {
          identity: idBlockedChat,
          thread_label: 'Blokeli Sohbet',
          project_label: 'Alpha Repo',
          availability: 'available',
          activity: 'waiting_for_approval',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
          active_job_count: 1,
          attention_count: 1,
          updated_at_ms: NOW_MS - 4_000,
        },
      ],
      native_samples: [
        makeLiveNativeSample(idBlockedChat, {
          activity: 'waiting_for_approval',
          observed_at_ms: NOW_MS - 2_000,
          last_success_at_ms: NOW_MS - 2_000,
        }),
      ],
      job_histories: [blockedJob],
    });
    const snapshotBeforeJson = JSON.stringify(ws);

    // 1. Legacy bulk cutoff and 14-day age do NOT hide chats or migrate to per-chat preferences.
    const initialProj = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      preferenceStorage: dualStorage,
    });
    assert.equal(initialProj.activeChatCount, 2);
    assert.equal(initialProj.archivedChatCount, 0);
    assert.equal(rawStore.has(WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY), false);
    assert.deepEqual(readWorkspaceChatArchivePreferences(dualStorage), {});

    // 2. Archive ONLY idBlockedChat; snapshot stays unmutated and current blocker info is preserved!
    const archivedPrefs = setWorkspaceChatArchivePreference(
      readWorkspaceChatArchivePreferences(dualStorage),
      idBlockedChat,
      'archived',
      NOW_MS,
    );
    assert.equal(writeWorkspaceChatArchivePreferences(dualStorage, archivedPrefs), true);

    const activeViewAfterArchive = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      lifecycleFilter: 'active',
      preferenceStorage: dualStorage,
    });
    assert.equal(JSON.stringify(ws), snapshotBeforeJson);
    assert.equal(activeViewAfterArchive.activeChatCount, 1);
    assert.equal(activeViewAfterArchive.archivedChatCount, 1);
    assert.equal(
      activeViewAfterArchive.visibleProjects[0]?.chats[0]?.chatKey,
      formatWorkspaceChatIdentityKey(idOldActive),
    );

    // Current blocker in archived chat remains visible from Projects/Active!
    assert.ok(activeViewAfterArchive.archivedBlockerCount >= 1);
    assert.equal(
      activeViewAfterArchive.totalNeedsHumanCount,
      activeViewAfterArchive.activeNeedsHumanCount +
        activeViewAfterArchive.archivedNeedsHumanCount,
    );
    assert.equal(activeViewAfterArchive.archivedBlockedChats.length, 1);
    assert.equal(
      activeViewAfterArchive.archivedBlockedChats[0]?.threadLabel,
      'Blokeli Sohbet',
    );
    assert.ok(
      activeViewAfterArchive.archivedBlockerNotice?.includes('Projects/Archive'),
    );
    assert.ok(
      activeViewAfterArchive.statusNotices.some((n) =>
        n.includes('Projects/Archive'),
      ),
    );

    // Viewing Projects/Archive shows the archived chat with all blockers & execution state intact.
    const archiveView = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      lifecycleFilter: 'archived',
      preferenceStorage: dualStorage,
    });
    assert.equal(archiveView.visibleProjects.length, 1);
    const archivedChatItem = archiveView.visibleProjects[0]!.chats[0]!;
    assert.equal(archivedChatItem.lifecycle, 'archived');
    assert.equal(archivedChatItem.canRestore, true);
    assert.equal(archivedChatItem.hasCurrentBlocker, true);
    assert.ok(
      archivedChatItem.blockers.some((b) =>
        b.reason.includes('Veritabanı şema değişikliği için kullanıcı onayı gerekiyor.'),
      ),
    );
    assert.ok(
      archiveView.selectedChat?.statusNotices.some((n) =>
        n.includes('Projects/Archive (view preference only) and still has active human blocker(s)'),
      ),
    );

    // 3. Restore idBlockedChat back to Projects/Active.
    const restoredPrefs = setWorkspaceChatArchivePreference(
      readWorkspaceChatArchivePreferences(dualStorage),
      idBlockedChat,
      'active',
      NOW_MS + 1_000,
    );
    assert.equal(writeWorkspaceChatArchivePreferences(dualStorage, restoredPrefs), true);

    const restoredProj = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS + 1_000,
      lifecycleFilter: 'active',
      preferenceStorage: dualStorage,
    });
    assert.equal(restoredProj.activeChatCount, 2);
    assert.equal(restoredProj.archivedChatCount, 0);
    assert.equal(restoredProj.archivedBlockerCount, 0);
    assert.equal(restoredProj.archivedBlockerNotice, null);
  });

  it('projects a jobless live chat cleanly and never marks a chat Completed when MCP calls finish', () => {
    const idLiveNoJobs = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_live_chat',
      thread_uuid: UUID_CHAT_C,
      canonical_project_scope: '/work/alpha',
    });

    const mcpConnectReq = makeRequest({
      source_id: 'src-a',
      id: 'req_mcp_connect',
      job_id: null,
      binding_id: 'bnd_live_chat',
      thread_uuid: UUID_CHAT_C,
      title: 'connect_current_chat',
      operation: 'mcp.connect_current_chat',
      state: 'completed',
      executor: {
        tool: 'threadlane_mcp',
        provider: 'local',
        role: null,
        requested_model: null,
        observed_model: null,
        requested_effort: null,
        observed_effort: null,
      },
      usage: null,
      summary: 'Paired route connected.',
    });
    const mcpStatusReq = makeRequest({
      source_id: 'src-a',
      id: 'req_mcp_status',
      job_id: null,
      binding_id: 'bnd_live_chat',
      thread_uuid: UUID_CHAT_C,
      title: 'status',
      operation: 'mcp.status',
      state: 'completed',
      executor: null,
      usage: null,
      summary: 'Read scoped status.',
    });

    const ws = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [makeSource({ id: 'src-a', project_path: '/work/alpha' })],
        jobs: [],
        requests: [mcpConnectReq, mcpStatusReq],
      }),
      chats: [
        {
          identity: idLiveNoJobs,
          thread_label: 'İşsiz Canlı Sohbet',
          project_label: 'Alpha Repo',
          availability: 'available',
          activity: 'idle',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
          active_job_count: 0,
          attention_count: 0,
          updated_at_ms: NOW_MS - 500,
        },
      ],
      native_samples: [
        makeLiveNativeSample(idLiveNoJobs, {
          activity: 'idle',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
        }),
      ],
      job_histories: [],
    });

    const projected = projectWorkspaceSnapshot(ws, { nowMs: NOW_MS });
    assert.equal(projected.status, 'ready');
    assert.equal(projected.activeChatCount, 1);

    const chat = projected.visibleProjects[0]!.chats[0]!;
    assert.equal(chat.threadLabel, 'İşsiz Canlı Sohbet');
    assert.equal(chat.nativeState.availability, 'available');
    assert.equal(chat.nativeState.activity, 'idle');
    assert.equal(chat.nativeState.activityLabel, 'Coworker idle');
    assert.equal(chat.configuredSelection.selectedModel, 'gpt-5.4');
    assert.equal(chat.configuredSelection.selectedEffort, 'high');
    assert.equal(chat.configuredSelection.displayLabel, 'gpt-5.4 · high');
    assert.equal(chat.configuredSelection.countsAsRuntimeTelemetry, false);
    assert.equal(chat.workerSummary.activeWorkerCount, 0);
    assert.equal(chat.workerSummary.statusLabel, 'No active workers');
    assert.equal(chat.jobSummary.totalJobCount, 0);
    assert.equal(chat.jobSummary.statusLabel, 'No jobs');
    assert.notEqual(chat.statusSummaryLabel.toLowerCase(), 'completed');
    assert.notEqual(chat.statusSummaryLabel.toLowerCase(), 'done');

    assert.equal(projected.selectedChat?.activeTab, 'messages');
    assert.equal(projected.selectedChat?.activity.items.length, 2);
    for (const actItem of projected.selectedChat?.activity.items ?? []) {
      assert.equal(actItem.isLocalMcpCall, true);
      assert.equal(actItem.completesChat, false);
      assert.equal(actItem.stateLabel, 'MCP call finished');
    }
  });

  it('displays Coworker native state and selected model independently of Worker counts and job outcomes', () => {
    const idCoworkerBusy = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_coworker_busy',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha',
    });
    const idWorkerBusy = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_worker_busy',
      thread_uuid: UUID_CHAT_B,
      canonical_project_scope: '/work/alpha',
    });

    // Chat 1: Worker succeeded & job is verified accepted, while Coworker is actively working!
    const acceptedJob = makeWorkspaceJobHistoryRecord(idCoworkerBusy, {
      job: {
        source_id: 'src-a',
        job_id: 'job_worker_done',
        binding_id: 'bnd_coworker_busy',
        chat_identity: idCoworkerBusy,
        title: 'Tamamlanan Worker İşi',
        job_revision: 6,
        state: 'completed',
        phase: 'completed',
        approval_status: 'approved',
        execution_status: 'accepted',
        review_state: 'review_passed',
        created_at_ms: NOW_MS - 30_000,
        updated_at_ms: NOW_MS - 2_000,
        summary: 'Worker paketi kabul edildi.',
      },
      runs: [
        {
          run_id: 'run_done_01',
          worker_id: 'w_01',
          binding_id: 'bnd_coworker_busy',
          task_id: 't_01',
          attempt_no: 1,
          role: 'coder',
          status: 'completed',
          executor: makeExecutor(),
          started_at_ms: NOW_MS - 25_000,
          finished_at_ms: NOW_MS - 5_000,
          updated_at_ms: NOW_MS - 5_000,
        },
      ],
      combined_candidates: [
        {
          combined_candidate_id: 'ccand_verified_01',
          job_revision: 6,
          status: 'accepted',
          final_commit_sha: 'abc1234',
          final_tree_sha: 'def5678',
          candidate_digest: 'sha256_ok',
          closure_reservation_id: 'cres_01',
          check_count: 3,
          produced_at_ms: NOW_MS - 3_000,
        },
      ],
      acceptance_proof: {
        status: 'verified_accepted',
        accepted_candidate_id: 'cand_01',
        combined_candidate_id: 'ccand_verified_01',
        review_verdict_id: 'rverd_01',
        review_receipt_id: 'rrcpt_01',
        integration_receipt_id: 'ircpt_01',
        closure_reservation_id: 'cres_01',
        closure_verdict_id: 'cverd_verified_01',
        verified_at_ms: NOW_MS - 2_000,
      },
    });

    // Chat 2: Two Workers are actively running while Coworker native chat is idle!
    const activeWorkersJob = makeWorkspaceJobHistoryRecord(idWorkerBusy, {
      job: {
        source_id: 'src-a',
        job_id: 'job_two_workers',
        binding_id: 'bnd_worker_busy',
        chat_identity: idWorkerBusy,
        title: 'Paralel Worker İşi',
        job_revision: 2,
        state: 'running',
        phase: 'execution',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        created_at_ms: NOW_MS - 20_000,
        updated_at_ms: NOW_MS - 1_000,
        summary: 'İki paralel görev yürütülüyor.',
      },
      runs: [
        {
          run_id: 'run_live_01',
          worker_id: 'w_live_01',
          binding_id: 'bnd_worker_busy',
          task_id: 't_live_01',
          attempt_no: 1,
          role: 'coder',
          status: 'running',
          executor: makeExecutor(),
          started_at_ms: NOW_MS - 10_000,
          finished_at_ms: null,
          updated_at_ms: NOW_MS - 1_000,
        },
        {
          run_id: 'run_live_02',
          worker_id: 'w_live_02',
          binding_id: 'bnd_worker_busy',
          task_id: 't_live_02',
          attempt_no: 1,
          role: 'coder',
          status: 'running',
          executor: makeExecutor(),
          started_at_ms: NOW_MS - 9_000,
          finished_at_ms: null,
          updated_at_ms: NOW_MS - 1_000,
        },
      ],
    });

    const ws = makeWorkspaceSnapshotFixture({
      native_samples: [
        makeLiveNativeSample(idCoworkerBusy, {
          activity: 'working',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
        }),
        makeLiveNativeSample(idWorkerBusy, {
          activity: 'idle',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4-mini', 'medium'),
        }),
      ],
      job_histories: [acceptedJob, activeWorkersJob],
    });

    const proj1 = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: formatWorkspaceChatIdentityKey(idCoworkerBusy),
    });
    assert.ok(proj1.selectedChat);
    assert.equal(proj1.selectedChat.nativeState.activity, 'working');
    assert.equal(proj1.selectedChat.nativeState.activityLabel, 'Coworker working');
    assert.equal(proj1.selectedChat.configuredSelection.selectedModel, 'gpt-5.4');
    assert.equal(proj1.selectedChat.workerSummary.activeWorkerCount, 0);
    assert.equal(proj1.selectedChat.workerSummary.completedWorkerRunCount, 1);
    assert.equal(proj1.selectedChat.workerSummary.statusLabel, 'No active workers');
    assert.equal(proj1.selectedChat.jobSummary.verifiedAcceptedJobCount, 1);
    assert.equal(proj1.selectedChat.jobSummary.statusLabel, '1 verified accepted job');
    assert.equal(proj1.totalWorkingCoworkerCount, 1);
    assert.equal(proj1.totalActiveWorkerCount, 2);

    const proj2 = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: formatWorkspaceChatIdentityKey(idWorkerBusy),
    });
    assert.ok(proj2.selectedChat);
    assert.equal(proj2.selectedChat.nativeState.activity, 'idle');
    assert.equal(proj2.selectedChat.nativeState.activityLabel, 'Coworker idle');
    assert.equal(proj2.selectedChat.configuredSelection.selectedModel, 'gpt-5.4-mini');
    assert.equal(proj2.selectedChat.workerSummary.activeWorkerCount, 2);
    assert.equal(proj2.selectedChat.workerSummary.statusLabel, '2 workers running');
  });

  it('marks stale native samples and selected models stale in English without backfilling from historical request actor metadata', () => {
    const id = makeChatIdentity();
    const sampleObservedMs = NOW_MS - 2_000;

    const ws = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [makeSource({ id: 'src-a', project_path: '/work/alpha' })],
        requests: [
          makeRequest({
            source_id: 'src-a',
            id: 'req_hist_actor',
            job_id: null,
            binding_id: id.binding_id,
            thread_uuid: id.thread_uuid,
            state: 'completed',
            actor: makeActor({
              selected_model: 'gpt-5.4',
              selected_effort: 'high',
              evidence: 'observed',
            }),
          }),
        ],
      }),
      native_samples: [
        makeLiveNativeSample(id, {
          observed_at_ms: sampleObservedMs,
          last_success_at_ms: sampleObservedMs,
          activity: 'working',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
        }),
      ],
    });

    // Fresh at NOW_MS (age 2s <= 10s threshold)
    const freshProj = projectWorkspaceSnapshot(ws, { nowMs: NOW_MS });
    assert.equal(freshProj.selectedChat?.nativeState.availability, 'available');
    assert.equal(freshProj.selectedChat?.configuredSelection.selectedModel, 'gpt-5.4');
    assert.equal(freshProj.selectedChat?.configuredSelection.isStale, false);

    // Advance past 10s threshold via both `nowMs` AND `regenerateWorkspaceSnapshotAt`
    const laterMs = sampleObservedMs + 15_000;
    const staleViaNow = projectWorkspaceSnapshot(ws, { nowMs: laterMs });
    const regenerated = regenerateWorkspaceSnapshotAt(ws, laterMs);
    assert.equal(regenerated.native_samples[0]?.observed_at_ms, sampleObservedMs);
    assert.equal(regenerated.native_samples[0]?.last_success_at_ms, sampleObservedMs);
    const staleViaRegen = projectWorkspaceSnapshot(regenerated);

    for (const staleProj of [staleViaNow, staleViaRegen]) {
      const sc = staleProj.selectedChat!;
      assert.equal(sc.nativeState.availability, 'stale');
      assert.equal(sc.nativeState.freshness, 'stale');
      assert.equal(sc.nativeState.activity, 'unknown');
      assert.equal(sc.nativeState.activityLabel, 'Coworker state stale');
      assert.ok(sc.nativeState.statusNotice?.includes('Native chat sample is stale'));
      assert.equal(sc.configuredSelection.selectedModel, null);
      assert.equal(sc.configuredSelection.selectedEffort, null);
      assert.equal(sc.configuredSelection.evidence, 'unavailable');
      assert.equal(sc.configuredSelection.isStale, true);
      assert.equal(sc.configuredSelection.displayLabel, 'Selected model stale');
      assert.ok(
        sc.configuredSelection.statusNote.includes('native chat sample is stale'),
      );
      // Historical request actor metadata stays strictly on the activity item, never backfilling chat configuredSelection.
      assert.equal(sc.activity.items[0]?.historicalActorSelectedModel, 'gpt-5.4');
    }
  });

  it('preserves current attempt vs past attempts, candidates, historical failure vs current attention, and rejects unverified ID-format acceptance', () => {
    const id = makeChatIdentity();

    // Job 1: Attempt 1 failed (superseded historical failure), Attempt 2 is current attempt with candidate & current review_required attention.
    const jobRetryWithAttention = makeWorkspaceJobHistoryRecord(id, {
      job: {
        source_id: 'src-a',
        job_id: 'job_retry_attention',
        binding_id: id.binding_id,
        chat_identity: id,
        title: 'Retry ve İnceleme İşi',
        job_revision: 8,
        state: 'reviewing',
        phase: 'review',
        approval_status: 'approved',
        execution_status: 'reviewing',
        review_state: 'pending_review',
        created_at_ms: NOW_MS - 50_000,
        updated_at_ms: NOW_MS - 1_000,
        summary: 'İkinci deneme adayı incelemede.',
      },
      tasks: [
        {
          task_id: 'task_retry_01',
          title: 'Projeksiyon doğrulaması',
          role: 'coder',
          difficulty: 'hard',
          stage: 'review',
          status: 'ready_for_review',
          depends_on: [],
          current_attempt_no: 2,
          attempts: [
            {
              attempt_no: 1,
              status: 'failed',
              run_id: 'run_att_1',
              worker_id: 'w_att_1',
              outbox_id: 'out_att_1',
              executor: makeExecutor(),
              started_at_ms: NOW_MS - 45_000,
              finished_at_ms: NOW_MS - 35_000,
              summary: 'İlk deneme derleme hatasıyla durdu.',
            },
            {
              attempt_no: 2,
              status: 'completed',
              run_id: 'run_att_2',
              worker_id: 'w_att_2',
              outbox_id: 'out_att_2',
              executor: makeExecutor(),
              started_at_ms: NOW_MS - 25_000,
              finished_at_ms: NOW_MS - 5_000,
              summary: 'İkinci deneme adayı üretildi.',
            },
          ],
          candidates: [
            {
              candidate_id: 'cand_att1_rejected',
              attempt_no: 1,
              worker_id: 'w_att_1',
              head_commit_sha: '1111aaaa',
              tree_sha: '2222bbbb',
              verification_passed: false,
              review_status: 'rejected',
              review_verdict_id: 'rverd_rej_01',
              review_receipt_id: null,
              produced_at_ms: NOW_MS - 35_000,
              summary: 'Reddedilen ilk aday.',
            },
            {
              candidate_id: 'cand_att2_active',
              attempt_no: 2,
              worker_id: 'w_att_2',
              head_commit_sha: '3333cccc',
              tree_sha: '4444dddd',
              verification_passed: true,
              review_status: 'pending_review',
              review_verdict_id: null,
              review_receipt_id: null,
              produced_at_ms: NOW_MS - 5_000,
              summary: 'İnceleme bekleyen güncel aday.',
            },
          ],
          active_candidate_id: 'cand_att2_active',
          accepted_candidate_id: null,
          historical_failure: {
            failure_id: 'fail_att_1',
            stage: 'verification',
            failed_at_ms: NOW_MS - 35_000,
            task_id: 'task_retry_01',
            attempt_no: 1,
            run_id: 'run_att_1',
            outbox_id: 'out_att_1',
            reason: 'İlk deneme tip kontrolünde başarısız oldu.',
            superseded_or_resolved: true,
          },
          current_attention: {
            kind: 'review_required',
            reason: 'İkinci deneme adayının inceleme kararı bekleniyor.',
            next_step: 'Adayı inceleyip karar kaydedin.',
            blocking_hold_ids: [],
            blocking_outbox_ids: ['out_rev_02'],
            question_id: null,
            updated_at_ms: NOW_MS - 2_000,
          },
          acceptance_proof: {
            status: 'pending_review',
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
    });

    // Job 2: Settled historical failure with NO current_attention -> must NOT fabricate a current blocker.
    const idSettledFailChat = makeChatIdentity({
      binding_id: 'bnd_settled_fail',
      thread_uuid: UUID_CHAT_B,
    });
    const jobSettledFailure = makeWorkspaceJobHistoryRecord(idSettledFailChat, {
      job: {
        source_id: 'src-a',
        job_id: 'job_settled_fail',
        binding_id: idSettledFailChat.binding_id,
        chat_identity: idSettledFailChat,
        title: 'Sonlanmış Hatalı İş',
        job_revision: 3,
        state: 'failed',
        phase: 'failed',
        approval_status: 'approved',
        execution_status: 'failed',
        review_state: null,
        created_at_ms: NOW_MS - 60_000,
        updated_at_ms: NOW_MS - 20_000,
        summary: 'İş kalıcı hatayla sonlandı.',
      },
      historical_failure: {
        failure_id: 'fail_settled_01',
        stage: 'execution',
        failed_at_ms: NOW_MS - 20_000,
        task_id: null,
        attempt_no: 1,
        run_id: 'run_fail_01',
        outbox_id: null,
        reason: 'Alt süreç zaman aşımına uğradı.',
        superseded_or_resolved: false,
      },
      current_attention: null,
    });

    // Job 3: Spoofed/unverified IDs (`cverd_*`, `ccand_*`, `rverd_*`, `cand_*`) & `state: 'completed'` WITHOUT backend-verified acceptance_proof.
    const jobUnverifiedIds = makeWorkspaceJobHistoryRecord(id, {
      job: {
        source_id: 'src-a',
        job_id: 'job_unverified_ids',
        binding_id: id.binding_id,
        chat_identity: id,
        title: 'Kanıtsız Tamamlanmış Görünümlü İş',
        job_revision: 9,
        state: 'completed',
        phase: 'completed',
        approval_status: 'approved',
        execution_status: 'accepted',
        review_state: 'accepted',
        created_at_ms: NOW_MS - 40_000,
        updated_at_ms: NOW_MS - 3_000,
        summary: 'Backend kabul kanıtı eksik.',
      },
      tasks: [
        {
          task_id: 'task_unverified_01',
          title: 'Kanıtsız görev',
          role: 'coder',
          difficulty: 'medium',
          stage: 'completed',
          status: 'accepted',
          depends_on: [],
          current_attempt_no: 1,
          attempts: [],
          candidates: [
            {
              candidate_id: 'cand_looks_real_99',
              attempt_no: 1,
              worker_id: 'w_99',
              head_commit_sha: 'aaaa1111',
              tree_sha: 'bbbb2222',
              verification_passed: true,
              review_status: 'accepted',
              review_verdict_id: 'rverd_looks_real_99',
              review_receipt_id: 'rrcpt_looks_real_99',
              produced_at_ms: NOW_MS - 4_000,
              summary: 'ID formatı tek başına kabul kanıtı değildir.',
            },
          ],
          active_candidate_id: 'cand_looks_real_99',
          accepted_candidate_id: 'cand_looks_real_99',
          historical_failure: null,
          current_attention: null,
          // Missing review_verdict_id & review_receipt_id inside acceptance_proof -> normalized to 'unknown'
          acceptance_proof: {
            status: 'verified_accepted',
            accepted_candidate_id: 'cand_looks_real_99',
            combined_candidate_id: null,
            review_verdict_id: null,
            review_receipt_id: null,
            integration_receipt_id: null,
            closure_reservation_id: null,
            closure_verdict_id: null,
            verified_at_ms: NOW_MS - 3_000,
          },
        },
      ],
      combined_candidates: [
        {
          combined_candidate_id: 'ccand_looks_real_99',
          job_revision: 9,
          status: 'accepted',
          final_commit_sha: 'cccc3333',
          final_tree_sha: 'dddd4444',
          candidate_digest: 'sha256_99',
          closure_reservation_id: 'cres_99',
          check_count: 2,
          produced_at_ms: NOW_MS - 3_500,
        },
      ],
      result: {
        outcome: 'accepted',
        summary: 'Sonuç kaydı var ama kapanış kanıtı doğrulanmadı.',
        produced_at_ms: NOW_MS - 3_000,
        delivery_stage: 'result_ready',
        closure_verdict_id: 'cverd_looks_real_99',
      },
      // Missing closure_verdict_id in acceptance_proof -> normalized to 'unknown'; frontend must NOT synthesize proof from result.closure_verdict_id!
      acceptance_proof: {
        status: 'verified_accepted',
        accepted_candidate_id: 'cand_looks_real_99',
        combined_candidate_id: 'ccand_looks_real_99',
        review_verdict_id: null,
        review_receipt_id: null,
        integration_receipt_id: 'ircpt_looks_real_99',
        closure_reservation_id: 'cres_99',
        closure_verdict_id: null,
        verified_at_ms: NOW_MS - 3_000,
      },
    });

    const ws = makeWorkspaceSnapshotFixture({
      native_samples: [
        makeLiveNativeSample(id),
        makeLiveNativeSample(idSettledFailChat),
      ],
      job_histories: [jobRetryWithAttention, jobSettledFailure, jobUnverifiedIds],
    });

    const projectedMain = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: formatWorkspaceChatIdentityKey(id),
      selectedTab: 'jobs',
    });
    assert.equal(projectedMain.selectedChat?.activeTab, 'jobs');

    const retryView = projectedMain.selectedChat?.jobs.items.find(
      (j) => j.jobId === 'job_retry_attention',
    );
    assert.ok(retryView);
    const taskView = retryView.tasks[0]!;
    assert.equal(taskView.currentAttemptNo, 2);
    assert.equal(taskView.currentAttempt?.attemptNo, 2);
    assert.equal(taskView.currentAttempt?.isCurrentAttempt, true);
    assert.deepEqual(
      taskView.pastAttempts.map((a) => a.attemptNo),
      [1],
    );
    assert.equal(taskView.candidates.length, 2);
    assert.equal(taskView.candidates[1]?.isActiveCandidate, true);
    assert.equal(taskView.candidates[1]?.isAcceptedByVerifiedProof, false);
    assert.equal(taskView.historicalFailure?.supersededOrResolved, true);
    assert.equal(
      taskView.historicalFailure?.statusLabel,
      'Historical failure (superseded)',
    );
    assert.equal(
      taskView.historicalFailure?.reason,
      'İlk deneme tip kontrolünde başarısız oldu.',
    );
    assert.equal(taskView.currentAttention?.kind, 'review_required');
    assert.equal(taskView.currentAttention?.kindLabel, 'Review required');
    assert.equal(
      taskView.currentAttention?.reason,
      'İkinci deneme adayının inceleme kararı bekleniyor.',
    );

    // Unverified job & task must NOT be treated as verified_accepted despite `cverd_*`, `ccand_*`, `rverd_*`, `cand_*` IDs.
    const unverifiedView = projectedMain.selectedChat?.jobs.items.find(
      (j) => j.jobId === 'job_unverified_ids',
    );
    assert.ok(unverifiedView);
    assert.equal(unverifiedView.isVerifiedAccepted, false);
    assert.equal(unverifiedView.statusLabel, 'Completed (unverified acceptance)');
    assert.equal(
      unverifiedView.acceptanceLabel,
      'Awaiting backend-verified acceptance proof',
    );
    assert.equal(
      unverifiedView.combinedCandidates[0]?.isAcceptedByVerifiedProof,
      false,
    );
    assert.equal(unverifiedView.tasks[0]?.isVerifiedAccepted, false);
    assert.equal(
      unverifiedView.tasks[0]?.candidates[0]?.isAcceptedByVerifiedProof,
      false,
    );

    // Settled failure chat has historical failure preserved and ZERO current blockers.
    const projectedSettled = projectWorkspaceSnapshot(ws, {
      nowMs: NOW_MS,
      selectedChatKey: formatWorkspaceChatIdentityKey(idSettledFailChat),
      selectedTab: 'jobs',
    });
    assert.equal(projectedSettled.selectedChat?.hasCurrentBlocker, false);
    assert.equal(projectedSettled.selectedChat?.needsHumanCount, 0);
    const settledJobView = projectedSettled.selectedChat?.jobs.items[0]!;
    assert.equal(settledJobView.historicalFailure?.supersededOrResolved, false);
    assert.equal(settledJobView.historicalFailure?.statusLabel, 'Historical failure');
    assert.equal(settledJobView.currentAttention, null);
  });

  it('renders bounded window, omitted, has_more, and truncated conditions in English while keeping agent content in its source language', () => {
    const id = makeChatIdentity();
    const turkishSummary =
      'Çalışma özeti: Önbellek tutarlılığı ve çoklu kaynak izolasyonu doğrulandı (şğüşıöçİ).';
    const turkishJobTitle = 'Sınırlı pencere ve özet doğrulama işi';

    const longTurkishBody = `${turkishSummary} ${'Ek ayrıntı metni '.repeat(20)}`;
    const truncatedMsg = createSafeWorkspaceMessageItem({
      source_kind: 'task_result_summary',
      source_record_id: 'res_tr_01',
      source_order: { recorded_at_ms: null, source_seq: 7, sub_index: 0 },
      scope: {
        source_id: id.source_id,
        binding_id: id.binding_id,
        thread_uuid: id.thread_uuid,
        canonical_project_scope: id.canonical_project_scope,
        chat_identity: id,
        job_id: 'job_bounded_01',
      },
      raw_summary: longTurkishBody,
      incomplete_reason: 'Upstream summary capped',
    });

    const boundedJob = makeWorkspaceJobHistoryRecord(id, {
      job: {
        source_id: id.source_id,
        job_id: 'job_bounded_01',
        binding_id: id.binding_id,
        chat_identity: id,
        title: turkishJobTitle,
        job_revision: 4,
        state: 'running',
        phase: 'execution',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: null,
        created_at_ms: NOW_MS - 20_000,
        updated_at_ms: NOW_MS - 1_000,
        summary: turkishSummary,
      },
      truncation: {
        tasks_omitted: 2,
        attempts_omitted: 3,
        candidates_omitted: 1,
        runs_omitted: 4,
        outbox_omitted: 5,
        combined_candidates_omitted: 1,
      },
    });

    const boundedRequest = makeRequest({
      source_id: id.source_id,
      id: 'req_bounded_children',
      job_id: 'job_bounded_01',
      binding_id: id.binding_id,
      thread_uuid: id.thread_uuid,
      title: 'Alt çağrılı yürütme isteği',
      summary: turkishSummary,
      children: [
        {
          id: 'call_01',
          title: 'Birinci alt çağrı',
          state: 'completed',
          executor: makeExecutor(),
          started_at_ms: NOW_MS - 8_000,
          finished_at_ms: NOW_MS - 4_000,
          duration_ms: 4_000,
          usage: null,
          summary: 'Alt çağrı tamamlandı.',
        },
      ],
      children_count: 4,
    });

    const ws = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [makeSource({ id: id.source_id, project_path: id.canonical_project_scope })],
        requests: [boundedRequest],
        has_more: true,
        jobs_has_more: true,
      }),
      chats: [
        {
          identity: id,
          thread_label: 'Türkçe Başlıklı Sohbet',
          project_label: 'Türkçe Proje Etiketi',
          availability: 'available',
          activity: 'working',
          configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
          active_job_count: 1,
          attention_count: 0,
          updated_at_ms: NOW_MS - 1_000,
        },
      ],
      native_samples: [makeLiveNativeSample(id, { activity: 'working' })],
      job_histories: [boundedJob],
      messages: {
        availability: 'available',
        items: [truncatedMsg],
        max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
        omitted_count: 6,
        has_more: true,
        truncated: true,
        requires_transcript: false,
        allows_synthetic_user_messages: false,
      },
      bounds: {
        native_samples_omitted: 3,
        job_histories_omitted: 2,
        messages_omitted: 6,
        requests_capped: true,
        jobs_capped: true,
      },
    });

    const projected = projectWorkspaceSnapshot(ws, { nowMs: NOW_MS });
    assert.ok(projected.boundsNotices.some((n) => n.includes('3 native sample(s) were omitted')));
    assert.ok(
      projected.boundsNotices.some((n) => n.includes('2 job history record(s) were omitted')),
    );
    assert.ok(projected.boundsNotices.some((n) => n.includes('jobs_has_more')));
    assert.ok(
      projected.boundsNotices.some((n) => n.includes('6 message summary item(s) were omitted')),
    );
    assert.ok(
      projected.boundsNotices.some((n) =>
        n.includes('Additional message summaries exist beyond the current bounded window.'),
      ),
    );
    assert.ok(
      projected.boundsNotices.some((n) => n.includes('capped at page limit')),
    );

    const sc = projected.selectedChat!;
    // Agent/user content remains in Turkish (source language):
    assert.equal(sc.threadLabel, 'Türkçe Başlıklı Sohbet');
    assert.equal(sc.projectLabel, 'Türkçe Proje Etiketi');
    assert.ok(sc.messages.items[0]?.summary.startsWith(turkishSummary));
    assert.equal(sc.jobs.items[0]?.title, turkishJobTitle);
    assert.equal(sc.jobs.items[0]?.summary, turkishSummary);
    assert.equal(sc.activity.items[0]?.title, 'Alt çağrılı yürütme isteği');
    assert.equal(sc.activity.items[0]?.summary, turkishSummary);

    // Bounded/truncated/omitted notices are visible in English across all three surfaces:
    assert.ok(
      sc.messages.statusNotices.some((n) => n.includes('6 summary item(s) were omitted')),
    );
    assert.ok(
      sc.messages.statusNotices.some((n) =>
        n.includes('Additional message summaries exist beyond this bounded window.'),
      ),
    );
    assert.ok(
      sc.messages.items[0]?.completenessNotices.some((n) =>
        n.includes('Summary truncated to 240 characters'),
      ),
    );
    assert.ok(
      sc.messages.items[0]?.completenessNotices.some((n) =>
        n.includes('Incomplete metadata: missing recorded_at_ms.'),
      ),
    );
    assert.equal(sc.jobs.items[0]?.truncationNotices.length, 6);
    assert.ok(
      sc.jobs.statusNotices.some((n) => n.includes('Bounded attempt history: 3 older attempt(s) omitted.')),
    );
    assert.ok(
      sc.activity.items[0]?.truncationNotice?.includes(
        'showing 1 of 4 child call(s) (3 omitted)',
      ),
    );
    assert.ok(
      sc.activity.statusNotices.some((n) => n.includes('page limit reached')),
    );
  });

  it('handles loading, null/all-failed sources, legacy ObservationSnapshot transition inputs, and multi-scope source ambiguity cleanly', () => {
    const loadingProj = projectWorkspaceSnapshot(null, { nowMs: NOW_MS, loading: true });
    assert.equal(loadingProj.status, 'loading');
    assert.equal(loadingProj.defaultTab, 'messages');

    const nullProj = projectWorkspaceSnapshot(null, {
      nowMs: NOW_MS,
      error: 'IPC bridge timed out',
    });
    assert.equal(nullProj.status, 'unavailable');
    assert.equal(nullProj.unavailableReason, 'IPC bridge timed out');
    assert.deepEqual(nullProj.statusNotices, ['IPC bridge timed out']);

    const allFailedWs = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [
          makeSource({
            id: 'src-down',
            label: 'Offline Store',
            availability: 'unavailable',
            message: 'Directory missing',
          }),
        ],
      }),
    });
    const allFailedProj = projectWorkspaceSnapshot(allFailedWs, { nowMs: NOW_MS });
    assert.equal(allFailedProj.status, 'unavailable');
    assert.ok(
      allFailedProj.unavailableReason?.includes(
        'Observation source "Offline Store" (src-down) is unavailable: Directory missing',
      ),
    );

    // Transition compatibility: legacy ObservationSnapshot upgrades cleanly through shared helpers.
    const legacySnapshot = makeSnapshot({
      sources: [makeSource({ id: 'src-a', project_path: '/work/alpha' })],
      jobs: [
        makeJob({
          source_id: 'src-a',
          id: 'job_legacy_01',
          binding_id: 'bnd_legacy',
          thread_uuid: UUID_CHAT_A,
          project_path: '/work/alpha',
          state: 'running',
        }),
      ],
      requests: [
        makeRequest({
          source_id: 'src-a',
          id: 'req_legacy_01',
          job_id: 'job_legacy_01',
          binding_id: 'bnd_legacy',
          thread_uuid: UUID_CHAT_A,
          state: 'running',
        }),
      ],
    });
    const legacyUpgradedProj = projectWorkspaceSnapshot(legacySnapshot, { nowMs: NOW_MS });
    assert.equal(legacyUpgradedProj.status, 'ready');
    assert.equal(legacyUpgradedProj.activeChatCount, 1);
    assert.equal(legacyUpgradedProj.selectedChat?.activeTab, 'messages');
    assert.equal(legacyUpgradedProj.selectedChat?.jobs.items[0]?.jobId, 'job_legacy_01');
    assert.equal(
      legacyUpgradedProj.selectedChat?.activity.items[0]?.requestId,
      'req_legacy_01',
    );

    // Multi-project source ambiguity: if src-a serves both /work/alpha and /work/alpha-wt, an unlinked request on a brand-new triple without canonical_project_scope must be withheld.
    const idAlpha = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_1',
      thread_uuid: UUID_CHAT_A,
      canonical_project_scope: '/work/alpha',
    });
    const idAlphaWt = makeChatIdentity({
      source_id: 'src-a',
      binding_id: 'bnd_2',
      thread_uuid: UUID_CHAT_B,
      canonical_project_scope: '/work/alpha-wt',
    });
    const unseenTripleReq = makeRequest({
      source_id: 'src-a',
      id: 'req_unseen_triple',
      job_id: null,
      binding_id: 'bnd_unseen',
      thread_uuid: UUID_CHAT_C,
      state: 'running',
    });
    const multiScopeWs = makeWorkspaceSnapshotFixture({
      observation: makeSnapshot({
        sources: [makeSource({ id: 'src-a', project_path: '/work/alpha' })],
        requests: [unseenTripleReq],
      }),
      native_samples: [makeLiveNativeSample(idAlpha), makeLiveNativeSample(idAlphaWt)],
    });
    assert.equal(
      resolveObservationRequestChatIdentity(unseenTripleReq, multiScopeWs),
      null,
    );
  });
});
