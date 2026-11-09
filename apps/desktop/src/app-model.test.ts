import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  isCanonicalUuid,
  projectDesktopWorkspace,
  readProjectsArchiveCutoff,
  writeProjectsArchiveCutoff,
  type ProjectsArchiveStorage,
} from './app-model.ts';
import type {
  ObservationActor,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from './observation/types.ts';

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
