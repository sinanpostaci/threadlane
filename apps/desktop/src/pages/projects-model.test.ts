import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  ObservationCall,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from '../observation/types.ts';
import { projectProjects } from './projects-model.ts';

const NOW_MS = Date.parse('2026-11-08T12:00:00.000Z');
const ONE_HOUR_MS = 60 * 60 * 1000;

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src-1',
    label: 'Threadlane',
    project_path: '/work/threadlane',
    availability: 'available',
    message: null,
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

function makeLocalMcpExecutor(): ObservationExecutor {
  return {
    tool: 'threadlane_mcp',
    provider: 'local',
    role: null,
    requested_model: null,
    observed_model: null,
    requested_effort: null,
    observed_effort: null,
  };
}

function makeCall(overrides: Partial<ObservationCall> = {}): ObservationCall {
  return {
    id: 'call-1',
    title: 'Implement projection',
    state: 'completed',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 10_000,
    finished_at_ms: NOW_MS - 5_000,
    duration_ms: 5_000,
    usage: {
      input_tokens: 1200,
      output_tokens: 400,
      cache_read_tokens: 200,
      total_tokens: 1600,
      provenance: 'provider_receipt',
    },
    summary: 'Completed projection module.',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src-1',
    id: 'req-1',
    job_id: null,
    parent_request_id: null,
    title: 'Worker handoff',
    operation: 'flow.handoff',
    project_label: 'Threadlane',
    binding_id: 'bind-1',
    thread_uuid: 'thread-1',
    thread_label: 'Projects UI',
    state: 'completed',
    started_at_ms: NOW_MS - 15_000,
    finished_at_ms: NOW_MS - 5_000,
    updated_at_ms: NOW_MS - 5_000,
    duration_ms: 10_000,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'codex',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: makeExecutor(),
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Dispatched worker.',
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src-1',
    id: 'job-1',
    project_label: 'Threadlane',
    project_path: '/work/threadlane',
    title: 'Build Projects UI',
    thread_uuid: 'thread-1',
    thread_label: 'Projects UI',
    binding_id: 'bind-1',
    plan_id: 'plan-7',
    plan_revision: 3,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 2_000,
    active_call_count: 1,
    tasks: [
      { id: 't1', title: 'Projection model', status: 'completed', depends_on: [] },
      { id: 't2', title: 'Page component', status: 'running', depends_on: ['t1'] },
    ],
    summary: 'Implementing Projects view.',
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

test('same-name different chats and same-name different projects stay isolated', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-a', label: 'App', project_path: '/orgs/a/app' }),
      makeSource({ id: 'src-b', label: 'App', project_path: '/orgs/b/app' }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-a',
        id: 'req-a1',
        project_label: 'App',
        thread_uuid: 'thread-a1',
        binding_id: 'bind-a1',
        thread_label: 'Bug Triage',
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a2',
        project_label: 'App',
        thread_uuid: 'thread-a2',
        binding_id: 'bind-a2',
        thread_label: 'Bug Triage',
      }),
      makeRequest({
        source_id: 'src-b',
        id: 'req-b1',
        project_label: 'App',
        thread_uuid: 'thread-b1',
        binding_id: 'bind-b1',
        thread_label: 'Bug Triage',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 2);
  const firstProject = projection.projects[0];
  const secondProject = projection.projects[1];
  assert.ok(firstProject);
  assert.ok(secondProject);
  assert.notEqual(firstProject.key, secondProject.key);
  assert.equal(firstProject.path, '/orgs/a/app');
  assert.equal(secondProject.path, '/orgs/b/app');
  assert.equal(firstProject.sessions.length, 2);
  assert.equal(firstProject.sessions[0]?.label, 'Bug Triage');
  assert.equal(firstProject.sessions[1]?.label, 'Bug Triage');
  assert.notEqual(firstProject.sessions[0]?.key, firstProject.sessions[1]?.key);
  assert.equal(firstProject.sessions[0]?.threadUuid, 'thread-a1');
  assert.equal(firstProject.sessions[1]?.threadUuid, 'thread-a2');
});

test('same IDs across different sources do not collide in projects, sessions, jobs, or scope', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-1', label: 'Service', project_path: null }),
      makeSource({ id: 'src-2', label: 'Service', project_path: null }),
    ],
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-shared',
        project_label: 'Service',
        project_path: null,
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 1 Job',
      }),
      makeJob({
        source_id: 'src-2',
        id: 'job-shared',
        project_label: 'Service',
        project_path: null,
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 2 Job',
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-shared',
        job_id: 'job-shared',
        project_label: 'Service',
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 1 Call',
      }),
      makeRequest({
        source_id: 'src-2',
        id: 'req-shared',
        job_id: 'job-shared',
        project_label: 'Service',
        thread_uuid: 'thread-shared',
        binding_id: 'bind-shared',
        title: 'Source 2 Call',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 2);
  assert.notEqual(projection.projects[0]?.key, projection.projects[1]?.key);
  assert.notEqual(
    projection.projects[0]?.sessions[0]?.key,
    projection.projects[1]?.sessions[0]?.key
  );
  assert.notEqual(
    projection.projects[0]?.sessions[0]?.jobs[0]?.key,
    projection.projects[1]?.sessions[0]?.jobs[0]?.key
  );

  const scoped = projectProjects(snapshot, {
    nowMs: NOW_MS,
    scope: { source_id: 'src-2', job_id: 'job-shared' },
  });
  assert.equal(scoped.projects.length, 1);
  assert.equal(scoped.projects[0]?.sessions.length, 1);
  assert.equal(scoped.projects[0]?.sessions[0]?.jobs[0]?.job.title, 'Source 2 Job');
  assert.equal(scoped.projects[0]?.sessions[0]?.calls[0]?.title, 'Source 2 Call');
});

test('missing thread and binding identities isolate to job or request without collapsing by label', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-a',
        title: 'Job Alpha',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
      }),
      makeJob({
        id: 'job-b',
        title: 'Job Beta',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-linked-a',
        job_id: 'job-a',
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Call for Job Alpha',
      }),
      makeRequest({
        id: 'req-unlinked-1',
        job_id: null,
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Standalone Call 1',
      }),
      makeRequest({
        id: 'req-unlinked-2',
        job_id: null,
        thread_uuid: null,
        binding_id: null,
        thread_label: 'Shared Label',
        title: 'Standalone Call 2',
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 1);
  const sessions = projection.projects[0]?.sessions ?? [];
  assert.equal(sessions.length, 4);

  const jobASession = sessions.find((s) => s.jobs[0]?.job.id === 'job-a');
  assert.ok(jobASession);
  assert.equal(jobASession.calls.length, 1);
  assert.equal(jobASession.calls[0]?.title, 'Call for Job Alpha');
});

test('duplicate requests and jobs deduplicate stably by source+id and newest meaningful timestamp', () => {
  const olderRequest = makeRequest({
    id: 'req-dup',
    state: 'running',
    updated_at_ms: NOW_MS - 20_000,
    finished_at_ms: null,
    title: 'Stale running copy',
  });
  const newerRequest = makeRequest({
    id: 'req-dup',
    state: 'completed',
    updated_at_ms: NOW_MS - 5_000,
    finished_at_ms: NOW_MS - 5_000,
    title: 'Newest completed copy',
  });
  const olderJob = makeJob({
    id: 'job-dup',
    state: 'planning',
    updated_at_ms: NOW_MS - 30_000,
  });
  const newerJob = makeJob({
    id: 'job-dup',
    state: 'reviewing',
    updated_at_ms: NOW_MS - 1_000,
  });

  const forward = projectProjects(
    makeSnapshot({
      jobs: [olderJob, newerJob],
      requests: [olderRequest, newerRequest],
    }),
    { nowMs: NOW_MS }
  );
  const reverse = projectProjects(
    makeSnapshot({
      jobs: [newerJob, olderJob],
      requests: [newerRequest, olderRequest],
    }),
    { nowMs: NOW_MS }
  );

  const forwardSession = forward.projects[0]?.sessions[0];
  const reverseSession = reverse.projects[0]?.sessions[0];
  assert.ok(forwardSession);
  assert.ok(reverseSession);
  assert.equal(forwardSession.calls.length, 1);
  assert.equal(forwardSession.calls[0]?.title, 'Newest completed copy');
  assert.equal(forwardSession.calls[0]?.running, false);
  assert.equal(forwardSession.jobs.length, 1);
  assert.equal(forwardSession.jobs[0]?.stateLabel, 'Reviewing');
  assert.deepEqual(forwardSession, reverseSession);
});

test('root completed with running child deduplicates root/child identity and displays running child while keeping local MCP in recent', () => {
  const runningChild = makeCall({
    id: 'call-worker-1',
    title: 'Argon implementation worker',
    state: 'running',
    started_at_ms: NOW_MS - 8_000,
    finished_at_ms: null,
    duration_ms: null,
  });
  const queuedChild = makeCall({
    id: 'call-worker-2',
    title: 'Queued follow-up',
    state: 'queued',
    started_at_ms: null,
    finished_at_ms: null,
    duration_ms: null,
  });

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-root',
        title: 'Dispatch worker',
        state: 'completed',
        executor: makeExecutor(),
        children: [runningChild, queuedChild],
        children_count: 2,
      }),
      makeRequest({
        id: 'call-worker-1',
        parent_request_id: 'req-root',
        title: 'Argon implementation worker',
        state: 'running',
        started_at_ms: NOW_MS - 8_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 2_000,
        executor: makeExecutor(),
      }),
      makeRequest({
        id: 'req-mcp-status',
        title: 'Check threadlane status',
        operation: 'threadlane.status',
        state: 'running',
        executor: makeLocalMcpExecutor(),
        children: [],
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.calls.length, 2);
  assert.equal(session.runningCount, 1);
  assert.equal(projection.runningCount, 1);
  assert.equal(session.calls[0]?.title, 'Argon implementation worker');
  assert.equal(session.calls[0]?.running, true);
  assert.equal(session.calls[0]?.stateLabel, 'Running');
  assert.equal(session.calls[1]?.title, 'Queued follow-up');
  assert.equal(session.calls[1]?.running, false);
  assert.equal(session.recent.length, 1);
  assert.equal(session.recent[0]?.label, 'Check threadlane status');
});

test('old active, waiting, unresolved approval, and unknown sessions are retained beyond 24h while old terminal completed/failed sessions are filtered', () => {
  const oldMs = NOW_MS - 48 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-old-reviewing',
        thread_uuid: 'thread-reviewing',
        thread_label: 'Active Reviewing',
        state: 'reviewing',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-ready',
        thread_uuid: 'thread-ready',
        thread_label: 'Unresolved Approval',
        state: 'ready',
        approval_status: 'pending',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-unknown',
        thread_uuid: 'thread-unknown',
        thread_label: 'Unknown Outcome',
        state: 'unknown',
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-done',
        thread_uuid: 'thread-done',
        thread_label: 'Old Completed',
        state: 'completed',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
      makeJob({
        id: 'job-old-failed',
        thread_uuid: 'thread-failed',
        thread_label: 'Old Failed',
        state: 'failed',
        active_call_count: 0,
        updated_at_ms: oldMs,
      }),
    ],
    requests: [
      makeRequest({
        id: 'req-worker-done-under-review',
        job_id: 'job-old-reviewing',
        thread_uuid: 'thread-reviewing',
        thread_label: 'Active Reviewing',
        state: 'completed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
      makeRequest({
        id: 'req-old-waiting',
        thread_uuid: 'thread-waiting',
        thread_label: 'Waiting For User',
        state: 'waiting_for_user',
        updated_at_ms: oldMs,
        finished_at_ms: null,
        started_at_ms: oldMs,
      }),
      makeRequest({
        id: 'req-old-done',
        job_id: 'job-old-done',
        thread_uuid: 'thread-done',
        thread_label: 'Old Completed',
        state: 'completed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
      makeRequest({
        id: 'req-old-failed',
        job_id: 'job-old-failed',
        thread_uuid: 'thread-failed',
        thread_label: 'Old Failed',
        state: 'failed',
        updated_at_ms: oldMs,
        finished_at_ms: oldMs,
        started_at_ms: oldMs - 5_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
  });
  assert.equal(projection.hiddenSessionCount, 2);
  const sessions = projection.projects[0]?.sessions ?? [];
  assert.equal(
    sessions.map((s) => s.label).join(','),
    'Active Reviewing,Unknown Outcome,Unresolved Approval,Waiting For User'
  );

  const reviewingSession = sessions.find((s) => s.label === 'Active Reviewing');
  assert.ok(reviewingSession);
  assert.equal(reviewingSession.jobs[0]?.stateLabel, 'Reviewing');
  assert.equal(reviewingSession.stateLabel, 'Reviewing');
});

test('exact 6h boundary retains session at cutoff and filters session 1ms older under 6h window', () => {
  const exactSixHoursAgo = NOW_MS - 6 * ONE_HOUR_MS;
  const beyondSixHoursAgo = exactSixHoursAgo - 1;

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-exact-6h',
        thread_uuid: 'thread-exact',
        thread_label: 'Exact Boundary',
        state: 'completed',
        started_at_ms: exactSixHoursAgo,
        finished_at_ms: exactSixHoursAgo,
        updated_at_ms: exactSixHoursAgo,
      }),
      makeRequest({
        id: 'req-beyond-6h',
        thread_uuid: 'thread-beyond',
        thread_label: 'Beyond Boundary',
        state: 'completed',
        started_at_ms: beyondSixHoursAgo,
        finished_at_ms: beyondSixHoursAgo,
        updated_at_ms: beyondSixHoursAgo,
      }),
    ],
  });

  const sixHourView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 6,
  });
  assert.equal(sixHourView.hiddenSessionCount, 1);
  assert.equal(sixHourView.projects[0]?.sessions.length, 1);
  assert.equal(sixHourView.projects[0]?.sessions[0]?.label, 'Exact Boundary');

  const twentyFourHourView = projectProjects(snapshot, {
    nowMs: NOW_MS,
    windowHours: 24,
  });
  assert.equal(twentyFourHourView.hiddenSessionCount, 0);
  assert.equal(twentyFourHourView.projects[0]?.sessions.length, 2);
});

test('project and session order stays alphabetical and stable after new activity and with future or nonfinite timestamps', () => {
  const initial = makeSnapshot({
    sources: [
      makeSource({ id: 'src-z', label: 'Zeta', project_path: '/work/zeta' }),
      makeSource({ id: 'src-a', label: 'Alpha', project_path: '/work/alpha' }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-z',
        id: 'req-z',
        project_label: 'Zeta',
        thread_uuid: 'thread-z',
        thread_label: 'Zeta Chat',
        updated_at_ms: NOW_MS - 1_000,
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-beta',
        project_label: 'Alpha',
        thread_uuid: 'thread-beta',
        thread_label: 'Beta Session',
        updated_at_ms: NOW_MS - 1_000,
      }),
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-alpha',
        project_label: 'Alpha',
        thread_uuid: 'thread-alpha',
        thread_label: 'Alpha Session',
        updated_at_ms: NOW_MS - 60_000,
      }),
    ],
  });

  const firstProjection = projectProjects(initial, { nowMs: NOW_MS });
  assert.deepEqual(
    firstProjection.projects.map((p) => p.label),
    ['Alpha', 'Zeta']
  );
  assert.deepEqual(
    firstProjection.projects[0]?.sessions.map((s) => s.label),
    ['Alpha Session', 'Beta Session']
  );

  const afterNewActivity = makeSnapshot({
    ...initial,
    requests: [
      ...initial.requests,
      makeRequest({
        source_id: 'src-a',
        id: 'req-a-beta-new',
        project_label: 'Alpha',
        thread_uuid: 'thread-beta',
        thread_label: 'Beta Session',
        state: 'running',
        started_at_ms: NOW_MS + 30_000,
        finished_at_ms: Number.NaN,
        updated_at_ms: NOW_MS + 30_000,
      }),
    ],
  });

  const secondProjection = projectProjects(afterNewActivity, { nowMs: NOW_MS });
  assert.deepEqual(
    secondProjection.projects.map((p) => p.label),
    ['Alpha', 'Zeta']
  );
  assert.deepEqual(
    secondProjection.projects[0]?.sessions.map((s) => s.label),
    ['Alpha Session', 'Beta Session']
  );
});

test('job-only snapshot projects sessions and inline job summaries with zero requests', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        id: 'job-only-1',
        title: 'Standalone Job Plan',
        thread_uuid: 'thread-job-only',
        thread_label: 'Planning Chat',
        state: 'planning',
        plan_id: 'plan-42',
        plan_revision: 2,
        updated_at_ms: NOW_MS - 10_000,
      }),
    ],
    requests: [],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.projects.length, 1);
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.label, 'Planning Chat');
  assert.equal(session.calls.length, 0);
  assert.equal(session.recent.length, 0);
  assert.equal(session.jobs.length, 1);
  assert.equal(session.jobs[0]?.stateLabel, 'Planning');
  assert.equal(session.jobs[0]?.planLabel, 'Plan · revision 2');
  assert.equal(session.jobs[0]?.job.plan_id, 'plan-42');
  assert.match(session.jobs[0]?.taskSummary ?? '', /2 tasks recorded · 1 marked completed/);
});

test('missing usage stays Usage unknown, requested model is not faked as observed, and parent+child tokens are never summed', () => {
  const childWithoutObservedOrUsage = makeCall({
    id: 'call-unobserved',
    title: 'Draft patch',
    executor: makeExecutor({
      requested_model: 'opencode-go/deepseek-v4.1-flash',
      observed_model: null,
      requested_effort: 'max',
      observed_effort: null,
      tool: 'opencode-go',
      provider: 'opencode-go',
    }),
    usage: null,
  });

  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-parent-tokens',
        usage: {
          input_tokens: 500,
          output_tokens: 500,
          cache_read_tokens: 0,
          total_tokens: 1000,
          provenance: 'parent_estimate',
        },
        children: [childWithoutObservedOrUsage],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const call = projection.projects[0]?.sessions[0]?.calls[0];
  assert.ok(call);
  assert.equal(call.usageLabel, 'Usage unknown');
  assert.equal(call.model, 'DeepSeek');
  assert.equal(call.effort, 'max');
  assert.equal(call.brand, 'opencode');
  assert.match(
    call.detail,
    /Model: requested opencode-go\/deepseek-v4\.1-flash · observed Not reported\./
  );
  assert.match(call.detail, /Effort: requested max · observed Not reported\./);
  assert.doesNotMatch(call.detail, /1,000/);
});

test('partial source coverage and bounded snapshot flags surface unavailable counts and warnings', () => {
  const snapshot = makeSnapshot({
    sources: [
      makeSource({ id: 'src-ok', label: 'Primary', availability: 'available' }),
      makeSource({
        id: 'src-down',
        label: 'Secondary',
        availability: 'unavailable',
        message: 'Socket disconnected',
      }),
    ],
    has_more: true,
    jobs_has_more: true,
    warnings: ['Bounded observation page returned.'],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  assert.equal(projection.unavailableSourceCount, 1);
  assert.equal(projection.hasMore, true);
  assert.equal(projection.jobsHasMore, true);
  assert.ok(
    projection.warnings.some((w) => w.includes('Bounded observation page returned.'))
  );
  assert.ok(
    projection.warnings.some((w) =>
      w.includes('Source Secondary is unavailable: Socket disconnected')
    )
  );
  assert.ok(projection.warnings.some((w) => w.includes('jobs_has_more')));
  assert.ok(projection.warnings.some((w) => w.includes('has_more')));
  assert.ok(projection.emptyMessage !== null);
});

test('completed job settles earlier failed request for summary while calls retain failed history', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-1',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-failed',
        job_id: 'job-1',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        started_at_ms: NOW_MS - 20_000,
        finished_at_ms: NOW_MS - 10_000,
        updated_at_ms: NOW_MS - 10_000,
        children: [
          makeCall({
            id: 'call-failed',
            state: 'failed',
            started_at_ms: NOW_MS - 19_000,
            finished_at_ms: NOW_MS - 10_000,
          }),
        ],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.stateLabel, 'Completed');
  assert.equal(session.attention, false);
  assert.ok(session.calls.some((call) => call.stateLabel === 'Failed'));
});

test('same chat old terminal failed job does not override current completed job or appear inline', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-old-failed',
        title: 'Old failed job',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-current-ok',
        title: 'Current completed job',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.stateLabel, 'Completed');
  assert.equal(session.attention, false);
  assert.deepEqual(
    session.jobs.map((job) => job.job.id),
    ['job-current-ok']
  );
});

test('current failed job with running call surfaces running count and needs attention', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-running',
        job_id: 'job-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'running',
        started_at_ms: NOW_MS - 3_000,
        finished_at_ms: null,
        updated_at_ms: NOW_MS - 1_000,
        children: [
          makeCall({
            id: 'call-running',
            state: 'running',
            started_at_ms: NOW_MS - 3_000,
            finished_at_ms: null,
          }),
        ],
        children_count: 1,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.equal(session.attention, true);
  assert.equal(session.runningCount, 1);
  assert.equal(session.tone, 'danger');
  assert.ok(session.stateLabel.includes('Needs attention'));
});

test('old failed job and old failed request do not mark current completed chat failed, while current failed request still demands attention', () => {
  const snapshot = makeSnapshot({
    jobs: [
      makeJob({
        source_id: 'src-1',
        id: 'job-old-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        execution_status: 'failed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
      }),
      makeJob({
        source_id: 'src-1',
        id: 'job-current-ok',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'completed',
        execution_status: 'completed',
        active_call_count: 0,
        updated_at_ms: NOW_MS - 2_000,
      }),
    ],
    requests: [
      makeRequest({
        source_id: 'src-1',
        id: 'req-old-failed',
        job_id: 'job-old-failed',
        thread_uuid: 'thread-1',
        binding_id: 'bind-1',
        state: 'failed',
        started_at_ms: NOW_MS - 48 * ONE_HOUR_MS - 5_000,
        finished_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
        updated_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
        children: [
          makeCall({
            id: 'call-old-failed',
            state: 'failed',
            started_at_ms: NOW_MS - 48 * ONE_HOUR_MS - 4_000,
            finished_at_ms: NOW_MS - 48 * ONE_HOUR_MS,
          }),
        ],
        children_count: 1,
      }),
      makeRequest({
        source_id: 'src-1',
        id: 'req-current-failed',
        job_id: null,
        thread_uuid: 'thread-2',
        thread_label: 'Active Failing Chat',
        binding_id: 'bind-2',
        state: 'failed',
        started_at_ms: NOW_MS - 4_000,
        finished_at_ms: NOW_MS - 1_000,
        updated_at_ms: NOW_MS - 1_000,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const settledSession = projection.projects[0]?.sessions.find(
    (item) => item.label === 'Projects UI'
  );
  assert.ok(settledSession);
  assert.equal(settledSession.stateLabel, 'Completed');
  assert.equal(settledSession.attention, false);
  assert.equal(settledSession.calls.length, 0);

  const failingSession = projection.projects[0]?.sessions.find(
    (item) => item.label === 'Active Failing Chat'
  );
  assert.ok(failingSession);
  assert.equal(failingSession.stateLabel, 'Failed');
  assert.equal(failingSession.attention, true);
});

test('child calls and local child events prefer own timestamps over newer parent updated_at_ms', () => {
  const oldFinishedMs = NOW_MS - 48 * ONE_HOUR_MS;
  const snapshot = makeSnapshot({
    requests: [
      makeRequest({
        id: 'req-parent-updated',
        state: 'completed',
        started_at_ms: oldFinishedMs - 5_000,
        finished_at_ms: NOW_MS - 1_000,
        updated_at_ms: NOW_MS - 1_000,
        children: [
          makeCall({
            id: 'call-old-child',
            title: 'Old finished child',
            state: 'failed',
            started_at_ms: oldFinishedMs - 2_000,
            finished_at_ms: oldFinishedMs,
          }),
          makeCall({
            id: 'call-new-child',
            title: 'Fresh finished child',
            state: 'completed',
            started_at_ms: NOW_MS - 3_000,
            finished_at_ms: NOW_MS - 1_000,
          }),
          makeCall({
            id: 'call-old-local',
            title: 'Old local child',
            state: 'completed',
            executor: makeLocalMcpExecutor(),
            started_at_ms: oldFinishedMs - 1_000,
            finished_at_ms: oldFinishedMs,
          }),
        ],
        children_count: 3,
      }),
    ],
  });

  const projection = projectProjects(snapshot, { nowMs: NOW_MS });
  const session = projection.projects[0]?.sessions[0];
  assert.ok(session);
  assert.deepEqual(
    session.calls.map((call) => call.title),
    ['Fresh finished child']
  );
  const localEvent = session.recent.find((event) => event.label === 'Old local child');
  assert.ok(localEvent);
  assert.equal(localEvent.atMs, oldFinishedMs);
});
