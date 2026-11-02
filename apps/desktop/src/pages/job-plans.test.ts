import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type {
  ObservationJob,
  ObservationSnapshot,
  ObservationSource,
} from '../observation/types.ts';
import {
  boundConversationLabel,
  buildJobPlanGroups,
  describeJobPlansState,
  formatJobLifecycle,
  formatRecordedStatus,
  formatReviewState,
  jobActivityScope,
  jobPlanGroupsSignature,
  jobPlanKey,
  jobPlanRenderSignature,
  jobPlanRowSignature,
  resolveTaskDependencies,
  summarizeActiveCalls,
  summarizeJobProgress,
} from './job-plans-model.ts';

const NOW_MS = 1_730_000_000_000;

function makeSource(overrides: Partial<ObservationSource> = {}): ObservationSource {
  return {
    id: 'source-alpha',
    label: 'Alpha source',
    project_path: null,
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'source-alpha',
    id: 'job-1',
    project_label: 'Alpha project',
    project_path: '/work/alpha',
    title: 'Recorded job',
    thread_uuid: '00000000-0000-0000-0000-000000000001',
    thread_label: 'Alpha chat',
    binding_id: 'binding-alpha',
    plan_id: null,
    plan_revision: null,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS,
    active_call_count: 0,
    tasks: [],
    summary: null,
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<ObservationSnapshot> = {}): ObservationSnapshot {
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

describe('job plan projections', () => {
  it('groups projects deterministically while keeping same-label source scopes separate', () => {
    const snapshot = makeSnapshot({
      sources: [
        makeSource({ id: 'source-alpha', label: 'Alpha source' }),
        makeSource({ id: 'source-beta', label: 'Beta source' }),
      ],
      jobs: [
        makeJob({
          source_id: 'source-alpha',
          id: 'alpha-old',
          project_label: 'Alpha project',
          project_path: '/work/alpha',
          updated_at_ms: 100,
        }),
        makeJob({
          source_id: 'source-beta',
          id: 'loose-beta',
          project_label: 'Loose project',
          project_path: null,
          updated_at_ms: 400,
        }),
        makeJob({
          source_id: 'source-alpha',
          id: 'alpha-new',
          project_label: 'Alpha project',
          project_path: '/work/alpha',
          updated_at_ms: 300,
        }),
        makeJob({
          source_id: 'source-alpha',
          id: 'loose-alpha',
          project_label: 'Loose project',
          project_path: null,
          updated_at_ms: null,
        }),
        makeJob({
          source_id: 'source-alpha',
          id: 'beta-current',
          project_label: 'Beta project',
          project_path: '/work/beta',
          updated_at_ms: 200,
        }),
      ],
    });

    const groups = buildJobPlanGroups(snapshot);
    assert.deepEqual(
      groups.map((group) => group.label),
      ['Loose project', 'Alpha project', 'Beta project', 'Loose project']
    );
    assert.deepEqual(
      groups.map((group) => group.jobs.map((job) => job.id)),
      [
        ['loose-beta'],
        ['alpha-new', 'alpha-old'],
        ['beta-current'],
        ['loose-alpha'],
      ]
    );
    assert.notEqual(groups[0]?.key, groups[3]?.key);
    assert.deepEqual(groups[0]?.sourceIds, ['source-beta']);
    assert.deepEqual(groups[3]?.sourceIds, ['source-alpha']);
  });

  it('keeps two jobs in one project and one conversation independent', () => {
    const first = makeJob({
      id: 'job-shared-chat-old',
      updated_at_ms: 100,
      thread_uuid: 'shared-thread',
      thread_label: 'Shared chat',
      binding_id: 'shared-binding',
    });
    const second = makeJob({
      id: 'job-shared-chat-new',
      updated_at_ms: 200,
      thread_uuid: 'shared-thread',
      thread_label: 'Shared chat',
      binding_id: 'shared-binding',
    });
    const groups = buildJobPlanGroups(makeSnapshot({ jobs: [first, second] }));

    assert.equal(groups.length, 1);
    assert.deepEqual(
      groups[0]?.jobs.map((job) => job.id),
      ['job-shared-chat-new', 'job-shared-chat-old']
    );
    assert.notEqual(jobPlanKey(first), jobPlanKey(second));
    assert.deepEqual(jobActivityScope(first), {
      source_id: 'source-alpha',
      job_id: 'job-shared-chat-old',
    });
    assert.deepEqual(jobActivityScope(second), {
      source_id: 'source-alpha',
      job_id: 'job-shared-chat-new',
    });
  });

  it('keeps identical job IDs from different sources distinct', () => {
    const alpha = makeJob({ source_id: 'source-alpha', id: 'job-1' });
    const beta = makeJob({ source_id: 'source-beta', id: 'job-1' });
    const groups = buildJobPlanGroups(
      makeSnapshot({
        sources: [
          makeSource({ id: 'source-alpha' }),
          makeSource({ id: 'source-beta' }),
        ],
        jobs: [beta, alpha],
      })
    );

    assert.equal(groups.length, 1);
    assert.equal(groups[0]?.jobs.length, 2);
    assert.notEqual(jobPlanKey(alpha), jobPlanKey(beta));
    assert.equal(new Set([jobPlanKey(alpha), jobPlanKey(beta)]).size, 2);
    assert.deepEqual(jobActivityScope(alpha), {
      source_id: 'source-alpha',
      job_id: 'job-1',
    });
    assert.deepEqual(jobActivityScope(beta), {
      source_id: 'source-beta',
      job_id: 'job-1',
    });
  });

  it('encodes job identity without NUL separators or tuple collisions', () => {
    const left = makeJob({ source_id: 'a:b', id: 'c' });
    const right = makeJob({ source_id: 'a', id: 'b:c' });
    const rawControl = makeJob({
      source_id: 'source\u0000alpha',
      id: 'job\u0000one',
    });

    assert.notEqual(jobPlanKey(left), jobPlanKey(right));
    assert.ok(!jobPlanKey(rawControl).includes('\u0000'));
    assert.equal(
      new Set([jobPlanKey(left), jobPlanKey(right), jobPlanKey(rawControl)]).size,
      3
    );
  });

  it('orders missing timestamps deterministically after recorded ones', () => {
    const snapshot = makeSnapshot({
      jobs: [
        makeJob({ id: 'job-z', updated_at_ms: null }),
        makeJob({ id: 'job-a', updated_at_ms: null }),
        makeJob({ id: 'job-m', updated_at_ms: 50 }),
        makeJob({ id: 'job-b', updated_at_ms: 50 }),
      ],
    });

    const groups = buildJobPlanGroups(snapshot);
    assert.equal(groups.length, 1);
    assert.deepEqual(
      groups[0]?.jobs.map((job) => job.id),
      ['job-b', 'job-m', 'job-a', 'job-z']
    );
  });

  it('reports empty, loading, and error states without claiming data exists', () => {
    const loading = describeJobPlansState(null, { loading: true });
    assert.equal(loading.loading, true);
    assert.equal(loading.emptyMessage, null);

    const failed = describeJobPlansState(null, { error: 'IPC unavailable' });
    assert.equal(failed.error, 'IPC unavailable');
    assert.match(failed.emptyMessage ?? '', /failed/i);

    const empty = describeJobPlansState(makeSnapshot({ jobs: [] }));
    assert.equal(empty.loading, false);
    assert.match(empty.emptyMessage ?? '', /no jobs were recorded/i);

    const capped = describeJobPlansState(
      makeSnapshot({ jobs: [], jobs_has_more: true })
    );
    assert.match(capped.emptyMessage ?? '', /more jobs exist/i);
    assert.doesNotMatch(capped.emptyMessage ?? '', /no jobs were recorded/i);
  });

  it('surfaces warnings, unavailable sources, and bounded-history notices', () => {
    const summary = describeJobPlansState(
      makeSnapshot({
        warnings: ['Partial observation page.'],
        sources: [
          makeSource({
            id: 'source-offline',
            label: 'Offline source',
            availability: 'unavailable',
            message: 'Runtime attachment is offline.',
          }),
        ],
        jobs_has_more: true,
        has_more: true,
      })
    );

    assert.ok(summary.notices.includes('Partial observation page.'));
    assert.ok(
      summary.notices.some((notice) =>
        /Source Offline source is unavailable: Runtime attachment is offline\./.test(notice)
      )
    );
    assert.ok(
      summary.notices.some((notice) => /job list is limited/i.test(notice))
    );
    assert.ok(
      summary.notices.some((notice) => /more request activity/i.test(notice))
    );
    assert.ok(
      summary.notices.some((notice) => /bounded observation page/i.test(notice))
    );
    assert.ok(
      summary.notices.every((notice) => !/load more from Activity/i.test(notice))
    );
  });

  it('never invents lifecycle, review, progress, or active-call evidence', () => {
    assert.equal(formatJobLifecycle('unknown'), 'Unknown');
    assert.equal(formatJobLifecycle('not-a-real-state'), 'Unknown');
    assert.equal(formatReviewState(null), 'Unrecorded');
    assert.equal(formatRecordedStatus(''), 'Unrecorded');
    assert.equal(formatRecordedStatus('queued'), 'queued');
    assert.equal(
      boundConversationLabel({ thread_label: null }),
      'Conversation label unavailable'
    );
    assert.equal(
      boundConversationLabel({ thread_label: '  ' }),
      'Conversation label unavailable'
    );
    assert.equal(
      boundConversationLabel(
        makeJob({ thread_label: null, binding_id: 'binding-secret' })
      ),
      'Conversation label unavailable',
      'a recorded binding ID must not become a human-readable conversation label'
    );
    assert.equal(
      boundConversationLabel({ thread_label: 'Shared chat' }),
      'Shared chat'
    );

    const noTasks = makeJob({ tasks: [] });
    assert.equal(summarizeJobProgress(noTasks), null);
    assert.equal(summarizeActiveCalls({ active_call_count: 0 }), null);
    assert.equal(summarizeActiveCalls({ active_call_count: -2 }), null);
    assert.equal(summarizeActiveCalls({ active_call_count: 2 }), '2 active calls');

    const unclassifiedTasks = makeJob({
      tasks: [
        { id: 'task-1', title: 'One', status: 'in_review', depends_on: [] },
        { id: 'task-2', title: 'Two', status: 'weird', depends_on: ['task-1'] },
      ],
    });
    const progress = summarizeJobProgress(unclassifiedTasks) ?? '';
    assert.match(progress, /2 tasks recorded/);
    assert.doesNotMatch(progress, /%|0 of|marked completed/);

    const completedTask = makeJob({
      tasks: [
        { id: 'task-1', title: 'One', status: 'completed', depends_on: [] },
      ],
    });
    assert.match(summarizeJobProgress(completedTask) ?? '', /1 marked completed/);
  });

  it('resolves recorded task titles and keeps unknown dependencies explicit', () => {
    const job = makeJob({
      tasks: [
        { id: 'task-a', title: 'Design the plan', status: 'completed', depends_on: [] },
        {
          id: 'task-b',
          title: 'Build the change',
          status: 'running',
          depends_on: ['task-a', 'missing-task'],
        },
      ],
    });

    assert.deepEqual(resolveTaskDependencies(job, job.tasks[0]!), []);
    assert.deepEqual(resolveTaskDependencies(job, job.tasks[1]!), [
      { id: 'task-a', title: 'Design the plan' },
      { id: 'missing-task', title: null },
    ]);
    assert.equal(
      formatReviewState(job.review_state),
      'Unrecorded',
      'a completed task must not invent a passed review'
    );
  });

  it('keeps activity scope on the exact source/job pair regardless of chat labels', () => {
    const job = makeJob({
      source_id: 'source-alpha',
      id: 'job-1',
      title: 'Original title',
      thread_label: 'Original chat',
      binding_id: 'binding-alpha',
    });
    const relabeled = makeJob({
      source_id: 'source-alpha',
      id: 'job-1',
      title: 'Changed title',
      thread_label: 'Changed chat',
      binding_id: 'binding-beta',
    });

    assert.deepEqual(jobActivityScope(job), {
      source_id: 'source-alpha',
      job_id: 'job-1',
    });
    assert.deepEqual(jobActivityScope(relabeled), jobActivityScope(job));
    assert.equal(jobPlanKey(relabeled), jobPlanKey(job));
  });

  it('keeps the rendered signature stable for generated-time-only snapshot changes', () => {
    const job = makeJob({
      summary: 'Recorded outcome',
      tasks: [
        { id: 'task-a', title: 'Design the plan', status: 'completed', depends_on: [] },
      ],
    });
    const snapshot = makeSnapshot({ jobs: [job] });
    const signature = jobPlanRenderSignature(snapshot);

    const generatedLater = makeSnapshot({
      generated_at_ms: NOW_MS + 90_000,
      jobs: [job],
    });
    assert.equal(jobPlanRenderSignature(generatedLater), signature);
    assert.equal(
      jobPlanGroupsSignature(generatedLater),
      jobPlanGroupsSignature(snapshot)
    );
    assert.notEqual(
      jobPlanRenderSignature(snapshot, { error: 'IPC unavailable' }),
      signature
    );
  });

  it('changes the rendered signature for notice-only warnings while keeping job groups stable', () => {
    const job = makeJob({
      summary: 'Recorded outcome',
      tasks: [
        { id: 'task-a', title: 'Design the plan', status: 'completed', depends_on: [] },
      ],
    });
    const snapshot = makeSnapshot({ jobs: [job] });
    const warned = makeSnapshot({
      jobs: [job],
      warnings: ['Partial observation page.'],
    });

    assert.deepEqual(describeJobPlansState(snapshot).notices, []);
    assert.deepEqual(describeJobPlansState(warned).notices, [
      'Partial observation page.',
    ]);
    assert.notEqual(
      jobPlanRenderSignature(warned),
      jobPlanRenderSignature(snapshot)
    );
    assert.equal(
      jobPlanGroupsSignature(warned),
      jobPlanGroupsSignature(snapshot),
      'a warning-only change must not invalidate existing project/job rows'
    );
  });

  it('changes the rendered signature for recorded task, dependency, and review values', () => {
    const job = makeJob({
      approval_status: 'approved',
      execution_status: 'running',
      review_state: 'pending',
      tasks: [
        { id: 'task-a', title: 'Design the plan', status: 'completed', depends_on: [] },
        {
          id: 'task-b',
          title: 'Build the change',
          status: 'running',
          depends_on: ['task-a'],
        },
      ],
    });
    const signature = jobPlanRowSignature(job, 'Alpha source');

    assert.equal(
      jobPlanRowSignature({ ...job }, 'Alpha source'),
      signature,
      'unchanged recorded values must keep the same job signature'
    );
    assert.notEqual(
      jobPlanRowSignature(
        {
          ...job,
          tasks: [
            job.tasks[0]!,
            { ...job.tasks[1]!, status: 'completed' },
          ],
        },
        'Alpha source'
      ),
      signature
    );
    assert.notEqual(
      jobPlanRowSignature(
        {
          ...job,
          tasks: [
            job.tasks[0]!,
            { ...job.tasks[1]!, depends_on: [] },
          ],
        },
        'Alpha source'
      ),
      signature
    );
    assert.notEqual(
      jobPlanRowSignature(
        {
          ...job,
          tasks: [
            { ...job.tasks[0]!, title: 'Design the revised plan' },
            job.tasks[1]!,
          ],
        },
        'Alpha source'
      ),
      signature
    );
    assert.notEqual(
      jobPlanRowSignature({ ...job, review_state: 'passed' }, 'Alpha source'),
      signature
    );
    assert.notEqual(
      jobPlanRowSignature({ ...job, approval_status: 'pending' }, 'Alpha source'),
      signature
    );
    assert.notEqual(
      jobPlanRowSignature({ ...job, execution_status: 'completed' }, 'Alpha source'),
      signature
    );
  });
});
