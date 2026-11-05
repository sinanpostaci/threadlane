import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import type {
  ObservationActor,
  ObservationCall,
  ObservationDelivery,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationUsage,
} from '../observation/types.ts';
import {
  buildActivityRequestView,
  countRunningRequests,
  describeDeliveryStage,
  formatActivityScopeLabel,
  formatChildCoverage,
  formatCompactDuration,
  formatCompactUsage,
  formatDeliveryStageLabel,
  formatDurationDetail,
  formatDurationSummary,
  formatExecutorIdentity,
  formatExecutorRoleLabel,
  formatFeedTimestamp,
  formatFriendlyTool,
  formatLifecycleLabel,
  formatLatestProvenReturnStage,
  formatOptionalField,
  formatRecordedOperation,
  formatRelativeTime,
  formatRunningCountLabel,
  formatRequesterEvidence,
  formatRequesterIdentity,
  formatRequesterSelectedModel,
  formatRequesterSummary,
  isLocalMcpExecutor,
  projectActivityFeed,
  projectDuration,
  projectExecutorDisplay,
  projectRecordedModel,
  projectRequestExecutorDisplay,
  projectRequesterLine,
  projectTimelineDisplay,
  projectVisibleTimeline,
  projectUsage,
  requestMeaningfulTimestampMs,
  resolveLifecycleTone,
  shouldShowSpinner,
  type DeliveryStage,
  type ObservationLifecycleState,
} from './activity-feed-model.ts';

const NOW_MS = 1_730_000_000_000;

function makeSource(overrides: Partial<ObservationSource> = {}): ObservationSource {
  return {
    id: 'source-a',
    label: 'Source A',
    project_path: '/tmp/source-a',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeActor(overrides: Partial<ObservationActor> = {}): ObservationActor {
  return {
    label: 'Operator',
    role: 'operator',
    source: 'CLI',
    model: null,
    effort: null,
    evidence: 'operator',
    ...overrides,
  };
}

function makeExecutor(overrides: Partial<ObservationExecutor> = {}): ObservationExecutor {
  return {
    tool: 'codex',
    provider: 'openai',
    role: 'coder',
    requested_model: 'gpt-5',
    observed_model: 'gpt-5',
    requested_effort: 'high',
    observed_effort: 'high',
    ...overrides,
  };
}

function makeUsage(overrides: Partial<ObservationUsage> = {}): ObservationUsage {
  return {
    input_tokens: 100,
    output_tokens: 20,
    cache_read_tokens: 5,
    total_tokens: 125,
    provenance: 'provider',
    ...overrides,
  };
}

function makeDelivery(overrides: Partial<ObservationDelivery> = {}): ObservationDelivery {
  return {
    stage: 'response_returned',
    at_ms: NOW_MS,
    summary: 'Response returned to the origin.',
    recipient: 'operator',
    ...overrides,
  };
}

function makeCall(overrides: Partial<ObservationCall> = {}): ObservationCall {
  return {
    id: 'call-1',
    title: 'Child call',
    state: 'completed',
    executor: makeExecutor(),
    started_at_ms: NOW_MS - 1_000,
    finished_at_ms: NOW_MS,
    duration_ms: 1_000,
    usage: makeUsage(),
    summary: 'Child call summary',
    ...overrides,
  };
}

type DeliveryInput =
  | ObservationDelivery
  | readonly ObservationDelivery[]
  | null;
type RequestOverrides = Omit<Partial<ObservationRequest>, 'delivery'> & {
  delivery?: DeliveryInput;
};

function makeRequest(overrides: RequestOverrides = {}): ObservationRequest {
  const { delivery = [makeDelivery()], ...rest } = overrides;
  return {
    source_id: 'source-a',
    id: 'request-1',
    job_id: 'job-1',
    parent_request_id: '',
    title: 'Recorded request',
    operation: 'execute',
    project_label: 'Project A',
    binding_id: 'binding-a',
    thread_uuid: 'thread-a',
    thread_label: 'Thread A',
    state: 'completed',
    started_at_ms: NOW_MS - 2_000,
    finished_at_ms: NOW_MS,
    updated_at_ms: NOW_MS,
    duration_ms: 2_000,
    actor: makeActor(),
    executor: makeExecutor(),
    usage: makeUsage(),
    delivery: delivery as unknown as ObservationRequest['delivery'],
    children: [],
    children_count: 0,
    summary: 'Recorded request summary',
    ...rest,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'source-a',
    id: 'job-1',
    project_label: 'Project A',
    project_path: '/tmp/project-a',
    title: 'Job One',
    thread_uuid: 'thread-a',
    thread_label: 'Thread A',
    binding_id: 'binding-a',
    plan_id: 'plan-a',
    plan_revision: 1,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: 'approved',
    updated_at_ms: NOW_MS,
    active_call_count: 0,
    tasks: [],
    summary: 'Job summary',
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<ObservationSnapshot> = {}): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: NOW_MS,
    sources: [makeSource()],
    jobs: [makeJob()],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    ...overrides,
  };
}

describe('activity feed request projection', () => {
  it('keeps exact source+job scope and same request IDs across sources distinct', () => {
    const snapshot = makeSnapshot({
      sources: [
        makeSource({ id: 'source-a', label: 'Alpha source' }),
        makeSource({ id: 'source-b', label: 'Beta source' }),
      ],
      jobs: [
        makeJob({
          source_id: 'source-a',
          id: 'job-shared',
          project_label: 'Alpha project',
          title: 'Alpha job',
        }),
        makeJob({
          source_id: 'source-b',
          id: 'job-shared',
          project_label: 'Beta project',
          title: 'Beta job',
        }),
      ],
      requests: [
        makeRequest({
          source_id: 'source-a',
          id: 'request-shared',
          job_id: 'job-shared',
          title: 'Alpha request',
        }),
        makeRequest({
          source_id: 'source-b',
          id: 'request-shared',
          job_id: 'job-shared',
          title: 'Beta request',
        }),
      ],
    });

    const unscoped = projectActivityFeed(snapshot);
    assert.equal(unscoped.rows.length, 2);
    assert.deepEqual(
      new Set(unscoped.rows.map((row) => row.key)),
      new Set([
        'source-a\u0000request-shared',
        'source-b\u0000request-shared',
      ])
    );
    assert.match(unscoped.scope.summary, /all recorded activity/i);
    assert.equal(
      formatActivityScopeLabel(unscoped.scope),
      'All recorded activity · Available sources'
    );

    const scoped = projectActivityFeed(snapshot, {
      scope: { source_id: 'source-a', job_id: 'job-shared' },
    });
    assert.equal(scoped.rows.length, 1);
    assert.equal(scoped.rows[0]?.request.source_id, 'source-a');
    assert.equal(scoped.rows[0]?.request.id, 'request-shared');
    assert.equal(scoped.rows[0]?.jobLabel, 'Alpha job');
    assert.match(scoped.scope.summary, /Alpha project/);
    assert.match(scoped.scope.summary, /Alpha job/);
    assert.doesNotMatch(scoped.scope.summary, /Beta/);
    assert.equal(scoped.scope.ids, 'Source source-a · Job job-shared');
    assert.equal(
      formatActivityScopeLabel(scoped.scope),
      'Alpha project · Alpha job'
    );

    const otherJobScope = projectActivityFeed(snapshot, {
      scope: { source_id: 'source-a', job_id: 'job-other' },
    });
    assert.equal(otherJobScope.rows.length, 0);
    assert.equal(otherJobScope.scope.jobUnavailable, true);
    assert.equal(
      formatActivityScopeLabel(otherJobScope.scope),
      'Job details unavailable'
    );
  });

  it('orders by the newest meaningful request, delivery, or child-call timestamp', () => {
    const snapshot = makeSnapshot({
      requests: [
        makeRequest({
          id: 'updated-old',
          updated_at_ms: NOW_MS - 5_000,
          started_at_ms: NOW_MS - 6_000,
          finished_at_ms: NOW_MS - 5_000,
          delivery: [makeDelivery({ at_ms: NOW_MS - 20_000 })],
        }),
        makeRequest({
          id: 'child-new',
          updated_at_ms: NOW_MS - 8_000,
          started_at_ms: NOW_MS - 9_000,
          finished_at_ms: NOW_MS - 8_000,
          delivery: [makeDelivery({ at_ms: NOW_MS - 20_000 })],
          children: [
            makeCall({
              started_at_ms: NOW_MS - 3_000,
              finished_at_ms: NOW_MS - 2_000,
            }),
          ],
          children_count: 1,
        }),
        makeRequest({
          id: 'delivery-new',
          updated_at_ms: NOW_MS - 9_000,
          started_at_ms: NOW_MS - 10_000,
          finished_at_ms: NOW_MS - 9_000,
          delivery: [
            makeDelivery({ stage: 'batch_delivered', at_ms: NOW_MS - 1_000 }),
            makeDelivery({
              stage: 'notification_submitted',
              at_ms: NOW_MS - 20_000,
            }),
          ],
        }),
      ],
    });

    const projection = projectActivityFeed(snapshot);
    assert.deepEqual(
      projection.rows.map((row) => row.request.id),
      ['delivery-new', 'child-new', 'updated-old']
    );
    assert.equal(
      requestMeaningfulTimestampMs(snapshot.requests[0]!),
      NOW_MS - 5_000
    );
    assert.equal(
      requestMeaningfulTimestampMs(snapshot.requests[1]!),
      NOW_MS - 2_000
    );
    assert.equal(
      requestMeaningfulTimestampMs(snapshot.requests[2]!),
      NOW_MS - 1_000
    );
  });

  it('breaks equal timestamps with a deterministic source+id tiebreak and deduplicates exact identity', () => {
    const tiedTimestamp = NOW_MS - 1_000;
    const snapshot = makeSnapshot({
      sources: [
        makeSource({ id: 'source-b', label: 'Beta' }),
        makeSource({ id: 'source-a', label: 'Alpha' }),
      ],
      jobs: [],
      requests: [
        makeRequest({
          source_id: 'source-b',
          id: 'a',
          job_id: '',
          updated_at_ms: tiedTimestamp,
          started_at_ms: tiedTimestamp - 1_000,
          finished_at_ms: tiedTimestamp,
          delivery: [makeDelivery({ at_ms: tiedTimestamp - 10_000 })],
        }),
        makeRequest({
          source_id: 'source-a',
          id: 'z',
          job_id: '',
          updated_at_ms: tiedTimestamp,
          started_at_ms: tiedTimestamp - 1_000,
          finished_at_ms: tiedTimestamp,
          delivery: [makeDelivery({ at_ms: tiedTimestamp - 10_000 })],
        }),
        makeRequest({
          source_id: 'source-a',
          id: 'a',
          job_id: '',
          updated_at_ms: tiedTimestamp,
          started_at_ms: tiedTimestamp - 1_000,
          finished_at_ms: tiedTimestamp,
          delivery: [makeDelivery({ at_ms: tiedTimestamp - 10_000 })],
        }),
      ],
    });

    const first = projectActivityFeed(snapshot);
    const second = projectActivityFeed(snapshot);
    const expected = ['source-a\u0000a', 'source-a\u0000z', 'source-b\u0000a'];
    assert.deepEqual(first.rows.map((row) => row.key), expected);
    assert.deepEqual(second.rows.map((row) => row.key), expected);

    const duplicateSnapshot = makeSnapshot({
      requests: [
        makeRequest({
          id: 'same',
          updated_at_ms: NOW_MS - 500,
          started_at_ms: NOW_MS - 1_500,
          finished_at_ms: NOW_MS - 500,
          delivery: [makeDelivery({ at_ms: NOW_MS - 20_000 })],
        }),
        makeRequest({
          id: 'same',
          updated_at_ms: NOW_MS,
          started_at_ms: NOW_MS - 1_000,
          finished_at_ms: NOW_MS,
          delivery: [makeDelivery({ at_ms: NOW_MS - 20_000 })],
        }),
      ],
    });
    const duplicateProjection = projectActivityFeed(duplicateSnapshot);
    assert.equal(duplicateProjection.rows.length, 1);
    assert.equal(duplicateProjection.rows[0]?.request.updated_at_ms, NOW_MS);
  });

  it('keeps every lifecycle state distinct and spins only while running', () => {
    const states: ObservationLifecycleState[] = [
      'queued',
      'running',
      'waiting_for_user',
      'completed',
      'ready_for_review',
      'failed',
      'cancelled',
      'unknown',
    ];
    const labels = new Set<string>();
    for (const state of states) {
      const label = formatLifecycleLabel(state);
      labels.add(label);
      assert.equal(
        shouldShowSpinner(state),
        state === 'running',
        `spinner decision for ${state}`
      );
      assert.ok(resolveLifecycleTone(state).length > 0);
    }
    assert.equal(labels.size, states.length);
    assert.equal(formatLifecycleLabel('unknown'), 'Unknown');
    assert.notEqual(formatLifecycleLabel('unknown'), 'Completed');
    assert.notEqual(formatLifecycleLabel('unknown'), 'Cancelled');
  });

  it('keeps a completed root completed while a verified child is running', () => {
    const snapshot = makeSnapshot();
    const request = makeRequest({
      state: 'completed',
      started_at_ms: NOW_MS - 5_000,
      finished_at_ms: NOW_MS - 1_000,
      updated_at_ms: NOW_MS - 1_000,
      duration_ms: 4_000,
      delivery: [makeDelivery({ at_ms: NOW_MS - 1_000 })],
      children: [
        makeCall({
          id: 'running-child',
          state: 'running',
          finished_at_ms: null,
          duration_ms: null,
        }),
        makeCall({ id: 'done-child', state: 'completed' }),
      ],
      children_count: 2,
    });

    const view = buildActivityRequestView(snapshot, request, NOW_MS);
    assert.equal(view.state, 'completed');
    assert.equal(view.callsRunning, true);
    assert.equal(view.runningChildCount, 1);
    assert.equal(view.stateLabel, 'Completed');
    assert.equal(view.stateTone, 'ok');
    assert.equal(view.showSpinner, false);
    assert.equal(view.startedAtValue, formatFeedTimestamp(request.started_at_ms));
    assert.equal(view.finishedAtValue, formatFeedTimestamp(request.finished_at_ms));
    assert.equal(view.duration.kind, 'recorded');
    assert.equal(view.durationSummary, 'Duration 4s');
    assert.match(view.durationDetail, /Recorded duration/);
    assert.equal(view.deliveryAtValue, formatFeedTimestamp(NOW_MS - 1_000));

    const settledChild = makeCall({
      id: 'running-child',
      state: 'completed',
      finished_at_ms: NOW_MS,
      duration_ms: 1_000,
    });
    const settledView = buildActivityRequestView(
      snapshot,
      makeRequest({
        state: 'completed',
        children: [settledChild],
        children_count: 1,
      })
    );
    assert.equal(settledView.state, 'completed');
    assert.equal(settledView.callsRunning, false);
    assert.equal(settledView.runningChildCount, 0);
    assert.equal(settledView.stateLabel, 'Completed');
    assert.equal(settledView.stateTone, 'ok');
    assert.equal(settledView.showSpinner, false);
  });

  it('does not infer a root lifecycle or spinner from child state or configured identity', () => {
    const nonRunningStates: ObservationLifecycleState[] = [
      'queued',
      'waiting_for_user',
      'completed',
      'ready_for_review',
      'failed',
      'cancelled',
      'unknown',
    ];
    for (const state of nonRunningStates) {
      const view = buildActivityRequestView(
        makeSnapshot(),
        makeRequest({
          state: 'completed',
          executor: makeExecutor({
            role: 'coder',
            requested_model: 'configured-model',
            observed_model: 'configured-model',
          }),
          children: [makeCall({ id: `child-${state}`, state })],
          children_count: 1,
        })
      );
      assert.equal(view.state, 'completed', `root state with ${state} child`);
      assert.equal(view.callsRunning, false, `callsRunning with ${state} child`);
      assert.equal(view.stateLabel, 'Completed', `label with ${state} child`);
      assert.equal(view.stateTone, 'ok', `tone with ${state} child`);
      assert.equal(view.showSpinner, false, `spinner with ${state} child`);
    }

    const runningRoot = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        state: 'running',
        finished_at_ms: null,
        children: [makeCall({ id: 'running-child', state: 'running' })],
        children_count: 1,
      })
    );
    assert.equal(runningRoot.state, 'running');
    assert.equal(runningRoot.callsRunning, false);
    assert.equal(runningRoot.stateLabel, 'Running');
    assert.equal(runningRoot.stateTone, 'active');
    assert.equal(runningRoot.showSpinner, true);
  });

  it('counts only projected running roots and verified calls-running rows', () => {
    const snapshot = makeSnapshot({
      requests: [
        makeRequest({
          id: 'running-root',
          state: 'running',
          finished_at_ms: null,
          updated_at_ms: NOW_MS,
        }),
        makeRequest({
          id: 'calls-running',
          state: 'completed',
          children: [makeCall({ id: 'running-child', state: 'running' })],
          children_count: 1,
        }),
        makeRequest({ id: 'finished-root', state: 'completed' }),
      ],
    });
    const projection = projectActivityFeed(snapshot);

    assert.equal(projection.rows.length, 3);
    assert.equal(countRunningRequests(projection.rows), 2);
    assert.equal(countRunningRequests([]), 0);
    assert.equal(formatRunningCountLabel(2), '2 calls running');
    assert.equal(formatRunningCountLabel(1), '1 call running');
    assert.equal(formatRunningCountLabel(0), '');
  });

  it('reports unknown requester identity separately from a known requester source', () => {
    const evidenceCases: Array<[ObservationActor['evidence'], string]> = [
      ['configured', 'Configured'],
      ['observed', 'Observed'],
      ['operator', 'Operator supplied'],
      ['unknown', 'Unknown'],
    ];
    const evidenceLabels = new Set<string>();

    for (const [evidence, expected] of evidenceCases) {
      const actor = makeActor({
        label: null,
        source: 'CLI',
        evidence,
      });
      const label = formatRequesterEvidence(evidence);
      evidenceLabels.add(label);
      assert.equal(label, expected);
      assert.equal(formatRequesterIdentity(actor), 'Unknown');
      assert.match(formatRequesterSummary(actor), /Requester: Unknown/);
      assert.equal(formatOptionalField(actor.source), 'CLI');
    }

    assert.equal(evidenceLabels.size, evidenceCases.length);
  });

  it('keeps requested and observed model/effort separate and never substitutes one for the other', () => {
    const snapshot = makeSnapshot();
    const view = buildActivityRequestView(
      snapshot,
      makeRequest({
        executor: makeExecutor({
          requested_model: 'requested-model',
          observed_model: 'observed-model',
          requested_effort: 'low',
          observed_effort: 'max',
        }),
      })
    );

    assert.equal(view.requestedModel, 'requested-model');
    assert.equal(view.observedModel, 'observed-model');
    assert.equal(view.requestedEffort, 'low');
    assert.equal(view.observedEffort, 'max');
    assert.match(view.modelSummary, /requested requested-model/);
    assert.match(view.modelSummary, /observed observed-model/);
    assert.match(view.effortSummary, /requested low/);
    assert.match(view.effortSummary, /observed max/);

    const requestedOnly = buildActivityRequestView(
      snapshot,
      makeRequest({
        executor: makeExecutor({
          requested_model: 'requested-only',
          observed_model: null,
          requested_effort: 'medium',
          observed_effort: null,
        }),
      })
    );
    assert.equal(requestedOnly.requestedModel, 'requested-only');
    assert.equal(requestedOnly.observedModel, 'Not reported');
    assert.equal(requestedOnly.requestedEffort, 'medium');
    assert.equal(requestedOnly.observedEffort, 'Not reported');

    const absentExecutor = buildActivityRequestView(
      snapshot,
      makeRequest({
        executor: makeExecutor({
          tool: null,
          provider: null,
          role: null,
          requested_model: null,
          observed_model: null,
          requested_effort: null,
          observed_effort: null,
        }),
      })
    );
    assert.equal(formatExecutorIdentity(null), 'Executor not recorded');
    assert.equal(absentExecutor.executorIdentity, 'Executor not recorded');
  });

  it('shows only non-null persisted usage and never invents zero usage', () => {
    const unreported = projectUsage(null);
    assert.equal(unreported.reported, false);
    assert.equal(unreported.summary, 'Usage not reported');
    assert.doesNotMatch(unreported.summary, /0/);

    const allNull = projectUsage(
      makeUsage({
        input_tokens: null,
        output_tokens: null,
        cache_read_tokens: null,
        total_tokens: null,
      })
    );
    assert.equal(allNull.reported, false);
    assert.equal(allNull.summary, 'Usage not reported');

    const partial = projectUsage(
      makeUsage({
        input_tokens: null,
        output_tokens: 0,
        cache_read_tokens: null,
        total_tokens: null,
      })
    );
    assert.equal(partial.reported, true);
    assert.match(partial.summary, /out 0/);
    assert.doesNotMatch(partial.summary, /in /);
    assert.doesNotMatch(partial.summary, /cache read/);
    assert.doesNotMatch(partial.summary, /total/);
  });

  it('keeps every delivery stage distinct and does not treat an acknowledgement as a read receipt', () => {
    const stages: DeliveryStage[] = [
      'result_ready',
      'notification_submitted',
      'batch_delivered',
      'batch_acknowledged',
      'response_returned',
      'unknown',
      'failed',
    ];
    const labels = new Set<string>();
    for (const stage of stages) {
      const description = describeDeliveryStage(stage);
      labels.add(description.label);
      assert.equal(formatDeliveryStageLabel(stage), description.label);
      assert.ok(description.note.length > 0);
    }
    assert.equal(labels.size, stages.length);
    assert.match(
      describeDeliveryStage('batch_acknowledged').note,
      /not proof/i
    );
    assert.doesNotMatch(
      describeDeliveryStage('batch_acknowledged').note,
      /response returned/i
    );
    assert.match(
      formatLatestProvenReturnStage([
        makeDelivery({ stage: 'batch_acknowledged' }),
      ]),
      /No response return recorded/
    );
    assert.equal(
      formatLatestProvenReturnStage([
        makeDelivery({ stage: 'response_returned' }),
      ]),
      'Latest proven return stage: Response returned.'
    );
    assert.equal(formatLatestProvenReturnStage([]), 'No return stage recorded.');
    const afterDispatchResponse = formatLatestProvenReturnStage([
      makeDelivery({ stage: 'response_returned', at_ms: NOW_MS - 5_000 }),
      makeDelivery({ stage: 'batch_acknowledged', at_ms: NOW_MS - 1_000 }),
    ]);
    assert.match(afterDispatchResponse, /Latest recorded stage: Batch acknowledged/);
    assert.match(afterDispatchResponse, /does not prove final result delivery/);
    const failedLater = formatLatestProvenReturnStage([
      makeDelivery({ stage: 'response_returned', at_ms: NOW_MS - 5_000 }),
      makeDelivery({ stage: 'failed', at_ms: NOW_MS }),
    ]);
    assert.match(failedLater, /Latest recorded stage: Delivery failed/);
  });

  it('projects every delivery record and uses the newest delivery timestamp', () => {
    const delivery = [
      makeDelivery({
        stage: 'batch_acknowledged',
        at_ms: NOW_MS - 2_000,
        recipient: 'reviewer',
        summary: 'Batch acknowledged.',
      }),
      makeDelivery({
        stage: 'notification_submitted',
        at_ms: NOW_MS - 4_000,
        recipient: 'operator',
        summary: 'Notification submitted.',
      }),
      makeDelivery({
        stage: 'batch_delivered',
        at_ms: NOW_MS - 3_000,
        recipient: 'operator',
        summary: 'Batch delivered.',
      }),
    ];
    const request = makeRequest({
      updated_at_ms: NOW_MS - 10_000,
      started_at_ms: NOW_MS - 11_000,
      finished_at_ms: NOW_MS - 10_000,
      delivery,
    });
    const view = buildActivityRequestView(makeSnapshot(), request);

    assert.deepEqual(
      view.delivery.map((record) => record.stage),
      ['notification_submitted', 'batch_delivered', 'batch_acknowledged']
    );
    assert.deepEqual(
      view.deliveries.map((record) => record.stage),
      ['notification_submitted', 'batch_delivered', 'batch_acknowledged']
    );
    assert.deepEqual(
      view.deliveries.map((record) => record.label),
      ['Notification submitted', 'Batch delivered', 'Batch acknowledged']
    );
    assert.equal(view.deliveryAtValue, formatFeedTimestamp(NOW_MS - 2_000));
    assert.equal(view.deliveryRecipient, 'operator · reviewer');
    assert.match(view.deliveryLabel, /3 delivery records/);
    assert.match(view.deliverySummary, /Notification submitted/);
    assert.match(view.deliverySummary, /Batch delivered/);
    assert.match(view.deliverySummary, /Batch acknowledged/);
    assert.match(view.latestReturn, /No response return recorded/);
    assert.equal(requestMeaningfulTimestampMs(request), NOW_MS - 2_000);
  });

  it('handles an empty delivery array without inventing a delivery state', () => {
    const request = makeRequest({ delivery: [] });
    const view = buildActivityRequestView(makeSnapshot(), request);

    assert.deepEqual(view.delivery, []);
    assert.deepEqual(view.deliveries, []);
    assert.equal(view.deliveryLabel, 'Delivery not reported');
    assert.equal(view.deliveryAtValue, 'Not reported');
    assert.equal(view.deliveryRecipient, 'Not reported');
    assert.equal(view.deliverySummary, 'Delivery summary not reported.');
    assert.equal(view.latestReturn, 'No return stage recorded.');
    assert.equal(requestMeaningfulTimestampMs(request), NOW_MS);
  });

  it('returns all child calls and states when additional reported calls are not included', () => {
    const children = [
      makeCall({
        id: 'jev-call',
        title: 'Jev call',
        executor: makeExecutor({ tool: 'jev', provider: null }),
      }),
      makeCall({
        id: 'api-call',
        title: 'API call',
        executor: makeExecutor({ tool: 'api', provider: null }),
      }),
      makeCall({
        id: 'reviewer-call',
        title: 'Reviewer call',
        executor: makeExecutor({ tool: 'reviewer', provider: null }),
      }),
      makeCall({
        id: 'parallel-call',
        title: 'Parallel call',
        executor: makeExecutor({ tool: 'parallel', provider: null }),
      }),
    ];
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({ children, children_count: 7 })
    );

    assert.deepEqual(
      view.children.map((child) => child.call.id),
      ['jev-call', 'api-call', 'reviewer-call', 'parallel-call']
    );
    assert.equal(view.childrenReturned, 4);
    assert.equal(view.childrenCount, 7);
    assert.match(view.childCoverage, /Additional calls are not included/);
    assert.match(view.childCoverage, /4 of 7/);

    assert.equal(formatChildCoverage(2, 2), '2 child calls returned.');
    assert.equal(formatChildCoverage(1, 1), '1 child call returned.');
    assert.equal(formatChildCoverage(0, 0), 'No child calls returned.');
  });

  it('uses recorded duration, derives only from known timestamps, and labels running elapsed time', () => {
    const snapshot = makeSnapshot();

    const recorded = buildActivityRequestView(
      snapshot,
      makeRequest({ duration_ms: 0 })
    );
    assert.equal(recorded.duration.kind, 'recorded');
    assert.equal(recorded.durationSummary, 'Duration 0ms');
    assert.doesNotMatch(recorded.durationSummary, /not reported/i);

    const derived = buildActivityRequestView(
      snapshot,
      makeRequest({
        state: 'completed',
        duration_ms: null,
        started_at_ms: NOW_MS - 5_000,
        finished_at_ms: NOW_MS - 1_000,
      })
    );
    assert.equal(derived.duration.kind, 'derived');
    assert.equal(derived.durationSummary, 'Duration 4s');
    assert.match(derived.durationDetail, /Derived from recorded start and end timestamps/);

    const runningWithRecordedDuration = buildActivityRequestView(
      snapshot,
      makeRequest({
        state: 'running',
        duration_ms: 5_000,
        started_at_ms: NOW_MS - 60_000,
        finished_at_ms: null,
      }),
      NOW_MS
    );
    assert.equal(runningWithRecordedDuration.duration.kind, 'recorded');
    assert.equal(runningWithRecordedDuration.durationSummary, 'Duration 5s');

    const elapsed = buildActivityRequestView(
      snapshot,
      makeRequest({
        state: 'running',
        duration_ms: null,
        started_at_ms: NOW_MS - 60_000,
        finished_at_ms: null,
      }),
      NOW_MS
    );
    assert.equal(elapsed.duration.kind, 'elapsed');
    assert.match(elapsed.durationSummary, /^Elapsed /);
    assert.match(elapsed.durationDetail, /recorded start timestamp/);

    const unknown = projectDuration({
      state: 'completed',
      started_at_ms: null,
      finished_at_ms: null,
      duration_ms: null,
    });
    assert.equal(unknown.kind, 'unknown');
    assert.equal(formatDurationSummary(unknown), 'Duration not reported');
    assert.equal(formatDurationDetail(unknown), 'Duration not reported.');

    const invalidRange = projectDuration({
      state: 'completed',
      started_at_ms: NOW_MS,
      finished_at_ms: NOW_MS - 1_000,
      duration_ms: null,
    });
    assert.equal(invalidRange.kind, 'unknown');
  });

  it('resolves job labels only from the matching source_id+job_id and never guesses from a title', () => {
    const snapshot = makeSnapshot({
      jobs: [
        makeJob({
          source_id: 'source-a',
          id: 'job-1',
          title: 'Source A job',
        }),
        makeJob({
          source_id: 'source-b',
          id: 'job-2',
          title: 'Source B job',
        }),
      ],
    });

    const wrongSource = buildActivityRequestView(
      snapshot,
      makeRequest({
        source_id: 'source-b',
        id: 'request-wrong-source',
        job_id: 'job-1',
        title: 'A title that looks like a job',
      })
    );
    assert.equal(wrongSource.jobLabel, 'Job details unavailable');
    assert.equal(wrongSource.jobUnavailable, true);

    const unlinked = buildActivityRequestView(
      snapshot,
      makeRequest({
        id: 'request-unlinked',
        job_id: '',
        title: 'A title that looks like a job',
      })
    );
    assert.equal(unlinked.jobLabel, 'No linked job');
    assert.equal(unlinked.jobLinked, false);

    const missing = buildActivityRequestView(
      snapshot,
      makeRequest({
        id: 'request-missing',
        job_id: 'job-missing',
        title: 'A title that looks like a job',
      })
    );
    assert.equal(missing.jobLabel, 'Job details unavailable');
    assert.equal(missing.jobUnavailable, true);
    assert.doesNotMatch(missing.sourceDetails, /private/);
    assert.doesNotMatch(missing.projectLabel, /private/);
  });

  it('surfaces unavailable sources, warnings, pagination, and job caps without inventing rows', () => {
    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'source-a',
          availability: 'unavailable',
          message: 'Observer offline',
        }),
      ],
      requests: [],
      warnings: ['Partial snapshot', 'Another warning'],
      has_more: true,
      jobs_has_more: true,
      next_cursor: 'cursor-1',
    });

    const projection = projectActivityFeed(snapshot);
    assert.equal(projection.rows.length, 0);
    assert.equal(projection.unavailableSources.length, 1);
    assert.equal(projection.unavailableSources[0]?.label, 'Source A');
    assert.equal(projection.unavailableSources[0]?.message, 'Observer offline');
    assert.deepEqual(projection.warnings, ['Partial snapshot', 'Another warning']);
    assert.equal(projection.hasMore, true);
    assert.equal(projection.jobsHasMore, true);
    assert.equal(projection.nextCursor, 'cursor-1');
    assert.equal(projection.filteredOutCount, 0);
  });

  it('bounds long summaries without fabricating content', () => {
    const longSummary = `${'bounded '.repeat(200)}end`;
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({ summary: longSummary })
    );
    assert.ok(view.summary.length <= 640);
    assert.match(view.summary, /…$/);
    assert.doesNotMatch(view.summary, /end$/);
  });

  it('surfaces one recorded child executor when the root executor is absent', () => {
    const request = makeRequest({
      executor: null,
      children: [
        makeCall({
          id: 'worker-call',
          title: 'Worker call',
          executor: makeExecutor({
            tool: 'opencode',
            provider: 'opencode',
            role: 'worker',
            requested_model: 'deepseek-v4.1-flash',
            observed_model: 'deepseek-v4.1-flash-2026',
            requested_effort: 'max',
            observed_effort: 'high',
          }),
        }),
      ],
      children_count: 1,
    });
    const view = buildActivityRequestView(makeSnapshot(), request, NOW_MS);

    // Root request fields remain root-sourced and unknown.
    assert.equal(view.executorIdentity, 'Executor not recorded');
    assert.equal(view.executorSummary, 'Executor not recorded');
    assert.equal(view.executorTool, 'Not reported');
    assert.equal(view.executorProvider, 'Not reported');
    assert.equal(view.requestedModel, 'Not reported');
    assert.equal(view.observedModel, 'Not reported');
    assert.equal(view.requestedEffort, 'Not reported');
    assert.equal(view.observedEffort, 'Not reported');
    assert.equal(view.requesterLabel, 'Operator');
    assert.equal(view.requesterEvidence, 'Operator supplied');

    const called = view.calledExecutor;
    assert.ok(called);
    assert.equal(called.recordedCount, 1);
    assert.equal(called.summary, 'tool opencode · provider opencode · model requested deepseek-v4.1-flash · observed deepseek-v4.1-flash-2026 · effort requested max · observed high');
    assert.match(called.detail, /Child call worker-call/);
    assert.match(called.detail, /model requested deepseek-v4\.1-flash/);
    assert.match(called.detail, /observed deepseek-v4\.1-flash-2026/);
    assert.match(called.detail, /effort requested max/);
    assert.match(called.detail, /observed high/);
    assert.equal(called.brand, 'opencode');

    // The accordion child details stay exact and unchanged.
    assert.equal(view.children[0]?.requestedModel, 'deepseek-v4.1-flash');
    assert.equal(view.children[0]?.observedModel, 'deepseek-v4.1-flash-2026');
    assert.equal(view.children[0]?.requestedEffort, 'max');
    assert.equal(view.children[0]?.observedEffort, 'high');
  });

  it('counts distinct child executors without choosing a representative model or provider', () => {
    const request = makeRequest({
      executor: null,
      children: [
        makeCall({
          id: 'worker-codex',
          executor: makeExecutor({
            tool: 'codex',
            provider: 'openai',
            requested_model: 'gpt-5',
            observed_model: 'gpt-5',
            requested_effort: 'high',
            observed_effort: 'high',
          }),
        }),
        makeCall({
          id: 'worker-gemini',
          executor: makeExecutor({
            tool: 'gemini',
            provider: 'google',
            requested_model: 'gemini-3',
            observed_model: 'gemini-3',
            requested_effort: 'low',
            observed_effort: 'low',
          }),
        }),
      ],
      children_count: 2,
    });
    const called = buildActivityRequestView(makeSnapshot(), request).calledExecutor;

    assert.ok(called);
    assert.equal(called.toolCount, 2);
    assert.equal(called.providerCount, 2);
    assert.equal(called.modelCount, 2);
    assert.equal(called.effortCount, 2);
    assert.equal(called.summary, '2 tools / 2 models');
    assert.equal(called.brand, 'generic');
    assert.match(called.detail, /2 child executors recorded/);
    assert.doesNotMatch(
      `${called.summary} ${called.detail}`,
      /codex|gemini|openai|google|gpt-5/
    );
  });

  it('marks partial returned-call coverage and avoids a child-derived brand', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: null,
        children: [
          makeCall({
            id: 'returned-worker',
            executor: makeExecutor({
              tool: 'opencode',
              provider: 'opencode',
              requested_model: 'deepseek-v4.1-flash',
              observed_model: 'deepseek-v4.1-flash',
              requested_effort: 'max',
              observed_effort: 'max',
            }),
          }),
        ],
        children_count: 3,
      })
    );
    const called = view.calledExecutor;

    assert.ok(called);
    assert.match(called.summary, /1 of 3 reported child calls returned/);
    assert.match(called.summary, /1 tool \/ 1 model/);
    assert.doesNotMatch(called.summary, /opencode|deepseek/);
    assert.equal(called.brand, 'generic');
    assert.equal(view.children[0]?.call.id, 'returned-worker');
    assert.equal(view.children[0]?.requestedModel, 'deepseek-v4.1-flash');
  });

  it('keeps unknown child executor data unreported without inferred attribution', () => {
    const request = makeRequest({
      executor: null,
      children: [
        makeCall({ id: 'unknown-null', executor: null }),
        makeCall({
          id: 'unknown-empty',
          executor: makeExecutor({
            tool: null,
            provider: null,
            role: null,
            requested_model: null,
            observed_model: null,
            requested_effort: null,
            observed_effort: null,
          }),
        }),
      ],
      children_count: 2,
    });
    const view = buildActivityRequestView(makeSnapshot(), request);

    assert.equal(view.calledExecutor, null);
    assert.equal(view.executorIdentity, 'Executor not recorded');
    assert.equal(view.executorTool, 'Not reported');
    assert.equal(view.executorProvider, 'Not reported');
    assert.equal(view.requestedModel, 'Not reported');
    assert.equal(view.observedModel, 'Not reported');
    assert.equal(view.requestedEffort, 'Not reported');
    assert.equal(view.observedEffort, 'Not reported');
    assert.equal(view.children[0]?.executorSummary, 'Executor not recorded');
    assert.equal(view.children[1]?.executorSummary, 'Executor not recorded');
  });

  it('preserves explicit root executor attribution when child executors differ', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: makeExecutor({
          tool: 'codex',
          provider: 'openai',
          role: 'root',
          requested_model: 'gpt-5',
          observed_model: 'gpt-5-2026',
          requested_effort: 'high',
          observed_effort: 'high',
        }),
        children: [
          makeCall({
            id: 'worker-opencode',
            executor: makeExecutor({
              tool: 'opencode',
              provider: 'opencode',
              requested_model: 'deepseek-v4.1-flash',
              observed_model: 'deepseek-v4.1-flash',
              requested_effort: 'max',
              observed_effort: 'max',
            }),
          }),
        ],
        children_count: 1,
      })
    );

    // An explicit root executor keeps its own compact attribution.
    assert.equal(view.calledExecutor, null);
    assert.equal(view.executorIdentity, 'root · codex · openai');
    assert.equal(view.executorSummary, 'Executor: root · codex · openai');
    assert.equal(view.executorTool, 'codex');
    assert.equal(view.executorProvider, 'openai');
    assert.equal(view.requestedModel, 'gpt-5');
    assert.equal(view.observedModel, 'gpt-5-2026');
    assert.equal(view.requestedEffort, 'high');
    assert.equal(view.observedEffort, 'high');
    assert.doesNotMatch(
      `${view.executorIdentity} ${view.executorSummary}`,
      /opencode|deepseek/
    );
    assert.doesNotMatch(view.modelSummary, /deepseek/);
    assert.doesNotMatch(view.effortSummary, /max/);
  });

  it('projects one compact requester line without repeating unknown identity', () => {
    const unknown = projectRequesterLine(
      makeActor({ label: null, evidence: 'unknown' })
    );
    assert.equal(unknown.value, 'Unknown');
    assert.equal(unknown.evidence, null);
    assert.equal(unknown.evidenceKey, 'unknown');
    assert.doesNotMatch(unknown.detail, /Unknown Unknown/);
    assert.match(unknown.detail, /identity and evidence not reported/i);

    const unknownWithEvidence = projectRequesterLine(
      makeActor({ label: null, evidence: 'observed' })
    );
    assert.equal(unknownWithEvidence.value, 'Unknown');
    assert.equal(unknownWithEvidence.evidence, 'Observed');

    const known = projectRequesterLine(
      makeActor({ label: 'Operator', evidence: 'operator' })
    );
    assert.equal(known.value, 'Operator');
    assert.equal(known.evidence, 'Operator supplied');
    assert.match(known.detail, /Requester Operator · Operator supplied/);
  });

  it('keeps requester evidence in its own timeline event without repeating unknown identity', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        actor: makeActor({
          label: null,
          evidence: 'unknown',
          source: null,
          role: null,
        }),
      })
    );
    const requesterEvent = view.timeline.find(
      (event) => event.kind === 'requester'
    );
    assert.ok(requesterEvent);
    assert.equal(requesterEvent.label, 'Requester');
    assert.equal(requesterEvent.detail, 'Unknown');
    const lines = requesterEvent.lines.join(' ');
    assert.match(lines, /Evidence: not reported/);
    assert.match(lines, /Source: Not reported · role Not reported/);
    assert.match(lines, /Model: Not reported · effort Not reported/);
    assert.doesNotMatch(`${requesterEvent.detail} ${lines}`, /Unknown Unknown/);
    const requestEvent = view.timeline.find(
      (event) => event.kind === 'request'
    );
    assert.ok(requestEvent);
    assert.doesNotMatch(requestEvent.lines.join(' '), /Requester:/);
    assert.equal(requestEvent.note, '');
  });

  it('projects the compact role and recorded model without inventing defaults', () => {
    const equal = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: makeExecutor({
          role: 'worker',
          requested_model: 'gpt-5',
          observed_model: 'gpt-5',
        }),
      })
    );
    assert.equal(equal.executorRoleLabel, 'worker');
    assert.equal(equal.recordedModelValue, 'gpt-5');
    assert.equal(
      equal.recordedModelDetail,
      'Model requested gpt-5 · observed gpt-5.'
    );

    const different = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: makeExecutor({
          role: 'root_worker',
          requested_model: 'gpt-5',
          observed_model: 'gpt-5-2026',
        }),
      })
    );
    assert.equal(different.executorRoleLabel, 'root worker');
    assert.equal(
      different.recordedModelValue,
      'req gpt-5 · obs gpt-5-2026'
    );
    assert.match(different.recordedModelDetail, /requested gpt-5/);
    assert.match(different.recordedModelDetail, /observed gpt-5-2026/);

    assert.equal(
      projectRecordedModel('gpt-5', null).value,
      'requested gpt-5'
    );
    assert.equal(
      projectRecordedModel(null, 'gpt-5-2026').value,
      'observed gpt-5-2026'
    );
    assert.equal(projectRecordedModel(null, null).value, 'Not reported');
    assert.equal(
      projectRecordedModel(null, null).detail,
      'Model requested and observed not reported.'
    );
    assert.equal(formatExecutorRoleLabel(null), 'Role not reported');
    assert.equal(formatExecutorRoleLabel('root_worker'), 'root worker');
    assert.equal(formatRecordedOperation('worker_call'), 'worker call');
  });

  it('projects only recorded timeline events in time order', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        started_at_ms: NOW_MS - 10_000,
        finished_at_ms: NOW_MS,
        updated_at_ms: NOW_MS,
        delivery: [
          makeDelivery({
            stage: 'response_returned',
            at_ms: NOW_MS - 1_000,
            summary: 'Response returned to the origin.',
          }),
          makeDelivery({
            stage: 'notification_submitted',
            at_ms: NOW_MS - 5_000,
            summary: 'Notification submitted.',
          }),
        ],
        children: [
          makeCall({
            id: 'child-a',
            title: 'Child A',
            started_at_ms: NOW_MS - 3_000,
            finished_at_ms: NOW_MS - 2_000,
          }),
        ],
        children_count: 1,
      })
    );

    assert.deepEqual(
      view.timeline.map((event) => event.kind),
      ['requester', 'request', 'delivery', 'child', 'delivery']
    );
    assert.deepEqual(
      view.timeline.map((event) => event.label),
      [
        'Requester',
        'Request recorded',
        'Notification submitted',
        'Child call',
        'Response returned',
      ]
    );

    const requesterEvent = view.timeline[0];
    assert.equal(requesterEvent?.kind, 'requester');
    assert.equal(requesterEvent?.label, 'Requester');
    assert.equal(requesterEvent?.detail, 'Operator');
    const requesterLines = requesterEvent?.lines.join(' ') ?? '';
    assert.match(requesterLines, /Evidence: Operator supplied/);
    assert.match(requesterLines, /Source: CLI · role operator/);
    assert.match(requesterLines, /Model: Not reported · effort Not reported/);

    const requestEvent = view.timeline[1];
    assert.equal(
      requestEvent?.atValue,
      formatFeedTimestamp(NOW_MS - 10_000)
    );
    assert.equal(requestEvent?.stateLabel, 'Completed');
    assert.match(requestEvent?.detail ?? '', /Operation: execute/);
    assert.match(
      requestEvent?.lines.join(' ') ?? '',
      /Source: Source A/
    );
    const requestLines = requestEvent?.lines.join(' ') ?? '';
    assert.match(requestLines, /Title: Recorded request/);
    assert.match(requestLines, /Job: Job One · Job job-1/);
    assert.doesNotMatch(requestLines, /Requester:/);
    assert.match(
      requestLines,
      /Root model: requested gpt-5 · observed gpt-5/
    );
    assert.match(
      requestLines,
      /Root effort: requested high · observed high/
    );
    assert.doesNotMatch(requestLines, /Started |Finished |Updated /);
    assert.equal(requestEvent?.note, '');

    assert.equal(view.timeline[3]?.detail, 'Child A');
    assert.match(view.timeline[3]?.lines.join(' ') ?? '', /Call: child-a/);
    assert.match(view.timeline[3]?.lines.join(' ') ?? '', /requested gpt-5/);
    assert.doesNotMatch(
      view.timeline[3]?.lines.join(' ') ?? '',
      /Started |Finished /
    );
    assert.match(
      view.timeline[4]?.lines.join(' ') ?? '',
      /Response returned to the origin/
    );
  });

  it('does not invent delivery or child events when none were recorded', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({ delivery: [], children: [], children_count: 0 })
    );

    assert.equal(view.timeline.length, 2);
    assert.equal(view.timeline[0]?.kind, 'requester');
    assert.equal(view.timeline[1]?.kind, 'request');
    assert.match(view.childCoverage, /No child calls returned/);
    assert.match(view.latestReturn, /No return stage recorded/);
  });

  it('keeps root attribution unknown while labeling child call evidence exactly', () => {
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: null,
        children: [
          makeCall({
            id: 'worker-call',
            title: 'Worker call',
            executor: makeExecutor({
              tool: 'opencode',
              provider: 'opencode',
              role: 'worker',
              requested_model: 'deepseek-v4.1-flash',
              observed_model: 'deepseek-v4.1-flash-2026',
              requested_effort: 'max',
              observed_effort: 'high',
            }),
          }),
        ],
        children_count: 1,
      })
    );

    assert.equal(view.executorRoleLabel, 'Role not reported');
    assert.equal(view.recordedModelValue, 'Not reported');
    assert.match(view.recordedModelDetail, /not reported/);
    assert.doesNotMatch(view.recordedModelValue, /opencode|deepseek/);

    const childEvent = view.timeline.find((event) => event.kind === 'child');
    assert.ok(childEvent);
    assert.equal(childEvent.label, 'Child call');
    assert.equal(childEvent.detail, 'Worker call');
    const childLines = childEvent.lines.join(' ');
    assert.match(childLines, /Call: worker-call/);
    assert.match(childLines, /requested deepseek-v4\.1-flash/);
    assert.match(childLines, /observed deepseek-v4\.1-flash-2026/);
    assert.match(childLines, /Effort: requested max/);
    assert.match(childLines, /observed high/);

    const requestEvent = view.timeline.find(
      (event) => event.kind === 'request'
    );
    assert.ok(requestEvent);
    const requestLines = requestEvent.lines.join(' ');
    assert.match(requestLines, /Root model: requested Not reported/);
    assert.match(requestLines, /Root effort: requested Not reported/);
    assert.match(requestLines, /Called tools\/models: Child call worker-call/);
    assert.equal(requestEvent.note, '');
  });

  it('formats compact duration and usage without inventing aggregates', () => {
    const recorded = projectDuration({
      state: 'completed',
      started_at_ms: NOW_MS - 2_000,
      finished_at_ms: NOW_MS,
      duration_ms: 2_000,
    });
    assert.equal(
      formatCompactDuration(recorded, 'Duration 2s. Recorded duration.').value,
      '2s'
    );
    const elapsed = projectDuration(
      {
        state: 'running',
        started_at_ms: NOW_MS - 60_000,
        finished_at_ms: null,
        duration_ms: null,
      },
      NOW_MS
    );
    assert.equal(
      formatCompactDuration(elapsed, 'Elapsed 1m.').value,
      elapsed.text
    );
    assert.equal(
      formatCompactDuration(
        projectDuration({
          state: 'completed',
          started_at_ms: null,
          finished_at_ms: null,
          duration_ms: null,
        }),
        'Duration not reported.'
      ).value,
      'Duration not reported'
    );

    assert.equal(
      formatCompactUsage(projectUsage(makeUsage())).value,
      '125 tokens'
    );
    assert.equal(
      formatCompactUsage(
        projectUsage(makeUsage({ total_tokens: 1_200 }))
      ).value,
      '1.2K tokens'
    );
    const partial = formatCompactUsage(
      projectUsage(makeUsage({ total_tokens: null }))
    );
    assert.equal(partial.value, 'Partial usage');
    assert.doesNotMatch(partial.value, /in |out |cache/);
    assert.equal(formatCompactUsage(projectUsage(null)).value, 'Usage not reported');
  });

  it('formats relative recorded times and leaves missing time unknown', () => {
    assert.equal(formatRelativeTime(NOW_MS - 30_000, NOW_MS), 'Just now');
    assert.equal(formatRelativeTime(NOW_MS - 5 * 60_000, NOW_MS), '5m ago');
    assert.equal(formatRelativeTime(NOW_MS - 2 * 3_600_000, NOW_MS), '2h ago');
    assert.equal(
      formatRelativeTime(NOW_MS - 3 * 86_400_000, NOW_MS),
      '3d ago'
    );
    assert.equal(formatRelativeTime(null, NOW_MS), 'Time not reported');
  });

  it('uses one model receipt for compact copy while retaining requested and observed evidence', () => {
    const observed = projectExecutorDisplay(makeExecutor({
      role: 'reviewer',
      requested_model: 'gemini-4-argon-high',
      observed_model: 'gpt-6-sol',
      requested_effort: 'high',
      observed_effort: null,
    }));
    assert.equal(observed.role, 'Reviewer');
    assert.equal(observed.model, 'Sol');
    assert.match(observed.detail, /requested gemini-4-argon-high · observed gpt-6-sol/);
    assert.match(observed.detail, /Effort requested high · observed Not reported/);

    const requested = projectExecutorDisplay(makeExecutor({
      requested_model: 'opencode-go/deepseek-v4.1-flash',
      observed_model: null,
      requested_effort: 'max',
      observed_effort: null,
    }));
    assert.equal(requested.model, 'DeepSeek · Max');
    assert.match(requested.detail, /observed Not reported/);
    assert.equal(projectExecutorDisplay(null).model, 'Model not reported');
  });

  it('identifies called tools without promoting a child executor into the root request', () => {
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({
      executor: null,
      children_count: 1,
      children: [makeCall({ executor: makeExecutor({
        requested_model: 'deepseek-v4.1-flash',
        observed_model: 'deepseek-v4.1-flash',
        observed_effort: 'max',
      }) })],
    }));
    const display = projectRequestExecutorDisplay(view);
    assert.equal(display.role, 'Called Coder');
    assert.equal(display.model, 'DeepSeek · Max');
    assert.equal(view.executorRoleLabel, 'Role not reported');
    assert.equal(view.observedModel, 'Not reported');
    assert.match(display.detail, /Child call/);

    const unknown = buildActivityRequestView(makeSnapshot(), makeRequest({ executor: null }));
    assert.equal(projectRequestExecutorDisplay(unknown).role, 'Executor not recorded');
    assert.equal(projectRequestExecutorDisplay(unknown).model, null);
  });

  it('keeps causal timeline copy quiet while preserving the underlying delivery and identity records', () => {
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({
      actor: makeActor({ label: null, source: 'CLI', role: null }),
      thread_label: 'Recorded conversation',
      delivery: makeDelivery({ stage: 'batch_acknowledged' }),
    }));
    const requester = projectTimelineDisplay(view, view.timeline[0]!);
    assert.equal(requester.label, 'Requester');
    assert.equal(requester.detail, 'Unknown · CLI');
    assert.ok(requester.lines.includes('Conversation: Recorded conversation'));
    assert.ok(requester.lines.includes('Source is known; caller identity is unavailable.'));
    const request = projectTimelineDisplay(view, view.timeline[1]!);
    assert.equal(request.label, 'CLI → Threadlane');
    assert.doesNotMatch(request.detail, /Operation:/);
    const delivery = view.timeline.find((event) => event.kind === 'delivery')!;
    const display = projectTimelineDisplay(view, delivery);
    assert.equal(display.label, 'Batch acknowledged');
    assert.match(display.lines.join(' '), /not proof.*opened or read/);
    assert.match(view.timeline[0]!.lines.join(' '), /Evidence:/);
    assert.match(view.timeline[1]!.lines.join(' '), /Root model:/);
  });

  it('groups actual delivery records without interpreting batch prose or changing raw evidence', () => {
    const records = [
      ...Array.from({ length: 3 }, (_, index) => makeDelivery({
        stage: 'batch_delivered',
        recipient: 'bnd_private',
        summary: `bat_private covering seq ${index}; text claims 999 batches`,
      })),
      ...Array.from({ length: 2 }, (_, index) => makeDelivery({
        stage: 'batch_acknowledged',
        recipient: 'bnd_private',
        summary: `bat_private ACK covering seq ${index}`,
      })),
    ];
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({
      binding_id: 'bnd_private',
      delivery: records,
      children: [makeCall()],
      children_count: 1,
    }));
    const rawBefore = JSON.stringify(view.timeline);
    const visible = projectVisibleTimeline(view);
    assert.deepEqual(visible.map((entry) => entry.event.kind), [
      'requester', 'request', 'child', 'delivery', 'delivery',
    ]);
    const groups = visible.filter((entry) => entry.event.kind === 'delivery');
    assert.deepEqual(groups.map((entry) => [entry.display.label, entry.recordCount]), [
      ['Events delivered', 3], ['Delivery acknowledged', 2],
    ]);
    assert.equal(groups[0]!.display.detail, '3 records · Linked conversation');
    const copy = groups.map((entry) => JSON.stringify(entry.display)).join(' ');
    assert.doesNotMatch(copy, /bat_|bnd_|seq|999|batch count/i);
    assert.equal((copy.match(/human reading or review acceptance/g) ?? []).length, 1);
    assert.equal(JSON.stringify(view.timeline), rawBefore);
    assert.equal(view.timeline.filter((event) => event.kind === 'delivery').length, 5);
    assert.deepEqual(view.deliveries.map((delivery) => delivery.record), records);
    assert.match(rawBefore, /bat_private covering seq 0; text claims 999 batches/);
  });

  it('keeps independent recipients and different transport stages in separate visible groups', () => {
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({ delivery: [
      makeDelivery({ stage: 'batch_delivered', recipient: 'bnd_a' }),
      makeDelivery({ stage: 'batch_delivered', recipient: 'bnd_b' }),
      makeDelivery({ stage: 'batch_delivered', recipient: 'bnd_a' }),
      makeDelivery({ stage: 'batch_acknowledged', recipient: 'bnd_b' }),
      makeDelivery({ stage: 'batch_delivered', recipient: null }),
    ] }));
    const groups = projectVisibleTimeline(view).filter((entry) => entry.event.kind === 'delivery');
    assert.equal(groups.length, 4);
    assert.deepEqual(groups.map((entry) => [entry.display.label, entry.recordCount, entry.display.detail]), [
      ['Events delivered', 1, '1 record · Recipient 2'],
      ['Events delivered', 2, '2 records · Recipient 1'],
      ['Delivery acknowledged', 1, '1 record · Recipient 2'],
      ['Events delivered', 1, '1 record · Recipient not recorded'],
    ]);
    assert.doesNotMatch(groups.map((entry) => entry.display.detail).join(' '), /bnd_/);
  });

  it('shows explicitly called models for metadata-only roots while preserving meaningful root execution', () => {
    const metadataRoot = makeExecutor({
      tool: 'Threadlane', provider: null, role: null,
      requested_model: null, observed_model: null,
      requested_effort: null, observed_effort: null,
    });
    const child = makeCall({ executor: makeExecutor({
      tool: 'jev', provider: 'jev', role: 'decision_advisor',
      requested_model: 'jev-latest', observed_model: null,
      requested_effort: null, observed_effort: null,
    }) });
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({
      executor: metadataRoot, children: [child], children_count: 1,
    }));
    const display = projectRequestExecutorDisplay(view);
    assert.equal(display.role, 'Called Decision advisor');
    assert.equal(display.model, 'Jev · jev-latest');
    assert.match(display.detail, /Child call/);
    assert.match(display.detail, /requested jev-latest · observed Not reported/);
    assert.equal(view.request.executor, metadataRoot);
    assert.equal(view.observedModel, 'Not reported');

    const multiple = buildActivityRequestView(makeSnapshot(), makeRequest({
      executor: metadataRoot, children: [child, makeCall({ id: 'second-call' })], children_count: 2,
    }));
    assert.equal(projectRequestExecutorDisplay(multiple).role, 'Called tools');
    assert.match(projectRequestExecutorDisplay(multiple).detail, /2 child executors recorded/);
    const root = buildActivityRequestView(makeSnapshot(), makeRequest({
      executor: makeExecutor(), children: [child], children_count: 1,
    }));
    assert.equal(projectRequestExecutorDisplay(root).role, 'Coder');
    assert.equal(projectRequestExecutorDisplay(root).model, 'GPT-5 · High');
    const noModel = buildActivityRequestView(makeSnapshot(), makeRequest({ executor: metadataRoot }));
    assert.equal(projectRequestExecutorDisplay(noModel).role, 'Threadlane');
    assert.equal(projectRequestExecutorDisplay(noModel).model, null);
  });

  it('labels historical worker recovery without inventing an original request or requester', () => {
    const view = buildActivityRequestView(makeSnapshot(), makeRequest({
      operation: 'worker.historical',
      actor: makeActor({ label: null, role: null, source: null, evidence: 'unknown' }),
      delivery: [],
    }));
    const visible = projectVisibleTimeline(view);
    assert.equal(visible[0]!.display.detail, 'Unknown');
    assert.equal(visible[1]!.display.label, 'Recovered from worker history');
    assert.match(visible[1]!.display.detail, /original request was not recorded/);
    assert.doesNotMatch(JSON.stringify(visible.map((entry) => entry.display)), /Worker\.historical|Request recorded/);
    assert.equal(view.request.operation, 'worker.historical');
    assert.match(view.timeline[1]!.detail, /worker\.historical/);
  });

  it('keeps the scoped action, single heading, compact copy, and removed diagnostics', () => {
    const source = readFileSync(
      new URL('./activity-feed.ts', import.meta.url),
      'utf8'
    );
    assert.equal(
      (source.match(/'data-action': 'activity-all'/g) ?? []).length,
      1
    );
    assert.equal(
      (source.match(/aq-feed-tools tl-tools/g) ?? []).length,
      1
    );
    assert.match(source, /aq-feed-all-activity tl-action/);
    assert.match(source, /private readonly allActivityButton: HTMLButtonElement/);
    assert.match(source, /this\.allActivityButton = h\(/);
    assert.match(source, /this\.scopeEl,\s*this\.allActivityButton/);
    assert.equal((source.match(/'h2'/g) ?? []).length, 1);
    assert.match(source, /refreshTimeLabels\(\): void/);
    assert.match(source, /allActivityButton\.hidden = !projection\.scope\.scoped/);
    assert.match(source, /countRunningRequests\(projection\.rows\)/);
    assert.match(source, /formatRunningCountLabel\(running\)/);
    assert.match(source, /formatActivityScopeLabel\(projection\.scope\)/);
    assert.match(source, /Requested by \$\{requester\.value\}/);
    assert.match(source, /Job: \$\{jobText\}/);
    assert.doesNotMatch(source, /requester\.evidence/);
    assert.doesNotMatch(source, /aq-feed-evidence/);
    assert.doesNotMatch(source, /aq-feed-operation/);
    assert.doesNotMatch(source, /Operation:/);
    assert.doesNotMatch(source, /'Model'|'Effort'/);
    assert.doesNotMatch(source, /aq-feed-expand/);
    assert.doesNotMatch(source, /aq-feed-status-dot/);
    assert.doesNotMatch(source, /aq-feed-event-time/);
    assert.doesNotMatch(source, /footerCount/);
    assert.doesNotMatch(source, /onAllActivity/);
  });

  it('maps threadlane_mcp local executor to Threadlane MCP and Local operation without concealing worker or token evidence', () => {
    const localMcp = makeExecutor({
      tool: 'threadlane_mcp',
      provider: 'local',
      role: null,
      requested_model: null,
      observed_model: null,
      requested_effort: null,
      observed_effort: null,
    });
    assert.equal(isLocalMcpExecutor(localMcp), true);
    assert.equal(isLocalMcpExecutor({ ...localMcp, provider: null }), false);
    assert.equal(isLocalMcpExecutor({ ...localMcp, provider: 'openai' }), false);
    assert.equal(isLocalMcpExecutor({ ...localMcp, requested_model: 'configured-model' }), false);
    assert.equal(isLocalMcpExecutor(null), false);
    assert.equal(isLocalMcpExecutor(makeExecutor()), false);
    assert.equal(formatFriendlyTool(localMcp), 'Threadlane MCP');

    // Verified paired Follower caller + local MCP handler without child work or tokens.
    const localView = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        project_label: 'threadlane-follower-context',
        thread_label: 'Follower Onboarding',
        actor: makeActor({
          label: 'Codex · Follower Onboarding',
          role: 'follower',
          source: 'paired_mcp',
          model: null,
          effort: null,
          evidence: 'configured',
        }),
        executor: localMcp,
        usage: null,
        children: [],
        children_count: 0,
      })
    );
    assert.equal(localView.projectLabel, 'threadlane-follower-context');
    assert.equal(localView.requesterLine.value, 'Codex · Follower Onboarding');
    assert.equal(localView.requesterLine.evidence, 'Configured');
    assert.equal(localView.actorRole, 'follower');
    assert.equal(localView.actorModel, 'Not reported');
    assert.equal(localView.actorEffort, 'Not reported');
    assert.equal(localView.executorIdentity, 'Threadlane MCP · local');
    const localExecDisplay = projectRequestExecutorDisplay(localView);
    assert.equal(localExecDisplay.role, 'Threadlane MCP');
    assert.equal(localExecDisplay.model, 'Local operation');
    assert.match(localExecDisplay.detail, /Local MCP operation/);
    assert.equal(localView.usage.reported, false);
    assert.equal(localView.usage.summary, 'Local operation');
    assert.equal(formatCompactUsage(localView.usage).value, 'Local operation');

    // Explicit local MCP root with worker child calls must NOT conceal worker evidence or missing-usage warning.
    const workerChild = makeCall({
      id: 'worker-1',
      executor: makeExecutor({
        tool: 'opencode-go',
        provider: 'opencode',
        role: 'coder',
        requested_model: 'opencode-go/deepseek-v4.1-flash',
        observed_model: 'opencode-go/deepseek-v4.1-flash',
        requested_effort: 'max',
        observed_effort: 'max',
      }),
    });
    const withWorker = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: localMcp,
        usage: null,
        children: [workerChild],
        children_count: 1,
      })
    );
    assert.ok(withWorker.calledExecutor);
    const withWorkerDisplay = projectRequestExecutorDisplay(withWorker);
    assert.equal(withWorkerDisplay.role, 'Called Coder');
    assert.equal(withWorkerDisplay.model, 'DeepSeek · Max');
    assert.equal(withWorker.usage.summary, 'Usage not reported');
    assert.equal(formatCompactUsage(withWorker.usage).value, 'Usage not reported');

    // Reported tokens on a local MCP root are preserved and never concealed.
    const withTokens = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: localMcp,
        usage: makeUsage({ total_tokens: 125 }),
      })
    );
    assert.equal(withTokens.usage.reported, true);
    assert.equal(formatCompactUsage(withTokens.usage).value, '125 tokens');

    // Real model inference without reported tokens still reports 'Usage not reported'.
    const realInferenceNoUsage = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        executor: makeExecutor(),
        usage: null,
      })
    );
    assert.equal(realInferenceNoUsage.usage.summary, 'Usage not reported');
    assert.equal(
      formatCompactUsage(realInferenceNoUsage.usage).value,
      'Usage not reported'
    );

    // Unknown historic rows stay unknown without cosmetic invented attribution.
    const unknownHistoric = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        actor: makeActor({
          label: null,
          role: null,
          source: 'paired_mcp',
          model: null,
          effort: null,
          evidence: 'unknown',
        }),
        executor: null,
        usage: null,
      })
    );
    assert.equal(unknownHistoric.requesterLine.value, 'Unknown');
    assert.equal(projectRequestExecutorDisplay(unknownHistoric).role, 'Executor not recorded');
    assert.equal(projectRequestExecutorDisplay(unknownHistoric).model, null);
    assert.equal(formatCompactUsage(unknownHistoric.usage).value, 'Usage not reported');
  });

  it('displays native Coworker selected model and effort without claiming runtime-observed execution', () => {
    const legacyActor = makeActor({
      label: 'Codex · Follower Onboarding',
      role: 'coworker',
      source: 'paired_mcp',
      model: null,
      effort: null,
      evidence: 'configured',
    });
    assert.equal(formatRequesterSelectedModel(legacyActor), null);
    const legacyView = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({ actor: legacyActor })
    );
    assert.equal(legacyView.requesterLine.value, 'Codex · Follower Onboarding');
    assert.equal(legacyView.requesterLine.evidence, 'Configured');
    assert.equal(legacyView.actorSelectedModel, 'Not reported');
    assert.equal(legacyView.actorSelectedEffort, 'Not reported');
    assert.equal(legacyView.actorSelectedSummary, 'Not reported');
    assert.equal(legacyView.actorModel, 'Not reported');
    assert.equal(legacyView.actorEffort, 'Not reported');

    const selectedCoworker = makeActor({
      label: 'Codex · Follower Onboarding',
      role: 'coworker',
      source: 'paired_mcp',
      model: null,
      effort: null,
      selected_model: 'Sol',
      selected_effort: 'xhigh',
      evidence: 'configured',
    });
    assert.equal(
      formatRequesterSelectedModel(selectedCoworker),
      'Sol · xhigh (selected)'
    );
    assert.equal(
      formatRequesterSelectedModel({
        ...selectedCoworker,
        selected_effort: null,
      }),
      'Sol (selected)'
    );

    const localMcpExecutor = makeExecutor({
      tool: 'threadlane_mcp',
      provider: 'local',
      role: null,
      requested_model: null,
      observed_model: null,
      requested_effort: null,
      observed_effort: null,
    });
    const view = buildActivityRequestView(
      makeSnapshot(),
      makeRequest({
        actor: selectedCoworker,
        executor: localMcpExecutor,
        usage: null,
        children: [],
        children_count: 0,
      })
    );
    assert.equal(
      view.requesterLine.value,
      'Codex · Follower Onboarding · Sol · xhigh (selected)'
    );
    assert.equal(view.requesterLine.evidence, 'Configured');
    assert.equal(
      view.requesterLine.detail,
      'Requester Codex · Follower Onboarding · Sol · xhigh (selected) · Configured.'
    );
    assert.equal(
      view.requesterSummary,
      'Requester: Codex · Follower Onboarding · Sol · xhigh (selected) (Configured)'
    );
    assert.equal(view.actorSelectedModel, 'Sol');
    assert.equal(view.actorSelectedEffort, 'xhigh');
    assert.equal(view.actorSelectedSummary, 'Sol · xhigh (selected)');
    // Runtime-observed actor model/effort remain strictly separate and unreported.
    assert.equal(view.actorModel, 'Not reported');
    assert.equal(view.actorEffort, 'Not reported');
    // Executor rows and usage remain untouched.
    assert.equal(view.requestedModel, 'Not reported');
    assert.equal(view.observedModel, 'Not reported');
    assert.equal(projectRequestExecutorDisplay(view).role, 'Threadlane MCP');
    assert.equal(projectRequestExecutorDisplay(view).model, 'Local operation');
    assert.equal(formatCompactUsage(view.usage).value, 'Local operation');

    const requesterEvent = view.timeline.find((e) => e.kind === 'requester');
    assert.ok(requesterEvent);
    assert.deepEqual(requesterEvent.lines, [
      'Evidence: Configured',
      'Source: paired_mcp · role coworker',
      'Configured: Sol · xhigh (selected)',
      'Model: Not reported · effort Not reported',
    ]);
    const timelineDisplay = projectTimelineDisplay(view, requesterEvent);
    assert.equal(
      timelineDisplay.detail,
      'Codex · Follower Onboarding · Coworker · Sol · xhigh (selected) · paired_mcp'
    );
    assert.ok(
      timelineDisplay.lines.includes('Configured model: Sol · xhigh (selected)')
    );
  });
});
