import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
} from '../observation/types.ts';
import {
  HAS_MORE_DIAGNOSTIC_WARNING,
  JOBS_HAS_MORE_DIAGNOSTIC_WARNING,
  projectDiagnostics,
} from './projects-diagnostics.ts';
import { projectProjects } from './projects-model.ts';

const NOW_MS = 1_760_000_000_000;

function makeSource(
  overrides: Partial<ObservationSource> = {}
): ObservationSource {
  return {
    id: 'src_alpha',
    label: 'Threadlane Alpha',
    project_path: '/workspace/alpha',
    availability: 'available',
    message: null,
    ...overrides,
  };
}

function makeJob(overrides: Partial<ObservationJob> = {}): ObservationJob {
  return {
    source_id: 'src_alpha',
    id: 'job_sole_1',
    project_label: 'Threadlane Alpha',
    project_path: '/workspace/alpha',
    title: 'Sole Visible Job',
    thread_uuid: '0196ed00-0000-7000-8000-000000000001',
    thread_label: 'Sole Visible Chat',
    binding_id: 'bnd_sole_1',
    plan_id: 'plan_sole_1',
    plan_revision: 1,
    state: 'running',
    approval_status: 'approved',
    execution_status: 'running',
    review_state: null,
    updated_at_ms: NOW_MS - 1_000,
    active_call_count: 1,
    tasks: [],
    summary: 'Running task',
    ...overrides,
  };
}

function makeRequest(
  overrides: Partial<ObservationRequest> = {}
): ObservationRequest {
  return {
    source_id: 'src_alpha',
    id: 'obsreq_sole_1',
    job_id: 'job_sole_1',
    parent_request_id: null,
    title: 'workflow status',
    operation: 'context.workflow.status',
    project_label: 'Threadlane Alpha',
    binding_id: 'bnd_sole_1',
    thread_uuid: '0196ed00-0000-7000-8000-000000000001',
    thread_label: 'Sole Visible Chat',
    state: 'running',
    started_at_ms: NOW_MS - 2_000,
    finished_at_ms: null,
    updated_at_ms: NOW_MS - 1_000,
    duration_ms: null,
    actor: {
      label: 'Operator',
      role: 'operator',
      source: 'mcp',
      model: null,
      effort: null,
      evidence: 'observed',
    },
    executor: null,
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: 'Status check',
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

describe('projects diagnostics projection', () => {
  it('maps the seven screenshot warnings to clear meanings, severities, and source provenance without claiming corruption or fake thread identity', () => {
    const screenshotWarnings = [
      'Skipped unreadable or invalid coworker workflow ledger in observation source.',
      'Skipped unreadable or invalid coworker controller runtime ledger in observation source.',
      'Ignored unverified job, plan, task, or approval hint on journal record.',
      'Detected conflicting scope fields across journal phases for a request.',
      'Rejected plan request link due to mismatched plan ID between record and finish result.',
      'Rejected unverified or mismatched parent request hint; kept request standalone.',
      HAS_MORE_DIAGNOSTIC_WARNING,
    ];

    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'src_alpha',
          label: 'Threadlane Alpha',
        }),
      ],
      jobs: [makeJob()],
      requests: [makeRequest()],
      has_more: true,
      warnings: screenshotWarnings,
      warning_contexts: [
        {
          warning: screenshotWarnings[0]!,
          source_id: 'src_alpha',
          origin: 'jobs',
        },
        {
          warning: screenshotWarnings[1]!,
          source_id: 'src_alpha',
          origin: 'jobs',
        },
        {
          warning: screenshotWarnings[2]!,
          source_id: 'src_alpha',
          origin: 'journal',
        },
        {
          warning: screenshotWarnings[3]!,
          source_id: 'src_alpha',
          origin: 'requests',
        },
        {
          warning: screenshotWarnings[4]!,
          source_id: 'src_alpha',
          origin: 'journal',
        },
        {
          warning: screenshotWarnings[5]!,
          source_id: 'src_alpha',
          origin: 'requests',
        },
      ],
    });

    const rows = projectDiagnostics(snapshot);
    assert.equal(rows.length, 7);

    // 1. Coworker workflow ledger skipped
    const workflowRow = rows[0]!;
    assert.equal(workflowRow.severity, 'warning');
    assert.equal(workflowRow.sourceId, 'src_alpha');
    assert.equal(workflowRow.sourceLabel, 'Threadlane Alpha (alpha)');
    assert.doesNotMatch(workflowRow.sourceLabel, /src_alpha/);
    assert.match(workflowRow.technicalDetails, /src_alpha/);
    assert.equal(workflowRow.area, 'Workflow history');
    assert.match(workflowRow.impact, /not proof of a worker failure/i);
    assert.doesNotMatch(workflowRow.impact, /corrupt/i);
    assert.doesNotMatch(workflowRow.title, /corrupt/i);
    assert.match(workflowRow.nextStep, /compatibility|readability/i);
    assert.equal(workflowRow.conversationNote, 'Exact conversation not identified');

    // 2. Coworker controller runtime ledger skipped
    const runtimeRow = rows[1]!;
    assert.equal(runtimeRow.severity, 'warning');
    assert.equal(runtimeRow.area, 'Execution history');
    assert.match(runtimeRow.impact, /runtime execution history is incomplete/i);
    assert.equal(runtimeRow.conversationNote, 'Exact conversation not identified');

    // 3. Ignored unverified job/plan/task/approval hint
    const hintRow = rows[2]!;
    assert.equal(hintRow.severity, 'warning');
    assert.match(hintRow.title, /job association not verified/i);
    assert.match(hintRow.impact, /shown without that association/i);
    assert.equal(hintRow.conversationNote, 'Exact conversation not identified');

    // 4. Conflicting scope fields across journal phases
    const conflictRow = rows[3]!;
    assert.equal(conflictRow.severity, 'warning');
    assert.match(conflictRow.title, /inconsistent history association/i);
    assert.match(
      conflictRow.impact,
      /cannot be safely attached to a conversation or job/i
    );
    assert.equal(conflictRow.conversationNote, 'Exact conversation not identified');

    // 5. Rejected plan request link due to mismatched plan ID
    const planLinkRow = rows[4]!;
    assert.equal(planLinkRow.severity, 'warning');
    assert.match(planLinkRow.title, /plan and activity link rejected/i);
    assert.match(planLinkRow.impact, /history association is incomplete/i);
    assert.equal(planLinkRow.conversationNote, 'Exact conversation not identified');

    // 6. Rejected parent request hint
    const parentRow = rows[5]!;
    assert.equal(parentRow.severity, 'warning');
    assert.match(parentRow.title, /parent request association rejected/i);
    assert.match(parentRow.impact, /request is shown independently/i);
    assert.equal(parentRow.conversationNote, 'Exact conversation not identified');

    // 7. Pagination has_more is informational, not a warning fault
    const pagingRow = rows[6]!;
    assert.equal(pagingRow.severity, 'info');
    assert.match(pagingRow.impact, /information only/i);
    assert.match(pagingRow.impact, /not a fault/i);

    // None of the rows may fabricate attribution to the sole visible chat or job.
    for (const row of rows) {
      const combined = `${row.title} ${row.sourceLabel} ${row.area} ${row.conversationNote} ${row.impact} ${row.nextStep}`;
      assert.doesNotMatch(combined, /Sole Visible Chat/);
      assert.doesNotMatch(combined, /Sole Visible Job/);
      assert.doesNotMatch(combined, /job_sole_1/);
      assert.doesNotMatch(combined, /0196ed00-0000-7000-8000-000000000001/);
    }
  });

  it('explicitly reports source not recorded on legacy snapshots without warning_contexts and never infers from a sole source or chat', () => {
    const legacySnapshot: ObservationSnapshot = {
      schema_version: 1,
      generated_at_ms: NOW_MS,
      sources: [
        makeSource({
          id: 'src_only',
          label: 'Sole Configured Source',
        }),
      ],
      jobs: [makeJob({ source_id: 'src_only' })],
      requests: [makeRequest({ source_id: 'src_only' })],
      next_cursor: null,
      has_more: false,
      jobs_has_more: false,
      warnings: [
        'Skipped unreadable or invalid coworker workflow ledger in observation source.',
      ],
    };

    const projection = projectProjects(legacySnapshot, { nowMs: NOW_MS });
    assert.equal(projection.diagnostics.length, 1);
    const diag = projection.diagnostics[0]!;
    assert.equal(diag.sourceId, null);
    assert.match(diag.sourceLabel, /source not recorded/i);
    assert.match(diag.sourceLabel, /source unknown/i);
    assert.doesNotMatch(diag.sourceLabel, /Sole Configured Source/);
    assert.doesNotMatch(diag.sourceLabel, /src_only/);
    assert.equal(diag.conversationNote, 'Exact conversation not identified');
  });

  it('projects separate diagnostic rows with distinguishing project paths when multiple sources share the generic Linked Threadlane label', () => {
    const sharedWarning =
      'Skipped unreadable or invalid coworker controller runtime ledger in observation source.';
    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'src_alpha',
          label: 'Linked Threadlane',
          project_path: '/workspace/alpha',
        }),
        makeSource({
          id: 'src_beta',
          label: 'Linked Threadlane',
          project_path: '/workspace/beta',
        }),
        makeSource({
          id: 'src_service_a',
          label: 'Linked Threadlane',
          project_path: '/repos/team-a/service',
        }),
        makeSource({
          id: 'src_service_b',
          label: 'Linked Threadlane',
          project_path: '/repos/team-b/service',
        }),
      ],
      warnings: [sharedWarning],
      warning_contexts: [
        {
          warning: sharedWarning,
          source_id: 'src_alpha',
          origin: 'jobs',
        },
        {
          warning: sharedWarning,
          source_id: 'src_beta',
          origin: 'jobs',
        },
        {
          warning: sharedWarning,
          source_id: 'src_service_a',
          origin: 'jobs',
        },
        {
          warning: sharedWarning,
          source_id: 'src_service_b',
          origin: 'jobs',
        },
      ],
    });

    const rows = projectDiagnostics(snapshot);
    assert.equal(rows.length, 4);
    assert.equal(rows[0]!.sourceId, 'src_alpha');
    assert.equal(rows[0]!.sourceLabel, 'Linked Threadlane (alpha)');
    assert.doesNotMatch(rows[0]!.sourceLabel, /src_alpha/);
    assert.match(rows[0]!.technicalDetails, /src_alpha/);

    assert.equal(rows[1]!.sourceId, 'src_beta');
    assert.equal(rows[1]!.sourceLabel, 'Linked Threadlane (beta)');
    assert.doesNotMatch(rows[1]!.sourceLabel, /src_beta/);
    assert.match(rows[1]!.technicalDetails, /src_beta/);

    assert.equal(rows[2]!.sourceId, 'src_service_a');
    assert.equal(rows[2]!.sourceLabel, 'Linked Threadlane (team-a/service)');
    assert.equal(rows[3]!.sourceId, 'src_service_b');
    assert.equal(rows[3]!.sourceLabel, 'Linked Threadlane (team-b/service)');
  });

  it('treats jobs_has_more and has_more as informational rows and retains unknown warnings with full technical details', () => {
    const unknownWarning =
      'Unexpected custom diagnostic notice from future observation build.';
    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'src_unavailable',
          label: 'Linked Threadlane',
          project_path: null,
          availability: 'unavailable',
          message: 'Local Threadlane durable stores are unavailable.',
        }),
      ],
      has_more: true,
      jobs_has_more: true,
      warnings: [unknownWarning],
      warning_contexts: [
        {
          warning: unknownWarning,
          source_id: 'src_missing_from_inventory',
          origin: 'journal',
        },
      ],
    });

    const rows = projectDiagnostics(snapshot);
    const unknownRow = rows.find((row) => row.rawWarning === unknownWarning);
    assert.ok(unknownRow);
    assert.equal(unknownRow.severity, 'warning');
    assert.equal(
      unknownRow.sourceLabel,
      'Source details unavailable (project path unknown)'
    );
    assert.doesNotMatch(unknownRow.sourceLabel, /src_missing_from_inventory/);
    assert.equal(unknownRow.conversationNote, 'Exact conversation not identified');
    assert.match(unknownRow.technicalDetails, new RegExp(unknownWarning));
    assert.match(unknownRow.technicalDetails, /src_missing_from_inventory/);

    const unavailableRow = rows.find(
      (row) => row.sourceId === 'src_unavailable'
    );
    assert.ok(unavailableRow);
    assert.equal(unavailableRow.severity, 'warning');
    assert.equal(unavailableRow.area, 'Source availability');
    assert.equal(
      unavailableRow.sourceLabel,
      'Linked Threadlane (project path unknown)'
    );

    const jobsMoreRow = rows.find(
      (row) => row.rawWarning === JOBS_HAS_MORE_DIAGNOSTIC_WARNING
    );
    assert.ok(jobsMoreRow);
    assert.equal(jobsMoreRow.severity, 'info');

    const requestsMoreRow = rows.find(
      (row) => row.rawWarning === HAS_MORE_DIAGNOSTIC_WARNING
    );
    assert.ok(requestsMoreRow);
    assert.equal(requestsMoreRow.severity, 'info');
  });

  it('filters out other sources when scoped, keeps matching-source note accurate, and never claims absent-context warnings were recorded for the scoped source', () => {
    const attributedWarning =
      'Ignored unverified job, plan, task, or approval hint on journal record.';
    const legacyUnattributedWarning =
      'Skipped unreadable or invalid coworker workflow ledger in observation source.';
    const snapshot = makeSnapshot({
      sources: [
        makeSource({
          id: 'src_alpha',
          label: 'Linked Threadlane',
          project_path: '/workspace/alpha',
        }),
        makeSource({
          id: 'src_beta',
          label: 'Linked Threadlane',
          project_path: '/workspace/beta',
        }),
      ],
      jobs: [makeJob({ source_id: 'src_alpha', id: 'job_selected_99' })],
      warnings: [attributedWarning, legacyUnattributedWarning],
      warning_contexts: [
        {
          warning: attributedWarning,
          source_id: 'src_alpha',
          origin: 'journal',
        },
        {
          warning: attributedWarning,
          source_id: 'src_beta',
          origin: 'journal',
        },
      ],
    });

    const scopedRows = projectDiagnostics(snapshot, {
      scope: { source_id: 'src_alpha', job_id: 'job_selected_99' },
    });
    assert.equal(scopedRows.length, 2);

    const matchedSourceRow = scopedRows[0]!;
    assert.equal(matchedSourceRow.sourceId, 'src_alpha');
    assert.equal(matchedSourceRow.sourceLabel, 'Linked Threadlane (alpha)');
    assert.match(
      matchedSourceRow.conversationNote,
      /Exact conversation not identified/
    );
    assert.match(
      matchedSourceRow.conversationNote,
      /recorded for this source, not proven to belong to the selected job/i
    );
    assert.doesNotMatch(matchedSourceRow.conversationNote, /job_selected_99/);

    const absentContextRow = scopedRows[1]!;
    assert.equal(absentContextRow.sourceId, null);
    assert.equal(
      absentContextRow.sourceLabel,
      'Source not recorded (source unknown)'
    );
    assert.equal(
      absentContextRow.conversationNote,
      'Exact conversation not identified'
    );
    assert.doesNotMatch(
      absentContextRow.conversationNote,
      /recorded for this source/i
    );
    assert.doesNotMatch(absentContextRow.conversationNote, /job_selected_99/);
  });
});
