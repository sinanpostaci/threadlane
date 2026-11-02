import type { ObservationJob, ObservationSnapshot, ObservationSource, ObservationTask } from '../observation/types.ts';

export interface JobPlansPageOptions {
  onViewActivity?: (scope: { source_id: string; job_id: string }) => void;
  onOpenAdvanced?: () => void;
}

export interface JobPlansUpdateOptions {
  loading?: boolean;
  error?: string | null;
}

export interface JobPlanProjectGroup {
  key: string;
  label: string;
  project_path: string | null;
  sourceIds: string[];
  jobs: ObservationJob[];
}

export interface JobPlansStateSummary {
  loading: boolean;
  error: string | null;
  notices: string[];
  emptyMessage: string | null;
}

const UNRECORDED = 'Unrecorded';
const UNKNOWN = 'Unknown';
const UNNAMED_SOURCE = 'Unnamed source';
const CONVERSATION_LABEL_UNAVAILABLE = 'Conversation label unavailable';

/**
 * Collision-safe DOM/dataset identity for a job. JSON encoding keeps source and
 * job IDs distinct without embedding NUL separators in attribute values.
 */
export function jobPlanKey(job: Pick<ObservationJob, 'source_id' | 'id'>): string {
  return JSON.stringify([job.source_id, job.id]);
}

export function jobActivityScope(
  job: Pick<ObservationJob, 'source_id' | 'id'>
): { source_id: string; job_id: string } {
  return { source_id: job.source_id, job_id: job.id };
}

function recordedTimestamp(job: Pick<ObservationJob, 'updated_at_ms'>): number {
  return job.updated_at_ms !== null && Number.isFinite(job.updated_at_ms)
    ? job.updated_at_ms
    : Number.NEGATIVE_INFINITY;
}

function compareStableStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

export function compareObservationJobs(a: ObservationJob, b: ObservationJob): number {
  const aTimestamp = recordedTimestamp(a);
  const bTimestamp = recordedTimestamp(b);
  if (aTimestamp !== bTimestamp) {
    return aTimestamp > bTimestamp ? -1 : 1;
  }
  const idOrder = compareStableStrings(a.id, b.id);
  return idOrder !== 0 ? idOrder : compareStableStrings(a.source_id, b.source_id);
}

function projectLabel(job: ObservationJob): string {
  const label = job.project_label.trim();
  if (label) {
    return label;
  }
  const path = job.project_path?.trim();
  return path ? path : 'Unassigned project';
}

function projectGroupKey(job: ObservationJob): string {
  const path = job.project_path?.trim();
  if (path) {
    return JSON.stringify(['path', path]);
  }
  return JSON.stringify(['label', job.source_id, projectLabel(job)]);
}

/**
 * Groups recorded jobs by project without merging same-label projects from
 * different observation sources when no project path is recorded.
 */
export function buildJobPlanGroups(
  snapshot: ObservationSnapshot | null
): JobPlanProjectGroup[] {
  if (!snapshot) {
    return [];
  }
  const groups = new Map<string, JobPlanProjectGroup>();
  for (const job of snapshot.jobs) {
    const key = projectGroupKey(job);
    let group = groups.get(key);
    if (!group) {
      group = {
        key,
        label: projectLabel(job),
        project_path: job.project_path?.trim() || null,
        sourceIds: [],
        jobs: [],
      };
      groups.set(key, group);
    }
    group.jobs.push(job);
    if (!group.sourceIds.includes(job.source_id)) {
      group.sourceIds.push(job.source_id);
    }
  }

  const ordered = Array.from(groups.values());
  for (const group of ordered) {
    group.jobs.sort(compareObservationJobs);
    group.sourceIds.sort(compareStableStrings);
  }
  ordered.sort((a, b) => {
    const newestOrder = compareObservationJobs(a.jobs[0]!, b.jobs[0]!);
    if (newestOrder !== 0) {
      return newestOrder;
    }
    const labelOrder = compareStableStrings(a.label, b.label);
    return labelOrder !== 0 ? labelOrder : compareStableStrings(a.key, b.key);
  });
  return ordered;
}

export function formatJobLifecycle(state: string): string {
  switch (state) {
    case 'planning':
      return 'Planning';
    case 'ready':
      return 'Ready';
    case 'running':
      return 'Running';
    case 'reviewing':
      return 'Reviewing';
    case 'needs_user':
      return 'Needs you';
    case 'completed':
      return 'Completed';
    case 'failed':
      return 'Failed';
    case 'unknown':
      return UNKNOWN;
    default:
      return UNKNOWN;
  }
}

export function formatRecordedStatus(value: string | null | undefined): string {
  const trimmed = typeof value === 'string' ? value.trim() : '';
  return trimmed || UNRECORDED;
}

export function formatReviewState(value: string | null | undefined): string {
  return formatRecordedStatus(value);
}

export function boundConversationLabel(
  job: Pick<ObservationJob, 'thread_label'>
): string {
  const threadLabel = job.thread_label?.trim();
  return threadLabel || CONVERSATION_LABEL_UNAVAILABLE;
}

function isCompletedTaskStatus(status: string): boolean {
  return status.trim().toLowerCase() === 'completed';
}

/**
 * Reports only recorded task/status evidence. It never invents a percentage,
 * zero progress, or an aggregate state for tasks that were not recorded.
 */
export function summarizeJobProgress(
  job: Pick<ObservationJob, 'tasks'>
): string | null {
  if (job.tasks.length === 0) {
    return null;
  }
  const completed = job.tasks.filter((task) => isCompletedTaskStatus(task.status)).length;
  const taskLabel = `${job.tasks.length} task${job.tasks.length === 1 ? '' : 's'} recorded`;
  return completed > 0
    ? `${taskLabel} · ${completed} marked completed`
    : taskLabel;
}

export function summarizeActiveCalls(
  job: Pick<ObservationJob, 'active_call_count'>
): string | null {
  if (
    !Number.isSafeInteger(job.active_call_count) ||
    job.active_call_count <= 0
  ) {
    return null;
  }
  return `${job.active_call_count} active call${
    job.active_call_count === 1 ? '' : 's'
  }`;
}

export interface ResolvedTaskDependency {
  id: string;
  title: string | null;
}

/**
 * Resolves dependency references against tasks recorded in the same job.
 * Unknown references stay explicit instead of being presented as valid tasks.
 */
export function resolveTaskDependencies(
  job: Pick<ObservationJob, 'tasks'>,
  task: Pick<ObservationTask, 'depends_on'>
): ResolvedTaskDependency[] {
  const tasksById = new Map(job.tasks.map((candidate) => [candidate.id, candidate]));
  return task.depends_on.map((id) => {
    const title = tasksById.get(id)?.title.trim() ?? '';
    return { id, title: title || null };
  });
}

export function sourceLabel(source: ObservationSource): string {
  return source.label.trim() || UNNAMED_SOURCE;
}

export function describeJobPlansState(
  snapshot: ObservationSnapshot | null,
  options: JobPlansUpdateOptions = {}
): JobPlansStateSummary {
  const error = options.error?.trim() || null;
  const notices: string[] = [];

  if (snapshot) {
    for (const warning of snapshot.warnings) {
      const trimmed = warning.trim();
      if (trimmed && !notices.includes(trimmed)) {
        notices.push(trimmed);
      }
    }
    for (const source of snapshot.sources) {
      if (source.availability !== 'available') {
        const message = source.message?.trim();
        notices.push(
          `Source ${sourceLabel(source)} is unavailable${
            message ? `: ${message}` : '.'
          }`
        );
      }
    }
    if (snapshot.jobs_has_more) {
      notices.push(
        'More jobs exist than this bounded observation page shows. The job list is limited to the observation snapshot; request pagination in Activity does not extend this job list.'
      );
    }
    if (snapshot.has_more) {
      notices.push(
        'More request activity is available in Activity. This plan view lists only the jobs on the current observation page.'
      );
    }
  }

  let emptyMessage: string | null = null;
  if (!options.loading && (!snapshot || snapshot.jobs.length === 0)) {
    if (error) {
      emptyMessage =
        'No observed plan data is available. The latest observation request failed; retry from Activity.';
    } else if (!snapshot) {
      emptyMessage = 'No observation snapshot is available yet.';
    } else if (snapshot.jobs_has_more) {
      emptyMessage =
        'No jobs are shown on this bounded observation page, but more jobs exist outside the current limit.';
    } else {
      emptyMessage = 'No jobs were recorded in the current observation scope.';
    }
  }

  return {
    loading: options.loading === true,
    error,
    notices,
    emptyMessage,
  };
}

/**
 * Semantic signature for one rendered job row. It contains exactly the values
 * that row displays, including technical identifiers and the resolved source
 * label, so unrelated snapshot changes can leave the row DOM untouched.
 */
export function jobPlanRowSignature(
  job: ObservationJob,
  sourceLabelValue: string
): string {
  return JSON.stringify([
    jobPlanKey(job),
    job.title,
    job.state,
    job.thread_label,
    job.active_call_count,
    job.tasks.map((task) => [
      task.id,
      task.title,
      task.status,
      task.depends_on,
    ]),
    job.summary,
    job.approval_status,
    job.execution_status,
    job.review_state,
    job.project_path,
    job.plan_id,
    job.plan_revision,
    job.thread_uuid,
    job.binding_id,
    sourceLabelValue,
  ]);
}

function jobPlanGroupSignatureInput(
  snapshot: ObservationSnapshot | null
): Array<{ key: string; label: string; jobs: string[] }> {
  const groups = buildJobPlanGroups(snapshot);
  const sourceLabels = new Map(
    (snapshot?.sources ?? []).map(
      (source) => [source.id, sourceLabel(source)] as const
    )
  );
  return groups.map((group) => ({
    key: group.key,
    label: group.label,
    jobs: group.jobs.map((job) =>
      jobPlanRowSignature(
        job,
        sourceLabels.get(job.source_id) ?? UNNAMED_SOURCE
      )
    ),
  }));
}

/**
 * Semantic signature for the rendered project/job tree only. The component
 * uses this to skip DOM reconciliation when only an error, empty message, or
 * notice changed, while still reusing every unchanged project and job node.
 */
export function jobPlanGroupsSignature(
  snapshot: ObservationSnapshot | null
): string {
  return JSON.stringify(jobPlanGroupSignatureInput(snapshot));
}

/**
 * Semantic signature for the rendered Plans body. Order is included because
 * project/job order is visible. Notices are included because they render below
 * the jobs, while unrendered snapshot fields (generation time) are deliberately
 * excluded.
 */
export function jobPlanRenderSignature(
  snapshot: ObservationSnapshot | null,
  options: JobPlansUpdateOptions = {}
): string {
  const state = describeJobPlansState(snapshot, options);
  return JSON.stringify({
    error: state.error,
    emptyMessage: state.error ? null : state.emptyMessage,
    notices: state.notices,
    groups: jobPlanGroupSignatureInput(snapshot),
  });
}
