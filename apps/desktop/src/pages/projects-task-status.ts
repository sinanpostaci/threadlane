export type ProjectTaskStatusTone =
  | 'ok'
  | 'danger'
  | 'waiting'
  | 'active'
  | 'unknown'
  | 'neutral'
  | 'cancelled';

type StatusDescriptor = {
  label: string;
  tone: ProjectTaskStatusTone;
};

const UNKNOWN_STATUS: StatusDescriptor = {
  label: 'Status unknown',
  tone: 'unknown',
};

const STATUS_BY_ALIAS = new Map<string, StatusDescriptor>([
  ['completed', { label: 'Done', tone: 'ok' }],
  ['accepted', { label: 'Done', tone: 'ok' }],
  ['historical_accepted', { label: 'Done', tone: 'ok' }],
  ['integrated', { label: 'Done', tone: 'ok' }],
  ['ready_for_review', { label: 'Awaiting review', tone: 'neutral' }],
  ['pending_review', { label: 'Awaiting review', tone: 'neutral' }],
  ['failed', { label: 'Failed', tone: 'danger' }],
  ['timed_out', { label: 'Timed out', tone: 'danger' }],
  ['cancelled', { label: 'Stopped', tone: 'cancelled' }],
  ['canceled', { label: 'Stopped', tone: 'cancelled' }],
  ['not_started', { label: 'Not started', tone: 'neutral' }],
  ['pending_dependencies', { label: 'Not started', tone: 'neutral' }],
  ['dependency_inputs_unverified', { label: 'Not started', tone: 'neutral' }],
  ['dependency_inputs_ready', { label: 'Not started', tone: 'neutral' }],
  ['native_route_required', { label: 'Not started', tone: 'neutral' }],
  ['not_approved', { label: 'Approval required', tone: 'waiting' }],
  ['pending_approval', { label: 'Approval required', tone: 'waiting' }],
  ['running', { label: 'Running', tone: 'active' }],
  ['claimed', { label: 'Running', tone: 'active' }],
  ['queued', { label: 'Queued', tone: 'neutral' }],
  ['pending', { label: 'Queued', tone: 'neutral' }],
  ['held', { label: 'On hold', tone: 'waiting' }],
  ['on_hold', { label: 'On hold', tone: 'waiting' }],
]);

export function projectTaskStatus(status: string): {
  label: string;
  tone: ProjectTaskStatusTone;
} {
  const normalized =
    typeof status === 'string' ? status.trim().toLowerCase() : '';
  const descriptor = STATUS_BY_ALIAS.get(normalized) ?? UNKNOWN_STATUS;
  return { label: descriptor.label, tone: descriptor.tone };
}

const SUMMARY_ORDER = [
  'Failed',
  'Timed out',
  'Status unknown',
  'On hold',
  'Approval required',
  'Awaiting review',
  'Running',
  'Queued',
  'Not started',
  'Done',
  'Stopped',
] as const;

export function summarizeProjectTaskStatuses(
  tasks: readonly { status: string }[],
): string | null {
  if (tasks.length === 0) return null;

  const counts = new Map<string, number>();
  for (const task of tasks) {
    const { label } = projectTaskStatus(task.status);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }

  const parts: string[] = [];
  for (const label of SUMMARY_ORDER) {
    const count = counts.get(label);
    if (count !== undefined) {
      parts.push(`${count} ${label.toLowerCase()}`);
    }
  }
  return parts.join(' · ');
}
