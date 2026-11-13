import './workspace-panels.css';

// Workspace panels are retained for compatibility; the selected-chat surface
// now renders the minimal chat-work stream and backlog drawer.

import { Dialog, IconButton } from '@radix-ui/themes';
import {
  AlertCircle,
  Blocks,
  Bot,
  ExternalLink,
  FolderGit2,
  History,
  Loader2,
  RefreshCw,
  SquareTerminal,
  Terminal,
  X,
} from 'lucide-react';
import {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type { KeyboardEvent as ReactKeyboardEvent } from 'react';

import { ProviderBrandBadge, resolveProviderBrandKey } from './app-brand.tsx';
import {
  DEFAULT_WORKSPACE_CHAT_TAB,
  WORKSPACE_CHAT_SURFACE_TABS,
  normalizeWorkspaceChatTab,
} from './app-contract.ts';
import type {
  CodexChatTarget,
  WorkspaceChatIdentity,
  WorkspaceChatSurfaceTab,
  WorkspaceScopedChatTarget,
} from './app-contract.ts';
import {
  projectWorkspaceJobExternalResolutionViews,
  projectWorkspaceTaskExternalResolutionState,
} from './app-model.ts';
import type {
  WorkspaceCoveredAttentionView,
  WorkspaceExternalResolutionView,
} from './app-model.ts';
import {
  BACKLOG_STATUS_LABELS,
  DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
  backlogStatusTone,
  buildChatWorkBacklogGroups,
  buildChatWorkEventViews,
  computeAnchorScrollTop,
  createMessageScrollStore,
  doesJobBelongToChat,
  doesMessageBelongToChat,
  doesObservationRequestMatchChat,
  evaluateChatWorkLiveness,
  filterJobsForChat,
  filterMessagesForChat,
  filterObservationRequestsForActivity,
  getActiveChatScrollState,
  getChatScrollState,
  handleMessageViewportScroll,
  jumpToLatestMessages,
  measureDomMessageViewport,
  orderWorkspaceMessageItems,
  readWorkspaceChatWork,
  resolveMessageScrollChatKey,
  resolveMessageViewportResize,
  resolveVerifiedChatJobIds,
  selectWorkspaceChatWork,
  setChatScrollState,
  switchMessageScrollChat,
  syncChatWorkEventScroll,
  syncMessageScrollItems,
} from './message-scroll.ts';
import type {
  ChatWorkLiveness,
  MessageScrollMode,
  MessageScrollState,
  MessageScrollStore,
  ObservationChatMatchContext,
} from './message-scroll.ts';
import type {
  ObservationDeliveryStage,
  ObservationExecutor,
  ObservationJobState,
  ObservationSnapshot,
} from './observation/types.ts';
import { formatRelativeTimestamp } from './format.ts';
import {
  describeDeliveryStage,
  formatActivityRunningCountLabel,
  formatCompactDuration,
  formatCompactUsage,
  formatExecutorRoleLabel,
  formatFeedTimestamp,
  formatFriendlyModel,
  formatRelativeTime,
  formatRequestedObserved,
  hasExecutorRecord,
  projectActivityFeed,
  projectRequestExecutorDisplay,
  projectVisibleTimeline,
} from './pages/activity-feed-model.ts';
import type {
  ActivityFeedScope,
  ActivityRequestView,
} from './pages/activity-feed-model.ts';
import {
  MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW,
  evaluateWorkspaceNativeSampleAt,
  isValidWorkspaceChatIdentity,
} from './workspace-contract.ts';
import type {
  WorkspaceAcceptanceProof,
  WorkspaceAcceptanceStatus,
  WorkspaceActivityState,
  WorkspaceAttentionState,
  WorkspaceAvailabilityState,
  WorkspaceChatSummary,
  WorkspaceCombinedCandidate,
  WorkspaceHistoricalFailure,
  WorkspaceJobHistory,
  WorkspaceJobHistoryTruncation,
  WorkspaceJobResult,
  WorkspaceMessagesWindow,
  WorkspaceNativeSample,
  WorkspaceOutboxRecord,
  WorkspaceRecordLinks,
  WorkspaceRunRecord,
  WorkspaceSafeMessageSourceKind,
  WorkspaceSnapshot,
  WorkspaceSnapshotBounds,
  WorkspaceTaskAttempt,
  WorkspaceTaskCandidate,
  WorkspaceTaskHistory,
} from './workspace-contract.ts';

// ---------------------------------------------------------------------------
// Centered & Row-Aligned Activity Spinner
// ---------------------------------------------------------------------------

export interface ActivitySpinnerProps {
  label?: string;
  className?: string;
}

/**
 * Row-aligned, geometrically centered spinner for live `running` Activity items.
 * Uses a fixed 14x14 `inline-flex` box (`line-height: 0`) around a `display: block`
 * square `<svg>` with concentric `<circle cx="12" cy="12" r="9">` elements so
 * rotation never wobbles off-center or shifts baseline row alignment.
 */
export function ActivitySpinner({ label, className }: ActivitySpinnerProps) {
  const classes = className ? `tl-wp-spinner ${className}` : 'tl-wp-spinner';
  return (
    <span
      className={classes}
      data-activity-spinner="true"
      role={label ? 'status' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : 'true'}
    >
      <svg
        className="tl-wp-spinner-svg"
        viewBox="0 0 24 24"
        width="14"
        height="14"
        fill="none"
        focusable="false"
        aria-hidden="true"
      >
        <circle
          className="tl-wp-spinner-track"
          cx="12"
          cy="12"
          r="9"
          stroke="currentColor"
          strokeWidth="2.25"
        />
        <circle
          className="tl-wp-spinner-arc"
          cx="12"
          cy="12"
          r="9"
          stroke="currentColor"
          strokeWidth="2.25"
          strokeLinecap="round"
          strokeDasharray="16 42"
        />
      </svg>
    </span>
  );
}

// ---------------------------------------------------------------------------
// Formatting & Scoping Helpers (English Fixed UI Labels; Plain-Text Content)
// ---------------------------------------------------------------------------

function formatAvailabilityBadge(availability: WorkspaceAvailabilityState): {
  label: string;
  tone: string;
} {
  switch (availability) {
    case 'available':
      return { label: 'Connected', tone: 'ok' };
    case 'disconnected':
      return { label: 'Disconnected', tone: 'danger' };
    case 'stale':
      return { label: 'Stale', tone: 'attention' };
    case 'unavailable':
      return { label: 'Unavailable', tone: 'neutral' };
  }
}

function formatChatActivityBadge(activity: WorkspaceActivityState): {
  label: string;
  tone: string;
} {
  switch (activity) {
    case 'working':
      return { label: 'Coworker working', tone: 'working' };
    case 'idle':
      return { label: 'Coworker idle', tone: 'neutral' };
    case 'waiting_for_approval':
      return { label: 'Coworker awaiting approval', tone: 'attention' };
    case 'waiting_for_input':
      return { label: 'Coworker awaiting input', tone: 'attention' };
    case 'unknown':
      return { label: 'Coworker status unknown', tone: 'unknown' };
  }
}

type MessageCategoryIcon = typeof Blocks;

/**
 * Maps recorded safe message source kinds to neutral category metadata.
 * `WorkspaceMessageItem` carries no receipt-backed structured outcome, so source
 * kind alone never produces green/verified/success or live-waiting tones.
 */
export function formatMessageSourceMeta(kind: WorkspaceSafeMessageSourceKind): {
  label: string;
  category: 'summary' | 'result' | 'question';
  tone: 'neutral';
  icon: MessageCategoryIcon;
} {
  switch (kind) {
    case 'work_brief_summary':
      return { label: 'Work brief', category: 'summary', tone: 'neutral', icon: Blocks };
    case 'plan_summary':
      return { label: 'Plan', category: 'summary', tone: 'neutral', icon: Blocks };
    case 'journal_summary':
      return { label: 'Journal', category: 'summary', tone: 'neutral', icon: History };
    case 'question_prompt':
      return { label: 'Question', category: 'question', tone: 'neutral', icon: AlertCircle };
    case 'question_answer':
      return { label: 'Answer', category: 'question', tone: 'neutral', icon: Bot };
    case 'attention_summary':
      return { label: 'Attention note', category: 'question', tone: 'neutral', icon: AlertCircle };
    case 'task_result_summary':
      return { label: 'Task result', category: 'result', tone: 'neutral', icon: Terminal };
    case 'review_verdict_summary':
      return { label: 'Review verdict', category: 'result', tone: 'neutral', icon: Blocks };
    case 'closure_summary':
      return { label: 'Closure summary', category: 'result', tone: 'neutral', icon: History };
    case 'job_result_summary':
      return { label: 'Job result', category: 'result', tone: 'neutral', icon: SquareTerminal };
  }
}

const SHORT_MONTH_LABELS = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
] as const;

function capitalizeFirst(value: string): string {
  return value.length > 0 ? value.charAt(0).toUpperCase() + value.slice(1) : value;
}

function formatConciseMessageTime(
  tsMs: number | null | undefined,
  referenceDayMs: number | null | undefined,
): string {
  if (typeof tsMs !== 'number' || !Number.isFinite(tsMs) || tsMs <= 0) {
    return 'No time';
  }
  const date = new Date(tsMs);
  const hhmm = `${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
  if (
    typeof referenceDayMs === 'number' &&
    Number.isFinite(referenceDayMs) &&
    referenceDayMs > 0
  ) {
    const ref = new Date(referenceDayMs);
    if (
      date.getFullYear() === ref.getFullYear() &&
      date.getMonth() === ref.getMonth() &&
      date.getDate() === ref.getDate()
    ) {
      return hhmm;
    }
    const month = SHORT_MONTH_LABELS[date.getMonth()] ?? '';
    if (date.getFullYear() === ref.getFullYear()) {
      return `${month} ${date.getDate()}, ${hhmm}`;
    }
    return `${month} ${date.getDate()}, ${date.getFullYear()}`;
  }
  return hhmm;
}

function formatConfiguredSelectionDisplay(
  selection: WorkspaceChatSummary['configured_selection'] | null | undefined,
): { friendly: string | null; raw: string | null } {
  if (!selection || selection.evidence !== 'verified_configuration') {
    return { friendly: null, raw: null };
  }
  const rawParts = [selection.selected_model, selection.selected_effort].filter(
    (token): token is string => Boolean(token),
  );
  if (rawParts.length === 0) {
    return { friendly: null, raw: null };
  }
  const friendlyParts = [
    selection.selected_model ? formatFriendlyModel(selection.selected_model) : null,
    selection.selected_effort ? capitalizeFirst(selection.selected_effort) : null,
  ].filter((token): token is string => Boolean(token));
  return {
    friendly: friendlyParts.join(' · '),
    raw: rawParts.join(' · '),
  };
}

function formatJobStateMeta(state: ObservationJobState): { label: string; tone: string } {
  switch (state) {
    case 'planning':
      return { label: 'Planning', tone: 'active' };
    case 'ready':
      return { label: 'Ready', tone: 'neutral' };
    case 'running':
      return { label: 'Running', tone: 'working' };
    case 'reviewing':
      return { label: 'Reviewing', tone: 'review' };
    case 'needs_user':
      return { label: 'Needs user', tone: 'attention' };
    case 'completed':
      return { label: 'Completed', tone: 'ok' };
    case 'failed':
      return { label: 'Failed', tone: 'danger' };
    case 'unknown':
      return { label: 'Unknown', tone: 'unknown' };
  }
}

function isRetiredSupersededPlanningJobHistory(history: WorkspaceJobHistory): boolean {
  const { job } = history;
  if (job.state !== 'unknown' || history.acceptance_proof.status === 'verified_accepted') {
    return false;
  }
  if (history.current_attention !== null && history.current_attention.kind !== 'none') {
    return false;
  }
  if (
    history.tasks.some(
      (task) => task.current_attention !== null && task.current_attention.kind !== 'none',
    )
  ) {
    return false;
  }
  if (job.review_state !== null && job.review_state.trim().length > 0) {
    return false;
  }
  const approvalLower = job.approval_status.trim().toLowerCase();
  const execLower = job.execution_status.trim().toLowerCase();
  const phaseLower = job.phase?.trim().toLowerCase() ?? '';
  return (
    approvalLower === 'superseded' &&
    execLower === 'not_started' &&
    (phaseLower === 'awaiting_approval' || phaseLower === 'held')
  );
}

function formatWorkspaceJobHistoryStateMeta(history: WorkspaceJobHistory): {
  label: string;
  tone: string;
} {
  if (isRetiredSupersededPlanningJobHistory(history)) {
    return { label: 'Superseded', tone: 'neutral' };
  }
  return formatJobStateMeta(history.job.state);
}

function formatAcceptanceStatusMeta(status: WorkspaceAcceptanceStatus): {
  label: string;
  tone: string;
  explanation: string;
} {
  switch (status) {
    case 'verified_accepted':
      return {
        label: 'Verified accepted',
        tone: 'verified',
        explanation: 'Backed by recorded candidate and review/closure receipts.',
      };
    case 'pending_review':
      return {
        label: 'Pending review',
        tone: 'review',
        explanation: 'Worker output awaits review and closure verification.',
      };
    case 'rejected':
      return {
        label: 'Rejected',
        tone: 'danger',
        explanation: 'Recorded review or closure rejected this candidate.',
      };
    case 'not_accepted':
      return {
        label: 'Not accepted',
        tone: 'neutral',
        explanation: 'Execution has not produced an accepted candidate.',
      };
    case 'unknown':
      return {
        label: 'Acceptance unverified',
        tone: 'unknown',
        explanation: 'Completion alone does not prove review acceptance or integration.',
      };
  }
}

function formatAttentionKindLabel(kind: WorkspaceAttentionState['kind']): string {
  switch (kind) {
    case 'needs_approval':
      return 'Needs approval';
    case 'needs_user_input':
      return 'Needs user input';
    case 'on_hold':
      return 'On hold';
    case 'unresolved_effect':
      return 'Unresolved outbox effect';
    case 'blocked_failure':
      return 'Blocked by failure';
    case 'review_required':
      return 'Review required';
    case 'none':
      return 'No active blocker';
    case 'unknown':
      return 'Attention required';
  }
}

function formatTimestampOrFallback(tsMs: number | null | undefined, fallback = 'Time not recorded'): string {
  if (typeof tsMs !== 'number' || !Number.isFinite(tsMs) || tsMs <= 0) {
    return fallback;
  }
  return formatFeedTimestamp(tsMs);
}

function compactSha(sha: string | null | undefined): string | null {
  if (typeof sha !== 'string' || sha.trim().length === 0) {
    return null;
  }
  const clean = sha.trim();
  return clean.length <= 12 ? clean : clean.slice(0, 12);
}

interface WorkspacePanelExternalResolutionEntry {
  view: WorkspaceExternalResolutionView;
  jobId: string;
  jobTitle: string;
  sourceId: string;
}

function collectWorkspacePanelExternalResolutionEntries(
  jobHistories: readonly WorkspaceJobHistory[],
  referenceNowMs: number,
): WorkspacePanelExternalResolutionEntry[] {
  const entries: Array<{
    sortTs: number;
    tieKey: string;
    entry: WorkspacePanelExternalResolutionEntry;
  }> = [];
  for (const history of jobHistories) {
    const views = projectWorkspaceJobExternalResolutionViews(
      history.external_resolutions,
      history.job.job_id,
      referenceNowMs,
    );
    for (const view of views) {
      entries.push({
        sortTs: view.recordedAtMs ?? history.job.updated_at_ms ?? Number.MIN_SAFE_INTEGER,
        tieKey: `${history.job.source_id}\u0000${history.job.job_id}\u0000${view.receiptId}`,
        entry: {
          view,
          jobId: history.job.job_id,
          jobTitle: history.job.title,
          sourceId: history.job.source_id,
        },
      });
    }
  }
  entries.sort((left, right) => {
    if (right.sortTs !== left.sortTs) {
      return right.sortTs - left.sortTs;
    }
    return left.tieKey.localeCompare(right.tieKey);
  });
  return entries.map((entry) => entry.entry);
}

function renderCoveredAttentionSection(
  items: readonly WorkspaceCoveredAttentionView[],
  heading: string,
) {
  if (items.length === 0) {
    return null;
  }
  return (
    <div className="tl-wp-section-list" data-covered-attention-section="true">
      <h6 className="tl-wp-section-heading">
        {heading} ({items.length})
      </h6>
      <ul className="tl-wp-sublist">
        {items.map((covered) => (
          <li
            key={covered.key}
            className="tl-wp-subitem"
            data-covered-attention-key={covered.key}
          >
            <div className="tl-wp-kv-row">
              <span className="tl-wp-pill" data-tone="neutral">
                <span className="tl-wp-pill-label">{covered.attention.kindLabel}</span>
              </span>
              <span className="tl-wp-mono">
                {covered.taskId ? `Task: ${covered.taskId}` : 'Job-level'}
              </span>
              <span>Covered: {formatTimestampOrFallback(covered.recordedAtMs)}</span>
            </div>
            <span>{covered.attention.reason}</span>
            {covered.attention.nextStep && (
              <span className="tl-wp-state-desc">Next step: {covered.attention.nextStep}</span>
            )}
            <div className="tl-wp-kv-row tl-wp-mono">
              {covered.attention.questionId && (
                <span>Question: {covered.attention.questionId}</span>
              )}
              {covered.attention.blockingHoldIds.length > 0 && (
                <span>Holds: {covered.attention.blockingHoldIds.join(', ')}</span>
              )}
              {covered.attention.blockingOutboxIds.length > 0 && (
                <span>
                  Blocking outbox: {covered.attention.blockingOutboxIds.join(', ')}
                </span>
              )}
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}

function formatExternalResolutionScopeBadge(
  view: WorkspaceExternalResolutionView,
): string {
  if (view.scope === 'job') {
    return view.applicability === 'applicable'
      ? 'Full job'
      : `Full job (${view.applicability})`;
  }
  const taskCount = view.taskIds.length;
  const taskCountLabel = `${taskCount} ${taskCount === 1 ? 'task' : 'tasks'}`;
  return view.applicability === 'applicable'
    ? `Partial · ${taskCountLabel}`
    : `Partial · ${taskCountLabel} (${view.applicability})`;
}

function renderExternalResolutionCard(
  entry: WorkspacePanelExternalResolutionEntry,
  options: {
    onInspectJob?: (scope: ActivityFeedScope) => void;
    onSelectJob?: (scope: ActivityFeedScope) => void;
    onInspectActivity?: (scope: ActivityFeedScope) => void;
    showJobContext?: boolean;
  } = {},
) {
  const res = entry.view;
  const applicabilityTone = res.isApplicable ? 'neutral' : 'attention';
  return (
    <article
      key={`${entry.sourceId}:${entry.jobId}:${res.receiptId}`}
      className="tl-wp-callout"
      data-kind="external-resolution"
      data-external-resolution-receipt-id={res.receiptId}
      data-external-resolution-applicability={res.applicability}
      data-external-resolution-scope={res.scope}
      data-external-resolution-history={res.isApplicable ? undefined : 'true'}
    >
      <div className="tl-wp-callout-title">
        <span>{res.statusLabel}</span>
        <span className="tl-wp-pill" data-tone={applicabilityTone}>
          <span className="tl-wp-pill-label">
            {formatExternalResolutionScopeBadge(res)}
          </span>
        </span>
        {res.isFullJob && (
          <span className="tl-wp-pill" data-tone="neutral">
            <span className="tl-wp-pill-label">Full job</span>
          </span>
        )}
        {res.isPartial && (
          <span className="tl-wp-pill" data-tone="neutral">
            <span className="tl-wp-pill-label">Partial</span>
          </span>
        )}
      </div>
      {options.showJobContext && (
        <span className="tl-wp-state-desc">{entry.jobTitle}</span>
      )}
      <span>{res.summary}</span>
      {(options.onInspectJob || options.onSelectJob || options.onInspectActivity) && (
        <div className="tl-wp-chat-actions">
          {options.onInspectJob && (
            <button
              type="button"
              className="tl-wp-btn tl-wp-btn-subtle"
              onClick={() =>
                options.onInspectJob?.({
                  source_id: entry.sourceId,
                  job_id: entry.jobId,
                })
              }
            >
              View job
            </button>
          )}
          {options.onInspectActivity && (
            <button
              type="button"
              className="tl-wp-btn tl-wp-btn-subtle"
              onClick={() =>
                options.onInspectActivity?.({
                  source_id: entry.sourceId,
                  job_id: entry.jobId,
                })
              }
            >
              View job activity
            </button>
          )}
          {options.onSelectJob && (
            <button
              type="button"
              className="tl-wp-btn tl-wp-btn-subtle"
              onClick={() =>
                options.onSelectJob?.({
                  source_id: entry.sourceId,
                  job_id: entry.jobId,
                })
              }
            >
              Open in Jobs
            </button>
          )}
        </div>
      )}
      <details
        className="tl-wp-message-details"
        data-external-resolution-details={res.receiptId}
      >
        <summary className="tl-wp-message-details-summary">Receipt details</summary>
        <div className="tl-wp-message-details-body">
          <span className="tl-wp-mono">Receipt: {res.receiptId}</span>
          <div className="tl-wp-kv-row tl-wp-mono">
            <span>Job: {entry.jobId}</span>
            {res.requestKey && <span>Request key: {res.requestKey}</span>}
            {res.previewDigest && <span>Preview digest: {res.previewDigest}</span>}
            {res.taskIds.length > 0 && <span>Tasks: {res.taskIds.join(', ')}</span>}
            {res.recordedAtMs !== null && (
              <span>Recorded: {formatTimestampOrFallback(res.recordedAtMs)}</span>
            )}
            <span>{res.ageLabel}</span>
          </div>
          {res.evidenceRefs.length > 0 && (
            <div className="tl-wp-kv-row tl-wp-mono">
              <span>Evidence: {res.evidenceRefs.join(', ')}</span>
            </div>
          )}
          {renderCoveredAttentionSection(
            res.coveredAttention,
            res.isApplicable ? 'Covered attention' : 'Historical covered attention',
          )}
        </div>
      </details>
    </article>
  );
}

export {
  doesJobBelongToChat,
  doesMessageBelongToChat,
  doesObservationRequestMatchChat,
  filterJobsForChat,
  filterMessagesForChat,
  filterObservationRequestsForActivity,
  resolveVerifiedChatJobIds,
};
export type { ObservationChatMatchContext };

function summarizeJobTruncation(truncation: WorkspaceJobHistoryTruncation): string[] {
  const parts: string[] = [];
  if (truncation.tasks_omitted > 0) {
    parts.push(`${truncation.tasks_omitted} tasks omitted`);
  }
  if (truncation.attempts_omitted > 0) {
    parts.push(`${truncation.attempts_omitted} attempts omitted`);
  }
  if (truncation.candidates_omitted > 0) {
    parts.push(`${truncation.candidates_omitted} candidates omitted`);
  }
  if (truncation.runs_omitted > 0) {
    parts.push(`${truncation.runs_omitted} runs omitted`);
  }
  if (truncation.outbox_omitted > 0) {
    parts.push(`${truncation.outbox_omitted} outbox entries omitted`);
  }
  if (truncation.combined_candidates_omitted > 0) {
    parts.push(`${truncation.combined_candidates_omitted} combined candidates omitted`);
  }
  if ((truncation.external_resolutions_omitted ?? 0) > 0) {
    parts.push(
      `${truncation.external_resolutions_omitted} external resolution receipts omitted`,
    );
  }
  return parts;
}

export function getWorkspaceSurfaceTabId(idPrefix: string, tab: WorkspaceChatSurfaceTab): string {
  return `${idPrefix}-tab-${tab}`;
}

export function getWorkspaceSurfacePanelId(idPrefix: string, tab: WorkspaceChatSurfaceTab): string {
  return `${idPrefix}-panel-${tab}`;
}

// ---------------------------------------------------------------------------
// Accessible WAI-ARIA Surface Tabs (`Messages`, `Jobs`, `Activity`)
// ---------------------------------------------------------------------------

export interface WorkspaceSurfaceTabCounts {
  messages?: number | null;
  newMessages?: number | null;
  jobs?: number | null;
  attentionJobs?: number | null;
  activity?: number | null;
  runningActivity?: number | null;
}

export interface WorkspaceSurfaceTabsProps {
  activeTab: WorkspaceChatSurfaceTab;
  onTabChange: (tab: WorkspaceChatSurfaceTab) => void;
  idPrefix: string;
  counts?: WorkspaceSurfaceTabCounts;
  ariaLabel?: string;
}

const SURFACE_TAB_LABELS: Readonly<Record<WorkspaceChatSurfaceTab, string>> = {
  messages: 'Messages',
  jobs: 'Jobs',
  activity: 'Activity',
};

export function WorkspaceSurfaceTabs({
  activeTab,
  onTabChange,
  idPrefix,
  counts,
  ariaLabel = 'Workspace chat surfaces',
}: WorkspaceSurfaceTabsProps) {
  const normalizedActive = normalizeWorkspaceChatTab(activeTab);
  const tabRefs = useRef<Record<WorkspaceChatSurfaceTab, HTMLButtonElement | null>>({
    messages: null,
    jobs: null,
    activity: null,
  });

  const focusAndActivateTab = useCallback(
    (nextTab: WorkspaceChatSurfaceTab) => {
      onTabChange(nextTab);
      tabRefs.current[nextTab]?.focus();
    },
    [onTabChange],
  );

  const handleTabKeyDown = useCallback(
    (event: ReactKeyboardEvent<HTMLButtonElement>, currentTab: WorkspaceChatSurfaceTab) => {
      const currentIdx = WORKSPACE_CHAT_SURFACE_TABS.indexOf(currentTab);
      if (currentIdx < 0) {
        return;
      }
      switch (event.key) {
        case 'ArrowRight': {
          event.preventDefault();
          const nextTab =
            WORKSPACE_CHAT_SURFACE_TABS[(currentIdx + 1) % WORKSPACE_CHAT_SURFACE_TABS.length]!;
          focusAndActivateTab(nextTab);
          return;
        }
        case 'ArrowLeft': {
          event.preventDefault();
          const prevTab =
            WORKSPACE_CHAT_SURFACE_TABS[
              (currentIdx - 1 + WORKSPACE_CHAT_SURFACE_TABS.length) %
                WORKSPACE_CHAT_SURFACE_TABS.length
            ]!;
          focusAndActivateTab(prevTab);
          return;
        }
        case 'Home': {
          event.preventDefault();
          focusAndActivateTab(WORKSPACE_CHAT_SURFACE_TABS[0]!);
          return;
        }
        case 'End': {
          event.preventDefault();
          focusAndActivateTab(
            WORKSPACE_CHAT_SURFACE_TABS[WORKSPACE_CHAT_SURFACE_TABS.length - 1]!,
          );
          return;
        }
        case 'Enter':
        case ' ': {
          event.preventDefault();
          onTabChange(currentTab);
          return;
        }
        default:
          return;
      }
    },
    [focusAndActivateTab, onTabChange],
  );

  const renderTabBadge = (tab: WorkspaceChatSurfaceTab) => {
    if (!counts) {
      return null;
    }
    if (tab === 'messages') {
      if (typeof counts.newMessages === 'number' && counts.newMessages > 0) {
        return (
          <span
            className="tl-wp-tab-count"
            data-tone="active"
            aria-label={`${counts.newMessages} unseen new ${counts.newMessages === 1 ? 'message' : 'messages'}`}
          >
            +{counts.newMessages}
          </span>
        );
      }
      if (typeof counts.messages === 'number' && counts.messages > 0) {
        return <span className="tl-wp-tab-count">{counts.messages}</span>;
      }
      return null;
    }
    if (tab === 'jobs') {
      if (typeof counts.attentionJobs === 'number' && counts.attentionJobs > 0) {
        return (
          <span
            className="tl-wp-tab-count"
            data-tone="attention"
            aria-label={`${counts.attentionJobs} ${counts.attentionJobs === 1 ? 'job needs' : 'jobs need'} attention`}
          >
            {counts.attentionJobs}
          </span>
        );
      }
      if (typeof counts.jobs === 'number' && counts.jobs > 0) {
        return <span className="tl-wp-tab-count">{counts.jobs}</span>;
      }
      return null;
    }
    if (typeof counts.runningActivity === 'number' && counts.runningActivity > 0) {
      return (
        <span
          className="tl-wp-tab-count"
          data-tone="active"
          aria-label={`${counts.runningActivity} running ${counts.runningActivity === 1 ? 'call' : 'calls'}`}
        >
          {counts.runningActivity}
        </span>
      );
    }
    if (typeof counts.activity === 'number' && counts.activity > 0) {
      return <span className="tl-wp-tab-count">{counts.activity}</span>;
    }
    return null;
  };

  return (
    <div className="tl-wp-tabs-bar">
      <div className="tl-wp-tablist" role="tablist" aria-label={ariaLabel} aria-orientation="horizontal">
        {WORKSPACE_CHAT_SURFACE_TABS.map((tab) => {
          const isSelected = tab === normalizedActive;
          return (
            <button
              key={tab}
              ref={(node) => {
                tabRefs.current[tab] = node;
              }}
              id={getWorkspaceSurfaceTabId(idPrefix, tab)}
              type="button"
              role="tab"
              className="tl-wp-tab"
              data-surface-tab={tab}
              aria-selected={isSelected}
              aria-controls={getWorkspaceSurfacePanelId(idPrefix, tab)}
              tabIndex={isSelected ? 0 : -1}
              onClick={() => onTabChange(tab)}
              onKeyDown={(event) => handleTabKeyDown(event, tab)}
            >
              <span>{SURFACE_TAB_LABELS[tab]}</span>
              {renderTabBadge(tab)}
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 1. Messages Panel (`WorkspaceMessagesPanel`)
// ---------------------------------------------------------------------------

export interface WorkspaceMessagesPanelProps {
  chat?: WorkspaceChatSummary | null;
  messages: WorkspaceMessagesWindow | null;
  jobHistories?: readonly WorkspaceJobHistory[] | null;
  bounds?: WorkspaceSnapshotBounds | null;
  knownChatJobIds?: ReadonlySet<string>;
  nowMs?: number | null;
  isActive?: boolean;
  loading?: boolean;
  error?: string | null;
  isStale?: boolean;
  staleReason?: string | null;
  bottomThresholdPx?: number;
  onRefresh?: () => void;
  onInspectJob?: (scope: ActivityFeedScope) => void;
  onPendingNewCountChange?: (count: number) => void;
}

export function WorkspaceMessagesPanel({
  chat = null,
  messages,
  jobHistories = null,
  bounds = null,
  knownChatJobIds,
  nowMs = null,
  isActive = true,
  loading = false,
  error = null,
  isStale = false,
  staleReason = null,
  bottomThresholdPx = DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
  onRefresh,
  onInspectJob,
  onPendingNewCountChange,
}: WorkspaceMessagesPanelProps) {
  const chatKey = resolveMessageScrollChatKey(chat);
  const storeRef = useRef<MessageScrollStore>(createMessageScrollStore(chatKey));
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const listRef = useRef<HTMLUListElement | null>(null);
  const programmaticScrollRef = useRef<{
    active: boolean;
    targetTop: number;
    rafId: number | null;
  }>({
    active: false,
    targetTop: 0,
    rafId: null,
  });
  const lastLayoutSizeRef = useRef<{
    chatKey: string;
    clientHeight: number;
    scrollHeight: number;
  } | null>(null);

  const [uiScroll, setUiScroll] = useState<{
    chatKey: string;
    mode: MessageScrollMode;
    pendingNewCount: number;
  }>(() => ({
    chatKey,
    mode: 'follow',
    pendingNewCount: 0,
  }));

  const syncUiScroll = useCallback((state: MessageScrollState) => {
    setUiScroll((prev) =>
      prev.chatKey === state.chatKey &&
      prev.mode === state.mode &&
      prev.pendingNewCount === state.pendingNewCount
        ? prev
        : {
            chatKey: state.chatKey,
            mode: state.mode,
            pendingNewCount: state.pendingNewCount,
          },
    );
  }, []);

  const scopedItems = useMemo(
    () => (messages ? filterMessagesForChat(messages.items, chat, knownChatJobIds) : []),
    [messages, chat, knownChatJobIds],
  );

  const orderedItems = useMemo(
    () => orderWorkspaceMessageItems(scopedItems),
    [scopedItems],
  );

  const referenceNowMs =
    typeof nowMs === 'number' && Number.isFinite(nowMs) ? nowMs : Date.now();
  const scopedJobHistories = useMemo(
    () =>
      jobHistories
        ? filterJobsForChat(jobHistories, chat, knownChatJobIds)
        : [],
    [jobHistories, chat, knownChatJobIds],
  );
  const externalResolutionEntries = useMemo(
    () =>
      collectWorkspacePanelExternalResolutionEntries(
        scopedJobHistories,
        referenceNowMs,
      ),
    [scopedJobHistories, referenceNowMs],
  );
  const applicableExternalResolutions = externalResolutionEntries.filter(
    (entry) => entry.view.isApplicable,
  );
  const historicalExternalResolutions = externalResolutionEntries.filter(
    (entry) => !entry.view.isApplicable,
  );
  const receiptEntryByReceiptId = useMemo(() => {
    const byReceiptId = new Map<string, WorkspacePanelExternalResolutionEntry>();
    for (const entry of externalResolutionEntries) {
      byReceiptId.set(entry.view.receiptId, entry);
    }
    return byReceiptId;
  }, [externalResolutionEntries]);

  const latestRecordedDayMs = useMemo(() => {
    for (let idx = orderedItems.length - 1; idx >= 0; idx -= 1) {
      const ts = orderedItems[idx]?.source_order.recorded_at_ms;
      if (typeof ts === 'number' && Number.isFinite(ts) && ts > 0) {
        return ts;
      }
    }
    return chat?.updated_at_ms ?? null;
  }, [orderedItems, chat?.updated_at_ms]);

  const applyProgrammaticScrollTop = useCallback((scroller: HTMLDivElement, rawTargetTop: number) => {
    const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    const clampedTarget = Math.min(maxTop, Math.max(0, rawTargetTop));
    if (Math.abs(scroller.scrollTop - clampedTarget) <= 0.5) {
      scroller.scrollTop = clampedTarget;
      return;
    }
    if (programmaticScrollRef.current.rafId !== null && typeof window !== 'undefined') {
      window.cancelAnimationFrame(programmaticScrollRef.current.rafId);
    }
    programmaticScrollRef.current = {
      active: true,
      targetTop: clampedTarget,
      rafId:
        typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function'
          ? window.requestAnimationFrame(() => {
              programmaticScrollRef.current.active = false;
              programmaticScrollRef.current.rafId = null;
            })
          : null,
    };
    scroller.scrollTop = clampedTarget;
  }, []);

  useEffect(() => {
    return () => {
      if (programmaticScrollRef.current.rafId !== null && typeof window !== 'undefined') {
        window.cancelAnimationFrame(programmaticScrollRef.current.rafId);
      }
    };
  }, []);

  // Synchronously reconcile item additions, in-place edits, head-trims, and chat switches before paint.
  useLayoutEffect(() => {
    storeRef.current = switchMessageScrollChat(storeRef.current, chatKey);
    const currentChatState = getActiveChatScrollState(storeRef.current);
    const syncResult = syncMessageScrollItems(currentChatState, orderedItems);
    storeRef.current = setChatScrollState(storeRef.current, syncResult.state);
    syncUiScroll(syncResult.state);

    const scroller = scrollerRef.current;
    if (!scroller || !isActive) {
      return;
    }
    if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
      return;
    }

    if (syncResult.action.kind === 'scroll_to_bottom') {
      const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
      applyProgrammaticScrollTop(scroller, maxTop);
    } else if (syncResult.action.kind === 'preserve_anchor') {
      const { metrics, items: measured } = measureDomMessageViewport(scroller);
      const targetAnchor = syncResult.action.anchor;
      const found = measured.find((entry) => entry.itemId === targetAnchor.itemId);
      if (found) {
        const nextTop = computeAnchorScrollTop(
          metrics,
          found.relativeTopPx,
          targetAnchor.offsetPx,
        );
        applyProgrammaticScrollTop(scroller, nextTop);
      }
    } else if (orderedItems.length === 0 && scroller.scrollTop !== 0) {
      applyProgrammaticScrollTop(scroller, 0);
    }

    lastLayoutSizeRef.current = {
      chatKey,
      clientHeight: scroller.clientHeight,
      scrollHeight: scroller.scrollHeight,
    };
  }, [chatKey, orderedItems, isActive, applyProgrammaticScrollTop, syncUiScroll]);

  const reconcileViewportResize = useCallback(
    (scroller: HTMLDivElement) => {
      if (!isActive) {
        return;
      }
      if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
        return;
      }
      const currentState = getChatScrollState(storeRef.current, chatKey);
      const { metrics, items: measured } = measureDomMessageViewport(scroller);
      const decision = resolveMessageViewportResize(currentState, metrics, measured);
      storeRef.current = setChatScrollState(storeRef.current, decision.state);
      syncUiScroll(decision.state);

      if (decision.action.kind === 'scroll_to_bottom') {
        applyProgrammaticScrollTop(scroller, decision.action.targetScrollTop);
      } else if (decision.action.kind === 'restore_anchor') {
        applyProgrammaticScrollTop(scroller, decision.action.targetScrollTop);
      }

      lastLayoutSizeRef.current = {
        chatKey,
        clientHeight: scroller.clientHeight,
        scrollHeight: scroller.scrollHeight,
      };
    },
    [chatKey, isActive, applyProgrammaticScrollTop, syncUiScroll],
  );

  // Observe both scrollport and inner list resizes (window resize, text wrapping, font load).
  useEffect(() => {
    const scroller = scrollerRef.current;
    if (!scroller || !isActive || typeof ResizeObserver === 'undefined') {
      return;
    }
    const observer = new ResizeObserver(() => {
      const node = scrollerRef.current;
      if (!node) {
        return;
      }
      const prevSize = lastLayoutSizeRef.current;
      if (
        prevSize &&
        prevSize.chatKey === chatKey &&
        prevSize.clientHeight === node.clientHeight &&
        prevSize.scrollHeight === node.scrollHeight
      ) {
        return;
      }
      reconcileViewportResize(node);
    });
    observer.observe(scroller);
    if (listRef.current) {
      observer.observe(listRef.current);
    }
    return () => {
      observer.disconnect();
    };
  }, [chatKey, isActive, orderedItems.length, reconcileViewportResize]);

  const handleScrollerScroll = useCallback(() => {
    const scroller = scrollerRef.current;
    if (!scroller || !isActive) {
      return;
    }
    if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
      return;
    }
    if (
      programmaticScrollRef.current.active &&
      Math.abs(scroller.scrollTop - programmaticScrollRef.current.targetTop) <= 2
    ) {
      programmaticScrollRef.current.active = false;
      if (programmaticScrollRef.current.rafId !== null && typeof window !== 'undefined') {
        window.cancelAnimationFrame(programmaticScrollRef.current.rafId);
        programmaticScrollRef.current.rafId = null;
      }
      lastLayoutSizeRef.current = {
        chatKey,
        clientHeight: scroller.clientHeight,
        scrollHeight: scroller.scrollHeight,
      };
      return;
    }
    programmaticScrollRef.current.active = false;

    const currentState = getChatScrollState(storeRef.current, chatKey);
    const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);

    // If scroll fired due to synchronous bottom-clamping during layout shrink while reading, restore anchor.
    const prevSize = lastLayoutSizeRef.current;
    if (
      currentState.mode === 'reading' &&
      prevSize &&
      prevSize.chatKey === chatKey &&
      (prevSize.clientHeight !== scroller.clientHeight ||
        prevSize.scrollHeight !== scroller.scrollHeight) &&
      scroller.scrollTop >= maxTop - 2
    ) {
      reconcileViewportResize(scroller);
      return;
    }

    const { metrics, items: measured } = measureDomMessageViewport(scroller);
    const nextState = handleMessageViewportScroll(
      currentState,
      metrics,
      measured,
      bottomThresholdPx,
    );
    storeRef.current = setChatScrollState(storeRef.current, nextState);
    syncUiScroll(nextState);
    lastLayoutSizeRef.current = {
      chatKey,
      clientHeight: scroller.clientHeight,
      scrollHeight: scroller.scrollHeight,
    };
  }, [chatKey, isActive, bottomThresholdPx, reconcileViewportResize, syncUiScroll]);

  const handleJumpToLatest = useCallback(() => {
    const currentState = getChatScrollState(storeRef.current, chatKey);
    const jumped = jumpToLatestMessages(currentState);
    storeRef.current = setChatScrollState(storeRef.current, jumped.state);
    syncUiScroll(jumped.state);

    const scroller = scrollerRef.current;
    if (scroller) {
      const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
      applyProgrammaticScrollTop(scroller, maxTop);
      lastLayoutSizeRef.current = {
        chatKey,
        clientHeight: scroller.clientHeight,
        scrollHeight: scroller.scrollHeight,
      };
    }
  }, [chatKey, applyProgrammaticScrollTop, syncUiScroll]);

  const activeScrollView = useMemo(() => {
    if (uiScroll.chatKey === chatKey) {
      return uiScroll;
    }
    const stored = getChatScrollState(storeRef.current, chatKey);
    return {
      chatKey,
      mode: stored.mode,
      pendingNewCount: stored.pendingNewCount,
    };
  }, [uiScroll, chatKey]);

  useEffect(() => {
    onPendingNewCountChange?.(activeScrollView.pendingNewCount);
  }, [activeScrollView.pendingNewCount, onPendingNewCountChange]);

  const windowOmittedCount = Math.max(
    messages?.omitted_count ?? 0,
    bounds?.messages_omitted ?? 0,
  );
  const isBoundedWindow =
    Boolean(messages?.truncated) || Boolean(messages?.has_more) || windowOmittedCount > 0;

  const effectiveAvailability: WorkspaceAvailabilityState =
    messages?.availability ?? chat?.availability ?? 'unavailable';
  const showStaleBanner =
    isStale ||
    effectiveAvailability === 'stale' ||
    chat?.availability === 'stale' ||
    (Boolean(error) && messages !== null);
  const showDisconnectedBanner =
    effectiveAvailability === 'disconnected' || chat?.availability === 'disconnected';

  const hasBanners = showStaleBanner || showDisconnectedBanner || isBoundedWindow;
  const showNewEventsButton =
    activeScrollView.mode === 'reading' && activeScrollView.pendingNewCount > 0;

  return (
    <section
      className="tl-wp-messages-panel"
      data-chat-key={chatKey}
      data-scroll-mode={activeScrollView.mode}
      aria-label="Safe messages"
    >
      {hasBanners && (
        <div className="tl-wp-banners" role="region" aria-label="Messages status notices">
          {showStaleBanner && (
            <div className="tl-wp-banner" data-tone="stale" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Stale messages</span>
                <span className="tl-wp-banner-text">
                  {error
                    ? `Refresh failed (${error}). Showing last recorded messages.`
                    : staleReason ??
                      'Updates are delayed. Showing last recorded messages.'}
                </span>
              </div>
              {onRefresh && (
                <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                  Refresh
                </button>
              )}
            </div>
          )}

          {showDisconnectedBanner && (
            <div className="tl-wp-banner" data-tone="disconnected" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Chat disconnected</span>
                <span className="tl-wp-banner-text">
                  Live chat connection is disconnected. Recorded summaries, questions, and results
                  remain available below.
                </span>
              </div>
            </div>
          )}

          {isBoundedWindow && (
            <div className="tl-wp-banner" data-tone="warning" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Recent recorded messages</span>
                <span className="tl-wp-banner-text">
                  {windowOmittedCount > 0
                    ? `Showing recent messages; ${windowOmittedCount} older records are unavailable in this view.`
                    : 'Showing recent recorded messages; some older history is unavailable in this view.'}
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      <div className="tl-wp-messages-stage">
        <div
          ref={scrollerRef}
          className="tl-wp-messages-scroller"
          data-messages-scroller="true"
          onScroll={handleScrollerScroll}
        >
          {externalResolutionEntries.length > 0 && (
            <div
              className="tl-wp-section-list"
              aria-label="External resolution receipts"
              data-external-resolution-surface="messages"
            >
              <h4 className="tl-wp-section-heading">
                External resolution receipts ({applicableExternalResolutions.length})
              </h4>
              {applicableExternalResolutions.length === 0 ? (
                <span className="tl-wp-state-desc">
                  No applicable external resolution receipts recorded for this chat.
                </span>
              ) : (
                applicableExternalResolutions.map((entry) =>
                  renderExternalResolutionCard(entry, {
                    onInspectJob,
                    showJobContext: true,
                  }),
                )
              )}

              {historicalExternalResolutions.length > 0 && (
                <details
                  className="tl-wp-disclosure"
                  data-external-resolution-surface="messages-history"
                  data-external-resolution-history-disclosure="true"
                >
                  <summary>
                    External resolution history ({historicalExternalResolutions.length})
                  </summary>
                  <div className="tl-wp-disclosure-body">
                    {historicalExternalResolutions.map((entry) =>
                      renderExternalResolutionCard(entry, {
                        onInspectJob,
                        showJobContext: true,
                      }),
                    )}
                  </div>
                </details>
              )}
            </div>
          )}
          {loading && !messages ? (
            <div className="tl-wp-state-card" data-state="loading" role="status">
              <h3 className="tl-wp-state-title">Loading messages…</h3>
              <p className="tl-wp-state-desc">
                Reading recorded summaries, questions, and outcomes.
              </p>
            </div>
          ) : error && !messages ? (
            <div className="tl-wp-state-card" data-state="error" role="alert">
              <h3 className="tl-wp-state-title">Messages unavailable</h3>
              <p className="tl-wp-state-desc">{error}</p>
              {onRefresh && (
                <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                  Retry
                </button>
              )}
            </div>
          ) : !chat && !messages ? (
            <div className="tl-wp-state-card" data-state="empty">
              <h3 className="tl-wp-state-title">No chat selected</h3>
              <p className="tl-wp-state-desc">
                Select a chat to inspect its recorded summaries, results, and questions.
              </p>
            </div>
          ) : effectiveAvailability === 'unavailable' && orderedItems.length === 0 ? (
            <div className="tl-wp-state-card" data-state="unavailable" role="status">
              <h3 className="tl-wp-state-title">Messages unavailable</h3>
              <p className="tl-wp-state-desc">
                Summary, question, and result records are unavailable for this chat. Unrecorded
                transcripts and synthetic turns are never fabricated.
              </p>
            </div>
          ) : orderedItems.length === 0 ? (
            <div className="tl-wp-state-card" data-state="empty" role="status">
              <h3 className="tl-wp-state-title">No messages recorded yet</h3>
              <p className="tl-wp-state-desc">
                Work briefs, plan summaries, questions, answers, review verdicts, and job results
                appear here as they are recorded.
              </p>
            </div>
          ) : (
            <ul ref={listRef} className="tl-wp-messages-list" aria-label="Recorded safe messages">
              {orderedItems.map((item) => {
                const sourceMeta = formatMessageSourceMeta(item.source_kind);
                const SourceIcon = sourceMeta.icon;
                const actorLabel = item.actor_role
                  ? capitalizeFirst(formatExecutorRoleLabel(item.actor_role))
                  : null;
                const conciseTime = formatConciseMessageTime(
                  item.source_order.recorded_at_ms,
                  latestRecordedDayMs,
                );
                const { completeness } = item;
                const conciseWarnings: string[] = [];
                if (completeness.redacted) {
                  conciseWarnings.push('Sensitive metadata redacted');
                }
                if (completeness.summary_truncated) {
                  conciseWarnings.push('Summary shortened');
                }
                if (completeness.incomplete_reason) {
                  conciseWarnings.push(completeness.incomplete_reason);
                }
                const candidateReceiptEntry =
                  receiptEntryByReceiptId.get(item.source_record_id) ?? null;
                const linkedReceiptEntry =
                  candidateReceiptEntry !== null &&
                  (item.scope.job_id === null ||
                    candidateReceiptEntry.jobId === item.scope.job_id)
                    ? candidateReceiptEntry
                    : null;
                return (
                  <li
                    key={item.item_id}
                    className="tl-wp-message-item"
                    data-message-item-id={item.item_id}
                    data-item-id={item.item_id}
                    data-source-kind={item.source_kind}
                    data-category={sourceMeta.category}
                  >
                    <div className="tl-wp-message-meta">
                      <div className="tl-wp-message-meta-left">
                        <span className="tl-wp-message-category" data-tone={sourceMeta.tone}>
                          <SourceIcon size={13} aria-hidden="true" />
                          <span>{sourceMeta.label}</span>
                        </span>
                        {actorLabel && (
                          <>
                            <span className="tl-wp-meta-sep" aria-hidden="true">
                              ·
                            </span>
                            <span className="tl-wp-message-actor">{actorLabel}</span>
                          </>
                        )}
                        <span className="tl-wp-meta-sep" aria-hidden="true">
                          ·
                        </span>
                        <span className="tl-wp-message-time">{conciseTime}</span>
                      </div>

                      {(item.scope.job_id || linkedReceiptEntry) && (
                        <div className="tl-wp-message-meta-right">
                          {linkedReceiptEntry && (
                            <button
                              type="button"
                              className="tl-wp-message-job-btn"
                              data-message-receipt-id={linkedReceiptEntry.view.receiptId}
                              disabled={!onInspectJob}
                              onClick={() => {
                                if (!onInspectJob) {
                                  return;
                                }
                                onInspectJob({
                                  source_id: linkedReceiptEntry.sourceId,
                                  job_id: linkedReceiptEntry.jobId,
                                });
                              }}
                            >
                              Receipt: {linkedReceiptEntry.view.receiptId}
                            </button>
                          )}
                          {item.scope.job_id && onInspectJob && (
                            <button
                              type="button"
                              className="tl-wp-message-job-btn"
                              onClick={() =>
                                onInspectJob({
                                  source_id: item.scope.source_id,
                                  job_id: item.scope.job_id!,
                                })
                              }
                            >
                              View job
                            </button>
                          )}
                        </div>
                      )}
                    </div>

                    <p className="tl-wp-message-summary">{item.summary}</p>

                    {conciseWarnings.length > 0 && (
                      <div className="tl-wp-message-notice" aria-label="Record completeness">
                        <AlertCircle size={12} aria-hidden="true" />
                        <span>{conciseWarnings.join(' · ')}</span>
                      </div>
                    )}

                    <details className="tl-wp-message-details">
                      <summary className="tl-wp-message-details-summary">Details</summary>
                      <div className="tl-wp-message-details-body">
                        <div className="tl-wp-kv-row tl-wp-mono">
                          <span>Record: {item.source_record_id}</span>
                          {item.scope.job_id && <span>Job: {item.scope.job_id}</span>}
                          {item.scope.task_id && <span>Task: {item.scope.task_id}</span>}
                          {item.scope.attempt_no !== null && (
                            <span>Attempt #{item.scope.attempt_no}</span>
                          )}
                        </div>
                        <div className="tl-wp-kv-row">
                          <span>
                            Recorded: {formatTimestampOrFallback(item.source_order.recorded_at_ms)}
                          </span>
                          <span>Seq #{item.source_order.source_seq}</span>
                          {item.actor_role && <span>Role token: {item.actor_role}</span>}
                        </div>
                        {(completeness.summary_truncated ||
                          completeness.missing_fields.length > 0) && (
                          <div className="tl-wp-kv-row">
                            {completeness.summary_truncated && (
                              <span>
                                {completeness.original_char_count !== null
                                  ? `Length: ${completeness.original_char_count} → ${completeness.max_summary_chars} chars`
                                  : `Capped at ${completeness.max_summary_chars} chars`}
                              </span>
                            )}
                            {completeness.missing_fields.length > 0 && (
                              <span>
                                Unrecorded fields: {completeness.missing_fields.join(', ')}
                              </span>
                            )}
                          </div>
                        )}
                      </div>
                    </details>
                  </li>
                );
              })}
            </ul>
          )}
        </div>

        {showNewEventsButton && (
          <div className="tl-wp-new-events-layer">
            <button
              type="button"
              className="tl-wp-new-events-btn"
              data-new-events-button="true"
              onClick={handleJumpToLatest}
            >
              <span>
                {activeScrollView.pendingNewCount === 1
                  ? '1 new message — Jump to latest'
                  : `${activeScrollView.pendingNewCount} new messages — Jump to latest`}
              </span>
              <span aria-hidden="true">↓</span>
            </button>
          </div>
        )}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// 2. Jobs Panel (`WorkspaceJobsPanel`)
// ---------------------------------------------------------------------------

export interface WorkspaceJobsPanelProps {
  chat?: WorkspaceChatSummary | null;
  jobHistories: readonly WorkspaceJobHistory[] | null;
  knownChatJobIds?: ReadonlySet<string>;
  bounds?: WorkspaceSnapshotBounds | null;
  jobsHasMore?: boolean;
  focusedJobId?: string | null;
  nowMs?: number | null;
  loading?: boolean;
  error?: string | null;
  isStale?: boolean;
  staleReason?: string | null;
  onRefresh?: () => void;
  onInspectActivityForJob?: (scope: ActivityFeedScope) => void;
}

function renderAcceptanceProofBlock(proof: WorkspaceAcceptanceProof, heading: string) {
  const meta = formatAcceptanceStatusMeta(proof.status);
  const receiptItems = [
    proof.accepted_candidate_id ? `Candidate: ${proof.accepted_candidate_id}` : null,
    proof.combined_candidate_id ? `Combined: ${proof.combined_candidate_id}` : null,
    proof.review_verdict_id ? `Review verdict: ${proof.review_verdict_id}` : null,
    proof.review_receipt_id ? `Review receipt: ${proof.review_receipt_id}` : null,
    proof.integration_receipt_id ? `Integration receipt: ${proof.integration_receipt_id}` : null,
    proof.closure_reservation_id ? `Closure reservation: ${proof.closure_reservation_id}` : null,
    proof.closure_verdict_id ? `Closure verdict: ${proof.closure_verdict_id}` : null,
    proof.verified_at_ms !== null
      ? `Verified at: ${formatTimestampOrFallback(proof.verified_at_ms)}`
      : null,
  ].filter((entry): entry is string => entry !== null);

  return (
    <div className="tl-wp-callout" data-kind="acceptance">
      <div className="tl-wp-callout-title">
        <span>{heading}</span>
        <span className="tl-wp-pill" data-tone={meta.tone}>
          <span className="tl-wp-pill-label">{meta.label}</span>
        </span>
      </div>
      <span>{meta.explanation}</span>
      {receiptItems.length > 0 && (
        <div className="tl-wp-kv-row tl-wp-mono">
          {receiptItems.map((item) => (
            <span key={item}>{item}</span>
          ))}
        </div>
      )}
    </div>
  );
}

function renderAttentionCallout(attention: WorkspaceAttentionState) {
  if (attention.kind === 'none') {
    return null;
  }
  return (
    <div className="tl-wp-callout" data-kind="attention" role="status">
      <div className="tl-wp-callout-title">
        <span>Active attention</span>
        <span className="tl-wp-pill" data-tone="attention">
          <span className="tl-wp-pill-label">{formatAttentionKindLabel(attention.kind)}</span>
        </span>
      </div>
      <span>{attention.reason}</span>
      {attention.next_step && <span>Next step: {attention.next_step}</span>}
      <div className="tl-wp-kv-row tl-wp-mono">
        {attention.question_id && <span>Question: {attention.question_id}</span>}
        {attention.blocking_hold_ids.length > 0 && (
          <span>Holds: {attention.blocking_hold_ids.join(', ')}</span>
        )}
        {attention.blocking_outbox_ids.length > 0 && (
          <span>Blocking outbox: {attention.blocking_outbox_ids.join(', ')}</span>
        )}
        {attention.updated_at_ms !== null && (
          <span>Updated: {formatTimestampOrFallback(attention.updated_at_ms)}</span>
        )}
      </div>
    </div>
  );
}

function renderHistoricalFailureCallout(failure: WorkspaceHistoricalFailure) {
  return (
    <div
      className="tl-wp-callout"
      data-kind={failure.superseded_or_resolved ? 'historical-superseded' : 'historical-active'}
    >
      <div className="tl-wp-callout-title">
        <span>
          {failure.superseded_or_resolved
            ? 'Historical failure (superseded or resolved)'
            : 'Recorded failure'}
        </span>
        <span
          className="tl-wp-pill"
          data-tone={failure.superseded_or_resolved ? 'neutral' : 'danger'}
        >
          <span className="tl-wp-pill-label">Stage: {failure.stage}</span>
        </span>
      </div>
      <span>{failure.reason}</span>
      <div className="tl-wp-kv-row tl-wp-mono">
        {failure.failure_id && <span>Failure: {failure.failure_id}</span>}
        {failure.task_id && <span>Task: {failure.task_id}</span>}
        {failure.attempt_no !== null && <span>Attempt #{failure.attempt_no}</span>}
        {failure.run_id && <span>Run: {failure.run_id}</span>}
        {failure.outbox_id && <span>Outbox: {failure.outbox_id}</span>}
        {failure.failed_at_ms !== null && (
          <span>Failed: {formatTimestampOrFallback(failure.failed_at_ms)}</span>
        )}
      </div>
    </div>
  );
}

function renderExecutorMeta(executor: ObservationExecutor | null): string {
  if (!executor || !hasExecutorRecord(executor)) {
    return 'Executor not recorded';
  }
  const identityParts = [
    executor.role ? formatExecutorRoleLabel(executor.role) : null,
    executor.tool,
    executor.provider,
  ].filter((part): part is string => Boolean(part));
  const modelText = formatRequestedObserved(executor.requested_model, executor.observed_model);
  const effortText = formatRequestedObserved(executor.requested_effort, executor.observed_effort);
  return `${identityParts.join(' · ') || 'Executor'} — Model (${modelText}) · Effort (${effortText})`;
}

function renderJobResultBlock(result: WorkspaceJobResult) {
  const deliveryDesc = describeDeliveryStage(result.delivery_stage as ObservationDeliveryStage);
  return (
    <div className="tl-wp-callout" data-kind="acceptance">
      <div className="tl-wp-callout-title">
        <span>Recorded job result</span>
        <span className="tl-wp-pill" data-tone="ok">
          <span className="tl-wp-pill-label">Outcome: {result.outcome}</span>
        </span>
        <span className="tl-wp-pill" data-tone="neutral">
          <span className="tl-wp-pill-label">{deliveryDesc.label}</span>
        </span>
      </div>
      <p className="tl-wp-job-summary">{result.summary}</p>
      <div className="tl-wp-kv-row">
        <span>{deliveryDesc.note}</span>
        {result.closure_verdict_id && (
          <span className="tl-wp-mono">Closure verdict: {result.closure_verdict_id}</span>
        )}
        {result.produced_at_ms !== null && (
          <span>Produced: {formatTimestampOrFallback(result.produced_at_ms)}</span>
        )}
      </div>
    </div>
  );
}

function renderTaskAttemptItem(attempt: WorkspaceTaskAttempt) {
  return (
    <li key={`attempt-${attempt.attempt_no}-${attempt.run_id ?? 'none'}`} className="tl-wp-subitem">
      <div className="tl-wp-kv-row">
        <strong>Attempt #{attempt.attempt_no}</strong>
        <span className="tl-wp-pill" data-tone="neutral">
          <span className="tl-wp-pill-label">{attempt.status}</span>
        </span>
        {attempt.worker_id && <span className="tl-wp-mono">Worker: {attempt.worker_id}</span>}
        {attempt.run_id && <span className="tl-wp-mono">Run: {attempt.run_id}</span>}
        {attempt.outbox_id && <span className="tl-wp-mono">Outbox: {attempt.outbox_id}</span>}
      </div>
      <div className="tl-wp-kv-row">
        <span>{renderExecutorMeta(attempt.executor)}</span>
        <span>Started: {formatTimestampOrFallback(attempt.started_at_ms, 'Not reported')}</span>
        <span>Finished: {formatTimestampOrFallback(attempt.finished_at_ms, 'Not reported')}</span>
      </div>
      {attempt.summary && <p className="tl-wp-job-summary">{attempt.summary}</p>}
    </li>
  );
}

function renderTaskCandidateItem(candidate: WorkspaceTaskCandidate, task: WorkspaceTaskHistory) {
  const isAccepted =
    task.accepted_candidate_id !== null && task.accepted_candidate_id === candidate.candidate_id;
  const isActive =
    task.active_candidate_id !== null && task.active_candidate_id === candidate.candidate_id;
  const commitShort = compactSha(candidate.head_commit_sha);
  const treeShort = compactSha(candidate.tree_sha);
  const verificationLabel =
    candidate.verification_passed === true
      ? 'Verification passed'
      : candidate.verification_passed === false
        ? 'Verification failed'
        : 'Verification not reported';

  return (
    <li key={candidate.candidate_id} className="tl-wp-subitem">
      <div className="tl-wp-kv-row">
        <strong className="tl-wp-mono">{candidate.candidate_id}</strong>
        <span>Attempt #{candidate.attempt_no}</span>
        <span className="tl-wp-pill" data-tone={isAccepted ? 'verified' : 'neutral'}>
          <span className="tl-wp-pill-label">Review: {candidate.review_status}</span>
        </span>
        {isAccepted && (
          <span className="tl-wp-pill" data-tone="verified">
            <span className="tl-wp-pill-label">Accepted candidate</span>
          </span>
        )}
        {isActive && !isAccepted && (
          <span className="tl-wp-pill" data-tone="active">
            <span className="tl-wp-pill-label">Active candidate</span>
          </span>
        )}
        <span>{verificationLabel}</span>
      </div>
      <div className="tl-wp-kv-row tl-wp-mono">
        {commitShort && <span>Commit: {commitShort}</span>}
        {treeShort && <span>Tree: {treeShort}</span>}
        {candidate.worker_id && <span>Worker: {candidate.worker_id}</span>}
        {candidate.review_verdict_id && <span>Verdict: {candidate.review_verdict_id}</span>}
        {candidate.review_receipt_id && <span>Receipt: {candidate.review_receipt_id}</span>}
        {candidate.produced_at_ms !== null && (
          <span>Produced: {formatTimestampOrFallback(candidate.produced_at_ms)}</span>
        )}
      </div>
      {candidate.summary && <p className="tl-wp-job-summary">{candidate.summary}</p>}
    </li>
  );
}

function renderTaskHistoryCard(
  task: WorkspaceTaskHistory,
  jobExternalResolutions: readonly WorkspaceExternalResolutionView[],
  referenceNowMs: number,
) {
  const proofMeta = formatAcceptanceStatusMeta(task.acceptance_proof.status);
  const externalState = projectWorkspaceTaskExternalResolutionState(
    task,
    jobExternalResolutions,
    referenceNowMs,
  );
  return (
    <div key={task.task_id} className="tl-wp-task-card" data-task-id={task.task_id}>
      <div className="tl-wp-job-header">
        <div className="tl-wp-job-title-group">
          <h5 className="tl-wp-job-title">{task.title}</h5>
          <div className="tl-wp-job-subline">
            <span className="tl-wp-mono">{task.task_id}</span>
            {task.role && <span>Role: {formatExecutorRoleLabel(task.role)}</span>}
            {task.difficulty && <span>Difficulty: {task.difficulty}</span>}
            {task.stage && <span>Stage: {task.stage}</span>}
            <span>Current attempt: #{task.current_attempt_no}</span>
            {task.depends_on.length > 0 && (
              <span className="tl-wp-mono">Depends on: {task.depends_on.join(', ')}</span>
            )}
          </div>
        </div>
        <div className="tl-wp-chat-actions">
          <span className="tl-wp-pill" data-tone="neutral">
            <span className="tl-wp-pill-label">Status: {task.status}</span>
          </span>
          <span className="tl-wp-pill" data-tone={proofMeta.tone}>
            <span className="tl-wp-pill-label">{proofMeta.label}</span>
          </span>
          {externalState.label && (
            <span
              className="tl-wp-pill"
              data-tone="neutral"
              data-task-external-resolution={externalState.label}
            >
              <span className="tl-wp-pill-label">{externalState.label}</span>
            </span>
          )}
        </div>
      </div>

      {task.current_attention && renderAttentionCallout(task.current_attention)}
      {task.historical_failure && renderHistoricalFailureCallout(task.historical_failure)}

      {externalState.coveredAttention.length > 0 &&
        renderCoveredAttentionSection(externalState.coveredAttention, 'Covered attention')}

      {externalState.applicable.length > 0 && (
        <div className="tl-wp-kv-row tl-wp-mono">
          <span>
            External receipts: {externalState.applicable.map((res) => res.receiptId).join(', ')}
          </span>
        </div>
      )}

      {externalState.historical.length > 0 && (
        <div className="tl-wp-kv-row">
          <span>
            External receipt history:{' '}
            {externalState.historical
              .map((res) => `${res.statusLabel} (${res.receiptId})`)
              .join(' · ')}
          </span>
        </div>
      )}

      {task.attempts.length > 0 && (
        <div className="tl-wp-section-list">
          <h6 className="tl-wp-section-heading">Attempts ({task.attempts.length})</h6>
          <ul className="tl-wp-sublist">{task.attempts.map(renderTaskAttemptItem)}</ul>
        </div>
      )}

      {task.candidates.length > 0 && (
        <div className="tl-wp-section-list">
          <h6 className="tl-wp-section-heading">Candidates ({task.candidates.length})</h6>
          <ul className="tl-wp-sublist">
            {task.candidates.map((cand) => renderTaskCandidateItem(cand, task))}
          </ul>
        </div>
      )}
    </div>
  );
}

function renderCombinedCandidateItem(item: WorkspaceCombinedCandidate) {
  return (
    <li key={item.combined_candidate_id} className="tl-wp-subitem">
      <div className="tl-wp-kv-row">
        <strong className="tl-wp-mono">{item.combined_candidate_id}</strong>
        <span className="tl-wp-pill" data-tone="neutral">
          <span className="tl-wp-pill-label">{item.status}</span>
        </span>
        {item.job_revision !== null && <span>Job rev #{item.job_revision}</span>}
        <span>Checks: {item.check_count}</span>
      </div>
      <div className="tl-wp-kv-row tl-wp-mono">
        {item.final_commit_sha && <span>Commit: {compactSha(item.final_commit_sha)}</span>}
        {item.final_tree_sha && <span>Tree: {compactSha(item.final_tree_sha)}</span>}
        {item.candidate_digest && <span>Digest: {item.candidate_digest}</span>}
        {item.closure_reservation_id && (
          <span>Closure reservation: {item.closure_reservation_id}</span>
        )}
        {item.produced_at_ms !== null && (
          <span>Produced: {formatTimestampOrFallback(item.produced_at_ms)}</span>
        )}
      </div>
    </li>
  );
}

function renderRunItem(run: WorkspaceRunRecord) {
  return (
    <li key={run.run_id} className="tl-wp-subitem">
      <div className="tl-wp-kv-row">
        <strong className="tl-wp-mono">{run.run_id}</strong>
        <span className="tl-wp-pill" data-tone="neutral">
          <span className="tl-wp-pill-label">{run.status}</span>
        </span>
        {run.role && <span>Role: {formatExecutorRoleLabel(run.role)}</span>}
        {run.task_id && <span className="tl-wp-mono">Task: {run.task_id}</span>}
        {run.attempt_no !== null && <span>Attempt #{run.attempt_no}</span>}
        {run.worker_id && <span className="tl-wp-mono">Worker: {run.worker_id}</span>}
      </div>
      <div className="tl-wp-kv-row">
        <span>{renderExecutorMeta(run.executor)}</span>
        <span>Updated: {formatTimestampOrFallback(run.updated_at_ms ?? run.started_at_ms)}</span>
      </div>
    </li>
  );
}

function renderOutboxItem(entry: WorkspaceOutboxRecord) {
  return (
    <li key={entry.outbox_id} className="tl-wp-subitem">
      <div className="tl-wp-kv-row">
        <strong className="tl-wp-mono">{entry.outbox_id}</strong>
        <span>Kind: {entry.kind}</span>
        <span className="tl-wp-pill" data-tone="neutral">
          <span className="tl-wp-pill-label">{entry.status}</span>
        </span>
        {entry.receipt_id && <span className="tl-wp-mono">Receipt: {entry.receipt_id}</span>}
        {entry.request_key && <span className="tl-wp-mono">Key: {entry.request_key}</span>}
      </div>
      <div className="tl-wp-kv-row tl-wp-mono">
        {entry.task_id && <span>Task: {entry.task_id}</span>}
        {entry.attempt_no !== null && <span>Attempt #{entry.attempt_no}</span>}
        {entry.run_id && <span>Run: {entry.run_id}</span>}
        {entry.reserved_at_job_revision !== null && (
          <span>Reserved rev #{entry.reserved_at_job_revision}</span>
        )}
        {entry.updated_at_job_revision !== null && (
          <span>Updated rev #{entry.updated_at_job_revision}</span>
        )}
      </div>
      {entry.diagnostic && <p className="tl-wp-job-summary">{entry.diagnostic}</p>}
    </li>
  );
}

function collectRecordLinkItems(links: WorkspaceRecordLinks): string[] {
  const items: string[] = [];
  if (links.source_plan_id) {
    items.push(
      links.source_plan_revision !== null
        ? `Plan: ${links.source_plan_id} (rev ${links.source_plan_revision})`
        : `Plan: ${links.source_plan_id}`,
    );
  }
  if (links.source_approval_id) {
    items.push(`Approval: ${links.source_approval_id}`);
  }
  if (links.source_work_brief_id) {
    items.push(`Work brief: ${links.source_work_brief_id}`);
  }
  if (links.planning_grant_id) {
    items.push(`Planning grant: ${links.planning_grant_id}`);
  }
  if (links.planning_reservation_id) {
    items.push(`Planning reservation: ${links.planning_reservation_id}`);
  }
  if (links.parent_job_id) {
    items.push(`Parent job: ${links.parent_job_id}`);
  }
  if (links.supersedes_job_id) {
    items.push(`Supersedes: ${links.supersedes_job_id}`);
  }
  if (links.superseded_by_job_id) {
    items.push(`Superseded by: ${links.superseded_by_job_id}`);
  }
  if (links.follow_up_job_ids.length > 0) {
    items.push(`Follow-up jobs: ${links.follow_up_job_ids.join(', ')}`);
  }
  if (links.architect_advice_ids.length > 0) {
    items.push(`Architect advice: ${links.architect_advice_ids.join(', ')}`);
  }
  if (links.source_request_ids.length > 0) {
    items.push(`Source requests: ${links.source_request_ids.join(', ')}`);
  }
  return items;
}

export function WorkspaceJobsPanel({
  chat = null,
  jobHistories,
  knownChatJobIds,
  bounds = null,
  jobsHasMore = false,
  focusedJobId = null,
  nowMs = null,
  loading = false,
  error = null,
  isStale = false,
  staleReason = null,
  onRefresh,
  onInspectActivityForJob,
}: WorkspaceJobsPanelProps) {
  const [expandedJobKeys, setExpandedJobKeys] = useState<ReadonlySet<string>>(
    () => new Set<string>(),
  );

  const chatKey = resolveMessageScrollChatKey(chat);
  useEffect(() => {
    setExpandedJobKeys(new Set<string>());
  }, [chatKey]);

  const filteredJobs = useMemo(
    () => (jobHistories ? filterJobsForChat(jobHistories, chat, knownChatJobIds) : []),
    [jobHistories, chat, knownChatJobIds],
  );

  const referenceNowMs =
    typeof nowMs === 'number' && Number.isFinite(nowMs) ? nowMs : Date.now();
  const externalResolutionsByJobKey = useMemo(() => {
    const byJobKey = new Map<string, WorkspaceExternalResolutionView[]>();
    for (const history of filteredJobs) {
      byJobKey.set(
        `${history.job.source_id}:${history.job.job_id}`,
        projectWorkspaceJobExternalResolutionViews(
          history.external_resolutions,
          history.job.job_id,
          referenceNowMs,
        ),
      );
    }
    return byJobKey;
  }, [filteredJobs, referenceNowMs]);

  useEffect(() => {
    if (!focusedJobId) {
      return;
    }
    const match = filteredJobs.find((entry) => entry.job.job_id === focusedJobId);
    if (!match) {
      return;
    }
    const targetKey = `${match.job.source_id}:${match.job.job_id}`;
    setExpandedJobKeys((prev) => {
      if (prev.has(targetKey)) {
        return prev;
      }
      const next = new Set(prev);
      next.add(targetKey);
      return next;
    });
  }, [focusedJobId, filteredJobs]);

  const toggleJobExpanded = useCallback((jobKey: string) => {
    setExpandedJobKeys((prev) => {
      const next = new Set(prev);
      if (next.has(jobKey)) {
        next.delete(jobKey);
      } else {
        next.add(jobKey);
      }
      return next;
    });
  }, []);

  const omittedJobsCount = bounds?.job_histories_omitted ?? 0;
  const showBoundedJobsBanner =
    omittedJobsCount > 0 || Boolean(bounds?.jobs_capped) || jobsHasMore;
  const showStaleBanner =
    isStale || chat?.availability === 'stale' || (Boolean(error) && jobHistories !== null);
  const showDisconnectedBanner = chat?.availability === 'disconnected';
  const suppressLiveSpinner = showStaleBanner || Boolean(error);

  return (
    <section className="tl-wp-jobs-panel" aria-label="Durable job history">
      {(showStaleBanner || showDisconnectedBanner || showBoundedJobsBanner) && (
        <div className="tl-wp-banners" role="region" aria-label="Job history notices">
          {showStaleBanner && (
            <div className="tl-wp-banner" data-tone="stale" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Stale job history</span>
                <span className="tl-wp-banner-text">
                  {error
                    ? `Refresh failed (${error}). Showing last recorded job history.`
                    : staleReason ?? 'Updates are delayed. Showing last recorded job history.'}
                </span>
              </div>
              {onRefresh && (
                <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                  Refresh
                </button>
              )}
            </div>
          )}

          {showDisconnectedBanner && (
            <div className="tl-wp-banner" data-tone="disconnected" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Chat disconnected</span>
                <span className="tl-wp-banner-text">
                  Live chat connection is disconnected. Recorded job history remains available
                  below.
                </span>
              </div>
            </div>
          )}

          {showBoundedJobsBanner && (
            <div className="tl-wp-banner" data-tone="warning" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Recent job history</span>
                <span className="tl-wp-banner-text">
                  {omittedJobsCount > 0
                    ? `Showing recent jobs; ${omittedJobsCount} older jobs are unavailable in this view.`
                    : 'Showing recent recorded jobs; some older job history is unavailable in this view.'}
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      <div className="tl-wp-jobs-scroller">
        {loading && !jobHistories ? (
          <div className="tl-wp-state-card" data-state="loading" role="status">
            <h3 className="tl-wp-state-title">Loading job history…</h3>
            <p className="tl-wp-state-desc">
              Reading recorded jobs, attempts, candidates, and acceptance status.
            </p>
          </div>
        ) : error && !jobHistories ? (
          <div className="tl-wp-state-card" data-state="error" role="alert">
            <h3 className="tl-wp-state-title">Job history unavailable</h3>
            <p className="tl-wp-state-desc">{error}</p>
            {onRefresh && (
              <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                Retry
              </button>
            )}
          </div>
        ) : !chat && !jobHistories ? (
          <div className="tl-wp-state-card" data-state="empty">
            <h3 className="tl-wp-state-title">No chat selected</h3>
            <p className="tl-wp-state-desc">
              Select a chat to inspect its jobs and acceptance status.
            </p>
          </div>
        ) : filteredJobs.length === 0 ? (
          <div className="tl-wp-state-card" data-state="empty" role="status">
            <h3 className="tl-wp-state-title">No jobs recorded</h3>
            <p className="tl-wp-state-desc">
              {chat
                ? 'No jobs have been recorded for this chat yet.'
                : 'No jobs have been recorded yet.'}
            </p>
          </div>
        ) : (
          filteredJobs.map((history) => {
            const { job } = history;
            const jobKey = `${job.source_id}:${job.job_id}`;
            const safeDomId = jobKey.replace(/[^A-Za-z0-9_-]/g, '_');
            const detailId = `tl-wp-job-detail-${safeDomId}`;
            const isExpanded = expandedJobKeys.has(jobKey);
            const stateMeta = formatWorkspaceJobHistoryStateMeta(history);
            const acceptanceMeta = formatAcceptanceStatusMeta(history.acceptance_proof.status);
            const truncationNotes = summarizeJobTruncation(history.truncation);
            const linkItems = collectRecordLinkItems(history.links);
            const isFocused = focusedJobId !== null && focusedJobId === job.job_id;
            const activeAttention =
              history.current_attention && history.current_attention.kind !== 'none'
                ? history.current_attention
                : null;
            const externalResolutions = externalResolutionsByJobKey.get(jobKey) ?? [];
            const applicableExternalResolutions = externalResolutions.filter(
              (res) => res.isApplicable,
            );
            const historicalExternalResolutions = externalResolutions.filter(
              (res) => !res.isApplicable,
            );
            const hasApplicableFullExternalResolution =
              applicableExternalResolutions.some((res) => res.scope === 'job');
            const hasApplicablePartialExternalResolution =
              !hasApplicableFullExternalResolution &&
              applicableExternalResolutions.some((res) => res.scope === 'tasks');
            const hasUnknownJobOrTaskEffect =
              (job.state === 'unknown' &&
                job.execution_status.trim().toLowerCase() !== 'resolved_externally') ||
              history.tasks.some(
                (task) =>
                  task.status.trim().toLowerCase() === 'unknown' ||
                  (task.current_attempt_no > 0 &&
                    task.attempts.some(
                      (attempt) =>
                        attempt.attempt_no === task.current_attempt_no &&
                        attempt.status.trim().toLowerCase() === 'unknown',
                    )),
              );
            const hasAnyCurrentAttention =
              activeAttention !== null ||
              history.tasks.some(
                (task) =>
                  task.current_attention !== null &&
                  task.current_attention.kind !== 'none',
              );
            const isActiveJobState =
              job.state === 'running' ||
              job.state === 'reviewing' ||
              job.state === 'planning' ||
              job.state === 'needs_user';
            const isVerifiedAcceptedJob =
              history.acceptance_proof.status === 'verified_accepted';
            const showExternalResolutionHeaderPill =
              !hasUnknownJobOrTaskEffect &&
              !hasAnyCurrentAttention &&
              !isActiveJobState &&
              !isVerifiedAcceptedJob;
            const showJobSpinner = job.state === 'running' && !suppressLiveSpinner;

            return (
              <article
                key={jobKey}
                className="tl-wp-job-card"
                data-job-id={job.job_id}
                data-expanded={isExpanded ? 'true' : 'false'}
                data-focused={isFocused ? 'true' : undefined}
              >
                <button
                  type="button"
                  className="tl-wp-job-row-btn"
                  aria-expanded={isExpanded}
                  aria-controls={detailId}
                  onClick={() => toggleJobExpanded(jobKey)}
                >
                  <div className="tl-wp-activity-row-top">
                    <div className="tl-wp-activity-row-lead">
                      <span className="tl-wp-pill" data-tone={stateMeta.tone}>
                        {showJobSpinner && <ActivitySpinner />}
                        <span className="tl-wp-pill-label">{stateMeta.label}</span>
                      </span>
                      <span className="tl-wp-pill" data-tone={acceptanceMeta.tone}>
                        <span className="tl-wp-pill-label">{acceptanceMeta.label}</span>
                      </span>
                      {activeAttention && (
                        <span className="tl-wp-pill" data-tone="attention">
                          <span className="tl-wp-pill-label">
                            {formatAttentionKindLabel(activeAttention.kind)}
                          </span>
                        </span>
                      )}
                      {showExternalResolutionHeaderPill &&
                        hasApplicableFullExternalResolution && (
                        <span
                          className="tl-wp-pill"
                          data-tone="neutral"
                          data-job-external-resolution="Resolved externally"
                        >
                          <span className="tl-wp-pill-label">Resolved externally</span>
                        </span>
                      )}
                      {showExternalResolutionHeaderPill &&
                        hasApplicablePartialExternalResolution && (
                        <span
                          className="tl-wp-pill"
                          data-tone="neutral"
                          data-job-external-resolution="Partially resolved externally"
                        >
                          <span className="tl-wp-pill-label">
                            Partially resolved externally
                          </span>
                        </span>
                      )}
                      <span className="tl-wp-job-title">{job.title}</span>
                    </div>

                    <div className="tl-wp-activity-row-metrics">
                      <span>
                        {history.tasks.length === 1
                          ? '1 task'
                          : `${history.tasks.length} tasks`}
                      </span>
                      <span>Updated {formatTimestampOrFallback(job.updated_at_ms)}</span>
                      <span>{isExpanded ? 'Hide details' : 'Details'}</span>
                    </div>
                  </div>

                  {!isExpanded && job.summary && (
                    <p className="tl-wp-job-summary">{job.summary}</p>
                  )}
                </button>

                {isExpanded && (
                  <div
                    id={detailId}
                    className="tl-wp-job-detail"
                    role="region"
                    aria-label={`Details for ${job.title}`}
                  >
                    <div className="tl-wp-job-header">
                      <div className="tl-wp-job-subline">
                        <span className="tl-wp-mono">{job.job_id}</span>
                        <span>Source: {job.source_id}</span>
                        {job.job_revision !== null && <span>Revision #{job.job_revision}</span>}
                        {job.phase && <span>Phase: {job.phase}</span>}
                        <span>Approval: {job.approval_status}</span>
                        <span>Execution: {job.execution_status}</span>
                        {job.review_state && <span>Review: {job.review_state}</span>}
                      </div>

                      {onInspectActivityForJob && (
                        <div className="tl-wp-chat-actions">
                          <button
                            type="button"
                            className="tl-wp-btn tl-wp-btn-subtle"
                            onClick={() =>
                              onInspectActivityForJob({
                                source_id: job.source_id,
                                job_id: job.job_id,
                              })
                            }
                          >
                            View job activity
                          </button>
                        </div>
                      )}
                    </div>

                    {job.summary && <p className="tl-wp-job-summary">{job.summary}</p>}

                    {truncationNotes.length > 0 && (
                      <div className="tl-wp-banner" data-tone="warning" role="status">
                        <div className="tl-wp-banner-body">
                          <span className="tl-wp-banner-title">Some older sub-records omitted</span>
                          <span className="tl-wp-banner-text">
                            {truncationNotes.join(' · ')}.
                          </span>
                        </div>
                      </div>
                    )}

                    {history.current_attention && renderAttentionCallout(history.current_attention)}
                    {history.historical_failure &&
                      renderHistoricalFailureCallout(history.historical_failure)}

                    {applicableExternalResolutions.length > 0 && (
                      <div
                        className="tl-wp-section-list"
                        aria-label="External resolution receipts"
                        data-external-resolution-surface="jobs"
                      >
                        <h5 className="tl-wp-section-heading">
                          External resolution receipts ({applicableExternalResolutions.length})
                        </h5>
                        {applicableExternalResolutions.map((res) =>
                          renderExternalResolutionCard(
                            {
                              view: res,
                              jobId: job.job_id,
                              jobTitle: job.title,
                              sourceId: job.source_id,
                            },
                            {
                              onInspectActivity: onInspectActivityForJob,
                            },
                          ),
                        )}
                      </div>
                    )}

                    {historicalExternalResolutions.length > 0 && (
                      <details
                        className="tl-wp-disclosure"
                        aria-label="External resolution history"
                        data-external-resolution-surface="jobs-history"
                        data-external-resolution-history-disclosure="true"
                      >
                        <summary>
                          External resolution history ({historicalExternalResolutions.length})
                        </summary>
                        <div className="tl-wp-disclosure-body">
                          {historicalExternalResolutions.map((res) =>
                            renderExternalResolutionCard(
                              {
                                view: res,
                                jobId: job.job_id,
                                jobTitle: job.title,
                                sourceId: job.source_id,
                              },
                              {
                                onInspectActivity: onInspectActivityForJob,
                              },
                            ),
                          )}
                        </div>
                      </details>
                    )}

                    {renderAcceptanceProofBlock(history.acceptance_proof, 'Job acceptance proof')}

                    {history.result && renderJobResultBlock(history.result)}

                    <div className="tl-wp-section-list">
                      <h5 className="tl-wp-section-heading">Tasks ({history.tasks.length})</h5>
                      {history.tasks.length === 0 ? (
                        <span className="tl-wp-state-desc">
                          No task records reported for this job.
                        </span>
                      ) : (
                        history.tasks.map((task) =>
                          renderTaskHistoryCard(
                            task,
                            externalResolutions,
                            referenceNowMs,
                          ),
                        )
                      )}
                    </div>

                    {history.combined_candidates.length > 0 && (
                      <details className="tl-wp-disclosure">
                        <summary>
                          Combined candidates ({history.combined_candidates.length})
                        </summary>
                        <div className="tl-wp-disclosure-body">
                          <ul className="tl-wp-sublist">
                            {history.combined_candidates.map(renderCombinedCandidateItem)}
                          </ul>
                        </div>
                      </details>
                    )}

                    {history.runs.length > 0 && (
                      <details className="tl-wp-disclosure">
                        <summary>Worker runs ({history.runs.length})</summary>
                        <div className="tl-wp-disclosure-body">
                          <ul className="tl-wp-sublist">{history.runs.map(renderRunItem)}</ul>
                        </div>
                      </details>
                    )}

                    {history.outbox.length > 0 && (
                      <details className="tl-wp-disclosure">
                        <summary>Outbox & delivery ledger ({history.outbox.length})</summary>
                        <div className="tl-wp-disclosure-body">
                          <ul className="tl-wp-sublist">{history.outbox.map(renderOutboxItem)}</ul>
                        </div>
                      </details>
                    )}

                    {linkItems.length > 0 && (
                      <details className="tl-wp-disclosure">
                        <summary>Lineage & record links ({linkItems.length})</summary>
                        <div className="tl-wp-disclosure-body">
                          <div className="tl-wp-kv-row tl-wp-mono">
                            {linkItems.map((entry) => (
                              <span key={entry}>{entry}</span>
                            ))}
                          </div>
                        </div>
                      </details>
                    )}
                  </div>
                )}
              </article>
            );
          })
        )}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// 3. Activity Panel (`WorkspaceActivityPanel`)
// ---------------------------------------------------------------------------

export interface WorkspaceActivityPanelProps {
  chat?: WorkspaceChatSummary | null;
  observation: ObservationSnapshot | null;
  jobHistories?: readonly WorkspaceJobHistory[] | null;
  bounds?: WorkspaceSnapshotBounds | null;
  knownChatJobIds?: ReadonlySet<string>;
  scope?: ActivityFeedScope | null;
  loading?: boolean;
  error?: string | null;
  isStale?: boolean;
  staleReason?: string | null;
  nowMs?: number | null;
  onRefresh?: () => void;
  onClearScope?: () => void;
  onSelectJob?: (scope: ActivityFeedScope) => void;
}

function ActivityRequestAuditCard({
  row,
  expanded,
  showWorkspaceContext,
  suppressLiveSpinner,
  nowMs,
  onToggle,
  onSelectJob,
}: {
  row: ActivityRequestView;
  expanded: boolean;
  showWorkspaceContext: boolean;
  suppressLiveSpinner: boolean;
  nowMs: number | null;
  onToggle: (key: string) => void;
  onSelectJob?: (scope: ActivityFeedScope) => void;
}) {
  const detailId = `tl-wp-act-detail-${row.domKey}`;
  const executorDisplay = useMemo(() => projectRequestExecutorDisplay(row), [row]);
  const visibleTimeline = useMemo(
    () => (expanded ? projectVisibleTimeline(row) : []),
    [expanded, row],
  );
  const compactDuration = formatCompactDuration(row.duration, row.durationDetail);
  const compactUsage = formatCompactUsage(row.usage);
  const showRootSpinner = row.showSpinner && !suppressLiveSpinner;
  const showChildSpinner = !showRootSpinner && row.callsRunning && !suppressLiveSpinner;
  const brandKey = resolveProviderBrandKey(
    row.request.executor?.tool ?? row.request.executor?.provider,
    row.request.executor?.observed_model ?? row.request.executor?.requested_model,
  );
  const relativeUpdated = formatRelativeTime(
    row.meaningfulAtMs,
    nowMs ?? row.meaningfulAtMs,
  );
  const hasSecondaryLine =
    showWorkspaceContext || Boolean(row.request.job_id) || row.childrenCount > 0;

  return (
    <article
      className="tl-wp-activity-card"
      data-request-key={row.key}
      data-lifecycle-state={row.state}
    >
      <button
        type="button"
        className="tl-wp-activity-row-btn"
        aria-expanded={expanded}
        aria-controls={detailId}
        onClick={() => onToggle(row.key)}
      >
        <div className="tl-wp-activity-row-top">
          <div className="tl-wp-activity-row-lead">
            <span className="tl-wp-pill" data-tone={row.stateTone}>
              {showRootSpinner && <ActivitySpinner />}
              <span className="tl-wp-pill-label">{row.stateLabel}</span>
            </span>

            {row.callsRunning && (
              <span className="tl-wp-pill" data-tone="working">
                {showChildSpinner && <ActivitySpinner />}
                <span className="tl-wp-pill-label">
                  {row.runningChildCount === 1
                    ? '1 child running'
                    : `${row.runningChildCount} children running`}
                </span>
              </span>
            )}

            <span className="tl-wp-activity-row-title">{row.title}</span>
          </div>

          <div className="tl-wp-activity-row-metrics">
            <span className="tl-wp-status-inline">
              <ProviderBrandBadge brand={brandKey} size={13} />
              <span>
                {executorDisplay.model
                  ? `${executorDisplay.role} · ${executorDisplay.model}`
                  : executorDisplay.role}
              </span>
            </span>
            <span>{compactDuration.value}</span>
            <span>{compactUsage.value}</span>
            <span>{relativeUpdated}</span>
          </div>
        </div>

        {hasSecondaryLine && (
          <div className="tl-wp-activity-row-metrics">
            {showWorkspaceContext && <span>{row.projectLabel}</span>}
            {showWorkspaceContext && <span>{row.chatLabel}</span>}
            {row.request.job_id && <span className="tl-wp-mono">Job {row.request.job_id}</span>}
            {row.childrenCount > 0 && (
              <span>
                {row.childrenCount === 1
                  ? '1 child call'
                  : `${row.childrenCount} child calls`}
              </span>
            )}
          </div>
        )}
      </button>

      {expanded && (
        <div id={detailId} className="tl-wp-activity-detail" role="region" aria-label="Request audit details">
          <div className="tl-wp-audit-grid">
            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Request & Operation</span>
              <span className="tl-wp-audit-value tl-wp-mono">
                {row.request.source_id} · {row.request.id}
              </span>
              <span>Operation: {row.operation}</span>
              {row.parentRequestId && (
                <span className="tl-wp-mono">Parent: {row.parentRequestId}</span>
              )}
            </div>

            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Conversation & Scope</span>
              <span className="tl-wp-audit-value">{row.conversationDetails}</span>
              <span>{row.sourceDetails}</span>
              {row.request.job_id && (
                <div className="tl-wp-kv-row">
                  <span className="tl-wp-mono">Job: {row.request.job_id}</span>
                  {onSelectJob && (
                    <button
                      type="button"
                      className="tl-wp-btn tl-wp-btn-subtle"
                      onClick={() =>
                        onSelectJob({
                          source_id: row.request.source_id,
                          job_id: row.request.job_id!,
                        })
                      }
                    >
                      Open in Jobs
                    </button>
                  )}
                </div>
              )}
            </div>

            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Requester Evidence</span>
              <span className="tl-wp-audit-value">{row.requesterLine.value}</span>
              <span>
                Evidence: {row.requesterEvidence} · Source: {row.actorSource} · Role:{' '}
                {row.actorRole}
              </span>
              <span>Configured selection: {row.actorSelectedSummary}</span>
            </div>

            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Root Executor</span>
              <span className="tl-wp-audit-value">{row.executorIdentity}</span>
              <span>Model: {row.modelSummary}</span>
              <span>Effort: {row.effortSummary}</span>
            </div>

            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Usage & Duration</span>
              <span className="tl-wp-audit-value">{row.usage.detail}</span>
              <span>{row.durationDetail}</span>
              <span>
                Started: {row.startedAtValue} · Finished: {row.finishedAtValue} · Updated:{' '}
                {row.updatedAtValue}
              </span>
            </div>

            <div className="tl-wp-audit-cell">
              <span className="tl-wp-audit-label">Delivery Audit</span>
              <span className="tl-wp-audit-value">{row.latestReturn}</span>
              <span>Stage: {row.deliveryLabel} · Recipient: {row.deliveryRecipient}</span>
              <span>
                Transport delivery or acknowledgement never proves human reading or review
                acceptance.
              </span>
            </div>
          </div>

          <div className="tl-wp-audit-cell">
            <span className="tl-wp-audit-label">Recorded Summary</span>
            <p className="tl-wp-job-summary">{row.summary}</p>
          </div>

          {row.childrenCount > 0 && (
            <div className="tl-wp-section-list">
              <h5 className="tl-wp-section-heading">
                Child calls ({row.childrenReturned} of {row.childrenCount} returned)
              </h5>
              {row.childrenCount > row.childrenReturned && (
                <div className="tl-wp-banner" data-tone="warning">
                  <span className="tl-wp-banner-text">{row.childCoverage}</span>
                </div>
              )}
              <ul className="tl-wp-sublist">
                {row.children.map((child) => {
                  const childSpin = child.showSpinner && !suppressLiveSpinner;
                  return (
                    <li key={child.call.id} className="tl-wp-subitem">
                      <div className="tl-wp-kv-row">
                        <span className="tl-wp-pill" data-tone={child.stateTone}>
                          {childSpin && <ActivitySpinner />}
                          <span className="tl-wp-pill-label">{child.stateLabel}</span>
                        </span>
                        <strong>{child.title}</strong>
                        <span className="tl-wp-mono">{child.call.id}</span>
                      </div>
                      <div className="tl-wp-kv-row">
                        <span>{child.executorSummary}</span>
                        <span>Model: {child.modelSummary}</span>
                        <span>Effort: {child.effortSummary}</span>
                        <span>{child.durationSummary}</span>
                        <span>{child.usage.summary}</span>
                      </div>
                      {child.summary && <p className="tl-wp-job-summary">{child.summary}</p>}
                    </li>
                  );
                })}
              </ul>
            </div>
          )}

          {row.deliveries.length > 0 && (
            <div className="tl-wp-section-list">
              <h5 className="tl-wp-section-heading">
                Delivery records ({row.deliveries.length})
              </h5>
              <ul className="tl-wp-sublist">
                {row.deliveries.map((delivery, idx) => (
                  <li key={`${delivery.stage}-${idx}`} className="tl-wp-subitem">
                    <div className="tl-wp-kv-row">
                      <span className="tl-wp-pill" data-tone="neutral">
                        <span className="tl-wp-pill-label">{delivery.label}</span>
                      </span>
                      <span>Recipient: {delivery.recipient}</span>
                      <span>At: {delivery.atValue}</span>
                    </div>
                    <p className="tl-wp-job-summary">{delivery.summary}</p>
                    <span className="tl-wp-state-desc">{delivery.note}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {visibleTimeline.length > 0 && (
            <div className="tl-wp-section-list">
              <h5 className="tl-wp-section-heading">Causal timeline</h5>
              <ol className="tl-wp-timeline">
                {visibleTimeline.map((entry) => (
                  <li key={entry.event.key} className="tl-wp-timeline-item">
                    <div className="tl-wp-kv-row">
                      <strong>{entry.display.label}</strong>
                      {entry.event.stateLabel && (
                        <span className="tl-wp-pill" data-tone={entry.event.tone}>
                          <span className="tl-wp-pill-label">{entry.event.stateLabel}</span>
                        </span>
                      )}
                      <span>{entry.event.atValue}</span>
                    </div>
                    <span>{entry.display.detail}</span>
                    {entry.display.lines.map((line, lineIdx) => (
                      <span key={`${entry.event.key}-line-${lineIdx}`} className="tl-wp-state-desc">
                        {line}
                      </span>
                    ))}
                  </li>
                ))}
              </ol>
            </div>
          )}
        </div>
      )}
    </article>
  );
}

export function WorkspaceActivityPanel({
  chat = null,
  observation,
  jobHistories = null,
  bounds = null,
  knownChatJobIds,
  scope = null,
  loading = false,
  error = null,
  isStale = false,
  staleReason = null,
  nowMs = null,
  onRefresh,
  onClearScope,
  onSelectJob,
}: WorkspaceActivityPanelProps) {
  const [expandedKeys, setExpandedKeys] = useState<ReadonlySet<string>>(() => new Set<string>());

  const chatKey = resolveMessageScrollChatKey(chat);
  useEffect(() => {
    setExpandedKeys(new Set<string>());
  }, [chatKey]);

  const toggleExpanded = useCallback((rowKey: string) => {
    setExpandedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(rowKey)) {
        next.delete(rowKey);
      } else {
        next.add(rowKey);
      }
      return next;
    });
  }, []);

  const projection = useMemo(
    () =>
      observation
        ? projectActivityFeed(observation, {
            scope,
            now_ms: nowMs ?? observation.generated_at_ms,
          })
        : null,
    [observation, scope, nowMs],
  );

  const matchContext = useMemo<ObservationChatMatchContext>(
    () => ({
      knownChatJobIds,
      observation,
      jobHistories,
    }),
    [knownChatJobIds, observation, jobHistories],
  );

  const verifiedChatJobIds = useMemo(
    () => resolveVerifiedChatJobIds(chat, jobHistories, observation),
    [chat, jobHistories, observation],
  );

  const referenceNowMs =
    typeof nowMs === 'number' && Number.isFinite(nowMs)
      ? nowMs
      : projection?.generatedAtMs ??
        observation?.generated_at_ms ??
        Date.now();
  const scopedJobHistories = useMemo(
    () =>
      jobHistories
        ? filterJobsForChat(jobHistories, chat, verifiedChatJobIds)
        : [],
    [jobHistories, chat, verifiedChatJobIds],
  );
  const externalResolutionEntries = useMemo(() => {
    const entries = collectWorkspacePanelExternalResolutionEntries(
      scopedJobHistories,
      referenceNowMs,
    );
    if (!scope) {
      return entries;
    }
    return entries.filter((entry) => entry.jobId === scope.job_id);
  }, [scopedJobHistories, referenceNowMs, scope]);
  const applicableExternalResolutions = externalResolutionEntries.filter(
    (entry) => entry.view.isApplicable,
  );
  const historicalExternalResolutions = externalResolutionEntries.filter(
    (entry) => !entry.view.isApplicable,
  );

  // Selected-chat scope is always active when `chat` is non-null; a job filter intersects it and never bypasses it.
  const effectiveChatScopeActive = chat !== null;

  const displayedRows = useMemo(() => {
    if (!projection) {
      return [];
    }
    if (!effectiveChatScopeActive || !chat) {
      return projection.rows;
    }
    return projection.rows.filter((row) =>
      doesObservationRequestMatchChat(row.request, chat, matchContext),
    );
  }, [projection, effectiveChatScopeActive, chat, matchContext]);

  const scopeMatchesChat = useMemo(() => {
    if (!scope || !chat) {
      return true;
    }
    if (!isValidWorkspaceChatIdentity(chat.identity)) {
      return false;
    }
    if (scope.source_id !== chat.identity.source_id) {
      return false;
    }
    if (observation) {
      for (const candidate of observation.jobs) {
        if (candidate.source_id === scope.source_id && candidate.id === scope.job_id) {
          if (
            (candidate.binding_id !== null &&
              candidate.binding_id !== chat.identity.binding_id) ||
            (candidate.thread_uuid !== null &&
              candidate.thread_uuid !== chat.identity.thread_uuid) ||
            (candidate.project_path !== null &&
              candidate.project_path !== chat.identity.canonical_project_scope)
          ) {
            return false;
          }
        }
      }
    }
    if (jobHistories) {
      for (const history of jobHistories) {
        if (
          history.job.source_id === scope.source_id &&
          history.job.job_id === scope.job_id
        ) {
          if (
            (history.job.binding_id !== null &&
              history.job.binding_id !== chat.identity.binding_id) ||
            history.runs.some((run) => run.binding_id !== chat.identity.binding_id) ||
            (history.job.chat_identity !== null &&
              (!isValidWorkspaceChatIdentity(history.job.chat_identity) ||
                history.job.chat_identity.source_id !== chat.identity.source_id ||
                history.job.chat_identity.binding_id !== chat.identity.binding_id ||
                history.job.chat_identity.thread_uuid !== chat.identity.thread_uuid ||
                history.job.chat_identity.canonical_project_scope !==
                  chat.identity.canonical_project_scope))
          ) {
            return false;
          }
        }
      }
    }
    return Boolean(
      verifiedChatJobIds.has(scope.job_id) || knownChatJobIds?.has(scope.job_id),
    );
  }, [scope, chat, observation, jobHistories, verifiedChatJobIds, knownChatJobIds]);

  const suppressLiveSpinner =
    isStale || chat?.availability === 'stale' || Boolean(error);

  const runningBannerText = useMemo(() => {
    if (!projection) {
      return '';
    }
    if (!effectiveChatScopeActive || !chat) {
      return formatActivityRunningCountLabel(
        projection.visibleRunningCount,
        projection.scopeRunningCount,
      );
    }
    const runningInScope = displayedRows.filter(
      (row) => row.showSpinner || row.callsRunning,
    ).length;
    return formatActivityRunningCountLabel(runningInScope, runningInScope);
  }, [projection, effectiveChatScopeActive, chat, displayedRows]);

  const scopedUnavailableSources = useMemo(() => {
    if (!projection) {
      return [];
    }
    if (!chat) {
      return projection.unavailableSources;
    }
    return projection.unavailableSources.filter(
      (src) => src.source.id === chat.identity.source_id,
    );
  }, [projection, chat]);

  const scopedWarnings = useMemo(() => {
    if (!projection) {
      return [];
    }
    if (!chat) {
      return projection.warnings;
    }
    if (observation?.warning_contexts && observation.warning_contexts.length > 0) {
      return observation.warning_contexts
        .filter(
          (ctx) => ctx.source_id === null || ctx.source_id === chat.identity.source_id,
        )
        .map((ctx) => ctx.warning.trim())
        .filter((w) => w.length > 0);
    }
    if (observation && observation.sources.every((s) => s.id === chat.identity.source_id)) {
      return projection.warnings;
    }
    return [];
  }, [projection, chat, observation]);

  const isBoundedActivity =
    Boolean(projection?.hasMore) ||
    Boolean(projection?.jobsHasMore) ||
    Boolean(bounds?.requests_capped) ||
    Boolean(bounds?.jobs_capped) ||
    (projection?.totalRecorded ?? 0) >= MAX_OBSERVATION_FRONTEND_REQUEST_WINDOW;

  const diagnosticLines = useMemo(() => {
    const lines: string[] = [...scopedWarnings];
    if (projection && isBoundedActivity) {
      lines.push(`Showing ${displayedRows.length} recent calls in current scope.`);
      if (projection.hasMore || bounds?.requests_capped) {
        lines.push('Older recorded calls exist beyond the recent window.');
      }
      if (projection.jobsHasMore || bounds?.jobs_capped) {
        lines.push('Some older job metadata was omitted from this window.');
      }
    }
    return lines;
  }, [scopedWarnings, projection, isBoundedActivity, displayedRows.length, bounds]);

  const showStaleBanner =
    isStale || chat?.availability === 'stale' || (Boolean(error) && observation !== null);
  const showDisconnectedBanner = chat?.availability === 'disconnected';

  const hasBanners =
    showStaleBanner ||
    showDisconnectedBanner ||
    scopedUnavailableSources.length > 0 ||
    isBoundedActivity ||
    diagnosticLines.length > 0 ||
    Boolean(projection?.scope.scoped);

  return (
    <section className="tl-wp-activity-panel" aria-label="Recent recorded activity">
      {projection && (
        <div className="tl-wp-activity-toolbar">
          <div className="tl-wp-activity-status">
            <span className="tl-wp-filter-label">Recent recorded activity</span>
            {runningBannerText && (
              <span className="tl-wp-pill" data-tone="working">
                {!suppressLiveSpinner && <ActivitySpinner />}
                <span className="tl-wp-pill-label">{runningBannerText}</span>
              </span>
            )}
          </div>

          <div className="tl-wp-activity-status">
            <span className="tl-wp-filter-label">
              {projection.generatedAtMs !== null
                ? `Updated ${formatFeedTimestamp(projection.generatedAtMs)}`
                : 'Update time not recorded'}
            </span>
          </div>
        </div>
      )}

      {hasBanners && projection && (
        <div className="tl-wp-banners" role="region" aria-label="Activity status notices">
          {showStaleBanner && (
            <div className="tl-wp-banner" data-tone="stale" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Stale activity</span>
                <span className="tl-wp-banner-text">
                  {error
                    ? `Refresh failed (${error}). Live indicators are paused; showing last recorded activity.`
                    : staleReason ??
                      'Updates are delayed. Live indicators are paused; showing last recorded activity.'}
                </span>
              </div>
              {onRefresh && (
                <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                  Refresh
                </button>
              )}
            </div>
          )}

          {showDisconnectedBanner && (
            <div className="tl-wp-banner" data-tone="disconnected" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Chat disconnected</span>
                <span className="tl-wp-banner-text">
                  Live chat connection is disconnected. Recent recorded activity remains available
                  below.
                </span>
              </div>
            </div>
          )}

          {projection.scope.scoped && scope && (
            <div className="tl-wp-banner" data-tone="neutral" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">
                  {scopeMatchesChat
                    ? `Filtered to job ${
                        projection.scope.job?.title
                          ? `${projection.scope.job.title} (${scope.job_id})`
                          : scope.job_id
                      }`
                    : 'Job filter outside selected chat'}
                </span>
                <span className="tl-wp-banner-text">
                  {scopeMatchesChat
                    ? 'Showing recent recorded activity for this job.'
                    : `Job ${scope.job_id} does not belong to this chat, so no outside activity is shown.`}
                </span>
              </div>
              {onClearScope && (
                <button type="button" className="tl-wp-btn" onClick={onClearScope}>
                  Clear job filter
                </button>
              )}
            </div>
          )}

          {scopedUnavailableSources.map((src) => (
            <div
              key={src.source.id}
              className="tl-wp-banner"
              data-tone="unavailable"
              role="status"
            >
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Activity source unavailable: {src.label}</span>
                <span className="tl-wp-banner-text">{src.message}</span>
              </div>
            </div>
          ))}

          {isBoundedActivity && (
            <div className="tl-wp-banner" data-tone="warning" role="status">
              <div className="tl-wp-banner-body">
                <span className="tl-wp-banner-title">Some older activity unavailable</span>
                <span className="tl-wp-banner-text">
                  Showing recent recorded activity; older history is not included in this view.
                </span>
              </div>
            </div>
          )}

          {diagnosticLines.length > 0 && (
            <details className="tl-wp-disclosure">
              <summary>Diagnostics</summary>
              <div className="tl-wp-disclosure-body">
                {diagnosticLines.map((line, idx) => (
                  <span key={`diag-${idx}`} className="tl-wp-state-desc">
                    {line}
                  </span>
                ))}
              </div>
            </details>
          )}
        </div>
      )}

      <div className="tl-wp-activity-scroller">
        {externalResolutionEntries.length > 0 && (
          <div
            className="tl-wp-section-list"
            aria-label="External resolution receipts"
            data-external-resolution-surface="activity"
          >
            <h4 className="tl-wp-section-heading">
              External resolution receipts ({applicableExternalResolutions.length})
            </h4>
            <span className="tl-wp-state-desc" data-external-resolution-note="true">
              External resolution receipts are recorded in job history.
            </span>
            {applicableExternalResolutions.length === 0 ? (
              <span className="tl-wp-state-desc">
                No applicable external resolution receipts recorded for this activity scope.
              </span>
            ) : (
              applicableExternalResolutions.map((entry) =>
                renderExternalResolutionCard(entry, {
                  onSelectJob,
                  showJobContext: true,
                }),
              )
            )}

            {historicalExternalResolutions.length > 0 && (
              <details
                className="tl-wp-disclosure"
                data-external-resolution-surface="activity-history"
                data-external-resolution-history-disclosure="true"
              >
                <summary>
                  External resolution history ({historicalExternalResolutions.length})
                </summary>
                <div className="tl-wp-disclosure-body">
                  {historicalExternalResolutions.map((entry) =>
                    renderExternalResolutionCard(entry, {
                      onSelectJob,
                      showJobContext: true,
                    }),
                  )}
                </div>
              </details>
            )}
          </div>
        )}
        {loading && !observation ? (
          <div className="tl-wp-state-card" data-state="loading" role="status">
            <h3 className="tl-wp-state-title">Loading recent activity…</h3>
            <p className="tl-wp-state-desc">
              Reading recorded calls, status updates, and delivery history.
            </p>
          </div>
        ) : error && !observation ? (
          <div className="tl-wp-state-card" data-state="error" role="alert">
            <h3 className="tl-wp-state-title">Activity unavailable</h3>
            <p className="tl-wp-state-desc">{error}</p>
            {onRefresh && (
              <button type="button" className="tl-wp-btn" onClick={onRefresh}>
                Retry
              </button>
            )}
          </div>
        ) : !projection ? (
          <div className="tl-wp-state-card" data-state="unavailable" role="status">
            <h3 className="tl-wp-state-title">Activity unavailable</h3>
            <p className="tl-wp-state-desc">
              Recent activity history is currently unavailable.
            </p>
          </div>
        ) : displayedRows.length === 0 ? (
          <div className="tl-wp-state-card" data-state="empty" role="status">
            <h3 className="tl-wp-state-title">
              {externalResolutionEntries.length > 0
                ? 'No request activity recorded'
                : 'No recorded activity'}
            </h3>
            <p className="tl-wp-state-desc">
              {externalResolutionEntries.length > 0
                ? 'External resolution receipts are listed above. No request activity was recorded for this scope.'
                : scope
                  ? 'No recent activity is recorded for this job in the current chat.'
                  : chat
                    ? 'No recent activity has been recorded for this chat yet.'
                    : 'No recent activity has been recorded yet.'}
            </p>
          </div>
        ) : (
          displayedRows.map((row) => (
            <ActivityRequestAuditCard
              key={row.key}
              row={row}
              expanded={expandedKeys.has(row.key)}
              showWorkspaceContext={!chat}
              suppressLiveSpinner={suppressLiveSpinner}
              nowMs={nowMs ?? projection.generatedAtMs}
              onToggle={toggleExpanded}
              onSelectJob={onSelectJob}
            />
          ))
        )}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Minimal chat-work surface: one bottom-growing stream and a backlog drawer
// ---------------------------------------------------------------------------

export interface WorkspaceChatWorkSurfaceProps {
  chat: WorkspaceChatSummary | null;
  snapshot?: WorkspaceSnapshot | null;
  nativeSample?: WorkspaceNativeSample | null;
  nowMs?: number | null;
  loading?: boolean;
  error?: string | null;
  isStale?: boolean;
  onRefresh?: () => void;
  onOpenCodex?: (target: CodexChatTarget) => void;
  onOpenScopedCodex?: (target: WorkspaceScopedChatTarget) => void;
  codexOpenInFlight?: boolean;
}

function ChatWorkStatusDot({ tone }: { tone: ChatWorkLiveness['statusTone'] }) {
  return <span className="tl-cw-dot" data-tone={tone} aria-hidden="true" />;
}

function ChatWorkSpinner({ small = false }: { small?: boolean }) {
  return (
    <span
      className={small ? 'tl-cw-spinner tl-cw-spinner--small' : 'tl-cw-spinner'}
      data-activity-spinner="true"
      aria-hidden="true"
    />
  );
}

function buildChatWorkCodexTargets(
  identity: WorkspaceChatIdentity,
): { legacy: CodexChatTarget; scoped: WorkspaceScopedChatTarget } | null {
  if (!isValidWorkspaceChatIdentity(identity)) {
    return null;
  }
  return {
    legacy: {
      source_id: identity.source_id,
      thread_uuid: identity.thread_uuid,
      job_id: null,
    },
    scoped: {
      identity,
      job_id: null,
    },
  };
}

export function WorkspaceChatWorkSurface({
  chat,
  snapshot = null,
  nativeSample = null,
  nowMs = null,
  loading = false,
  error = null,
  isStale = false,
  onRefresh,
  onOpenCodex,
  onOpenScopedCodex,
  codexOpenInFlight = false,
}: WorkspaceChatWorkSurfaceProps) {
  const [backlogOpen, setBacklogOpen] = useState(false);
  const streamRef = useRef<HTMLDivElement | null>(null);
  const listRef = useRef<HTMLOListElement | null>(null);
  const backlogTriggerRef = useRef<HTMLButtonElement | null>(null);
  const lastLayoutSizeRef = useRef<{
    chatKey: string;
    clientHeight: number;
    scrollHeight: number;
  } | null>(null);

  const referenceNowMs =
    typeof nowMs === 'number' && Number.isFinite(nowMs)
      ? nowMs
      : snapshot?.generated_at_ms ?? Date.now();

  const chatWork = useMemo(() => readWorkspaceChatWork(snapshot), [snapshot]);
  const selection = useMemo(
    () => selectWorkspaceChatWork(chatWork, chat?.identity ?? null),
    [chat, chatWork],
  );
  const liveness = useMemo(
    () =>
      evaluateChatWorkLiveness(selection, nativeSample, {
        nowMs: referenceNowMs,
        staleThresholdMs: snapshot?.sampling_policy.stale_threshold_ms,
        isStale,
        refreshFailed: error !== null,
      }),
    [error, isStale, nativeSample, referenceNowMs, selection, snapshot],
  );
  const eventViews = useMemo(
    () => buildChatWorkEventViews(selection),
    [selection],
  );
  const backlogGroups = useMemo(
    () => buildChatWorkBacklogGroups(selection),
    [selection],
  );
  const chatKey = selection.chatKey ?? '__none__';
  const backlogCount = selection.items.length;
  const codexTargets = chat ? buildChatWorkCodexTargets(chat.identity) : null;

  const scrollStoreRef = useRef<MessageScrollStore>(createMessageScrollStore(chatKey));
  const [scrollView, setScrollView] = useState<{
    chatKey: string;
    mode: MessageScrollMode;
    pendingNewCount: number;
  }>(() => ({
    chatKey,
    mode: 'follow',
    pendingNewCount: 0,
  }));

  const syncUiScroll = useCallback((state: MessageScrollState) => {
    setScrollView((previous) =>
      previous.chatKey === state.chatKey &&
      previous.mode === state.mode &&
      previous.pendingNewCount === state.pendingNewCount
        ? previous
        : {
            chatKey: state.chatKey,
            mode: state.mode,
            pendingNewCount: state.pendingNewCount,
          },
    );
  }, []);

  const activeScrollView = useMemo(() => {
    if (scrollView.chatKey === chatKey) {
      return scrollView;
    }
    const stored = getChatScrollState(scrollStoreRef.current, chatKey);
    return {
      chatKey,
      mode: stored.mode,
      pendingNewCount: stored.pendingNewCount,
    };
  }, [scrollView, chatKey]);
  const showJumpToLatest = activeScrollView.mode === 'reading';

  useEffect(() => {
    setBacklogOpen(false);
  }, [chatKey]);

  // Reconcile appends, head trims, and chat switches before paint using the
  // shared message anchor reducer keyed by stable event ids.
  useLayoutEffect(() => {
    scrollStoreRef.current = switchMessageScrollChat(scrollStoreRef.current, chatKey);
    const currentState = getActiveChatScrollState(scrollStoreRef.current);
    const sync = syncChatWorkEventScroll(currentState, eventViews);
    scrollStoreRef.current = setChatScrollState(scrollStoreRef.current, sync.state);
    syncUiScroll(sync.state);

    const scroller = streamRef.current;
    if (!scroller) {
      return;
    }
    if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
      return;
    }

    if (sync.action.kind === 'scroll_to_bottom') {
      scroller.scrollTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    } else if (sync.action.kind === 'preserve_anchor') {
      const anchor = sync.action.anchor;
      const { metrics, items } = measureDomMessageViewport(
        scroller,
        '[data-chat-event-id]',
      );
      const found = items.find((entry) => entry.itemId === anchor.itemId);
      if (found) {
        scroller.scrollTop = computeAnchorScrollTop(
          metrics,
          found.relativeTopPx,
          anchor.offsetPx,
        );
      }
    } else if (eventViews.length === 0 && scroller.scrollTop !== 0) {
      scroller.scrollTop = 0;
    }

    lastLayoutSizeRef.current = {
      chatKey,
      clientHeight: scroller.clientHeight,
      scrollHeight: scroller.scrollHeight,
    };
  }, [chatKey, eventViews, syncUiScroll]);

  const reconcileStreamViewport = useCallback(() => {
    const scroller = streamRef.current;
    if (!scroller) {
      return;
    }
    if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
      return;
    }
    const currentState = getChatScrollState(scrollStoreRef.current, chatKey);
    const { metrics, items } = measureDomMessageViewport(
      scroller,
      '[data-chat-event-id]',
    );
    const decision = resolveMessageViewportResize(currentState, metrics, items);
    scrollStoreRef.current = setChatScrollState(scrollStoreRef.current, decision.state);
    syncUiScroll(decision.state);

    if (decision.action.kind === 'scroll_to_bottom') {
      scroller.scrollTop = decision.action.targetScrollTop;
    } else if (decision.action.kind === 'restore_anchor') {
      scroller.scrollTop = decision.action.targetScrollTop;
    }

    lastLayoutSizeRef.current = {
      chatKey,
      clientHeight: scroller.clientHeight,
      scrollHeight: scroller.scrollHeight,
    };
  }, [chatKey, syncUiScroll]);

  // Observe both the scrollport and the event list so text reflow above the
  // viewport restores the reading anchor instead of drifting or jumping.
  useEffect(() => {
    const scroller = streamRef.current;
    if (!scroller || typeof ResizeObserver === 'undefined') {
      return;
    }
    const observer = new ResizeObserver(() => {
      reconcileStreamViewport();
    });
    observer.observe(scroller);
    if (listRef.current) {
      observer.observe(listRef.current);
    }
    return () => observer.disconnect();
  }, [chatKey, eventViews.length, reconcileStreamViewport]);

  const handleStreamScroll = useCallback(() => {
    const scroller = streamRef.current;
    if (!scroller) {
      return;
    }
    if (scroller.clientHeight === 0 && scroller.scrollHeight === 0) {
      return;
    }
    const currentState = getChatScrollState(scrollStoreRef.current, chatKey);
    const maxTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);

    // A layout shrink can clamp scrollTop before the resize observer runs.
    // Restore the reading anchor instead of mistaking the clamp for a jump to
    // the bottom.
    const previousSize = lastLayoutSizeRef.current;
    if (
      currentState.mode === 'reading' &&
      previousSize !== null &&
      previousSize.chatKey === chatKey &&
      (previousSize.clientHeight !== scroller.clientHeight ||
        previousSize.scrollHeight !== scroller.scrollHeight) &&
      scroller.scrollTop >= maxTop - 2
    ) {
      reconcileStreamViewport();
      return;
    }

    const { metrics, items } = measureDomMessageViewport(
      scroller,
      '[data-chat-event-id]',
    );
    const nextState = handleMessageViewportScroll(
      currentState,
      metrics,
      items,
      DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
    );
    scrollStoreRef.current = setChatScrollState(scrollStoreRef.current, nextState);
    syncUiScroll(nextState);
    lastLayoutSizeRef.current = {
      chatKey,
      clientHeight: scroller.clientHeight,
      scrollHeight: scroller.scrollHeight,
    };
  }, [chatKey, reconcileStreamViewport, syncUiScroll]);

  const handleJumpToLatest = useCallback(() => {
    const currentState = getChatScrollState(scrollStoreRef.current, chatKey);
    const jumped = jumpToLatestMessages(currentState);
    scrollStoreRef.current = setChatScrollState(scrollStoreRef.current, jumped.state);
    syncUiScroll(jumped.state);

    const scroller = streamRef.current;
    if (scroller) {
      scroller.scrollTop = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    }
  }, [chatKey, syncUiScroll]);

  return (
    <div className="tl-cw-root" data-chat-work-surface="true">
      {chat ? (
        <header className="tl-cw-header">
          <div className="tl-cw-heading">
            <div className="tl-cw-title-row">
              <h2 className="tl-cw-title">{chat.thread_label}</h2>
              {liveness.statusLabel !== null ? (
                <span
                  className="tl-cw-status"
                  data-tone={liveness.statusTone}
                  role="status"
                >
                  {liveness.isWorking ? <ChatWorkSpinner /> : null}
                  <span>{liveness.statusLabel}</span>
                </span>
              ) : null}
            </div>
            <div className="tl-cw-subtitle">
              <span className="tl-cw-meta-item">
                <FolderGit2 size={13} aria-hidden="true" />
                <span>{chat.project_label}</span>
              </span>
            </div>
          </div>

          <div className="tl-cw-actions">
            {onRefresh ? (
              <IconButton
                size="2"
                variant="ghost"
                color="gray"
                aria-label="Refresh workspace"
                onClick={onRefresh}
              >
                <RefreshCw size={14} aria-hidden="true" />
              </IconButton>
            ) : null}
            <button
              ref={backlogTriggerRef}
              type="button"
              className="tl-cw-btn"
              aria-haspopup="dialog"
              aria-expanded={backlogOpen}
              onClick={() => setBacklogOpen(true)}
            >
              <History size={14} aria-hidden="true" />
              <span>Backlog</span>
              <span className="tl-cw-count" aria-label={`${backlogCount} backlog items`}>
                {backlogCount}
              </span>
            </button>
            {codexTargets && (onOpenScopedCodex || onOpenCodex) ? (
              <button
                type="button"
                className="tl-cw-btn tl-cw-btn--primary"
                disabled={codexOpenInFlight}
                onClick={() => {
                  if (onOpenScopedCodex) {
                    onOpenScopedCodex(codexTargets.scoped);
                  } else if (onOpenCodex) {
                    onOpenCodex(codexTargets.legacy);
                  }
                }}
              >
                <ExternalLink size={14} aria-hidden="true" />
                <span>{codexOpenInFlight ? 'Opening Codex…' : 'Open in Codex'}</span>
              </button>
            ) : null}
          </div>
        </header>
      ) : (
        <header className="tl-cw-header">
          <div className="tl-cw-heading">
            <h2 className="tl-cw-title">Workspace</h2>
            <div className="tl-cw-subtitle">
              <span>Select a chat to view its activity stream.</span>
            </div>
          </div>
        </header>
      )}

      {chat && liveness.agents.length > 0 ? (
        <ul className="tl-cw-agents" aria-label="Live agents">
          {liveness.agents.map((agent) => (
            <li
              key={agent.key}
              className="tl-cw-agent"
              data-agent-role={agent.role}
            >
              <ChatWorkStatusDot tone="working" />
              <span className="tl-cw-agent-role">{agent.roleLabel}</span>
              <span className="tl-cw-agent-detail">{agent.detail}</span>
              <ChatWorkSpinner small />
            </li>
          ))}
        </ul>
      ) : null}

      <div className="tl-cw-stage">
        <div
          ref={streamRef}
          className="tl-cw-stream-scroller"
          data-chat-stream-scroller="true"
          aria-label="Chat activity stream"
          role="log"
          tabIndex={0}
          onScroll={handleStreamScroll}
        >
          <ol ref={listRef} className="tl-cw-stream-list">
            {eventViews.map((event) => {
              const note =
                event.note !== null && event.note.trim() !== event.title.trim()
                  ? event.note
                  : null;
              return (
                <li
                  key={event.key}
                  className="tl-cw-event"
                  data-tone={event.tone}
                  data-item-id={event.eventId}
                  data-chat-event-key={event.key}
                  data-chat-event-id={event.eventId}
                  data-chat-event-sequence={event.sequence}
                >
                  <div className="tl-cw-event-meta">
                    <ChatWorkStatusDot tone={event.tone} />
                    <span className="tl-cw-event-action">{event.actionLabel}</span>
                    <span className="tl-cw-event-time">
                      {formatRelativeTimestamp(event.recordedAtMs, referenceNowMs)}
                    </span>
                  </div>
                  <p className="tl-cw-event-title">
                    {event.title.trim().length > 0 ? event.title : 'Untitled backlog item'}
                  </p>
                  {note !== null ? <p className="tl-cw-event-note">{note}</p> : null}
                </li>
              );
            })}
          </ol>

          {eventViews.length === 0 ? (
            <div className="tl-cw-empty" role="status">
              {loading ? (
                <>
                  <Loader2 size={15} className="tl-spin" aria-hidden="true" />
                  <span>Reading chat activity…</span>
                </>
              ) : error !== null ? (
                <>
                  <AlertCircle size={15} aria-hidden="true" />
                  <span>Chat activity is unavailable until the next successful refresh.</span>
                </>
              ) : selection.availability === 'unavailable' ? (
                <span>
                  {selection.unavailableReason ?? 'Chat activity is unavailable.'}
                </span>
              ) : (
                <span>No backlog activity is recorded for this chat yet.</span>
              )}
            </div>
          ) : null}
        </div>

        {showJumpToLatest ? (
          <button
            type="button"
            className="tl-cw-jump"
            onClick={handleJumpToLatest}
          >
            Jump to latest
          </button>
        ) : null}
      </div>

      <Dialog.Root open={backlogOpen} onOpenChange={setBacklogOpen}>
        <Dialog.Content
          className="tl-cw-drawer-dialog"
          aria-label="Backlog"
          onCloseAutoFocus={(event) => {
            event.preventDefault();
            backlogTriggerRef.current?.focus();
          }}
        >
          <header className="tl-cw-drawer-header">
            <div>
              <Dialog.Title className="tl-cw-drawer-title">Backlog</Dialog.Title>
              <Dialog.Description className="tl-cw-drawer-description">
                Read-only durable work for this chat.
              </Dialog.Description>
            </div>
            <IconButton
              size="2"
              variant="ghost"
              color="gray"
              aria-label="Close backlog drawer"
              onClick={() => setBacklogOpen(false)}
            >
              <X size={17} aria-hidden="true" />
            </IconButton>
          </header>

          <div className="tl-cw-drawer-body">
            {backlogGroups.map((group) => (
              <section
                key={group.status}
                className="tl-cw-backlog-section"
                aria-label={`${group.label} backlog items`}
              >
                <header className="tl-cw-backlog-heading">
                  <ChatWorkStatusDot tone={backlogStatusTone(group.status)} />
                  <h3>{BACKLOG_STATUS_LABELS[group.status]}</h3>
                  <span className="tl-cw-backlog-count">{group.items.length}</span>
                </header>

                {group.items.length === 0 ? (
                  <p className="tl-cw-backlog-empty">No {group.label.toLowerCase()} items.</p>
                ) : (
                  <div className="tl-cw-backlog-list">
                    {group.items.map((item) => (
                      <details
                        key={item.key}
                        className="tl-cw-backlog-item"
                        data-backlog-item-id={item.itemId}
                        data-status={item.status}
                      >
                        <summary className="tl-cw-backlog-summary">
                          <ChatWorkStatusDot tone={item.statusTone} />
                          <span className="tl-cw-backlog-title">{item.title}</span>
                          <span className="tl-cw-backlog-kind">{item.kindLabel}</span>
                        </summary>
                        <div className="tl-cw-backlog-details">
                          {item.status === 'blocked' ? (
                            <p>
                              <strong>Blocked reason</strong>
                              <span>
                                {item.blockedReason ??
                                  'No blocked reason was recorded.'}
                              </span>
                            </p>
                          ) : null}
                          {item.acceptanceCondition !== null ? (
                            <p>
                              <strong>Acceptance</strong>
                              <span>{item.acceptanceCondition}</span>
                            </p>
                          ) : null}
                          {item.deliveryDescription !== null ? (
                            <p>
                              <strong>Delivery</strong>
                              <span>{item.deliveryDescription}</span>
                            </p>
                          ) : null}
                          {item.completionNote !== null ? (
                            <p>
                              <strong>Completion note</strong>
                              <span>{item.completionNote}</span>
                            </p>
                          ) : null}
                          {item.completionEvidence !== null ? (
                            <p>
                              <strong>Completion evidence</strong>
                              <span>{item.completionEvidence}</span>
                            </p>
                          ) : null}
                          {item.completedAtMs !== null ? (
                            <p>
                              <strong>Completed</strong>
                              <span>
                                {formatRelativeTimestamp(item.completedAtMs, referenceNowMs)}
                              </span>
                            </p>
                          ) : null}
                        </div>
                      </details>
                    ))}
                  </div>
                )}
              </section>
            ))}

            {selection.notices.length > 0 ? (
              <div className="tl-cw-bounds" role="status">
                {selection.notices.map((notice) => (
                  <p key={notice}>{notice}</p>
                ))}
              </div>
            ) : null}
          </div>
        </Dialog.Content>
      </Dialog.Root>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Unified Chat-First Workspace Surface (`WorkspaceChatPanels` / `WorkspacePanels`)
// ---------------------------------------------------------------------------

export interface WorkspaceChatPanelsProps {
  chat: WorkspaceChatSummary | null;
  messages?: WorkspaceMessagesWindow | null;
  jobHistories?: readonly WorkspaceJobHistory[] | null;
  observation?: ObservationSnapshot | null;
  snapshot?: WorkspaceSnapshot | null;
  bounds?: WorkspaceSnapshotBounds | null;
  nativeSample?: WorkspaceNativeSample | null;
  activeTab?: WorkspaceChatSurfaceTab;
  defaultTab?: WorkspaceChatSurfaceTab;
  onTabChange?: (tab: WorkspaceChatSurfaceTab) => void;
  activityScope?: ActivityFeedScope | null;
  onActivityScopeChange?: (scope: ActivityFeedScope | null) => void;
  loading?: boolean;
  error?: string | null;
  isStale?: boolean;
  staleReason?: string | null;
  nowMs?: number | null;
  bottomThresholdPx?: number;
  onRefresh?: () => void;
  onOpenCodex?: (target: CodexChatTarget) => void;
  onOpenScopedCodex?: (target: WorkspaceScopedChatTarget) => void;
  codexOpenInFlight?: boolean;
}

export type WorkspacePanelsProps = WorkspaceChatPanelsProps;

function buildCodexTargets(
  identity: WorkspaceChatIdentity,
  jobId: string | null = null,
): { legacy: CodexChatTarget; scoped: WorkspaceScopedChatTarget } | null {
  if (!isValidWorkspaceChatIdentity(identity)) {
    return null;
  }
  return {
    legacy: {
      source_id: identity.source_id,
      thread_uuid: identity.thread_uuid,
      job_id: jobId,
    },
    scoped: {
      identity,
      job_id: jobId,
    },
  };
}

export function WorkspaceChatPanels({
  chat,
  messages: messagesProp,
  jobHistories: jobHistoriesProp,
  observation: observationProp,
  snapshot = null,
  bounds: boundsProp,
  nativeSample = null,
  activeTab: controlledTab,
  defaultTab = DEFAULT_WORKSPACE_CHAT_TAB,
  onTabChange,
  activityScope: controlledActivityScope,
  onActivityScopeChange,
  loading = false,
  error = null,
  isStale = false,
  staleReason = null,
  nowMs = null,
  bottomThresholdPx = DEFAULT_BOTTOM_FOLLOW_THRESHOLD_PX,
  onRefresh,
  onOpenCodex,
  onOpenScopedCodex,
  codexOpenInFlight = false,
}: WorkspaceChatPanelsProps) {
  const idPrefix = useId();
  const [uncontrolledTab, setUncontrolledTab] = useState<WorkspaceChatSurfaceTab>(() =>
    normalizeWorkspaceChatTab(defaultTab),
  );
  const [uncontrolledActivityScope, setUncontrolledActivityScope] =
    useState<ActivityFeedScope | null>(null);
  const [focusedJobId, setFocusedJobId] = useState<string | null>(null);
  const [pendingNewMessages, setPendingNewMessages] = useState(0);

  const chatKey = resolveMessageScrollChatKey(chat);
  useEffect(() => {
    setFocusedJobId(null);
    if (controlledActivityScope === undefined) {
      setUncontrolledActivityScope(null);
    }
  }, [chatKey, controlledActivityScope]);

  const activeTab =
    controlledTab !== undefined ? normalizeWorkspaceChatTab(controlledTab) : uncontrolledTab;
  const effectiveActivityScope =
    controlledActivityScope !== undefined ? controlledActivityScope : uncontrolledActivityScope;

  const setTab = useCallback(
    (nextTab: WorkspaceChatSurfaceTab) => {
      const normalized = normalizeWorkspaceChatTab(nextTab);
      if (controlledTab === undefined) {
        setUncontrolledTab(normalized);
      }
      onTabChange?.(normalized);
    },
    [controlledTab, onTabChange],
  );

  const setScope = useCallback(
    (nextScope: ActivityFeedScope | null) => {
      if (controlledActivityScope === undefined) {
        setUncontrolledActivityScope(nextScope);
      }
      onActivityScopeChange?.(nextScope);
    },
    [controlledActivityScope, onActivityScopeChange],
  );

  const resolvedMessages = messagesProp !== undefined ? messagesProp : (snapshot?.messages ?? null);
  const resolvedJobs =
    jobHistoriesProp !== undefined ? jobHistoriesProp : (snapshot?.job_histories ?? null);
  const resolvedObservation =
    observationProp !== undefined ? observationProp : (snapshot?.observation ?? null);
  const resolvedBounds = boundsProp !== undefined ? boundsProp : (snapshot?.bounds ?? null);

  const evaluatedSample = useMemo(() => {
    if (!nativeSample) {
      return null;
    }
    const refTime = nowMs ?? snapshot?.generated_at_ms ?? Date.now();
    return evaluateWorkspaceNativeSampleAt(
      nativeSample,
      refTime,
      snapshot?.sampling_policy.stale_threshold_ms,
    );
  }, [nativeSample, nowMs, snapshot]);

  const rawAvailability: WorkspaceAvailabilityState =
    evaluatedSample?.effective_availability ?? chat?.availability ?? 'unavailable';
  const effectiveAvailability: WorkspaceAvailabilityState =
    isStale && rawAvailability === 'available' ? 'stale' : rawAvailability;
  const effectiveActivity: WorkspaceActivityState =
    effectiveAvailability === 'available'
      ? (evaluatedSample?.effective_activity ?? chat?.activity ?? 'unknown')
      : 'unknown';
  const effectiveSelection =
    evaluatedSample?.effective_configured_selection ?? chat?.configured_selection ?? null;

  const panelIsStale = isStale || effectiveAvailability === 'stale';
  const referenceNowMs =
    typeof nowMs === 'number' && Number.isFinite(nowMs)
      ? nowMs
      : snapshot?.generated_at_ms ??
        resolvedObservation?.generated_at_ms ??
        Date.now();

  const verifiedChatJobIds = useMemo(
    () => resolveVerifiedChatJobIds(chat, resolvedJobs, resolvedObservation),
    [chat, resolvedJobs, resolvedObservation],
  );

  const scopedJobs = useMemo(
    () =>
      resolvedJobs && chat ? filterJobsForChat(resolvedJobs, chat, verifiedChatJobIds) : [],
    [resolvedJobs, chat, verifiedChatJobIds],
  );

  const activeFocusedJobId =
    focusedJobId !== null && verifiedChatJobIds.has(focusedJobId) ? focusedJobId : null;

  const scopedMessagesCount = useMemo(
    () =>
      resolvedMessages && chat
        ? filterMessagesForChat(resolvedMessages.items, chat, verifiedChatJobIds).length
        : 0,
    [resolvedMessages, chat, verifiedChatJobIds],
  );

  const tabCounts = useMemo<WorkspaceSurfaceTabCounts>(() => {
    const attentionJobs =
      chat?.attention_count ??
      scopedJobs.filter(
        (entry) => entry.current_attention !== null && entry.current_attention.kind !== 'none',
      ).length;
    let activityTotal = 0;
    let runningCalls = 0;
    if (resolvedObservation && chat) {
      const matchedRequests = filterObservationRequestsForActivity(
        resolvedObservation.requests,
        {
          chat,
          knownChatJobIds: verifiedChatJobIds,
          observation: resolvedObservation,
          jobHistories: resolvedJobs,
        },
      );
      activityTotal = matchedRequests.length;
      for (const req of matchedRequests) {
        if (req.state === 'running' || req.children.some((call) => call.state === 'running')) {
          runningCalls += 1;
        }
      }
    }
    return {
      messages: scopedMessagesCount,
      newMessages: pendingNewMessages,
      jobs: scopedJobs.length,
      attentionJobs,
      activity: activityTotal,
      runningActivity: panelIsStale ? 0 : runningCalls,
    };
  }, [
    chat,
    scopedJobs,
    scopedMessagesCount,
    pendingNewMessages,
    resolvedObservation,
    resolvedJobs,
    verifiedChatJobIds,
    panelIsStale,
  ]);

  const codexTargets = useMemo(
    () => (chat ? buildCodexTargets(chat.identity, activeFocusedJobId) : null),
    [chat, activeFocusedJobId],
  );

  const handleInspectJobFromMessagesOrActivity = useCallback(
    (scope: ActivityFeedScope) => {
      setFocusedJobId(scope.job_id);
      setTab('jobs');
    },
    [setTab],
  );

  const handleInspectActivityFromJobs = useCallback(
    (scope: ActivityFeedScope) => {
      setScope(scope);
      setTab('activity');
    },
    [setScope, setTab],
  );

  const availabilityBadge = formatAvailabilityBadge(effectiveAvailability);
  const activityBadge = formatChatActivityBadge(effectiveActivity);
  const configuredDisplay = formatConfiguredSelectionDisplay(effectiveSelection);

  return (
    <div className="tl-wp-root" data-active-tab={activeTab}>
      {chat ? (
        <header className="tl-wp-chat-header">
          <div className="tl-wp-chat-heading">
            <div className="tl-wp-chat-title-row">
              <h2 className="tl-wp-chat-title">{chat.thread_label}</h2>
              {effectiveAvailability !== 'available' ? (
                <span className="tl-wp-pill" data-tone={availabilityBadge.tone}>
                  <span className="tl-wp-pill-label">{availabilityBadge.label}</span>
                </span>
              ) : (
                <span className="tl-wp-pill" data-tone={activityBadge.tone}>
                  {effectiveActivity === 'working' && !panelIsStale && <ActivitySpinner />}
                  <span className="tl-wp-pill-label">{activityBadge.label}</span>
                </span>
              )}
              {chat.attention_count > 0 && (
                <span className="tl-wp-pill" data-tone="attention">
                  <span className="tl-wp-pill-label">
                    {chat.attention_count === 1
                      ? '1 item needs attention'
                      : `${chat.attention_count} items need attention`}
                  </span>
                </span>
              )}
            </div>

            <div className="tl-wp-chat-subtitle">
              <span className="tl-wp-chat-meta-item">
                <FolderGit2 size={13} aria-hidden="true" />
                <span>{chat.project_label}</span>
              </span>
              <span className="tl-wp-meta-sep" aria-hidden="true">
                ·
              </span>
              <span className="tl-wp-chat-meta-item">
                Configured: {configuredDisplay.friendly ?? 'Unavailable'}
              </span>
            </div>

            <details className="tl-wp-header-details">
              <summary className="tl-wp-header-details-summary">Context & diagnostics</summary>
              <div className="tl-wp-header-details-body">
                <div className="tl-wp-kv-row tl-wp-mono">
                  <span>Path: {chat.identity.canonical_project_scope}</span>
                </div>
                <div className="tl-wp-kv-row tl-wp-mono">
                  <span>Source: {chat.identity.source_id}</span>
                  <span>Binding: {chat.identity.binding_id}</span>
                  <span>Thread: {chat.identity.thread_uuid}</span>
                </div>
                <div className="tl-wp-kv-row">
                  <span>Availability: {availabilityBadge.label}</span>
                  <span>{activityBadge.label}</span>
                  <span>
                    Configured model:{' '}
                    {configuredDisplay.raw ?? 'Unavailable (not runtime telemetry)'}
                  </span>
                </div>
                {nativeSample?.connection.route && (
                  <div className="tl-wp-kv-row tl-wp-mono">
                    <span>Route: {nativeSample.connection.route}</span>
                    <span>Freshness: {nativeSample.connection.freshness}</span>
                  </div>
                )}
                {nativeSample?.connection.diagnostic && (
                  <span className="tl-wp-state-desc">{nativeSample.connection.diagnostic}</span>
                )}
              </div>
            </details>
          </div>

          <div className="tl-wp-chat-actions">
            {onRefresh && (
              <button type="button" className="tl-wp-btn tl-wp-btn-subtle" onClick={onRefresh}>
                <RefreshCw size={13} aria-hidden="true" />
                <span>Refresh</span>
              </button>
            )}
            {codexTargets && (onOpenScopedCodex || onOpenCodex) && (
              <button
                type="button"
                className="tl-wp-btn"
                disabled={codexOpenInFlight}
                onClick={() => {
                  if (onOpenScopedCodex) {
                    onOpenScopedCodex(codexTargets.scoped);
                  } else if (onOpenCodex) {
                    onOpenCodex(codexTargets.legacy);
                  }
                }}
              >
                <ExternalLink size={13} aria-hidden="true" />
                <span>{codexOpenInFlight ? 'Opening Codex…' : 'Open in Codex'}</span>
              </button>
            )}
          </div>
        </header>
      ) : (
        <header className="tl-wp-chat-header">
          <div className="tl-wp-chat-heading">
            <h2 className="tl-wp-chat-title">Workspace</h2>
            <div className="tl-wp-chat-subtitle">
              <span>Select a chat to inspect Messages, Jobs, and Activity.</span>
            </div>
          </div>
        </header>
      )}

      <WorkspaceSurfaceTabs
        activeTab={activeTab}
        onTabChange={setTab}
        idPrefix={idPrefix}
        counts={chat ? tabCounts : undefined}
      />

      <div
        id={getWorkspaceSurfacePanelId(idPrefix, 'messages')}
        role="tabpanel"
        aria-labelledby={getWorkspaceSurfaceTabId(idPrefix, 'messages')}
        className="tl-wp-tabpanel"
        hidden={activeTab !== 'messages'}
        tabIndex={0}
      >
        <WorkspaceMessagesPanel
          chat={chat}
          messages={resolvedMessages}
          jobHistories={resolvedJobs}
          bounds={resolvedBounds}
          knownChatJobIds={verifiedChatJobIds}
          nowMs={referenceNowMs}
          isActive={activeTab === 'messages'}
          loading={loading}
          error={error}
          isStale={panelIsStale}
          staleReason={staleReason}
          bottomThresholdPx={bottomThresholdPx}
          onRefresh={onRefresh}
          onInspectJob={handleInspectJobFromMessagesOrActivity}
          onPendingNewCountChange={setPendingNewMessages}
        />
      </div>

      <div
        id={getWorkspaceSurfacePanelId(idPrefix, 'jobs')}
        role="tabpanel"
        aria-labelledby={getWorkspaceSurfaceTabId(idPrefix, 'jobs')}
        className="tl-wp-tabpanel"
        hidden={activeTab !== 'jobs'}
        tabIndex={0}
      >
        <WorkspaceJobsPanel
          chat={chat}
          jobHistories={resolvedJobs}
          knownChatJobIds={verifiedChatJobIds}
          bounds={resolvedBounds}
          jobsHasMore={Boolean(resolvedObservation?.jobs_has_more)}
          focusedJobId={activeFocusedJobId}
          nowMs={referenceNowMs}
          loading={loading}
          error={error}
          isStale={panelIsStale}
          staleReason={staleReason}
          onRefresh={onRefresh}
          onInspectActivityForJob={handleInspectActivityFromJobs}
        />
      </div>

      <div
        id={getWorkspaceSurfacePanelId(idPrefix, 'activity')}
        role="tabpanel"
        aria-labelledby={getWorkspaceSurfaceTabId(idPrefix, 'activity')}
        className="tl-wp-tabpanel"
        hidden={activeTab !== 'activity'}
        tabIndex={0}
      >
        <WorkspaceActivityPanel
          chat={chat}
          observation={resolvedObservation}
          jobHistories={resolvedJobs}
          bounds={resolvedBounds}
          knownChatJobIds={verifiedChatJobIds}
          scope={effectiveActivityScope}
          loading={loading}
          error={error}
          isStale={panelIsStale}
          staleReason={staleReason}
          nowMs={referenceNowMs}
          onRefresh={onRefresh}
          onClearScope={() => setScope(null)}
          onSelectJob={handleInspectJobFromMessagesOrActivity}
        />
      </div>
    </div>
  );
}

export const WorkspacePanels = WorkspaceChatPanels;
