import {
  DEFAULT_APPEARANCE,
  DEFAULT_EXECUTION_PREFERENCES,
  type ActivityPage,
  type AppearancePreferences,
  type DesktopSnapshot,
  type DetailTab,
  type DetailTarget,
  type EventKind,
  type ExecutionPreferences,
  type NavPage,
  type PendingMutation,
  type RunDetail,
  type TaskProviderFilter,
  type ToolStatus,
} from './types.ts';
import {
  createEmptyRegistrySnapshot,
  type PlanRevision,
  type RegistrySnapshot,
} from './registry/types.ts';
import {
  DEFAULT_HISTORY_PAGE_LIMIT,
  MAX_RETAINED_HISTORY_EVENTS,
  applyHistoryFilters,
  clearHistoryFilters,
  computeHistoryScopeKey,
  createInitialHistoryState,
  mergeAndCapHistoryEvents,
  resolveNextHistoryCursorOrAfterSeq,
  setHistoryRunIdDraft,
  syncHistoryScope,
  toActivityHistoryIpcArgs,
  toggleHistoryEventDetail,
  toggleHistoryExpanded,
  type HistoryLoadingMode,
  type HistoryQueryTicket,
  type HistorySubViewState,
} from './pages/history.ts';

export {
  DEFAULT_HISTORY_PAGE_LIMIT,
  MAX_RETAINED_HISTORY_EVENTS,
  computeHistoryScopeKey,
  resolveNextHistoryCursorOrAfterSeq,
  toActivityHistoryIpcArgs,
  type HistoryLoadingMode,
  type HistoryQueryTicket,
  type HistorySubViewState,
};

export interface SnapshotTicket {
  readonly seq: number;
  readonly epoch: number;
  readonly requestedBindingId: string | null;
}

export interface MutationTicket {
  readonly seq: number;
  readonly epoch: number;
  readonly kind: NonNullable<PendingMutation>;
}

export interface DetailTicket {
  readonly seq: number;
  readonly bindingId: string;
  readonly runId: string;
}

export interface RegistryTicket {
  readonly seq: number;
}

export interface PlansTicket {
  readonly seq: number;
}

export interface DesktopStoreState {
  page: NavPage;
  selectedBindingId: string | null;
  providerFilter: TaskProviderFilter;
  showFilters: boolean;
  completedExpanded: boolean;
  snapshot: DesktopSnapshot;
  snapshotLoading: boolean;
  pollInFlight: boolean;
  selectionEpoch: number;
  snapshotRequestSeq: number;
  detailTarget: DetailTarget | null;
  detailTab: DetailTab;
  runDetail: RunDetail | null;
  detailLoading: boolean;
  detailError: string | null;
  detailInFlight: boolean;
  detailRequestSeq: number;
  selectedToolId: string | null;
  tools: ToolStatus[];
  toolsLoading: boolean;
  toolsError: string | null;
  registrySnapshot: RegistrySnapshot;
  registryLoading: boolean;
  registryInFlight: boolean;
  registryRequestSeq: number;
  registryError: string | null;
  plansSnapshot: PlanRevision[];
  plansLoading: boolean;
  plansInFlight: boolean;
  plansRequestSeq: number;
  plansError: string | null;
  pendingMutation: PendingMutation;
  actionError: string | null;
  stateDirDraft: string;
  appearance: AppearancePreferences;
  executionPrefs: ExecutionPreferences;
  isDevPreviewFixture: boolean;
  history: HistorySubViewState;
}

export function createInitialSnapshot(nowMs: number = Date.now()): DesktopSnapshot {
  return {
    attachment: {
      mode: 'unconfigured',
      connection: 'unconfigured',
      state_dir: null,
      message: null,
      updated_at_ms: nowMs,
    },
    bindings: [],
    selected_binding_id: null,
    tasks: [],
    total_runs: 0,
    active_runs: 0,
    has_more: false,
    demo_controls: {
      available: false,
      run_id: null,
      binding_id: null,
      can_answer: false,
      can_cancel: false,
      revision: null,
    },
  };
}

export function createInitialState(
  appearance: AppearancePreferences = { ...DEFAULT_APPEARANCE },
  nowMs: number = Date.now(),
  executionPrefs: ExecutionPreferences = { ...DEFAULT_EXECUTION_PREFERENCES }
): DesktopStoreState {
  const initialSnapshot = createInitialSnapshot(nowMs);
  return {
    page: 'activity',
    selectedBindingId: null,
    providerFilter: 'all',
    showFilters: false,
    completedExpanded: false,
    snapshot: initialSnapshot,
    snapshotLoading: true,
    pollInFlight: false,
    selectionEpoch: 1,
    snapshotRequestSeq: 0,
    detailTarget: null,
    detailTab: 'overview',
    runDetail: null,
    detailLoading: false,
    detailInFlight: false,
    detailError: null,
    detailRequestSeq: 0,
    selectedToolId: null,
    tools: [],
    toolsLoading: true,
    toolsError: null,
    registrySnapshot: createEmptyRegistrySnapshot(),
    registryLoading: true,
    registryInFlight: false,
    registryRequestSeq: 0,
    registryError: null,
    plansSnapshot: [],
    plansLoading: true,
    plansInFlight: false,
    plansRequestSeq: 0,
    plansError: null,
    pendingMutation: null,
    actionError: null,
    stateDirDraft: appearance.rememberStateDir ? appearance.savedStateDir : '',
    appearance: { ...appearance },
    executionPrefs: { ...executionPrefs },
    isDevPreviewFixture: false,
    history: createInitialHistoryState(initialSnapshot.attachment, null),
  };
}

/**
 * Returns true only when a background 2s runtime observer poll should rebuild the main pane.
 * Editable pages (`tools`, `profiles`, `plans`, `settings`, `chats`) are never rebuilt on background
 * runtime polls so that form drafts, focus, and scroll positions remain intact.
 */
export function shouldRebuildMainOnPoll(state: DesktopStoreState): boolean {
  if (state.detailTarget !== null) {
    return true;
  }
  return state.page === 'activity';
}

function resolveScopedBindingId(
  snapshot: DesktopSnapshot,
  preferredBindingId: string | null
): string | null {
  if (snapshot.bindings.length === 0) {
    return null;
  }
  if (
    preferredBindingId !== null &&
    snapshot.bindings.some((b) => b.binding_id === preferredBindingId)
  ) {
    return preferredBindingId;
  }
  if (
    snapshot.selected_binding_id !== null &&
    snapshot.bindings.some((b) => b.binding_id === snapshot.selected_binding_id)
  ) {
    return snapshot.selected_binding_id;
  }
  return snapshot.bindings[0].binding_id;
}

export function beginSnapshotPoll(
  state: DesktopStoreState,
  force: boolean = false
): { state: DesktopStoreState; ticket: SnapshotTicket | null } {
  if (state.pendingMutation !== null) {
    return { state, ticket: null };
  }
  if (state.pollInFlight && !force) {
    return { state, ticket: null };
  }
  const nextSeq = state.snapshotRequestSeq + 1;
  const ticket: SnapshotTicket = {
    seq: nextSeq,
    epoch: state.selectionEpoch,
    requestedBindingId: state.selectedBindingId,
  };
  return {
    state: {
      ...state,
      pollInFlight: true,
      snapshotRequestSeq: nextSeq,
    },
    ticket,
  };
}

export function applySnapshotPollSuccess(
  state: DesktopStoreState,
  ticket: SnapshotTicket,
  snapshot: DesktopSnapshot
): DesktopStoreState {
  if (
    ticket.epoch !== state.selectionEpoch ||
    ticket.seq < state.snapshotRequestSeq ||
    state.pendingMutation !== null
  ) {
    return state;
  }

  const nextSelectedBindingId = resolveScopedBindingId(snapshot, state.selectedBindingId);
  const nextDraft =
    state.stateDirDraft.trim().length === 0 && snapshot.attachment.state_dir
      ? snapshot.attachment.state_dir
      : state.stateDirDraft;

  let nextDetailTarget = state.detailTarget;
  let nextRunDetail = state.runDetail;
  let nextDetailLoading = state.detailLoading;
  let nextDetailInFlight = state.detailInFlight;
  let nextDetailError = state.detailError;
  let nextDetailSeq = state.detailRequestSeq;

  if (
    nextDetailTarget !== null &&
    snapshot.bindings.length > 0 &&
    !snapshot.bindings.some((b) => b.binding_id === nextDetailTarget?.bindingId)
  ) {
    nextDetailTarget = null;
    nextRunDetail = null;
    nextDetailLoading = false;
    nextDetailInFlight = false;
    nextDetailError = null;
    nextDetailSeq += 1;
  }

  return {
    ...state,
    snapshot,
    selectedBindingId: nextSelectedBindingId,
    snapshotLoading: false,
    pollInFlight: false,
    stateDirDraft: nextDraft,
    detailTarget: nextDetailTarget,
    runDetail: nextRunDetail,
    detailLoading: nextDetailLoading,
    detailInFlight: nextDetailInFlight,
    detailError: nextDetailError,
    detailRequestSeq: nextDetailSeq,
    history: syncHistoryScope(state.history, snapshot.attachment, nextSelectedBindingId),
  };
}

export function applySnapshotPollError(
  state: DesktopStoreState,
  ticket: SnapshotTicket,
  errorMessage: string
): DesktopStoreState {
  if (
    ticket.epoch !== state.selectionEpoch ||
    ticket.seq < state.snapshotRequestSeq ||
    state.pendingMutation !== null
  ) {
    return state;
  }
  return {
    ...state,
    snapshotLoading: false,
    pollInFlight: false,
    actionError: errorMessage,
  };
}

export function beginRegistryRefresh(
  state: DesktopStoreState,
  force: boolean = true
): { state: DesktopStoreState; ticket: RegistryTicket | null } {
  if (state.registryInFlight && !force) {
    return { state, ticket: null };
  }
  const nextSeq = state.registryRequestSeq + 1;
  return {
    state: {
      ...state,
      registryInFlight: true,
      registryRequestSeq: nextSeq,
    },
    ticket: { seq: nextSeq },
  };
}

export function applyRegistryRefreshSuccess(
  state: DesktopStoreState,
  ticket: RegistryTicket,
  registrySnapshot: RegistrySnapshot
): DesktopStoreState {
  if (ticket.seq < state.registryRequestSeq) {
    return state;
  }
  return {
    ...state,
    registrySnapshot,
    registryLoading: false,
    registryInFlight: false,
    registryError: null,
  };
}

export function applyRegistryRefreshError(
  state: DesktopStoreState,
  ticket: RegistryTicket,
  errorMessage: string
): DesktopStoreState {
  if (ticket.seq < state.registryRequestSeq) {
    return state;
  }
  return {
    ...state,
    registryLoading: false,
    registryInFlight: false,
    registryError: errorMessage,
  };
}

export function beginPlansRefresh(
  state: DesktopStoreState,
  force: boolean = true
): { state: DesktopStoreState; ticket: PlansTicket | null } {
  if (state.plansInFlight && !force) {
    return { state, ticket: null };
  }
  const nextSeq = state.plansRequestSeq + 1;
  return {
    state: {
      ...state,
      plansInFlight: true,
      plansRequestSeq: nextSeq,
    },
    ticket: { seq: nextSeq },
  };
}

export function applyPlansRefreshSuccess(
  state: DesktopStoreState,
  ticket: PlansTicket,
  plansSnapshot: PlanRevision[]
): DesktopStoreState {
  if (ticket.seq < state.plansRequestSeq) {
    return state;
  }
  return {
    ...state,
    plansSnapshot,
    plansLoading: false,
    plansInFlight: false,
    plansError: null,
  };
}

export function applyPlansRefreshError(
  state: DesktopStoreState,
  ticket: PlansTicket,
  errorMessage: string
): DesktopStoreState {
  if (ticket.seq < state.plansRequestSeq) {
    return state;
  }
  return {
    ...state,
    plansLoading: false,
    plansInFlight: false,
    plansError: errorMessage,
  };
}

export function selectBinding(
  state: DesktopStoreState,
  bindingId: string | null,
  switchToActivity: boolean = false
): { state: DesktopStoreState; ticket: SnapshotTicket | null } {
  if (state.pendingMutation !== null) {
    return { state, ticket: null };
  }
  const nextEpoch = state.selectionEpoch + 1;
  const nextSeq = state.snapshotRequestSeq + 1;
  const clearDetail =
    state.detailTarget !== null && state.detailTarget.bindingId !== bindingId;

  const nextState: DesktopStoreState = {
    ...state,
    page: switchToActivity ? 'activity' : state.page,
    selectedBindingId: bindingId,
    selectionEpoch: nextEpoch,
    snapshotRequestSeq: nextSeq,
    pollInFlight: true,
    actionError: null,
    detailTarget: clearDetail ? null : state.detailTarget,
    runDetail: clearDetail ? null : state.runDetail,
    detailLoading: clearDetail ? false : state.detailLoading,
    detailInFlight: clearDetail ? false : state.detailInFlight,
    detailError: clearDetail ? null : state.detailError,
    detailRequestSeq: clearDetail ? state.detailRequestSeq + 1 : state.detailRequestSeq,
    history: syncHistoryScope(state.history, state.snapshot.attachment, bindingId),
  };

  return {
    state: nextState,
    ticket: {
      seq: nextSeq,
      epoch: nextEpoch,
      requestedBindingId: bindingId,
    },
  };
}

export function beginMutation(
  state: DesktopStoreState,
  kind: NonNullable<PendingMutation>
): { state: DesktopStoreState; ticket: MutationTicket | null } {
  if (state.pendingMutation !== null) {
    return { state, ticket: null };
  }
  const nextEpoch = state.selectionEpoch + 1;
  const nextSeq = state.snapshotRequestSeq + 1;
  const ticket: MutationTicket = {
    seq: nextSeq,
    epoch: nextEpoch,
    kind,
  };
  return {
    state: {
      ...state,
      pendingMutation: kind,
      pollInFlight: false,
      selectionEpoch: nextEpoch,
      snapshotRequestSeq: nextSeq,
      actionError: null,
    },
    ticket,
  };
}

export function applyMutationSuccess(
  state: DesktopStoreState,
  ticket: MutationTicket,
  snapshot: DesktopSnapshot
): DesktopStoreState {
  if (ticket.epoch !== state.selectionEpoch || state.pendingMutation !== ticket.kind) {
    return state;
  }

  const nextSelectedBindingId = resolveScopedBindingId(snapshot, snapshot.selected_binding_id);
  const nextDraft =
    snapshot.attachment.state_dir && ticket.kind === 'attach'
      ? snapshot.attachment.state_dir
      : state.stateDirDraft;

  let nextDetailTarget = state.detailTarget;
  let nextRunDetail = state.runDetail;
  let nextDetailLoading = state.detailLoading;
  let nextDetailInFlight = state.detailInFlight;
  let nextDetailError = state.detailError;
  let nextDetailSeq = state.detailRequestSeq;

  if (ticket.kind === 'attach' || ticket.kind === 'start_demo') {
    nextDetailTarget = null;
    nextRunDetail = null;
    nextDetailLoading = false;
    nextDetailInFlight = false;
    nextDetailError = null;
    nextDetailSeq += 1;
  }

  const isDemoAction =
    ticket.kind === 'start_demo' ||
    ticket.kind === 'answer_demo' ||
    ticket.kind === 'cancel_demo';

  const nextPage: NavPage =
    isDemoAction || ticket.kind === 'attach' ? 'activity' : state.page;
  const nextCompletedExpanded = isDemoAction ? true : state.completedExpanded;

  return {
    ...state,
    page: nextPage,
    completedExpanded: nextCompletedExpanded,
    snapshot,
    selectedBindingId: nextSelectedBindingId,
    snapshotLoading: false,
    pollInFlight: false,
    pendingMutation: null,
    actionError: null,
    stateDirDraft: nextDraft,
    detailTarget: nextDetailTarget,
    runDetail: nextRunDetail,
    detailLoading: nextDetailLoading,
    detailInFlight: nextDetailInFlight,
    detailError: nextDetailError,
    detailRequestSeq: nextDetailSeq,
    history: syncHistoryScope(state.history, snapshot.attachment, nextSelectedBindingId),
  };
}

export function applyMutationError(
  state: DesktopStoreState,
  ticket: MutationTicket,
  errorMessage: string
): DesktopStoreState {
  if (ticket.epoch !== state.selectionEpoch || state.pendingMutation !== ticket.kind) {
    return state;
  }
  return {
    ...state,
    pendingMutation: null,
    pollInFlight: false,
    actionError: errorMessage,
  };
}

export function openRunDetail(
  state: DesktopStoreState,
  bindingId: string,
  runId: string
): { state: DesktopStoreState; ticket: DetailTicket } {
  const nextSeq = state.detailRequestSeq + 1;
  const ticket: DetailTicket = {
    seq: nextSeq,
    bindingId,
    runId,
  };
  return {
    state: {
      ...state,
      detailTarget: { bindingId, runId },
      detailTab: 'overview',
      runDetail:
        state.runDetail &&
        state.runDetail.task.binding_id === bindingId &&
        state.runDetail.task.run_id === runId
          ? state.runDetail
          : null,
      detailLoading: true,
      detailInFlight: true,
      detailError: null,
      detailRequestSeq: nextSeq,
    },
    ticket,
  };
}

export function beginRunDetailRefresh(
  state: DesktopStoreState,
  force: boolean = false
): { state: DesktopStoreState; ticket: DetailTicket | null } {
  if (!state.detailTarget) {
    return { state, ticket: null };
  }
  if (state.detailInFlight && !force) {
    return { state, ticket: null };
  }
  const nextSeq = state.detailRequestSeq + 1;
  const ticket: DetailTicket = {
    seq: nextSeq,
    bindingId: state.detailTarget.bindingId,
    runId: state.detailTarget.runId,
  };
  return {
    state: {
      ...state,
      detailInFlight: true,
      detailRequestSeq: nextSeq,
    },
    ticket,
  };
}

export function applyRunDetailSuccess(
  state: DesktopStoreState,
  ticket: DetailTicket,
  detail: RunDetail
): DesktopStoreState {
  if (
    state.detailTarget === null ||
    state.detailTarget.bindingId !== ticket.bindingId ||
    state.detailTarget.runId !== ticket.runId ||
    detail.task.binding_id !== ticket.bindingId ||
    detail.task.run_id !== ticket.runId ||
    ticket.seq !== state.detailRequestSeq
  ) {
    return state;
  }
  return {
    ...state,
    runDetail: detail,
    detailLoading: false,
    detailInFlight: false,
    detailError: null,
  };
}

export function applyRunDetailError(
  state: DesktopStoreState,
  ticket: DetailTicket,
  errorMessage: string
): DesktopStoreState {
  if (
    state.detailTarget === null ||
    state.detailTarget.bindingId !== ticket.bindingId ||
    state.detailTarget.runId !== ticket.runId ||
    ticket.seq !== state.detailRequestSeq
  ) {
    return state;
  }
  return {
    ...state,
    detailLoading: false,
    detailInFlight: false,
    detailError: errorMessage,
  };
}

export function navigateBack(state: DesktopStoreState): DesktopStoreState {
  if (state.detailTarget !== null) {
    return {
      ...state,
      detailTarget: null,
      runDetail: null,
      detailLoading: false,
      detailInFlight: false,
      detailError: null,
      detailRequestSeq: state.detailRequestSeq + 1,
    };
  }
  if (state.selectedToolId !== null) {
    return {
      ...state,
      selectedToolId: null,
    };
  }
  return state;
}

export function navigateToPage(state: DesktopStoreState, page: NavPage): DesktopStoreState {
  const clearDetail = state.detailTarget !== null;
  const pauseHistoryPoll = page !== 'activity' && state.history.inFlight;
  return {
    ...state,
    page,
    selectedToolId: null,
    detailTarget: null,
    runDetail: null,
    detailLoading: false,
    detailInFlight: false,
    detailError: null,
    detailRequestSeq: clearDetail ? state.detailRequestSeq + 1 : state.detailRequestSeq,
    history: pauseHistoryPoll
      ? {
          ...state.history,
          inFlight: false,
          loadingMode: null,
          requestSeq: state.history.requestSeq + 1,
        }
      : state.history,
  };
}

export function resolveExpectedDemoRevision(state: DesktopStoreState): number | null {
  if (
    state.runDetail &&
    state.runDetail.demo_controls.available &&
    state.runDetail.demo_controls.revision !== null
  ) {
    return state.runDetail.demo_controls.revision;
  }
  if (
    state.snapshot.demo_controls.available &&
    state.snapshot.demo_controls.revision !== null
  ) {
    return state.snapshot.demo_controls.revision;
  }
  return null;
}

export function resolveActiveHistoryBindingId(state: DesktopStoreState): string | null {
  return state.selectedBindingId ?? state.snapshot.selected_binding_id;
}

/**
 * Returns true only when the Activity History subview should be polled in the background:
 * - User is on the `activity` page (with no run detail overlay open)
 * - The History subview toggle is expanded
 * - Document/window is not hidden in the background
 * - Runtime attachment is connected and a binding is selected
 * - No mutation or history request is currently in flight
 */
export function shouldPollHistory(
  state: DesktopStoreState,
  isDocumentHidden: boolean = false
): boolean {
  if (isDocumentHidden) {
    return false;
  }
  if (state.page !== 'activity' || state.detailTarget !== null) {
    return false;
  }
  if (!state.history.expanded || state.history.inFlight) {
    return false;
  }
  if (state.pendingMutation !== null) {
    return false;
  }
  if (
    state.snapshot.attachment.connection !== 'connected' ||
    state.snapshot.attachment.mode === 'unconfigured'
  ) {
    return false;
  }
  return resolveActiveHistoryBindingId(state) !== null;
}

export function setHistoryExpanded(
  state: DesktopStoreState,
  expanded?: boolean
): DesktopStoreState {
  const synced = syncHistoryScope(
    state.history,
    state.snapshot.attachment,
    resolveActiveHistoryBindingId(state)
  );
  return {
    ...state,
    history: toggleHistoryExpanded(synced, expanded),
  };
}

export function updateHistoryRunIdDraft(
  state: DesktopStoreState,
  draft: string
): DesktopStoreState {
  const nextHistory = setHistoryRunIdDraft(state.history, draft);
  if (nextHistory === state.history) {
    return state;
  }
  return {
    ...state,
    history: nextHistory,
  };
}

export function commitHistoryFilters(
  state: DesktopStoreState,
  runIdInput?: string | null,
  kindFilter?: EventKind | null
): DesktopStoreState {
  return {
    ...state,
    history: applyHistoryFilters(
      state.history,
      state.snapshot.attachment,
      resolveActiveHistoryBindingId(state),
      runIdInput,
      kindFilter
    ),
  };
}

export function resetHistoryFilters(state: DesktopStoreState): DesktopStoreState {
  return {
    ...state,
    history: clearHistoryFilters(
      state.history,
      state.snapshot.attachment,
      resolveActiveHistoryBindingId(state)
    ),
  };
}

export function selectHistoryEvent(
  state: DesktopStoreState,
  eventId: string | null
): DesktopStoreState {
  return {
    ...state,
    history: toggleHistoryEventDetail(state.history, eventId),
  };
}

export function beginHistoryQuery(
  state: DesktopStoreState,
  mode: Exclude<HistoryLoadingMode, null> = 'refresh',
  force: boolean = false,
  limit: number = DEFAULT_HISTORY_PAGE_LIMIT,
  isDocumentHidden: boolean = false
): { state: DesktopStoreState; ticket: HistoryQueryTicket | null } {
  const bindingId = resolveActiveHistoryBindingId(state);
  const syncedHistory = syncHistoryScope(
    state.history,
    state.snapshot.attachment,
    bindingId
  );
  const syncedState: DesktopStoreState =
    syncedHistory === state.history ? state : { ...state, history: syncedHistory };

  if (!bindingId || syncedState.pendingMutation !== null) {
    return { state: syncedState, ticket: null };
  }

  if (mode === 'poll' && !shouldPollHistory(syncedState, isDocumentHidden)) {
    return { state: syncedState, ticket: null };
  }

  if (syncedState.history.inFlight && !force) {
    return { state: syncedState, ticket: null };
  }

  if (mode === 'more' && syncedState.history.nextCursor === null) {
    return { state: syncedState, ticket: null };
  }

  const { cursor, afterSeq } = resolveNextHistoryCursorOrAfterSeq(syncedState.history);
  const nextSeq = syncedState.history.requestSeq + 1;
  const boundedLimit = Math.max(1, Math.min(50, limit));

  const ticket: HistoryQueryTicket = {
    seq: nextSeq,
    queryEpoch: syncedState.history.queryEpoch,
    scopeKey: syncedState.history.scopeKey,
    bindingId,
    afterSeq,
    cursor,
    limit: boundedLimit,
    runId: syncedState.history.runIdFilter,
    kind: syncedState.history.kindFilter,
    mode,
  };

  return {
    state: {
      ...syncedState,
      history: {
        ...syncedState.history,
        inFlight: true,
        loadingMode: mode,
        requestSeq: nextSeq,
        error: mode === 'poll' ? syncedState.history.error : null,
      },
    },
    ticket,
  };
}

export function applyHistoryPageSuccess(
  state: DesktopStoreState,
  ticket: HistoryQueryTicket,
  page: ActivityPage,
  maxRetained: number = MAX_RETAINED_HISTORY_EVENTS
): DesktopStoreState {
  const activeBindingId = resolveActiveHistoryBindingId(state);
  if (
    ticket.queryEpoch !== state.history.queryEpoch ||
    ticket.seq !== state.history.requestSeq ||
    ticket.scopeKey !== state.history.scopeKey ||
    ticket.bindingId !== page.binding_id ||
    activeBindingId !== page.binding_id
  ) {
    return state;
  }

  const mergedEvents = mergeAndCapHistoryEvents(
    state.history.events,
    page.events,
    page.binding_id,
    maxRetained
  );

  const nextHasMore = Boolean(page.has_more && page.next_cursor);
  const nextCursor = nextHasMore ? page.next_cursor : null;
  const nextHighWater = Math.max(
    state.history.highWaterMarkSeq,
    page.high_water_mark_seq
  );

  // Only advance `completedHighWaterSeq` when the anchored window is completely drained
  // (`nextHasMore === false` / `nextCursor === null`). When drained, even an empty filtered
  // page advances `completedHighWaterSeq` to `page.high_water_mark_seq`.
  const nextCompletedHighWater = nextHasMore
    ? state.history.completedHighWaterSeq
    : Math.max(state.history.completedHighWaterSeq, page.high_water_mark_seq);

  const selectedStillRetained =
    state.history.selectedEventId !== null &&
    mergedEvents.some((event) => event.event_id === state.history.selectedEventId);

  return {
    ...state,
    history: {
      ...state.history,
      inFlight: false,
      loadingMode: null,
      events: mergedEvents,
      nextCursor,
      hasMore: nextHasMore,
      highWaterMarkSeq: nextHighWater,
      completedHighWaterSeq: nextCompletedHighWater,
      lastAfterSeq: page.after_seq,
      error: null,
      selectedEventId: selectedStillRetained ? state.history.selectedEventId : null,
      hasLoadedOnce: true,
    },
  };
}

export function applyHistoryPageError(
  state: DesktopStoreState,
  ticket: HistoryQueryTicket,
  errorMessage: string
): DesktopStoreState {
  if (
    ticket.queryEpoch !== state.history.queryEpoch ||
    ticket.seq !== state.history.requestSeq ||
    ticket.scopeKey !== state.history.scopeKey
  ) {
    return state;
  }

  return {
    ...state,
    history: {
      ...state.history,
      inFlight: false,
      loadingMode: null,
      error: errorMessage,
      hasLoadedOnce: true,
    },
  };
}
