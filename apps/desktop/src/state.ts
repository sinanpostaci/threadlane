import {
  DEFAULT_APPEARANCE,
  DEFAULT_EXECUTION_PREFERENCES,
  type AppearancePreferences,
  type DesktopSnapshot,
  type DetailTab,
  type DetailTarget,
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
  return {
    page: 'activity',
    selectedBindingId: null,
    providerFilter: 'all',
    showFilters: false,
    completedExpanded: false,
    snapshot: createInitialSnapshot(nowMs),
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
