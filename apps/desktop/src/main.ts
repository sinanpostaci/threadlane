import './styles.css';

import {
  ACCENT_PALETTES,
  DEFAULT_APPEARANCE,
  DEFAULT_EXECUTION_PREFERENCES,
  EXECUTION_PREFERENCE_BOUNDS,
  type AccentName,
  type AppearancePreferences,
  type DetailTab,
  type EventKind,
  type ExecutionPreferences,
  type NavPage,
  type SurfaceMode,
  type TaskProviderFilter,
  type TaskSummary,
  type ThemeMode,
  type ToolStatus,
} from './types.ts';
import {
  classifyTaskBucket,
  formatAttachmentBadge,
  formatBindingLabel,
  formatEvidenceFieldCheck,
  formatEvidenceSummary,
  formatFooterStatus,
  formatModelAndEffort,
  formatProviderName,
  formatRelativeTimestamp,
  formatStatusLabel,
  formatToolAvailability,
  matchesProviderFilter,
  resolveTaskBrand,
  resolveToolBrand,
  sanitizeAppearancePreferences,
  sanitizeExecutionPreferences,
} from './format.ts';
import {
  applyHistoryPageError,
  applyHistoryPageSuccess,
  applyMutationError,
  applyMutationSuccess,
  applyPlansRefreshError,
  applyPlansRefreshSuccess,
  applyRegistryRefreshError,
  applyRegistryRefreshSuccess,
  applyRunDetailError,
  applyRunDetailSuccess,
  applySnapshotPollError,
  applySnapshotPollSuccess,
  beginHistoryQuery,
  beginMutation,
  beginPlansRefresh,
  beginRegistryRefresh,
  beginRunDetailRefresh,
  beginSnapshotPoll,
  commitHistoryFilters,
  createInitialState,
  hasPlansSnapshotChanged,
  hasRegistrySnapshotChanged,
  navigateBack,
  navigateToPage,
  openRunDetail,
  resetHistoryFilters,
  resolveExpectedDemoRevision,
  selectBinding,
  selectHistoryEvent,
  setHistoryExpanded,
  shouldPollActivePlansStore,
  shouldPollHistory,
  shouldRebuildMainOnPoll,
  toActivityHistoryIpcArgs,
  updateHistoryRunIdDraft,
  type DesktopStoreState,
  type DetailTicket,
  type HistoryLoadingMode,
  type HistoryQueryTicket,
  type SnapshotTicket,
} from './state.ts';
import {
  hasTauriRuntime,
  ipcDesktopActivityHistory,
  ipcDesktopAnswerDemo,
  ipcDesktopAttach,
  ipcDesktopCancelDemo,
  ipcDesktopRunDetail,
  ipcDesktopSnapshot,
  ipcDesktopStartDemo,
  ipcDesktopToolStatus,
  ipcDesktopWindowAction,
  isDevPreviewFixtureMode,
} from './ipc.ts';
import {
  desktop_decision_models,
  desktop_plans_snapshot,
  desktop_registry_snapshot,
  desktop_test_decision,
  formatAppError,
  type DecisionAdvice,
  type DecisionProvider,
  type ModelDefinition,
} from './registry/ipc.ts';
import { createBrandIconElement, createUiIcon, type UiIconName } from './icons.ts';
import { createButton, h } from './ui/dom.ts';
import { ToolsPage, isConfigurableDiscoveredCli } from './pages/tools.ts';
import { ProfilesPage } from './pages/profiles.ts';
import { PlansPage } from './pages/plans.ts';
import { NativeLinkingPanel } from './pages/native.ts';
import { renderActivityHistorySection } from './pages/history.ts';

const APPEARANCE_STORAGE_KEY = 'threadlane.desktop.appearance.v1';
const EXECUTION_PREFS_STORAGE_KEY = 'threadlane.desktop.execution_prefs.v1';
const POLL_INTERVAL_MS = 2000;

type StoreRenderMode = 'auto' | 'defer';
type SnapshotRenderMode = 'preserve' | 'defer';

const NAV_ITEMS: ReadonlyArray<{ page: NavPage; label: string; icon: UiIconName }> = [
  { page: 'activity', label: 'Activity', icon: 'activity' },
  { page: 'chats', label: 'Chats', icon: 'messages-square' },
  { page: 'plans', label: 'Plans', icon: 'clipboard-list' },
  { page: 'tools', label: 'Tools', icon: 'blocks' },
  { page: 'profiles', label: 'Profiles', icon: 'sliders-horizontal' },
  { page: 'settings', label: 'Settings', icon: 'settings-2' },
];

const PROVIDER_FILTERS: ReadonlyArray<{ id: TaskProviderFilter; label: string }> = [
  { id: 'all', label: 'All providers' },
  { id: 'opencode', label: 'OpenCode' },
  { id: 'antigravity', label: 'Antigravity' },
  { id: 'codex', label: 'Codex' },
  { id: 'demo', label: 'Local demo' },
];

function loadSavedAppearance(): AppearancePreferences {
  try {
    const raw = window.localStorage.getItem(APPEARANCE_STORAGE_KEY);
    if (!raw) {
      return { ...DEFAULT_APPEARANCE };
    }
    return sanitizeAppearancePreferences(JSON.parse(raw));
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

function saveAppearance(prefs: AppearancePreferences): void {
  try {
    const persisted: AppearancePreferences = {
      theme: prefs.theme,
      surface: prefs.surface,
      opacity: prefs.opacity,
      accent: prefs.accent,
      rememberStateDir: prefs.rememberStateDir,
      savedStateDir: prefs.rememberStateDir ? prefs.savedStateDir.trim().slice(0, 512) : '',
    };
    window.localStorage.setItem(APPEARANCE_STORAGE_KEY, JSON.stringify(persisted));
  } catch {
    // Ignore storage write errors in restricted webviews.
  }
}

function loadSavedExecutionPreferences(): ExecutionPreferences {
  try {
    const raw = window.localStorage.getItem(EXECUTION_PREFS_STORAGE_KEY);
    if (!raw) {
      return { ...DEFAULT_EXECUTION_PREFERENCES };
    }
    return sanitizeExecutionPreferences(JSON.parse(raw));
  } catch {
    return { ...DEFAULT_EXECUTION_PREFERENCES };
  }
}

function saveExecutionPreferences(prefs: ExecutionPreferences): void {
  try {
    const sanitized = sanitizeExecutionPreferences(prefs);
    window.localStorage.setItem(EXECUTION_PREFS_STORAGE_KEY, JSON.stringify(sanitized));
  } catch {
    // Ignore storage write errors in restricted webviews.
  }
}

interface DecisionTestUiState {
  selectedToolId: string;
  selectedModel: string;
  fetchedModels: ModelDefinition[];
  loadingModels: boolean;
  runningTest: boolean;
  advice: DecisionAdvice | null;
  error: string | null;
}

class ThreadlaneDesktopApp {
  private state: DesktopStoreState;
  private readonly root: HTMLElement;
  private readonly headerEl: HTMLElement;
  private readonly mainEl: HTMLElement;
  private readonly footerEl: HTMLElement;
  private readonly navEl: HTMLElement;
  private readonly toolsPage: ToolsPage;
  private readonly profilesPage: ProfilesPage;
  private readonly plansPage: PlansPage;
  private readonly nativeLinkingPanel: NativeLinkingPanel;
  private decisionUi: DecisionTestUiState = {
    selectedToolId: '',
    selectedModel: '',
    fetchedModels: [],
    loadingModels: false,
    runningTest: false,
    advice: null,
    error: null,
  };
  private returnFocusKey: string | null = null;
  private pollTimerId: number | null = null;
  private sharedRefreshInFlight = false;

  constructor(mountNode: HTMLElement) {
    const savedAppearance = loadSavedAppearance();
    const savedExecPrefs = loadSavedExecutionPreferences();
    this.state = createInitialState(savedAppearance, Date.now(), savedExecPrefs);
    this.state.isDevPreviewFixture = isDevPreviewFixtureMode();

    this.toolsPage = new ToolsPage(() => {
      void this.refreshRegistry();
    });
    this.profilesPage = new ProfilesPage(
      () => {
        void this.refreshRegistry();
      },
      () => {
        this.returnFocusKey = null;
        this.state = navigateToPage(this.state, 'tools');
        this.render('nav-tools');
        this.mainEl.scrollTop = 0;
      }
    );
    this.plansPage = new PlansPage(() => {
      void this.refreshPlansPageData(true);
    });
    this.nativeLinkingPanel = new NativeLinkingPanel(() => {
      this.returnFocusKey = null;
      this.state = navigateToPage(this.state, 'tools');
      this.render('nav-tools');
    });

    this.headerEl = h('header', { className: 'aq-top', 'data-tauri-drag-region': 'true' });
    this.mainEl = h('main', { className: 'aq-main' });
    this.footerEl = h('div', { className: 'aq-foot', role: 'status', 'aria-live': 'polite' });
    this.navEl = h('nav', { className: 'aq-nav', 'aria-label': 'Main navigation' });

    this.root = h(
      'section',
      {
        className: 'aq-app',
        'aria-label': 'Threadlane control panel',
      },
      this.headerEl,
      this.mainEl,
      this.footerEl,
      this.navEl
    );

    mountNode.replaceChildren(this.root);
    this.bindEvents();
    this.render();
    void this.initialize();
  }

  private async initialize(): Promise<void> {
    await Promise.all([
      this.triggerSnapshotPoll(true, false),
      this.refreshTools(),
      this.refreshRegistry(),
      this.refreshPlans(),
    ]);
    this.scheduleNextPoll();
  }

  private scheduleNextPoll(): void {
    if (this.pollTimerId !== null) {
      window.clearTimeout(this.pollTimerId);
      this.pollTimerId = null;
    }
    this.pollTimerId = window.setTimeout(() => {
      void this.runPollCycle();
    }, POLL_INTERVAL_MS);
  }

  private async runPollCycle(): Promise<void> {
    try {
      await this.triggerSnapshotPoll(false, true);
      if (this.state.detailTarget !== null) {
        const refresh = beginRunDetailRefresh(this.state, false);
        this.state = refresh.state;
        if (refresh.ticket) {
          await this.executeDetailTicket(refresh.ticket, false);
        }
      } else {
        if (this.canPollActivePlansStore()) {
          await this.refreshPlansPageData(false);
        }
        if (this.isDocumentHidden()) {
          return;
        }
        if (shouldPollHistory(this.state, false)) {
          await this.triggerHistoryQuery('poll', false);
        }
      }
    } finally {
      this.scheduleNextPoll();
    }
  }

  private isDocumentHidden(): boolean {
    return typeof document !== 'undefined' && Boolean(document.hidden);
  }

  private isPlansViewActive(): boolean {
    return this.state.page === 'plans' && this.state.detailTarget === null;
  }

  private canPollActivePlansStore(): boolean {
    if (!hasTauriRuntime() && !isDevPreviewFixtureMode()) {
      return false;
    }
    return shouldPollActivePlansStore(this.state, this.isDocumentHidden());
  }

  private syncPlansStoredContext(deferView: boolean): void {
    if (!this.isPlansViewActive()) {
      return;
    }
    this.plansPage.syncStoredContext(
      this.state.plansSnapshot,
      this.state.registrySnapshot,
      this.state.snapshot.bindings,
      this.state.selectedBindingId ?? this.state.snapshot.selected_binding_id,
      this.state.snapshot.attachment,
      { deferView }
    );
  }

  private async refreshPlansPageData(force: boolean): Promise<void> {
    if (!this.isPlansViewActive()) {
      return;
    }
    const previousPlansError = this.state.plansError;
    const previousRegistryError = this.state.registryError;
    const [registryChanged, plansChanged] = await Promise.all([
      this.refreshRegistry(force, 'defer'),
      this.refreshPlans(force, 'defer'),
    ]);
    if (!this.isPlansViewActive()) {
      return;
    }
    const editing = this.plansPage.hasEditableFocus();
    const storeChanged = registryChanged || plansChanged;
    const statusChanged =
      previousPlansError !== this.state.plansError ||
      previousRegistryError !== this.state.registryError;
    if (!storeChanged) {
      if ((statusChanged || this.plansPage.hasPendingStoredUpdates()) && !editing) {
        this.syncPlansStoredContext(false);
        this.renderPreserveActiveInput();
      }
      return;
    }
    this.syncPlansStoredContext(editing);
    if (editing) {
      this.renderHeader();
      this.renderFooter();
      return;
    }
    this.renderPreserveActiveInput();
  }

  private async triggerSnapshotPoll(
    force: boolean,
    isBackgroundPoll: boolean = false,
    renderMode: SnapshotRenderMode = 'preserve'
  ): Promise<void> {
    const next = beginSnapshotPoll(this.state, force);
    this.state = next.state;
    if (!next.ticket) {
      return;
    }
    await this.executeSnapshotTicket(next.ticket, isBackgroundPoll, renderMode);
  }

  private executeAfterSnapshotSuccess(isBackgroundPoll: boolean): void {
    if (
      !isBackgroundPoll &&
      this.state.page === 'activity' &&
      this.state.detailTarget === null &&
      this.state.history.expanded &&
      !this.state.history.hasLoadedOnce &&
      !this.state.history.inFlight
    ) {
      void this.triggerHistoryQuery('initial', false);
    }
  }

  private async executeSnapshotTicket(
    ticket: SnapshotTicket,
    isBackgroundPoll: boolean = false,
    renderMode: SnapshotRenderMode = 'preserve'
  ): Promise<void> {
    try {
      const snapshot = await ipcDesktopSnapshot(ticket.requestedBindingId);
      this.state = applySnapshotPollSuccess(this.state, ticket, snapshot);
      const plansActive = this.isPlansViewActive();
      if (plansActive) {
        this.plansPage.syncRuntimeContext(
          this.state.snapshot.bindings,
          this.state.selectedBindingId ?? this.state.snapshot.selected_binding_id,
          this.state.snapshot.attachment
        );
      }
      this.executeAfterSnapshotSuccess(isBackgroundPoll);
      if (renderMode === 'defer') {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      if (isBackgroundPoll && !shouldRebuildMainOnPoll(this.state)) {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      if (plansActive && this.plansPage.hasEditableFocus()) {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      this.renderPreserveActiveInput();
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to load desktop snapshot.';
      this.state = applySnapshotPollError(this.state, ticket, msg);
      const plansActive = this.isPlansViewActive();
      if (plansActive) {
        // The state now exposes a disconnected observer attachment, so approval
        // confirmations and submit controls are invalidated in place.
        this.plansPage.syncRuntimeContext(
          this.state.snapshot.bindings,
          this.state.selectedBindingId ?? this.state.snapshot.selected_binding_id,
          this.state.snapshot.attachment
        );
      }
      if (renderMode === 'defer') {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      if (isBackgroundPoll && !shouldRebuildMainOnPoll(this.state)) {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      if (plansActive && this.plansPage.hasEditableFocus()) {
        this.renderHeader();
        this.renderFooter();
        return;
      }
      this.renderPreserveActiveInput();
    }
  }

  private async handleSharedRefresh(): Promise<void> {
    if (this.sharedRefreshInFlight) {
      return;
    }
    const previousPlans = this.state.plansSnapshot;
    const previousRegistry = this.state.registrySnapshot;
    this.sharedRefreshInFlight = true;
    try {
      await Promise.all([
        this.triggerSnapshotPoll(true, false, 'defer'),
        this.refreshRegistry(true, 'defer'),
        this.refreshPlans(true, 'defer'),
      ]);
      const storeChanged =
        hasPlansSnapshotChanged(previousPlans, this.state.plansSnapshot) ||
        hasRegistrySnapshotChanged(previousRegistry, this.state.registrySnapshot);
      if (this.isPlansViewActive()) {
        const editing = this.plansPage.hasEditableFocus();
        if (editing) {
          if (storeChanged) {
            this.syncPlansStoredContext(true);
          }
          this.renderHeader();
          this.renderFooter();
          return;
        }
        this.syncPlansStoredContext(false);
      }
      this.renderPreserveActiveInput();
    } finally {
      this.sharedRefreshInFlight = false;
    }
  }

  private async triggerHistoryQuery(
    mode: Exclude<HistoryLoadingMode, null>,
    force: boolean = false
  ): Promise<void> {
    const isHidden = typeof document !== 'undefined' && Boolean(document.hidden);
    const started = beginHistoryQuery(this.state, mode, force, undefined, isHidden);
    this.state = started.state;
    if (!started.ticket) {
      return;
    }
    if (mode !== 'poll') {
      this.renderPreserveActiveInput();
    }
    await this.executeHistoryTicket(started.ticket);
  }

  private async executeHistoryTicket(ticket: HistoryQueryTicket): Promise<void> {
    try {
      const page = await ipcDesktopActivityHistory(toActivityHistoryIpcArgs(ticket));
      this.state = applyHistoryPageSuccess(this.state, ticket, page);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to load activity history.';
      this.state = applyHistoryPageError(this.state, ticket, msg);
    }
    if (this.state.page === 'activity' && this.state.detailTarget === null) {
      this.renderPreserveActiveInput();
    }
  }

  private renderAfterStoreRefresh(changed: boolean): void {
    if (this.state.detailTarget !== null) {
      return;
    }
    if (this.state.page === 'plans') {
      const editing = this.plansPage.hasEditableFocus();
      if (changed) {
        this.syncPlansStoredContext(editing);
      }
      if (editing) {
        this.renderHeader();
        this.renderFooter();
        return;
      }
    }
    if (
      this.state.page === 'tools' ||
      this.state.page === 'profiles' ||
      this.state.page === 'plans' ||
      this.state.page === 'chats' ||
      this.state.page === 'settings'
    ) {
      this.renderPreserveActiveInput();
    }
  }

  private async refreshRegistry(
    force: boolean = true,
    renderMode: StoreRenderMode = 'auto'
  ): Promise<boolean> {
    const previous = this.state.registrySnapshot;
    const started = beginRegistryRefresh(this.state, force);
    this.state = started.state;
    if (!started.ticket) {
      return false;
    }
    let changed = false;
    try {
      const snapshot = await desktop_registry_snapshot();
      this.state = applyRegistryRefreshSuccess(this.state, started.ticket, snapshot);
      changed = hasRegistrySnapshotChanged(previous, this.state.registrySnapshot);
    } catch (err) {
      this.state = applyRegistryRefreshError(
        this.state,
        started.ticket,
        formatAppError(err, 'Failed to load registry snapshot.')
      );
    }
    if (renderMode === 'auto') {
      this.renderAfterStoreRefresh(changed);
    }
    return changed;
  }

  private async refreshPlans(
    force: boolean = true,
    renderMode: StoreRenderMode = 'auto'
  ): Promise<boolean> {
    const previous = this.state.plansSnapshot;
    const started = beginPlansRefresh(this.state, force);
    this.state = started.state;
    if (!started.ticket) {
      return false;
    }
    let changed = false;
    try {
      const plans = await desktop_plans_snapshot();
      this.state = applyPlansRefreshSuccess(this.state, started.ticket, plans);
      changed = hasPlansSnapshotChanged(previous, this.state.plansSnapshot);
    } catch (err) {
      this.state = applyPlansRefreshError(
        this.state,
        started.ticket,
        formatAppError(err, 'Failed to load plans ledger.')
      );
    }
    if (renderMode === 'auto') {
      this.renderAfterStoreRefresh(changed);
    }
    return changed;
  }

  private async executeDetailTicket(
    ticket: DetailTicket,
    focusOnLoad: boolean
  ): Promise<void> {
    try {
      const detail = await ipcDesktopRunDetail(ticket.bindingId, ticket.runId);
      this.state = applyRunDetailSuccess(this.state, ticket, detail);
      this.render(focusOnLoad ? 'back' : undefined);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to load run details.';
      this.state = applyRunDetailError(this.state, ticket, msg);
      this.render(focusOnLoad ? 'back' : undefined);
    }
  }

  private async refreshTools(): Promise<void> {
    this.state = {
      ...this.state,
      toolsLoading: true,
      toolsError: null,
    };
    if (this.state.detailTarget === null && this.state.page === 'tools') {
      this.renderPreserveActiveInput();
    }
    try {
      const tools = await ipcDesktopToolStatus();
      this.state = {
        ...this.state,
        tools,
        toolsLoading: false,
        toolsError: null,
      };
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to check local PATH tools.';
      this.state = {
        ...this.state,
        toolsLoading: false,
        toolsError: msg,
      };
    }
    if (this.state.detailTarget === null && this.state.page === 'tools') {
      this.renderPreserveActiveInput();
    }
  }

  private async handleSelectBinding(
    bindingId: string | null,
    switchToActivity: boolean,
    focusTarget?: string
  ): Promise<void> {
    if (this.state.pendingMutation !== null) {
      return;
    }
    const switched = selectBinding(this.state, bindingId, switchToActivity);
    this.state = switched.state;
    if (!switched.ticket) {
      return;
    }
    this.render(focusTarget);
    await this.executeSnapshotTicket(switched.ticket, false);
    this.scheduleNextPoll();
  }

  private async handleOpenRunDetail(
    bindingId: string,
    runId: string,
    triggerFocusKey: string
  ): Promise<void> {
    this.returnFocusKey = triggerFocusKey;
    const opened = openRunDetail(this.state, bindingId, runId);
    this.state = opened.state;
    this.render('back');
    await this.executeDetailTicket(opened.ticket, false);
  }

  private async handleStartDemo(): Promise<void> {
    if (this.state.snapshot.demo_controls.can_cancel) {
      return;
    }
    const started = beginMutation(this.state, 'start_demo');
    this.state = started.state;
    if (!started.ticket) {
      return;
    }
    this.renderPreserveActiveInput();
    try {
      const snapshot = await ipcDesktopStartDemo();
      this.state = applyMutationSuccess(this.state, started.ticket, snapshot);
      this.render();
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to start local demo.';
      this.state = applyMutationError(this.state, started.ticket, msg);
      this.render();
    } finally {
      this.scheduleNextPoll();
    }
  }

  private async handleAnswerDemo(): Promise<void> {
    const expectedRevision = resolveExpectedDemoRevision(this.state);
    if (expectedRevision === null) {
      this.state = {
        ...this.state,
        actionError: 'No active demo revision available to answer.',
      };
      this.render();
      return;
    }
    const started = beginMutation(this.state, 'answer_demo');
    this.state = started.state;
    if (!started.ticket) {
      return;
    }
    this.renderPreserveActiveInput();
    try {
      const snapshot = await ipcDesktopAnswerDemo(expectedRevision);
      this.state = applyMutationSuccess(this.state, started.ticket, snapshot);
      this.renderPreserveActiveInput();
      if (this.state.detailTarget !== null) {
        const refresh = beginRunDetailRefresh(this.state, true);
        this.state = refresh.state;
        if (refresh.ticket) {
          await this.executeDetailTicket(refresh.ticket, false);
        }
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to continue local demo.';
      this.state = applyMutationError(this.state, started.ticket, msg);
      this.renderPreserveActiveInput();
    } finally {
      this.scheduleNextPoll();
    }
  }

  private async handleCancelDemo(): Promise<void> {
    const expectedRevision = resolveExpectedDemoRevision(this.state);
    if (expectedRevision === null) {
      this.state = {
        ...this.state,
        actionError: 'No active demo revision available to cancel.',
      };
      this.render();
      return;
    }
    const started = beginMutation(this.state, 'cancel_demo');
    this.state = started.state;
    if (!started.ticket) {
      return;
    }
    this.renderPreserveActiveInput();
    try {
      const snapshot = await ipcDesktopCancelDemo(expectedRevision);
      this.state = applyMutationSuccess(this.state, started.ticket, snapshot);
      this.renderPreserveActiveInput();
      if (this.state.detailTarget !== null) {
        const refresh = beginRunDetailRefresh(this.state, true);
        this.state = refresh.state;
        if (refresh.ticket) {
          await this.executeDetailTicket(refresh.ticket, false);
        }
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to cancel local demo.';
      this.state = applyMutationError(this.state, started.ticket, msg);
      this.renderPreserveActiveInput();
    } finally {
      this.scheduleNextPoll();
    }
  }

  private async handleAttachRuntime(stateDirInput?: string): Promise<void> {
    const targetDir = (stateDirInput ?? this.state.stateDirDraft).trim();
    if (!targetDir) {
      this.state = {
        ...this.state,
        actionError: 'Provide a valid runtime state directory path before attaching.',
      };
      this.render('state-dir-input');
      return;
    }
    const started = beginMutation(this.state, 'attach');
    this.state = started.state;
    if (!started.ticket) {
      return;
    }
    this.renderPreserveActiveInput();
    try {
      const snapshot = await ipcDesktopAttach(targetDir);
      this.state = applyMutationSuccess(this.state, started.ticket, snapshot);
      if (this.state.appearance.rememberStateDir) {
        this.state.appearance = {
          ...this.state.appearance,
          savedStateDir: targetDir,
        };
        saveAppearance(this.state.appearance);
      }
      this.render();
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to attach runtime state directory.';
      this.state = applyMutationError(this.state, started.ticket, msg);
      this.renderPreserveActiveInput();
    } finally {
      this.scheduleNextPoll();
    }
  }

  private handleBack(): void {
    const focusTarget = this.returnFocusKey ?? `nav-${this.state.page}`;
    this.returnFocusKey = null;
    this.state = navigateBack(this.state);
    this.render(focusTarget);
  }

  private applyAppearanceStyles(): void {
    const { theme, surface, opacity, accent } = this.state.appearance;
    const effectiveOpacity = surface === 'solid' ? 100 : opacity;
    const colorScheme = theme === 'system' ? 'light dark' : theme;

    this.root.dataset.theme = theme;
    this.root.dataset.surface = surface;
    this.root.dataset.accent = accent;
    this.root.style.colorScheme = colorScheme;
    this.root.style.setProperty('--aq-opacity', `${effectiveOpacity}%`);
    document.documentElement.style.colorScheme = colorScheme;
  }

  private findFocusTarget(
    attr: 'focus' | 'focusKey',
    key: string
  ): HTMLElement | null {
    const selector = attr === 'focus' ? '[data-focus]' : '[data-focus-key]';
    for (const element of this.root.querySelectorAll<HTMLElement>(selector)) {
      if (element.dataset[attr] === key) {
        return element;
      }
    }
    return null;
  }

  private renderPreserveActiveInput(): void {
    const active =
      typeof document !== 'undefined' && document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    const hasActiveInApp =
      active !== null && active !== this.root && this.root.contains(active);
    if (!hasActiveInApp || !active) {
      this.render();
      return;
    }

    const focusAttr: 'focus' | 'focusKey' | null = active.dataset.focus
      ? 'focus'
      : active.dataset.focusKey
        ? 'focusKey'
        : null;
    const focusKey = focusAttr ? active.dataset[focusAttr] ?? null : null;

    const isTextControl =
      active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement;
    let selStart: number | null = null;
    let selEnd: number | null = null;
    let selDirection: 'forward' | 'backward' | 'none' | null = null;
    if (isTextControl) {
      try {
        selStart = active.selectionStart;
        selEnd = active.selectionEnd;
        selDirection = active.selectionDirection;
      } catch {
        selStart = null;
        selEnd = null;
        selDirection = null;
      }
    }

    const prevScrollTop = this.mainEl.scrollTop;
    this.render();

    const restored =
      (focusAttr && focusKey ? this.findFocusTarget(focusAttr, focusKey) : null) ??
      (this.root.contains(active) ? active : null);

    if (restored && !(restored as { disabled?: boolean }).disabled) {
      restored.focus({ preventScroll: true });
      if (
        (restored instanceof HTMLInputElement ||
          restored instanceof HTMLTextAreaElement) &&
        selStart !== null &&
        selEnd !== null
      ) {
        try {
          if (selDirection) {
            restored.setSelectionRange(selStart, selEnd, selDirection);
          } else {
            restored.setSelectionRange(selStart, selEnd);
          }
        } catch {
          // Non-text inputs (e.g. number/range/checkbox) do not support setSelectionRange.
        }
      }
    }
    this.mainEl.scrollTop = prevScrollTop;
  }

  private render(focusKey?: string): void {
    const prevScrollTop = this.mainEl.scrollTop;
    this.applyAppearanceStyles();
    this.renderHeader();
    this.renderMain();
    this.renderFooter();
    this.renderNav();
    this.mainEl.scrollTop = prevScrollTop;

    if (focusKey) {
      const target =
        this.findFocusTarget('focus', focusKey) ??
        this.findFocusTarget('focusKey', focusKey);
      if (target && !(target as { disabled?: boolean }).disabled) {
        target.focus({ preventScroll: true });
      }
      this.mainEl.scrollTop = prevScrollTop;
    }
  }

  private renderHeader(): void {
    const hasSubView = this.state.detailTarget !== null || this.state.selectedToolId !== null;
    const pageTitle =
      this.state.detailTarget !== null
        ? 'Call details'
        : this.state.selectedToolId !== null
          ? 'Tool status'
          : NAV_ITEMS.find((item) => item.page === this.state.page)?.label ?? 'Threadlane';

    const backButton = hasSubView
      ? h(
          'button',
          {
            type: 'button',
            className: 'aq-icon-button',
            'data-action': 'back',
            'data-focus': 'back',
            'aria-label': 'Back',
          },
          createUiIcon('arrow-left')
        )
      : null;

    const titleEl = h(
      'h1',
      { className: 'aq-title', 'data-tauri-drag-region': 'true' },
      backButton,
      h('span', { className: 'aq-title-text', 'data-tauri-drag-region': 'true' }, pageTitle)
    );

    const badgeLabel = formatAttachmentBadge(
      this.state.snapshot.attachment,
      this.state.isDevPreviewFixture
    );
    const badgeEl = h(
      'span',
      {
        className: 'aq-preview-label',
        'data-state': this.state.snapshot.attachment.connection,
      },
      badgeLabel
    );

    const windowControls = h(
      'div',
      { className: 'aq-window-controls' },
      h(
        'button',
        {
          type: 'button',
          className: 'aq-icon-button',
          'data-window-action': 'minimize',
          'data-focus': 'win-minimize',
          'aria-label': 'Minimize window',
          title: 'Minimize',
        },
        createUiIcon('minus')
      ),
      h(
        'button',
        {
          type: 'button',
          className: 'aq-icon-button',
          'data-window-action': 'close',
          'data-focus': 'win-close',
          'aria-label': 'Close window',
          title: 'Close',
        },
        createUiIcon('x')
      )
    );

    const rightEl = h('div', { className: 'aq-top-right' }, badgeEl, windowControls);
    this.headerEl.replaceChildren(titleEl, rightEl);
  }

  private renderMain(): void {
    // Only the visible Chats panel may keep an in-flight metadata query alive.
    this.nativeLinkingPanel.setActive(
      this.state.page === 'chats' && this.state.detailTarget === null
    );
    const nodes: Node[] = [];

    if (this.state.actionError) {
      nodes.push(
        h(
          'div',
          { className: 'aq-banner aq-banner-error', role: 'alert' },
          h('span', undefined, this.state.actionError),
          h(
            'button',
            {
              type: 'button',
              className: 'aq-icon-button',
              'data-action': 'dismiss-error',
              'data-focus': 'dismiss-error',
              'aria-label': 'Dismiss error',
            },
            createUiIcon('x')
          )
        )
      );
    }

    if (this.state.detailTarget !== null) {
      nodes.push(this.buildRunDetailView());
    } else {
      switch (this.state.page) {
        case 'activity':
          nodes.push(this.buildActivityView());
          break;
        case 'chats':
          nodes.push(this.buildChatsView());
          break;
        case 'plans':
          nodes.push(this.buildPlansView());
          break;
        case 'tools':
          nodes.push(this.buildToolsView());
          break;
        case 'profiles':
          nodes.push(this.buildProfilesView());
          break;
        case 'settings':
          nodes.push(this.buildSettingsView());
          break;
      }
    }

    this.mainEl.replaceChildren(...nodes);
  }

  private buildActivityView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    const { snapshot, providerFilter, showFilters, completedExpanded, pendingMutation } =
      this.state;
    const {
      bindings,
      selected_binding_id,
      tasks,
      active_runs,
      total_runs,
      has_more,
      demo_controls,
      attachment,
    } = snapshot;

    const selectEl = h('select', {
      'data-change': 'binding',
      'data-focus': 'binding-select',
      'aria-label': 'Bound conversation',
      disabled: bindings.length === 0 || pendingMutation !== null,
    });

    if (bindings.length === 0) {
      selectEl.appendChild(h('option', { value: '' }, 'No bound conversations'));
    } else {
      for (const binding of bindings) {
        const option = h(
          'option',
          {
            value: binding.binding_id,
            selected:
              binding.binding_id === (this.state.selectedBindingId ?? selected_binding_id),
          },
          formatBindingLabel(binding)
        );
        selectEl.appendChild(option);
      }
    }

    const contextBar = h(
      'div',
      { className: 'aq-context' },
      selectEl,
      h(
        'button',
        {
          type: 'button',
          className: 'aq-icon-button',
          'data-action': 'toggle-filters',
          'data-focus': 'toggle-filters',
          'aria-label': 'Filter by provider',
          'aria-expanded': String(showFilters),
          title: 'Filter by provider',
        },
        createUiIcon('list-filter')
      ),
      h(
        'button',
        {
          type: 'button',
          className: 'aq-icon-button',
          'data-action': 'refresh-snapshot',
          'data-focus': 'refresh-snapshot',
          'aria-label': 'Refresh runtime, registry, and plans',
          title: 'Refresh runtime, registry & plans',
        },
        createUiIcon('refresh-cw')
      )
    );
    frag.appendChild(contextBar);

    if (showFilters) {
      const filterRow = h(
        'div',
        { className: 'aq-filter', role: 'group', 'aria-label': 'Provider filter' },
        ...PROVIDER_FILTERS.map((f) =>
          h(
            'button',
            {
              type: 'button',
              'data-filter': f.id,
              'data-focus': `filter-${f.id}`,
              'aria-pressed': String(providerFilter === f.id),
            },
            f.label
          )
        )
      );
      frag.appendChild(filterRow);
    }

    const activeBinding = bindings.find(
      (b) => b.binding_id === (this.state.selectedBindingId ?? selected_binding_id)
    );
    if (activeBinding) {
      const limitNote =
        has_more || total_runs > tasks.length
          ? `Showing ${tasks.length} of ${total_runs} (10 most recent)`
          : 'Up to 10 recent runs';
      frag.appendChild(
        h(
          'div',
          { className: 'aq-scope-bar' },
          h('span', undefined, `${active_runs} active · ${total_runs} total runs`),
          h('span', undefined, limitNote)
        )
      );
    }

    if (demo_controls.available) {
      frag.appendChild(this.buildDemoControlsBanner(demo_controls, pendingMutation));
    }

    if (attachment.connection !== 'connected' || attachment.mode === 'unconfigured') {
      frag.appendChild(this.buildAttachmentNoticeCard());
    }

    if (this.state.snapshotLoading) {
      frag.appendChild(h('p', { className: 'aq-note' }, 'Loading activity…'));
      return frag;
    }

    const filteredTasks = tasks.filter((t) => matchesProviderFilter(t, providerFilter));
    const waitingTasks = filteredTasks.filter((t) => classifyTaskBucket(t) === 'waiting');
    const inProgressTasks = filteredTasks.filter((t) => classifyTaskBucket(t) === 'active');
    const completedTasks = filteredTasks.filter((t) => classifyTaskBucket(t) === 'completed');

    const bindingLabelMap = new Map(bindings.map((b) => [b.binding_id, formatBindingLabel(b)]));

    if (waitingTasks.length > 0) {
      frag.appendChild(
        h(
          'h2',
          { className: 'aq-section' },
          h('span', undefined, 'Needs you'),
          h('span', undefined, String(waitingTasks.length))
        )
      );
      for (const task of waitingTasks) {
        frag.appendChild(
          this.buildTaskRow(task, bindingLabelMap.get(task.binding_id) ?? task.binding_id)
        );
      }
    }

    frag.appendChild(
      h(
        'h2',
        { className: 'aq-section' },
        h('span', undefined, 'In progress'),
        h('span', undefined, String(inProgressTasks.length))
      )
    );

    if (inProgressTasks.length > 0) {
      for (const task of inProgressTasks) {
        frag.appendChild(
          this.buildTaskRow(task, bindingLabelMap.get(task.binding_id) ?? task.binding_id)
        );
      }
    } else if (waitingTasks.length === 0) {
      const emptyMsg =
        attachment.connection !== 'connected'
          ? 'No live connection. Attach a runtime folder or run the local demo.'
          : tasks.length === 0
            ? 'No runs in the selected conversation yet.'
            : 'No active runs match the current filter.';
      frag.appendChild(h('p', { className: 'aq-note' }, emptyMsg));
    }

    if (completedTasks.length > 0) {
      const detailsEl = h('details', {
        className: 'aq-completed',
        'data-completed-toggle': 'true',
        open: completedExpanded,
      });
      detailsEl.appendChild(
        h(
          'summary',
          { 'data-focus': 'completed-summary' },
          h('span', undefined, `Finished · ${completedTasks.length}`),
          createUiIcon('chevron-down')
        )
      );
      for (const task of completedTasks) {
        detailsEl.appendChild(
          this.buildTaskRow(task, bindingLabelMap.get(task.binding_id) ?? task.binding_id)
        );
      }
      frag.appendChild(detailsEl);
    }

    const resolvedBindingId = this.state.selectedBindingId ?? selected_binding_id;
    frag.appendChild(
      renderActivityHistorySection({
        history: this.state.history,
        attachment,
        selectedBindingId: resolvedBindingId,
        isDevPreviewFixture: this.state.isDevPreviewFixture,
        hasTauriRuntime: hasTauriRuntime(),
        callbacks: {
          onToggleExpanded: () => {
            this.state = setHistoryExpanded(this.state);
            this.renderPreserveActiveInput();
            if (
              this.state.history.expanded &&
              !this.state.history.hasLoadedOnce &&
              !this.state.history.inFlight
            ) {
              void this.triggerHistoryQuery('initial', false);
            }
          },
          onRunIdDraftInput: (draft: string) => {
            this.state = updateHistoryRunIdDraft(this.state, draft);
          },
          onApplyFilters: () => {
            this.state = commitHistoryFilters(this.state);
            this.renderPreserveActiveInput();
            void this.triggerHistoryQuery('initial', true);
          },
          onClearFilters: () => {
            this.state = resetHistoryFilters(this.state);
            this.renderPreserveActiveInput();
            void this.triggerHistoryQuery('initial', true);
          },
          onKindFilterChange: (kind: EventKind | null) => {
            this.state = commitHistoryFilters(this.state, undefined, kind);
            this.renderPreserveActiveInput();
            void this.triggerHistoryQuery('initial', true);
          },
          onLoadMore: () => {
            void this.triggerHistoryQuery('more', false);
          },
          onRefresh: () => {
            void this.triggerHistoryQuery('refresh', true);
          },
          onSelectEvent: (eventId: string) => {
            this.state = selectHistoryEvent(this.state, eventId);
            this.renderPreserveActiveInput();
          },
          onOpenRunDetail: (bindingId: string, runId: string) => {
            void this.handleOpenRunDetail(bindingId, runId, 'history-run-filter');
          },
        },
      })
    );

    return frag;
  }

  private buildDemoControlsBanner(
    demoControls: DesktopStoreState['snapshot']['demo_controls'],
    pendingMutation: DesktopStoreState['pendingMutation']
  ): HTMLElement {
    const busy = pendingMutation !== null;
    const demoActive = demoControls.can_cancel;
    const revLabel = demoControls.revision !== null ? ` (rev ${demoControls.revision})` : '';
    const startLabel =
      pendingMutation === 'start_demo'
        ? 'Starting…'
        : demoActive
          ? 'Demo active'
          : 'Run local demo';

    const actions = h(
      'div',
      { className: 'aq-actions' },
      h(
        'button',
        {
          type: 'button',
          className: 'aq-btn aq-btn-primary',
          'data-action': 'start-demo',
          'data-focus': 'start-demo',
          disabled: busy || demoActive,
        },
        createUiIcon('play'),
        h('span', undefined, startLabel)
      ),
      demoControls.can_answer && demoControls.revision !== null
        ? h(
            'button',
            {
              type: 'button',
              className: 'aq-btn',
              'data-action': 'answer-demo',
              'data-focus': 'answer-demo',
              disabled: busy,
            },
            createUiIcon('check'),
            h('span', undefined, `Continue${revLabel}`)
          )
        : null,
      demoControls.can_cancel && demoControls.revision !== null
        ? h(
            'button',
            {
              type: 'button',
              className: 'aq-btn aq-btn-danger',
              'data-action': 'cancel-demo',
              'data-focus': 'cancel-demo',
              disabled: busy,
            },
            createUiIcon('ban'),
            h('span', undefined, `Cancel${revLabel}`)
          )
        : null
    );

    return h(
      'section',
      { className: 'aq-banner', 'aria-label': 'Local demo controls' },
      h(
        'div',
        { className: 'aq-banner-head' },
        h('span', undefined, 'Local demo'),
        h('span', { className: 'aq-badge' }, 'No paid calls')
      ),
      h(
        'p',
        { className: 'aq-banner-sub' },
        'Try progress, a question, and completion without calling a model.'
      ),
      actions
    );
  }

  private buildAttachmentNoticeCard(): HTMLElement {
    const { attachment } = this.state.snapshot;
    const busy = this.state.pendingMutation !== null;

    const title =
      attachment.connection === 'blocked'
        ? 'Observer access blocked'
        : attachment.connection === 'disconnected'
          ? 'Connection offline'
          : 'No runtime attached';

    const detailText =
      attachment.message ||
      (attachment.state_dir
        ? `Folder: ${attachment.state_dir}`
        : 'Attach a Threadlane state folder for read-only observation, or run the local demo.');

    return h(
      'section',
      { className: 'aq-banner', 'aria-label': 'Runtime observer connection' },
      h('div', { className: 'aq-banner-head' }, h('span', undefined, title)),
      h('p', { className: 'aq-banner-sub' }, detailText),
      h(
        'div',
        { className: 'aq-input-row', style: 'margin-top: 8px;' },
        h('input', {
          type: 'text',
          className: 'aq-text-input',
          value: this.state.stateDirDraft,
          placeholder: '/path/to/runtime/state-dir',
          'data-input': 'state-dir',
          'data-focus': 'state-dir-input',
          'aria-label': 'Runtime state directory path',
          disabled: busy,
        }),
        h(
          'button',
          {
            type: 'button',
            className: 'aq-btn aq-btn-primary',
            'data-action': 'attach-runtime',
            'data-focus': 'attach-runtime',
            disabled: busy,
          },
          pendingLabel(this.state.pendingMutation === 'attach', 'Attach', 'Attaching…')
        )
      )
    );
  }

  private buildTaskRow(task: TaskSummary, bindingLabel: string): HTMLElement {
    const bucket = classifyTaskBucket(task);
    const brand = resolveTaskBrand(task);
    const statusLabel = formatStatusLabel(task.status);
    const providerLabel = formatProviderName(task);
    const modelLine = formatModelAndEffort(task);
    const timeLabel = `${formatRelativeTimestamp(task.updated_at_ms)} · r${task.revision}`;
    const focusKey = `task-${task.binding_id}-${task.run_id}`;

    const isDemoControllable =
      this.state.snapshot.demo_controls.available &&
      this.state.snapshot.demo_controls.run_id === task.run_id &&
      this.state.snapshot.demo_controls.binding_id === task.binding_id;

    let waitOrEvidenceNote: HTMLElement | null = null;
    if (task.needs_evidence) {
      waitOrEvidenceNote = h(
        'span',
        { className: 'aq-wait-note', 'data-tone': 'warn' },
        createUiIcon('alert-circle'),
        h('span', undefined, 'Missing verified model/effort evidence')
      );
    } else if (bucket === 'waiting') {
      waitOrEvidenceNote = h(
        'span',
        { className: 'aq-wait-note' },
        createUiIcon('corner-up-right'),
        h(
          'span',
          undefined,
          isDemoControllable && this.state.snapshot.demo_controls.can_answer
            ? 'Ready to continue in demo'
            : 'Waiting for reply in Codex'
        )
      );
    }

    const summarySnippet = task.last_summary
      ? `${bindingLabel} · ${providerLabel} — ${task.last_summary}`
      : `${bindingLabel} · ${providerLabel}`;

    return h(
      'button',
      {
        type: 'button',
        className: 'aq-task',
        'data-task-run-id': task.run_id,
        'data-task-binding-id': task.binding_id,
        'data-focus': focusKey,
        'aria-label': `${task.title}, ${statusLabel}, ${providerLabel}, revision ${task.revision}`,
      },
      createBrandIconElement(brand),
      h(
        'span',
        undefined,
        h(
          'span',
          { className: 'aq-row-head' },
          h('span', { className: 'aq-task-name' }, task.title),
          h('span', { className: 'aq-time' }, timeLabel)
        ),
        h('span', { className: 'aq-sub' }, modelLine),
        h(
          'span',
          { className: 'aq-subline' },
          h('span', undefined, summarySnippet),
          h(
            'span',
            { className: 'aq-state' },
            h('span', {
              className: 'aq-state-dot',
              'data-bucket': bucket,
              'data-evidence': task.needs_evidence ? 'missing' : 'ok',
            }),
            h('span', undefined, statusLabel)
          )
        ),
        waitOrEvidenceNote
      )
    );
  }

  private buildRunDetailView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    const target = this.state.detailTarget;
    if (!target) {
      return frag;
    }

    const fallbackTask = this.state.snapshot.tasks.find(
      (t) => t.binding_id === target.bindingId && t.run_id === target.runId
    );
    const detail = this.state.runDetail;
    const task = detail?.task ?? fallbackTask;
    const binding = this.state.snapshot.bindings.find((b) => b.binding_id === target.bindingId);
    const bindingLabel = binding ? formatBindingLabel(binding) : target.bindingId;

    if (!task) {
      if (this.state.detailLoading) {
        frag.appendChild(h('p', { className: 'aq-note' }, 'Loading run details…'));
      } else {
        frag.appendChild(
          h('p', { className: 'aq-note' }, this.state.detailError || 'Run details unavailable.')
        );
      }
      return frag;
    }

    frag.appendChild(
      h(
        'p',
        { className: 'aq-kicker' },
        `${bindingLabel} / ${formatProviderName(task)} · rev ${task.revision}`
      )
    );
    frag.appendChild(h('h2', { className: 'aq-page-title' }, task.title));

    const acceptancePart = detail?.acceptance_state
      ? ` · Acceptance: ${detail.acceptance_state}`
      : '';
    frag.appendChild(
      h(
        'span',
        { className: 'aq-sub' },
        `${formatStatusLabel(task.status)}${acceptancePart} · Updated ${formatRelativeTimestamp(
          task.updated_at_ms
        )}`
      )
    );

    if (this.state.detailError) {
      frag.appendChild(
        h('div', { className: 'aq-banner aq-banner-error', role: 'alert' }, this.state.detailError)
      );
    }

    if (detail) {
      const isOwnedDemoRun =
        detail.demo_controls.available &&
        detail.demo_controls.run_id === detail.task.run_id &&
        detail.demo_controls.binding_id === detail.task.binding_id;

      if (detail.question) {
        frag.appendChild(
          h(
            'section',
            { className: 'aq-banner', 'aria-label': 'Pending question' },
            h(
              'div',
              { className: 'aq-banner-head' },
              h('span', undefined, `Question (${detail.question.question_id})`)
            ),
            h('p', { className: 'aq-copy', style: 'margin: 4px 0;' }, detail.question.prompt),
            h(
              'p',
              { className: 'aq-banner-sub' },
              isOwnedDemoRun
                ? 'Use Continue below to answer this local demo question.'
                : 'Reply in the linked Codex conversation.'
            )
          )
        );
      }

      if (isOwnedDemoRun && (detail.demo_controls.can_answer || detail.demo_controls.can_cancel)) {
        frag.appendChild(
          this.buildDemoControlsBanner(detail.demo_controls, this.state.pendingMutation)
        );
      }

      const evidenceSummary = formatEvidenceSummary(
        task,
        detail.missing_model_evidence,
        detail.missing_effort_evidence
      );

      frag.appendChild(
        h(
          'dl',
          { className: 'aq-facts' },
          h(
            'div',
            undefined,
            h('dt', undefined, 'Requested'),
            h(
              'dd',
              undefined,
              `${task.requested_model ?? 'None'} · ${task.requested_effort ?? 'None'}`
            )
          ),
          h(
            'div',
            undefined,
            h('dt', undefined, 'Observed'),
            h(
              'dd',
              undefined,
              `${task.observed_model ?? 'Unverified'} · ${task.observed_effort ?? 'Unverified'}`
            )
          ),
          h(
            'div',
            undefined,
            h('dt', undefined, 'Profile ID'),
            h('dd', undefined, task.profile_id ?? 'None recorded')
          ),
          h('div', undefined, h('dt', undefined, 'Evidence'), h('dd', undefined, evidenceSummary))
        )
      );

      const tabs: ReadonlyArray<{ id: DetailTab; label: string }> = [
        { id: 'overview', label: 'Overview' },
        { id: 'prompt', label: 'Prompt' },
        { id: 'evidence', label: 'Evidence' },
      ];

      frag.appendChild(
        h(
          'div',
          { className: 'aq-tabs', role: 'tablist', 'aria-label': 'Run detail sections' },
          ...tabs.map((tab) =>
            h(
              'button',
              {
                type: 'button',
                'data-detail-tab': tab.id,
                'data-focus': `detail-tab-${tab.id}`,
                'aria-pressed': String(this.state.detailTab === tab.id),
              },
              tab.label
            )
          )
        )
      );

      const tabPanel = h('div', { className: 'aq-detail-copy', 'aria-live': 'polite' });
      if (this.state.detailTab === 'overview') {
        if (detail.failure_reason) {
          tabPanel.appendChild(
            h(
              'div',
              { className: 'aq-banner aq-banner-error' },
              h('span', undefined, `Failure reason: ${detail.failure_reason}`)
            )
          );
        }
        tabPanel.appendChild(
          h(
            'p',
            { className: 'aq-copy' },
            detail.outcome_summary ||
              task.last_summary ||
              'No outcome summary recorded for this run yet.'
          )
        );
        if (detail.latest_activity && detail.latest_activity !== detail.outcome_summary) {
          tabPanel.appendChild(
            h('p', { className: 'aq-note' }, `Latest activity: ${detail.latest_activity}`)
          );
        }
      } else if (this.state.detailTab === 'prompt') {
        tabPanel.appendChild(
          h('p', { className: 'aq-copy' }, detail.instruction || 'No instruction text recorded.')
        );
      } else {
        tabPanel.appendChild(
          h(
            'dl',
            { className: 'aq-facts', style: 'margin-top: 4px;' },
            h(
              'div',
              undefined,
              h('dt', undefined, 'Durable events'),
              h('dd', undefined, String(detail.counts.durable_progress_events))
            ),
            h(
              'div',
              undefined,
              h('dt', undefined, 'Coalesced'),
              h('dd', undefined, String(detail.counts.coalesced_progress_events))
            ),
            h(
              'div',
              undefined,
              h('dt', undefined, 'Dropped live'),
              h('dd', undefined, String(detail.counts.dropped_live_events))
            ),
            h(
              'div',
              undefined,
              h('dt', undefined, 'Model check'),
              h(
                'dd',
                undefined,
                formatEvidenceFieldCheck(
                  task,
                  task.observed_model,
                  detail.missing_model_evidence,
                  'model'
                )
              )
            ),
            h(
              'div',
              undefined,
              h('dt', undefined, 'Effort check'),
              h(
                'dd',
                undefined,
                formatEvidenceFieldCheck(
                  task,
                  task.observed_effort,
                  detail.missing_effort_evidence,
                  'effort'
                )
              )
            )
          )
        );
        tabPanel.appendChild(
          h(
            'p',
            { className: 'aq-note' },
            'Counts reflect recorded progress events. This read-only observer does not consume conversation updates.'
          )
        );
      }
      frag.appendChild(tabPanel);

      frag.appendChild(
        h(
          'details',
          { className: 'aq-technical' },
          h(
            'summary',
            { 'data-focus': 'technical-summary' },
            h('span', undefined, 'Technical run metadata'),
            createUiIcon('chevron-down')
          ),
          h(
            'dl',
            { className: 'aq-technical-grid' },
            h('dt', undefined, 'Run ID'),
            h('dd', undefined, detail.task.run_id),
            h('dt', undefined, 'Binding ID'),
            h('dd', undefined, detail.task.binding_id),
            h('dt', undefined, 'Kind'),
            h('dd', undefined, detail.task.kind),
            h('dt', undefined, 'Revision'),
            h('dd', undefined, String(detail.task.revision)),
            h('dt', undefined, 'Exit code'),
            h('dd', undefined, detail.exit_code !== null ? String(detail.exit_code) : 'None'),
            h('dt', undefined, 'Branch'),
            h('dd', undefined, detail.branch_name ?? 'None'),
            h('dt', undefined, 'Worktree'),
            h('dd', undefined, detail.worktree_path ?? 'None')
          )
        )
      );
    } else if (this.state.detailLoading) {
      frag.appendChild(h('p', { className: 'aq-note' }, 'Loading run details…'));
    }

    return frag;
  }

  private buildChatsView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    const { bindings, selected_binding_id, active_runs, total_runs } = this.state.snapshot;
    const currentSelected = this.state.selectedBindingId ?? selected_binding_id;

    frag.appendChild(
      h(
        'div',
        { className: 'aq-section', style: 'margin-top: 4px;' },
        h('span', undefined, `Linked runtime conversations (${bindings.length})`),
        createButton('Open Plans ledger', () => {
          this.returnFocusKey = null;
          this.state = navigateToPage(this.state, 'plans');
          this.render('nav-plans');
          void this.refreshPlansPageData(true);
        })
      )
    );

    if (bindings.length === 0) {
      frag.appendChild(h('p', { className: 'aq-note' }, 'No linked conversations yet.'));
      if (this.state.snapshot.demo_controls.available) {
        frag.appendChild(
          this.buildDemoControlsBanner(
            this.state.snapshot.demo_controls,
            this.state.pendingMutation
          )
        );
      }
    } else {
      for (const binding of bindings) {
        const isSelected = binding.binding_id === currentSelected;
        const scopedStats = isSelected
          ? `${active_runs} active · ${total_runs} total runs`
          : 'Select to view activity';

        frag.appendChild(
          h(
            'button',
            {
              type: 'button',
              className: 'aq-link',
              'data-select-binding': binding.binding_id,
              'data-focus': `chat-${binding.binding_id}`,
              disabled: this.state.pendingMutation !== null,
            },
            h(
              'span',
              undefined,
              h(
                'span',
                { className: 'aq-row-head' },
                h('span', { className: 'aq-task-name' }, formatBindingLabel(binding)),
                isSelected
                  ? h('span', { className: 'aq-badge', 'data-status': 'active' }, 'Selected')
                  : binding.is_local_demo
                    ? h('span', { className: 'aq-badge' }, 'Local demo')
                    : null
              ),
              h('span', { className: 'aq-sub' }, scopedStats)
            ),
            createUiIcon('chevron-right')
          )
        );
      }

      frag.appendChild(
        h(
          'p',
          { className: 'aq-note' },
          'Only verified runtime bindings appear above. Native thread metadata discovery below is read-only and never fabricates pairing.'
        )
      );
    }

    frag.appendChild(
      this.nativeLinkingPanel.render(this.state.registrySnapshot, bindings)
    );

    return frag;
  }

  private buildPlansView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    frag.appendChild(
      this.plansPage.render(
        this.state.plansSnapshot,
        this.state.registrySnapshot,
        this.state.snapshot.bindings,
        this.state.selectedBindingId ?? this.state.snapshot.selected_binding_id,
        this.state.plansError,
        this.state.snapshot.attachment
      )
    );
    return frag;
  }

  private buildRegistryHeaderBar(): HTMLElement {
    return h(
      'div',
      { className: 'aq-scope-bar' },
      h(
        'span',
        undefined,
        `Registry revision r${this.state.registrySnapshot.revision} · ${this.state.registrySnapshot.tools.length} tools · ${this.state.registrySnapshot.models.length} models · ${this.state.registrySnapshot.profiles.length} profiles`
      ),
      h(
        'button',
        {
          type: 'button',
          className: 'aq-icon-button',
          'data-action': 'refresh-registry',
          'data-focus': 'refresh-registry',
          'aria-label': 'Refresh registry',
          title: 'Refresh registry',
          disabled: this.state.registryInFlight,
        },
        createUiIcon('refresh-cw')
      )
    );
  }

  private buildToolsView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    frag.appendChild(this.buildLocalDiscoverySection());
    frag.appendChild(this.buildRegistryHeaderBar());

    if (this.state.registryError) {
      frag.appendChild(
        h(
          'div',
          { className: 'aq-banner aq-banner-error', role: 'alert' },
          h('span', undefined, this.state.registryError)
        )
      );
    }

    frag.appendChild(this.toolsPage.render(this.state.registrySnapshot));
    return frag;
  }

  private buildLocalDiscoverySection(): HTMLElement {
    const { tools, toolsLoading, toolsError } = this.state;
    const hasResults = tools.length > 0;
    const titleId = 'aq-local-discovery-title';
    const refreshLabel = toolsLoading ? 'Refreshing…' : 'Refresh discovery';

    const section = h(
      'section',
      { className: 'aq-discovery', 'aria-labelledby': titleId, 'aria-busy': toolsLoading ? 'true' : undefined },
      h(
        'div',
        { className: 'aq-discovery-head' },
        h(
          'div',
          { className: 'aq-discovery-heading' },
          h('p', { className: 'aq-kicker' }, 'Local discovery'),
          h('h2', { className: 'aq-discovery-title', id: titleId }, 'Tools on this device')
        ),
        h(
          'button',
          {
            type: 'button',
            className: 'aq-btn aq-discovery-refresh',
            'data-action': 'refresh-tools',
            'data-focus': 'discovery-refresh',
            'aria-label': 'Refresh discovery',
            title: 'Refresh discovery',
            disabled: toolsLoading,
          },
          createUiIcon('refresh-cw'),
          h('span', undefined, refreshLabel)
        )
      ),
      h(
        'p',
        { className: 'aq-discovery-note' },
        'Read-only PATH lookup on this device. It does not verify login, credentials, version, model, or effort, and never adds registry tools or models.'
      )
    );

    if (toolsLoading) {
      section.appendChild(
        h(
          'p',
          { className: 'aq-discovery-status', role: 'status', 'aria-live': 'polite' },
          hasResults
            ? 'Refreshing discovery. Previous results stay visible until the check completes.'
            : 'Checking local PATH…'
        )
      );
    }

    if (toolsError) {
      section.appendChild(
        h(
          'div',
          { className: 'aq-discovery-error', role: 'alert' },
          h('span', undefined, toolsError),
          h(
            'button',
            {
              type: 'button',
              className: 'aq-btn',
              'data-action': 'refresh-tools',
              'aria-label': 'Retry local PATH discovery',
              title: 'Retry local PATH discovery',
            },
            createUiIcon('refresh-cw'),
            h('span', undefined, 'Retry discovery')
          )
        )
      );
      if (hasResults) {
        section.appendChild(
          h(
            'p',
            { className: 'aq-discovery-status' },
            'The latest discovery refresh failed. Results below are from the last successful check.'
          )
        );
      }
    }

    if (hasResults) {
      section.appendChild(
        h(
          'ul',
          { className: 'aq-discovery-list', 'aria-label': 'Tools discovered on this device' },
          ...tools.map((tool) => this.buildDiscoveryRow(tool))
        )
      );
    } else if (!toolsLoading && !toolsError) {
      section.appendChild(
        h(
          'p',
          { className: 'aq-discovery-empty', role: 'status' },
          'Local discovery returned no commands. Add tools explicitly in the registry below.'
        )
      );
    }

    section.appendChild(
      h(
        'p',
        { className: 'aq-discovery-note' },
        'Registry tools, models, and API adapters are configured below. Configure only prefills a draft; save it, load models, import entries, then assign them in Profiles. Discovery does not change the registry.'
      )
    );
    return section;
  }

  private buildDiscoveryRow(tool: ToolStatus): HTMLElement {
    const status = formatToolAvailability(tool.availability);
    const executablePath = tool.executable_path?.trim() ?? '';
    const canConfigure =
      tool.availability === 'found' &&
      executablePath.length > 0 &&
      isConfigurableDiscoveredCli(tool.id, tool.label);
    const statusTone =
      tool.availability === 'found'
        ? 'found'
        : tool.availability === 'not_found'
          ? 'warn'
          : undefined;
    const detail = executablePath
      ? executablePath
      : tool.availability === 'not_configured'
        ? 'No executable reported for discovery. Registry and API configuration is below.'
        : tool.availability === 'found'
          ? 'Found on PATH; no executable path reported.'
          : 'No executable found on PATH.';

    return h(
      'li',
      { className: 'aq-discovery-row' },
      createBrandIconElement(resolveToolBrand(tool.id, tool.label)),
      h(
        'div',
        { className: 'aq-discovery-body' },
        h(
          'div',
          { className: 'aq-discovery-name-row' },
          h('span', { className: 'aq-discovery-name' }, tool.label),
          h(
            'span',
            { className: 'aq-badge aq-discovery-badge', 'data-status': statusTone },
            status
          )
        ),
        h('code', { className: 'aq-discovery-detail' }, detail),
        canConfigure
          ? h(
              'div',
              { className: 'aq-discovery-actions' },
              h(
                'button',
                {
                  type: 'button',
                  className: 'aq-btn aq-discovery-configure',
                  'data-action': 'configure-discovered-tool',
                  'data-discovery-tool-id': tool.id,
                  'data-discovery-tool-label': tool.label,
                  'data-discovery-executable': executablePath,
                  'data-focus': `configure-discovery-${tool.id}`,
                  'aria-label': `Configure ${tool.label} from local discovery`,
                  title: `Prefill the Add tool form with ${tool.label}`,
                },
                'Configure'
              )
            )
          : null
      )
    );
  }

  private buildProfilesView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    frag.appendChild(this.buildRegistryHeaderBar());

    if (this.state.registryError) {
      frag.appendChild(
        h(
          'div',
          { className: 'aq-banner aq-banner-error', role: 'alert' },
          h('span', undefined, this.state.registryError)
        )
      );
    }

    frag.appendChild(this.profilesPage.render(this.state.registrySnapshot));
    return frag;
  }

  private buildSettingsView(): DocumentFragment {
    const frag = document.createDocumentFragment();
    const { appearance, executionPrefs, snapshot, pendingMutation } = this.state;
    const busy = pendingMutation !== null;

    frag.appendChild(h('p', { className: 'aq-kicker' }, 'Appearance'));
    frag.appendChild(
      this.buildSegmentControl<ThemeMode>('Theme', 'theme', appearance.theme, [
        ['system', 'System'],
        ['light', 'Light'],
        ['dark', 'Dark'],
      ])
    );
    frag.appendChild(
      this.buildSegmentControl<SurfaceMode>('Surface', 'surface', appearance.surface, [
        ['glass', 'Glass'],
        ['solid', 'Solid'],
      ])
    );

    const opacitySliderId = 'threadlane-opacity-range';
    frag.appendChild(
      h(
        'div',
        { className: 'aq-setting' },
        h(
          'label',
          { className: 'aq-label', for: opacitySliderId },
          h('span', undefined, 'Background opacity'),
          h(
            'output',
            { for: opacitySliderId, id: 'threadlane-opacity-output' },
            `${appearance.opacity}%`
          )
        ),
        h('input', {
          id: opacitySliderId,
          type: 'range',
          min: '65',
          max: '100',
          value: String(appearance.opacity),
          'data-opacity-slider': 'true',
          'data-focus': 'opacity-slider',
          disabled: appearance.surface === 'solid',
        })
      )
    );

    const swatchEntries = Object.entries(ACCENT_PALETTES) as Array<
      [AccentName, readonly [string, string]]
    >;
    frag.appendChild(
      h(
        'div',
        { className: 'aq-setting' },
        h('div', { className: 'aq-label' }, h('span', undefined, 'Accent')),
        h(
          'div',
          { className: 'aq-swatches', role: 'group', 'aria-label': 'Accent color' },
          ...swatchEntries.map(([name, [lightHex, darkHex]]) =>
            h('button', {
              type: 'button',
              'data-setting-accent': name,
              'data-focus': `accent-${name}`,
              style: `--aq-swatch: light-dark(${lightHex}, ${darkHex}); background-color: ${
                appearance.theme === 'dark' ? darkHex : lightHex
              };`,
              'aria-label': name.charAt(0).toUpperCase() + name.slice(1),
              'aria-pressed': String(appearance.accent === name),
            })
          )
        )
      )
    );

    frag.appendChild(this.buildExecutionPreferencesSection(executionPrefs));
    frag.appendChild(this.buildDecisionVerificationSection());

    frag.appendChild(
      h('p', { className: 'aq-kicker', style: 'margin-top: 18px;' }, 'Connection & local demo')
    );

    frag.appendChild(
      h(
        'dl',
        { className: 'aq-facts', style: 'margin: 8px 0 12px;' },
        h(
          'div',
          undefined,
          h('dt', undefined, 'Active mode'),
          h('dd', undefined, snapshot.attachment.mode)
        ),
        h(
          'div',
          undefined,
          h('dt', undefined, 'Connection'),
          h('dd', undefined, snapshot.attachment.connection)
        ),
        h(
          'div',
          undefined,
          h('dt', undefined, 'State dir'),
          h('dd', undefined, snapshot.attachment.state_dir ?? 'None attached')
        )
      )
    );

    frag.appendChild(
      h(
        'div',
        { className: 'aq-setting' },
        h(
          'label',
          { className: 'aq-label', for: 'settings-state-dir-input' },
          h('span', undefined, 'Attach runtime folder (read-only observer)')
        ),
        h(
          'div',
          { className: 'aq-input-row' },
          h('input', {
            id: 'settings-state-dir-input',
            type: 'text',
            className: 'aq-text-input',
            value: this.state.stateDirDraft,
            placeholder: '/path/to/threadlane/state-dir',
            'data-input': 'state-dir',
            'data-focus': 'state-dir-input',
            disabled: busy,
          }),
          h(
            'button',
            {
              type: 'button',
              className: 'aq-btn aq-btn-primary',
              'data-action': 'attach-runtime',
              'data-focus': 'settings-attach-btn',
              disabled: busy,
            },
            pendingLabel(pendingMutation === 'attach', 'Attach', 'Attaching…')
          )
        ),
        h(
          'label',
          { className: 'aq-toggle' },
          h('span', undefined, 'Remember folder path on this device'),
          h('input', {
            type: 'checkbox',
            checked: appearance.rememberStateDir,
            'data-toggle-remember-dir': 'true',
            'data-focus': 'remember-state-dir',
          })
        )
      )
    );

    if (snapshot.demo_controls.available) {
      const demoActive = snapshot.demo_controls.can_cancel;
      frag.appendChild(
        h(
          'div',
          { className: 'aq-setting' },
          h('div', { className: 'aq-label' }, h('span', undefined, 'Local demo')),
          h(
            'button',
            {
              type: 'button',
              className: 'aq-btn',
              'data-action': 'start-demo',
              'data-focus': 'settings-start-demo',
              disabled: busy || demoActive,
            },
            createUiIcon('play'),
            h(
              'span',
              undefined,
              pendingMutation === 'start_demo'
                ? 'Starting…'
                : demoActive
                   ? 'Local demo active'
                   : 'Run local demo'
            )
          )
        )
      );
    }

    frag.appendChild(
      h(
        'p',
        { className: 'aq-note' },
        'Only appearance, bounded execution preferences, and an optional remembered folder path are saved locally. Prompts, run history, and credentials are never stored in browser storage.'
      )
    );

    return frag;
  }

  private buildExecutionPreferencesSection(prefs: ExecutionPreferences): HTMLElement {
    const updatePref = (patch: Partial<ExecutionPreferences>, focusKey?: string): void => {
      const next = sanitizeExecutionPreferences({
        ...this.state.executionPrefs,
        ...patch,
      });
      this.state = {
        ...this.state,
        executionPrefs: next,
      };
      saveExecutionPreferences(next);
      this.render(focusKey);
    };

    const workersInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_PREFERENCE_BOUNDS.maxConcurrentWorkers.min),
      max: String(EXECUTION_PREFERENCE_BOUNDS.maxConcurrentWorkers.max),
      value: String(prefs.maxConcurrentWorkers),
      'data-focus': 'exec-max-workers',
      'aria-label': 'Maximum concurrent workers',
    });
    workersInput.addEventListener('change', () => {
      updatePref({ maxConcurrentWorkers: Number(workersInput.value) }, 'exec-max-workers');
    });

    const tasksInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_PREFERENCE_BOUNDS.maxTasksPerPlan.min),
      max: String(EXECUTION_PREFERENCE_BOUNDS.maxTasksPerPlan.max),
      value: String(prefs.maxTasksPerPlan),
      'data-focus': 'exec-max-tasks',
      'aria-label': 'Maximum tasks per plan',
    });
    tasksInput.addEventListener('change', () => {
      updatePref({ maxTasksPerPlan: Number(tasksInput.value) }, 'exec-max-tasks');
    });

    const timeoutInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_PREFERENCE_BOUNDS.workerTimeoutSecs.min),
      max: String(EXECUTION_PREFERENCE_BOUNDS.workerTimeoutSecs.max),
      value: String(prefs.workerTimeoutSecs),
      'data-focus': 'exec-timeout-secs',
      'aria-label': 'Worker timeout in seconds',
    });
    timeoutInput.addEventListener('change', () => {
      updatePref({ workerTimeoutSecs: Number(timeoutInput.value) }, 'exec-timeout-secs');
    });

    const budgetInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_PREFERENCE_BOUNDS.decisionBudgetTokens.min),
      max: String(EXECUTION_PREFERENCE_BOUNDS.decisionBudgetTokens.max),
      value: String(prefs.decisionBudgetTokens),
      'data-focus': 'exec-decision-budget',
      'aria-label': 'Decision token budget',
    });
    budgetInput.addEventListener('change', () => {
      updatePref({ decisionBudgetTokens: Number(budgetInput.value) }, 'exec-decision-budget');
    });

    const redactToggle = h('input', {
      type: 'checkbox',
      checked: prefs.redactPathsInLogs,
      'data-focus': 'exec-redact-paths',
    });
    redactToggle.addEventListener('change', () => {
      updatePref({ redactPathsInLogs: redactToggle.checked }, 'exec-redact-paths');
    });

    const explicitPublishToggle = h('input', {
      type: 'checkbox',
      checked: prefs.requireExplicitPublish,
      'data-focus': 'exec-explicit-publish',
    });
    explicitPublishToggle.addEventListener('change', () => {
      updatePref({ requireExplicitPublish: explicitPublishToggle.checked }, 'exec-explicit-publish');
    });

    const resetBtn = createButton('Reset defaults', () => {
      updatePref({ ...DEFAULT_EXECUTION_PREFERENCES });
    });

    return h(
      'section',
      { className: 'aq-setting', 'aria-label': 'Execution and privacy preferences' },
      h(
        'div',
        { className: 'aq-row-head' },
        h('p', { className: 'aq-kicker', style: 'margin: 0;' }, 'Execution & privacy bounds'),
        resetBtn
      ),
      h(
        'div',
        { className: 'aq-form-grid', style: 'margin-top: 8px;' },
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max concurrent workers (1–8)'),
          workersInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max tasks per plan (1–32)'),
          tasksInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Worker timeout secs (30–3600)'),
          timeoutInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Decision budget tokens (128–8192)'),
          budgetInput
        )
      ),
      h(
        'label',
        { className: 'aq-toggle' },
        h('span', undefined, 'Redact home and workspace paths in exported diagnostics'),
        redactToggle
      ),
      h(
        'label',
        { className: 'aq-toggle' },
        h('span', undefined, 'Require explicit operator publish before plan admission'),
        explicitPublishToggle
      ),
      h(
        'p',
        { className: 'aq-note', style: 'margin-bottom: 0;' },
        'Typed bounds only: arbitrary shell flags, custom CLI arguments, and raw environment variable overrides are prohibited. Editing preferences here never alters running tasks.'
      )
    );
  }

  private buildDecisionVerificationSection(): HTMLElement {
    const apiTools = this.state.registrySnapshot.tools.filter(
      (t) => t.adapter === 'jev_api' || t.adapter === 'openai_decisions_api'
    );

    if (
      !this.decisionUi.selectedToolId ||
      !apiTools.some((t) => t.id === this.decisionUi.selectedToolId)
    ) {
      this.decisionUi.selectedToolId = apiTools[0]?.id ?? '';
    }

    const selectedTool =
      apiTools.find((t) => t.id === this.decisionUi.selectedToolId) ?? null;
    const provider: DecisionProvider =
      selectedTool?.adapter === 'openai_decisions_api' ? 'openai' : 'jev';

    if (!this.decisionUi.selectedModel) {
      this.decisionUi.selectedModel =
        this.decisionUi.fetchedModels[0]?.model_arg ??
        (provider === 'openai' ? 'gpt-6-luna' : 'jev-1.14.0');
    }

    const busy = this.decisionUi.loadingModels || this.decisionUi.runningTest;

    if (apiTools.length === 0) {
      return h(
        'section',
        { className: 'aq-setting', 'aria-label': 'Decision API verification' },
        h('p', { className: 'aq-kicker' }, 'Decision shadow-mode verification'),
        h(
          'p',
          { className: 'aq-note' },
          'Configure a Jev API or OpenAI Decisions API tool and OS keychain secret on the Tools page to test synthetic classification.'
        ),
        createButton('Configure API tool in Tools', () => {
          this.returnFocusKey = null;
          this.state = navigateToPage(this.state, 'tools');
          this.render('nav-tools');
        })
      );
    }

    const toolSelect = h(
      'select',
      {
        className: 'aq-select-input',
        'aria-label': 'Decision API tool',
        disabled: busy,
      },
      ...apiTools.map((tool) =>
        h(
          'option',
          { value: tool.id, selected: tool.id === this.decisionUi.selectedToolId },
          `${tool.label} (${tool.adapter})`
        )
      )
    );
    toolSelect.addEventListener('change', () => {
      const nextTool = apiTools.find((t) => t.id === toolSelect.value);
      const nextProvider: DecisionProvider =
        nextTool?.adapter === 'openai_decisions_api' ? 'openai' : 'jev';
      this.decisionUi.selectedToolId = toolSelect.value;
      this.decisionUi.fetchedModels = [];
      this.decisionUi.selectedModel =
        nextProvider === 'openai' ? 'gpt-6-luna' : 'jev-1.14.0';
      this.decisionUi.advice = null;
      this.decisionUi.error = null;
      this.render();
    });

    const modelInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: this.decisionUi.selectedModel,
      placeholder: provider === 'openai' ? 'gpt-6-luna' : 'jev-1.14.0',
      'aria-label': 'Decision model',
      disabled: busy,
    });
    modelInput.addEventListener('input', () => {
      this.decisionUi.selectedModel = modelInput.value;
    });

    const fetchModelsBtn = createButton(
      this.decisionUi.loadingModels ? 'Loading models…' : 'Fetch decision models',
      () => {
        void this.handleFetchDecisionModels(toolSelect.value, provider);
      },
      busy
    );

    const runSyntheticBtn = createButton(
      this.decisionUi.runningTest
        ? 'Testing synthetic sample…'
        : 'Run fixed synthetic decision test',
      () => {
        void this.handleRunSyntheticDecisionTest(
          toolSelect.value,
          provider,
          this.decisionUi.selectedModel
        );
      },
      busy
    );
    runSyntheticBtn.classList.add('aq-btn-primary');

    const adviceEl = this.decisionUi.advice
      ? h(
          'dl',
          { className: 'aq-facts', style: 'margin-top: 8px;' },
          h(
            'div',
            undefined,
            h('dt', undefined, 'Provider / Model'),
            h(
              'dd',
              undefined,
              `${this.decisionUi.advice.provider} · ${this.decisionUi.advice.model}`
            )
          ),
          h(
            'div',
            undefined,
            h('dt', undefined, 'Difficulty'),
            h('dd', undefined, this.decisionUi.advice.difficulty ?? 'Unclassified')
          ),
          h(
            'div',
            undefined,
            h('dt', undefined, 'Confidence'),
            h(
              'dd',
              undefined,
              this.decisionUi.advice.confidence !== null
                ? this.decisionUi.advice.confidence.toFixed(2)
                : 'N/A'
            )
          ),
          h(
            'div',
            undefined,
            h('dt', undefined, 'Reason / Review'),
            h(
              'dd',
              undefined,
              `${this.decisionUi.advice.reason_code} · needs_review=${String(
                this.decisionUi.advice.needs_review
              )}`
            )
          )
        )
      : null;

    return h(
      'section',
      { className: 'aq-setting', 'aria-label': 'Decision API verification' },
      h(
        'div',
        { className: 'aq-row-head' },
        h('p', { className: 'aq-kicker', style: 'margin: 0;' }, 'Decision shadow-mode verification'),
        h('span', { className: 'aq-badge' }, 'Explicit click only')
      ),
      this.decisionUi.error
        ? h(
            'div',
            { className: 'aq-banner aq-banner-error', role: 'alert' },
            h('span', undefined, this.decisionUi.error)
          )
        : null,
      h(
        'div',
        { className: 'aq-form-grid', style: 'margin-top: 8px;' },
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'API Tool'),
          toolSelect
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Decision model'),
          modelInput
        )
      ),
      h('div', { className: 'aq-actions' }, fetchModelsBtn, runSyntheticBtn),
      adviceEl,
      h(
        'p',
        { className: 'aq-note', style: 'margin-bottom: 0;' },
        'Uses a fixed synthetic verification task in Rust. User prompts are never sent, and decision advice never triggers automatic approval or execution.'
      )
    );
  }

  private async handleFetchDecisionModels(
    toolId: string,
    provider: DecisionProvider
  ): Promise<void> {
    this.decisionUi.loadingModels = true;
    this.decisionUi.error = null;
    this.render();
    try {
      const models = await desktop_decision_models({ toolId, provider });
      this.decisionUi.fetchedModels = models;
      if (models[0]) {
        this.decisionUi.selectedModel = models[0].model_arg;
      }
    } catch (err) {
      this.decisionUi.error = formatAppError(
        err,
        'Failed to fetch decision models metadata.'
      );
    } finally {
      this.decisionUi.loadingModels = false;
      this.render();
    }
  }

  private async handleRunSyntheticDecisionTest(
    toolId: string,
    provider: DecisionProvider,
    model: string
  ): Promise<void> {
    this.decisionUi.runningTest = true;
    this.decisionUi.error = null;
    this.render();
    try {
      const advice = await desktop_test_decision({
        toolId,
        provider,
        model: model.trim(),
      });
      this.decisionUi.advice = advice;
    } catch (err) {
      this.decisionUi.error = formatAppError(
        err,
        'Synthetic decision verification test failed.'
      );
    } finally {
      this.decisionUi.runningTest = false;
      this.render();
    }
  }

  private buildSegmentControl<T extends string>(
    label: string,
    key: 'theme' | 'surface',
    currentValue: T,
    options: ReadonlyArray<readonly [T, string]>
  ): HTMLElement {
    return h(
      'div',
      { className: 'aq-setting' },
      h('div', { className: 'aq-label' }, h('span', undefined, label)),
      h(
        'div',
        { className: 'aq-segment', role: 'group', 'aria-label': label },
        ...options.map(([value, text]) =>
          h(
            'button',
            {
              type: 'button',
              'data-setting-key': key,
              'data-setting-value': value,
              'data-focus': `${key}-${value}`,
              'aria-pressed': String(currentValue === value),
            },
            text
          )
        )
      )
    );
  }

  private renderFooter(): void {
    const status = formatFooterStatus(
      this.state.snapshot.attachment,
      this.state.isDevPreviewFixture
    );
    this.footerEl.replaceChildren(
      h('span', undefined, status.left),
      h('span', undefined, status.right)
    );
  }

  private renderNav(): void {
    const buttons = NAV_ITEMS.map((item) =>
      h(
        'button',
        {
          type: 'button',
          'data-nav-page': item.page,
          'data-focus': `nav-${item.page}`,
          'aria-current': this.state.page === item.page ? 'page' : undefined,
        },
        createUiIcon(item.icon),
        h('span', undefined, item.label)
      )
    );
    this.navEl.replaceChildren(...buttons);
  }

  private bindEvents(): void {
    this.root.addEventListener('click', (event) => {
      const target = event.target as HTMLElement | null;
      const button = target?.closest('button');
      if (!button || !this.root.contains(button)) {
        return;
      }

      const windowAction = button.dataset.windowAction;
      if (windowAction === 'minimize' || windowAction === 'close') {
        void ipcDesktopWindowAction(windowAction);
        return;
      }

      const action = button.dataset.action;
      if (action === 'back') {
        this.handleBack();
        return;
      }
      if (action === 'dismiss-error') {
        this.state = { ...this.state, actionError: null };
        this.render();
        return;
      }
      if (action === 'toggle-filters') {
        this.state = { ...this.state, showFilters: !this.state.showFilters };
        this.render('toggle-filters');
        return;
      }
      if (action === 'refresh-snapshot') {
        void this.handleSharedRefresh();
        return;
      }
      if (action === 'refresh-registry') {
        void this.refreshRegistry();
        return;
      }
      if (action === 'refresh-tools') {
        void this.refreshTools();
        return;
      }
      if (action === 'configure-discovered-tool') {
        const toolId = button.dataset.discoveryToolId;
        const toolLabel = button.dataset.discoveryToolLabel;
        const executablePath = button.dataset.discoveryExecutable;
        if (toolId && toolLabel && executablePath) {
          this.toolsPage.configureDiscoveredTool(toolId, toolLabel, executablePath);
        }
        return;
      }
      if (action === 'start-demo') {
        void this.handleStartDemo();
        return;
      }
      if (action === 'answer-demo') {
        void this.handleAnswerDemo();
        return;
      }
      if (action === 'cancel-demo') {
        void this.handleCancelDemo();
        return;
      }
      if (action === 'attach-runtime') {
        void this.handleAttachRuntime();
        return;
      }

      const navPage = button.dataset.navPage as NavPage | undefined;
      if (navPage) {
        this.returnFocusKey = null;
        this.state = navigateToPage(this.state, navPage);
        this.render(`nav-${navPage}`);
        if (navPage === 'plans') {
          void this.refreshPlansPageData(true);
        }
        return;
      }

      const filter = button.dataset.filter as TaskProviderFilter | undefined;
      if (filter) {
        this.state = { ...this.state, providerFilter: filter };
        this.render(`filter-${filter}`);
        return;
      }

      const taskRunId = button.dataset.taskRunId;
      const taskBindingId = button.dataset.taskBindingId;
      if (taskRunId && taskBindingId) {
        void this.handleOpenRunDetail(
          taskBindingId,
          taskRunId,
          button.dataset.focus ?? `task-${taskBindingId}-${taskRunId}`
        );
        return;
      }

      const detailTab = button.dataset.detailTab as DetailTab | undefined;
      if (detailTab) {
        this.state = { ...this.state, detailTab };
        this.render(`detail-tab-${detailTab}`);
        return;
      }

      const selectBindingId = button.dataset.selectBinding;
      if (selectBindingId) {
        void this.handleSelectBinding(selectBindingId, true, 'binding-select');
        return;
      }

      const settingKey = button.dataset.settingKey as 'theme' | 'surface' | undefined;
      const settingValue = button.dataset.settingValue;
      if (settingKey && settingValue) {
        const nextAppearance = sanitizeAppearancePreferences({
          ...this.state.appearance,
          [settingKey]: settingValue,
        });
        this.state = { ...this.state, appearance: nextAppearance };
        saveAppearance(nextAppearance);
        this.render(`${settingKey}-${settingValue}`);
        return;
      }

      const settingAccent = button.dataset.settingAccent as AccentName | undefined;
      if (settingAccent) {
        const nextAppearance = sanitizeAppearancePreferences({
          ...this.state.appearance,
          accent: settingAccent,
        });
        this.state = { ...this.state, appearance: nextAppearance };
        saveAppearance(nextAppearance);
        this.render(`accent-${settingAccent}`);
        return;
      }
    });

    this.root.addEventListener('change', (event) => {
      const target = event.target as HTMLElement | null;
      if (!target) {
        return;
      }

      if (target instanceof HTMLSelectElement && target.dataset.change === 'binding') {
        const nextBindingId = target.value || null;
        void this.handleSelectBinding(nextBindingId, false, 'binding-select');
        return;
      }

      if (target instanceof HTMLInputElement && target.hasAttribute('data-toggle-remember-dir')) {
        const rememberStateDir = target.checked;
        const nextAppearance = sanitizeAppearancePreferences({
          ...this.state.appearance,
          rememberStateDir,
          savedStateDir: rememberStateDir ? this.state.stateDirDraft : '',
        });
        this.state = { ...this.state, appearance: nextAppearance };
        saveAppearance(nextAppearance);
        this.render('remember-state-dir');
        return;
      }

      if (target instanceof HTMLInputElement && target.hasAttribute('data-opacity-slider')) {
        saveAppearance(this.state.appearance);
      }
    });

    this.root.addEventListener('input', (event) => {
      const target = event.target as HTMLElement | null;
      if (!target) {
        return;
      }

      if (target instanceof HTMLInputElement && target.dataset.input === 'state-dir') {
        this.state.stateDirDraft = target.value;
        if (this.state.appearance.rememberStateDir) {
          this.state.appearance = sanitizeAppearancePreferences({
            ...this.state.appearance,
            savedStateDir: target.value,
          });
          saveAppearance(this.state.appearance);
        }
        return;
      }

      if (target instanceof HTMLInputElement && target.hasAttribute('data-opacity-slider')) {
        const nextOpacity = Math.round(Math.min(100, Math.max(65, Number(target.value))));
        this.state.appearance = {
          ...this.state.appearance,
          opacity: nextOpacity,
        };
        const output = this.root.querySelector<HTMLOutputElement>('#threadlane-opacity-output');
        if (output) {
          output.textContent = `${nextOpacity}%`;
        }
        this.applyAppearanceStyles();
      }
    });

    this.root.addEventListener(
      'toggle',
      (event) => {
        const target = event.target as HTMLElement | null;
        if (target instanceof HTMLDetailsElement && target.hasAttribute('data-completed-toggle')) {
          this.state.completedExpanded = target.open;
        }
      },
      true
    );

    this.root.addEventListener('keydown', (event) => {
      if (
        event.key === 'Escape' &&
        (this.state.detailTarget !== null || this.state.selectedToolId !== null)
      ) {
        event.preventDefault();
        this.handleBack();
        return;
      }

      const target = event.target as HTMLElement | null;
      if (
        event.key === 'Enter' &&
        target instanceof HTMLInputElement &&
        target.dataset.input === 'state-dir'
      ) {
        event.preventDefault();
        void this.handleAttachRuntime(target.value);
      }
    });
  }
}

function pendingLabel(isPending: boolean, idleText: string, pendingText: string): string {
  return isPending ? pendingText : idleText;
}

const mount = document.getElementById('app');
if (mount) {
  new ThreadlaneDesktopApp(mount);
}
