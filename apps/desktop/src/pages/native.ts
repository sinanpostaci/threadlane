import { createButton, h } from '../ui/dom.ts';
import type { BindingSummary } from '../types.ts';
import {
  desktop_native_threads,
  formatAppError,
  type RegistrySnapshot,
} from '../registry/ipc.ts';
import {
  NATIVE_THREAD_RETAINED_LIMIT,
  NativeThreadsController,
  nativeToolBlockReason,
  type NativeQueryMode,
  type NativeThreadsState,
} from './native-logic.ts';

export class NativeLinkingPanel {
  private readonly onNavigateToTools: () => void;
  private readonly container: HTMLElement;
  private readonly controller: NativeThreadsController;
  private readonly errorBanner: HTMLElement;
  private readonly errorText: HTMLElement;
  private readonly noToolsBanner: HTMLElement;
  private readonly formGrid: HTMLElement;
  private readonly toolSelect: HTMLSelectElement;
  private readonly projectInput: HTMLInputElement;
  private readonly blockedNote: HTMLElement;
  private readonly actionsRow: HTMLElement;
  private readonly fetchButton: HTMLButtonElement;
  private readonly contextNote: HTMLElement;
  private readonly statusNote: HTMLElement;
  private readonly pairingNote: HTMLElement;
  private readonly threadsContainer: HTMLElement;
  private readonly limitNote: HTMLElement;
  private readonly loadMoreContainer: HTMLElement;
  private readonly loadMoreButton: HTMLButtonElement;
  private toolOptionsSignature: string = '';
  private threadsSignature: string = '';

  constructor(onNavigateToTools: () => void) {
    this.onNavigateToTools = onNavigateToTools;
    this.controller = new NativeThreadsController(
      (args) => desktop_native_threads(args),
      {
        formatError: (err) =>
          formatAppError(err, 'Native Codex metadata query failed.'),
      }
    );

    // Build form controls once. Rendering updates values and result rows without
    // replacing the filter input, so typing and focus survive polls.
    this.container = h('section', {
      className: 'aq-card',
      'aria-label': 'Native Codex metadata and pairing verification',
    });

    const sectionHeader = h(
      'div',
      { className: 'aq-section', style: 'margin-top: 14px;' },
      h('span', undefined, 'Native Codex metadata & pairing gate'),
      h(
        'span',
        { className: 'aq-badge', 'data-status': 'warn' },
        'needs_verification'
      )
    );

    const verificationBanner = h(
      'div',
      { className: 'aq-banner' },
      h(
        'div',
        { className: 'aq-banner-head' },
        h('span', undefined, 'Paired MCP & busy-turn gate pending verification'),
        h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Unverified')
      ),
      h(
        'p',
        { className: 'aq-banner-sub' },
        'Two-chat isolation and busy-turn steer delivery remain marked needs_verification until an explicit authorized operator experiment is run. Enrollment requires an operator-retrieved connection_identity nonce on the thread-scoped MCP connection; model parameters never authorize binding.'
      )
    );

    const errorText = h('span');
    const errorBanner = h(
      'div',
      { className: 'aq-banner aq-banner-error', role: 'alert' },
      errorText
    );
    errorBanner.hidden = true;

    const noToolsBanner = h(
      'div',
      { className: 'aq-banner' },
      h(
        'div',
        { className: 'aq-banner-head' },
        h('span', undefined, 'Codex CLI adapter not configured'),
        h('span', { className: 'aq-badge' }, 'not_configured')
      ),
      h(
        'p',
        { className: 'aq-banner-sub' },
        'Register a tool with the codex_cli adapter on the Tools page to query read-only native thread metadata.'
      ),
      h(
        'div',
        { className: 'aq-actions' },
        createButton('Open Tools page', () => this.onNavigateToTools())
      )
    );

    const toolSelect = h('select', {
      className: 'aq-select-input',
      'aria-label': 'Codex CLI tool',
    });
    toolSelect.addEventListener('change', () => {
      this.controller.selectTool(toolSelect.value);
      this.updateDom();
    });

    const projectInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: '',
      placeholder: 'Optional project_id filter',
      'aria-label': 'Project ID filter',
      'data-focus': 'native-project-filter',
    });
    projectInput.addEventListener('input', () => {
      this.controller.updateProjectDraft(projectInput.value);
      this.updateDom();
    });

    const formGrid = h(
      'div',
      { className: 'aq-form-grid' },
      h(
        'label',
        { className: 'aq-field' },
        h('span', { className: 'aq-field-label' }, 'Codex CLI tool'),
        toolSelect
      ),
      h(
        'label',
        { className: 'aq-field' },
        h('span', { className: 'aq-field-label' }, 'Project filter'),
        projectInput
      )
    );

    const blockedNote = h('p', {
      className: 'aq-note',
      style: 'color: var(--aq-warn);',
    });
    blockedNote.hidden = true;

    const fetchButton = createButton('Load native thread metadata', () => {
      void this.load('fresh');
    });
    fetchButton.classList.add('aq-btn-primary');
    const actionsRow = h('div', { className: 'aq-actions' }, fetchButton);

    const contextNote = h('p', { className: 'aq-note' });
    contextNote.hidden = true;
    const statusNote = h('p', { className: 'aq-note' });
    statusNote.hidden = true;
    const pairingNote = h(
      'p',
      { className: 'aq-note' },
      'Observed binding labels come from local metadata only. A matching thread ID does not authenticate identity or verify enrollment.'
    );
    pairingNote.hidden = true;
    const threadsContainer = h('div');
    const limitNote = h('p', {
      className: 'aq-note',
      style: 'color: var(--aq-warn);',
    });
    limitNote.hidden = true;
    const loadMoreButton = createButton('Load next page', () => {
      void this.load('more');
    });
    const loadMoreContainer = h(
      'div',
      { className: 'aq-actions' },
      loadMoreButton
    );
    loadMoreContainer.hidden = true;

    this.errorBanner = errorBanner;
    this.errorText = errorText;
    this.noToolsBanner = noToolsBanner;
    this.formGrid = formGrid;
    this.toolSelect = toolSelect;
    this.projectInput = projectInput;
    this.blockedNote = blockedNote;
    this.actionsRow = actionsRow;
    this.fetchButton = fetchButton;
    this.contextNote = contextNote;
    this.statusNote = statusNote;
    this.pairingNote = pairingNote;
    this.threadsContainer = threadsContainer;
    this.limitNote = limitNote;
    this.loadMoreContainer = loadMoreContainer;
    this.loadMoreButton = loadMoreButton;

    this.container.append(
      sectionHeader,
      verificationBanner,
      errorBanner,
      noToolsBanner,
      formGrid,
      blockedNote,
      actionsRow,
      contextNote,
      statusNote,
      pairingNote,
      threadsContainer,
      limitNote,
      loadMoreContainer
    );
  }

  public setActive(active: boolean): void {
    this.controller.setActive(active);
  }

  public render(registry: RegistrySnapshot, bindings: BindingSummary[]): HTMLElement {
    this.controller.sync(registry, bindings);
    this.updateDom();
    return this.container;
  }

  private async load(mode: NativeQueryMode): Promise<void> {
    const pending = this.controller.load(mode);
    this.updateDom();
    await pending;
    if (this.controller.getState().active) {
      this.updateDom();
    }
  }

  private updateDom(): void {
    const state = this.controller.getState();
    const hasCodexTools = state.codexTools.length > 0;
    const blockReason = nativeToolBlockReason(state);

    this.errorBanner.hidden = state.errorMessage === null;
    this.errorText.textContent = state.errorMessage ?? '';

    this.noToolsBanner.hidden = hasCodexTools;
    this.formGrid.hidden = !hasCodexTools;
    this.actionsRow.hidden = !hasCodexTools;

    if (!hasCodexTools) {
      this.blockedNote.hidden = true;
      this.contextNote.hidden = true;
      this.statusNote.hidden = true;
      this.pairingNote.hidden = true;
      this.threadsContainer.replaceChildren();
      this.threadsSignature = '';
      this.limitNote.hidden = true;
      this.loadMoreContainer.hidden = true;
      this.toolOptionsSignature = '';
      this.toolSelect.replaceChildren();
      return;
    }

    this.updateToolOptions(state);
    if (this.projectInput.value !== state.projectDraft) {
      this.projectInput.value = state.projectDraft;
    }

    this.blockedNote.hidden = blockReason === null;
    this.blockedNote.textContent = blockReason ?? '';

    this.fetchButton.disabled = state.loading || blockReason !== null;
    this.fetchButton.textContent = state.loading
      ? 'Querying metadata…'
      : 'Load native thread metadata';

    const context = state.appliedContext;
    if (context !== null) {
      const projectLabel =
        context.projectFilter === null
          ? 'all projects'
          : `project "${context.projectFilter}"`;
      this.contextNote.textContent = `List represents ${context.toolLabel} (${context.toolId}) · ${projectLabel} · registry r${context.registryRevision}.`;
      this.contextNote.hidden = false;
    } else {
      this.contextNote.hidden = true;
    }

    const statusText = this.resolveStatusText(state);
    this.statusNote.textContent = statusText ?? '';
    this.statusNote.hidden = statusText === null;

    const threadsSignature = JSON.stringify({
      threads: state.threads.map((thread) => [
        thread.id,
        thread.title,
        thread.status,
        thread.model,
        thread.effort,
        thread.project_id,
        thread.cwd,
      ]),
      bindings: state.bindings.map((binding) => [
        binding.thread_uuid,
        binding.binding_id,
      ]),
    });
    if (threadsSignature !== this.threadsSignature) {
      this.threadsSignature = threadsSignature;
      this.threadsContainer.replaceChildren(...this.buildThreadNodes(state));
    }

    this.pairingNote.hidden = state.threads.length === 0;

    this.limitNote.hidden = !state.limitReached;
    this.limitNote.textContent = state.limitReached
      ? `Showing the first ${NATIVE_THREAD_RETAINED_LIMIT} native threads. Load more stops at this bound; narrow the project filter to inspect additional threads.`
      : '';

    const showLoadMore = state.nextCursor !== null && !state.limitReached;
    this.loadMoreContainer.hidden = !showLoadMore;
    this.loadMoreButton.disabled = state.loading || blockReason !== null;
    this.loadMoreButton.textContent = state.loading ? 'Loading…' : 'Load next page';
  }

  private updateToolOptions(state: NativeThreadsState): void {
    const signature = state.codexTools
      .map((tool) => `${tool.id}\u0000${tool.label}\u0000${tool.enabled ? '1' : '0'}`)
      .join('\u0001');
    if (signature !== this.toolOptionsSignature) {
      this.toolOptionsSignature = signature;
      this.toolSelect.replaceChildren(
        ...state.codexTools.map((tool) =>
          h(
            'option',
            {
              value: tool.id,
              selected: tool.id === state.selectedToolId,
              disabled: !tool.enabled,
            },
            `${tool.label} (${tool.id})${tool.enabled ? '' : ' [disabled]'}`
          )
        )
      );
    }
    if (this.toolSelect.value !== state.selectedToolId) {
      this.toolSelect.value = state.selectedToolId;
    }
  }

  private resolveStatusText(state: NativeThreadsState): string | null {
    if (state.loading) {
      return state.threads.length === 0
        ? 'Querying native thread metadata…'
        : 'Refreshing native thread metadata…';
    }
    if (state.errorMessage !== null) {
      return state.threads.length === 0
        ? null
        : 'Showing the last successful metadata page; the latest request failed.';
    }
    if (state.appliedContext === null) {
      return 'Passive view: no CLI or proxy commands are executed on page load or polling. Click "Load native thread metadata" to inspect read-only thread metadata.';
    }
    if (state.threads.length === 0) {
      return 'No native threads returned for the current filter.';
    }
    return null;
  }

  private buildThreadNodes(state: NativeThreadsState): HTMLElement[] {
    const boundByThreadUuid = new Map(
      state.bindings.map((binding) => [binding.thread_uuid, binding])
    );

    return state.threads.map((thread) => {
      const matchedBinding = boundByThreadUuid.get(thread.id) ?? null;
      const pairingStatusLabel = matchedBinding
        ? `Observed binding ${matchedBinding.binding_id} · enrollment unverified`
        : 'No observed binding · enrollment unverified';

      return h(
        'div',
        { className: 'aq-role' },
        h(
          'div',
          { className: 'aq-role-head' },
          h('span', undefined, thread.title?.trim() || 'Untitled native thread'),
          h(
            'span',
            {
              className: 'aq-badge',
              'data-status': 'warn',
              title:
                'A matching local binding record is an observation only; it does not verify enrollment or authenticated identity.',
            },
            pairingStatusLabel
          )
        ),
        h(
          'div',
          { className: 'aq-role-line' },
          h(
            'span',
            undefined,
            `Native thread.id: ${thread.id} · Status: ${thread.status}`
          )
        ),
        h(
          'div',
          { className: 'aq-role-line' },
          h(
            'span',
            undefined,
            `Model: ${thread.model ?? 'Unspecified'} · Effort: ${
              thread.effort ?? 'Unspecified'
            } · Project: ${thread.project_id ?? 'None'}`
          )
        ),
        h(
          'div',
          { className: 'aq-role-line' },
          h('span', undefined, `Working directory: ${thread.cwd}`)
        )
      );
    });
  }
}
