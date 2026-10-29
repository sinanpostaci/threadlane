import { createButton, h } from '../ui/dom.ts';
import type { BindingSummary } from '../types.ts';
import {
  desktop_native_threads,
  formatAppError,
  type NativeThread,
  type RegistrySnapshot,
} from '../registry/ipc.ts';

export class NativeLinkingPanel {
  private readonly onNavigateToTools: () => void;
  private readonly container: HTMLElement;
  private selectedToolId: string = '';
  private projectIdFilter: string = '';
  private threads: NativeThread[] = [];
  private nextCursor: string | null = null;
  private hasQueried: boolean = false;
  private loading: boolean = false;
  private errorMessage: string | null = null;
  private latestRegistry: RegistrySnapshot | null = null;
  private latestBindings: BindingSummary[] = [];

  constructor(onNavigateToTools: () => void) {
    this.onNavigateToTools = onNavigateToTools;
    this.container = h('section', {
      className: 'aq-card',
      'aria-label': 'Native Codex metadata and pairing verification',
    });
  }

  public render(registry: RegistrySnapshot, bindings: BindingSummary[]): HTMLElement {
    this.latestRegistry = registry;
    this.latestBindings = bindings;

    const codexTools = registry.tools.filter((t) => t.adapter === 'codex_cli');
    if (
      !this.selectedToolId ||
      !codexTools.some((t) => t.id === this.selectedToolId)
    ) {
      const enabledTool = codexTools.find((t) => t.enabled) ?? codexTools[0];
      this.selectedToolId = enabledTool ? enabledTool.id : '';
    }

    this.rebuildDom();
    return this.container;
  }

  private async handleFetchThreads(appendCursor: string | null): Promise<void> {
    if (this.loading) {
      return;
    }
    if (!this.selectedToolId) {
      this.errorMessage = 'Configure and select a Codex CLI tool in Tools first.';
      this.rebuildDom();
      return;
    }

    this.loading = true;
    this.errorMessage = null;
    this.rebuildDom();

    try {
      const page = await desktop_native_threads({
        toolId: this.selectedToolId,
        projectId: this.projectIdFilter.trim() || null,
        cursor: appendCursor,
      });
      this.hasQueried = true;
      if (appendCursor) {
        const existingIds = new Set(this.threads.map((t) => t.id));
        for (const thread of page.threads) {
          if (!existingIds.has(thread.id)) {
            this.threads.push(thread);
          }
        }
      } else {
        this.threads = page.threads;
      }
      this.nextCursor = page.next_cursor;
    } catch (err) {
      this.errorMessage = formatAppError(
        err,
        'Native Codex metadata query failed.'
      );
    } finally {
      this.loading = false;
      this.rebuildDom();
    }
  }

  private rebuildDom(): void {
    const registry = this.latestRegistry;
    const codexTools = (registry?.tools ?? []).filter((t) => t.adapter === 'codex_cli');
    const selectedTool = codexTools.find((t) => t.id === this.selectedToolId) ?? null;

    const nodes: Node[] = [];

    nodes.push(
      h(
        'div',
        { className: 'aq-section', style: 'margin-top: 14px;' },
        h('span', undefined, 'Native Codex metadata & pairing gate'),
        h(
          'span',
          { className: 'aq-badge', 'data-status': 'warn' },
          'needs_verification'
        )
      )
    );

    nodes.push(
      h(
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
      )
    );

    if (this.errorMessage) {
      nodes.push(
        h(
          'div',
          { className: 'aq-banner aq-banner-error', role: 'alert' },
          h('span', undefined, this.errorMessage)
        )
      );
    }

    if (codexTools.length === 0) {
      nodes.push(
        h(
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
        )
      );
      this.container.replaceChildren(...nodes);
      return;
    }

    const toolSelect = h(
      'select',
      {
        className: 'aq-select-input',
        'aria-label': 'Codex CLI tool',
        disabled: this.loading,
      },
      ...codexTools.map((tool) =>
        h(
          'option',
          {
            value: tool.id,
            selected: tool.id === this.selectedToolId,
          },
          `${tool.label} (${tool.id})${tool.enabled ? '' : ' [disabled]'}`
        )
      )
    );
    toolSelect.addEventListener('change', () => {
      this.selectedToolId = toolSelect.value;
      this.rebuildDom();
    });

    const projectInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: this.projectIdFilter,
      placeholder: 'Optional project_id filter',
      'aria-label': 'Project ID filter',
      disabled: this.loading,
    });
    projectInput.addEventListener('input', () => {
      this.projectIdFilter = projectInput.value;
    });

    const toolBlocked = selectedTool !== null && !selectedTool.enabled;
    const fetchBtn = createButton(
      this.loading ? 'Querying metadata…' : 'Load native thread metadata',
      () => {
        void this.handleFetchThreads(null);
      },
      this.loading || toolBlocked
    );
    fetchBtn.classList.add('aq-btn-primary');

    nodes.push(
      h(
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
      )
    );

    if (toolBlocked) {
      nodes.push(
        h(
          'p',
          { className: 'aq-note', style: 'color: var(--aq-warn);' },
          `Selected tool "${selectedTool.label}" is currently disabled in the registry. Enable it in Tools before querying native metadata.`
        )
      );
    }

    nodes.push(h('div', { className: 'aq-actions' }, fetchBtn));

    if (!this.hasQueried) {
      nodes.push(
        h(
          'p',
          { className: 'aq-note' },
          'Passive view: no CLI or proxy commands are executed on page load. Click "Load native thread metadata" to inspect read-only thread metadata.'
        )
      );
    } else if (this.threads.length === 0) {
      nodes.push(
        h('p', { className: 'aq-note' }, 'No native threads returned for the current filter.')
      );
    } else {
      const boundByThreadUuid = new Map(
        this.latestBindings.map((b) => [b.thread_uuid, b])
      );

      for (const thread of this.threads) {
        const matchedBinding = boundByThreadUuid.get(thread.id) ?? null;
        const pairingStatusLabel = matchedBinding
          ? `Observed in binding ${matchedBinding.binding_id}`
          : 'Unbound · needs_verification';
        const badgeTone = matchedBinding ? 'active' : 'warn';

        nodes.push(
          h(
            'div',
            { className: 'aq-role' },
            h(
              'div',
              { className: 'aq-role-head' },
              h('span', undefined, thread.title?.trim() || 'Untitled native thread'),
              h(
                'span',
                { className: 'aq-badge', 'data-status': badgeTone },
                pairingStatusLabel
              )
            ),
            h(
              'div',
              { className: 'aq-role-line' },
              h(
                'span',
                undefined,
                `Authoritative thread.id: ${thread.id} · Status: ${thread.status}`
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
          )
        );
      }

      if (this.nextCursor) {
        const cursor = this.nextCursor;
        nodes.push(
          h(
            'div',
            { className: 'aq-actions' },
            createButton(
              this.loading ? 'Loading…' : 'Load next page',
              () => {
                void this.handleFetchThreads(cursor);
              },
              this.loading
            )
          )
        );
      }
    }

    this.container.replaceChildren(...nodes);
  }
}
