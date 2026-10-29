import './tools.css';

import { desktop_decision_models, desktop_probe_tool, desktop_registry_apply, desktop_secret_delete, desktop_secret_set, desktop_secret_status, desktop_test_decision } from '../registry/ipc.ts';

import { createBrandIconElement } from '../icons.ts';
import type {
  ModelDefinition,
  RegistryChange,
  RegistrySnapshot,
  ToolDefinition,
} from '../registry/types.ts';
import { createButton, h } from '../ui/dom.ts';

type Adapter = ToolDefinition['adapter'];
type Provider = 'jev' | 'openai';
type Tone = 'ok' | 'warn' | 'danger';

interface ToolProbe {
  tool_id: string;
  availability: string;
  version: string | null;
  auth_status: string;
  models: ModelDefinition[];
  message: string;
}

interface DecisionAdvice {
  provider: string;
  model: string;
  difficulty: string | null;
  confidence: number | null;
  needs_review: boolean;
  reason_code: string;
  input_tokens: number | null;
  output_tokens: number | null;
}

interface SafeError {
  code: string | null;
  message: string;
}

interface Notice {
  tone: 'success' | 'error' | 'info';
  title?: string;
  text: string;
  code?: string | null;
}

interface DraftMeta {
  baseRevision: number;
  dirty: boolean;
  conflict: boolean;
  overwrite: boolean;
}

interface ToolDraft extends DraftMeta {
  id: string;
  label: string;
  adapter: Adapter;
  executable: string;
  enabled: boolean;
}

interface ModelDraft extends DraftMeta {
  id: string;
  toolId: string;
  label: string;
  modelArg: string;
  efforts: string;
  allowNoEffort: boolean;
}

interface ProbeState {
  loading: boolean;
  result: ToolProbe | null;
  error: SafeError | null;
}

interface SecretState {
  loading: boolean;
  status: string | null;
  error: SafeError | null;
  confirmDelete: boolean;
}

interface ApiState {
  loading: boolean;
  loaded: boolean;
  models: ModelDefinition[];
  error: SafeError | null;
  testModel: string;
  testLoading: boolean;
  advice: DecisionAdvice | null;
  testError: SafeError | null;
}

const ADAPTERS: readonly Adapter[] = [
  'codex_cli',
  'opencode_v2',
  'antigravity',
  'jev_api',
  'openai_decisions_api',
  'git',
  'tmux',
  'codegraph',
];

const ADAPTER_LABELS: Record<Adapter, string> = {
  codex_cli: 'Codex CLI',
  opencode_v2: 'OpenCode v2',
  antigravity: 'Antigravity',
  jev_api: 'Jev decisions API',
  openai_decisions_api: 'OpenAI decisions API',
  git: 'Git',
  tmux: 'tmux',
  codegraph: 'CodeGraph',
};

let pageCounter = 0;

function token(raw: string): string {
  let hash = 2166136261;
  for (let index = 0; index < raw.length; index += 1) {
    hash ^= raw.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  const slug = raw.replace(/[^a-zA-Z0-9_-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 36);
  return `${slug || 'item'}-${(hash >>> 0).toString(36)}`;
}

function providerFor(adapter: Adapter): Provider | null {
  if (adapter === 'jev_api') return 'jev';
  if (adapter === 'openai_decisions_api') return 'openai';
  return null;
}

function providerLabel(provider: Provider): string {
  return provider === 'jev' ? 'Jev' : 'OpenAI decisions';
}

function brandFor(adapter: Adapter) {
  if (adapter === 'codex_cli' || adapter === 'openai_decisions_api') return 'openai' as const;
  if (adapter === 'opencode_v2') return 'opencode' as const;
  if (adapter === 'antigravity') return 'antigravity' as const;
  return 'generic' as const;
}

function parseEfforts(raw: string): string[] {
  const seen = new Set<string>();
  const efforts: string[] = [];
  for (const part of raw.split(/[\s,]+/)) {
    const effort = part.trim();
    if (effort && !seen.has(effort)) {
      seen.add(effort);
      efforts.push(effort);
    }
  }
  return efforts;
}

function effortText(model: ModelDefinition): string {
  const values = Array.isArray(model.efforts) ? model.efforts : [];
  const efforts = values.length > 0 ? values.join(', ') : 'No efforts reported';
  return model.allow_no_effort ? `${efforts} · no-effort allowed` : efforts;
}

function safeText(value: unknown, fallback: string): string {
  const raw = typeof value === 'string' ? value : fallback;
  const clean = raw
    .replace(/bearer\s+[a-z0-9._~+/=-]+/gi, 'Bearer [redacted]')
    .replace(/\b(token|secret|api[_-]?key|authorization)\b(\s*[:=]\s*)\S+/gi, '$1$2[redacted]')
    .replace(/\bsk-[a-z0-9_-]{8,}/gi, '[redacted]')
    .replace(/\s+/g, ' ')
    .trim();
  if (!clean) return fallback;
  return clean.length > 280 ? `${clean.slice(0, 277)}...` : clean;
}

function safeError(err: unknown): SafeError {
  let code: unknown = null;
  let message: unknown = null;
  if (err && typeof err === 'object') {
    const record = err as Record<string, unknown>;
    code = record.code;
    message = record.message;
    if (typeof message !== 'string' && typeof record.error === 'string') {
      message = record.error;
    }
  }
  if (typeof message !== 'string' && typeof err === 'string') {
    message = err;
  }
  if (typeof message !== 'string' && err instanceof Error) {
    message = err.message;
  }
  if (typeof message === 'string' && message.trim().startsWith('{')) {
    try {
      const parsed = JSON.parse(message) as Record<string, unknown>;
      if (typeof parsed.message === 'string') message = parsed.message;
      if (typeof parsed.code === 'string') code = parsed.code;
    } catch {
      // Keep the sanitized original text.
    }
  }
  return {
    code:
      typeof code === 'string' && /^[a-zA-Z0-9_.-]{1,64}$/.test(code.trim())
        ? code.trim()
        : null,
    message: safeText(message, 'The request failed.'),
  };
}

function isConflict(error: SafeError): boolean {
  const text = `${error.code ?? ''} ${error.message}`.toLowerCase();
  return /(revision|conflict|stale|mismatch|expected.*(?:current|registry)|cas\b)/.test(text);
}

function formatAvailability(value: string): string {
  const status = value.trim().toLowerCase();
  if (status === 'found') return 'Found on PATH';
  if (status === 'not_found') return 'Not found';
  if (status === 'not_configured') return 'Not configured';
  if (status === 'unknown') return 'Unknown';
  return value.trim() || 'Unknown';
}

function formatAuth(value: string): string {
  const status = value.trim().toLowerCase();
  if (status === 'logged_in' || status === 'authenticated') return 'Logged in (CLI-reported)';
  if (status === 'logged_out') return 'Logged out (CLI-reported)';
  if (status === 'unknown') return 'Unknown';
  if (status === 'not_configured') return 'Not configured';
  return value.trim() || 'Unknown';
}

function availabilityTone(value: string): Tone {
  return value.trim().toLowerCase() === 'found' ? 'ok' : 'warn';
}

function secretTone(value: string): Tone {
  const status = value.trim().toLowerCase();
  if (status === 'configured' || status === 'available') return 'ok';
  if (status === 'locked' || status === 'unavailable') return 'danger';
  return 'warn';
}

function emptyTool(revision: number): ToolDraft {
  return {
    id: '',
    label: '',
    adapter: 'codex_cli',
    executable: '',
    enabled: true,
    baseRevision: revision,
    dirty: false,
    conflict: false,
    overwrite: false,
  };
}

function toolDraft(tool: ToolDefinition, revision: number): ToolDraft {
  return {
    id: tool.id,
    label: tool.label,
    adapter: tool.adapter,
    executable: tool.executable_path ?? '',
    enabled: tool.enabled,
    baseRevision: revision,
    dirty: false,
    conflict: false,
    overwrite: false,
  };
}

function sameTool(tool: ToolDefinition, draft: ToolDraft): boolean {
  return (
    tool.label === draft.label &&
    tool.adapter === draft.adapter &&
    (tool.executable_path ?? '') === draft.executable.trim() &&
    tool.enabled === draft.enabled
  );
}

function emptyModel(revision: number, toolId: string): ModelDraft {
  return {
    id: '',
    toolId,
    label: '',
    modelArg: '',
    efforts: '',
    allowNoEffort: false,
    baseRevision: revision,
    dirty: false,
    conflict: false,
    overwrite: false,
  };
}

function modelDraft(model: ModelDefinition, revision: number): ModelDraft {
  return {
    id: model.id,
    toolId: model.tool_id,
    label: model.label,
    modelArg: model.model_arg,
    efforts: model.efforts.join(', '),
    allowNoEffort: model.allow_no_effort,
    baseRevision: revision,
    dirty: false,
    conflict: false,
    overwrite: false,
  };
}

function sameModel(model: ModelDefinition, draft: ModelDraft): boolean {
  const efforts = parseEfforts(draft.efforts);
  return (
    model.tool_id === draft.toolId &&
    model.label === draft.label &&
    model.model_arg === draft.modelArg &&
    model.allow_no_effort === draft.allowNoEffort &&
    model.efforts.length === efforts.length &&
    model.efforts.every((effort, index) => effort === efforts[index])
  );
}

function hasProfileReference(snapshot: RegistrySnapshot, modelId: string): boolean {
  return snapshot.profiles.some((profile) =>
    profile.assignments.some((assignment) => assignment.choice.model_id === modelId)
  );
}

interface FocusState {
  key: string;
  start: number | null;
  end: number | null;
}

function captureFocus(root: HTMLElement): FocusState | null {
  if (typeof document === 'undefined') return null;
  const active = document.activeElement;
  if (!(active instanceof HTMLElement) || !root.contains(active)) {
    return null;
  }
  const key = active.dataset.focusKey;
  if (!key) {
    return null;
  }
  const input = active as HTMLInputElement;
  return {
    key,
    start: typeof input.selectionStart === 'number' ? input.selectionStart : null,
    end: typeof input.selectionEnd === 'number' ? input.selectionEnd : null,
  };
}

function restoreFocus(root: HTMLElement, state: FocusState | null, disposed: () => boolean): void {
  if (!state) return;
  const restore = (): void => {
    if (disposed() || !root.isConnected) return;
    const target = root.querySelector<HTMLElement>(`[data-focus-key="${state.key}"]`);
    if (!target) return;
    target.focus({ preventScroll: true });
    if (
      (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) &&
      state.start !== null &&
      state.end !== null
    ) {
      try {
        target.setSelectionRange(state.start, state.end);
      } catch {
        // Non-text controls do not expose a selection range.
      }
    }
  };
  restore();
  queueMicrotask(restore);
}

export class ToolsPage {
  private readonly onRegistryChanged: () => void;
  private readonly pageId = `aq-tools-${++pageCounter}`;
  private root: HTMLElement | null = null;
  private snapshot: RegistrySnapshot | null = null;
  private renderedRevision: number | null = null;
  private disposed = false;
  private notice: Notice | null = null;
  private pending = new Set<string>();
  private newTool = emptyTool(-1);
  private newModel = emptyModel(-1, '');
  private toolDrafts = new Map<string, ToolDraft>();
  private modelDrafts = new Map<string, ModelDraft>();
  private probes = new Map<string, ProbeState>();
  private secrets = new Map<string, SecretState>();
  private apiStates = new Map<string, ApiState>();
  private selections = new Map<string, Set<string>>();
  private deleteConfirm: { kind: 'tool' | 'model'; id: string } | null = null;

  constructor(onRegistryChanged: () => void) {
    this.onRegistryChanged = onRegistryChanged;
  }

  render(snapshot: RegistrySnapshot): HTMLElement {
    if (this.disposed) {
      return h('section', { className: 'aq-tools-page', 'aria-label': 'Tools and models' });
    }
    const needsRender = this.root === null || this.renderedRevision !== snapshot.revision;
    this.snapshot = snapshot;
    if (this.newTool.baseRevision < 0) this.newTool.baseRevision = snapshot.revision;
    if (this.newModel.baseRevision < 0) this.newModel.baseRevision = snapshot.revision;
    if (
      !this.newModel.dirty &&
      (!this.newModel.toolId || !snapshot.tools.some((tool) => tool.id === this.newModel.toolId))
    ) {
      this.newModel.toolId = snapshot.tools[0]?.id ?? '';
    }
    const root =
      this.root ??
      h('section', {
        className: 'aq-tools-page',
        'aria-labelledby': `${this.pageId}-title`,
      });
    this.root = root;
    if (needsRender) this.rewrite();
    return root;
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.pending.clear();
    this.toolDrafts.clear();
    this.modelDrafts.clear();
    this.probes.clear();
    this.secrets.clear();
    this.apiStates.clear();
    this.selections.clear();
    this.deleteConfirm = null;
    this.snapshot = null;
    this.renderedRevision = null;
    this.notice = null;
    this.root?.replaceChildren();
    this.root = null;
  }

  private id(key: string): string {
    return `${this.pageId}-${token(key)}`;
  }

  private refresh(): void {
    if (!this.disposed) this.rewrite();
  }

  private rewrite(): void {
    const root = this.root;
    const snapshot = this.snapshot;
    if (!root || !snapshot) return;
    const focus = captureFocus(root);
    root.replaceChildren(...this.build(snapshot));
    root.dataset.revision = String(snapshot.revision);
    this.renderedRevision = snapshot.revision;
    restoreFocus(root, focus, () => this.disposed);
  }

  private notifyOuter(): void {
    try {
      this.onRegistryChanged();
    } catch {
      // The integrator owns registry refresh feedback.
    }
  }

  private build(snapshot: RegistrySnapshot): Node[] {
    const nodes: Node[] = [this.buildHeader(snapshot)];
    if (this.notice) nodes.push(this.buildNotice(this.notice));
    nodes.push(
      h(
        'p',
        { className: 'aq-note aq-tools-guidance' },
        'Saves use a registry revision check. Nothing is probed, imported, or sent until you choose an action. PATH discovery, CLI login state, credential status, and catalog metadata are separate from entitlement.'
      )
    );
    nodes.push(this.buildTools(snapshot));
    nodes.push(this.buildModels(snapshot));
    nodes.push(
      h(
        'p',
        { className: 'aq-note aq-tools-footer' },
        'Deleting a referenced tool or model is rejected here and by the Rust registry. Revision 0 starts with no implicit model defaults.'
      )
    );
    return nodes;
  }

  private buildHeader(snapshot: RegistrySnapshot): HTMLElement {
    return h(
      'div',
      { className: 'aq-tools-hero' },
      h(
        'div',
        undefined,
        h('p', { className: 'aq-kicker' }, 'Global registry'),
        h('h2', { className: 'aq-page-title', id: `${this.pageId}-title` }, 'Tools and models'),
        h(
          'p',
          { className: 'aq-note' },
          'Configure adapters, executable paths, manual model/effort entries, and explicit catalog imports. Editing preferences here does not change running jobs.'
        )
      ),
      h(
        'div',
        { className: 'aq-tools-hero-actions' },
        h('span', { className: 'aq-badge' }, `Revision ${snapshot.revision}`),
        h(
          'span',
          { className: 'aq-badge' },
          `${snapshot.tools.length} tool${snapshot.tools.length === 1 ? '' : 's'} · ${snapshot.models.length} model${snapshot.models.length === 1 ? '' : 's'}`
        ),
        this.button('Reload registry', () => this.notifyOuter(), 'reload-registry')
      )
    );
  }

  private buildNotice(notice: Notice): HTMLElement {
    return h(
      'div',
      {
        className: `aq-tools-notice aq-tools-notice-${notice.tone}`,
        role: notice.tone === 'error' ? 'alert' : 'status',
      },
      h(
        'div',
        { className: 'aq-tools-notice-copy' },
        h(
          'div',
          { className: 'aq-tools-notice-head' },
          h(
            'strong',
            undefined,
            notice.title ?? (notice.tone === 'error' ? 'Request failed' : 'Registry updated')
          ),
          notice.code ? h('code', { className: 'aq-tools-code' }, notice.code) : null
        ),
        h('p', { className: 'aq-tools-notice-text' }, notice.text)
      ),
      this.button(
        'Dismiss',
        () => {
          this.notice = null;
          this.refresh();
        },
        'dismiss-notice'
      )
    );
  }

  private buildTools(snapshot: RegistrySnapshot): HTMLElement {
    const enabled = snapshot.tools.filter((tool) => tool.enabled).length;
    return h(
      'section',
      { className: 'aq-tools-section', 'aria-labelledby': `${this.pageId}-tools-title` },
      h(
        'div',
        { className: 'aq-tools-section-head' },
        h(
          'div',
          undefined,
          h('h3', { className: 'aq-tools-heading', id: `${this.pageId}-tools-title` }, 'Tools'),
          h(
            'p',
            { className: 'aq-note' },
            'Custom executable paths and enabled state live here. API adapters never spawn a CLI.'
          )
        ),
        h('span', { className: 'aq-badge' }, `${enabled} enabled`)
      ),
      this.buildCreateTool(snapshot),
      snapshot.tools.length === 0
        ? h(
            'p',
            { className: 'aq-note aq-tools-empty' },
            'No tools are configured. Add a tool explicitly; no model defaults are created.'
          )
        : h(
            'div',
            { className: 'aq-tools-list' },
            ...snapshot.tools.map((tool) => this.buildTool(snapshot, tool))
          )
    );
  }

  private buildCreateTool(snapshot: RegistrySnapshot): HTMLElement {
    const draft = this.newTool;
    const duplicate =
      draft.id.trim() !== '' && snapshot.tools.some((tool) => tool.id === draft.id.trim());
    const id = this.input('new-tool-id', draft.id, 'unique-tool-id');
    const label = this.input('new-tool-label', draft.label, 'Display label');
    const adapter = this.adapterSelect('new-tool-adapter', draft.adapter, (value) => {
      draft.adapter = value;
      draft.dirty = true;
    });
    const executable = this.input(
      'new-tool-executable',
      draft.executable,
      '/absolute/path/to/executable'
    );
    const enabled = this.checkbox('new-tool-enabled', draft.enabled);
    id.addEventListener('input', () => {
      draft.id = id.value;
      draft.dirty = true;
    });
    label.addEventListener('input', () => {
      draft.label = label.value;
      draft.dirty = true;
    });
    executable.addEventListener('input', () => {
      draft.executable = executable.value;
      draft.dirty = true;
    });
    enabled.addEventListener('change', () => {
      draft.enabled = enabled.checked;
      draft.dirty = true;
    });
    return h(
      'div',
      {
        className: 'aq-tools-form aq-tools-create',
        role: 'group',
        'aria-labelledby': `${this.pageId}-add-tool`,
      },
      h(
        'div',
        { className: 'aq-tools-form-head' },
        h('h4', { id: `${this.pageId}-add-tool` }, 'Add tool'),
        h(
          'p',
          { className: 'aq-note' },
          'ID and label are required. The executable path is optional and is never executed by the registry.'
        )
      ),
      h(
        'div',
        { className: 'aq-tools-form-grid' },
        this.field('Tool ID', id, 'Unique registry key. Existing IDs are not overwritten here.'),
        this.field('Label', label),
        this.field('Adapter', adapter, 'API adapters use the Rust vault and never spawn a CLI.'),
        this.field(
          'Executable path',
          executable,
          'Optional absolute path for custom tools. PATH discovery is explicit.'
        ),
        this.checkField('Enabled for profiles', enabled)
      ),
      duplicate ? h('p', { className: 'aq-tools-inline-error' }, 'This tool ID already exists.') : null,
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          this.pending.has('tool-create') ? 'Adding…' : 'Add tool',
          () => void this.createTool(),
          'new-tool-submit',
          'primary',
          this.pending.has('tool-create')
        )
      )
    );
  }

  private buildTool(snapshot: RegistrySnapshot, tool: ToolDefinition): HTMLElement {
    const draft = this.toolEdit(tool);
    const changed = !sameTool(tool, draft);
    const provider = providerFor(tool.adapter);
    const probe = this.probes.get(tool.id);
    const using = snapshot.models.filter((model) => model.tool_id === tool.id);
    const deleting = this.pending.has(`tool-delete:${tool.id}`);
    const confirming = this.deleteConfirm?.kind === 'tool' && this.deleteConfirm?.id === tool.id;
    const label = this.input(`tool-${tool.id}-label`, draft.label, 'Display label');
    const adapter = this.adapterSelect(`tool-${tool.id}-adapter`, draft.adapter, (value) => {
      draft.adapter = value;
      draft.dirty = true;
    });
    const executable = this.input(
      `tool-${tool.id}-executable`,
      draft.executable,
      '/absolute/path/to/executable'
    );
    const enabled = this.checkbox(`tool-${tool.id}-enabled`, draft.enabled);
    label.addEventListener('input', () => {
      draft.label = label.value;
      draft.dirty = true;
    });
    executable.addEventListener('input', () => {
      draft.executable = executable.value;
      draft.dirty = true;
    });
    enabled.addEventListener('change', () => {
      draft.enabled = enabled.checked;
      draft.dirty = true;
    });
    const saveDisabled =
      this.pending.has(`tool-save:${tool.id}`) || (draft.conflict && !draft.overwrite);
    return h(
      'article',
      { className: `aq-tools-card${tool.enabled ? '' : ' aq-tools-card-disabled'}` },
      h(
        'div',
        { className: 'aq-tools-tool-head' },
        createBrandIconElement(brandFor(tool.adapter)),
        h(
          'div',
          { className: 'aq-tools-tool-title' },
          h('h4', undefined, tool.label),
          h('code', { className: 'aq-tools-code' }, tool.id)
        ),
        h(
          'div',
          { className: 'aq-tools-badges' },
          h(
            'span',
            { className: 'aq-badge', 'data-status': tool.enabled ? 'active' : 'warn' },
            tool.enabled ? 'Enabled' : 'Disabled'
          ),
          h('span', { className: 'aq-badge' }, ADAPTER_LABELS[tool.adapter]),
          provider ? h('span', { className: 'aq-badge' }, 'API adapter') : null,
          changed ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Unsaved draft') : null
        )
      ),
      h(
        'div',
        { className: 'aq-tools-form-grid aq-tools-edit-grid' },
        this.field('Label', label),
        this.field('Adapter', adapter),
        this.field(
          'Executable path',
          executable,
          'Custom path applies to this tool only. Save before probing a changed path.'
        ),
        this.checkField('Enabled for profiles', enabled)
      ),
      this.conflictBlock(
        draft,
        () => {
          this.toolDrafts.delete(tool.id);
          this.refresh();
        },
        `tool-${tool.id}-conflict`
      ),
      h(
        'div',
        { className: 'aq-tools-actions aq-tools-card-actions' },
        this.button(
          this.pending.has(`tool-save:${tool.id}`)
            ? 'Saving…'
            : draft.conflict && draft.overwrite
              ? 'Overwrite tool'
              : 'Save changes',
          () => void this.saveTool(tool),
          `tool-${tool.id}-save`,
          draft.conflict && draft.overwrite ? 'danger' : 'primary',
          saveDisabled
        ),
        this.button(
          'Discard draft',
          () => {
            this.toolDrafts.delete(tool.id);
            this.refresh();
          },
          `tool-${tool.id}-discard`,
          undefined,
          this.pending.has(`tool-save:${tool.id}`)
        ),
        provider === null
          ? this.button(
              this.pending.has(`probe:${tool.id}`) ? 'Probing…' : 'Probe saved tool',
              () => void this.probe(tool),
              `tool-${tool.id}-probe`,
              undefined,
              this.pending.has(`probe:${tool.id}`)
            )
          : null,
        using.length > 0
          ? h(
              'span',
              { className: 'aq-tools-hint aq-tools-delete-note' },
              `Delete ${using.length} model${using.length === 1 ? '' : 's'} first.`
            )
          : confirming
            ? h(
                'span',
                { className: 'aq-tools-delete-confirm' },
                h('span', undefined, `Delete ${tool.label}?`),
                provider
                  ? h(
                      'span',
                      { className: 'aq-tools-hint' },
                      'The OS vault credential is not removed by tool deletion.'
                    )
                  : null,
                this.button(
                  deleting ? 'Deleting…' : 'Confirm delete',
                  () => void this.deleteTool(tool),
                  `tool-${tool.id}-confirm-delete`,
                  'danger',
                  deleting
                ),
                this.button(
                  'Cancel',
                  () => {
                    this.deleteConfirm = null;
                    this.refresh();
                  },
                  `tool-${tool.id}-cancel-delete`
                )
              )
            : this.button(
                'Delete tool',
                () => {
                  this.deleteConfirm = { kind: 'tool', id: tool.id };
                  this.refresh();
                },
                `tool-${tool.id}-delete`,
                'danger',
                deleting
              )
      ),
      h(
        'p',
        { className: 'aq-tools-hint' },
        changed
          ? 'Draft changes are not used by probing or API controls until saved.'
          : provider === null
            ? 'Probe uses the saved tool definition. A found executable is not login or subscription proof.'
            : 'API adapters never spawn a CLI. Credential and synthetic controls use the saved tool definition.'
      ),
      this.buildStatus(provider, probe),
      provider === null ? this.buildProbe(tool, probe) : null,
      provider ? this.buildApi(tool, provider) : null
    );
  }

  private buildStatus(provider: Provider | null, probe: ProbeState | undefined): HTMLElement {
    const result = probe?.result ?? null;
    const availability = provider
      ? 'Not applicable'
      : result?.availability ?? (probe?.loading ? 'Checking…' : 'Not checked');
    const auth = provider
      ? 'Not applicable'
      : result?.auth_status ?? (probe?.loading ? 'Checking…' : 'Not checked');
    return h(
      'div',
      { className: 'aq-tools-status-panel' },
      h(
        'div',
        { className: 'aq-tools-status-grid' },
        this.statusRow(
          'PATH availability',
          safeText(formatAvailability(availability), 'Unknown'),
          provider === null ? availabilityTone(availability) : undefined
        ),
        this.statusRow('CLI login state', safeText(formatAuth(auth), 'Unknown')),
        this.statusRow('Capability', 'Unverified', 'warn'),
        result?.version ? this.statusRow('Reported version', safeText(result.version, 'Unknown')) : null
      ),
      h(
        'p',
        { className: 'aq-tools-hint' },
        provider
          ? 'Credential status is separate from CLI login and model entitlement. No credential is read on page load.'
          : 'Installed availability, CLI login state, and capability are separate signals. A found executable does not prove login, subscription, or effort support.'
      ),
      result?.message
        ? h('p', { className: 'aq-tools-message' }, safeText(result.message, 'No diagnostic message.'))
        : null,
      probe?.loading ? h('p', { className: 'aq-tools-inline-status', role: 'status' }, 'Probing saved tool…') : null,
      probe?.error ? this.errorBlock(probe.error) : null
    );
  }

  private buildProbe(tool: ToolDefinition, probe: ProbeState | undefined): HTMLElement {
    if (!probe || (!probe.result && !probe.error && !probe.loading)) {
      return h(
        'p',
        { className: 'aq-note aq-tools-probe-empty' },
        'No probe result. Nothing has been executed for this tool in this view.'
      );
    }
    if (!probe.result) {
      return h(
        'div',
        { className: 'aq-tools-probe' },
        h('h5', undefined, 'Catalog probe'),
        probe.error ? this.errorBlock(probe.error) : null
      );
    }
    const key = `probe:${tool.id}`;
    const models = probe.result.models.filter((model) => this.importable(model));
    const selection = this.selection(key, models);
    const existing = models.filter(
      (model) =>
        selection.has(model.id) &&
        this.snapshot?.models.some((saved) => saved.id === model.id)
    ).length;
    return h(
      'div',
      { className: 'aq-tools-probe' },
      h('h5', undefined, 'Catalog probe'),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'Catalog metadata only. Import uses upsert_models at the latest registry revision; entitlement and effort support remain unverified.'
      ),
      existing > 0
        ? h(
            'p',
            { className: 'aq-tools-hint' },
            `${existing} catalog model${existing === 1 ? '' : 's'} already exist and would be updated by this explicit import.`
          )
        : null,
      probe.result.models.length === 0
        ? h('p', { className: 'aq-note' }, 'The catalog reported no models.')
        : h(
            'div',
            { className: 'aq-tools-catalog' },
            ...probe.result.models.map((model, index) =>
              this.catalogRow(key, model, selection, index)
            )
          ),
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          this.pending.has(`import:${key}`)
            ? 'Importing…'
            : `Import selected${selection.size > 0 ? ` (${selection.size})` : ''}`,
          () => void this.importModels(tool, key, models),
          `${key}-import`,
          selection.size > 0 ? 'primary' : undefined,
          selection.size === 0 || this.pending.has(`import:${key}`)
        )
      )
    );
  }

  private buildApi(tool: ToolDefinition, provider: Provider): HTMLElement {
    const secret = this.secretState(tool.id);
    const api = this.apiState(tool.id);
    const secretBusy =
      this.pending.has(`secret-status:${tool.id}`) ||
      this.pending.has(`secret-set:${tool.id}`) ||
      this.pending.has(`secret-delete:${tool.id}`);
    const status = secret.loading
      ? 'Checking…'
      : secret.status ?? (secret.error ? 'Unavailable' : 'Not checked');
    const credential = this.input(`api-${tool.id}-credential`, '', 'Paste API credential', {
      type: 'password',
      autocomplete: 'new-password',
    });
    credential.setAttribute('autocapitalize', 'off');
    credential.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        void this.saveSecret(tool, credential);
      }
    });
    const listId = this.id(`api-${tool.id}-model-list`);
    const suggestions = new Map<string, string>();
    for (const model of this.snapshot?.models.filter((entry) => entry.tool_id === tool.id) ?? []) {
      if (model.model_arg) suggestions.set(model.model_arg, `${model.label} (${model.id})`);
    }
    for (const model of api.models) {
      if (model.model_arg) suggestions.set(model.model_arg, `${model.label} (catalog)`);
    }
    const testModel = this.input(`api-${tool.id}-test-model`, api.testModel, 'Provider model ID');
    testModel.setAttribute('list', listId);
    testModel.addEventListener('input', () => {
      api.testModel = testModel.value;
      api.advice = null;
      api.testError = null;
    });
    const catalogKey = `api:${tool.id}`;
    const selection = this.selection(catalogKey, api.models);
    const existingCatalog = api.models.filter(
      (model) =>
        selection.has(model.id) &&
        this.snapshot?.models.some((saved) => saved.id === model.id)
    ).length;
    return h(
      'div',
      { className: 'aq-tools-api' },
      h('h5', undefined, `${providerLabel(provider)} credential and synthetic test`),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'The credential is stored only by the Rust OS keychain bridge. This page never reads it back and never writes it to page state or browser storage.'
      ),
      h(
        'div',
        { className: 'aq-tools-status-panel' },
        h(
          'div',
          { className: 'aq-tools-status-grid' },
          this.statusRow(
            'Credential status',
            safeText(status, 'Unknown'),
            secret.error ? 'danger' : secret.status ? secretTone(secret.status) : undefined
          ),
          this.statusRow('CLI login state', 'Not applicable (API adapter)'),
          this.statusRow('Capability', 'Unverified', 'warn')
        ),
        secret.error ? this.errorBlock(secret.error) : null,
        secret.confirmDelete
          ? h(
              'div',
              { className: 'aq-tools-actions' },
              h('span', { className: 'aq-tools-hint' }, 'Remove the stored credential?'),
              this.button(
                secretBusy ? 'Removing…' : 'Confirm remove',
                () => void this.deleteSecret(tool),
                `api-${tool.id}-confirm-delete`,
                'danger',
                secretBusy
              ),
              this.button(
                'Cancel',
                () => {
                  secret.confirmDelete = false;
                  this.refresh();
                },
                `api-${tool.id}-cancel-delete`
              )
            )
          : h(
              'div',
              { className: 'aq-tools-actions' },
              this.button(
                secret.loading ? 'Checking…' : 'Check credential status',
                () => void this.checkSecret(tool.id),
                `api-${tool.id}-secret-status`,
                undefined,
                secretBusy
              ),
              this.button(
                'Remove credential',
                () => {
                  secret.confirmDelete = true;
                  this.refresh();
                },
                `api-${tool.id}-delete`,
                'danger',
                secretBusy
              )
            )
      ),
      this.field(
        'Set API credential',
        credential,
        'Submitted explicitly. The field is cleared before the vault request completes and is never retained in page state, localStorage, or the DOM.'
      ),
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          this.pending.has(`secret-set:${tool.id}`) ? 'Saving credential…' : 'Set credential',
          () => void this.saveSecret(tool, credential),
          `api-${tool.id}-secret-set`,
          'primary',
          secretBusy
        )
      ),
      h('hr', { className: 'aq-tools-rule' }),
      h('h5', undefined, 'API model catalog'),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'Explicit metadata request only. A model listed by the provider catalog is not proof of entitlement.'
      ),
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          api.loading ? 'Loading catalog…' : 'Load API model catalog',
          () => void this.loadModels(tool, provider),
          `api-${tool.id}-catalog`,
          undefined,
          this.pending.has(`catalog:${catalogKey}`)
        )
      ),
      api.error ? this.errorBlock(api.error) : null,
      api.loaded
        ? h(
            'div',
            { className: 'aq-tools-catalog' },
            api.models.length === 0
              ? h('p', { className: 'aq-note' }, 'The provider returned no model metadata.')
              : h(
                  'div',
                  undefined,
                  ...api.models.map((model, index) =>
                    this.catalogRow(catalogKey, model, selection, index)
                  )
                )
          )
        : null,
      existingCatalog > 0
        ? h(
            'p',
            { className: 'aq-tools-hint' },
            `${existingCatalog} selected catalog model${existingCatalog === 1 ? '' : 's'} already exist and would be updated by this explicit import.`
          )
        : null,
      api.loaded
        ? h(
            'div',
            { className: 'aq-tools-actions' },
            this.button(
              this.pending.has(`import:${catalogKey}`)
                ? 'Importing…'
                : `Import selected${selection.size > 0 ? ` (${selection.size})` : ''}`,
              () => void this.importModels(tool, catalogKey, api.models),
              `api-${tool.id}-catalog-import`,
              selection.size > 0 ? 'primary' : undefined,
              selection.size === 0 || this.pending.has(`import:${catalogKey}`)
            )
          )
        : null,
      h('hr', { className: 'aq-tools-rule' }),
      h('h5', undefined, 'Synthetic decision test'),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'Runs one fixed synthetic classification in Rust. No conversation text is sent, and this does not prove general API entitlement.'
      ),
      this.field(
        'Provider model ID',
        testModel,
        'Choose from suggestions or enter the exact provider model ID. No model is selected automatically.'
      ),
      h(
        'datalist',
        { id: listId },
        ...Array.from(suggestions, ([value, label]) => h('option', { value }, label))
      ),
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          api.testLoading ? 'Running synthetic test…' : 'Run synthetic decision test',
          () => void this.testDecision(tool, provider),
          `api-${tool.id}-decision-test`,
          'primary',
          this.pending.has(`decision-test:${tool.id}`)
        )
      ),
      api.testError ? this.errorBlock(api.testError) : null,
      api.advice ? this.buildAdvice(api.advice) : null
    );
  }

  private buildAdvice(advice: DecisionAdvice): HTMLElement {
    return h(
      'div',
      { className: 'aq-tools-advice', role: 'status' },
      h(
        'div',
        { className: 'aq-tools-advice-head' },
        h('h5', undefined, 'Synthetic advice'),
        h(
          'span',
          { className: 'aq-badge', 'data-status': advice.needs_review ? 'warn' : 'found' },
          advice.needs_review ? 'Review requested' : 'No review flag'
        )
      ),
      h(
        'dl',
        { className: 'aq-tools-facts' },
        this.fact('Provider', safeText(advice.provider, 'Unknown')),
        this.fact('Model', safeText(advice.model, 'Unknown')),
        this.fact('Difficulty', advice.difficulty ?? 'Not reported'),
        this.fact(
          'Confidence',
          typeof advice.confidence === 'number' ? String(advice.confidence) : 'Not reported'
        ),
        this.fact('Reason code', safeText(advice.reason_code, 'Not reported')),
        this.fact(
          'Input tokens',
          typeof advice.input_tokens === 'number' ? String(advice.input_tokens) : 'Not reported'
        ),
        this.fact(
          'Output tokens',
          typeof advice.output_tokens === 'number' ? String(advice.output_tokens) : 'Not reported'
        )
      ),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'One synthetic classification only. No task content was included and no automatic action was taken.'
      )
    );
  }

  private buildModels(snapshot: RegistrySnapshot): HTMLElement {
    const toolById = new Map(snapshot.tools.map((tool) => [tool.id, tool]));
    const models = [...snapshot.models].sort((left, right) => {
      const leftTool = toolById.get(left.tool_id)?.label ?? left.tool_id;
      const rightTool = toolById.get(right.tool_id)?.label ?? right.tool_id;
      return leftTool.localeCompare(rightTool) || left.label.localeCompare(right.label);
    });
    return h(
      'section',
      { className: 'aq-tools-section', 'aria-labelledby': `${this.pageId}-models-title` },
      h(
        'div',
        { className: 'aq-tools-section-head' },
        h(
          'div',
          undefined,
          h('h3', { className: 'aq-tools-heading', id: `${this.pageId}-models-title` }, 'Models'),
          h(
            'p',
            { className: 'aq-note' },
            'Manual model and effort entries are authoritative here. Catalog imports do not mark efforts as verified.'
          )
        ),
        h('span', { className: 'aq-badge' }, `${models.length} model${models.length === 1 ? '' : 's'}`)
      ),
      this.buildCreateModel(snapshot),
      models.length === 0
        ? h(
            'p',
            { className: 'aq-note aq-tools-empty' },
            'No models are configured. Add a manual model and its explicit effort list.'
          )
        : h(
            'div',
            { className: 'aq-tools-list' },
            ...models.map((model) => this.buildModel(snapshot, model))
          )
    );
  }

  private buildCreateModel(snapshot: RegistrySnapshot): HTMLElement {
    const draft = this.newModel;
    const tool = this.toolSelect('new-model-tool', draft.toolId, (value) => {
      draft.toolId = value;
      draft.dirty = true;
    });
    const id = this.input('new-model-id', draft.id, 'unique-model-id');
    const label = this.input('new-model-label', draft.label, 'Display label');
    const arg = this.input('new-model-arg', draft.modelArg, 'provider model argument');
    const efforts = this.input('new-model-efforts', draft.efforts, 'low, medium, high, max');
    const allow = this.checkbox('new-model-no-effort', draft.allowNoEffort);
    id.addEventListener('input', () => {
      draft.id = id.value;
      draft.dirty = true;
    });
    label.addEventListener('input', () => {
      draft.label = label.value;
      draft.dirty = true;
    });
    arg.addEventListener('input', () => {
      draft.modelArg = arg.value;
      draft.dirty = true;
    });
    efforts.addEventListener('input', () => {
      draft.efforts = efforts.value;
      draft.dirty = true;
    });
    allow.addEventListener('change', () => {
      draft.allowNoEffort = allow.checked;
      draft.dirty = true;
    });
    const duplicate =
      draft.id.trim() !== '' && snapshot.models.some((model) => model.id === draft.id.trim());
    const noTools = snapshot.tools.length === 0;
    return h(
      'div',
      {
        className: 'aq-tools-form aq-tools-create',
        role: 'group',
        'aria-labelledby': `${this.pageId}-add-model`,
      },
      h(
        'div',
        { className: 'aq-tools-form-head' },
        h('h4', { id: `${this.pageId}-add-model` }, 'Add manual model'),
        h(
          'p',
          { className: 'aq-note' },
          'Efforts are entered manually. Do not add an effort the provider may not support; catalog metadata is not verification.'
        )
      ),
      h(
        'div',
        { className: 'aq-tools-form-grid' },
        this.field('Tool', tool, noTools ? 'Add a tool before creating a model.' : undefined),
        this.field('Model ID', id, 'Unique registry key. Existing IDs are not overwritten by this form.'),
        this.field('Label', label),
        this.field('Model arg', arg, 'Exact provider argument passed to the adapter when selected.'),
        this.field(
          'Efforts',
          efforts,
          'Comma or space separated manual entries. Leave empty only if this model should be saved without effort metadata.'
        ),
        this.checkField('Allow selection without effort', allow, 'This does not imply any specific effort is supported.')
      ),
      duplicate ? h('p', { className: 'aq-tools-inline-error' }, 'This model ID already exists.') : null,
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          this.pending.has('model-create') ? 'Adding…' : 'Add model',
          () => void this.createModel(),
          'new-model-submit',
          'primary',
          noTools || this.pending.has('model-create')
        )
      )
    );
  }

  private buildModel(snapshot: RegistrySnapshot, model: ModelDefinition): HTMLElement {
    const draft = this.modelEdit(model);
    const changed = !sameModel(model, draft);
    const toolMissing = !snapshot.tools.some((tool) => tool.id === draft.toolId);
    const references = hasProfileReference(snapshot, model.id);
    const deleting = this.pending.has(`model-delete:${model.id}`);
    const confirming = this.deleteConfirm?.kind === 'model' && this.deleteConfirm?.id === model.id;
    const tool = this.toolSelect(`model-${model.id}-tool`, draft.toolId, (value) => {
      draft.toolId = value;
      draft.dirty = true;
    });
    const label = this.input(`model-${model.id}-label`, draft.label, 'Display label');
    const arg = this.input(`model-${model.id}-arg`, draft.modelArg, 'provider model argument');
    const efforts = this.input(`model-${model.id}-efforts`, draft.efforts, 'low, medium, high, max');
    const allow = this.checkbox(`model-${model.id}-no-effort`, draft.allowNoEffort);
    label.addEventListener('input', () => {
      draft.label = label.value;
      draft.dirty = true;
    });
    arg.addEventListener('input', () => {
      draft.modelArg = arg.value;
      draft.dirty = true;
    });
    efforts.addEventListener('input', () => {
      draft.efforts = efforts.value;
      draft.dirty = true;
    });
    allow.addEventListener('change', () => {
      draft.allowNoEffort = allow.checked;
      draft.dirty = true;
    });
    return h(
      'article',
      { className: 'aq-tools-card aq-tools-model-card' },
      h(
        'div',
        { className: 'aq-tools-model-head' },
        h(
          'div',
          { className: 'aq-tools-tool-title' },
          h('h4', undefined, model.label),
          h('code', { className: 'aq-tools-code' }, model.id),
          h(
            'span',
            { className: 'aq-tools-hint' },
            `${snapshot.tools.find((entry) => entry.id === model.tool_id)?.label ?? 'Missing tool'} · ${model.model_arg} · ${effortText(model)}`
          )
        ),
        h(
          'div',
          { className: 'aq-tools-badges' },
          h('span', { className: 'aq-badge' }, model.tool_id),
          changed ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Unsaved draft') : null,
          references ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Referenced') : null
        )
      ),
      h(
        'div',
        { className: 'aq-tools-form-grid aq-tools-edit-grid' },
        this.field(
          'Tool',
          tool,
          toolMissing ? 'The referenced tool is missing. Choose a current tool or discard.' : undefined
        ),
        this.field('Label', label),
        this.field('Model arg', arg),
        this.field('Efforts', efforts, 'Manual entries only. No effort is inferred from a model name.'),
        this.checkField('Allow selection without effort', allow)
      ),
      this.conflictBlock(
        draft,
        () => {
          this.modelDrafts.delete(model.id);
          this.refresh();
        },
        `model-${model.id}-conflict`
      ),
      h(
        'div',
        { className: 'aq-tools-actions aq-tools-card-actions' },
        this.button(
          this.pending.has(`model-save:${model.id}`)
            ? 'Saving…'
            : draft.conflict && draft.overwrite
              ? 'Overwrite model'
              : 'Save changes',
          () => void this.saveModel(model),
          `model-${model.id}-save`,
          draft.conflict && draft.overwrite ? 'danger' : 'primary',
          toolMissing ||
            this.pending.has(`model-save:${model.id}`) ||
            (draft.conflict && !draft.overwrite)
        ),
        this.button(
          'Discard draft',
          () => {
            this.modelDrafts.delete(model.id);
            this.refresh();
          },
          `model-${model.id}-discard`,
          undefined,
          this.pending.has(`model-save:${model.id}`)
        ),
        references
          ? h(
              'span',
              { className: 'aq-tools-hint aq-tools-delete-note' },
              'Referenced by a workflow profile; remove those assignments first.'
            )
          : confirming
            ? h(
                'span',
                { className: 'aq-tools-delete-confirm' },
                h('span', undefined, `Delete ${model.label}?`),
                this.button(
                  deleting ? 'Deleting…' : 'Confirm delete',
                  () => void this.deleteModel(model),
                  `model-${model.id}-confirm-delete`,
                  'danger',
                  deleting
                ),
                this.button(
                  'Cancel',
                  () => {
                    this.deleteConfirm = null;
                    this.refresh();
                  },
                  `model-${model.id}-cancel-delete`
                )
              )
            : this.button(
                'Delete model',
                () => {
                  this.deleteConfirm = { kind: 'model', id: model.id };
                  this.refresh();
                },
                `model-${model.id}-delete`,
                'danger',
                deleting
              )
      )
    );
  }

  private conflictBlock(draft: DraftMeta, discard: () => void, key: string): HTMLElement | null {
    if (!draft.conflict) return null;
    const overwrite = this.checkbox(`${key}-overwrite`, draft.overwrite);
    overwrite.addEventListener('change', () => {
      draft.overwrite = overwrite.checked;
      this.refresh();
    });
    return h(
      'div',
      { className: 'aq-tools-conflict', role: 'status' },
      h('p', { className: 'aq-tools-conflict-title' }, 'Registry revision changed while this draft was open.'),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'Your local values are preserved. Reload the latest registry before saving, or explicitly allow overwrite. Nothing has been overwritten silently.'
      ),
      h(
        'div',
        { className: 'aq-tools-actions' },
        this.button(
          'Reload latest',
          () => {
            draft.overwrite = false;
            this.notifyOuter();
            this.refresh();
          },
          `${key}-reload`
        ),
        this.button('Discard draft', discard, `${key}-discard`)
      ),
      this.checkField(
        'Allow overwrite with this draft',
        overwrite,
        'Required before saving the preserved local values over a newer registry revision.'
      )
    );
  }

  private catalogRow(
    key: string,
    model: ModelDefinition,
    selection: Set<string>,
    index: number
  ): HTMLElement {
    const importable = this.importable(model);
    const check = this.checkbox(
      `${key}-model-${index}-${model.id || 'missing'}`,
      importable && selection.has(model.id)
    );
    check.disabled = !importable;
    check.addEventListener('change', () => {
      if (check.checked) selection.add(model.id);
      else selection.delete(model.id);
      this.refresh();
    });
    return h(
      'div',
      { className: `aq-tools-catalog-row${importable ? '' : ' aq-tools-catalog-row-invalid'}` },
      h(
        'label',
        { className: 'aq-tools-catalog-check', for: check.id },
        check,
        h(
          'span',
          { className: 'aq-tools-catalog-copy' },
          h('strong', undefined, safeText(model.label, model.id || 'Unnamed model')),
          h('code', { className: 'aq-tools-code' }, model.id || 'missing-id'),
          h(
            'span',
            { className: 'aq-tools-hint' },
            `Model arg: ${safeText(model.model_arg, 'missing')} · ${effortText(model)}`
          ),
          h('span', { className: 'aq-tools-hint' }, 'Effort entries are catalog-reported and unverified.')
        )
      ),
      importable ? null : h('span', { className: 'aq-tools-inline-error' }, 'Missing required catalog metadata.')
    );
  }

  private adapterSelect(key: string, value: Adapter, onChange: (value: Adapter) => void): HTMLSelectElement {
    const select = this.select(
      key,
      ADAPTERS.map((adapter) => ({ value: adapter, label: ADAPTER_LABELS[adapter] })),
      value
    );
    select.addEventListener('change', () => onChange(select.value as Adapter));
    return select;
  }

  private toolSelect(key: string, value: string, onChange: (value: string) => void): HTMLSelectElement {
    const tools = this.snapshot?.tools ?? [];
    const options = [{ value: '', label: 'Select a tool…' }];
    for (const tool of tools) {
      options.push({
        value: tool.id,
        label: `${tool.label} (${tool.id})${tool.enabled ? '' : ' · disabled'}`,
      });
    }
    if (value && !tools.some((tool) => tool.id === value)) {
      options.push({ value, label: `Missing tool: ${value}` });
    }
    const select = this.select(key, options, value);
    select.addEventListener('change', () => onChange(select.value));
    return select;
  }

  private input(
    key: string,
    value: string,
    placeholder: string,
    options: { type?: string; autocomplete?: string } = {}
  ): HTMLInputElement {
    const input = h('input', {
      id: this.id(key),
      className: 'aq-text-input',
      type: options.type ?? 'text',
      value,
      placeholder,
      autocomplete: options.autocomplete ?? 'off',
      spellcheck: 'false',
    });
    input.value = value;
    input.dataset.focusKey = token(key);
    return input;
  }

  private select(
    key: string,
    options: readonly { value: string; label: string }[],
    value: string
  ): HTMLSelectElement {
    const select = h('select', { id: this.id(key), className: 'aq-text-input aq-tools-select' });
    for (const option of options) {
      select.appendChild(h('option', { value: option.value }, option.label));
    }
    select.value = value;
    select.dataset.focusKey = token(key);
    return select;
  }

  private checkbox(key: string, checked: boolean): HTMLInputElement {
    const input = h('input', { id: this.id(key), type: 'checkbox' });
    input.checked = checked;
    input.dataset.focusKey = token(key);
    return input;
  }

  private field(label: string, control: HTMLInputElement | HTMLSelectElement, hint?: string): HTMLElement {
    const hintId = hint ? `${control.id}-hint` : undefined;
    if (hintId) control.setAttribute('aria-describedby', hintId);
    return h(
      'div',
      { className: 'aq-tools-field' },
      h('label', { className: 'aq-label', for: control.id }, label),
      control,
      hint ? h('p', { className: 'aq-tools-hint', id: hintId }, hint) : null
    );
  }

  private checkField(label: string, input: HTMLInputElement, hint?: string): HTMLElement {
    const hintId = hint ? `${input.id}-hint` : undefined;
    if (hintId) input.setAttribute('aria-describedby', hintId);
    return h(
      'div',
      { className: 'aq-tools-field aq-tools-check-field' },
      h('label', { className: 'aq-tools-check', for: input.id }, input, h('span', undefined, label)),
      hint ? h('p', { className: 'aq-tools-hint', id: hintId }, hint) : null
    );
  }

  private statusRow(label: string, value: string, tone?: Tone): HTMLElement {
    return h(
      'div',
      { className: 'aq-tools-status-row' },
      h('span', { className: 'aq-tools-status-label' }, label),
      h('span', { className: 'aq-tools-status-value', 'data-tone': tone ?? null }, value)
    );
  }

  private fact(label: string, value: string): HTMLElement {
    return h('div', undefined, h('dt', undefined, label), h('dd', undefined, safeText(value, 'Not reported')));
  }

  private errorBlock(error: SafeError): HTMLElement {
    return h(
      'div',
      { className: 'aq-tools-error', role: 'alert' },
      error.code ? h('code', { className: 'aq-tools-code' }, error.code) : null,
      h('span', undefined, error.message)
    );
  }

  private button(
    text: string,
    onClick: () => void,
    focusKey: string,
    variant?: 'primary' | 'danger',
    disabled?: boolean
  ): HTMLButtonElement {
    const button = createButton(text, onClick, disabled);
    button.type = 'button';
    button.classList.add('aq-btn');
    if (variant) button.classList.add(`aq-btn-${variant}`);
    button.dataset.focusKey = token(focusKey);
    return button;
  }

  private importable(model: ModelDefinition): boolean {
    return Boolean(
      model.id?.trim() &&
        model.label?.trim() &&
        model.model_arg?.trim() &&
        Array.isArray(model.efforts)
    );
  }

  private selection(key: string, models: readonly ModelDefinition[]): Set<string> {
    let selection = this.selections.get(key);
    if (!selection) {
      selection = new Set<string>();
      this.selections.set(key, selection);
    }
    const valid = new Set(
      models.filter((model) => this.importable(model)).map((model) => model.id)
    );
    for (const id of Array.from(selection)) {
      if (!valid.has(id)) selection.delete(id);
    }
    return selection;
  }

  private toolEdit(tool: ToolDefinition): ToolDraft {
    const revision = this.snapshot?.revision ?? 0;
    const existing = this.toolDrafts.get(tool.id);
    if (!existing) {
      const created = toolDraft(tool, revision);
      this.toolDrafts.set(tool.id, created);
      return created;
    }
    if (!existing.dirty && !existing.conflict) {
      Object.assign(existing, toolDraft(tool, revision));
      return existing;
    }
    if (existing.dirty && existing.baseRevision !== revision && !existing.conflict) {
      existing.conflict = true;
      existing.overwrite = false;
    }
    return existing;
  }

  private modelEdit(model: ModelDefinition): ModelDraft {
    const revision = this.snapshot?.revision ?? 0;
    const existing = this.modelDrafts.get(model.id);
    if (!existing) {
      const created = modelDraft(model, revision);
      this.modelDrafts.set(model.id, created);
      return created;
    }
    if (!existing.dirty && !existing.conflict) {
      Object.assign(existing, modelDraft(model, revision));
      return existing;
    }
    if (existing.dirty && existing.baseRevision !== revision && !existing.conflict) {
      existing.conflict = true;
      existing.overwrite = false;
    }
    return existing;
  }

  private probeState(toolId: string): ProbeState {
    let state = this.probes.get(toolId);
    if (!state) {
      state = { loading: false, result: null, error: null };
      this.probes.set(toolId, state);
    }
    return state;
  }

  private secretState(toolId: string): SecretState {
    let state = this.secrets.get(toolId);
    if (!state) {
      state = { loading: false, status: null, error: null, confirmDelete: false };
      this.secrets.set(toolId, state);
    }
    return state;
  }

  private apiState(toolId: string): ApiState {
    let state = this.apiStates.get(toolId);
    if (!state) {
      state = {
        loading: false,
        loaded: false,
        models: [],
        error: null,
        testModel: '',
        testLoading: false,
        advice: null,
        testError: null,
      };
      this.apiStates.set(toolId, state);
    }
    return state;
  }

  private validateTool(draft: ToolDraft, isNew: boolean): ToolDefinition | string {
    const id = draft.id.trim();
    const label = draft.label.trim();
    if (!id) return 'Enter a tool ID.';
    if (!label) return 'Enter a tool label.';
    if (isNew && this.snapshot?.tools.some((tool) => tool.id === id)) {
      return 'A tool with this ID already exists. Edit the existing tool instead.';
    }
    return {
      id,
      label,
      adapter: draft.adapter,
      executable_path: draft.executable.trim() || null,
      enabled: draft.enabled,
    };
  }

  private validateModel(draft: ModelDraft, isNew: boolean): ModelDefinition | string {
    const id = draft.id.trim();
    const label = draft.label.trim();
    const modelArg = draft.modelArg.trim();
    if (!id) return 'Enter a model ID.';
    if (!label) return 'Enter a model label.';
    if (!modelArg) return 'Enter the exact provider model argument.';
    if (isNew && this.snapshot?.models.some((model) => model.id === id)) {
      return 'A model with this ID already exists. Edit the existing model instead.';
    }
    if (!this.snapshot?.tools.some((tool) => tool.id === draft.toolId)) {
      return 'Choose a current tool before saving this model.';
    }
    return {
      id,
      tool_id: draft.toolId,
      label,
      model_arg: modelArg,
      efforts: parseEfforts(draft.efforts),
      allow_no_effort: draft.allowNoEffort,
    };
  }

  private validationError(message: string): void {
    this.notice = { tone: 'error', text: safeText(message, 'The request could not be applied.') };
    this.refresh();
  }

  private async apply(
    key: string,
    change: RegistryChange,
    success: (next: RegistrySnapshot) => void,
    failure?: (error: SafeError) => void
  ): Promise<boolean> {
    if (this.disposed || !this.snapshot || this.pending.has(key)) return false;
    const expectedRevision = this.snapshot.revision;
    this.pending.add(key);
    this.notice = null;
    this.refresh();
    let notify = false;
    try {
      const next = await desktop_registry_apply({
        expectedRevision,
        change,
      });
      if (this.disposed) return false;
      this.snapshot = next;
      success(next);
      notify = true;
    } catch (err) {
      if (this.disposed) return false;
      const error = safeError(err);
      this.notice = { tone: 'error', text: error.message, code: error.code };
      failure?.(error);
    } finally {
      this.pending.delete(key);
      if (!this.disposed) {
        this.refresh();
        if (notify) this.notifyOuter();
      }
    }
    return notify;
  }

  private async createTool(): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const draft = this.newTool;
    const tool = this.validateTool(draft, true);
    if (typeof tool === 'string') {
      this.validationError(tool);
      return;
    }
    await this.apply('tool-create', { op: 'upsert_tool', tool }, (next) => {
      this.newTool = emptyTool(next.revision);
      this.notice = { tone: 'success', text: `Added tool "${tool.label}" at revision ${next.revision}.` };
    });
  }

  private async saveTool(tool: ToolDefinition): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const draft = this.toolEdit(tool);
    if (draft.conflict && !draft.overwrite) {
      this.validationError('Reload the latest registry or explicitly allow overwrite first.');
      return;
    }
    const value = this.validateTool(draft, false);
    if (typeof value === 'string') {
      this.validationError(value);
      return;
    }
    await this.apply(
      `tool-save:${tool.id}`,
      { op: 'upsert_tool', tool: value },
      (next) => {
        this.toolDrafts.delete(tool.id);
        this.probes.delete(tool.id);
        this.selections.delete(`probe:${tool.id}`);
        if (tool.adapter !== draft.adapter) {
          this.secrets.delete(tool.id);
          this.apiStates.delete(tool.id);
          this.selections.delete(`api:${tool.id}`);
        }
        this.notice = { tone: 'success', text: `Saved tool "${value.label}" at revision ${next.revision}.` };
      },
      (error) => {
        if (isConflict(error)) {
          draft.conflict = true;
          draft.overwrite = false;
        }
      }
    );
  }

  private async deleteTool(tool: ToolDefinition): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    if (this.snapshot.models.some((model) => model.tool_id === tool.id)) {
      this.validationError('Delete this tool’s models before deleting the tool.');
      return;
    }
    const deleted = await this.apply(
      `tool-delete:${tool.id}`,
      { op: 'delete_tool', tool_id: tool.id },
      (next) => {
        this.deleteConfirm = null;
        this.toolDrafts.delete(tool.id);
        this.probes.delete(tool.id);
        this.secrets.delete(tool.id);
        this.apiStates.delete(tool.id);
        this.selections.delete(`probe:${tool.id}`);
        this.selections.delete(`api:${tool.id}`);
        this.notice = { tone: 'success', text: `Deleted tool "${tool.label}" at revision ${next.revision}.` };
      }
    );
    if (!deleted) this.deleteConfirm = null;
  }

  private async probe(tool: ToolDefinition): Promise<void> {
    if (this.disposed || this.pending.has(`probe:${tool.id}`)) return;
    const state = this.probeState(tool.id);
    state.loading = true;
    state.error = null;
    state.result = null;
    this.selections.delete(`probe:${tool.id}`);
    this.pending.add(`probe:${tool.id}`);
    this.refresh();
    try {
      const result = await desktop_probe_tool({ toolId: tool.id });
      if (!this.disposed) {
        state.result = result;
        this.selections.set(`probe:${tool.id}`, new Set());
      }
    } catch (err) {
      if (!this.disposed) state.error = safeError(err);
    } finally {
      if (!this.disposed) {
        state.loading = false;
        this.pending.delete(`probe:${tool.id}`);
        this.refresh();
      }
    }
  }

  private async importModels(
    tool: ToolDefinition,
    key: string,
    source: readonly ModelDefinition[]
  ): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const selection = this.selection(key, source);
    const selected = source.filter((model) => this.importable(model) && selection.has(model.id));
    if (selected.length === 0) {
      this.validationError('Select at least one complete catalog model to import.');
      return;
    }
    const models: ModelDefinition[] = selected.map((model) => ({
      id: model.id.trim(),
      tool_id: tool.id,
      label: model.label.trim(),
      model_arg: model.model_arg.trim(),
      efforts: [...model.efforts],
      allow_no_effort: Boolean(model.allow_no_effort),
    }));
    await this.apply(`import:${key}`, { op: 'upsert_models', models }, (next) => {
      this.selections.delete(key);
      this.notice = {
        tone: 'success',
        text: `Imported ${models.length} catalog model${models.length === 1 ? '' : 's'} at revision ${next.revision}. Effort entries remain unverified.`,
      };
    });
  }

  private async createModel(): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const draft = this.newModel;
    const model = this.validateModel(draft, true);
    if (typeof model === 'string') {
      this.validationError(model);
      return;
    }
    const preferredTool = draft.toolId;
    await this.apply('model-create', { op: 'upsert_model', model }, (next) => {
      const toolId = next.tools.some((tool) => tool.id === preferredTool)
        ? preferredTool
        : next.tools[0]?.id ?? '';
      this.newModel = emptyModel(next.revision, toolId);
      this.notice = { tone: 'success', text: `Added model "${model.label}" at revision ${next.revision}.` };
    });
  }

  private async saveModel(model: ModelDefinition): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const draft = this.modelEdit(model);
    if (draft.conflict && !draft.overwrite) {
      this.validationError('Reload the latest registry or explicitly allow overwrite first.');
      return;
    }
    const value = this.validateModel(draft, false);
    if (typeof value === 'string') {
      this.validationError(value);
      return;
    }
    await this.apply(
      `model-save:${model.id}`,
      { op: 'upsert_model', model: value },
      (next) => {
        this.modelDrafts.delete(model.id);
        this.notice = { tone: 'success', text: `Saved model "${value.label}" at revision ${next.revision}.` };
      },
      (error) => {
        if (isConflict(error)) {
          draft.conflict = true;
          draft.overwrite = false;
        }
      }
    );
  }

  private async deleteModel(model: ModelDefinition): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    if (hasProfileReference(this.snapshot, model.id)) {
      this.validationError('Remove workflow profile assignments before deleting this model.');
      return;
    }
    const deleted = await this.apply(
      `model-delete:${model.id}`,
      { op: 'delete_model', model_id: model.id },
      (next) => {
        this.deleteConfirm = null;
        this.modelDrafts.delete(model.id);
        this.notice = { tone: 'success', text: `Deleted model "${model.label}" at revision ${next.revision}.` };
      }
    );
    if (!deleted) this.deleteConfirm = null;
  }

  private async checkSecret(toolId: string): Promise<void> {
    if (
      this.disposed ||
      this.pending.has(`secret-status:${toolId}`) ||
      this.pending.has(`secret-set:${toolId}`) ||
      this.pending.has(`secret-delete:${toolId}`)
    ) {
      return;
    }
    const state = this.secretState(toolId);
    state.loading = true;
    state.error = null;
    state.status = null;
    this.pending.add(`secret-status:${toolId}`);
    this.refresh();
    try {
      const status = await desktop_secret_status({ toolId });
      if (!this.disposed) state.status = safeText(status, 'Unknown');
    } catch (err) {
      if (!this.disposed) state.error = safeError(err);
    } finally {
      if (!this.disposed) {
        state.loading = false;
        this.pending.delete(`secret-status:${toolId}`);
        this.refresh();
      }
    }
  }

  private async saveSecret(tool: ToolDefinition, input: HTMLInputElement): Promise<void> {
    if (
      this.disposed ||
      this.pending.has(`secret-status:${tool.id}`) ||
      this.pending.has(`secret-set:${tool.id}`) ||
      this.pending.has(`secret-delete:${tool.id}`)
    ) {
      return;
    }
    this.notice = null;
    let secret = input.value;
    input.value = '';
    const state = this.secretState(tool.id);
    state.error = null;
    state.status = null;
    if (!secret.trim()) {
      state.error = { code: null, message: 'Enter a credential before saving.' };
      this.refresh();
      return;
    }
    const key = `secret-set:${tool.id}`;
    state.loading = true;
    this.pending.add(key);
    this.refresh();
    try {
      await desktop_secret_set({ toolId: tool.id, secret });
      if (this.disposed) return;
      this.notice = {
        tone: 'success',
        title: 'Credential vault',
        text: 'Credential submitted to the Rust OS vault. Credential status is separate from CLI login and entitlement.',
      };
      state.status = null;
      try {
        const status = await desktop_secret_status({ toolId: tool.id });
        if (!this.disposed) {
          state.status = safeText(status, 'Unknown');
          state.error = null;
        }
      } catch (err) {
        if (!this.disposed) state.error = safeError(err);
      }
    } catch (err) {
      if (!this.disposed) state.error = safeError(err);
    } finally {
      secret = '';
      if (!this.disposed) {
        state.loading = false;
        this.pending.delete(key);
        this.refresh();
      }
    }
  }

  private async deleteSecret(tool: ToolDefinition): Promise<void> {
    if (
      this.disposed ||
      this.pending.has(`secret-delete:${tool.id}`) ||
      this.pending.has(`secret-set:${tool.id}`) ||
      this.pending.has(`secret-status:${tool.id}`)
    ) {
      return;
    }
    this.notice = null;
    const state = this.secretState(tool.id);
    state.confirmDelete = false;
    state.error = null;
    state.status = null;
    state.loading = true;
    this.pending.add(`secret-delete:${tool.id}`);
    this.refresh();
    try {
      await desktop_secret_delete({ toolId: tool.id });
      if (this.disposed) return;
      this.notice = {
        tone: 'success',
        title: 'Credential vault',
        text: 'Credential removal submitted to the Rust OS vault.',
      };
      state.status = null;
      try {
        const status = await desktop_secret_status({ toolId: tool.id });
        if (!this.disposed) {
          state.status = safeText(status, 'Unknown');
          state.error = null;
        }
      } catch (err) {
        if (!this.disposed) state.error = safeError(err);
      }
    } catch (err) {
      if (!this.disposed) state.error = safeError(err);
    } finally {
      if (!this.disposed) {
        state.loading = false;
        this.pending.delete(`secret-delete:${tool.id}`);
        this.refresh();
      }
    }
  }

  private async loadModels(tool: ToolDefinition, provider: Provider): Promise<void> {
    const key = `catalog:api:${tool.id}`;
    if (this.disposed || this.pending.has(key)) return;
    const state = this.apiState(tool.id);
    state.loading = true;
    state.error = null;
    state.loaded = false;
    state.models = [];
    this.selections.delete(`api:${tool.id}`);
    this.pending.add(key);
    this.refresh();
    try {
      const models = await desktop_decision_models({
        toolId: tool.id,
        provider,
      });
      if (!this.disposed) {
        state.models = models;
        state.loaded = true;
        this.selections.set(`api:${tool.id}`, new Set());
      }
    } catch (err) {
      if (!this.disposed) state.error = safeError(err);
    } finally {
      if (!this.disposed) {
        state.loading = false;
        this.pending.delete(key);
        this.refresh();
      }
    }
  }

  private async testDecision(tool: ToolDefinition, provider: Provider): Promise<void> {
    const key = `decision-test:${tool.id}`;
    if (this.disposed || this.pending.has(key)) return;
    const state = this.apiState(tool.id);
    const model = state.testModel.trim();
    state.testError = null;
    state.advice = null;
    if (!model) {
      state.testError = {
        code: null,
        message: 'Choose or enter an exact provider model ID before running the synthetic test.',
      };
      this.refresh();
      return;
    }
    state.testLoading = true;
    this.pending.add(key);
    this.refresh();
    try {
      const advice = await desktop_test_decision({
        toolId: tool.id,
        provider,
        model,
      });
      if (!this.disposed) {
        state.advice = advice;
        state.testError = null;
      }
    } catch (err) {
      if (!this.disposed) state.testError = safeError(err);
    } finally {
      if (!this.disposed) {
        state.testLoading = false;
        this.pending.delete(key);
        this.refresh();
      }
    }
  }
}
