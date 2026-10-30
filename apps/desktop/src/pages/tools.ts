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
import { planDetectedEffortUpdates } from './effort-setup.ts';
import { catalogProvider, catalogProviderOptions, matchesCatalogProvider, type CatalogProvider } from './catalog-providers.ts';

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

export interface ToolsDiscoveryItem {
  id: string;
  label: string;
  availability: string;
  executable_path: string | null;
}

export interface ToolsRenderContext {
  tools?: readonly ToolsDiscoveryItem[];
  loading?: boolean;
  error?: string | null;
  onRefreshDiscovery?: () => void;
  onOpenProfiles?: () => void;
}

export interface ToolsPageOptions {
  onOpenProfiles?: () => void;
  onRefreshDiscovery?: () => void;
}

interface Notice {
  tone: 'success' | 'error' | 'info';
  title?: string;
  text: string;
  code?: string | null;
  showOpenProfiles?: boolean;
}

interface PendingToolSwitch {
  fromKind: 'create-tool' | 'edit-tool';
  fromToolId: string | null;
  fromLabel: string;
  toLabel: string;
  returnFocusKey: string;
  proceed: () => void;
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

interface DiscoveryCliSetup {
  adapter: Adapter;
  label: string;
  idBase: string;
}

function discoveryCliSetup(toolId: string, label: string): DiscoveryCliSetup | null {
  const identity = `${toolId} ${label}`.toLowerCase();
  if (identity.includes('opencode')) {
    return {
      adapter: 'opencode_v2',
      label: ADAPTER_LABELS.opencode_v2,
      idBase: 'opencode-v2',
    };
  }
  if (identity.includes('antigravity') || /\bagy\b/.test(identity)) {
    return {
      adapter: 'antigravity',
      label: ADAPTER_LABELS.antigravity,
      idBase: 'antigravity',
    };
  }
  if (identity.includes('codex')) {
    return {
      adapter: 'codex_cli',
      label: ADAPTER_LABELS.codex_cli,
      idBase: 'codex-cli',
    };
  }
  return null;
}

export function isConfigurableDiscoveredCli(toolId: string, label: string): boolean {
  return discoveryCliSetup(toolId, label) !== null;
}

function uniqueToolId(baseId: string, tools: readonly ToolDefinition[]): string {
  const used = new Set(tools.map((tool) => tool.id));
  if (!used.has(baseId)) return baseId;
  let suffix = 2;
  while (used.has(`${baseId}-${suffix}`)) {
    suffix += 1;
  }
  return `${baseId}-${suffix}`;
}

function hasNewToolContent(draft: ToolDraft): boolean {
  return Boolean(
    draft.id.trim() ||
      draft.label.trim() ||
      draft.executable.trim() ||
      draft.adapter !== 'codex_cli' ||
      !draft.enabled
  );
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
  direction: 'forward' | 'backward' | 'none' | null;
}

function findByFocusKey(root: HTMLElement, key: string): HTMLElement | null {
  for (const element of root.querySelectorAll<HTMLElement>('[data-focus-key]')) {
    if (element.dataset.focusKey === key) {
      return element;
    }
  }
  return null;
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
  let start: number | null = null;
  let end: number | null = null;
  let direction: 'forward' | 'backward' | 'none' | null = null;
  if (active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement) {
    try {
      start = typeof active.selectionStart === 'number' ? active.selectionStart : null;
      end = typeof active.selectionEnd === 'number' ? active.selectionEnd : null;
      direction = active.selectionDirection;
    } catch {
      start = null;
      end = null;
      direction = null;
    }
  }
  return { key, start, end, direction };
}

function restoreFocus(root: HTMLElement, state: FocusState | null, disposed: () => boolean): void {
  if (!state) return;
  const restore = (allowIfAnotherFocused: boolean): void => {
    if (disposed() || !root.isConnected) return;
    if (!allowIfAnotherFocused && typeof document !== 'undefined') {
      const current = document.activeElement;
      if (
        current instanceof HTMLElement &&
        root.contains(current) &&
        current.dataset.focusKey &&
        current.dataset.focusKey !== state.key
      ) {
        return;
      }
    }
    const target = findByFocusKey(root, state.key);
    if (!target || (target as { disabled?: boolean }).disabled) return;
    target.focus({ preventScroll: true });
    if (
      (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) &&
      state.start !== null &&
      state.end !== null
    ) {
      try {
        if (state.direction) {
          target.setSelectionRange(state.start, state.end, state.direction);
        } else {
          target.setSelectionRange(state.start, state.end);
        }
      } catch {
        // Non-text controls do not expose a selection range.
      }
    }
  };
  restore(true);
  queueMicrotask(() => restore(false));
}

function findRegisteredToolForDiscovery(
  item: ToolsDiscoveryItem,
  tools: readonly ToolDefinition[]
): ToolDefinition | null {
  if (item.availability !== 'found') {
    return null;
  }
  const setup = discoveryCliSetup(item.id, item.label);
  const path = item.executable_path?.trim() ?? '';
  if (!setup || path.length === 0) {
    return null;
  }
  return (
    tools.find(
      (tool) =>
        tool.adapter === setup.adapter &&
        (tool.executable_path ?? '').trim() === path
    ) ?? null
  );
}

function deriveToolIdFromLabel(label: string, adapter: Adapter): string {
  const slug = label
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 32);
  if (slug) return slug;
  return adapter.replace(/_/g, '-');
}

export class ToolsPage {
  private readonly onRegistryChanged: () => void;
  private onOpenProfiles: (() => void) | null = null;
  private onRefreshDiscovery: (() => void) | null = null;
  private readonly pageId = `aq-tools-${++pageCounter}`;
  private root: HTMLElement | null = null;
  private snapshot: RegistrySnapshot | null = null;
  private renderedRevision: number | null = null;
  private renderedDiscoverySig: string | null = null;
  private discoveredTools: readonly ToolsDiscoveryItem[] = [];
  private discoveryLoading = false;
  private discoveryError: string | null = null;
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
  private catalogSearch = new Map<string, string>();
  private catalogProviders = new Map<string, CatalogProvider>();
  private openPickers = new Set<string>();
  private expanded = new Set<string>();
  private configuringDiscoveredId: string | null = null;
  private pendingSwitch: PendingToolSwitch | null = null;
  private deleteConfirm: { kind: 'tool' | 'model'; id: string } | null = null;

  constructor(
    onRegistryChanged: () => void,
    options?: ToolsPageOptions | (() => void)
  ) {
    this.onRegistryChanged = onRegistryChanged;
    if (typeof options === 'function') {
      this.onOpenProfiles = options;
    } else if (options) {
      this.onOpenProfiles = options.onOpenProfiles ?? null;
      this.onRefreshDiscovery = options.onRefreshDiscovery ?? null;
    }
  }

  render(snapshot: RegistrySnapshot, context?: ToolsRenderContext): HTMLElement {
    if (this.disposed) {
      return h('section', { className: 'aq-tools-page', 'aria-label': 'Tools and models' });
    }
    if (context) {
      if (context.tools !== undefined) this.discoveredTools = context.tools;
      if (context.loading !== undefined) this.discoveryLoading = context.loading;
      if (context.error !== undefined) this.discoveryError = context.error;
      if (context.onRefreshDiscovery !== undefined) {
        this.onRefreshDiscovery = context.onRefreshDiscovery;
      }
      if (context.onOpenProfiles !== undefined) {
        this.onOpenProfiles = context.onOpenProfiles;
      }
    }
    const discoverySig = JSON.stringify({
      loading: this.discoveryLoading,
      error: this.discoveryError,
      tools: this.discoveredTools.map((item) => ({
        id: item.id,
        label: item.label,
        availability: item.availability,
        executable_path: item.executable_path,
      })),
    });
    const needsRender =
      this.root === null ||
      this.renderedRevision !== snapshot.revision ||
      this.renderedDiscoverySig !== discoverySig;
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
    this.renderedDiscoverySig = discoverySig;
    if (needsRender) this.rewrite();
    return root;
  }

  configureDiscoveredTool(toolId: string, label: string, executablePath: string): void {
    if (this.disposed || !this.snapshot) return;
    const setup = discoveryCliSetup(toolId, label);
    const path = executablePath.trim();
    if (!setup || !path) {
      this.notice = {
        tone: 'info',
        title: 'Custom tool setup',
        text: 'This command does not map to a preset CLI adapter. Opened the custom tool form below.',
      };
      this.openCustomToolForm();
      return;
    }

    const existing = findRegisteredToolForDiscovery(
      { id: toolId, label, availability: 'found', executable_path: path },
      this.snapshot.tools
    );
    if (existing) {
      this.requestToolSwitch(existing.label, `edit:${existing.id}`, () => {
        this.expanded.add(`tool:${existing.id}`);
        this.notice = {
          tone: 'info',
          title: 'Already added',
          text: `${existing.label} is already in your registry. Opened its settings; nothing was changed or probed.`,
        };
        this.rewrite();
        this.focusAndScroll(`tool-${existing.id}-label`);
      });
      return;
    }

    this.requestToolSwitch(setup.label, `configure:${toolId}`, () => {
      if (!this.snapshot) return;
      this.configuringDiscoveredId = toolId;
      this.newTool = {
        id: uniqueToolId(setup.idBase, this.snapshot.tools),
        label: setup.label,
        adapter: setup.adapter,
        executable: path,
        enabled: true,
        baseRevision: this.snapshot.revision,
        dirty: true,
        conflict: false,
        overwrite: false,
      };
      this.expanded.add('create-tool');
      this.notice = {
        tone: 'info',
        title: 'Customize before adding',
        text: `Prefilled ${setup.label} from local PATH discovery. Review or adjust settings below, then click Save and choose models or Cancel.`,
      };
      this.rewrite();
      this.focusAndScroll('new-tool-label');
    });
  }

  addAndChooseModels(toolId: string, label: string, executablePath: string): void {
    if (this.disposed || !this.snapshot) return;
    const setup = discoveryCliSetup(toolId, label);
    const path = executablePath.trim();
    if (!setup || !path) {
      this.configureDiscoveredTool(toolId, label, executablePath);
      return;
    }

    this.requestToolSwitch(setup.label, `add-choose:${toolId}`, () => {
      void this.performAddAndChooseModels(toolId, label, path, setup);
    });
  }

  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.clearSecretInputs();
    this.pending.clear();
    this.toolDrafts.clear();
    this.modelDrafts.clear();
    this.probes.clear();
    this.secrets.clear();
    this.apiStates.clear();
    this.selections.clear();
    this.catalogSearch.clear();
    this.catalogProviders.clear();
    this.openPickers.clear();
    this.expanded.clear();
    this.configuringDiscoveredId = null;
    this.pendingSwitch = null;
    this.deleteConfirm = null;
    this.snapshot = null;
    this.renderedRevision = null;
    this.renderedDiscoverySig = null;
    this.notice = null;
    this.root?.replaceChildren();
    this.root = null;
  }

  private async performAddAndChooseModels(
    toolId: string,
    label: string,
    path: string,
    setup: DiscoveryCliSetup
  ): Promise<void> {
    if (this.disposed || !this.snapshot) return;
    const existing = findRegisteredToolForDiscovery(
      { id: toolId, label, availability: 'found', executable_path: path },
      this.snapshot.tools
    );
    if (existing) {
      this.openPickers.add(existing.id);
      this.rewrite();
      await this.probe(existing);
      if (!this.disposed && this.openPickers.has(existing.id)) {
        this.focusAndScroll(`probe:${existing.id}-search`);
      }
      return;
    }

    const tool: ToolDefinition = {
      id: uniqueToolId(setup.idBase, this.snapshot.tools),
      label: setup.label,
      adapter: setup.adapter,
      executable_path: path,
      enabled: true,
    };
    const key = `add-choose:${toolId}`;
    const hadCreateOpenAtStart = this.expanded.has('create-tool');
    const createDraftAtStart = this.newTool;
    let openModelsAfter = false;
    const saved = await this.apply(key, { op: 'upsert_tool', tool }, (next) => {
      if (
        this.configuringDiscoveredId === toolId &&
        (!this.expanded.has('create-tool') ||
          (this.newTool === createDraftAtStart && !hasNewToolContent(this.newTool)))
      ) {
        this.configuringDiscoveredId = null;
        this.expanded.delete('create-tool');
        this.newTool = emptyTool(next.revision);
      } else if (this.configuringDiscoveredId === toolId) {
        this.configuringDiscoveredId = null;
      }
      const userOpenedFormWhileSaving =
        (!hadCreateOpenAtStart && this.expanded.has('create-tool')) ||
        this.getActiveDirtyToolEditor() !== null;
      if (!userOpenedFormWhileSaving) {
        this.openPickers.add(tool.id);
        openModelsAfter = true;
      }
      this.notice = {
        tone: 'info',
        title: `${tool.label} added`,
        text: openModelsAfter
          ? `Saved ${tool.label} and reading its model catalog. Select the models you want to use in Profiles.`
          : `Saved ${tool.label} at revision ${next.revision}. Click Choose models on its card when you are ready to add models.`,
      };
    });
    if (saved && openModelsAfter && !this.disposed && this.openPickers.has(tool.id)) {
      await this.probe(tool);
      if (!this.disposed && this.openPickers.has(tool.id)) {
        this.focusAndScroll(`probe:${tool.id}-search`);
      }
    }
  }

  private getActiveDirtyToolEditor(): {
    kind: 'create-tool' | 'edit-tool';
    toolId: string | null;
    label: string;
    focusKey: string;
  } | null {
    if (this.expanded.has('create-tool') && this.newTool.dirty && hasNewToolContent(this.newTool)) {
      return {
        kind: 'create-tool',
        toolId: this.configuringDiscoveredId,
        label: this.newTool.label.trim() || 'new tool draft',
        focusKey: 'new-tool-label',
      };
    }
    if (this.snapshot) {
      for (const tool of this.snapshot.tools) {
        if (!this.expanded.has(`tool:${tool.id}`)) continue;
        const draft = this.toolDrafts.get(tool.id);
        if (draft && (draft.dirty || draft.conflict) && !sameTool(tool, draft)) {
          return {
            kind: 'edit-tool',
            toolId: tool.id,
            label: tool.label,
            focusKey: `tool-${tool.id}-label`,
          };
        }
      }
    }
    return null;
  }

  private requestToolSwitch(
    toLabel: string,
    targetKey: string,
    action: () => void
  ): void {
    const activeDirty = this.getActiveDirtyToolEditor();
    if (!activeDirty) {
      this.pendingSwitch = null;
      this.closeCleanToolFormsExcept(targetKey);
      action();
      return;
    }
    const isSameCreateTarget =
      activeDirty.kind === 'create-tool' &&
      ((activeDirty.toolId !== null && targetKey === `configure:${activeDirty.toolId}`) ||
        (activeDirty.toolId === null && targetKey === 'create-custom'));
    const isSameEditTarget =
      activeDirty.kind === 'edit-tool' &&
      activeDirty.toolId !== null &&
      targetKey === `edit:${activeDirty.toolId}`;
    if (isSameCreateTarget || isSameEditTarget) {
      this.pendingSwitch = null;
      action();
      return;
    }

    this.pendingSwitch = {
      fromKind: activeDirty.kind,
      fromToolId: activeDirty.toolId,
      fromLabel: activeDirty.label,
      toLabel,
      returnFocusKey: activeDirty.focusKey,
      proceed: () => {
        if (activeDirty.kind === 'create-tool') {
          this.discardCreateToolDraft();
        } else if (activeDirty.toolId) {
          this.discardToolEditDraft(activeDirty.toolId);
        }
        this.pendingSwitch = null;
        this.closeCleanToolFormsExcept(targetKey);
        action();
      },
    };
    this.rewrite();
    this.focusAndScroll('switch-keep-editing');
  }

  private closeCleanToolFormsExcept(targetKey: string): void {
    if (
      this.expanded.has('create-tool') &&
      (!this.newTool.dirty || !hasNewToolContent(this.newTool)) &&
      !targetKey.startsWith('configure:') &&
      targetKey !== 'create-custom'
    ) {
      this.expanded.delete('create-tool');
      this.configuringDiscoveredId = null;
      if (this.snapshot) {
        this.newTool = emptyTool(this.snapshot.revision);
      }
    }
    if (this.snapshot) {
      for (const tool of this.snapshot.tools) {
        if (targetKey === `edit:${tool.id}`) continue;
        const cardKey = `tool:${tool.id}`;
        if (!this.expanded.has(cardKey)) continue;
        const draft = this.toolDrafts.get(tool.id);
        if (!draft || (!draft.dirty && !draft.conflict) || sameTool(tool, draft)) {
          this.expanded.delete(cardKey);
          this.toolDrafts.delete(tool.id);
        }
      }
    }
  }

  private discardCreateToolDraft(): void {
    this.expanded.delete('create-tool');
    this.configuringDiscoveredId = null;
    this.pendingSwitch = null;
    this.newTool = emptyTool(this.snapshot?.revision ?? -1);
  }

  private discardToolEditDraft(toolId: string): void {
    this.clearSecretInputs(toolId);
    this.toolDrafts.delete(toolId);
    this.expanded.delete(`tool:${toolId}`);
    if (this.deleteConfirm?.kind === 'tool' && this.deleteConfirm.id === toolId) {
      this.deleteConfirm = null;
    }
    const secret = this.secrets.get(toolId);
    if (secret) secret.confirmDelete = false;
    this.pendingSwitch = null;
  }

  private closeModelPicker(toolId: string, clearSelection: boolean): void {
    this.openPickers.delete(toolId);
    const probeKey = `probe:${toolId}`;
    const apiKey = `api:${toolId}`;
    if (clearSelection) {
      this.selections.delete(probeKey);
      this.selections.delete(apiKey);
    }
    this.catalogProviders.delete(probeKey);
    this.catalogProviders.delete(apiKey);
    this.catalogSearch.delete(probeKey);
    this.catalogSearch.delete(apiKey);
    const probe = this.probes.get(toolId);
    if (probe && !probe.loading) {
      probe.error = null;
    }
    const api = this.apiStates.get(toolId);
    if (api && !api.loading) {
      api.error = null;
    }
    this.refresh();
    this.focusAndScroll(`tool-${toolId}-probe`);
  }

  private openCustomToolForm(): void {
    const focusTarget = this.configuringDiscoveredId
      ? `configure-discovery-${this.configuringDiscoveredId}`
      : 'toggle-create-tool';
    if (this.expanded.has('create-tool') && this.configuringDiscoveredId === null) {
      this.discardCreateToolDraft();
      this.refresh();
      this.focusAndScroll(focusTarget);
      return;
    }
    this.requestToolSwitch('custom tool form', 'create-custom', () => {
      this.configuringDiscoveredId = null;
      this.newTool = emptyTool(this.snapshot?.revision ?? -1);
      this.expanded.add('create-tool');
      this.rewrite();
      this.focusAndScroll('new-tool-label');
    });
  }

  private clearSecretInputs(toolId?: string): void {
    if (!this.root) return;
    for (const input of this.root.querySelectorAll<HTMLInputElement>('input[type="password"]')) {
      if (!toolId || input.id.includes(token(`api-${toolId}-credential`))) {
        input.value = '';
      }
    }
  }

  private id(key: string): string {
    return `${this.pageId}-${token(key)}`;
  }

  private refresh(): void {
    if (!this.disposed) this.rewrite();
  }

  private focusAndScroll(key: string): void {
    const focusKey = token(key);
    const attempt = (): boolean => {
      if (this.disposed || !this.root || !this.root.isConnected) return false;
      const target = findByFocusKey(this.root, focusKey);
      if (!target) return false;
      target.focus({ preventScroll: true });
      target.scrollIntoView?.({ block: 'nearest' });
      return true;
    };

    queueMicrotask(() => {
      if (attempt()) return;
      setTimeout(attempt, 0);
    });
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
    if (this.pendingSwitch) nodes.push(this.buildSwitchPrompt(this.pendingSwitch));
    nodes.push(this.buildTools(snapshot));
    nodes.push(this.buildModels(snapshot));
    return nodes;
  }

  private buildHeader(snapshot: RegistrySnapshot): HTMLElement {
    const enabledTools = snapshot.tools.filter((tool) => tool.enabled);
    const enabledToolIds = new Set(enabledTools.map((tool) => tool.id));
    const readyModels = snapshot.models.filter((model) => enabledToolIds.has(model.tool_id));

    return h(
      'div',
      { className: 'aq-tools-hero' },
      h(
        'div',
        undefined,
        h('p', { className: 'aq-kicker' }, 'Guided setup & registry'),
        h('h2', { className: 'aq-page-title', id: `${this.pageId}-title` }, 'Tools and models'),
        h(
          'p',
          { className: 'aq-note' },
          'Add detected CLIs, choose models from their catalog, and assign them in Profiles. Nothing is probed or executed in the background.'
        )
      ),
      h(
        'div',
        { className: 'aq-tools-hero-actions aq-tools-progress' },
        h('span', { className: 'aq-badge' }, `Revision ${snapshot.revision}`),
        h(
          'span',
          { className: 'aq-badge', 'data-status': snapshot.tools.length > 0 ? 'active' : undefined },
          `${snapshot.tools.length} tool${snapshot.tools.length === 1 ? '' : 's'}`
        ),
        h(
          'span',
          { className: 'aq-badge', 'data-status': readyModels.length > 0 ? 'active' : 'warn' },
          readyModels.length > 0
            ? `${readyModels.length} model${readyModels.length === 1 ? '' : 's'} ready`
            : 'No models added'
        ),
        readyModels.length > 0 && this.onOpenProfiles
          ? this.button(
              'Open Profiles',
              () => this.onOpenProfiles?.(),
              'open-profiles-hero',
              'primary'
            )
          : null,
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
      h(
        'div',
        { className: 'aq-tools-actions aq-tools-form-actions' },
        notice.showOpenProfiles && this.onOpenProfiles
          ? this.button(
              'Open Profiles',
              () => this.onOpenProfiles?.(),
              'open-profiles',
              'primary'
            )
          : null,
        this.button(
          'Dismiss',
          () => {
            this.notice = null;
            this.refresh();
          },
          'dismiss-notice'
        )
      )
    );
  }

  private buildSwitchPrompt(prompt: PendingToolSwitch): HTMLElement {
    return h(
      'div',
      {
        className: 'aq-tools-switch',
        role: 'region',
        'aria-label': 'Unsaved tool changes',
      },
      h(
        'div',
        { className: 'aq-tools-notice-copy' },
        h(
          'strong',
          undefined,
          `Switch from ${prompt.fromLabel} to ${prompt.toLabel}?`
        ),
        h(
          'p',
          { className: 'aq-tools-notice-text' },
          `You have unsaved changes for ${prompt.fromLabel}. Keep editing to finish or save them, or discard those unsaved edits and switch to ${prompt.toLabel}.`
        )
      ),
      h(
        'div',
        { className: 'aq-tools-actions aq-tools-switch-actions' },
        this.button(
          `Keep editing ${prompt.fromLabel}`,
          () => {
            const focusKey = prompt.returnFocusKey;
            this.pendingSwitch = null;
            this.refresh();
            this.focusAndScroll(focusKey);
          },
          'switch-keep-editing',
          'primary'
        ),
        this.button(
          `Discard and switch to ${prompt.toLabel}`,
          () => {
            prompt.proceed();
          },
          'switch-discard-and-switch'
        ),
        this.button(
          'Cancel',
          () => {
            const focusKey = prompt.returnFocusKey;
            this.pendingSwitch = null;
            this.refresh();
            this.focusAndScroll(focusKey);
          },
          'switch-cancel'
        )
      )
    );
  }

  private buildTools(snapshot: RegistrySnapshot): HTMLElement {
    const enabled = snapshot.tools.filter((tool) => tool.enabled).length;
    const unregisteredFoundClis: Array<{
      item: ToolsDiscoveryItem;
      setup: DiscoveryCliSetup;
      path: string;
    }> = [];
    const registeredMatches = new Map<string, ToolsDiscoveryItem>();
    const diagnosticItems: ToolsDiscoveryItem[] = [];

    for (const item of this.discoveredTools) {
      const matched = findRegisteredToolForDiscovery(item, snapshot.tools);
      if (matched) {
        if (!registeredMatches.has(matched.id)) {
          registeredMatches.set(matched.id, item);
        }
        continue;
      }
      const path = item.executable_path?.trim() ?? '';
      const setup = discoveryCliSetup(item.id, item.label);
      if (item.availability === 'found' && path.length > 0 && setup !== null) {
        unregisteredFoundClis.push({ item, setup, path });
      } else {
        diagnosticItems.push(item);
      }
    }

    const createOpen = this.expanded.has('create-tool');
    const inlineConfigureTarget =
      createOpen &&
      this.configuringDiscoveredId !== null &&
      unregisteredFoundClis.some((entry) => entry.item.id === this.configuringDiscoveredId)
        ? this.configuringDiscoveredId
        : null;

    const refreshBtn = this.button(
      this.discoveryLoading ? 'Refreshing…' : 'Refresh discovery',
      () => {
        if (this.onRefreshDiscovery) {
          this.onRefreshDiscovery();
        }
      },
      'discovery-refresh',
      undefined,
      this.discoveryLoading
    );
    refreshBtn.dataset.action = 'refresh-tools';
    refreshBtn.dataset.focus = 'discovery-refresh';
    refreshBtn.setAttribute('aria-label', 'Refresh discovery');

    const customFormOpen = createOpen && inlineConfigureTarget === null;
    const customToggleBtn = this.button(
      customFormOpen ? 'Cancel custom tool' : 'Add custom or API tool',
      () => this.openCustomToolForm(),
      'toggle-create-tool'
    );
    customToggleBtn.setAttribute('aria-expanded', String(customFormOpen));

    return h(
      'section',
      {
        className: 'aq-tools-section aq-tools-setup',
        'aria-labelledby': `${this.pageId}-tools-title`,
        'aria-busy': this.discoveryLoading ? 'true' : undefined,
      },
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
            'Step 1: Add a detected CLI or custom tool. Step 2: Click Choose models and add the models you want to assign in Profiles.'
          )
        ),
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-progress' },
          h('span', { className: 'aq-badge' }, `${enabled} enabled`),
          this.discoveryLoading
            ? h(
                'span',
                { className: 'aq-badge', role: 'status', 'aria-live': 'polite' },
                'Checking PATH…'
              )
            : null,
          refreshBtn,
          customToggleBtn
        )
      ),
      this.discoveryError
        ? h(
            'div',
            { className: 'aq-tools-error', role: 'alert' },
            h('span', undefined, this.discoveryError),
            (() => {
              const retryBtn = this.button(
                'Retry discovery',
                () => this.onRefreshDiscovery?.(),
                'retry-discovery'
              );
              retryBtn.dataset.action = 'refresh-tools';
              return retryBtn;
            })()
          )
        : null,
      customFormOpen ? this.buildCreateTool(snapshot, null) : null,
      unregisteredFoundClis.length === 0 && snapshot.tools.length === 0
        ? h(
            'p',
            { className: 'aq-note aq-tools-empty' },
            this.discoveryLoading
              ? 'Checking local PATH for supported CLIs…'
              : 'No tools are registered yet. Refresh discovery to check local PATH or click Add custom or API tool.'
          )
        : h(
            'div',
            { className: 'aq-tools-list' },
            ...unregisteredFoundClis.map(({ item, setup, path }) =>
              this.buildDiscoveredChoiceCard(
                snapshot,
                item,
                setup,
                path,
                inlineConfigureTarget === item.id
              )
            ),
            ...snapshot.tools.map((tool) =>
              this.buildTool(snapshot, tool, registeredMatches.get(tool.id) ?? null)
            )
          ),
      diagnosticItems.length > 0
        ? h(
            'details',
            { className: 'aq-tools-advanced' },
            h(
              'summary',
              undefined,
              `Other local PATH checks (${diagnosticItems.length})`
            ),
            h(
              'p',
              { className: 'aq-tools-hint' },
              'Read-only PATH lookup on this device. Commands below were not found on PATH or are configured via custom/API adapters.'
            ),
            h(
              'div',
              { className: 'aq-tools-status-grid' },
              ...diagnosticItems.map((item) =>
                this.statusRow(
                  item.label,
                  item.executable_path?.trim()
                    ? `${formatAvailability(item.availability)} · ${item.executable_path.trim()}`
                    : formatAvailability(item.availability),
                  availabilityTone(item.availability)
                )
              )
            )
          )
        : null
    );
  }

  private buildDiscoveredChoiceCard(
    snapshot: RegistrySnapshot,
    item: ToolsDiscoveryItem,
    setup: DiscoveryCliSetup,
    path: string,
    isConfiguringInline: boolean
  ): HTMLElement {
    const busyKey = `add-choose:${item.id}`;
    const isAdding = this.pending.has(busyKey);
    const defaultId = uniqueToolId(setup.idBase, snapshot.tools);

    const primaryBtn = this.button(
      isAdding ? 'Adding & loading models…' : 'Add and choose models',
      () => this.addAndChooseModels(item.id, item.label, path),
      `add-choose-discovery-${item.id}`,
      'primary',
      isAdding
    );
    primaryBtn.classList.add('aq-tools-primary');
    primaryBtn.dataset.focus = `add-choose-discovery-${item.id}`;

    const configureBtn = this.button(
      isConfiguringInline ? 'Cancel' : 'Configure',
      () => {
        if (isConfiguringInline) {
          this.discardCreateToolDraft();
          this.refresh();
          this.focusAndScroll(`configure-discovery-${item.id}`);
        } else {
          this.configureDiscoveredTool(item.id, item.label, path);
        }
      },
      `configure-discovery-${item.id}`,
      undefined,
      isAdding
    );
    if (!isConfiguringInline) {
      configureBtn.dataset.action = 'configure-discovered-tool';
      configureBtn.dataset.discoveryToolId = item.id;
      configureBtn.dataset.discoveryToolLabel = item.label;
      configureBtn.dataset.discoveryExecutable = path;
    }
    configureBtn.dataset.focus = `configure-discovery-${item.id}`;

    return h(
      'article',
      {
        className: 'aq-tools-card aq-tools-choice',
        'data-discovery-card': item.id,
      },
      h(
        'div',
        { className: 'aq-tools-tool-head' },
        createBrandIconElement(brandFor(setup.adapter)),
        h(
          'div',
          { className: 'aq-tools-tool-title' },
          h('h4', undefined, setup.label),
          h(
            'span',
            { className: 'aq-tools-hint' },
            'Detected on this device · Not yet added to registry'
          )
        ),
        h(
          'div',
          { className: 'aq-tools-card-controls' },
          h(
            'div',
            { className: 'aq-tools-badges' },
            h('span', { className: 'aq-badge', 'data-status': 'found' }, 'Detected on PATH'),
            h('span', { className: 'aq-badge' }, 'Not added')
          ),
          h(
            'div',
            { className: 'aq-tools-actions aq-tools-form-actions' },
            primaryBtn,
            configureBtn
          )
        )
      ),
      h(
        'p',
        { className: 'aq-tools-hint' },
        'Add and choose models saves this detected CLI to the registry and reads its model catalog without running a task.'
      ),
      isConfiguringInline
        ? this.buildCreateTool(snapshot, item)
        : h(
            'details',
            { className: 'aq-tools-advanced' },
            h('summary', undefined, 'Advanced details'),
            h(
              'p',
              { className: 'aq-tools-hint' },
              `Executable: ${path} · Adapter: ${ADAPTER_LABELS[setup.adapter]} · Default ID: ${defaultId}`
            )
          )
    );
  }

  private buildCreateTool(
    snapshot: RegistrySnapshot,
    inlineItem: ToolsDiscoveryItem | null = null
  ): HTMLElement {
    const draft = this.newTool;
    const duplicate =
      draft.id.trim() !== '' && snapshot.tools.some((tool) => tool.id === draft.id.trim());
    const bodyId = `${this.pageId}-create-tool-body`;
    const id = this.input('new-tool-id', draft.id, 'Auto-generated if left blank');
    const label = this.input('new-tool-label', draft.label, 'Display label');
    const adapter = this.adapterSelect('new-tool-adapter', draft.adapter, (value) => {
      draft.adapter = value;
      if (!draft.label.trim()) {
        draft.label = ADAPTER_LABELS[value];
        label.value = draft.label;
      }
      draft.dirty = true;
      this.refresh();
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

    const cancelForm = (): void => {
      const returnFocus = inlineItem
        ? `configure-discovery-${inlineItem.id}`
        : 'toggle-create-tool';
      this.discardCreateToolDraft();
      this.refresh();
      this.focusAndScroll(returnFocus);
    };

    const closeBtn = this.button('Cancel', cancelForm, 'new-tool-close');
    closeBtn.setAttribute('aria-label', 'Cancel add tool form');

    const isCliAdapter = providerFor(draft.adapter) === null;
    const creating = this.pending.has('tool-create');

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
        h(
          'div',
          { className: 'aq-tools-create-heading' },
          h(
            'h4',
            { id: `${this.pageId}-add-tool` },
            inlineItem ? `Configure ${inlineItem.label}` : 'Add custom or API tool'
          ),
          h(
            'p',
            { className: 'aq-note' },
            inlineItem
              ? 'Review the label or open Advanced to customize the registry ID, adapter, or executable path.'
              : 'Enter a display label. Technical IDs and custom paths are optional under Advanced.'
          )
        ),
        closeBtn
      ),
      h(
        'div',
        { className: 'aq-tools-create-body', id: bodyId },
        h(
          'div',
          { className: 'aq-tools-form-grid' },
          this.field('Label', label, 'Shown on tool cards and in Profiles.'),
          inlineItem === null
            ? this.field('Adapter', adapter, 'API adapters use the OS keychain vault and never spawn a CLI.')
            : null,
          this.checkField('Enabled for profiles', enabled)
        ),
        h(
          'details',
          { className: 'aq-tools-advanced', open: duplicate ? true : undefined },
          h('summary', undefined, 'Advanced (ID, executable path & adapter)'),
          h(
            'div',
            { className: 'aq-tools-form-grid' },
            this.field(
              'Tool ID',
              id,
              'Optional unique registry key. Derived automatically if left blank.'
            ),
            inlineItem !== null
              ? this.field('Adapter', adapter, 'API adapters use the OS keychain vault and never spawn a CLI.')
              : null,
            this.field(
              'Executable path',
              executable,
              'Optional absolute path for CLI tools. Never executed until you explicitly load models or run a task.'
            )
          )
        ),
        duplicate
          ? h('p', { className: 'aq-tools-inline-error' }, 'This tool ID already exists.')
          : null,
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-form-actions' },
          isCliAdapter
            ? this.button(
                creating ? 'Saving…' : 'Save and choose models',
                () => void this.createTool(true),
                'new-tool-save-and-probe',
                'primary',
                creating
              )
            : null,
          this.button(
            creating ? 'Adding…' : 'Add tool',
            () => void this.createTool(false),
            'new-tool-submit',
            isCliAdapter ? undefined : 'primary',
            creating
          ),
          this.button(
            'Cancel',
            cancelForm,
            'new-tool-cancel'
          )
        )
      )
    );
  }

  private buildTool(
    snapshot: RegistrySnapshot,
    tool: ToolDefinition,
    discoveredMatch: ToolsDiscoveryItem | null = null
  ): HTMLElement {
    const draft = this.toolEdit(tool);
    const changed = !sameTool(tool, draft);
    const provider = providerFor(tool.adapter);
    const probe = this.probes.get(tool.id);
    const api = provider ? this.apiState(tool.id) : null;
    const using = snapshot.models.filter((model) => model.tool_id === tool.id);
    const deleting = this.pending.has(`tool-delete:${tool.id}`);
    const confirming = this.deleteConfirm?.kind === 'tool' && this.deleteConfirm?.id === tool.id;
    const cardKey = `tool:${tool.id}`;
    const open = this.expanded.has(cardKey);
    const pickerOpen = this.openPickers.has(tool.id);
    const bodyId = `${this.pageId}-tool-${token(tool.id)}-body`;
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

    const probeBusy =
      provider === null
        ? this.pending.has(`probe:${tool.id}`)
        : this.pending.has(`catalog:api:${tool.id}`);
    const hasLoadedCatalog =
      provider === null ? Boolean(probe?.result) : Boolean(api?.loaded);

    const chooseModelsBtn = this.button(
      probeBusy
        ? 'Loading models…'
        : pickerOpen && hasLoadedCatalog
          ? 'Refresh models'
          : 'Choose models',
      () => {
        this.openPickers.add(tool.id);
        if (provider === null) {
          void this.probe(tool);
        } else {
          void this.loadModels(tool, provider);
        }
      },
      provider === null ? `tool-${tool.id}-probe` : `api-${tool.id}-catalog`,
      using.length === 0 && !pickerOpen ? 'primary' : undefined,
      probeBusy
    );
    if (using.length === 0 && !pickerOpen) {
      chooseModelsBtn.classList.add('aq-tools-primary');
    }

    const handleToggleDetails = (): void => {
      if (open) {
        this.discardToolEditDraft(tool.id);
        this.refresh();
        this.focusAndScroll(`tool-${tool.id}-details`);
        return;
      }
      this.requestToolSwitch(tool.label, `edit:${tool.id}`, () => {
        this.expanded.add(cardKey);
        this.rewrite();
        this.focusAndScroll(`tool-${tool.id}-label`);
      });
    };

    const toggle = this.button(
      open ? 'Close' : 'Edit',
      handleToggleDetails,
      `tool-${tool.id}-details`
    );
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-controls', bodyId);
    toggle.setAttribute('aria-label', `${open ? 'Close' : 'Edit'} details for ${tool.label}`);

    return h(
      'article',
      {
        className: `aq-tools-card aq-tools-choice${tool.enabled ? '' : ' aq-tools-card-disabled'}`,
      },
      h(
        'div',
        { className: 'aq-tools-tool-head' },
        createBrandIconElement(brandFor(tool.adapter)),
        h(
          'div',
          { className: 'aq-tools-tool-title' },
          h('h4', undefined, tool.label),
          h(
            'span',
            { className: 'aq-tools-hint' },
            using.length === 0
              ? 'Registered · No models added yet'
              : `${using.length} model${using.length === 1 ? '' : 's'} added for Profiles`
          )
        ),
        h(
          'div',
          { className: 'aq-tools-card-controls' },
          h(
            'div',
            { className: 'aq-tools-badges' },
            h(
              'span',
              { className: 'aq-badge', 'data-status': tool.enabled ? 'active' : 'warn' },
              tool.enabled ? 'Enabled' : 'Disabled'
            ),
            discoveredMatch
              ? h('span', { className: 'aq-badge', 'data-status': 'found' }, 'Added')
              : null,
            h(
              'span',
              { className: 'aq-badge', 'data-status': using.length > 0 ? 'active' : 'warn' },
              using.length > 0
                ? `${using.length} model${using.length === 1 ? '' : 's'}`
                : 'No models added'
            ),
            h('span', { className: 'aq-badge' }, ADAPTER_LABELS[tool.adapter]),
            changed ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Unsaved draft') : null
          ),
          h(
            'div',
            { className: 'aq-tools-actions aq-tools-form-actions' },
            chooseModelsBtn,
            toggle
          )
        )
      ),
      using.length === 0 && !pickerOpen && !open
        ? h(
            'p',
            { className: 'aq-tools-hint' },
            'Registration alone does not add models. Click Choose models to read the catalog and select models for Profiles.'
          )
        : null,
      pickerOpen
        ? provider === null
          ? this.buildProbe(tool, probe)
          : this.buildApiCatalogPicker(tool, provider)
        : null,
      open
        ? h(
            'div',
            { className: 'aq-tools-card-body', id: bodyId },
            h(
              'div',
              { className: 'aq-tools-form-grid aq-tools-edit-grid' },
              this.field('Label', label),
              this.checkField('Enabled for profiles', enabled)
            ),
            h(
              'details',
              { className: 'aq-tools-advanced' },
              h('summary', undefined, 'Advanced (ID, adapter, executable path & diagnostics)'),
              h(
                'p',
                { className: 'aq-tools-hint' },
                `Tool ID: ${tool.id}`
              ),
              h(
                'div',
                { className: 'aq-tools-form-grid aq-tools-edit-grid' },
                this.field('Adapter', adapter),
                this.field(
                  'Executable path',
                  executable,
                  'Custom path applies to this tool only. Save before loading models from a changed path.'
                )
              ),
              this.buildStatus(provider, probe),
              h(
                'div',
                { className: 'aq-tools-actions aq-tools-form-actions' },
                using.length > 0
                  ? h(
                      'span',
                      { className: 'aq-tools-hint aq-tools-delete-note' },
                      `Remove ${using.length} model${using.length === 1 ? '' : 's'} before deleting this tool.`
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
              )
            ),
            this.conflictBlock(
              draft,
              () => {
                this.discardToolEditDraft(tool.id);
                this.refresh();
              },
              `tool-${tool.id}-conflict`
            ),
            h(
              'div',
              { className: 'aq-tools-actions aq-tools-card-actions aq-tools-form-actions' },
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
                'Cancel',
                () => {
                  this.discardToolEditDraft(tool.id);
                  this.refresh();
                  this.focusAndScroll(`tool-${tool.id}-details`);
                },
                `tool-${tool.id}-discard`
              )
            ),
            h(
              'p',
              { className: 'aq-tools-hint' },
              changed
                ? 'Save changes before loading models from an updated path or adapter.'
                : provider === null
                  ? 'Choose models reads the catalog from the saved tool definition.'
                  : 'API adapters never spawn a CLI. Credential and synthetic controls use the saved tool definition.'
            ),
            provider ? this.buildApi(tool, provider) : null
          )
        : null
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
      probe?.loading ? h('p', { className: 'aq-tools-inline-status', role: 'status' }, 'Loading catalog models…') : null,
      probe?.error ? this.errorBlock(probe.error) : null
    );
  }

  private buildProbe(tool: ToolDefinition, probe: ProbeState | undefined): HTMLElement {
    const key = `probe:${tool.id}`;
    return this.buildCatalogPicker(
      tool,
      key,
      Boolean(probe?.loading),
      probe?.error ?? null,
      probe?.result ? probe.result.models : null,
      () => void this.probe(tool)
    );
  }

  private buildApiCatalogPicker(tool: ToolDefinition, provider: Provider): HTMLElement {
    const api = this.apiState(tool.id);
    const key = `api:${tool.id}`;
    return this.buildCatalogPicker(
      tool,
      key,
      api.loading,
      api.error,
      api.loaded ? api.models : null,
      () => void this.loadModels(tool, provider)
    );
  }

  private buildCatalogPicker(
    tool: ToolDefinition,
    key: string,
    loading: boolean,
    error: SafeError | null,
    catalogModels: readonly ModelDefinition[] | null,
    onRefreshCatalog: () => void
  ): HTMLElement {
    const closePicker = (): void => {
      this.closeModelPicker(tool.id, true);
    };

    const head = h(
      'div',
      { className: 'aq-tools-form-head' },
      h(
        'div',
        { className: 'aq-tools-create-heading' },
        h('h5', undefined, `Choose models for ${tool.label}`),
        h(
          'p',
          { className: 'aq-tools-hint' },
          'Select new models to use in Profiles, then click Add selected models. Imported models keep any detected effort options. Already-added models stay saved with their existing settings.'
        )
      ),
      this.button('Close', closePicker, `close-picker-${tool.id}`)
    );

    if (loading) {
      return h(
        'div',
        { className: 'aq-tools-probe' },
        head,
        h(
          'p',
          { className: 'aq-tools-inline-status', role: 'status' },
          `Reading model catalog from ${tool.label}…`
        ),
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-form-actions' },
          this.button('Cancel', closePicker, `${key}-cancel`)
        )
      );
    }

    if (error) {
      return h(
        'div',
        { className: 'aq-tools-probe' },
        head,
        this.errorBlock(error),
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-form-actions' },
          this.button('Retry catalog', onRefreshCatalog, `${key}-retry`, 'primary'),
          this.button('Cancel', closePicker, `${key}-cancel`)
        )
      );
    }

    if (!catalogModels) {
      return h(
        'div',
        { className: 'aq-tools-probe aq-tools-probe-empty' },
        head,
        h(
          'p',
          { className: 'aq-tools-hint' },
          'No catalog loaded yet. Click Load catalog to query the saved tool.'
        ),
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-form-actions' },
          this.button('Load catalog', onRefreshCatalog, `${key}-load`, 'primary'),
          this.button('Cancel', closePicker, `${key}-cancel`)
        )
      );
    }

    const importableModels = catalogModels.filter((model) => this.importable(model));
    const currentQuery = this.catalogSearch.get(key) ?? '';
    const searchInput = this.input(
      `${key}-search`,
      currentQuery,
      'Search models by name, ID, or effort…'
    );
    searchInput.classList.add('aq-tools-catalog-search');
    searchInput.setAttribute('aria-label', `Search models for ${tool.label}`);

    const hasProviderFilter = tool.adapter === 'opencode_v2';
    const providerSelect = hasProviderFilter
      ? this.select(
          `${key}-provider`,
          catalogProviderOptions(catalogModels),
          this.catalogProviders.get(key) ?? 'all'
        )
      : null;
    providerSelect?.setAttribute('aria-label', `Model source for ${tool.label}`);

    const stateEl = h('div', { className: 'aq-tools-catalog-state', role: 'status' });
    const listEl = h('div', { className: 'aq-tools-catalog aq-tools-catalog-list' });
    const actionsEl = h('div', { className: 'aq-tools-actions aq-tools-form-actions' });
    const effortPlan = planDetectedEffortUpdates(
      this.snapshot?.models ?? [],
      catalogModels,
      tool.id
    );
    const effortUpdateCount = effortPlan.updates.length;
    const effortUpdating = this.pending.has(`effort-fill:${tool.id}`);
    const effortBlock =
      effortUpdateCount > 0
        ? h(
            'div',
            {
              className: 'aq-tools-catalog-state',
              role: 'group',
              'aria-label': 'Fill missing effort options',
            },
            h(
              'p',
              { className: 'aq-tools-hint' },
              `Detected effort options are available for ${effortUpdateCount} saved model${effortUpdateCount === 1 ? '' : 's'} on this tool with no saved effort metadata. This fills only the missing options and never overwrites a nonempty customized effort list. It keeps saved labels, model arguments, allow-without-effort choices, and existing profile assignments, and it does not add or remove models. Detected options are catalog metadata, not runtime verification.`
            ),
            h(
              'div',
              { className: 'aq-tools-actions aq-tools-form-actions' },
              this.button(
                effortUpdating
                  ? 'Updating effort options…'
                  : `Use detected effort options (${effortUpdateCount})`,
                () => void this.applyDetectedEfforts(tool, catalogModels),
                `${key}-fill-efforts`,
                undefined,
                effortUpdating
              )
            )
          )
        : null;

    const matchesSearch = (model: ModelDefinition, rawQuery: string): boolean => {
      const q = rawQuery.trim().toLowerCase();
      if (!q) return true;
      const haystack = `${model.label} ${model.id} ${model.model_arg} ${(model.efforts ?? []).join(' ')}`.toLowerCase();
      return haystack.includes(q);
    };

    const renderPickerBody = (renderRows = true): void => {
      const currentSelection = this.selection(key, importableModels);
      const query = this.catalogSearch.get(key) ?? '';
      const provider = hasProviderFilter ? this.catalogProviders.get(key) ?? 'all' : 'all';
      const filtered = catalogModels.filter((model) =>
        matchesCatalogProvider(model.model_arg, provider) && matchesSearch(model, query)
      );
      const visibleIds = new Set(filtered.map(model => model.id));
      const hiddenSelected = Array.from(currentSelection).filter(id => !visibleIds.has(id)).length;
      const addedForToolCount = catalogModels.filter((model) =>
        this.snapshot?.models.some(
          (saved) => saved.id.trim() === model.id.trim() && saved.tool_id === tool.id
        )
      ).length;

      const stateNodes: HTMLElement[] = [
        h(
          'span',
          undefined,
          currentSelection.size > 0
            ? `${currentSelection.size} selected to add`
            : '0 selected'
        ),
        h('span', undefined, `${addedForToolCount} already added for ${tool.label}`),
        h('span', undefined, `Showing ${filtered.length} of ${catalogModels.length}`),
      ];
      if (hiddenSelected > 0) {
        stateNodes.push(h('span', { className: 'aq-tools-hint' },
          `${hiddenSelected} selected outside the current view. Clear selection to remove them.`));
      }
      if (currentSelection.size > 0) {
        stateNodes.push(
          this.button(
            'Clear selection',
            () => {
              currentSelection.clear();
              renderPickerBody();
            },
            `${key}-clear-selection`
          )
        );
      }
      stateEl.replaceChildren(...stateNodes);

      if (renderRows && catalogModels.length === 0) {
        listEl.replaceChildren(
          h('p', { className: 'aq-note' }, 'The catalog reported no models.')
        );
      } else if (renderRows && filtered.length === 0) {
        listEl.replaceChildren(
          h('p', { className: 'aq-note' },
            provider === 'opencode-go' && query.trim() === ''
              ? 'OpenCode Go is not listed in this catalog. Check your Go connection in OpenCode, then refresh the catalog.'
              : `No models match the selected source and search "${query.trim()}".`)
        );
      } else if (renderRows) {
        listEl.replaceChildren(
          ...filtered.map((model, index) =>
            this.catalogRow(tool, key, model, currentSelection, index, () =>
              renderPickerBody(false)
            )
          )
        );
      }

      const importing = this.pending.has(`import:${key}`);
      const toolHasSavedModels = Boolean(
        this.snapshot?.models.some((saved) => saved.tool_id === tool.id)
      );
      const importBtn = this.button(
        importing
          ? 'Adding models…'
          : `Add selected models${currentSelection.size > 0 ? ` (${currentSelection.size})` : ''}`,
        () => void this.importModels(tool, key, importableModels),
        `${key}-import`,
        currentSelection.size > 0 ? 'primary' : undefined,
        currentSelection.size === 0 || importing
      );
      if (currentSelection.size > 0) {
        importBtn.classList.add('aq-tools-primary');
      }

      const actionNodes: HTMLElement[] = [
        importBtn,
        this.button(
          'Refresh catalog',
          onRefreshCatalog,
          `${key}-refresh`,
          undefined,
          importing
        ),
      ];
      if (toolHasSavedModels && this.onOpenProfiles) {
        const openProfiles = this.onOpenProfiles;
        actionNodes.push(
          this.button(
            'Open Profiles',
            () => openProfiles(),
            `${key}-open-profiles`
          )
        );
      }
      actionNodes.push(
        this.button(
          'Cancel',
          closePicker,
          `${key}-cancel`,
          undefined,
          importing
        )
      );
      actionsEl.replaceChildren(...actionNodes);
    };

    providerSelect?.addEventListener('change', () => {
      this.catalogProviders.set(key, catalogProvider(providerSelect.value));
      renderPickerBody();
    });

    searchInput.addEventListener('input', () => {
      this.catalogSearch.set(key, searchInput.value);
      renderPickerBody();
    });

    renderPickerBody();

    return h(
      'div',
      { className: 'aq-tools-probe' },
      head,
      providerSelect
        ? this.field('Model source', providerSelect,
            'Go and API use separate routes. Catalog visibility does not confirm subscription access.')
        : null,
      catalogModels.length > 0 ? searchInput : null,
      stateEl,
      listEl,
      effortBlock,
      actionsEl
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
              { className: 'aq-tools-actions aq-tools-form-actions' },
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
              { className: 'aq-tools-actions aq-tools-form-actions' },
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
        { className: 'aq-tools-actions aq-tools-form-actions' },
        this.button(
          this.pending.has(`secret-set:${tool.id}`) ? 'Saving credential…' : 'Set credential',
          () => void this.saveSecret(tool, credential),
          `api-${tool.id}-secret-set`,
          'primary',
          secretBusy
        ),
        this.button(
          'Clear',
          () => {
            credential.value = '';
            credential.focus({ preventScroll: true });
          },
          `api-${tool.id}-secret-clear`,
          undefined,
          secretBusy
        )
      ),
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
        ...Array.from(suggestions, ([value, labelText]) => h('option', { value }, labelText))
      ),
      h(
        'div',
        { className: 'aq-tools-actions aq-tools-form-actions' },
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
    const manualOpen = this.expanded.has('create-model');
    return h(
      'section',
      { className: 'aq-tools-section', 'aria-labelledby': `${this.pageId}-models-title` },
      h(
        'div',
        { className: 'aq-tools-section-head' },
        h(
          'div',
          undefined,
          h(
            'h3',
            { className: 'aq-tools-heading', id: `${this.pageId}-models-title` },
            'Saved models'
          ),
          h(
            'p',
            { className: 'aq-note' },
            'Models added from tool catalogs or manual setup appear here. Catalog imports keep any detected effort options; you can review or edit them on any saved model. Detected options are metadata, not runtime verification.'
          )
        ),
        h(
          'div',
          { className: 'aq-tools-actions aq-tools-progress' },
          h(
            'span',
            { className: 'aq-badge', 'data-status': models.length > 0 ? 'active' : 'warn' },
            `${models.length} model${models.length === 1 ? '' : 's'}`
          ),
          models.length > 0 && this.onOpenProfiles
            ? this.button(
                'Open Profiles',
                () => this.onOpenProfiles?.(),
                'open-profiles-models',
                'primary'
              )
            : null
        )
      ),
      models.length === 0
        ? h(
            'p',
            { className: 'aq-note aq-tools-empty' },
            'No models are added yet. Use Add and choose models or Choose models on a tool above, or open Advanced below to add a manual model.'
          )
        : h(
            'div',
            { className: 'aq-tools-list' },
            ...models.map((model) => this.buildModel(snapshot, model))
          ),
      h(
        'details',
        { className: 'aq-tools-advanced', open: manualOpen ? true : undefined },
        h('summary', undefined, 'Advanced: Add manual model'),
        this.buildCreateModel(snapshot)
      )
    );
  }

  private buildCreateModel(snapshot: RegistrySnapshot): HTMLElement {
    const draft = this.newModel;
    const key = 'create-model';
    const open = this.expanded.has(key);
    const bodyId = `${this.pageId}-create-model-body`;
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
    const cancelManualModel = (): void => {
      const defaultToolId = snapshot.tools[0]?.id ?? '';
      this.newModel = emptyModel(snapshot.revision, defaultToolId);
      this.expanded.delete(key);
      this.refresh();
      this.focusAndScroll('toggle-create-model');
    };
    const toggle = this.button(
      open ? 'Cancel' : 'Add manual model',
      () => {
        if (open) {
          cancelManualModel();
        } else {
          this.expanded.add(key);
          this.refresh();
          this.focusAndScroll('new-model-label');
        }
      },
      'toggle-create-model'
    );
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-controls', bodyId);
    toggle.setAttribute('aria-label', open ? 'Cancel add model form' : 'Show add model form');
    return h(
      'div',
      {
        className: `aq-tools-form aq-tools-create${open ? '' : ' aq-tools-create-collapsed'}`,
        role: 'group',
        'aria-labelledby': `${this.pageId}-add-model`,
      },
      h(
        'div',
        { className: 'aq-tools-form-head' },
        h(
          'div',
          { className: 'aq-tools-create-heading' },
          h('h4', { id: `${this.pageId}-add-model` }, 'Add manual model'),
          open
            ? h(
                'p',
                { className: 'aq-note' },
                'Optional manual override: enter effort options yourself. Catalog metadata is not runtime verification.'
              )
            : null
        ),
        toggle
      ),
      open
        ? h(
            'div',
            { className: 'aq-tools-create-body', id: bodyId },
            h(
              'div',
              { className: 'aq-tools-form-grid' },
              this.field('Tool', tool, noTools ? 'Add a tool before creating a model.' : undefined),
              this.field(
                'Model ID',
                id,
                'Unique registry key. Existing IDs are not overwritten by this form.'
              ),
              this.field('Label', label),
              this.field(
                'Model arg',
                arg,
                'Exact provider argument passed to the adapter when selected.'
              ),
              this.field(
                'Efforts',
                efforts,
                'Comma or space separated manual entries. Leave empty only if this model should be saved without effort metadata.'
              ),
              this.checkField(
                'Allow selection without effort',
                allow,
                'This does not imply any specific effort is supported.'
              )
            ),
            duplicate
              ? h('p', { className: 'aq-tools-inline-error' }, 'This model ID already exists.')
              : null,
            h(
              'div',
              { className: 'aq-tools-actions aq-tools-form-actions' },
              this.button(
                this.pending.has('model-create') ? 'Adding…' : 'Add model',
                () => void this.createModel(),
                'new-model-submit',
                'primary',
                noTools || this.pending.has('model-create')
              ),
              this.button(
                'Cancel',
                cancelManualModel,
                'new-model-cancel'
              )
            )
          )
        : null
    );
  }

  private buildModel(snapshot: RegistrySnapshot, model: ModelDefinition): HTMLElement {
    const draft = this.modelEdit(model);
    const changed = !sameModel(model, draft);
    const toolMissing = !snapshot.tools.some((tool) => tool.id === draft.toolId);
    const references = hasProfileReference(snapshot, model.id);
    const deleting = this.pending.has(`model-delete:${model.id}`);
    const confirming = this.deleteConfirm?.kind === 'model' && this.deleteConfirm?.id === model.id;
    const modelTool = snapshot.tools.find((entry) => entry.id === model.tool_id) ?? null;
    const cardKey = `model:${model.id}`;
    const open = this.expanded.has(cardKey);
    const bodyId = `${this.pageId}-model-${token(model.id)}-body`;
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
    const closeModelEdit = (): void => {
      this.modelDrafts.delete(model.id);
      this.expanded.delete(cardKey);
      if (this.deleteConfirm?.kind === 'model' && this.deleteConfirm.id === model.id) {
        this.deleteConfirm = null;
      }
      this.refresh();
      this.focusAndScroll(`model-${model.id}-details`);
    };
    const toggle = this.button(
      open ? 'Close' : 'Edit',
      () => {
        if (open) {
          closeModelEdit();
        } else {
          this.expanded.add(cardKey);
          this.refresh();
          this.focusAndScroll(`model-${model.id}-label`);
        }
      },
      `model-${model.id}-details`
    );
    toggle.setAttribute('aria-expanded', String(open));
    toggle.setAttribute('aria-controls', bodyId);
    toggle.setAttribute('aria-label', `${open ? 'Close' : 'Edit'} details for ${model.label}`);
    return h(
      'article',
      { className: 'aq-tools-card aq-tools-model-card' },
      h(
        'div',
        { className: 'aq-tools-model-head' },
        createBrandIconElement(modelTool ? brandFor(modelTool.adapter) : 'generic'),
        h(
          'div',
          { className: 'aq-tools-tool-title' },
          h('h4', undefined, model.label),
          h('code', { className: 'aq-tools-code' }, model.id),
          h(
            'span',
            { className: 'aq-tools-hint' },
            `${modelTool?.label ?? 'Missing tool'} · ${effortText(model)}`
          )
        ),
        h(
          'div',
          { className: 'aq-tools-card-controls' },
          h(
            'div',
            { className: 'aq-tools-badges' },
            modelTool ? h('span', { className: 'aq-badge' }, ADAPTER_LABELS[modelTool.adapter]) : null,
            changed ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Unsaved draft') : null,
            references ? h('span', { className: 'aq-badge', 'data-status': 'warn' }, 'Referenced') : null
          ),
          toggle
        )
      ),
      open
        ? h(
            'div',
            { className: 'aq-tools-card-body', id: bodyId },
            h(
              'p',
              { className: 'aq-tools-hint' },
              `Model arg: ${safeText(model.model_arg, 'missing')} · ${effortText(model)}`
            ),
            h(
              'div',
              { className: 'aq-tools-form-grid aq-tools-edit-grid' },
              this.field(
                'Tool',
                tool,
                toolMissing
                  ? 'The referenced tool is missing. Choose a current tool or cancel.'
                  : undefined
              ),
              this.field('Label', label),
              this.field('Model arg', arg),
              this.field(
                'Efforts',
                efforts,
                'Detected options come from the catalog when available. You can edit them or add manual entries; catalog metadata is not runtime verification.'
              ),
              this.checkField('Allow selection without effort', allow)
            ),
            this.conflictBlock(
              draft,
              closeModelEdit,
              `model-${model.id}-conflict`
            ),
            h(
              'div',
              { className: 'aq-tools-actions aq-tools-card-actions aq-tools-form-actions' },
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
                'Cancel',
                closeModelEdit,
                `model-${model.id}-discard`
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
          )
        : null
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
        { className: 'aq-tools-actions aq-tools-form-actions' },
        this.button(
          'Reload latest',
          () => {
            draft.overwrite = false;
            this.notifyOuter();
            this.refresh();
          },
          `${key}-reload`
        ),
        this.button('Cancel', discard, `${key}-discard`)
      ),
      this.checkField(
        'Allow overwrite with this draft',
        overwrite,
        'Required before saving the preserved local values over a newer registry revision.'
      )
    );
  }

  private catalogRow(
    tool: ToolDefinition,
    key: string,
    model: ModelDefinition,
    selection: Set<string>,
    index: number,
    onSelectionChanged?: () => void
  ): HTMLElement {
    const importable = this.importable(model);
    const modelId = model.id?.trim() ?? '';
    const saved = this.snapshot?.models.find((entry) => entry.id.trim() === modelId) ?? null;
    const alreadyOnThisTool = saved !== null && saved.tool_id === tool.id;
    const usedByOtherTool = saved !== null && saved.tool_id !== tool.id;
    const otherToolLabel = usedByOtherTool
      ? this.snapshot?.tools.find((entry) => entry.id === saved.tool_id)?.label ?? saved.tool_id
      : null;
    const canSelect = importable && saved === null;

    const check = this.checkbox(
      `${key}-model-${index}-${model.id || 'missing'}`,
      alreadyOnThisTool || (canSelect && selection.has(model.id))
    );
    check.disabled = !canSelect;
    check.addEventListener('change', () => {
      if (!canSelect) {
        check.checked = alreadyOnThisTool;
        return;
      }
      if (check.checked) selection.add(model.id);
      else selection.delete(model.id);
      if (onSelectionChanged) {
        onSelectionChanged();
      } else {
        this.refresh();
      }
    });
    return h(
      'div',
      {
        className: `aq-tools-catalog-row${!importable || usedByOtherTool ? ' aq-tools-catalog-row-invalid' : ''}`,
      },
      h(
        'label',
        { className: 'aq-tools-catalog-check', for: check.id },
        check,
        h(
          'span',
          { className: 'aq-tools-catalog-copy' },
          h(
            'span',
            { className: 'aq-tools-badges' },
            h('strong', undefined, safeText(model.label, model.id || 'Unnamed model')),
            alreadyOnThisTool
              ? h('span', { className: 'aq-badge', 'data-status': 'active' }, 'Added')
              : null,
            usedByOtherTool
              ? h(
                  'span',
                  { className: 'aq-badge', 'data-status': 'warn' },
                  `ID used by ${otherToolLabel}`
                )
              : null
          ),
          h('code', { className: 'aq-tools-code' }, model.id || 'missing-id'),
          h(
            'span',
            { className: 'aq-tools-hint' },
            `Model arg: ${safeText(model.model_arg, 'missing')} · ${effortText(model)}`
          ),
          alreadyOnThisTool
            ? h(
                'span',
                { className: 'aq-tools-hint' },
                'Already added for this tool. Existing label, model argument, effort options, and profile assignments are preserved.'
              )
            : usedByOtherTool
              ? h(
                  'span',
                  { className: 'aq-tools-hint' },
                  `Model ID "${model.id}" already belongs to ${otherToolLabel} and cannot be overwritten from ${tool.label}.`
                )
              : h(
                  'span',
                  { className: 'aq-tools-hint' },
                  'Detected effort options are catalog metadata, not runtime verification.'
                )
        )
      ),
      !importable
        ? h('span', { className: 'aq-tools-inline-error' }, 'Missing required catalog metadata.')
        : null
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
    const registeredIds = new Set(
      (this.snapshot?.models ?? []).map((saved) => saved.id.trim())
    );
    const valid = new Set(
      models
        .filter((model) => this.importable(model) && !registeredIds.has(model.id.trim()))
        .map((model) => model.id)
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
      this.expanded.add(`tool:${tool.id}`);
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
      this.expanded.add(`model:${model.id}`);
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
    const label = draft.label.trim();
    if (!label) return 'Enter a tool label.';
    const existingTools = this.snapshot?.tools ?? [];
    const id =
      isNew && !draft.id.trim()
        ? uniqueToolId(deriveToolIdFromLabel(label, draft.adapter), existingTools)
        : draft.id.trim();
    if (!id) return 'Enter a tool ID.';
    if (isNew && existingTools.some((tool) => tool.id === id)) {
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

  private async createTool(chooseModelsAfter = false): Promise<void> {
    if (!this.snapshot || this.disposed) return;
    const draft = this.newTool;
    const tool = this.validateTool(draft, true);
    if (typeof tool === 'string') {
      this.validationError(tool);
      return;
    }
    const submittedConfiguringId = this.configuringDiscoveredId;
    const submittedState = {
      id: draft.id,
      label: draft.label,
      adapter: draft.adapter,
      executable: draft.executable,
      enabled: draft.enabled,
    };
    const isSameCreateDraft = (): boolean =>
      this.expanded.has('create-tool') &&
      this.newTool === draft &&
      this.configuringDiscoveredId === submittedConfiguringId &&
      draft.id === submittedState.id &&
      draft.label === submittedState.label &&
      draft.adapter === submittedState.adapter &&
      draft.executable === submittedState.executable &&
      draft.enabled === submittedState.enabled;

    let openModelsAfter = false;
    const saved = await this.apply('tool-create', { op: 'upsert_tool', tool }, (next) => {
      if (isSameCreateDraft()) {
        this.newTool = emptyTool(next.revision);
        this.expanded.delete('create-tool');
        this.configuringDiscoveredId = null;
        if (this.pendingSwitch?.fromKind === 'create-tool') {
          this.pendingSwitch = null;
        }
        if (chooseModelsAfter) {
          this.openPickers.add(tool.id);
          openModelsAfter = true;
        }
      }
      this.notice = {
        tone: 'success',
        title: `${tool.label} added`,
        text:
          chooseModelsAfter && openModelsAfter
            ? `Added "${tool.label}" at revision ${next.revision} and reading its model catalog.`
            : `Added tool "${tool.label}" at revision ${next.revision}. Click Choose models on its card when you are ready to add models.`,
      };
    });
    if (saved && openModelsAfter && !this.disposed && this.openPickers.has(tool.id)) {
      const provider = providerFor(tool.adapter);
      if (provider === null) {
        await this.probe(tool);
        if (!this.disposed && this.openPickers.has(tool.id)) {
          this.focusAndScroll(`probe:${tool.id}-search`);
        }
      } else {
        await this.loadModels(tool, provider);
        if (!this.disposed && this.openPickers.has(tool.id)) {
          this.focusAndScroll(`api:${tool.id}-search`);
        }
      }
    }
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
    const submittedState = {
      label: draft.label,
      adapter: draft.adapter,
      executable: draft.executable,
      enabled: draft.enabled,
    };
    const isSameToolDraft = (): boolean => {
      const current = this.toolDrafts.get(tool.id);
      return (
        this.expanded.has(`tool:${tool.id}`) &&
        current === draft &&
        current.label === submittedState.label &&
        current.adapter === submittedState.adapter &&
        current.executable === submittedState.executable &&
        current.enabled === submittedState.enabled
      );
    };
    await this.apply(
      `tool-save:${tool.id}`,
      { op: 'upsert_tool', tool: value },
      (next) => {
        if (isSameToolDraft()) {
          this.toolDrafts.delete(tool.id);
          this.expanded.delete(`tool:${tool.id}`);
          if (
            this.pendingSwitch?.fromKind === 'edit-tool' &&
            this.pendingSwitch.fromToolId === tool.id
          ) {
            this.pendingSwitch = null;
          }
        }
        this.probes.delete(tool.id);
        this.selections.delete(`probe:${tool.id}`);
        if (tool.adapter !== value.adapter) {
          this.secrets.delete(tool.id);
          this.apiStates.delete(tool.id);
          this.selections.delete(`api:${tool.id}`);
        }
        this.notice = { tone: 'success', text: `Saved tool "${value.label}" at revision ${next.revision}.` };
      },
      (error) => {
        if (isConflict(error) && isSameToolDraft()) {
          draft.conflict = true;
          draft.overwrite = false;
          this.expanded.add(`tool:${tool.id}`);
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
        this.expanded.delete(`tool:${tool.id}`);
        this.openPickers.delete(tool.id);
        this.probes.delete(tool.id);
        this.secrets.delete(tool.id);
        this.apiStates.delete(tool.id);
        this.selections.delete(`probe:${tool.id}`);
        this.selections.delete(`api:${tool.id}`);
        this.catalogSearch.delete(`probe:${tool.id}`);
        this.catalogSearch.delete(`api:${tool.id}`);
        this.notice = { tone: 'success', text: `Deleted tool "${tool.label}" at revision ${next.revision}.` };
      }
    );
    if (!deleted) this.deleteConfirm = null;
  }

  private async probe(tool: ToolDefinition): Promise<void> {
    if (this.disposed || this.pending.has(`probe:${tool.id}`)) return;
    this.openPickers.add(tool.id);
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
        if (this.openPickers.has(tool.id)) {
          this.selections.set(`probe:${tool.id}`, new Set());
        } else {
          this.selections.delete(`probe:${tool.id}`);
        }
      }
    } catch (err) {
      if (!this.disposed) {
        state.error = safeError(err);
      }
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
    const rawSelection = this.selections.get(key);
    const hadAlreadyRegistered = Boolean(
      rawSelection &&
        Array.from(rawSelection).some((id) =>
          this.snapshot?.models.some((saved) => saved.id.trim() === id.trim())
        )
    );
    const selection = this.selection(key, source);
    const existingIds = new Set(this.snapshot.models.map((saved) => saved.id.trim()));
    const selected = source.filter(
      (model) =>
        this.importable(model) &&
        selection.has(model.id) &&
        !existingIds.has(model.id.trim())
    );
    if (selected.length === 0) {
      this.validationError(
        hadAlreadyRegistered
          ? 'All selected models are already added. Existing model settings were preserved.'
          : 'Select at least one new catalog model to add.'
      );
      return;
    }
    const models: ModelDefinition[] = selected.map((model) => ({
      id: model.id.trim(),
      tool_id: tool.id,
      label: model.label.trim(),
      model_arg: model.model_arg.trim(),
      efforts: [...model.efforts],
      allow_no_effort: Boolean(model.allow_no_effort) || model.efforts.length === 0,
    }));
    await this.apply(`import:${key}`, { op: 'upsert_models', models }, (next) => {
      this.selections.delete(key);
      this.catalogSearch.delete(key);
      this.openPickers.delete(tool.id);
      this.notice = {
        tone: 'success',
        title: 'Models added',
        text: `Added ${models.length} model${models.length === 1 ? '' : 's'} for ${tool.label} at revision ${next.revision}. Catalog metadata, including any detected effort options, was kept. You can now assign ${models.length === 1 ? 'it' : 'them'} in Profiles.`,
        showOpenProfiles: true,
      };
    });
  }

  private async applyDetectedEfforts(
    tool: ToolDefinition,
    catalogModels: readonly ModelDefinition[]
  ): Promise<void> {
    if (this.disposed || !this.snapshot) return;
    const key = `effort-fill:${tool.id}`;
    if (this.pending.has(key)) return;
    const plan = planDetectedEffortUpdates(this.snapshot.models, catalogModels, tool.id);
    const count = plan.updates.length;
    if (count === 0) {
      this.notice = {
        tone: 'info',
        title: 'No missing effort options',
        text: `No saved model on ${tool.label} matches a catalog model ID and model argument with empty effort options. Nothing was written.`,
      };
      this.refresh();
      return;
    }
    await this.apply(
      key,
      { op: 'upsert_models', models: plan.updates },
      () => {
        this.notice = {
          tone: 'success',
          title: 'Effort options updated',
          text: `Updated effort options for ${count} saved model${count === 1 ? '' : 's'} on ${tool.label}. You can now select them in Profiles.`,
          showOpenProfiles: true,
        };
      }
    );
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
    const submittedState = {
      toolId: draft.toolId,
      id: draft.id,
      label: draft.label,
      modelArg: draft.modelArg,
      efforts: draft.efforts,
      allowNoEffort: draft.allowNoEffort,
    };
    const isSameCreateModelDraft = (): boolean =>
      this.expanded.has('create-model') &&
      this.newModel === draft &&
      draft.toolId === submittedState.toolId &&
      draft.id === submittedState.id &&
      draft.label === submittedState.label &&
      draft.modelArg === submittedState.modelArg &&
      draft.efforts === submittedState.efforts &&
      draft.allowNoEffort === submittedState.allowNoEffort;

    await this.apply('model-create', { op: 'upsert_model', model }, (next) => {
      if (isSameCreateModelDraft()) {
        const toolId = next.tools.some((tool) => tool.id === preferredTool)
          ? preferredTool
          : next.tools[0]?.id ?? '';
        this.newModel = emptyModel(next.revision, toolId);
        this.expanded.delete('create-model');
      }
      this.notice = {
        tone: 'success',
        title: 'Model added',
        text: `Added model "${model.label}" at revision ${next.revision}.`,
        showOpenProfiles: true,
      };
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
    const submittedState = {
      toolId: draft.toolId,
      label: draft.label,
      modelArg: draft.modelArg,
      efforts: draft.efforts,
      allowNoEffort: draft.allowNoEffort,
    };
    const isSameModelDraft = (): boolean => {
      const current = this.modelDrafts.get(model.id);
      return (
        this.expanded.has(`model:${model.id}`) &&
        current === draft &&
        current.toolId === submittedState.toolId &&
        current.label === submittedState.label &&
        current.modelArg === submittedState.modelArg &&
        current.efforts === submittedState.efforts &&
        current.allowNoEffort === submittedState.allowNoEffort
      );
    };
    await this.apply(
      `model-save:${model.id}`,
      { op: 'upsert_model', model: value },
      (next) => {
        if (isSameModelDraft()) {
          this.modelDrafts.delete(model.id);
          this.expanded.delete(`model:${model.id}`);
        }
        this.notice = { tone: 'success', text: `Saved model "${value.label}" at revision ${next.revision}.` };
      },
      (error) => {
        if (isConflict(error) && isSameModelDraft()) {
          draft.conflict = true;
          draft.overwrite = false;
          this.expanded.add(`model:${model.id}`);
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
        this.expanded.delete(`model:${model.id}`);
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
      if (!this.disposed) {
        state.error = safeError(err);
        this.expanded.add(`tool:${toolId}`);
      }
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
        if (!this.disposed) {
          state.error = safeError(err);
          this.expanded.add(`tool:${tool.id}`);
        }
      }
    } catch (err) {
      if (!this.disposed) {
        state.error = safeError(err);
        this.expanded.add(`tool:${tool.id}`);
      }
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
        if (!this.disposed) {
          state.error = safeError(err);
          this.expanded.add(`tool:${tool.id}`);
        }
      }
    } catch (err) {
      if (!this.disposed) {
        state.error = safeError(err);
        this.expanded.add(`tool:${tool.id}`);
      }
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
    this.openPickers.add(tool.id);
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
        if (this.openPickers.has(tool.id)) {
          this.selections.set(`api:${tool.id}`, new Set());
        } else {
          this.selections.delete(`api:${tool.id}`);
        }
      }
    } catch (err) {
      if (!this.disposed) {
        state.error = safeError(err);
      }
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
      if (!this.disposed) {
        state.testError = safeError(err);
        this.expanded.add(`tool:${tool.id}`);
      }
    } finally {
      if (!this.disposed) {
        state.testLoading = false;
        this.pending.delete(key);
        this.refresh();
      }
    }
  }
}
