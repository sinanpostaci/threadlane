import type {
  DesktopNativeThreadsArgs,
  NativeThread,
  NativeThreadPage,
  RegistrySnapshot,
  ToolDefinition,
} from '../registry/types.ts';
import type { BindingSummary } from '../types.ts';

export const NATIVE_THREAD_RETAINED_LIMIT = 100;

export type NativeQueryMode = 'fresh' | 'more';

export interface NativeQueryContext {
  readonly toolId: string;
  readonly toolLabel: string;
  readonly registryRevision: number;
  readonly projectFilter: string | null;
}

export interface NativeThreadsTicket {
  readonly seq: number;
  readonly generation: number;
  readonly mode: NativeQueryMode;
  readonly context: NativeQueryContext;
  readonly cursor: string | null;
}

export interface NativeThreadsState {
  readonly active: boolean;
  readonly generation: number;
  readonly requestSeq: number;
  readonly registryRevision: number;
  readonly codexTools: readonly ToolDefinition[];
  readonly selectedToolId: string;
  readonly projectDraft: string;
  readonly requestContext: NativeQueryContext | null;
  readonly appliedProjectFilter: string | null;
  readonly appliedContext: NativeQueryContext | null;
  readonly threads: readonly NativeThread[];
  readonly nextCursor: string | null;
  readonly hasQueried: boolean;
  readonly loading: boolean;
  readonly errorMessage: string | null;
  readonly limitReached: boolean;
  readonly bindings: readonly BindingSummary[];
}

export interface NativeQueryStart {
  readonly state: NativeThreadsState;
  readonly ticket: NativeThreadsTicket | null;
}

export type NativeThreadsQuery = (
  args: DesktopNativeThreadsArgs
) => Promise<NativeThreadPage>;

export interface NativeThreadsControllerOptions {
  active?: boolean;
  formatError?: (err: unknown) => string;
}

export function createInitialNativeThreadsState(active: boolean = false): NativeThreadsState {
  return {
    active,
    generation: 0,
    requestSeq: 0,
    registryRevision: -1,
    codexTools: [],
    selectedToolId: '',
    projectDraft: '',
    requestContext: null,
    appliedProjectFilter: null,
    appliedContext: null,
    threads: [],
    nextCursor: null,
    hasQueried: false,
    loading: false,
    errorMessage: null,
    limitReached: false,
    bindings: [],
  };
}

function normalizeProjectFilter(raw: string): string | null {
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeCursor(raw: string | null): string | null {
  if (raw === null) {
    return null;
  }
  return raw.trim().length > 0 ? raw : null;
}

function executableOf(tool: ToolDefinition | null): string | null {
  if (tool === null) {
    return null;
  }
  const trimmed = tool.executable_path?.trim() ?? '';
  return trimmed.length > 0 ? trimmed : null;
}

function cloneToolDefinition(tool: ToolDefinition): ToolDefinition {
  return { ...tool };
}

function toolRuntimeKey(tool: ToolDefinition | null): string {
  if (tool === null) {
    return '';
  }
  return [tool.id, tool.enabled ? '1' : '0', tool.adapter, executableOf(tool) ?? ''].join(
    '\u0000'
  );
}

function contextsEqual(left: NativeQueryContext, right: NativeQueryContext): boolean {
  return (
    left.toolId === right.toolId &&
    left.registryRevision === right.registryRevision &&
    left.projectFilter === right.projectFilter
  );
}

function contextMatchesState(
  state: NativeThreadsState,
  context: NativeQueryContext
): boolean {
  const tool = resolveNativeSelectedTool(state);
  return (
    tool !== null &&
    tool.id === context.toolId &&
    tool.adapter === 'codex_cli' &&
    tool.enabled &&
    state.registryRevision === context.registryRevision &&
    normalizeProjectFilter(state.projectDraft) === context.projectFilter
  );
}

export function resolveNativeSelectedTool(
  state: NativeThreadsState
): ToolDefinition | null {
  return state.codexTools.find((tool) => tool.id === state.selectedToolId) ?? null;
}

export function nativeToolBlockReason(state: NativeThreadsState): string | null {
  if (!state.active) {
    return 'Native metadata queries are available only while the Chats page is open.';
  }
  if (state.codexTools.length === 0) {
    return 'Register a tool with the codex_cli adapter on the Tools page to query read-only native thread metadata.';
  }
  const tool = resolveNativeSelectedTool(state);
  if (tool === null) {
    return 'Select a Codex CLI tool to query native thread metadata.';
  }
  if (tool.adapter !== 'codex_cli') {
    return `Selected tool "${tool.label}" does not use the codex_cli adapter.`;
  }
  if (!tool.enabled) {
    return `Selected tool "${tool.label}" is currently disabled in the registry. Enable it in Tools or select another enabled Codex CLI tool before querying native metadata.`;
  }
  return null;
}

function resetNativeContext(state: NativeThreadsState): NativeThreadsState {
  return {
    ...state,
    generation: state.generation + 1,
    requestContext: null,
    appliedProjectFilter: null,
    appliedContext: null,
    threads: [],
    nextCursor: null,
    hasQueried: false,
    loading: false,
    errorMessage: null,
    limitReached: false,
  };
}

export function syncNativePanel(
  state: NativeThreadsState,
  registry: RegistrySnapshot,
  bindings: readonly BindingSummary[]
): NativeThreadsState {
  const codexTools = registry.tools
    .filter((tool) => tool.adapter === 'codex_cli')
    .map(cloneToolDefinition);
  let selectedToolId = state.selectedToolId;
  if (
    selectedToolId.length === 0 ||
    !codexTools.some((tool) => tool.id === selectedToolId)
  ) {
    const fallback = codexTools.find((tool) => tool.enabled) ?? codexTools[0] ?? null;
    selectedToolId = fallback?.id ?? '';
  }

  const previousTool =
    state.codexTools.find((tool) => tool.id === state.selectedToolId) ?? null;
  const nextTool = codexTools.find((tool) => tool.id === selectedToolId) ?? null;
  const contextChanged =
    state.registryRevision !== registry.revision ||
    state.selectedToolId !== selectedToolId ||
    toolRuntimeKey(previousTool) !== toolRuntimeKey(nextTool);

  const nextState: NativeThreadsState = {
    ...state,
    registryRevision: registry.revision,
    codexTools,
    selectedToolId,
    bindings: [...bindings],
  };
  return contextChanged ? resetNativeContext(nextState) : nextState;
}

export function setNativePanelActive(
  state: NativeThreadsState,
  active: boolean
): NativeThreadsState {
  if (state.active === active) {
    return state;
  }
  return {
    ...state,
    active,
    generation: state.generation + 1,
    requestContext: null,
    loading: false,
  };
}

export function setNativeToolId(
  state: NativeThreadsState,
  toolId: string
): NativeThreadsState {
  if (toolId === state.selectedToolId) {
    return state;
  }
  const tool = state.codexTools.find(
    (candidate) => candidate.id === toolId && candidate.adapter === 'codex_cli'
  );
  if (tool === undefined) {
    return state;
  }
  return resetNativeContext({ ...state, selectedToolId: tool.id });
}

export function setNativeProjectDraft(
  state: NativeThreadsState,
  draft: string
): NativeThreadsState {
  if (draft === state.projectDraft) {
    return state;
  }
  const nextState: NativeThreadsState = { ...state, projectDraft: draft };
  const hasQueryContext =
    state.requestContext !== null ||
    state.appliedContext !== null ||
    state.appliedProjectFilter !== null ||
    state.loading ||
    state.hasQueried ||
    state.threads.length > 0 ||
    state.nextCursor !== null ||
    state.errorMessage !== null ||
    state.limitReached;
  return hasQueryContext ? resetNativeContext(nextState) : nextState;
}

export function beginNativeQuery(
  state: NativeThreadsState,
  mode: NativeQueryMode
): NativeQueryStart {
  if (!state.active || state.loading) {
    return { state, ticket: null };
  }
  const tool = resolveNativeSelectedTool(state);
  if (tool === null || tool.adapter !== 'codex_cli' || !tool.enabled) {
    return { state, ticket: null };
  }

  let context: NativeQueryContext;
  let cursor: string | null;
  if (mode === 'more') {
    if (state.nextCursor === null || state.appliedContext === null) {
      return { state, ticket: null };
    }
    if (!contextMatchesState(state, state.appliedContext)) {
      return { state, ticket: null };
    }
    context = state.appliedContext;
    cursor = state.nextCursor;
  } else {
    context = {
      toolId: tool.id,
      toolLabel: tool.label,
      registryRevision: state.registryRevision,
      projectFilter: normalizeProjectFilter(state.projectDraft),
    };
    cursor = null;
  }

  const seq = state.requestSeq + 1;
  return {
    state: {
      ...state,
      requestSeq: seq,
      requestContext: context,
      loading: true,
      errorMessage: null,
    },
    ticket: {
      seq,
      generation: state.generation,
      mode,
      context,
      cursor,
    },
  };
}

export function dedupeNativeThreads(threads: readonly NativeThread[]): NativeThread[] {
  const seen = new Set<string>();
  const deduped: NativeThread[] = [];
  for (const thread of threads) {
    if (seen.has(thread.id)) {
      continue;
    }
    seen.add(thread.id);
    deduped.push(thread);
  }
  return deduped;
}

export function isNativeTicketCurrent(
  state: NativeThreadsState,
  ticket: NativeThreadsTicket
): boolean {
  return (
    state.active &&
    state.loading &&
    ticket.seq === state.requestSeq &&
    ticket.generation === state.generation &&
    state.requestContext !== null &&
    contextsEqual(state.requestContext, ticket.context) &&
    contextMatchesState(state, ticket.context)
  );
}

export function applyNativePageSuccess(
  state: NativeThreadsState,
  ticket: NativeThreadsTicket,
  page: NativeThreadPage
): NativeThreadsState {
  if (!isNativeTicketCurrent(state, ticket)) {
    return state;
  }

  const incoming = dedupeNativeThreads(page.threads);
  const base = ticket.mode === 'more' ? state.threads : [];
  const merged = dedupeNativeThreads([...base, ...incoming]);
  const retained = merged.slice(0, NATIVE_THREAD_RETAINED_LIMIT);
  const pageCursor = normalizeCursor(page.next_cursor);
  const exceededRetainedLimit = merged.length > retained.length;
  const cappedWithMore =
    retained.length >= NATIVE_THREAD_RETAINED_LIMIT && pageCursor !== null;
  const limitReached = exceededRetainedLimit || cappedWithMore;

  return {
    ...state,
    requestContext: null,
    appliedProjectFilter: ticket.context.projectFilter,
    appliedContext: ticket.context,
    threads: retained,
    nextCursor: limitReached ? null : pageCursor,
    hasQueried: true,
    loading: false,
    errorMessage: null,
    limitReached,
  };
}

export function applyNativePageError(
  state: NativeThreadsState,
  ticket: NativeThreadsTicket,
  message: string
): NativeThreadsState {
  if (!isNativeTicketCurrent(state, ticket)) {
    return state;
  }
  const trimmed = message.trim();
  return {
    ...state,
    requestContext: null,
    hasQueried: true,
    loading: false,
    errorMessage:
      trimmed.length > 0 ? trimmed : 'Native Codex metadata query failed.',
  };
}

/**
 * Owns the read-only metadata request context. It calls the injected reader only
 * from load(); sync, selection, filter, and visibility changes invalidate tickets
 * without reading a tool or probing the CLI.
 */
export class NativeThreadsController {
  private currentState: NativeThreadsState;
  private readonly query: NativeThreadsQuery;
  private readonly formatError: (err: unknown) => string;

  constructor(
    query: NativeThreadsQuery,
    options: NativeThreadsControllerOptions = {}
  ) {
    this.query = query;
    this.formatError =
      options.formatError ??
      ((err: unknown) =>
        err instanceof Error && err.message.trim().length > 0
          ? err.message
          : 'Native Codex metadata query failed.');
    this.currentState = createInitialNativeThreadsState(options.active ?? false);
  }

  public getState(): NativeThreadsState {
    return this.currentState;
  }

  public setActive(active: boolean): void {
    this.currentState = setNativePanelActive(this.currentState, active);
  }

  public sync(
    registry: RegistrySnapshot,
    bindings: readonly BindingSummary[]
  ): void {
    this.currentState = syncNativePanel(this.currentState, registry, bindings);
  }

  public selectTool(toolId: string): void {
    this.currentState = setNativeToolId(this.currentState, toolId);
  }

  public updateProjectDraft(draft: string): void {
    this.currentState = setNativeProjectDraft(this.currentState, draft);
  }

  public async load(mode: NativeQueryMode = 'fresh'): Promise<boolean> {
    const started = beginNativeQuery(this.currentState, mode);
    this.currentState = started.state;
    const ticket = started.ticket;
    if (ticket === null) {
      return false;
    }

    try {
      const page = await this.query({
        toolId: ticket.context.toolId,
        projectId: ticket.context.projectFilter,
        cursor: ticket.cursor,
      });
      this.currentState = applyNativePageSuccess(this.currentState, ticket, page);
    } catch (err) {
      this.currentState = applyNativePageError(
        this.currentState,
        ticket,
        this.formatError(err)
      );
    }
    return true;
  }
}
