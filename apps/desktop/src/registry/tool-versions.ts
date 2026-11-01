import {
  isApiToolAdapter,
  type DesktopToolVersionsSnapshot,
  type ToolDefinition,
  type ToolVersionInspection,
} from './types.ts';

export type ToolVersionScanReason = 'startup' | 'manual' | 'after_save';

export type ToolVersionScanStatus = 'idle' | 'loading' | 'ready' | 'stale' | 'error';

export interface ToolVersionScanState {
  readonly status: ToolVersionScanStatus;
  readonly inspections: readonly ToolVersionInspection[];
  readonly inspectedRegistryRevision: number | null;
  readonly error: string | null;
  /** True once the one-per-app startup scan has been admitted. */
  readonly startupStarted: boolean;
  readonly nextTicketId: number;
  readonly activeTicketId: number | null;
}

export interface ToolVersionTicket {
  readonly id: number;
  readonly registryRevision: number;
  readonly reason: ToolVersionScanReason;
}

export interface ToolVersionScanStart {
  readonly state: ToolVersionScanState;
  readonly ticket: ToolVersionTicket | null;
}

export interface ToolVersionScanSettlement {
  readonly state: ToolVersionScanState;
  readonly accepted: boolean;
}

export function createToolVersionScanState(): ToolVersionScanState {
  return {
    status: 'idle',
    inspections: [],
    inspectedRegistryRevision: null,
    error: null,
    startupStarted: false,
    nextTicketId: 0,
    activeTicketId: null,
  };
}

function isSafeRegistryRevision(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isInteger(value) &&
    value >= 0 &&
    value <= Number.MAX_SAFE_INTEGER
  );
}

function startScan(
  state: ToolVersionScanState,
  registryRevision: number,
  reason: ToolVersionScanReason,
  markStartup: boolean
): ToolVersionScanStart {
  if (!isSafeRegistryRevision(registryRevision)) {
    return { state, ticket: null };
  }
  const nextTicketId = state.nextTicketId + 1;
  const ticket: ToolVersionTicket = {
    id: nextTicketId,
    registryRevision,
    reason,
  };
  return {
    state: {
      ...state,
      status: 'loading',
      error: null,
      startupStarted: markStartup ? true : state.startupStarted,
      nextTicketId,
      activeTicketId: nextTicketId,
    },
    ticket,
  };
}

/**
 * Admits the single startup scan. Later calls return no ticket, including after
 * the scan fails or is superseded by an explicit recheck.
 */
export function beginStartupToolVersionScan(
  state: ToolVersionScanState,
  registryRevision: number
): ToolVersionScanStart {
  if (state.startupStarted) {
    return { state, ticket: null };
  }
  return startScan(state, registryRevision, 'startup', true);
}

export function beginToolVersionScan(
  state: ToolVersionScanState,
  registryRevision: number,
  reason: Exclude<ToolVersionScanReason, 'startup'> = 'manual'
): ToolVersionScanStart {
  return startScan(state, registryRevision, reason, false);
}

function settleStale(
  state: ToolVersionScanState,
  ticket: ToolVersionTicket
): ToolVersionScanState {
  if (state.activeTicketId !== ticket.id) {
    return state;
  }
  return {
    ...state,
    status: 'stale',
    inspections: [],
    inspectedRegistryRevision: null,
    error: null,
    activeTicketId: null,
  };
}

export function applyToolVersionScanSuccess(
  state: ToolVersionScanState,
  ticket: ToolVersionTicket,
  response: DesktopToolVersionsSnapshot,
  currentRegistryRevision: number
): ToolVersionScanSettlement {
  if (state.activeTicketId !== ticket.id) {
    return { state, accepted: false };
  }
  if (
    !isSafeRegistryRevision(currentRegistryRevision) ||
    currentRegistryRevision !== ticket.registryRevision ||
    !isSafeRegistryRevision(response.registry_revision) ||
    response.registry_revision !== ticket.registryRevision
  ) {
    return { state: settleStale(state, ticket), accepted: false };
  }
  return {
    state: {
      ...state,
      status: 'ready',
      inspections: response.tools,
      inspectedRegistryRevision: ticket.registryRevision,
      error: null,
      activeTicketId: null,
    },
    accepted: true,
  };
}

export function applyToolVersionScanError(
  state: ToolVersionScanState,
  ticket: ToolVersionTicket,
  message: string,
  currentRegistryRevision: number
): ToolVersionScanSettlement {
  if (state.activeTicketId !== ticket.id) {
    return { state, accepted: false };
  }
  if (
    !isSafeRegistryRevision(currentRegistryRevision) ||
    currentRegistryRevision !== ticket.registryRevision
  ) {
    return { state: settleStale(state, ticket), accepted: false };
  }
  return {
    state: {
      ...state,
      status: 'error',
      error: message,
      activeTicketId: null,
    },
    accepted: true,
  };
}

/**
 * Keeps version metadata tied to the tool definitions it was requested for.
 * Callers remove an older accepted result when executable selection inputs change.
 */
export function markToolVersionInspectionsStale(
  state: ToolVersionScanState,
  currentRegistryRevision: number
): ToolVersionScanState {
  if (
    state.inspectedRegistryRevision === null ||
    state.inspectedRegistryRevision === currentRegistryRevision
  ) {
    return state;
  }
  return {
    ...state,
    status: state.status === 'loading' ? state.status : 'stale',
    inspections: [],
    inspectedRegistryRevision: null,
    error: state.status === 'loading' ? state.error : null,
  };
}

export function findToolVersionInspection(
  state: ToolVersionScanState,
  toolId: string
): ToolVersionInspection | null {
  return state.inspections.find((inspection) => inspection.tool_id === toolId) ?? null;
}

/**
 * Version metadata only depends on which local executables are enabled and how
 * they are selected. Model, effort, profile, and label edits do not invalidate it.
 */
export function toolInspectionInputsChanged(
  previousTools: readonly ToolDefinition[],
  nextTools: readonly ToolDefinition[]
): boolean {
  const previousLocalTools = previousTools.filter((tool) => !isApiToolAdapter(tool.adapter));
  const nextLocalTools = nextTools.filter((tool) => !isApiToolAdapter(tool.adapter));
  if (previousLocalTools.length !== nextLocalTools.length) {
    return true;
  }
  const previousById = new Map(previousLocalTools.map((tool) => [tool.id, tool]));
  for (const tool of nextLocalTools) {
    const previous = previousById.get(tool.id);
    if (
      !previous ||
      previous.adapter !== tool.adapter ||
      previous.enabled !== tool.enabled ||
      (previous.executable_path ?? '').trim() !== (tool.executable_path ?? '').trim()
    ) {
      return true;
    }
  }
  return false;
}
