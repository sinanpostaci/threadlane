import type { CodexChatTarget } from './app-contract.ts';
import {
  isCanonicalUuid,
  projectDesktopWorkspace,
  type DesktopWorkspaceProjection,
  type JournalWindowHours,
} from './app-model.ts';
import {
  collectObservationWindow,
  OBSERVATION_DEFAULT_PAGE_LIMIT,
  OBSERVATION_MAX_WINDOW_REQUESTS,
} from './observation/paging.ts';
import type {
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
} from './observation/types.ts';
import {
  applyToolVersionScanError,
  applyToolVersionScanSuccess,
  beginStartupToolVersionScan,
  beginToolVersionScan,
  createToolVersionScanState,
  markToolVersionInspectionsStale,
  toolInspectionInputsChanged,
  type ToolVersionScanReason,
  type ToolVersionScanState,
  type ToolVersionTicket,
} from './registry/tool-versions.ts';
import type {
  RegistrySnapshot,
  DesktopToolVersionsSnapshot,
} from './registry/types.ts';

export const OBSERVATION_POLL_INTERVAL_MS = 2_000;
export const REGISTRY_POLL_EVERY_N_TICKS = 15;

export interface ToolDiscoveryEntry {
  id: string;
  label: string;
  availability: string;
  executable_path: string | null;
}

export interface ObservationScopeFilter {
  source_id: string;
  job_id: string;
}

export type ObservationFetchMode = 'replace' | 'refresh' | 'more';

export interface DesktopCoordinatorPorts {
  fetchObservationSnapshot: (
    request?: ObservationSnapshotRequest
  ) => Promise<ObservationSnapshot>;
  fetchRegistrySnapshot?: () => Promise<RegistrySnapshot>;
  fetchToolVersions?: () => Promise<DesktopToolVersionsSnapshot>;
  fetchToolDiscovery?: () => Promise<ToolDiscoveryEntry[]>;
  openCodexChat?: (target: CodexChatTarget) => Promise<void>;
  now?: () => number;
}

export interface DesktopCoordinatorState {
  visible: boolean;
  focused: boolean;
  observationSnapshot: ObservationSnapshot | null;
  observationLoading: boolean;
  observationLoadingMore: boolean;
  observationError: string | null;
  observationScope: ObservationScopeFilter | null;
  observationGeneration: number;
  selectedChatKey: string | null;
  windowHours: JournalWindowHours;
  archiveBeforeMs: number | null;
  registry: RegistrySnapshot | null;
  registryLoading: boolean;
  registryError: string | null;
  toolVersions: ToolVersionScanState;
  discoveredTools: ToolDiscoveryEntry[];
  discoveryLoading: boolean;
  discoveryError: string | null;
  codexOpenInFlight: boolean;
  codexOpenError: string | null;
}

interface ObservationTicket {
  sequence: number;
  generation: number;
  mode: ObservationFetchMode;
  scope: ObservationScopeFilter | null;
  cursor: string | null;
}

function emptyRegistrySnapshot(): RegistrySnapshot {
  return {
    revision: 0,
    tools: [],
    models: [],
    profiles: [],
    schema_version: 1,
    default_profile_id: null,
  };
}

function formatErrorMessage(err: unknown, fallback: string): string {
  if (err instanceof Error && err.message.trim().length > 0) {
    return err.message.trim();
  }
  if (typeof err === 'string' && err.trim().length > 0) {
    return err.trim();
  }
  if (err && typeof err === 'object' && 'message' in err) {
    const msg = (err as { message?: unknown }).message;
    if (typeof msg === 'string' && msg.trim().length > 0) {
      return msg.trim();
    }
  }
  return fallback;
}

function sameScope(
  left: ObservationScopeFilter | null,
  right: ObservationScopeFilter | null
): boolean {
  return (
    (left?.source_id ?? null) === (right?.source_id ?? null) &&
    (left?.job_id ?? null) === (right?.job_id ?? null)
  );
}

/**
 * Merges observation requests using source-scoped keys (`source_id\0id`) so
 * identical request IDs from different observation sources never collide.
 */
export function mergeScopedObservationRequests(
  current: readonly ObservationRequest[],
  incoming: readonly ObservationRequest[]
): ObservationRequest[] {
  const merged = [...current];
  const indexByKey = new Map<string, number>();
  for (let idx = 0; idx < merged.length; idx += 1) {
    const req = merged[idx]!;
    indexByKey.set(`${req.source_id}\u0000${req.id}`, idx);
  }
  for (const req of incoming) {
    const key = `${req.source_id}\u0000${req.id}`;
    const existingIdx = indexByKey.get(key);
    if (existingIdx !== undefined) {
      merged[existingIdx] = req;
    } else {
      indexByKey.set(key, merged.length);
      merged.push(req);
    }
  }
  return merged;
}

/**
 * Merges observation jobs using source-scoped keys (`source_id\0id`).
 */
export function mergeScopedObservationJobs(
  current: readonly ObservationJob[],
  incoming: readonly ObservationJob[]
): ObservationJob[] {
  const merged = [...current];
  const indexByKey = new Map<string, number>();
  for (let idx = 0; idx < merged.length; idx += 1) {
    const job = merged[idx]!;
    indexByKey.set(`${job.source_id}\u0000${job.id}`, idx);
  }
  for (const job of incoming) {
    const key = `${job.source_id}\u0000${job.id}`;
    const existingIdx = indexByKey.get(key);
    if (existingIdx !== undefined) {
      merged[existingIdx] = job;
    } else {
      indexByKey.set(key, merged.length);
      merged.push(job);
    }
  }
  return merged;
}

/**
 * Preserves a healthy source from `previous` if a later page or partial
 * response reports it missing, while keeping authoritative updates from the
 * head page.
 */
export function mergeHealthyObservationSources(
  headSources: readonly ObservationSource[],
  laterSources: readonly ObservationSource[]
): ObservationSource[] {
  const merged = [...headSources];
  const indexById = new Map<string, number>();
  for (let idx = 0; idx < merged.length; idx += 1) {
    indexById.set(merged[idx]!.id, idx);
  }
  for (const source of laterSources) {
    const existingIdx = indexById.get(source.id);
    if (existingIdx === undefined) {
      indexById.set(source.id, merged.length);
      merged.push(source);
      continue;
    }
    const existing = merged[existingIdx]!;
    // Do not downgrade a source that was healthy on the head page just because a later page failed.
    if (
      existing.availability === 'available' &&
      source.availability !== 'available'
    ) {
      continue;
    }
    merged[existingIdx] = source;
  }
  return merged;
}

function mergeWarnings(
  current: readonly string[],
  incoming: readonly string[]
): string[] {
  const merged: string[] = [];
  for (const warning of [...current, ...incoming]) {
    const trimmed = warning.trim();
    if (trimmed.length > 0 && !merged.includes(trimmed)) {
      merged.push(trimmed);
    }
  }
  return merged;
}

export class DesktopAppCoordinator {
  private readonly ports: DesktopCoordinatorPorts;
  private readonly onChange: (state: DesktopCoordinatorState) => void;
  private state: DesktopCoordinatorState;
  private disposed = false;

  private observationTicketSeq = 0;
  private inFlightObservationTicket: ObservationTicket | null = null;
  private pendingObservationTicket: ObservationTicket | null = null;

  private registryTicketSeq = 0;
  private registryInFlightCount = 0;
  private hasLoadedRegistryOnce = false;
  private pendingToolVersionRecheckRevision: number | null = null;

  private discoveryTicketSeq = 0;
  private pollTickCounter = 0;

  constructor(
    ports: DesktopCoordinatorPorts,
    onChange: (state: DesktopCoordinatorState) => void,
    initialOptions: {
      windowHours?: JournalWindowHours;
      archiveBeforeMs?: number | null;
      selectedChatKey?: string | null;
      visible?: boolean;
      focused?: boolean;
    } = {}
  ) {
    this.ports = ports;
    this.onChange = onChange;
    this.state = {
      visible: initialOptions.visible ?? true,
      focused: initialOptions.focused ?? true,
      observationSnapshot: null,
      observationLoading: true,
      observationLoadingMore: false,
      observationError: null,
      observationScope: null,
      observationGeneration: 0,
      selectedChatKey: initialOptions.selectedChatKey ?? null,
      windowHours: initialOptions.windowHours === 24 ? 24 : 6,
      archiveBeforeMs: initialOptions.archiveBeforeMs ?? null,
      registry: null,
      registryLoading: true,
      registryError: null,
      toolVersions: createToolVersionScanState(),
      discoveredTools: [],
      discoveryLoading: false,
      discoveryError: null,
      codexOpenInFlight: false,
      codexOpenError: null,
    };
  }

  getState(): DesktopCoordinatorState {
    return this.state;
  }

  projectWorkspace(nowMs?: number): DesktopWorkspaceProjection {
    const currentNow = nowMs ?? (this.ports.now ? this.ports.now() : Date.now());
    return projectDesktopWorkspace(this.state.observationSnapshot, {
      nowMs: currentNow,
      windowHours: this.state.windowHours,
      archiveBeforeMs: this.state.archiveBeforeMs,
      selectedChatKey: this.state.selectedChatKey,
      scope: this.state.observationScope,
      loading: this.state.observationLoading,
      error: this.state.observationError,
    });
  }

  canPoll(): boolean {
    return !this.disposed && this.state.visible;
  }

  isObservationInFlight(): boolean {
    return this.inFlightObservationTicket !== null;
  }

  dispose(): void {
    this.disposed = true;
    this.inFlightObservationTicket = null;
    this.pendingObservationTicket = null;
  }

  private updateState(patch: Partial<DesktopCoordinatorState>): void {
    if (this.disposed) {
      return;
    }
    this.state = {
      ...this.state,
      ...patch,
    };
    this.onChange(this.state);
  }

  async initialize(): Promise<void> {
    if (this.disposed) {
      return;
    }
    await Promise.all([
      this.refreshObservation({ mode: 'replace' }),
      this.refreshRegistry({ force: true }),
      this.refreshToolDiscovery(),
    ]);
  }

  setVisibility(visible: boolean, focused: boolean = this.state.focused): void {
    if (this.disposed) {
      return;
    }
    const becameVisible = !this.state.visible && visible;
    const becameFocused = !this.state.focused && focused && visible;
    this.updateState({ visible, focused });
    if (becameVisible || becameFocused) {
      void this.refreshObservation({ mode: 'refresh' });
    }
  }

  selectChat(chatKey: string | null): void {
    if (this.disposed || this.state.selectedChatKey === chatKey) {
      return;
    }
    this.updateState({
      selectedChatKey: chatKey,
      codexOpenError: null,
    });
  }

  setWindowHours(windowHours: JournalWindowHours): void {
    if (this.disposed || this.state.windowHours === windowHours) {
      return;
    }
    this.updateState({ windowHours });
  }

  setArchiveBeforeMs(archiveBeforeMs: number | null): void {
    if (this.disposed || this.state.archiveBeforeMs === archiveBeforeMs) {
      return;
    }
    this.updateState({ archiveBeforeMs });
  }

  setObservationScope(scope: ObservationScopeFilter | null): void {
    if (this.disposed || sameScope(this.state.observationScope, scope)) {
      return;
    }
    const nextGeneration = this.state.observationGeneration + 1;
    this.pendingObservationTicket = null;
    this.updateState({
      observationScope: scope,
      observationGeneration: nextGeneration,
      observationSnapshot: null,
      observationLoading: true,
      observationLoadingMore: false,
      observationError: null,
    });
    void this.refreshObservation({ mode: 'replace' });
  }

  async pollTick(): Promise<void> {
    if (!this.canPoll()) {
      return;
    }
    this.pollTickCounter += 1;
    const tasks: Promise<unknown>[] = [
      this.refreshObservation({ mode: 'refresh' }),
    ];
    // Slower registry refresh cadence (never every 2s observation tick, and never triggers version scan)
    if (
      this.pollTickCounter % REGISTRY_POLL_EVERY_N_TICKS === 0 &&
      this.registryInFlightCount === 0
    ) {
      tasks.push(this.refreshRegistry({ force: false }));
    }
    await Promise.all(tasks);
  }

  async refreshObservation(
    options: { mode?: ObservationFetchMode } = {}
  ): Promise<void> {
    if (this.disposed) {
      return;
    }
    const requestedMode: ObservationFetchMode =
      options.mode ??
      (this.state.observationSnapshot === null ? 'replace' : 'refresh');

    const ticket: ObservationTicket = {
      sequence: ++this.observationTicketSeq,
      generation: this.state.observationGeneration,
      mode: requestedMode,
      scope: this.state.observationScope,
      cursor: null,
    };

    if (this.inFlightObservationTicket !== null) {
      // Coalesce into at most one bounded pending ticket.
      const prevPending = this.pendingObservationTicket;
      const upgradedMode: ObservationFetchMode =
        prevPending?.mode === 'replace' || requestedMode === 'replace'
          ? 'replace'
          : 'refresh';
      this.pendingObservationTicket = {
        ...ticket,
        mode: upgradedMode,
      };
      return;
    }

    await this.executeObservationTicket(ticket);
  }

  async loadMoreHistory(): Promise<void> {
    if (this.disposed) {
      return;
    }
    const cursor = this.state.observationSnapshot?.next_cursor ?? null;
    if (!cursor) {
      return;
    }
    if (
      this.inFlightObservationTicket?.mode === 'more' &&
      this.inFlightObservationTicket.cursor === cursor
    ) {
      return;
    }
    if (
      this.pendingObservationTicket?.mode === 'more' &&
      this.pendingObservationTicket.cursor === cursor
    ) {
      return;
    }

    const ticket: ObservationTicket = {
      sequence: ++this.observationTicketSeq,
      generation: this.state.observationGeneration,
      mode: 'more',
      scope: this.state.observationScope,
      cursor,
    };

    if (this.inFlightObservationTicket !== null) {
      this.pendingObservationTicket = ticket;
      return;
    }

    await this.executeObservationTicket(ticket);
  }

  private isObservationTicketCurrent(ticket: ObservationTicket): boolean {
    if (this.disposed) {
      return false;
    }
    if (this.inFlightObservationTicket !== ticket) {
      return false;
    }
    if (ticket.generation !== this.state.observationGeneration) {
      return false;
    }
    if (!sameScope(ticket.scope, this.state.observationScope)) {
      return false;
    }
    return true;
  }

  private async executeObservationTicket(
    ticket: ObservationTicket
  ): Promise<void> {
    this.inFlightObservationTicket = ticket;
    const retainedSnapshot = this.state.observationSnapshot;
    const isSilentRefresh =
      ticket.mode === 'refresh' && retainedSnapshot !== null;

    if (ticket.mode === 'more') {
      this.updateState({
        observationLoadingMore: true,
        observationError: null,
      });
    } else if (!isSilentRefresh) {
      this.updateState({
        observationLoading: true,
        observationError: null,
      });
    }

    try {
      let partialPageError: string | null = null;
      let headSnapshot: ObservationSnapshot | null = null;

      const fetchPage = async (
        cursor: string | null,
        limit: number
      ): Promise<ObservationSnapshot> => {
        try {
          const page = await this.ports.fetchObservationSnapshot({
            source_id: ticket.scope?.source_id ?? null,
            job_id: ticket.scope?.job_id ?? null,
            cursor,
            limit,
          });
          if (cursor === null) {
            headSnapshot = page;
          }
          return page;
        } catch (err) {
          // If a later page in a multi-page window fails after the head page succeeded,
          // do NOT discard the healthy head page or its sources!
          if (cursor !== null && headSnapshot !== null) {
            partialPageError = formatErrorMessage(
              err,
              'Failed to load an additional observation history page.'
            );
            return {
              ...headSnapshot,
              requests: [],
              next_cursor: null,
              has_more: false,
              warnings: [
                `Additional history page could not be loaded: ${partialPageError}`,
              ],
            };
          }
          throw err;
        }
      };

      let fetchedSnapshot: ObservationSnapshot;
      if (ticket.mode === 'replace' || ticket.mode === 'refresh') {
        const currentLoadedCount =
          ticket.mode === 'refresh'
            ? (retainedSnapshot?.requests.length ?? 0)
            : 0;
        const target = Math.max(
          OBSERVATION_DEFAULT_PAGE_LIMIT,
          Math.min(currentLoadedCount, OBSERVATION_MAX_WINDOW_REQUESTS)
        );
        const result = await collectObservationWindow({
          target,
          fetcher: ({ cursor, limit }) => fetchPage(cursor, limit),
          shouldContinue: () => this.isObservationTicketCurrent(ticket),
        });
        if (result.aborted || !this.isObservationTicketCurrent(ticket)) {
          return;
        }
        if (!result.snapshot) {
          throw new Error('Observation paging returned no snapshot.');
        }
        fetchedSnapshot = result.snapshot;
      } else {
        fetchedSnapshot = await this.ports.fetchObservationSnapshot({
          source_id: ticket.scope?.source_id ?? null,
          job_id: ticket.scope?.job_id ?? null,
          cursor: ticket.cursor,
          limit: OBSERVATION_DEFAULT_PAGE_LIMIT,
        });
      }

      if (!this.isObservationTicketCurrent(ticket)) {
        return;
      }

      // Reject stale/out-of-order snapshot on background refresh when a newer snapshot is already present
      const currentSnapshot = this.state.observationSnapshot;
      if (
        ticket.mode === 'refresh' &&
        currentSnapshot !== null &&
        typeof fetchedSnapshot.generated_at_ms === 'number' &&
        typeof currentSnapshot.generated_at_ms === 'number' &&
        fetchedSnapshot.generated_at_ms < currentSnapshot.generated_at_ms
      ) {
        this.updateState({
          observationLoading: false,
          observationLoadingMore: false,
        });
        return;
      }

      let nextSnapshot = fetchedSnapshot;
      if (ticket.mode === 'more' && currentSnapshot !== null) {
        const mergedRequests = mergeScopedObservationRequests(
          currentSnapshot.requests,
          fetchedSnapshot.requests
        );
        const mergedJobs = mergeScopedObservationJobs(
          currentSnapshot.jobs,
          fetchedSnapshot.jobs
        );
        const mergedSources = mergeHealthyObservationSources(
          currentSnapshot.sources,
          fetchedSnapshot.sources
        );
        let mergedWarnings = mergeWarnings(
          currentSnapshot.warnings,
          fetchedSnapshot.warnings
        );
        if (mergedRequests.length > OBSERVATION_MAX_WINDOW_REQUESTS) {
          mergedWarnings = mergeWarnings(mergedWarnings, [
            `Loaded request history exceeds the ${OBSERVATION_MAX_WINDOW_REQUESTS}-request refresh window. Older loaded rows remain visible; the next refresh reloads the newest ${OBSERVATION_MAX_WINDOW_REQUESTS}.`,
          ]);
        }
        nextSnapshot = {
          ...fetchedSnapshot,
          sources: mergedSources,
          jobs: mergedJobs,
          requests: mergedRequests,
          warnings: mergedWarnings,
          jobs_has_more:
            currentSnapshot.jobs_has_more || fetchedSnapshot.jobs_has_more,
        };
      }

      // Resolve default chat selection if none selected yet, without stealing an existing user selection
      const projected = projectDesktopWorkspace(nextSnapshot, {
        nowMs: this.ports.now ? this.ports.now() : Date.now(),
        windowHours: this.state.windowHours,
        archiveBeforeMs: this.state.archiveBeforeMs,
        selectedChatKey: this.state.selectedChatKey,
        scope: this.state.observationScope,
      });

      this.updateState({
        observationSnapshot: nextSnapshot,
        observationLoading: false,
        observationLoadingMore: false,
        observationError: partialPageError,
        selectedChatKey: this.state.selectedChatKey ?? projected.selectedChatKey,
      });
    } catch (err) {
      if (!this.isObservationTicketCurrent(ticket)) {
        return;
      }
      const message = formatErrorMessage(
        err,
        'Failed to load observation snapshot.'
      );
      // Never clear an already loaded healthy snapshot when a later refresh or load-more fails
      this.updateState({
        observationLoading: false,
        observationLoadingMore: false,
        observationError: message,
      });
    } finally {
      if (this.inFlightObservationTicket === ticket) {
        this.inFlightObservationTicket = null;
        const nextPending = this.pendingObservationTicket;
        this.pendingObservationTicket = null;
        if (
          nextPending &&
          !this.disposed &&
          nextPending.generation === this.state.observationGeneration &&
          sameScope(nextPending.scope, this.state.observationScope)
        ) {
          void this.executeObservationTicket(nextPending);
        }
      }
    }
  }

  /**
   * Applies a registry snapshot from an external panel mutation (such as
   * `ProfilesPanel.onRegistryChanged(snapshot)`), rejecting stale revisions.
   */
  applyExternalRegistrySnapshot(snapshot: RegistrySnapshot): boolean {
    if (this.disposed) {
      return false;
    }
    const current = this.state.registry;
    if (
      current !== null &&
      this.hasLoadedRegistryOnce &&
      snapshot.revision < current.revision
    ) {
      return false;
    }
    const prevTools = current?.tools ?? [];
    let nextToolVersions = this.state.toolVersions;
    const toolsChanged =
      current !== null && toolInspectionInputsChanged(prevTools, snapshot.tools);
    if (toolsChanged) {
      nextToolVersions = markToolVersionInspectionsStale(
        nextToolVersions,
        snapshot.revision
      );
    }
    this.hasLoadedRegistryOnce = true;
    this.updateState({
      registry: snapshot,
      registryLoading: false,
      registryError: null,
      toolVersions: nextToolVersions,
    });

    if (toolsChanged) {
      this.queueToolVersionRecheck(snapshot.revision);
    }
    // Trigger a read-only registry confirmation refresh if not already in flight
    void this.refreshRegistry({ force: true });
    return true;
  }

  async refreshRegistry(
    options: { force?: boolean } = {}
  ): Promise<boolean> {
    if (this.disposed || !this.ports.fetchRegistrySnapshot) {
      if (!this.ports.fetchRegistrySnapshot && this.state.registryLoading) {
        this.updateState({ registryLoading: false });
      }
      return false;
    }
    if (this.registryInFlightCount > 0 && !options.force) {
      return false;
    }

    const ticketSeq = ++this.registryTicketSeq;
    this.registryInFlightCount += 1;

    if (!this.hasLoadedRegistryOnce) {
      this.updateState({
        registryLoading: true,
        registryError: null,
      });
    }

    try {
      const snapshot = await this.ports.fetchRegistrySnapshot();
      if (this.disposed || ticketSeq < this.registryTicketSeq) {
        return false;
      }
      const current = this.state.registry;
      if (
        current !== null &&
        this.hasLoadedRegistryOnce &&
        snapshot.revision < current.revision
      ) {
        // Reject stale registry snapshot revision
        this.updateState({ registryLoading: false });
        return false;
      }

      const prevTools = current?.tools ?? [];
      let nextToolVersions = this.state.toolVersions;
      if (
        this.hasLoadedRegistryOnce &&
        toolInspectionInputsChanged(prevTools, snapshot.tools)
      ) {
        nextToolVersions = markToolVersionInspectionsStale(
          nextToolVersions,
          snapshot.revision
        );
      }

      this.hasLoadedRegistryOnce = true;
      this.updateState({
        registry: snapshot,
        registryLoading: false,
        registryError: null,
        toolVersions: nextToolVersions,
      });

      this.startStartupToolVersionScan();
      this.maybeRunPendingToolVersionRecheck();
      return true;
    } catch (err) {
      if (this.disposed || ticketSeq < this.registryTicketSeq) {
        return false;
      }
      const message = formatErrorMessage(
        err,
        'Failed to load registry snapshot.'
      );
      this.updateState({
        registryLoading: false,
        registryError: message,
      });
      return false;
    } finally {
      this.registryInFlightCount = Math.max(0, this.registryInFlightCount - 1);
    }
  }

  private startStartupToolVersionScan(): void {
    if (
      this.disposed ||
      !this.ports.fetchToolVersions ||
      this.state.registry === null ||
      this.state.registryError !== null
    ) {
      return;
    }
    const started = beginStartupToolVersionScan(
      this.state.toolVersions,
      this.state.registry.revision
    );
    if (!started.ticket) {
      return;
    }
    this.updateState({ toolVersions: started.state });
    void this.executeToolVersionTicket(started.ticket);
  }

  recheckToolVersions(): void {
    this.startToolVersionScan('manual');
  }

  queueToolVersionRecheck(registryRevision: number): void {
    if (!Number.isSafeInteger(registryRevision) || registryRevision < 0) {
      return;
    }
    this.pendingToolVersionRecheckRevision = Math.max(
      this.pendingToolVersionRecheckRevision ?? 0,
      registryRevision
    );
    this.maybeRunPendingToolVersionRecheck();
  }

  private maybeRunPendingToolVersionRecheck(): void {
    const targetRev = this.pendingToolVersionRecheckRevision;
    if (targetRev === null || this.registryInFlightCount > 0) {
      return;
    }
    const currentRev = this.state.registry?.revision ?? -1;
    if (currentRev < targetRev) {
      if (this.state.registryError) {
        this.pendingToolVersionRecheckRevision = null;
        return;
      }
      void this.refreshRegistry({ force: true });
      return;
    }
    this.pendingToolVersionRecheckRevision = null;
    this.startToolVersionScan('after_save');
  }

  private startToolVersionScan(
    reason: Exclude<ToolVersionScanReason, 'startup'>
  ): void {
    if (this.disposed || !this.ports.fetchToolVersions) {
      return;
    }
    const revision =
      this.state.registry?.revision ?? emptyRegistrySnapshot().revision;
    const started = beginToolVersionScan(
      this.state.toolVersions,
      revision,
      reason
    );
    this.updateState({ toolVersions: started.state });
    if (!started.ticket) {
      return;
    }
    void this.executeToolVersionTicket(started.ticket);
  }

  private async executeToolVersionTicket(
    ticket: ToolVersionTicket
  ): Promise<void> {
    if (!this.ports.fetchToolVersions) {
      return;
    }
    try {
      const response = await this.ports.fetchToolVersions();
      if (this.disposed) {
        return;
      }
      const currentRev = this.state.registry?.revision ?? ticket.registryRevision;
      const applied = applyToolVersionScanSuccess(
        this.state.toolVersions,
        ticket,
        response,
        currentRev
      );
      if (applied.state !== this.state.toolVersions) {
        this.updateState({ toolVersions: applied.state });
      }
    } catch (err) {
      if (this.disposed) {
        return;
      }
      const currentRev = this.state.registry?.revision ?? ticket.registryRevision;
      const applied = applyToolVersionScanError(
        this.state.toolVersions,
        ticket,
        formatErrorMessage(err, 'Failed to inspect installed tool versions.'),
        currentRev
      );
      if (applied.state !== this.state.toolVersions) {
        this.updateState({ toolVersions: applied.state });
      }
    }
  }

  async refreshToolDiscovery(): Promise<void> {
    if (this.disposed || !this.ports.fetchToolDiscovery) {
      return;
    }
    const seq = ++this.discoveryTicketSeq;
    this.updateState({
      discoveryLoading: true,
      discoveryError: null,
    });
    try {
      const tools = await this.ports.fetchToolDiscovery();
      if (this.disposed || seq !== this.discoveryTicketSeq) {
        return;
      }
      this.updateState({
        discoveredTools: tools,
        discoveryLoading: false,
        discoveryError: null,
      });
    } catch (err) {
      if (this.disposed || seq !== this.discoveryTicketSeq) {
        return;
      }
      this.updateState({
        discoveryLoading: false,
        discoveryError: formatErrorMessage(
          err,
          'Failed to check local PATH tools.'
        ),
      });
    }
  }

  async openCodexChat(target: CodexChatTarget): Promise<boolean> {
    if (this.disposed) {
      return false;
    }
    if (
      !target.source_id.trim() ||
      !isCanonicalUuid(target.thread_uuid)
    ) {
      this.updateState({
        codexOpenError:
          'Cannot open Codex chat: missing a verified canonical thread UUID.',
      });
      return false;
    }
    if (!this.ports.openCodexChat) {
      this.updateState({
        codexOpenError: 'Native desktop bridge is not connected.',
      });
      return false;
    }
    if (this.state.codexOpenInFlight) {
      return false;
    }

    this.updateState({
      codexOpenInFlight: true,
      codexOpenError: null,
    });

    try {
      await this.ports.openCodexChat({
        source_id: target.source_id.trim(),
        thread_uuid: target.thread_uuid.trim(),
        job_id: target.job_id?.trim() ? target.job_id.trim() : null,
      });
      if (!this.disposed) {
        this.updateState({
          codexOpenInFlight: false,
          codexOpenError: null,
        });
      }
      return true;
    } catch (err) {
      if (!this.disposed) {
        this.updateState({
          codexOpenInFlight: false,
          codexOpenError: formatErrorMessage(
            err,
            'Unable to open the selected conversation in Codex.'
          ),
        });
      }
      return false;
    }
  }

  clearCodexOpenError(): void {
    if (this.state.codexOpenError !== null) {
      this.updateState({ codexOpenError: null });
    }
  }
}
