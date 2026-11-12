import type { CodexChatTarget } from './app-contract.ts';
import {
  isCanonicalUuid,
  projectDesktopWorkspace,
  projectWorkspaceSnapshot,
  type DesktopWorkspaceProjection,
  type WorkspaceProductionProjection,
} from './app-model.ts';
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
  DesktopToolVersionsSnapshot,
  RegistrySnapshot,
} from './registry/types.ts';
import {
  createUnavailableConfiguredSelection,
  DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
  DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER,
  DEFAULT_WORKSPACE_CHAT_TAB,
  formatWorkspaceChatIdentityKey,
  normalizeWorkspaceChatTab,
  normalizeWorkspaceSnapshot,
  parseWorkspaceSnapshot,
  readWorkspaceChatArchivePreferences,
  reconcileWorkspaceNativeSample,
  setWorkspaceChatArchivePreference,
  upgradeLegacyObservationSnapshot,
  validateCorrelationId,
  validateWorkspaceChatIdentity,
  writeWorkspaceChatArchivePreferences,
  type WorkspaceAvailabilityState,
  type WorkspaceChatArchivePreferenceMap,
  type WorkspaceChatIdentity,
  type WorkspaceChatLifecycle,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceChatSummary,
  type WorkspaceChatSurfaceTab,
  type WorkspaceNativeSample,
  type WorkspaceSnapshot,
} from './workspace-contract.ts';

export const OBSERVATION_POLL_INTERVAL_MS = 2_000;
export const REGISTRY_POLL_EVERY_N_TICKS = 15;

const NIL_UUID_LOWER = '00000000-0000-0000-0000-000000000000';
const CANONICAL_CHAT_KEY_PREFIX = 'wchat:v1:';
const CHAT_KEY_ENCODER = new TextEncoder();
const CHAT_KEY_DECODER = new TextDecoder('utf-8', { fatal: true });

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

export type ObservationFetchMode = 'replace' | 'refresh';

export interface DesktopCoordinatorPorts {
  fetchWorkspaceSnapshot?: (
    request?: ObservationSnapshotRequest,
  ) => Promise<WorkspaceSnapshot>;
  /**
   * Explicit compatibility port for legacy observation-only test harnesses.
   * Production always prefers `fetchWorkspaceSnapshot`. When this fallback is
   * used, `upgradeLegacyObservationSnapshot` converts the payload without
   * fabricating native samples, acceptance proofs, or safe message summaries.
   */
  fetchObservationSnapshot?: (
    request?: ObservationSnapshotRequest,
  ) => Promise<ObservationSnapshot>;
  fetchRegistrySnapshot?: () => Promise<RegistrySnapshot>;
  fetchToolVersions?: () => Promise<DesktopToolVersionsSnapshot>;
  fetchToolDiscovery?: () => Promise<ToolDiscoveryEntry[]>;
  openCodexChat?: (target: CodexChatTarget) => Promise<void>;
  preferenceStorage?: WorkspaceChatPreferenceStorage | null;
  now?: () => number;
}

export interface DesktopCoordinatorOptions {
  selectedChatKey?: string | null;
  selectedTab?: WorkspaceChatSurfaceTab;
  workspaceLifecycle?: WorkspaceChatLifecycle;
  preferenceStorage?: WorkspaceChatPreferenceStorage | null;
  visible?: boolean;
  focused?: boolean;
}

export interface DesktopCoordinatorState {
  visible: boolean;
  focused: boolean;
  workspaceSnapshot: WorkspaceSnapshot | null;
  observationSnapshot: ObservationSnapshot | null;
  observationLoading: boolean;
  observationError: string | null;
  observationScope: ObservationScopeFilter | null;
  observationGeneration: number;
  selectedChatKey: string | null;
  selectedTab: WorkspaceChatSurfaceTab;
  workspaceLifecycle: WorkspaceChatLifecycle;
  archivePreferences: WorkspaceChatArchivePreferenceMap;
  archiveError: string | null;
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
  triggeredByVisibility: boolean;
}

interface CodexOpenTicket {
  sequence: number;
  generation: number;
  selectedChatKey: string | null;
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

function normalizeScopeFilter(
  scope: ObservationScopeFilter | null | undefined,
): ObservationScopeFilter | null {
  if (!scope) {
    return null;
  }
  const sourceId = scope.source_id.trim();
  const jobId = scope.job_id.trim();
  if (sourceId.length === 0 || jobId.length === 0) {
    return null;
  }
  return {
    source_id: sourceId,
    job_id: jobId,
  };
}

function sameScope(
  left: ObservationScopeFilter | null,
  right: ObservationScopeFilter | null,
): boolean {
  return (
    (left?.source_id ?? null) === (right?.source_id ?? null) &&
    (left?.job_id ?? null) === (right?.job_id ?? null)
  );
}

function normalizeWorkspaceLifecycle(
  value: unknown,
): WorkspaceChatLifecycle {
  return value === 'archived'
    ? 'archived'
    : DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER;
}

function resolveDefaultPreferenceStorage(): WorkspaceChatPreferenceStorage | null {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    const storage = window.localStorage;
    if (
      storage &&
      typeof storage.getItem === 'function' &&
      typeof storage.setItem === 'function'
    ) {
      return storage;
    }
    return null;
  } catch {
    return null;
  }
}

function isForbiddenIdentitySubstitute(value: string): boolean {
  const lower = value.toLowerCase();
  return (
    lower.startsWith('job_') ||
    lower.startsWith('job-') ||
    lower.startsWith('job:') ||
    lower.startsWith('plan_') ||
    lower.startsWith('plan-') ||
    lower.startsWith('plan:') ||
    lower.startsWith('sess_') ||
    lower.startsWith('sess-') ||
    lower.startsWith('sess:') ||
    lower.startsWith('session_') ||
    lower.startsWith('session-') ||
    lower.startsWith('session:') ||
    lower.startsWith('sessionid')
  );
}

/**
 * Parses and validates a canonical four-part workspace chat key
 * (`wchat:v1:<len>:<source_id>:<len>:<binding_id>:<len>:<thread_uuid>:<len>:<canonical_project_scope>`).
 * Non-canonical keys (titles, job IDs, session IDs, malformed UUIDs) return `null`.
 */
export function parseWorkspaceChatIdentityKey(
  rawKey: string | null | undefined,
): WorkspaceChatIdentity | null {
  if (typeof rawKey !== 'string') {
    return null;
  }
  const trimmed = rawKey.trim();
  if (!trimmed.startsWith(CANONICAL_CHAT_KEY_PREFIX)) {
    return null;
  }

  const bytes = CHAT_KEY_ENCODER.encode(trimmed);
  let offset = CANONICAL_CHAT_KEY_PREFIX.length;
  const parts: string[] = [];

  for (let idx = 0; idx < 4; idx += 1) {
    let lengthStr = '';
    while (offset < bytes.byteLength && bytes[offset] !== 0x3a) {
      const byte = bytes[offset]!;
      if (byte < 0x30 || byte > 0x39 || lengthStr.length >= 4) {
        return null;
      }
      lengthStr += String.fromCharCode(byte);
      offset += 1;
    }
    if (
      lengthStr.length === 0 ||
      offset >= bytes.byteLength ||
      bytes[offset] !== 0x3a
    ) {
      return null;
    }
    offset += 1; // consume ':'

    const fieldByteLen = Number.parseInt(lengthStr, 10);
    if (
      !Number.isSafeInteger(fieldByteLen) ||
      fieldByteLen <= 0 ||
      offset + fieldByteLen > bytes.byteLength
    ) {
      return null;
    }

    let decodedField: string;
    try {
      decodedField = CHAT_KEY_DECODER.decode(
        bytes.subarray(offset, offset + fieldByteLen),
      );
    } catch {
      return null;
    }
    parts.push(decodedField);
    offset += fieldByteLen;

    if (idx < 3) {
      if (offset >= bytes.byteLength || bytes[offset] !== 0x3a) {
        return null;
      }
      offset += 1; // consume separator ':'
    } else if (offset !== bytes.byteLength) {
      return null;
    }
  }

  try {
    const validated = validateWorkspaceChatIdentity({
      source_id: parts[0],
      binding_id: parts[1],
      thread_uuid: parts[2],
      canonical_project_scope: parts[3],
    });
    return formatWorkspaceChatIdentityKey(validated) === trimmed
      ? validated
      : null;
  } catch {
    return null;
  }
}

function normalizeSelectedChatKeyOption(
  rawKey: string | null | undefined,
): string | null {
  if (rawKey === null || rawKey === undefined) {
    return null;
  }
  const parsed = parseWorkspaceChatIdentityKey(rawKey);
  return parsed ? formatWorkspaceChatIdentityKey(parsed) : null;
}

function degradeNativeSampleToStalePreservingAge(
  sample: WorkspaceNativeSample,
): WorkspaceNativeSample {
  if (sample.availability !== 'available') {
    return sample;
  }
  const nextAvailability: WorkspaceAvailabilityState =
    sample.last_success_at_ms !== null ? 'stale' : 'unavailable';
  return {
    ...sample,
    // Producer timestamps stay untouched so failed reads never reset sample age.
    availability: nextAvailability,
    activity: 'unknown',
    connection: {
      ...sample.connection,
      state:
        nextAvailability === 'stale' && sample.connection.state === 'connected'
          ? 'degraded'
          : nextAvailability === 'unavailable' &&
              sample.connection.state === 'connected'
            ? 'unavailable'
            : sample.connection.state,
      freshness:
        nextAvailability === 'stale' ? 'stale' : sample.connection.freshness,
    },
    configured_selection: createUnavailableConfiguredSelection(),
  };
}

function degradeChatSummaryToStale(
  chat: WorkspaceChatSummary,
  hasPriorSuccessSample: boolean,
): WorkspaceChatSummary {
  if (chat.availability !== 'available') {
    return chat;
  }
  const nextAvailability: WorkspaceAvailabilityState =
    hasPriorSuccessSample || chat.updated_at_ms !== null ? 'stale' : 'unavailable';
  return {
    ...chat,
    availability: nextAvailability,
    activity: 'unknown',
    configured_selection: createUnavailableConfiguredSelection(),
  };
}

/**
 * Degrades retained `'available'` native samples and chat summaries when a
 * snapshot read fails, withholding live working/model claims immediately
 * without resetting `observed_at_ms` or `last_success_at_ms`.
 */
function markSnapshotStaleAfterReadFailure(
  snapshot: WorkspaceSnapshot,
): WorkspaceSnapshot {
  const sampleHasSuccessByKey = new Map<string, boolean>();
  const nextSamples = snapshot.native_samples.map((sample) => {
    const chatKey = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (chatKey !== null) {
      sampleHasSuccessByKey.set(chatKey, sample.last_success_at_ms !== null);
    }
    return degradeNativeSampleToStalePreservingAge(sample);
  });

  const nextChats = snapshot.chats.map((chat) => {
    const chatKey = formatWorkspaceChatIdentityKey(chat.identity);
    const hasPriorSuccess =
      chatKey !== null ? (sampleHasSuccessByKey.get(chatKey) ?? false) : false;
    return degradeChatSummaryToStale(chat, hasPriorSuccess);
  });

  return {
    ...snapshot,
    chats: nextChats,
    native_samples: nextSamples,
  };
}

/**
 * Ensures any chat or native sample belonging to an observation source whose
 * current health is not `'available'` cannot claim live `'available'` native
 * working/model state, while preserving `observed_at_ms` and `last_success_at_ms`.
 */
function reconcileSnapshotSourceHealth(
  snapshot: WorkspaceSnapshot,
): WorkspaceSnapshot {
  const unavailableSourceIds = new Set<string>();
  for (const source of snapshot.observation.sources) {
    if (source.availability !== 'available') {
      unavailableSourceIds.add(source.id);
    }
  }
  if (unavailableSourceIds.size === 0) {
    return snapshot;
  }

  let changed = false;
  const sampleHasSuccessByKey = new Map<string, boolean>();
  const nextSamples = snapshot.native_samples.map((sample) => {
    const chatKey = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (chatKey !== null) {
      sampleHasSuccessByKey.set(chatKey, sample.last_success_at_ms !== null);
    }
    if (
      unavailableSourceIds.has(sample.chat_identity.source_id) &&
      sample.availability === 'available'
    ) {
      changed = true;
      return degradeNativeSampleToStalePreservingAge(sample);
    }
    return sample;
  });

  const nextChats = snapshot.chats.map((chat) => {
    if (
      unavailableSourceIds.has(chat.identity.source_id) &&
      chat.availability === 'available'
    ) {
      changed = true;
      const chatKey = formatWorkspaceChatIdentityKey(chat.identity);
      const hasPriorSuccess =
        chatKey !== null ? (sampleHasSuccessByKey.get(chatKey) ?? false) : false;
      return degradeChatSummaryToStale(chat, hasPriorSuccess);
    }
    return chat;
  });

  if (!changed) {
    return snapshot;
  }
  return {
    ...snapshot,
    chats: nextChats,
    native_samples: nextSamples,
  };
}

/**
 * Guards against regressed per-chat native sample sequence/epoch values on
 * background refreshes while allowing unchanged sidecar sequences through.
 */
function reconcileRefreshNativeSamples(
  current: WorkspaceSnapshot | null,
  incoming: WorkspaceSnapshot,
): WorkspaceSnapshot {
  if (current === null || current.native_samples.length === 0) {
    return incoming;
  }

  const currentByChatKey = new Map<string, WorkspaceNativeSample>();
  for (const sample of current.native_samples) {
    const key = formatWorkspaceChatIdentityKey(sample.chat_identity);
    if (key !== null) {
      currentByChatKey.set(key, sample);
    }
  }

  let replacedRegressed = false;
  const reconciledSamples = incoming.native_samples.map((nextSample) => {
    const key = formatWorkspaceChatIdentityKey(nextSample.chat_identity);
    if (key === null) {
      return nextSample;
    }
    const prevSample = currentByChatKey.get(key);
    if (!prevSample) {
      return nextSample;
    }
    const decision = reconcileWorkspaceNativeSample(prevSample, nextSample);
    if (
      decision === 'rejected_stale_sequence' ||
      decision === 'rejected_stale_epoch' ||
      decision === 'rejected_epoch_process_mismatch' ||
      decision === 'rejected_invalid_sample'
    ) {
      replacedRegressed = true;
      return prevSample;
    }
    return nextSample;
  });

  if (!replacedRegressed) {
    return incoming;
  }
  return normalizeWorkspaceSnapshot({
    ...incoming,
    native_samples: reconciledSamples,
  });
}

/**
 * Merges observation requests using source-scoped keys (`source_id\0id`) so
 * identical request IDs from different observation sources never collide.
 * Retained for non-production / legacy paging helpers.
 */
export function mergeScopedObservationRequests(
  current: readonly ObservationRequest[],
  incoming: readonly ObservationRequest[],
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
 * Retained for non-production / legacy paging helpers.
 */
export function mergeScopedObservationJobs(
  current: readonly ObservationJob[],
  incoming: readonly ObservationJob[],
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
 * Preserves healthy sources from the head page when merging legacy pages.
 * Retained for non-production / legacy paging helpers.
 */
export function mergeHealthyObservationSources(
  headSources: readonly ObservationSource[],
  laterSources: readonly ObservationSource[],
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

export class DesktopAppCoordinator {
  private readonly ports: DesktopCoordinatorPorts;
  private readonly onChange: (state: DesktopCoordinatorState) => void;
  private readonly preferenceStorage: WorkspaceChatPreferenceStorage | null;
  private state: DesktopCoordinatorState;
  private disposed = false;

  private observationTicketSeq = 0;
  private inFlightObservationTicket: ObservationTicket | null = null;
  private pendingObservationTicket: ObservationTicket | null = null;

  private codexOpenTicketSeq = 0;
  private inFlightCodexOpenTicket: CodexOpenTicket | null = null;

  private registryTicketSeq = 0;
  private registryInFlightCount = 0;
  private hasLoadedRegistryOnce = false;
  private pendingToolVersionRecheckRevision: number | null = null;

  private discoveryTicketSeq = 0;
  private pollTickCounter = 0;

  constructor(
    ports: DesktopCoordinatorPorts,
    onChange: (state: DesktopCoordinatorState) => void = () => {},
    initialOptions: DesktopCoordinatorOptions = {},
  ) {
    this.ports = ports;
    this.onChange = onChange;
    this.preferenceStorage =
      initialOptions.preferenceStorage !== undefined
        ? initialOptions.preferenceStorage
        : ports.preferenceStorage !== undefined
          ? ports.preferenceStorage
          : resolveDefaultPreferenceStorage();

    const initialArchivePreferences = readWorkspaceChatArchivePreferences(
      this.preferenceStorage,
    );

    this.state = {
      visible: initialOptions.visible ?? true,
      focused: initialOptions.focused ?? true,
      workspaceSnapshot: null,
      observationSnapshot: null,
      observationLoading: true,
      observationError: null,
      observationScope: null,
      observationGeneration: 0,
      selectedChatKey: normalizeSelectedChatKeyOption(
        initialOptions.selectedChatKey,
      ),
      selectedTab:
        initialOptions.selectedTab !== undefined
          ? normalizeWorkspaceChatTab(initialOptions.selectedTab)
          : DEFAULT_WORKSPACE_CHAT_TAB,
      workspaceLifecycle: normalizeWorkspaceLifecycle(
        initialOptions.workspaceLifecycle,
      ),
      archivePreferences: initialArchivePreferences,
      archiveError: null,
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

  private nowMs(): number {
    return this.ports.now ? this.ports.now() : Date.now();
  }

  getState(): DesktopCoordinatorState {
    return this.state;
  }

  /**
   * Projects the current `WorkspaceSnapshot` through the pure production
   * `projectWorkspaceSnapshot` model with canonical chat selection, surface
   * tabs (`messages` / `jobs` / `activity`), and `active` / `archived` lifecycle.
   */
  projectWorkspace(nowMs?: number): WorkspaceProductionProjection {
    const currentNow = nowMs ?? this.nowMs();
    const snapshot =
      this.state.workspaceSnapshot !== null &&
      this.state.observationError !== null
        ? markSnapshotStaleAfterReadFailure(this.state.workspaceSnapshot)
        : this.state.workspaceSnapshot;

    return projectWorkspaceSnapshot(snapshot, {
      nowMs: currentNow,
      lifecycleFilter: this.state.workspaceLifecycle,
      archivePreferences: this.state.archivePreferences,
      selectedChatKey: this.state.selectedChatKey,
      selectedTab: this.state.selectedTab,
      activityJobId: this.state.observationScope?.job_id ?? null,
      loading: this.state.observationLoading,
      error: this.state.observationError,
    });
  }

  /**
   * Legacy observation journal projection kept explicitly separate for
   * non-production callers so `projectWorkspace` is unambiguous.
   */
  projectLegacyWorkspace(nowMs?: number): DesktopWorkspaceProjection {
    const currentNow = nowMs ?? this.nowMs();
    return projectDesktopWorkspace(this.state.observationSnapshot, {
      nowMs: currentNow,
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

  isWorkspaceInFlight(): boolean {
    return this.inFlightObservationTicket !== null;
  }

  dispose(): void {
    this.disposed = true;
    this.inFlightObservationTicket = null;
    this.pendingObservationTicket = null;
    this.inFlightCodexOpenTicket = null;
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

  private resolveFirstVisibleChatKey(
    snapshot: WorkspaceSnapshot | null,
    lifecycle: WorkspaceChatLifecycle,
    archivePreferences: WorkspaceChatArchivePreferenceMap,
  ): {
    firstVisibleKey: string | null;
    visibleKeys: Set<string>;
    allKnownKeys: Set<string>;
  } {
    if (snapshot === null) {
      return {
        firstVisibleKey: null,
        visibleKeys: new Set<string>(),
        allKnownKeys: new Set<string>(),
      };
    }

    const projected = projectWorkspaceSnapshot(snapshot, {
      nowMs: this.nowMs(),
      lifecycleFilter: lifecycle,
      archivePreferences,
      selectedChatKey: null,
      selectedTab: this.state.selectedTab,
      activityJobId: this.state.observationScope?.job_id ?? null,
      loading: false,
      error: null,
    });

    const visibleKeys = new Set<string>();
    for (const group of projected.visibleProjects) {
      for (const chat of group.chats) {
        visibleKeys.add(chat.chatKey);
      }
    }

    const allKnownKeys = new Set<string>();
    for (const group of [
      ...projected.activeProjects,
      ...projected.archivedProjects,
    ]) {
      for (const chat of group.chats) {
        allKnownKeys.add(chat.chatKey);
      }
    }

    return {
      firstVisibleKey: projected.visibleProjects[0]?.chats[0]?.chatKey ?? null,
      visibleKeys,
      allKnownKeys,
    };
  }

  async initialize(): Promise<void> {
    if (this.disposed) {
      return;
    }
    await Promise.all([
      this.refreshWorkspace({ mode: 'replace' }),
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
    if (!visible) {
      // Do not run queued background follow-up reads once the window hides.
      this.pendingObservationTicket = null;
    }
    this.updateState({ visible, focused });
    if (becameVisible || becameFocused) {
      if (
        this.inFlightObservationTicket !== null &&
        this.inFlightObservationTicket.triggeredByVisibility &&
        this.inFlightObservationTicket.generation ===
          this.state.observationGeneration &&
        sameScope(
          this.inFlightObservationTicket.scope,
          this.state.observationScope,
        )
      ) {
        return;
      }
      const mode: ObservationFetchMode =
        this.state.workspaceSnapshot === null ? 'replace' : 'refresh';
      void this.enqueueWorkspaceRefresh(mode, true);
    }
  }

  selectChat(chatKey: string | null): void {
    if (this.disposed) {
      return;
    }

    let nextChatKey: string | null;
    if (chatKey === null) {
      nextChatKey = null;
    } else if (typeof chatKey === 'string' && chatKey.trim().length === 0) {
      nextChatKey = null;
    } else {
      const parsed = parseWorkspaceChatIdentityKey(chatKey);
      if (parsed === null) {
        // Reject non-canonical chat identifiers (titles, job IDs, session IDs).
        return;
      }
      nextChatKey = formatWorkspaceChatIdentityKey(parsed);
    }

    if (this.state.selectedChatKey === nextChatKey) {
      return;
    }

    const nextGeneration = this.state.observationGeneration + 1;
    this.inFlightCodexOpenTicket = null;

    if (this.inFlightObservationTicket !== null) {
      if (this.state.visible) {
        const upgradedMode: ObservationFetchMode =
          this.state.workspaceSnapshot === null ||
          this.inFlightObservationTicket.mode === 'replace' ||
          this.pendingObservationTicket?.mode === 'replace'
            ? 'replace'
            : 'refresh';
        this.pendingObservationTicket = {
          sequence: ++this.observationTicketSeq,
          generation: nextGeneration,
          mode: upgradedMode,
          scope: this.state.observationScope,
          triggeredByVisibility: false,
        };
      } else {
        this.pendingObservationTicket = null;
      }
    }

    this.updateState({
      selectedChatKey: nextChatKey,
      observationGeneration: nextGeneration,
      archiveError: null,
      codexOpenInFlight: false,
      codexOpenError: null,
    });
  }

  setWorkspaceTab(tab: WorkspaceChatSurfaceTab): void {
    if (this.disposed) {
      return;
    }
    const normalizedTab = normalizeWorkspaceChatTab(tab);
    if (this.state.selectedTab === normalizedTab) {
      return;
    }
    this.updateState({ selectedTab: normalizedTab });
  }

  setWorkspaceLifecycle(lifecycle: WorkspaceChatLifecycle): void {
    if (this.disposed) {
      return;
    }
    const nextLifecycle = normalizeWorkspaceLifecycle(lifecycle);
    if (this.state.workspaceLifecycle === nextLifecycle) {
      return;
    }

    let nextSelectedChatKey = this.state.selectedChatKey;
    if (this.state.workspaceSnapshot !== null) {
      const { firstVisibleKey, visibleKeys } = this.resolveFirstVisibleChatKey(
        this.state.workspaceSnapshot,
        nextLifecycle,
        this.state.archivePreferences,
      );
      if (
        nextSelectedChatKey === null ||
        !visibleKeys.has(nextSelectedChatKey)
      ) {
        nextSelectedChatKey = firstVisibleKey;
      }
    } else if (nextSelectedChatKey !== null) {
      const entry = this.state.archivePreferences[nextSelectedChatKey];
      const currentKeyLifecycle: WorkspaceChatLifecycle =
        entry?.lifecycle === 'archived' ? 'archived' : 'active';
      if (currentKeyLifecycle !== nextLifecycle) {
        nextSelectedChatKey = null;
      }
    }

    const chatChanged = nextSelectedChatKey !== this.state.selectedChatKey;
    if (chatChanged) {
      this.inFlightCodexOpenTicket = null;
    }

    this.updateState({
      workspaceLifecycle: nextLifecycle,
      selectedChatKey: nextSelectedChatKey,
      archiveError: null,
      ...(chatChanged
        ? {
            codexOpenInFlight: false,
            codexOpenError: null,
          }
        : {}),
    });
  }

  setChatArchived(
    identity: WorkspaceChatIdentity,
    archived: boolean,
  ): boolean {
    if (this.disposed) {
      return false;
    }

    let validatedIdentity: WorkspaceChatIdentity;
    try {
      validatedIdentity = validateWorkspaceChatIdentity(identity);
    } catch {
      this.updateState({
        archiveError:
          'Cannot update archive preference: invalid canonical chat identity.',
      });
      return false;
    }

    const targetChatKey = formatWorkspaceChatIdentityKey(validatedIdentity);
    if (targetChatKey === null) {
      this.updateState({
        archiveError:
          'Cannot update archive preference: invalid canonical chat identity.',
      });
      return false;
    }

    const targetLifecycle: WorkspaceChatLifecycle = archived
      ? 'archived'
      : 'active';
    const nextPreferences = setWorkspaceChatArchivePreference(
      this.state.archivePreferences,
      validatedIdentity,
      targetLifecycle,
      this.nowMs(),
    );

    const persisted = writeWorkspaceChatArchivePreferences(
      this.preferenceStorage,
      nextPreferences,
    );
    if (!persisted) {
      this.updateState({
        archiveError: archived
          ? 'Unable to archive chat because preference storage could not be updated.'
          : 'Unable to restore chat because preference storage could not be updated.',
      });
      return false;
    }

    let nextSelectedChatKey = this.state.selectedChatKey;
    if (
      nextSelectedChatKey === targetChatKey &&
      targetLifecycle !== this.state.workspaceLifecycle
    ) {
      if (this.state.workspaceSnapshot !== null) {
        const { firstVisibleKey } = this.resolveFirstVisibleChatKey(
          this.state.workspaceSnapshot,
          this.state.workspaceLifecycle,
          nextPreferences,
        );
        nextSelectedChatKey = firstVisibleKey;
      } else {
        nextSelectedChatKey = null;
      }
    } else if (
      nextSelectedChatKey === null &&
      targetLifecycle === this.state.workspaceLifecycle
    ) {
      nextSelectedChatKey = targetChatKey;
    }

    const chatChanged = nextSelectedChatKey !== this.state.selectedChatKey;
    if (chatChanged) {
      this.inFlightCodexOpenTicket = null;
    }

    this.updateState({
      archivePreferences: nextPreferences,
      archiveError: null,
      selectedChatKey: nextSelectedChatKey,
      ...(chatChanged
        ? {
            codexOpenInFlight: false,
            codexOpenError: null,
          }
        : {}),
    });
    return true;
  }

  clearArchiveError(): void {
    if (this.state.archiveError !== null) {
      this.updateState({ archiveError: null });
    }
  }

  setObservationScope(scope: ObservationScopeFilter | null): void {
    if (this.disposed) {
      return;
    }
    const normalizedScope = normalizeScopeFilter(scope);
    if (sameScope(this.state.observationScope, normalizedScope)) {
      return;
    }

    const nextGeneration = this.state.observationGeneration + 1;
    this.pendingObservationTicket = null;
    this.inFlightCodexOpenTicket = null;

    let nextSelectedChatKey = this.state.selectedChatKey;
    if (normalizedScope !== null && nextSelectedChatKey !== null) {
      const parsed = parseWorkspaceChatIdentityKey(nextSelectedChatKey);
      if (parsed === null || parsed.source_id !== normalizedScope.source_id) {
        nextSelectedChatKey = null;
      }
    }

    this.updateState({
      observationScope: normalizedScope,
      observationGeneration: nextGeneration,
      workspaceSnapshot: null,
      observationSnapshot: null,
      observationLoading: true,
      observationError: null,
      selectedChatKey: nextSelectedChatKey,
      codexOpenInFlight: false,
      codexOpenError: null,
    });
    void this.refreshWorkspace({ mode: 'replace' });
  }

  async pollTick(): Promise<void> {
    if (!this.canPoll()) {
      return;
    }
    this.pollTickCounter += 1;
    const tasks: Promise<unknown>[] = [
      this.refreshWorkspace({ mode: 'refresh' }),
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

  async refreshWorkspace(
    options: { mode?: ObservationFetchMode } = {},
  ): Promise<void> {
    const requestedMode: ObservationFetchMode =
      options.mode ??
      (this.state.workspaceSnapshot === null ? 'replace' : 'refresh');
    await this.enqueueWorkspaceRefresh(requestedMode, false);
  }

  async refreshObservation(
    options: { mode?: ObservationFetchMode } = {},
  ): Promise<void> {
    await this.refreshWorkspace(options);
  }

  private async enqueueWorkspaceRefresh(
    requestedMode: ObservationFetchMode,
    triggeredByVisibility: boolean,
  ): Promise<void> {
    if (this.disposed) {
      return;
    }

    const ticket: ObservationTicket = {
      sequence: ++this.observationTicketSeq,
      generation: this.state.observationGeneration,
      mode: requestedMode,
      scope: this.state.observationScope,
      triggeredByVisibility,
    };

    if (this.inFlightObservationTicket !== null) {
      // Coalesce into at most one bounded pending ticket while preserving single-in-flight read.
      const prevPending = this.pendingObservationTicket;
      const upgradedMode: ObservationFetchMode =
        prevPending?.mode === 'replace' || requestedMode === 'replace'
          ? 'replace'
          : 'refresh';
      this.pendingObservationTicket = {
        ...ticket,
        mode: upgradedMode,
        triggeredByVisibility:
          Boolean(prevPending?.triggeredByVisibility) && triggeredByVisibility,
      };
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

  private async readBoundedWorkspaceSnapshot(
    request: ObservationSnapshotRequest,
  ): Promise<WorkspaceSnapshot> {
    if (this.ports.fetchWorkspaceSnapshot) {
      const raw = await this.ports.fetchWorkspaceSnapshot(request);
      return parseWorkspaceSnapshot(raw);
    }
    if (this.ports.fetchObservationSnapshot) {
      const legacy = await this.ports.fetchObservationSnapshot(request);
      return upgradeLegacyObservationSnapshot(legacy);
    }
    throw new Error('Workspace snapshot reader is not configured.');
  }

  private async executeObservationTicket(
    ticket: ObservationTicket,
  ): Promise<void> {
    this.inFlightObservationTicket = ticket;
    const retainedSnapshot = this.state.workspaceSnapshot;
    const isSilentRefresh =
      ticket.mode === 'refresh' && retainedSnapshot !== null;

    if (!isSilentRefresh) {
      this.updateState({
        observationLoading: true,
        observationError: null,
      });
    }

    try {
      const fetchedSnapshot = await this.readBoundedWorkspaceSnapshot({
        source_id: ticket.scope?.source_id ?? null,
        job_id: ticket.scope?.job_id ?? null,
        cursor: null,
        limit: DEFAULT_OBSERVATION_REQUEST_PAGE_LIMIT,
      });

      if (!this.isObservationTicketCurrent(ticket)) {
        return;
      }

      const currentSnapshot = this.state.workspaceSnapshot;
      if (
        ticket.mode === 'refresh' &&
        currentSnapshot !== null &&
        fetchedSnapshot.generated_at_ms < currentSnapshot.generated_at_ms
      ) {
        this.updateState({ observationLoading: false });
        return;
      }

      const sampleReconciled =
        ticket.mode === 'refresh'
          ? reconcileRefreshNativeSamples(currentSnapshot, fetchedSnapshot)
          : fetchedSnapshot;
      const nextSnapshot = reconcileSnapshotSourceHealth(sampleReconciled);

      const { firstVisibleKey, allKnownKeys } = this.resolveFirstVisibleChatKey(
        nextSnapshot,
        this.state.workspaceLifecycle,
        this.state.archivePreferences,
      );

      let nextSelectedChatKey = this.state.selectedChatKey;
      if (nextSelectedChatKey === null) {
        nextSelectedChatKey = firstVisibleKey;
      } else if (!allKnownKeys.has(nextSelectedChatKey)) {
        nextSelectedChatKey = firstVisibleKey;
      }

      const chatChanged = nextSelectedChatKey !== this.state.selectedChatKey;
      if (chatChanged) {
        this.inFlightCodexOpenTicket = null;
      }

      this.updateState({
        workspaceSnapshot: nextSnapshot,
        observationSnapshot: nextSnapshot.observation,
        observationLoading: false,
        observationError: null,
        selectedChatKey: nextSelectedChatKey,
        ...(chatChanged
          ? {
              codexOpenInFlight: false,
              codexOpenError: null,
            }
          : {}),
      });
    } catch (err) {
      if (!this.isObservationTicketCurrent(ticket)) {
        return;
      }
      const message = formatErrorMessage(
        err,
        'Failed to load workspace snapshot.',
      );
      const staleSnapshot =
        this.state.workspaceSnapshot !== null
          ? markSnapshotStaleAfterReadFailure(this.state.workspaceSnapshot)
          : null;

      this.updateState({
        workspaceSnapshot: staleSnapshot,
        observationSnapshot: staleSnapshot?.observation ?? null,
        observationLoading: false,
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
        } else if (!this.disposed && this.state.observationLoading) {
          this.updateState({ observationLoading: false });
        }
      }
    }
  }

  /**
   * Applies a registry snapshot from an external panel mutation (such as
   * `ProfilesPanel.onRegistryChanged(snapshot)`), rejecting stale revisions
   * while preserving dirty profile draft coordination in panel state.
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
        snapshot.revision,
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
    options: { force?: boolean } = {},
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
          snapshot.revision,
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
        'Failed to load registry snapshot.',
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
      this.state.registry.revision,
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
      registryRevision,
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
    reason: Exclude<ToolVersionScanReason, 'startup'>,
  ): void {
    if (this.disposed || !this.ports.fetchToolVersions) {
      return;
    }
    const revision =
      this.state.registry?.revision ?? emptyRegistrySnapshot().revision;
    const started = beginToolVersionScan(
      this.state.toolVersions,
      revision,
      reason,
    );
    this.updateState({ toolVersions: started.state });
    if (!started.ticket) {
      return;
    }
    void this.executeToolVersionTicket(started.ticket);
  }

  private async executeToolVersionTicket(
    ticket: ToolVersionTicket,
  ): Promise<void> {
    if (!this.ports.fetchToolVersions) {
      return;
    }
    try {
      const response = await this.ports.fetchToolVersions();
      if (this.disposed) {
        return;
      }
      const currentRev =
        this.state.registry?.revision ?? ticket.registryRevision;
      const applied = applyToolVersionScanSuccess(
        this.state.toolVersions,
        ticket,
        response,
        currentRev,
      );
      if (applied.state !== this.state.toolVersions) {
        this.updateState({ toolVersions: applied.state });
      }
    } catch (err) {
      if (this.disposed) {
        return;
      }
      const currentRev =
        this.state.registry?.revision ?? ticket.registryRevision;
      const applied = applyToolVersionScanError(
        this.state.toolVersions,
        ticket,
        formatErrorMessage(err, 'Failed to inspect installed tool versions.'),
        currentRev,
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
          'Failed to check local PATH tools.',
        ),
      });
    }
  }

  private validateVerifiedCodexTarget(
    target: CodexChatTarget,
  ): { ok: true; normalized: CodexChatTarget } | { ok: false; error: string } {
    const rawSourceId =
      typeof target.source_id === 'string' ? target.source_id.trim() : '';
    const rawThreadUuid =
      typeof target.thread_uuid === 'string' ? target.thread_uuid.trim() : '';
    const canonicalThreadUuid = rawThreadUuid.toLowerCase();

    if (
      rawSourceId.length === 0 ||
      rawSourceId.length > 128 ||
      !/^[A-Za-z0-9_.-]+$/.test(rawSourceId) ||
      isForbiddenIdentitySubstitute(rawSourceId) ||
      !isCanonicalUuid(rawThreadUuid) ||
      canonicalThreadUuid === NIL_UUID_LOWER
    ) {
      return {
        ok: false,
        error:
          'Cannot open Codex chat: missing a verified canonical source and thread UUID.',
      };
    }

    let cleanJobId: string | null = null;
    if (
      typeof target.job_id === 'string' &&
      target.job_id.trim().length > 0
    ) {
      const trimmedJobId = target.job_id.trim();
      const validatedJobId = validateCorrelationId(trimmedJobId);
      if (
        validatedJobId === null ||
        validatedJobId !== trimmedJobId ||
        trimmedJobId.includes('://')
      ) {
        return {
          ok: false,
          error:
            'Cannot open Codex chat: invalid job identifier for target conversation.',
        };
      }
      cleanJobId = validatedJobId;
    }

    if (this.state.workspaceSnapshot !== null) {
      const projected = this.projectWorkspace();
      const allVerifiedChats = [
        ...projected.activeProjects,
        ...projected.archivedProjects,
      ].flatMap((group) => group.chats);

      if (allVerifiedChats.length > 0) {
        const matchingChats = allVerifiedChats.filter(
          (chat) =>
            chat.identity.source_id === rawSourceId &&
            chat.identity.thread_uuid === canonicalThreadUuid,
        );
        if (matchingChats.length === 0) {
          return {
            ok: false,
            error:
              'Cannot open Codex chat: target does not match a verified workspace chat.',
          };
        }

        if (cleanJobId !== null) {
          for (const history of this.state.workspaceSnapshot.job_histories) {
            if (
              history.job.source_id === rawSourceId &&
              history.job.job_id === cleanJobId &&
              history.job.chat_identity !== null &&
              history.job.chat_identity.thread_uuid !== canonicalThreadUuid
            ) {
              return {
                ok: false,
                error:
                  'Cannot open Codex chat: job does not belong to the target thread.',
              };
            }
          }
          for (const obsJob of this.state.workspaceSnapshot.observation.jobs) {
            if (
              obsJob.source_id === rawSourceId &&
              obsJob.id === cleanJobId &&
              obsJob.thread_uuid !== null &&
              obsJob.thread_uuid.toLowerCase() !== canonicalThreadUuid
            ) {
              return {
                ok: false,
                error:
                  'Cannot open Codex chat: job does not belong to the target thread.',
              };
            }
          }
        }
      }
    }

    return {
      ok: true,
      normalized: {
        source_id: rawSourceId,
        thread_uuid: canonicalThreadUuid,
        job_id: cleanJobId,
      },
    };
  }

  private isCodexOpenTicketCurrent(ticket: CodexOpenTicket): boolean {
    return (
      !this.disposed &&
      this.inFlightCodexOpenTicket === ticket &&
      ticket.selectedChatKey === this.state.selectedChatKey &&
      ticket.generation === this.state.observationGeneration
    );
  }

  async openCodexChat(target: CodexChatTarget): Promise<boolean> {
    if (this.disposed) {
      return false;
    }

    // Coalesce duplicate clicks onto the single active in-flight send.
    if (this.inFlightCodexOpenTicket !== null || this.state.codexOpenInFlight) {
      return false;
    }

    const verified = this.validateVerifiedCodexTarget(target);
    if (!verified.ok) {
      this.updateState({
        codexOpenError: verified.error,
      });
      return false;
    }

    if (!this.ports.openCodexChat) {
      this.updateState({
        codexOpenError: 'Native desktop bridge is not connected.',
      });
      return false;
    }

    const ticket: CodexOpenTicket = {
      sequence: ++this.codexOpenTicketSeq,
      generation: this.state.observationGeneration,
      selectedChatKey: this.state.selectedChatKey,
    };
    this.inFlightCodexOpenTicket = ticket;

    this.updateState({
      codexOpenInFlight: true,
      codexOpenError: null,
    });

    try {
      await this.ports.openCodexChat(verified.normalized);
      if (!this.isCodexOpenTicketCurrent(ticket)) {
        return false;
      }
      this.inFlightCodexOpenTicket = null;
      this.updateState({
        codexOpenInFlight: false,
        codexOpenError: null,
      });
      return true;
    } catch (err) {
      if (!this.isCodexOpenTicketCurrent(ticket)) {
        return false;
      }
      this.inFlightCodexOpenTicket = null;
      this.updateState({
        codexOpenInFlight: false,
        codexOpenError: formatErrorMessage(
          err,
          'Unable to open the selected conversation in Codex.',
        ),
      });
      return false;
    }
  }

  clearCodexOpenError(): void {
    if (this.state.codexOpenError !== null) {
      this.updateState({ codexOpenError: null });
    }
  }
}
