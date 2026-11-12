import { invoke } from '@tauri-apps/api/core';
import { hasTauriRuntime } from '../ipc.ts';
import { normalizeAppError } from '../registry/ipc.ts';
import {
  OBSERVATION_SCHEMA_VERSION,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  WorkspaceContractError,
  parseWorkspaceSnapshot,
} from '../workspace-contract.ts';
import type {
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
  WorkspaceSnapshot,
} from './types.ts';

const BRIDGE_DISCONNECTED_MESSAGE =
  'Tauri desktop bridge is not connected in this browser window.';

function requireTauriBridge(): void {
  if (!hasTauriRuntime()) {
    throw normalizeAppError(
      BRIDGE_DISCONNECTED_MESSAGE,
      'bridge_disconnected',
      BRIDGE_DISCONNECTED_MESSAGE
    );
  }
}

function requireTauriInvokeBridge(): void {
  requireTauriBridge();
  const internals = (window as unknown as Record<string, unknown>).__TAURI_INTERNALS__;
  if (
    typeof internals !== 'object' ||
    internals === null ||
    typeof (internals as { invoke?: unknown }).invoke !== 'function'
  ) {
    throw normalizeAppError(
      BRIDGE_DISCONNECTED_MESSAGE,
      'bridge_disconnected',
      BRIDGE_DISCONNECTED_MESSAGE
    );
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isValidFiniteTimestamp(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
}

function isValidObservationEnvelope(value: unknown): value is Record<string, unknown> {
  if (!isPlainRecord(value)) {
    return false;
  }
  return (
    value.schema_version === OBSERVATION_SCHEMA_VERSION &&
    isValidFiniteTimestamp(value.generated_at_ms) &&
    Array.isArray(value.sources) &&
    Array.isArray(value.jobs) &&
    Array.isArray(value.requests) &&
    (value.warnings === undefined || Array.isArray(value.warnings))
  );
}

function parseValidatedWorkspaceSnapshot(raw: unknown): WorkspaceSnapshot {
  let candidate: unknown = raw;
  if (typeof raw === 'string') {
    try {
      candidate = JSON.parse(raw);
    } catch {
      return parseWorkspaceSnapshot(raw);
    }
  }

  if (!isPlainRecord(candidate)) {
    return parseWorkspaceSnapshot(candidate);
  }

  if (candidate.schema_version !== WORKSPACE_SNAPSHOT_SCHEMA_VERSION) {
    throw new WorkspaceContractError(
      'invalid_json',
      'WorkspaceSnapshot payload has an unsupported or missing schema_version.'
    );
  }

  if (!isValidFiniteTimestamp(candidate.generated_at_ms)) {
    throw new WorkspaceContractError(
      'invalid_json',
      'WorkspaceSnapshot payload requires a non-negative finite generated_at_ms.'
    );
  }

  if (candidate.observation !== undefined) {
    if (!isValidObservationEnvelope(candidate.observation)) {
      throw new WorkspaceContractError(
        'invalid_json',
        'WorkspaceSnapshot observation envelope is malformed.'
      );
    }
    if (
      (candidate.sampling_policy !== undefined && !isPlainRecord(candidate.sampling_policy)) ||
      (candidate.chats !== undefined && !Array.isArray(candidate.chats)) ||
      (candidate.native_samples !== undefined && !Array.isArray(candidate.native_samples)) ||
      (candidate.job_histories !== undefined && !Array.isArray(candidate.job_histories)) ||
      (candidate.messages !== undefined &&
        (!isPlainRecord(candidate.messages) ||
          (candidate.messages.items !== undefined && !Array.isArray(candidate.messages.items)))) ||
      (candidate.bounds !== undefined && !isPlainRecord(candidate.bounds))
    ) {
      throw new WorkspaceContractError(
        'invalid_json',
        'WorkspaceSnapshot top-level section has an invalid container type.'
      );
    }
  } else if (!isValidObservationEnvelope(candidate)) {
    throw new WorkspaceContractError(
      'invalid_json',
      'WorkspaceSnapshot payload is missing required observation fields.'
    );
  }

  return parseWorkspaceSnapshot(candidate);
}

export async function ipcWorkspaceSnapshot(
  request?: ObservationSnapshotRequest
): Promise<WorkspaceSnapshot> {
  requireTauriInvokeBridge();
  try {
    const raw = await invoke<unknown>('desktop_workspace_snapshot', { request });
    return parseValidatedWorkspaceSnapshot(raw);
  } catch (err) {
    throw normalizeAppError(
      err,
      'workspace_snapshot_failed',
      'Failed to load workspace snapshot.'
    );
  }
}

export async function ipcObservationSnapshot(
  request?: ObservationSnapshotRequest
): Promise<ObservationSnapshot> {
  requireTauriBridge();
  try {
    return await invoke<ObservationSnapshot>('desktop_observation_snapshot', { request });
  } catch (err) {
    throw normalizeAppError(
      err,
      'observation_snapshot_failed',
      'Failed to load observation snapshot.'
    );
  }
}

export async function ipcObservationSources(): Promise<ObservationSource[]> {
  requireTauriBridge();
  try {
    return await invoke<ObservationSource[]>('desktop_observation_sources');
  } catch (err) {
    throw normalizeAppError(
      err,
      'observation_sources_failed',
      'Failed to load observation sources.'
    );
  }
}

export async function ipcLinkObservationContext(contextPath: string): Promise<ObservationSource> {
  requireTauriBridge();
  try {
    return await invoke<ObservationSource>('desktop_link_observation_context', { contextPath });
  } catch (err) {
    throw normalizeAppError(
      err,
      'observation_link_failed',
      'Failed to link observation context.'
    );
  }
}
