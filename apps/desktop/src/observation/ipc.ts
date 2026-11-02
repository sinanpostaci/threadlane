import { invoke } from '@tauri-apps/api/core';
import { hasTauriRuntime } from '../ipc.ts';
import { normalizeAppError } from '../registry/ipc.ts';
import type {
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
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
