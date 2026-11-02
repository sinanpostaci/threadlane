import { invoke } from '@tauri-apps/api/core';

import { hasTauriRuntime } from '../ipc.ts';
import {
  createBrowserPreviewCodexSetupReport,
  normalizeCodexSetupReport,
  type CodexSetupMode,
  type CodexSetupReport,
} from './types.ts';

function fallbackErrorReport(mode: CodexSetupMode): CodexSetupReport {
  return {
    state: 'error',
    mode,
    config_path: null,
    registered: false,
    config_modified: false,
    restart_required: false,
    pairing_required: true,
    error_code: 'ipc_error',
    message:
      'Unable to communicate with the Threadlane desktop backend to inspect Codex MCP setup.',
  };
}

/**
 * Fetches the current startup Codex MCP setup report once from the desktop backend.
 * In browser preview mode, returns an explicit non-registered preview status without simulating success.
 */
export async function fetchCodexSetupStatus(): Promise<CodexSetupReport> {
  if (!hasTauriRuntime()) {
    return createBrowserPreviewCodexSetupReport();
  }
  try {
    const raw = await invoke<unknown>('desktop_codex_setup_status');
    return normalizeCodexSetupReport(raw);
  } catch {
    return fallbackErrorReport('check');
  }
}

/**
 * Runs an explicit read-only recheck (`Check` mode) of `<codex_home>/config.toml`.
 */
export async function recheckCodexSetup(): Promise<CodexSetupReport> {
  if (!hasTauriRuntime()) {
    return createBrowserPreviewCodexSetupReport();
  }
  try {
    const raw = await invoke<unknown>('desktop_codex_setup_recheck');
    return normalizeCodexSetupReport(raw);
  } catch {
    return fallbackErrorReport('check');
  }
}

/**
 * Runs an explicit setup retry (`Apply` mode) of `mcp_servers.threadlane` in `<codex_home>/config.toml`.
 */
export async function applyCodexSetup(): Promise<CodexSetupReport> {
  if (!hasTauriRuntime()) {
    return createBrowserPreviewCodexSetupReport();
  }
  try {
    const raw = await invoke<unknown>('desktop_codex_setup_apply');
    return normalizeCodexSetupReport(raw);
  } catch {
    return fallbackErrorReport('apply');
  }
}
