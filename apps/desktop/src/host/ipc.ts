import { invoke } from '@tauri-apps/api/core';
import {
  AppIpcError,
  hasTauriRuntime,
  normalizeAppError,
} from '../registry/ipc.ts';
import type {
  DesktopApprovedSessionArgs,
  DesktopApprovedTaskArgs,
  DesktopStartApprovedSessionArgs,
  HostSessionSummary,
  TaskStateSnapshot,
} from './types.ts';

export * from './types.ts';

function requireApprovalId(raw: string): string {
  const approvalId = raw.trim();
  if (!approvalId || approvalId.length > 128) {
    throw new AppIpcError(
      'invalid_input',
      'A valid approved-session identifier is required.'
    );
  }
  return approvalId;
}

function requireTaskId(raw: string): string {
  const taskId = raw.trim();
  if (!taskId || taskId.length > 128) {
    throw new AppIpcError('invalid_input', 'A valid approved task identifier is required.');
  }
  return taskId;
}

function requireNativeBridge(): void {
  if (!hasTauriRuntime()) {
    throw new AppIpcError(
      'bridge_disconnected',
      'Approved local sessions require the native Tauri desktop bridge.'
    );
  }
}

function normalizeNotifyCodexToolId(raw: string | null | undefined): string | null {
  if (raw === undefined || raw === null) {
    return null;
  }
  const toolId = raw.trim();
  if (!toolId) {
    return null;
  }
  if (toolId.length > 128) {
    throw new AppIpcError(
      'invalid_input',
      'Selected Codex CLI notification tool identifier is invalid.'
    );
  }
  return toolId;
}

/**
 * Explicit operator action only. This command starts a dedicated host process
 * for the frozen approval; it does not start models, tasks, or prompts.
 *
 * `notifyCodexToolId` is an explicit per-start startup choice. Omitted or null
 * means off and never enables a notification policy implicitly.
 */
export async function desktop_start_approved_session(
  args: DesktopStartApprovedSessionArgs
): Promise<HostSessionSummary> {
  const approvalId = requireApprovalId(args.approvalId);
  const notifyCodexToolId = normalizeNotifyCodexToolId(args.notifyCodexToolId);
  requireNativeBridge();
  try {
    return await invoke<HostSessionSummary>('desktop_start_approved_session', {
      approvalId,
      notifyCodexToolId,
    });
  } catch (err) {
    throw normalizeAppError(
      err,
      'approved_session_start_failed',
      'Failed to start the approved local session.'
    );
  }
}

/** Read-only status query for the current frozen approval. */
export async function desktop_approved_session(
  args: DesktopApprovedSessionArgs
): Promise<HostSessionSummary> {
  const approvalId = requireApprovalId(args.approvalId);
  requireNativeBridge();
  try {
    return await invoke<HostSessionSummary>('desktop_approved_session', {
      approvalId,
    });
  } catch (err) {
    throw normalizeAppError(
      err,
      'approved_session_status_failed',
      'Failed to read approved local session status.'
    );
  }
}

/** Read-only task status query. Never dispatches or starts a task. */
export async function desktop_approved_task(
  args: DesktopApprovedTaskArgs
): Promise<TaskStateSnapshot> {
  const approvalId = requireApprovalId(args.approvalId);
  const taskId = requireTaskId(args.taskId);
  requireNativeBridge();
  try {
    return await invoke<TaskStateSnapshot>('desktop_approved_task', {
      approvalId,
      taskId,
    });
  } catch (err) {
    throw normalizeAppError(
      err,
      'approved_task_status_failed',
      'Failed to read approved task status.'
    );
  }
}
