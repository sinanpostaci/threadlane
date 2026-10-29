import { invoke } from '@tauri-apps/api/core';
import type {
  DesktopAnswerDemoArgs,
  DesktopAttachArgs,
  DesktopCancelDemoArgs,
  DesktopRunDetailArgs,
  DesktopSnapshot,
  DesktopSnapshotArgs,
  DesktopWindowActionArgs,
  RunDetail,
  TaskSummary,
  ToolStatus,
  WindowAction,
} from './types.ts';

export function hasTauriRuntime(): boolean {
  if (typeof window === 'undefined') {
    return false;
  }
  const win = window as unknown as Record<string, unknown>;
  return Boolean(win.__TAURI_INTERNALS__ || win.__TAURI__);
}

export function isDevPreviewFixtureMode(): boolean {
  if (hasTauriRuntime() || typeof window === 'undefined') {
    return false;
  }
  const isDev = Boolean(import.meta.env?.DEV);
  if (!isDev) {
    return false;
  }
  const params = new URLSearchParams(window.location.search);
  return params.get('preview') === '1';
}

export function normalizeIpcError(err: unknown): string {
  const raw =
    typeof err === 'string'
      ? err
      : err instanceof Error
        ? err.message
        : 'Desktop bridge request failed.';
  const cleaned = raw
    .replace(/bearer\s+[a-z0-9._-]+/gi, '[redacted]')
    .replace(/token[=:]\s*[^\s,;]+/gi, 'token=[redacted]')
    .trim();
  if (!cleaned) {
    return 'Desktop bridge request failed.';
  }
  return cleaned.length > 220 ? `${cleaned.slice(0, 217)}...` : cleaned;
}

function createBrowserDisconnectedSnapshot(): DesktopSnapshot {
  return {
    attachment: {
      mode: 'unconfigured',
      connection: 'disconnected',
      state_dir: null,
      message: 'Tauri desktop bridge is not connected in this browser window.',
      updated_at_ms: Date.now(),
    },
    bindings: [],
    selected_binding_id: null,
    tasks: [],
    total_runs: 0,
    active_runs: 0,
    has_more: false,
    demo_controls: {
      available: false,
      run_id: null,
      binding_id: null,
      can_answer: false,
      can_cancel: false,
      revision: null,
    },
  };
}

function createBrowserDisconnectedTools(): ToolStatus[] {
  return [
    {
      id: 'opencode',
      label: 'OpenCode',
      executable_path: null,
      availability: 'not_configured',
      note: 'Launch inside the Threadlane Tauri desktop app to check PATH availability.',
    },
    {
      id: 'antigravity',
      label: 'Antigravity',
      executable_path: null,
      availability: 'not_configured',
      note: 'Launch inside the Threadlane Tauri desktop app to check PATH availability.',
    },
    {
      id: 'codex',
      label: 'Codex',
      executable_path: null,
      availability: 'not_configured',
      note: 'Launch inside the Threadlane Tauri desktop app to check PATH availability.',
    },
    {
      id: 'git',
      label: 'Git',
      executable_path: null,
      availability: 'not_configured',
      note: 'Launch inside the Threadlane Tauri desktop app to check PATH availability.',
    },
    {
      id: 'jev',
      label: 'Jev',
      executable_path: null,
      availability: 'not_configured',
      note: 'Jev decision integration is not configured in Milestone 3A.',
    },
  ];
}

// Explicit DEV-only ?preview=1 fixture state (all English, never active unless ?preview=1 in DEV)
interface DevPreviewState {
  demoRev: number;
  demoStatus: string;
  demoCanAnswer: boolean;
  demoCanCancel: boolean;
  selectedBindingId: string;
}

const devPreview: DevPreviewState = {
  demoRev: 2,
  demoStatus: 'WaitingForAnswer',
  demoCanAnswer: true,
  demoCanCancel: true,
  selectedBindingId: 'binding-demo-local',
};

function getDevPreviewSnapshot(bindingId?: string | null): DesktopSnapshot {
  const now = Date.now();
  if (bindingId) {
    devPreview.selectedBindingId = bindingId;
  }
  const selected = devPreview.selectedBindingId;

  const demoTask: TaskSummary = {
    run_id: 'run-demo-01',
    binding_id: 'binding-demo-local',
    revision: devPreview.demoRev,
    status: devPreview.demoStatus,
    title: 'Local deterministic lifecycle verification',
    provider: 'codex',
    requested_model: null,
    requested_effort: null,
    observed_model: null,
    observed_effort: null,
    last_summary:
      devPreview.demoStatus === 'WaitingForAnswer'
        ? 'Awaiting deterministic Continue confirmation'
        : devPreview.demoStatus === 'Completed'
          ? 'Deterministic local demo completed cleanly'
          : 'Deterministic local demo stopped by operator',
    updated_at_ms: now - 18_000,
    needs_evidence: false,
    kind: 'demo',
    profile_id: 'local-demo',
  };

  const workerTasks: TaskSummary[] = [
    {
      run_id: 'run-worker-agy-01',
      binding_id: 'binding-runtime-core',
      revision: 4,
      status: 'Running',
      title: 'Scoped worktree diff gate review',
      provider: 'antigravity',
      requested_model: 'gemini-4-argon-high',
      requested_effort: 'high',
      observed_model: 'gemini-4-argon-high',
      observed_effort: 'high',
      last_summary: 'Inspecting isolated worktree diff against base revision',
      updated_at_ms: now - 12_000,
      needs_evidence: false,
      kind: 'worker',
      profile_id: 'default',
    },
    {
      run_id: 'run-worker-oc-02',
      binding_id: 'binding-runtime-core',
      revision: 3,
      status: 'ExecutionFailed',
      title: 'Bounded history pagination check',
      provider: 'opencode',
      requested_model: 'deepseek-4.1-flash',
      requested_effort: 'max',
      observed_model: 'deepseek-4.1-flash',
      observed_effort: null,
      last_summary: 'Provider effort telemetry missing from worker report',
      updated_at_ms: now - 240_000,
      needs_evidence: true,
      kind: 'worker',
      profile_id: 'default',
    },
  ];

  const tasks = selected === 'binding-demo-local' ? [demoTask] : workerTasks;
  const activeCount = tasks.filter(
    (t) => t.status === 'Running' || t.status === 'WaitingForAnswer'
  ).length;

  return {
    attachment: {
      mode: selected === 'binding-demo-local' ? 'demo' : 'runtime',
      connection: 'connected',
      state_dir: '/tmp/threadlane-dev-preview',
      message: 'DEV preview fixture (?preview=1) — not connected to a live supervisor.',
      updated_at_ms: now,
    },
    bindings: [
      {
        binding_id: 'binding-demo-local',
        thread_uuid: '00000000-0000-4000-8000-000000000001',
        label: 'Local demo',
        is_local_demo: true,
      },
      {
        binding_id: 'binding-runtime-core',
        thread_uuid: '00000000-0000-4000-8000-000000000002',
        label: 'Workspace Dispatch Gate',
        is_local_demo: false,
      },
    ],
    selected_binding_id: selected,
    tasks,
    total_runs: tasks.length,
    active_runs: activeCount,
    has_more: false,
    demo_controls: {
      available: true,
      run_id: 'run-demo-01',
      binding_id: 'binding-demo-local',
      can_answer: devPreview.demoCanAnswer,
      can_cancel: devPreview.demoCanCancel,
      revision: devPreview.demoRev,
    },
  };
}

export async function ipcDesktopSnapshot(bindingId?: string | null): Promise<DesktopSnapshot> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return getDevPreviewSnapshot(bindingId);
    }
    return createBrowserDisconnectedSnapshot();
  }
  const args: DesktopSnapshotArgs = { bindingId: bindingId ?? null };
  try {
    return await invoke<DesktopSnapshot>('desktop_snapshot', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopAttach(stateDir: string): Promise<DesktopSnapshot> {
  const trimmed = stateDir.trim();
  if (!trimmed) {
    throw new Error('Enter a runtime state directory path to attach.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      devPreview.selectedBindingId = 'binding-runtime-core';
      return getDevPreviewSnapshot('binding-runtime-core');
    }
    throw new Error('Tauri desktop bridge is not connected in this browser window.');
  }
  const args: DesktopAttachArgs = { stateDir: trimmed };
  try {
    return await invoke<DesktopSnapshot>('desktop_attach', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopRunDetail(
  bindingId: string,
  runId: string
): Promise<RunDetail> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      const snap = getDevPreviewSnapshot(bindingId);
      const task = snap.tasks.find((t) => t.run_id === runId) ?? snap.tasks[0];
      if (!task) {
        throw new Error('Requested preview task was not found.');
      }
      return {
        task,
        instruction:
          task.kind === 'demo'
            ? 'Deterministic local demo run. Confirm Continue or Cancel to exercise revision-checked state transitions without invoking external workers.'
            : 'Verify isolated worktree execution and validate provider model/effort evidence before acceptance.',
        outcome_summary: task.last_summary,
        failure_reason: task.needs_evidence
          ? 'Missing verified effort telemetry from worker execution report.'
          : null,
        acceptance_state: task.needs_evidence ? 'RejectedMissingEvidence' : 'Pending',
        exit_code: task.status === 'Running' || task.status === 'WaitingForAnswer' ? null : 0,
        branch_name: task.kind === 'demo' ? null : `task/${task.run_id}`,
        worktree_path: task.kind === 'demo' ? null : `/tmp/threadlane/worktrees/${task.run_id}`,
        latest_activity: task.last_summary,
        question:
          task.status === 'WaitingForAnswer'
            ? {
                question_id: 'q-demo-1',
                prompt: 'Ready to advance the deterministic local demo with fixed answer Continue?',
              }
            : null,
        counts: {
          durable_progress_events: 3,
          coalesced_progress_events: 1,
          dropped_live_events: 0,
        },
        missing_model_evidence: false,
        missing_effort_evidence: task.needs_evidence,
        demo_controls: snap.demo_controls,
      };
    }
    throw new Error('Tauri desktop bridge is not connected in this browser window.');
  }
  const args: DesktopRunDetailArgs = { bindingId, runId };
  try {
    return await invoke<RunDetail>('desktop_run_detail', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopStartDemo(): Promise<DesktopSnapshot> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      devPreview.demoRev += 1;
      devPreview.demoStatus = 'WaitingForAnswer';
      devPreview.demoCanAnswer = true;
      devPreview.demoCanCancel = true;
      devPreview.selectedBindingId = 'binding-demo-local';
      return getDevPreviewSnapshot('binding-demo-local');
    }
    throw new Error('Tauri desktop bridge is not connected in this browser window.');
  }
  try {
    return await invoke<DesktopSnapshot>('desktop_start_demo');
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopAnswerDemo(expectedRevision: number): Promise<DesktopSnapshot> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      if (expectedRevision !== devPreview.demoRev) {
        throw new Error(
          `Stale demo revision: expected ${expectedRevision}, current is ${devPreview.demoRev}.`
        );
      }
      devPreview.demoRev += 1;
      devPreview.demoStatus = 'Completed';
      devPreview.demoCanAnswer = false;
      devPreview.demoCanCancel = false;
      return getDevPreviewSnapshot('binding-demo-local');
    }
    throw new Error('Tauri desktop bridge is not connected in this browser window.');
  }
  const args: DesktopAnswerDemoArgs = { expectedRevision };
  try {
    return await invoke<DesktopSnapshot>('desktop_answer_demo', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopCancelDemo(expectedRevision: number): Promise<DesktopSnapshot> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      if (expectedRevision !== devPreview.demoRev) {
        throw new Error(
          `Stale demo revision: expected ${expectedRevision}, current is ${devPreview.demoRev}.`
        );
      }
      devPreview.demoRev += 1;
      devPreview.demoStatus = 'Cancelled';
      devPreview.demoCanAnswer = false;
      devPreview.demoCanCancel = false;
      return getDevPreviewSnapshot('binding-demo-local');
    }
    throw new Error('Tauri desktop bridge is not connected in this browser window.');
  }
  const args: DesktopCancelDemoArgs = { expectedRevision };
  try {
    return await invoke<DesktopSnapshot>('desktop_cancel_demo', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopToolStatus(): Promise<ToolStatus[]> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return [
        {
          id: 'opencode',
          label: 'OpenCode',
          executable_path: '/run/current-system/sw/bin/opencode',
          availability: 'found',
          note: 'Discovered on PATH (preview fixture). No CLI or model probe executed.',
        },
        {
          id: 'antigravity',
          label: 'Antigravity',
          executable_path: '/run/current-system/sw/bin/agy',
          availability: 'found',
          note: 'Discovered on PATH (preview fixture). No CLI or model probe executed.',
        },
        {
          id: 'codex',
          label: 'Codex',
          executable_path: '/run/current-system/sw/bin/codex',
          availability: 'found',
          note: 'Discovered on PATH (preview fixture). Native conversation interface.',
        },
        {
          id: 'git',
          label: 'Git',
          executable_path: '/run/current-system/sw/bin/git',
          availability: 'found',
          note: 'Discovered on PATH (preview fixture). Used for per-task worktree isolation.',
        },
        {
          id: 'jev',
          label: 'Jev',
          executable_path: null,
          availability: 'not_configured',
          note: 'Decision shadow-mode integration is planned for Milestone 4.',
        },
      ];
    }
    return createBrowserDisconnectedTools();
  }
  try {
    return await invoke<ToolStatus[]>('desktop_tool_status');
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}

export async function ipcDesktopWindowAction(action: WindowAction): Promise<void> {
  if (!hasTauriRuntime()) {
    return;
  }
  const args: DesktopWindowActionArgs = { action };
  try {
    await invoke<void>('desktop_window_action', { ...args });
  } catch (err) {
    throw new Error(normalizeIpcError(err));
  }
}
