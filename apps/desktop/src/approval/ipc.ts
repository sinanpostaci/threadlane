import { invoke } from '@tauri-apps/api/core';
import {
  AppIpcError,
  getDevPreviewContext,
  hasTauriRuntime,
  isDevPreviewFixtureMode,
  MAX_SAFE_REVISION,
  normalizeAppError,
  resolve_choice,
  validatePlanTasksDag,
  type PlanRevision,
  type RegistrySnapshot,
} from '../registry/ipc.ts';
import {
  isActivePublishedPlanStatus,
  type ApprovalSummary,
  type DesktopApprovePlanArgs,
  type DesktopPlanApprovalsArgs,
  type DesktopPlanExecutionPreviewArgs,
  type OperatorApprovalRequest,
  type PlanExecutionPreview,
  type PlanTaskExecutionPreview,
} from './types.ts';

export * from './types.ts';

function validateSafeRevisionNumber(revision: number, label: string): void {
  if (
    typeof revision !== 'number' ||
    !Number.isInteger(revision) ||
    revision <= 0 ||
    revision > MAX_SAFE_REVISION
  ) {
    throw new AppIpcError('invalid_revision', `${label} must be a positive safe integer.`);
  }
}

/**
 * Deterministic synchronous 64-character lowercase hex digest used only for
 * read-only sample previews in browser DEV (`?preview=1`) fixture mode.
 */
function deterministicSampleHex64(input: string): string {
  const seeds = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
    0x5be0cd19,
  ];
  for (let i = 0; i < input.length; i += 1) {
    const code = input.charCodeAt(i);
    const lane = i & 7;
    seeds[lane] = Math.imul((seeds[lane]! ^ code) + i + 1, 0x9e3779b1) >>> 0;
    const nextLane = (lane + 1) & 7;
    seeds[nextLane] = (seeds[nextLane]! ^ ((seeds[lane]! << 7) | (seeds[lane]! >>> 25))) >>> 0;
  }
  return seeds.map((word) => word.toString(16).padStart(8, '0')).join('');
}

function computeTopologicalOrder(plan: PlanRevision): string[] {
  validatePlanTasksDag(plan.tasks);
  const inDegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();
  const declOrder = new Map<string, number>();

  plan.tasks.forEach((task, idx) => {
    const id = task.id.trim();
    declOrder.set(id, idx);
    inDegree.set(id, 0);
    dependents.set(id, []);
  });

  for (const task of plan.tasks) {
    const id = task.id.trim();
    const seenDeps = new Set<string>();
    for (const rawDep of task.depends_on) {
      const depId = rawDep.trim();
      if (!depId || seenDeps.has(depId)) {
        continue;
      }
      seenDeps.add(depId);
      inDegree.set(id, (inDegree.get(id) ?? 0) + 1);
      dependents.get(depId)?.push(id);
    }
  }

  const ready: string[] = [];
  for (const [id, deg] of inDegree.entries()) {
    if (deg === 0) {
      ready.push(id);
    }
  }
  ready.sort((a, b) => (declOrder.get(a) ?? 0) - (declOrder.get(b) ?? 0));

  const ordered: string[] = [];
  while (ready.length > 0) {
    const current = ready.shift()!;
    ordered.push(current);
    for (const nextId of dependents.get(current) ?? []) {
      const nextDeg = (inDegree.get(nextId) ?? 1) - 1;
      inDegree.set(nextId, nextDeg);
      if (nextDeg === 0) {
        ready.push(nextId);
        ready.sort((a, b) => (declOrder.get(a) ?? 0) - (declOrder.get(b) ?? 0));
      }
    }
  }

  return ordered;
}

export function compileSamplePreviewForDevMode(
  plan: PlanRevision,
  registry: RegistrySnapshot
): PlanExecutionPreview {
  if (!isActivePublishedPlanStatus(plan.status)) {
    throw new AppIpcError(
      'unpublished_or_superseded_plan',
      'Only active published plan revisions can be previewed or approved for execution.'
    );
  }
  if (registry.revision !== plan.profile_revision) {
    throw new AppIpcError(
      'stale_registry_revision',
      'Plan profile revision does not match the current registry snapshot.'
    );
  }

  const topologicalOrder = computeTopologicalOrder(plan);
  const compiledTasks: PlanTaskExecutionPreview[] = [];

  for (const task of plan.tasks) {
    const choice = resolve_choice(registry, plan.profile_id, task.role, task.difficulty);
    const modelDef = registry.models.find((m) => m.id === choice.model_id);
    if (!modelDef) {
      throw new AppIpcError(
        'registry_resolution_failed',
        'Task role and difficulty could not be resolved against the workflow profile.'
      );
    }
    const toolDef = registry.tools.find((t) => t.id === modelDef.tool_id);
    if (!toolDef || !toolDef.enabled) {
      throw new AppIpcError(
        'registry_resolution_failed',
        'Task tool is missing or disabled in the registry snapshot.'
      );
    }

    const instructionDigest = deterministicSampleHex64(`instr:${task.instruction}`);
    const taskDigest = deterministicSampleHex64(
      JSON.stringify({
        id: task.id,
        title: task.title,
        instructionDigest,
        role: task.role,
        difficulty: task.difficulty,
        toolId: toolDef.id,
        adapter: toolDef.adapter,
        modelId: modelDef.id,
        modelArg: modelDef.model_arg,
        effort: choice.effort,
        dependsOn: task.depends_on,
        allowedWritePaths: task.allowed_write_paths,
      })
    );

    compiledTasks.push({
      id: task.id,
      title: task.title,
      instruction: task.instruction,
      instruction_digest: instructionDigest,
      role: task.role,
      difficulty: task.difficulty,
      tool_id: toolDef.id,
      tool_label: toolDef.label,
      adapter: toolDef.adapter,
      model_id: modelDef.id,
      model_label: modelDef.label,
      model_arg: modelDef.model_arg,
      effort: choice.effort,
      depends_on: [...task.depends_on],
      allowed_write_paths: [...task.allowed_write_paths],
      task_digest: taskDigest,
    });
  }

  const planDigest = deterministicSampleHex64(
    JSON.stringify({
      planId: plan.plan_id,
      bindingId: plan.binding_id,
      planRevision: plan.revision,
      profileId: plan.profile_id,
      registryRevision: plan.profile_revision,
      topologicalOrder,
      tasks: compiledTasks.map((t) => [t.id, t.task_digest]),
    })
  );

  return {
    plan_id: plan.plan_id,
    plan_revision: plan.revision,
    binding_id: plan.binding_id,
    profile_id: plan.profile_id,
    registry_revision: plan.profile_revision,
    status: plan.status,
    topological_order: topologicalOrder,
    tasks: compiledTasks,
    plan_digest: planDigest,
  };
}

export async function desktop_plan_execution_preview(
  args: DesktopPlanExecutionPreviewArgs
): Promise<PlanExecutionPreview>;
export async function desktop_plan_execution_preview(
  planId: string,
  revision: number
): Promise<PlanExecutionPreview>;
export async function desktop_plan_execution_preview(
  arg1:
    | DesktopPlanExecutionPreviewArgs
    | { plan_id?: string; planId?: string; revision: number }
    | string,
  arg2?: number
): Promise<PlanExecutionPreview> {
  const planId = (
    typeof arg1 === 'string'
      ? arg1
      : arg1.planId ?? (arg1 as { plan_id?: string }).plan_id ?? ''
  ).trim();
  const revision = typeof arg1 === 'string' ? arg2 ?? -1 : arg1.revision;

  if (!planId) {
    throw new AppIpcError('invalid_input', 'Plan identifier is required to preview execution.');
  }
  validateSafeRevisionNumber(revision, 'Plan revision');

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      const { registry, plans } = getDevPreviewContext();
      const target = plans.find((p) => p.plan_id === planId && p.revision === revision);
      if (!target) {
        throw new AppIpcError(
          'plan_not_found',
          'Requested plan revision was not found in the plan ledger.'
        );
      }
      return compileSamplePreviewForDevMode(target, registry);
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }

  try {
    return await invoke<PlanExecutionPreview>('desktop_plan_execution_preview', {
      planId,
      revision,
    });
  } catch (err) {
    throw normalizeAppError(
      err,
      'plan_preview_failed',
      'Failed to compile plan execution preview.'
    );
  }
}

export async function desktop_approve_plan(
  args: DesktopApprovePlanArgs
): Promise<ApprovalSummary>;
export async function desktop_approve_plan(
  request: OperatorApprovalRequest
): Promise<ApprovalSummary>;
export async function desktop_approve_plan(
  arg: DesktopApprovePlanArgs | OperatorApprovalRequest
): Promise<ApprovalSummary> {
  const request: OperatorApprovalRequest =
    'request' in arg && arg.request ? arg.request : (arg as OperatorApprovalRequest);

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      throw new AppIpcError(
        'preview_read_only',
        'Browser preview mode is a read-only fixture and cannot record operator execution approvals.'
      );
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }

  try {
    return await invoke<ApprovalSummary>('desktop_approve_plan', { request });
  } catch (err) {
    throw normalizeAppError(
      err,
      'approve_plan_failed',
      'Failed to record operator plan approval.'
    );
  }
}

export async function desktop_plan_approvals(
  args: DesktopPlanApprovalsArgs
): Promise<ApprovalSummary[]>;
export async function desktop_plan_approvals(bindingId: string): Promise<ApprovalSummary[]>;
export async function desktop_plan_approvals(
  arg: DesktopPlanApprovalsArgs | { binding_id?: string; bindingId?: string } | string
): Promise<ApprovalSummary[]> {
  const bindingId = (
    typeof arg === 'string'
      ? arg
      : arg.bindingId ?? (arg as { binding_id?: string }).binding_id ?? ''
  ).trim();

  if (!bindingId) {
    throw new AppIpcError(
      'invalid_input',
      'Binding identifier is required to list plan approvals.'
    );
  }

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return [];
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }

  try {
    return await invoke<ApprovalSummary[]>('desktop_plan_approvals', { bindingId });
  } catch (err) {
    throw normalizeAppError(
      err,
      'plan_approvals_failed',
      'Failed to load recorded plan approvals.'
    );
  }
}

export const desktopPlanExecutionPreview = desktop_plan_execution_preview;
export const desktopApprovePlan = desktop_approve_plan;
export const desktopPlanApprovals = desktop_plan_approvals;

export const ipcDesktopPlanExecutionPreview = desktop_plan_execution_preview;
export const ipcDesktopApprovePlan = desktop_approve_plan;
export const ipcDesktopPlanApprovals = desktop_plan_approvals;
