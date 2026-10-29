import { invoke } from '@tauri-apps/api/core';
import { hasTauriRuntime, isDevPreviewFixtureMode } from '../ipc.ts';
export { hasTauriRuntime, isDevPreviewFixtureMode };
import {
  createEmptyRegistrySnapshot,
  DIFFICULTIES,
  isApiToolAdapter,
  MAX_SAFE_REVISION,
  TOOL_ADAPTERS,
  WORKFLOW_ROLES,
  type AppError,
  type DecisionAdvice,
  type DecisionProvider,
  type DesktopDecisionModelsArgs,
  type DesktopNativeThreadsArgs,
  type DesktopPlanChangeArgs,
  type DesktopProbeToolArgs,
  type DesktopRegistryApplyArgs,
  type DesktopSecretDeleteArgs,
  type DesktopSecretSetArgs,
  type DesktopSecretStatusArgs,
  type DesktopTestDecisionArgs,
  type ModelDefinition,
  type NativeThreadPage,
  type PlanChange,
  type PlanRevision,
  type PlanTask,
  type RegistryChange,
  type RegistrySnapshot,
  type ToolProbe,
} from './types.ts';

export * from './types.ts';

export class AppIpcError extends Error implements AppError {
  readonly code: string;
  override readonly message: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'AppIpcError';
    this.code = code;
    this.message = message;
  }

  toJSON(): AppError {
    return {
      code: this.code,
      message: this.message,
    };
  }
}

export function isAppError(value: unknown): value is AppError {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return typeof candidate.code === 'string' && typeof candidate.message === 'string';
}

export function sanitizeSafeMessage(
  raw: string,
  fallback: string = 'Desktop request failed.',
  secretToRedact?: string
): string {
  const trimmed = raw.trim();
  if (!trimmed) {
    return fallback;
  }
  if (secretToRedact && secretToRedact.trim().length > 0 && trimmed.includes(secretToRedact.trim())) {
    return fallback;
  }
  const redacted = trimmed
    .replace(/bearer\s+[a-z0-9._-]+/gi, '[redacted]')
    .replace(/sk-[a-z0-9_-]+/gi, '[redacted]')
    .replace(/(api_?key|secret|token|password)\s*[=:]\s*[^\s,;]+/gi, '$1=[redacted]')
    .trim();
  if (!redacted) {
    return fallback;
  }
  return redacted.length > 220 ? `${redacted.slice(0, 217)}...` : redacted;
}

export function normalizeAppError(
  err: unknown,
  defaultCode: string = 'ipc_error',
  fallbackMessage: string = 'Desktop request failed.',
  secretToRedact?: string
): AppIpcError {
  if (err instanceof AppIpcError) {
    const safeMsg = sanitizeSafeMessage(err.message, fallbackMessage, secretToRedact);
    return safeMsg === err.message ? err : new AppIpcError(err.code, safeMsg);
  }
  if (isAppError(err)) {
    const code = err.code.trim() || defaultCode;
    const msg = sanitizeSafeMessage(err.message, fallbackMessage, secretToRedact);
    return new AppIpcError(code, msg);
  }
  if (typeof err === 'string') {
    const trimmed = err.trim();
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        const parsed = JSON.parse(trimmed) as unknown;
        if (isAppError(parsed)) {
          return new AppIpcError(
            parsed.code.trim() || defaultCode,
            sanitizeSafeMessage(parsed.message, fallbackMessage, secretToRedact)
          );
        }
      } catch {
        // Fall through to string handling.
      }
    }
    return new AppIpcError(defaultCode, sanitizeSafeMessage(trimmed, fallbackMessage, secretToRedact));
  }
  if (err instanceof Error) {
    return new AppIpcError(
      defaultCode,
      sanitizeSafeMessage(err.message, fallbackMessage, secretToRedact)
    );
  }
  return new AppIpcError(defaultCode, fallbackMessage);
}

export function formatAppError(err: unknown, fallbackMessage?: string): string {
  const normalized = normalizeAppError(err, 'ipc_error', fallbackMessage);
  return `${normalized.message} (${normalized.code})`;
}

function validateSafeRevision(revision: number, label: string): void {
  if (
    typeof revision !== 'number' ||
    !Number.isInteger(revision) ||
    revision < 0 ||
    revision > MAX_SAFE_REVISION
  ) {
    throw new AppIpcError('invalid_revision', `${label} must be a safe non-negative integer.`);
  }
}

export function applyRegistryChangeLocally(
  snapshot: RegistrySnapshot,
  expectedRevision: number,
  change: RegistryChange
): RegistrySnapshot {
  validateSafeRevision(expectedRevision, 'Expected revision');
  if (expectedRevision !== snapshot.revision) {
    throw new AppIpcError(
      'revision_conflict',
      `Registry revision conflict: expected ${expectedRevision}, current is ${snapshot.revision}.`
    );
  }
  if (snapshot.revision >= MAX_SAFE_REVISION) {
    throw new AppIpcError('revision_overflow', 'Registry revision has reached maximum safe integer.');
  }

  const nextTools = snapshot.tools.map((t) => ({ ...t }));
  const nextModels = snapshot.models.map((m) => ({ ...m, efforts: [...m.efforts] }));
  const nextProfiles = snapshot.profiles.map((p) => ({
    ...p,
    assignments: p.assignments.map((a) => ({ ...a, choice: { ...a.choice } })),
  }));
  let nextDefaultProfileId = snapshot.default_profile_id;

  switch (change.op) {
    case 'upsert_tool': {
      const tool = change.tool;
      const id = tool.id.trim();
      const label = tool.label.trim();
      if (!id || id.length > 64) {
        throw new AppIpcError('invalid_tool', 'Tool ID must be 1 to 64 characters.');
      }
      if (!label || label.length > 128) {
        throw new AppIpcError('invalid_tool', 'Tool label must be 1 to 128 characters.');
      }
      if (!TOOL_ADAPTERS.includes(tool.adapter)) {
        throw new AppIpcError('invalid_tool', `Unsupported tool adapter "${tool.adapter}".`);
      }
      const normalizedTool = {
        id,
        label,
        adapter: tool.adapter,
        executable_path: isApiToolAdapter(tool.adapter)
          ? null
          : tool.executable_path?.trim() || null,
        enabled: Boolean(tool.enabled),
      };
      const existingIdx = nextTools.findIndex((t) => t.id === id);
      if (existingIdx >= 0) {
        nextTools[existingIdx] = normalizedTool;
      } else {
        if (nextTools.length >= 64) {
          throw new AppIpcError('capacity_exceeded', 'Registry cannot exceed 64 tools.');
        }
        nextTools.push(normalizedTool);
      }
      break;
    }

    case 'delete_tool': {
      const toolId = change.tool_id.trim();
      const idx = nextTools.findIndex((t) => t.id === toolId);
      if (idx < 0) {
        throw new AppIpcError('not_found', `Tool "${toolId}" was not found.`);
      }
      if (nextModels.some((m) => m.tool_id === toolId)) {
        throw new AppIpcError(
          'tool_in_use',
          `Cannot delete tool "${toolId}" while models reference it.`
        );
      }
      nextTools.splice(idx, 1);
      break;
    }

    case 'upsert_model': {
      upsertModelIntoList(nextTools, nextModels, change.model);
      break;
    }

    case 'upsert_models': {
      if (!Array.isArray(change.models) || change.models.length === 0) {
        throw new AppIpcError('invalid_model', 'At least one model is required for batch upsert.');
      }
      for (const model of change.models) {
        upsertModelIntoList(nextTools, nextModels, model);
      }
      break;
    }

    case 'delete_model': {
      const modelId = change.model_id.trim();
      const idx = nextModels.findIndex((m) => m.id === modelId);
      if (idx < 0) {
        throw new AppIpcError('not_found', `Model "${modelId}" was not found.`);
      }
      const referencedByProfile = nextProfiles.find((p) =>
        p.assignments.some((a) => a.choice.model_id === modelId)
      );
      if (referencedByProfile) {
        throw new AppIpcError(
          'model_in_use',
          `Cannot delete model "${modelId}" while profile "${referencedByProfile.name}" references it.`
        );
      }
      nextModels.splice(idx, 1);
      break;
    }

    case 'upsert_profile': {
      const profile = change.profile;
      const id = profile.id.trim();
      const name = profile.name.trim();
      if (!id || id.length > 64) {
        throw new AppIpcError('invalid_profile', 'Profile ID must be 1 to 64 characters.');
      }
      if (!name || name.length > 128) {
        throw new AppIpcError('invalid_profile', 'Profile name must be 1 to 128 characters.');
      }
      if (profile.assignments.length > 30) {
        throw new AppIpcError(
          'capacity_exceeded',
          'Profile cannot exceed 30 role/difficulty assignments.'
        );
      }
      const seenSlots = new Set<string>();
      const normalizedAssignments = profile.assignments.map((assignment) => {
        if (!WORKFLOW_ROLES.includes(assignment.role)) {
          throw new AppIpcError('invalid_profile', `Invalid workflow role "${assignment.role}".`);
        }
        if (
          assignment.difficulty !== null &&
          !DIFFICULTIES.includes(assignment.difficulty)
        ) {
          throw new AppIpcError(
            'invalid_profile',
            `Invalid difficulty "${String(assignment.difficulty)}".`
          );
        }
        const slotKey = `${assignment.role}:${assignment.difficulty ?? 'default'}`;
        if (seenSlots.has(slotKey)) {
          throw new AppIpcError(
            'duplicate_assignment',
            `Duplicate assignment for ${assignment.role} (${assignment.difficulty ?? 'default'}).`
          );
        }
        seenSlots.add(slotKey);

        const model = nextModels.find((m) => m.id === assignment.choice.model_id);
        if (!model) {
          throw new AppIpcError(
            'invalid_reference',
            `Referenced model "${assignment.choice.model_id}" does not exist.`
          );
        }
        const tool = nextTools.find((t) => t.id === model.tool_id);
        if (!tool || !tool.enabled) {
          throw new AppIpcError(
            'invalid_reference',
            `Tool "${model.tool_id}" for model "${model.label}" is missing or disabled.`
          );
        }

        const effort = assignment.choice.effort?.trim() || null;
        if (effort === null) {
          if (!model.allow_no_effort) {
            throw new AppIpcError(
              'unsupported_effort',
              `Model "${model.label}" requires an explicit reasoning effort.`
            );
          }
        } else if (!model.efforts.includes(effort)) {
          throw new AppIpcError(
            'unsupported_effort',
            `Effort "${effort}" is not supported by model "${model.label}".`
          );
        }

        return {
          role: assignment.role,
          difficulty: assignment.difficulty,
          choice: {
            model_id: model.id,
            effort,
          },
        };
      });

      const existingIdx = nextProfiles.findIndex((p) => p.id === id);
      const normalizedProfile = {
        id,
        name,
        assignments: normalizedAssignments,
      };
      if (existingIdx >= 0) {
        nextProfiles[existingIdx] = normalizedProfile;
      } else {
        if (nextProfiles.length >= 64) {
          throw new AppIpcError('capacity_exceeded', 'Registry cannot exceed 64 profiles.');
        }
        nextProfiles.push(normalizedProfile);
      }
      break;
    }

    case 'delete_profile': {
      const profileId = change.profile_id.trim();
      const idx = nextProfiles.findIndex((p) => p.id === profileId);
      if (idx < 0) {
        throw new AppIpcError('not_found', `Profile "${profileId}" was not found.`);
      }
      nextProfiles.splice(idx, 1);
      if (nextDefaultProfileId === profileId) {
        nextDefaultProfileId = null;
      }
      break;
    }

    case 'set_default_profile': {
      const targetId = change.profile_id?.trim() || null;
      if (targetId !== null && !nextProfiles.some((p) => p.id === targetId)) {
        throw new AppIpcError('not_found', `Default profile "${targetId}" does not exist.`);
      }
      nextDefaultProfileId = targetId;
      break;
    }
  }

  return {
    schema_version: snapshot.schema_version,
    revision: snapshot.revision + 1,
    tools: nextTools,
    models: nextModels,
    profiles: nextProfiles,
    default_profile_id: nextDefaultProfileId,
  };
}

function upsertModelIntoList(
  tools: RegistrySnapshot['tools'],
  models: RegistrySnapshot['models'],
  model: ModelDefinition
): void {
  const id = model.id.trim();
  const toolId = model.tool_id.trim();
  const label = model.label.trim();
  const modelArg = model.model_arg.trim();
  if (!id || id.length > 96) {
    throw new AppIpcError('invalid_model', 'Model ID must be 1 to 96 characters.');
  }
  if (!toolId || !tools.some((t) => t.id === toolId)) {
    throw new AppIpcError('invalid_reference', `Referenced tool "${toolId}" does not exist.`);
  }
  if (!label || label.length > 128) {
    throw new AppIpcError('invalid_model', 'Model label must be 1 to 128 characters.');
  }
  if (!modelArg || modelArg.length > 128) {
    throw new AppIpcError('invalid_model', 'Model argument must be 1 to 128 characters.');
  }
  const efforts: string[] = [];
  for (const rawEffort of model.efforts) {
    const trimmed = rawEffort.trim();
    if (!trimmed) {
      continue;
    }
    if (!efforts.includes(trimmed)) {
      efforts.push(trimmed);
    }
  }
  if (efforts.length === 0 && !model.allow_no_effort) {
    throw new AppIpcError(
      'invalid_model',
      `Model "${label}" must either list supported efforts or allow no effort.`
    );
  }
  const normalized: ModelDefinition = {
    id,
    tool_id: toolId,
    label,
    model_arg: modelArg,
    efforts,
    allow_no_effort: Boolean(model.allow_no_effort),
  };
  const existingIdx = models.findIndex((m) => m.id === id);
  if (existingIdx >= 0) {
    models[existingIdx] = normalized;
  } else {
    if (models.length >= 256) {
      throw new AppIpcError('capacity_exceeded', 'Registry cannot exceed 256 models.');
    }
    models.push(normalized);
  }
}

const PROTECTED_CONTROL_COMPONENTS = new Set([
  '.git',
  '.gitmodules',
  '.gitattributes',
  '.gitignore',
  'agents.md',
  'claude.md',
  '.codex',
  '.gemini',
  '.opencode',
  '.agents',
  '.claude',
]);

export function isProtectedControlWriteScope(relPath: string): boolean {
  const trimmed = relPath.replace(/^\/+|\/+$/g, '');
  if (!trimmed) {
    return true;
  }
  const lowerSegments = trimmed
    .split('/')
    .filter(Boolean)
    .map((seg) => seg.toLowerCase());
  if (lowerSegments.length === 0) {
    return true;
  }
  for (const seg of lowerSegments) {
    if (PROTECTED_CONTROL_COMPONENTS.has(seg)) {
      return true;
    }
    if (seg === '.env' || seg.startsWith('.env.')) {
      return true;
    }
  }
  if (lowerSegments[0] === '.github') {
    return true;
  }
  if (
    lowerSegments[0] === '.cargo' &&
    (lowerSegments[1] === 'config' || lowerSegments[1] === 'config.toml')
  ) {
    return true;
  }
  return false;
}

export function validateCanonicalWriteScopeEntry(
  rawPath: string,
  errorCode: string = 'invalid_scope'
): string {
  if (!rawPath || rawPath.length > 512) {
    throw new AppIpcError(
      errorCode,
      'Write path must be between 1 and 512 characters.'
    );
  }
  if (rawPath.trim() !== rawPath) {
    throw new AppIpcError(
      errorCode,
      `Write path "${rawPath}" must not have leading or trailing whitespace.`
    );
  }
  const stripped = rawPath.endsWith('/') ? rawPath.slice(0, -1) : rawPath;
  if (
    !stripped ||
    rawPath.startsWith('/') ||
    stripped.endsWith('/') ||
    rawPath.includes('//') ||
    rawPath.includes('\\') ||
    rawPath.includes('\0')
  ) {
    throw new AppIpcError(
      errorCode,
      `Write path "${rawPath}" must be a normalized repository-relative file or directory ("dir/") path without leading slashes, double slashes, or backslashes.`
    );
  }
  if (/[*?[\]{}!]/.test(rawPath)) {
    throw new AppIpcError(
      errorCode,
      `Write path "${rawPath}" must not contain wildcard or glob characters.`
    );
  }
  for (let i = 0; i < rawPath.length; i++) {
    const code = rawPath.charCodeAt(i);
    if (code <= 0x1f || code === 0x7f) {
      throw new AppIpcError(
        errorCode,
        `Write path "${rawPath}" must not contain control characters.`
      );
    }
  }
  const segments = stripped.split('/');
  if (segments.some((seg) => !seg || seg === '.' || seg === '..')) {
    throw new AppIpcError(
      errorCode,
      `Write path "${rawPath}" must not contain "." or ".." traversal components.`
    );
  }
  if (isProtectedControlWriteScope(stripped)) {
    throw new AppIpcError(
      errorCode,
      `Write path "${rawPath}" targets protected repository control or agent configuration files.`
    );
  }
  return rawPath;
}

export function validatePlanTasksDag(tasks: readonly PlanTask[]): void {
  if (tasks.length === 0) {
    throw new AppIpcError('invalid_plan', 'A plan must contain at least one task.');
  }
  if (tasks.length > 32) {
    throw new AppIpcError('invalid_plan', 'A plan cannot exceed 32 tasks.');
  }

  const ids = new Set<string>();
  for (const task of tasks) {
    const id = task.id.trim();
    if (!id || id.length > 64) {
      throw new AppIpcError('invalid_plan', 'Each task must have a valid ID (1 to 64 chars).');
    }
    if (ids.has(id)) {
      throw new AppIpcError('duplicate_task_id', `Duplicate task ID "${id}" in plan.`);
    }
    ids.add(id);

    if (!task.title.trim()) {
      throw new AppIpcError('invalid_plan', `Task "${id}" requires a title.`);
    }
    if (!task.instruction.trim()) {
      throw new AppIpcError('invalid_plan', `Task "${id}" requires an instruction.`);
    }
    if (!WORKFLOW_ROLES.includes(task.role)) {
      throw new AppIpcError('invalid_plan', `Task "${id}" has invalid role "${task.role}".`);
    }
    if (!DIFFICULTIES.includes(task.difficulty)) {
      throw new AppIpcError(
        'invalid_plan',
        `Task "${id}" has invalid difficulty "${task.difficulty}".`
      );
    }
    if (task.allowed_write_paths.length === 0 || task.allowed_write_paths.length > 64) {
      throw new AppIpcError(
        'invalid_scope',
        `Task "${id}" must specify between 1 and 64 allowed write paths.`
      );
    }
    const seenPaths = new Set<string>();
    for (const writePath of task.allowed_write_paths) {
      const checked = validateCanonicalWriteScopeEntry(writePath, 'invalid_scope');
      if (seenPaths.has(checked)) {
        throw new AppIpcError(
          'invalid_scope',
          `Task "${id}" has duplicate allowed write path "${checked}".`
        );
      }
      seenPaths.add(checked);
    }
  }

  const adj = new Map<string, string[]>();
  for (const task of tasks) {
    const id = task.id.trim();
    const deps: string[] = [];
    for (const dep of task.depends_on) {
      const depId = dep.trim();
      if (!depId) continue;
      if (depId === id) {
        throw new AppIpcError('invalid_dag', `Task "${id}" cannot depend on itself.`);
      }
      if (!ids.has(depId)) {
        throw new AppIpcError(
          'invalid_dag',
          `Task "${id}" depends on unknown task "${depId}".`
        );
      }
      if (!deps.includes(depId)) {
        deps.push(depId);
      }
    }
    adj.set(id, deps);
  }

  const visitState = new Map<string, 'visiting' | 'visited'>();
  const dfs = (nodeId: string): void => {
    const current = visitState.get(nodeId);
    if (current === 'visiting') {
      throw new AppIpcError('invalid_dag', `Dependency cycle detected involving task "${nodeId}".`);
    }
    if (current === 'visited') {
      return;
    }
    visitState.set(nodeId, 'visiting');
    for (const depId of adj.get(nodeId) ?? []) {
      dfs(depId);
    }
    visitState.set(nodeId, 'visited');
  };

  for (const id of ids) {
    dfs(id);
  }
}

export function applyPlanChangeLocally(
  revisions: readonly PlanRevision[],
  expectedRevision: number,
  change: PlanChange,
  nowMs: number = Date.now()
): { revisions: PlanRevision[]; updated: PlanRevision } {
  validateSafeRevision(expectedRevision, 'Expected plan revision');
  const planId = change.plan_id.trim();
  if (!planId || planId.length > 64) {
    throw new AppIpcError('invalid_plan', 'Plan ID must be 1 to 64 characters.');
  }

  const existingForPlan = revisions
    .filter((r) => r.plan_id === planId)
    .sort((a, b) => a.revision - b.revision);
  const latest = existingForPlan[existingForPlan.length - 1] ?? null;
  const currentRevision = latest ? latest.revision : 0;

  if (expectedRevision !== currentRevision) {
    throw new AppIpcError(
      'revision_conflict',
      `Plan "${planId}" revision conflict: expected ${expectedRevision}, current is ${currentRevision}.`
    );
  }

  const nextRevisions = revisions.map((r) => ({
    ...r,
    tasks: r.tasks.map((t) => ({
      ...t,
      depends_on: [...t.depends_on],
      allowed_write_paths: [...t.allowed_write_paths],
    })),
  }));

  if (change.op === 'save_draft') {
    const bindingId = change.binding_id.trim();
    const profileId = change.profile_id.trim();
    const summary = change.summary.trim();
    if (!bindingId) {
      throw new AppIpcError('invalid_plan', 'Binding ID is required to save a plan draft.');
    }
    if (!profileId) {
      throw new AppIpcError('invalid_plan', 'Workflow profile ID is required to save a plan draft.');
    }
    if (!summary) {
      throw new AppIpcError('invalid_plan', 'Plan summary is required.');
    }
    validatePlanTasksDag(change.tasks);

    for (const prev of nextRevisions) {
      if (prev.plan_id === planId && prev.status === 'draft') {
        prev.status = 'superseded';
      }
    }

    const created: PlanRevision = {
      plan_id: planId,
      binding_id: bindingId,
      revision: currentRevision + 1,
      profile_id: profileId,
      profile_revision: change.profile_revision,
      summary,
      tasks: change.tasks.map((t) => ({
        id: t.id.trim(),
        title: t.title.trim(),
        instruction: t.instruction.trim(),
        role: t.role,
        difficulty: t.difficulty,
        depends_on: t.depends_on.map((d) => d.trim()).filter(Boolean),
        allowed_write_paths: t.allowed_write_paths.map((p) => p.trim()).filter(Boolean),
      })),
      status: 'draft',
      created_at_ms: nowMs,
    };
    nextRevisions.push(created);
    return { revisions: nextRevisions, updated: created };
  }

  if (!latest) {
    throw new AppIpcError('not_found', `Plan "${planId}" does not exist.`);
  }
  if (change.revision !== latest.revision) {
    throw new AppIpcError(
      'revision_conflict',
      `Target revision ${change.revision} does not match latest revision ${latest.revision}.`
    );
  }

  const targetIdx = nextRevisions.findIndex(
    (r) => r.plan_id === planId && r.revision === latest.revision
  );
  const target = nextRevisions[targetIdx]!;

  if (change.op === 'publish') {
    if (target.status === 'published') {
      return { revisions: nextRevisions, updated: target };
    }
    if (target.status !== 'draft') {
      throw new AppIpcError(
        'invalid_transition',
        `Cannot publish plan revision ${target.revision} from status "${target.status}".`
      );
    }
    for (const prev of nextRevisions) {
      if (
        prev.plan_id === planId &&
        prev.revision !== target.revision &&
        (prev.status === 'published' || prev.status === 'received')
      ) {
        prev.status = 'superseded';
      }
    }
    target.status = 'published';
    return { revisions: nextRevisions, updated: target };
  }

  if (change.op === 'receive') {
    if (target.status === 'received') {
      return { revisions: nextRevisions, updated: target };
    }
    if (target.status !== 'published') {
      throw new AppIpcError(
        'invalid_transition',
        `Cannot mark plan revision ${target.revision} received from status "${target.status}".`
      );
    }
    target.status = 'received';
    return { revisions: nextRevisions, updated: target };
  }

  if (target.status === 'applied') {
    return { revisions: nextRevisions, updated: target };
  }
  if (target.status !== 'published' && target.status !== 'received') {
    throw new AppIpcError(
      'invalid_transition',
      `Cannot apply plan revision ${target.revision} from status "${target.status}".`
    );
  }
  target.status = 'applied';
  return { revisions: nextRevisions, updated: target };
}

// DEV-only (?preview=1) in-memory state
let previewRegistry: RegistrySnapshot = {
  schema_version: 1,
  revision: 1,
  tools: [
    {
      id: 'codex-main',
      label: 'Codex CLI',
      adapter: 'codex_cli',
      executable_path: '/run/current-system/sw/bin/codex',
      enabled: true,
    },
    {
      id: 'agy-main',
      label: 'Antigravity CLI',
      adapter: 'antigravity',
      executable_path: '/run/current-system/sw/bin/agy',
      enabled: true,
    },
    {
      id: 'opencode-main',
      label: 'OpenCode v2',
      adapter: 'opencode_v2',
      executable_path: '/run/current-system/sw/bin/opencode',
      enabled: true,
    },
    {
      id: 'jev-api',
      label: 'Jev System-One API',
      adapter: 'jev_api',
      executable_path: null,
      enabled: true,
    },
  ],
  models: [
    {
      id: 'agy-argon-high',
      tool_id: 'agy-main',
      label: 'Gemini 4 Argon',
      model_arg: 'gemini-4-argon-high',
      efforts: ['high'],
      allow_no_effort: false,
    },
    {
      id: 'oc-deepseek-flash',
      tool_id: 'opencode-main',
      label: 'DeepSeek v4.1 Flash',
      model_arg: 'opencode-go/deepseek-v4.1-flash',
      efforts: ['max'],
      allow_no_effort: false,
    },
    {
      id: 'codex-astra',
      tool_id: 'codex-main',
      label: 'GPT-6 Astra',
      model_arg: 'gpt-6-astra',
      efforts: ['high', 'max'],
      allow_no_effort: false,
    },
  ],
  profiles: [
    {
      id: 'default',
      name: 'Standard M4 Profile',
      assignments: [
        {
          role: 'coder',
          difficulty: 'easy',
          choice: { model_id: 'oc-deepseek-flash', effort: 'max' },
        },
        {
          role: 'coder',
          difficulty: 'medium',
          choice: { model_id: 'oc-deepseek-flash', effort: 'max' },
        },
        {
          role: 'coder',
          difficulty: 'hard',
          choice: { model_id: 'agy-argon-high', effort: 'high' },
        },
        {
          role: 'reviewer',
          difficulty: null,
          choice: { model_id: 'codex-astra', effort: 'high' },
        },
        {
          role: 'closure',
          difficulty: null,
          choice: { model_id: 'codex-astra', effort: 'max' },
        },
      ],
    },
  ],
  default_profile_id: 'default',
};

const previewSecrets = new Map<string, boolean>([['jev-api', false]]);
let previewPlans: PlanRevision[] = [];

export function resetDevPreviewRegistryState(): void {
  previewRegistry = createEmptyRegistrySnapshot();
  previewSecrets.clear();
  previewPlans = [];
}

export function getDevPreviewContext(): {
  registry: RegistrySnapshot;
  plans: PlanRevision[];
} {
  return {
    registry: structuredClone(previewRegistry),
    plans: structuredClone(previewPlans),
  };
}

export async function desktop_registry_snapshot(): Promise<RegistrySnapshot> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return structuredClone(previewRegistry);
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<RegistrySnapshot>('desktop_registry_snapshot');
  } catch (err) {
    throw normalizeAppError(err, 'registry_snapshot_failed', 'Failed to load registry snapshot.');
  }
}

export async function desktop_registry_apply(
  args: DesktopRegistryApplyArgs
): Promise<RegistrySnapshot>;
export async function desktop_registry_apply(
  expectedRevision: number,
  change: RegistryChange
): Promise<RegistrySnapshot>;
export async function desktop_registry_apply(
  arg1: DesktopRegistryApplyArgs | { expected_revision?: number; expectedRevision?: number; change: RegistryChange } | number,
  arg2?: RegistryChange
): Promise<RegistrySnapshot> {
  const expectedRevision =
    typeof arg1 === 'number'
      ? arg1
      : typeof arg1.expectedRevision === 'number'
        ? arg1.expectedRevision
        : (arg1 as { expected_revision?: number }).expected_revision ?? -1;
  const change = typeof arg1 === 'number' ? arg2! : arg1.change;

  validateSafeRevision(expectedRevision, 'Expected registry revision');

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      previewRegistry = applyRegistryChangeLocally(previewRegistry, expectedRevision, change);
      return structuredClone(previewRegistry);
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<RegistrySnapshot>('desktop_registry_apply', {
      expectedRevision,
      change,
    });
  } catch (err) {
    throw normalizeAppError(err, 'registry_apply_failed', 'Failed to apply registry change.');
  }
}

export async function desktop_probe_tool(args: DesktopProbeToolArgs): Promise<ToolProbe>;
export async function desktop_probe_tool(toolId: string): Promise<ToolProbe>;
export async function desktop_probe_tool(
  arg: DesktopProbeToolArgs | { tool_id?: string; toolId?: string } | string
): Promise<ToolProbe> {
  const toolId = (
    typeof arg === 'string' ? arg : arg.toolId ?? (arg as { tool_id?: string }).tool_id ?? ''
  ).trim();
  if (!toolId) {
    throw new AppIpcError('invalid_input', 'Tool ID is required.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      const tool = previewRegistry.tools.find((t) => t.id === toolId);
      if (!tool) {
        throw new AppIpcError('tool_not_found', `Tool "${toolId}" is not in the registry.`);
      }
      if (isApiToolAdapter(tool.adapter)) {
        const configured = previewSecrets.get(tool.id) === true;
        return {
          tool_id: tool.id,
          availability: 'api_adapter',
          version: null,
          auth_status: configured ? 'configured' : 'not_configured',
          models: [],
          message: 'API adapter does not spawn local CLI processes.',
        };
      }
      return {
        tool_id: tool.id,
        availability: 'found',
        version: 'preview-fixture',
        auth_status: 'unknown',
        models: previewRegistry.models.filter((m) => m.tool_id === tool.id),
        message: 'Preview fixture probe result (found on PATH is not login or entitlement proof).',
      };
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<ToolProbe>('desktop_probe_tool', { toolId });
  } catch (err) {
    throw normalizeAppError(err, 'probe_failed', 'Failed to probe tool.');
  }
}

export async function desktop_secret_status(args: DesktopSecretStatusArgs): Promise<string>;
export async function desktop_secret_status(toolId: string): Promise<string>;
export async function desktop_secret_status(
  arg: DesktopSecretStatusArgs | { tool_id?: string; toolId?: string } | string
): Promise<string> {
  const toolId = (
    typeof arg === 'string' ? arg : arg.toolId ?? (arg as { tool_id?: string }).tool_id ?? ''
  ).trim();
  if (!toolId) {
    throw new AppIpcError('invalid_input', 'Tool ID is required.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return previewSecrets.get(toolId) ? 'configured' : 'not configured';
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<string>('desktop_secret_status', { toolId });
  } catch (err) {
    throw normalizeAppError(
      err,
      'secret_status_failed',
      'Failed to query OS credential status.'
    );
  }
}

export async function desktop_secret_set(args: DesktopSecretSetArgs): Promise<void>;
export async function desktop_secret_set(toolId: string, secret: string): Promise<void>;
export async function desktop_secret_set(
  arg1: DesktopSecretSetArgs | { tool_id?: string; toolId?: string; secret: string } | string,
  arg2?: string
): Promise<void> {
  const toolId = (
    typeof arg1 === 'string'
      ? arg1
      : arg1.toolId ?? (arg1 as { tool_id?: string }).tool_id ?? ''
  ).trim();
  const secret = typeof arg1 === 'string' ? arg2 ?? '' : arg1.secret;
  if (!toolId) {
    throw new AppIpcError('invalid_input', 'Tool ID is required.');
  }
  if (!secret || !secret.trim()) {
    throw new AppIpcError('invalid_input', 'Credential secret cannot be empty.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      previewSecrets.set(toolId, true);
      return;
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    await invoke<void>('desktop_secret_set', { toolId, secret });
  } catch (err) {
    throw normalizeAppError(
      err,
      'secret_set_failed',
      'Failed to store credential in OS keychain.',
      secret
    );
  }
}

export async function desktop_secret_delete(args: DesktopSecretDeleteArgs): Promise<void>;
export async function desktop_secret_delete(toolId: string): Promise<void>;
export async function desktop_secret_delete(
  arg: DesktopSecretDeleteArgs | { tool_id?: string; toolId?: string } | string
): Promise<void> {
  const toolId = (
    typeof arg === 'string' ? arg : arg.toolId ?? (arg as { tool_id?: string }).tool_id ?? ''
  ).trim();
  if (!toolId) {
    throw new AppIpcError('invalid_input', 'Tool ID is required.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      previewSecrets.set(toolId, false);
      return;
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    await invoke<void>('desktop_secret_delete', { toolId });
  } catch (err) {
    throw normalizeAppError(
      err,
      'secret_delete_failed',
      'Failed to delete credential from OS keychain.'
    );
  }
}

export async function desktop_decision_models(
  args: DesktopDecisionModelsArgs
): Promise<ModelDefinition[]>;
export async function desktop_decision_models(
  toolId: string,
  provider: DecisionProvider
): Promise<ModelDefinition[]>;
export async function desktop_decision_models(
  arg1:
    | DesktopDecisionModelsArgs
    | { tool_id?: string; toolId?: string; provider: DecisionProvider }
    | string,
  arg2?: DecisionProvider
): Promise<ModelDefinition[]> {
  const toolId = (
    typeof arg1 === 'string'
      ? arg1
      : arg1.toolId ?? (arg1 as { tool_id?: string }).tool_id ?? ''
  ).trim();
  const provider = typeof arg1 === 'string' ? arg2! : arg1.provider;
  if (!toolId) {
    throw new AppIpcError('invalid_input', 'Tool ID is required.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      if (provider === 'jev') {
        return [
          {
            id: `${toolId}-jev-1.14.0`,
            tool_id: toolId,
            label: 'Jev System-One 1.14.0',
            model_arg: 'jev-1.14.0',
            efforts: [],
            allow_no_effort: true,
          },
        ];
      }
      return [
        {
          id: `${toolId}-gpt-6-luna`,
          tool_id: toolId,
          label: 'GPT-6 Luna Decision',
          model_arg: 'gpt-6-luna',
          efforts: [],
          allow_no_effort: true,
        },
      ];
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<ModelDefinition[]>('desktop_decision_models', { toolId, provider });
  } catch (err) {
    throw normalizeAppError(
      err,
      'decision_models_failed',
      'Failed to fetch decision models metadata.'
    );
  }
}

export async function desktop_test_decision(
  args: DesktopTestDecisionArgs
): Promise<DecisionAdvice>;
export async function desktop_test_decision(
  toolId: string,
  provider: DecisionProvider,
  model: string
): Promise<DecisionAdvice>;
export async function desktop_test_decision(
  arg1:
    | DesktopTestDecisionArgs
    | { tool_id?: string; toolId?: string; provider: DecisionProvider; model: string }
    | string,
  arg2?: DecisionProvider,
  arg3?: string
): Promise<DecisionAdvice> {
  const toolId = (
    typeof arg1 === 'string'
      ? arg1
      : arg1.toolId ?? (arg1 as { tool_id?: string }).tool_id ?? ''
  ).trim();
  const provider = typeof arg1 === 'string' ? arg2! : arg1.provider;
  const model = (typeof arg1 === 'string' ? arg3 ?? '' : arg1.model).trim();
  if (!toolId || !model) {
    throw new AppIpcError('invalid_input', 'Both tool ID and model are required.');
  }
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return {
        provider,
        model,
        difficulty: 'medium',
        confidence: 0.84,
        needs_review: true,
        reason_code: 'preview_synthetic_fixture',
        input_tokens: 42,
        output_tokens: 12,
      };
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<DecisionAdvice>('desktop_test_decision', {
      toolId,
      provider,
      model,
    });
  } catch (err) {
    throw normalizeAppError(
      err,
      'decision_test_failed',
      'Synthetic decision verification request failed.'
    );
  }
}

export async function desktop_plans_snapshot(): Promise<PlanRevision[]> {
  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return structuredClone(previewPlans);
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<PlanRevision[]>('desktop_plans_snapshot');
  } catch (err) {
    throw normalizeAppError(err, 'plans_snapshot_failed', 'Failed to load plans ledger.');
  }
}

export async function desktop_plan_change(
  args: DesktopPlanChangeArgs
): Promise<PlanRevision>;
export async function desktop_plan_change(
  expectedRevision: number,
  change: PlanChange
): Promise<PlanRevision>;
export async function desktop_plan_change(
  arg1:
    | DesktopPlanChangeArgs
    | { expected_revision?: number; expectedRevision?: number; change: PlanChange }
    | number,
  arg2?: PlanChange
): Promise<PlanRevision> {
  const expectedRevision =
    typeof arg1 === 'number'
      ? arg1
      : typeof arg1.expectedRevision === 'number'
        ? arg1.expectedRevision
        : (arg1 as { expected_revision?: number }).expected_revision ?? -1;
  const change = typeof arg1 === 'number' ? arg2! : arg1.change;

  validateSafeRevision(expectedRevision, 'Expected plan revision');

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      const result = applyPlanChangeLocally(previewPlans, expectedRevision, change);
      previewPlans = result.revisions;
      return structuredClone(result.updated);
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }
  try {
    return await invoke<PlanRevision>('desktop_plan_change', {
      expectedRevision,
      change,
    });
  } catch (err) {
    throw normalizeAppError(err, 'plan_change_failed', 'Failed to apply plan change.');
  }
}

export async function desktop_native_threads(
  args: DesktopNativeThreadsArgs
): Promise<NativeThreadPage>;
export async function desktop_native_threads(
  toolId: string,
  projectId?: string | null,
  cursor?: string | null
): Promise<NativeThreadPage>;
export async function desktop_native_threads(
  arg1:
    | DesktopNativeThreadsArgs
    | {
        tool_id?: string;
        toolId?: string;
        project_id?: string | null;
        projectId?: string | null;
        cursor?: string | null;
      }
    | string,
  arg2?: string | null,
  arg3?: string | null
): Promise<NativeThreadPage> {
  const toolId = (
    typeof arg1 === 'string'
      ? arg1
      : arg1.toolId ?? (arg1 as { tool_id?: string }).tool_id ?? ''
  ).trim();
  const projectId =
    typeof arg1 === 'string'
      ? arg2 ?? null
      : arg1.projectId ?? (arg1 as { project_id?: string | null }).project_id ?? null;
  const cursor = typeof arg1 === 'string' ? arg3 ?? null : arg1.cursor ?? null;

  if (!toolId) {
    throw new AppIpcError('not_configured', 'Select a Codex CLI tool to query native threads.');
  }

  if (!hasTauriRuntime()) {
    if (isDevPreviewFixtureMode()) {
      return {
        threads: [
          {
            id: '00000000-0000-4000-8000-000000000002',
            project_id: projectId || 'threadlane',
            title: 'Workspace Dispatch Gate (Preview fixture)',
            cwd: '/tmp/threadlane-dev-preview',
            status: 'idle',
            model: 'gpt-6-astra',
            effort: 'max',
          },
        ],
        next_cursor: null,
      };
    }
    throw new AppIpcError(
      'bridge_disconnected',
      'Tauri desktop bridge is not connected in this browser window.'
    );
  }

  try {
    return await invoke<NativeThreadPage>('desktop_native_threads', {
      toolId,
      projectId: projectId?.trim() || null,
      cursor: cursor?.trim() || null,
    });
  } catch (err) {
    throw normalizeAppError(err, 'native_threads_failed', 'Failed to list native Codex threads.');
  }
}

// CamelCase and ipcDesktop* aliases for interoperability across page modules
export const desktopRegistrySnapshot = desktop_registry_snapshot;
export const desktopRegistryApply = desktop_registry_apply;
export const desktopProbeTool = desktop_probe_tool;
export const desktopSecretStatus = desktop_secret_status;
export const desktopSecretSet = desktop_secret_set;
export const desktopSecretDelete = desktop_secret_delete;
export const desktopDecisionModels = desktop_decision_models;
export const desktopTestDecision = desktop_test_decision;
export const desktopPlansSnapshot = desktop_plans_snapshot;
export const desktopPlanChange = desktop_plan_change;
export const desktopNativeThreads = desktop_native_threads;

export const ipcDesktopRegistrySnapshot = desktop_registry_snapshot;
export const ipcDesktopRegistryApply = desktop_registry_apply;
export const ipcDesktopProbeTool = desktop_probe_tool;
export const ipcDesktopSecretStatus = desktop_secret_status;
export const ipcDesktopSecretSet = desktop_secret_set;
export const ipcDesktopSecretDelete = desktop_secret_delete;
export const ipcDesktopDecisionModels = desktop_decision_models;
export const ipcDesktopTestDecision = desktop_test_decision;
export const ipcDesktopPlansSnapshot = desktop_plans_snapshot;
export const ipcDesktopPlanChange = desktop_plan_change;
export const ipcDesktopNativeThreads = desktop_native_threads;
