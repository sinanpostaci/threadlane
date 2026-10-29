import type { Attachment, BindingSummary } from '../types.ts';
import {
  AppIpcError,
  MAX_SAFE_REVISION,
  validateCanonicalWriteScopeEntry,
  type Difficulty,
  type PlanRevision,
  type PlanStatus,
  type RegistrySnapshot,
  type ToolAdapter,
  type WorkflowRole,
} from '../registry/ipc.ts';

export const MIN_APPROVAL_MAX_PARALLEL = 1;
export const MAX_APPROVAL_MAX_PARALLEL = 8;
export const DEFAULT_APPROVAL_MAX_PARALLEL = 2;
export const MAX_APPROVED_TASKS = 64;
export const MAX_APPROVED_WRITE_PATHS = 64;

export interface ExecutionBudgets {
  timeout_ms: number;
  max_line_bytes: number;
  max_stdout_bytes: number;
  max_stderr_bytes: number;
  max_events: number;
}

export const DEFAULT_EXECUTION_BUDGETS: Readonly<ExecutionBudgets> = {
  timeout_ms: 300_000,
  max_line_bytes: 262_144,
  max_stdout_bytes: 4_194_304,
  max_stderr_bytes: 262_144,
  max_events: 256,
};

export const EXECUTION_BUDGET_BOUNDS = {
  timeout_ms: { min: 1, max: 3_600_000 },
  max_line_bytes: { min: 1, max: 4_194_304 },
  max_stdout_bytes: { min: 1, max: 33_554_432 },
  max_stderr_bytes: { min: 1, max: 4_194_304 },
  max_events: { min: 1, max: 4_096 },
} as const;

export interface OperatorApprovalRequest {
  binding_id: string;
  thread_uuid: string;
  plan_id: string;
  plan_revision: number;
  expected_registry_revision: number;
  expected_plan_digest: string;
  repository_path: string;
  base_commit: string;
  approved_task_ids: string[];
  approved_write_paths: string[];
  max_parallel: number;
  budgets: ExecutionBudgets;
}

export interface ApprovalSummary {
  approval_id: string;
  subject_digest: string;
  policy_version: number;
  binding_id: string;
  thread_uuid: string;
  plan_id: string;
  plan_revision: number;
  profile_id: string;
  registry_revision: number;
  plan_digest: string;
  repository_path: string;
  base_commit: string;
  approved_task_ids: string[];
  approved_write_paths: string[];
  max_parallel: number;
  worker_profile_count: number;
  native_route_count: number;
  created_at_ms: number;
}

export interface PlanTaskExecutionPreview {
  id: string;
  title: string;
  instruction: string;
  instruction_digest: string;
  role: WorkflowRole;
  difficulty: Difficulty;
  tool_id: string;
  tool_label: string;
  adapter: ToolAdapter;
  model_id: string;
  model_label: string;
  model_arg: string;
  effort: string | null;
  depends_on: string[];
  allowed_write_paths: string[];
  task_digest: string;
}

export interface PlanExecutionPreview {
  plan_id: string;
  plan_revision: number;
  binding_id: string;
  profile_id: string;
  registry_revision: number;
  status: PlanStatus;
  topological_order: string[];
  tasks: PlanTaskExecutionPreview[];
  plan_digest: string;
}

export interface DesktopPlanExecutionPreviewArgs {
  planId: string;
  revision: number;
}

export interface DesktopApprovePlanArgs {
  request: OperatorApprovalRequest;
}

export interface DesktopPlanApprovalsArgs {
  bindingId: string;
}

export type TaskApprovalDispositionKind =
  | 'root_headless'
  | 'dependent_headless'
  | 'native_route'
  | 'unsupported_adapter';

export interface TaskApprovalDisposition {
  kind: TaskApprovalDispositionKind;
  isHeadlessWorker: boolean;
  isRootTask: boolean;
  selectable: boolean;
  defaultSelected: boolean;
  badgeLabel: string;
  badgeTone: 'active' | 'warn' | 'default';
  note: string;
}

export interface ApprovalFormDraft {
  planId: string;
  planRevision: number;
  bindingId: string;
  expanded: boolean;
  preview: PlanExecutionPreview | null;
  previewIsSampleFixture: boolean;
  previewLoading: boolean;
  submitting: boolean;
  errorMessage: string | null;
  statusNotice: string | null;
  repositoryPath: string;
  baseCommit: string;
  selectedTaskIds: string[];
  approvedWritePathsText: string;
  maxParallelText: string;
  timeoutMsText: string;
  maxLineBytesText: string;
  maxStdoutBytesText: string;
  maxStderrBytesText: string;
  maxEventsText: string;
  reviewConfirmed: boolean;
  confirmedSubjectSignature: string | null;
}

export type AuthoritativeBindingResolution =
  | {
      ok: true;
      binding: BindingSummary;
      threadUuid: string;
      attachmentIdentity: string;
    }
  | {
      ok: false;
      code: string;
      reason: string;
    };

const CANONICAL_UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const NIL_UUID = '00000000-0000-0000-0000-000000000000';
const EXACT_40_HEX_SHA_RE = /^[0-9a-fA-F]{40}$/;
const SHA256_HEX_RE = /^[0-9a-f]{64}$/;

export function isActivePublishedPlanStatus(status: PlanStatus): boolean {
  return status === 'published' || status === 'received' || status === 'applied';
}

export function isCanonicalNonNilThreadUuid(raw: string): boolean {
  return CANONICAL_UUID_RE.test(raw) && raw !== NIL_UUID;
}

export function isExact40HexCommitSha(raw: string): boolean {
  return EXACT_40_HEX_SHA_RE.test(raw);
}

export function isSha256HexDigest(raw: string): boolean {
  return SHA256_HEX_RE.test(raw);
}

export function resolveAuthoritativeBindingForApproval(
  bindings: readonly BindingSummary[],
  bindingId: string,
  attachment?: Attachment | null
): AuthoritativeBindingResolution {
  const trimmed = bindingId.trim();
  if (!trimmed) {
    return {
      ok: false,
      code: 'invalid_input',
      reason: 'Plan revision is missing a conversation binding ID.',
    };
  }
  let attachmentIdentity = 'runtime:connected:';
  if (attachment !== undefined) {
    if (attachment === null || attachment.mode === 'unconfigured') {
      return {
        ok: false,
        code: 'runtime_not_attached',
        reason: 'Attach a live Threadlane runtime before approving a plan for execution.',
      };
    }
    if (attachment.mode === 'demo') {
      return {
        ok: false,
        code: 'demo_binding_forbidden',
        reason: 'Local demo mode cannot record operator execution approvals.',
      };
    }
    if (attachment.connection !== 'connected') {
      return {
        ok: false,
        code: 'runtime_offline',
        reason:
          'Attached Threadlane runtime must be connected to verify conversation binding identity.',
      };
    }
    const stateDir = attachment.state_dir?.trim() ?? '';
    if (!stateDir) {
      return {
        ok: false,
        code: 'runtime_not_attached',
        reason: 'Attached runtime state directory is not configured.',
      };
    }
    attachmentIdentity = `${attachment.mode}:${attachment.connection}:${stateDir}`;
  }

  const binding = bindings.find((candidate) => candidate.binding_id === trimmed);
  if (!binding) {
    return {
      ok: false,
      code: 'binding_not_found',
      reason: `Binding "${trimmed}" is not present in the attached live runtime.`,
    };
  }
  if (binding.is_local_demo) {
    return {
      ok: false,
      code: 'demo_binding_forbidden',
      reason: 'Local demo bindings cannot be approved for worker execution.',
    };
  }
  if (!isCanonicalNonNilThreadUuid(binding.thread_uuid)) {
    return {
      ok: false,
      code: 'invalid_thread_uuid',
      reason: 'Attached runtime binding does not expose a canonical non-nil native thread UUID.',
    };
  }
  return {
    ok: true,
    binding,
    threadUuid: binding.thread_uuid,
    attachmentIdentity,
  };
}

export function classifyTaskApprovalDisposition(
  task: Pick<PlanTaskExecutionPreview, 'adapter' | 'depends_on'>
): TaskApprovalDisposition {
  const isHeadless = task.adapter === 'antigravity' || task.adapter === 'opencode_v2';
  const isRoot = task.depends_on.length === 0;

  if (isHeadless && isRoot) {
    return {
      kind: 'root_headless',
      isHeadlessWorker: true,
      isRootTask: true,
      selectable: true,
      defaultSelected: true,
      badgeLabel: 'Root worker · ready',
      badgeTone: 'active',
      note: 'Root headless CLI worker task; freezes an isolated worker profile on approval (not started automatically).',
    };
  }

  if (isHeadless && !isRoot) {
    return {
      kind: 'dependent_headless',
      isHeadlessWorker: true,
      isRootTask: false,
      selectable: true,
      defaultSelected: false,
      badgeLabel: 'Blocked · pending inputs',
      badgeTone: 'warn',
      note: `Depends on upstream task(s): ${task.depends_on.join(', ')}. Blocked pending upstream inputs; approval does not run an automatic DAG scheduler.`,
    };
  }

  if (task.adapter === 'codex_cli') {
    const depSuffix = isRoot
      ? ''
      : ` after upstream task(s) (${task.depends_on.join(', ')})`;
    return {
      kind: 'native_route',
      isHeadlessWorker: false,
      isRootTask: isRoot,
      selectable: true,
      defaultSelected: false,
      badgeLabel: 'Manual native route',
      badgeTone: 'default',
      note: `Manual native Codex conversation route${depSuffix}; never synthesized into a headless worker profile.`,
    };
  }

  return {
    kind: 'unsupported_adapter',
    isHeadlessWorker: false,
    isRootTask: isRoot,
    selectable: false,
    defaultSelected: false,
    badgeLabel: 'Unsupported adapter',
    badgeTone: 'warn',
    note: `Adapter "${task.adapter}" is not a worker or native conversation adapter and cannot execute plan tasks.`,
  };
}

export function computeInitialSelectedTaskIds(preview: PlanExecutionPreview): string[] {
  return preview.tasks
    .filter((task) => classifyTaskApprovalDisposition(task).defaultSelected)
    .map((task) => task.id);
}

export function deriveDefaultApprovedWritePaths(
  preview: PlanExecutionPreview,
  selectedTaskIds: readonly string[]
): string[] {
  const selectedSet = new Set(selectedTaskIds);
  const sourceTasks =
    selectedSet.size > 0
      ? preview.tasks.filter((task) => selectedSet.has(task.id))
      : preview.tasks.filter((task) => classifyTaskApprovalDisposition(task).defaultSelected);
  const fallbackTasks = sourceTasks.length > 0 ? sourceTasks : preview.tasks;

  const paths: string[] = [];
  for (const task of fallbackTasks) {
    for (const rawPath of task.allowed_write_paths) {
      const trimmed = rawPath.trim();
      if (trimmed && !paths.includes(trimmed)) {
        paths.push(trimmed);
      }
    }
  }
  return paths;
}

export function parseWritePathsInput(rawText: string): string[] {
  const entries: string[] = [];
  for (const part of rawText.split(/[\n,]+/)) {
    const trimmed = part.trim();
    if (trimmed && !entries.includes(trimmed)) {
      entries.push(trimmed);
    }
  }
  return entries;
}

export function isTaskWriteScopeContainedInAllowlist(
  taskScope: string,
  operatorAllowlist: readonly string[]
): boolean {
  const trimmedTask = taskScope.trim();
  if (!trimmedTask) {
    return false;
  }
  const taskIsDir = trimmedTask.endsWith('/');
  const cleanTask = taskIsDir ? trimmedTask.slice(0, -1) : trimmedTask;
  if (!cleanTask || cleanTask.endsWith('/')) {
    return false;
  }

  return operatorAllowlist.some((rawOp) => {
    const opScope = rawOp.trim();
    if (!opScope) {
      return false;
    }
    if (opScope.endsWith('/')) {
      const cleanOp = opScope.slice(0, -1);
      if (!cleanOp || cleanOp.endsWith('/')) {
        return false;
      }
      if (cleanTask === cleanOp) {
        return taskIsDir;
      }
      return cleanTask.startsWith(`${cleanOp}/`);
    }
    return !taskIsDir && trimmedTask === opScope;
  });
}

export function createInitialApprovalFormDraft(
  planId: string,
  planRevision: number,
  bindingId: string
): ApprovalFormDraft {
  return {
    planId,
    planRevision,
    bindingId,
    expanded: false,
    preview: null,
    previewIsSampleFixture: false,
    previewLoading: false,
    submitting: false,
    errorMessage: null,
    statusNotice: null,
    repositoryPath: '',
    baseCommit: '',
    selectedTaskIds: [],
    approvedWritePathsText: '',
    maxParallelText: String(DEFAULT_APPROVAL_MAX_PARALLEL),
    timeoutMsText: String(DEFAULT_EXECUTION_BUDGETS.timeout_ms),
    maxLineBytesText: String(DEFAULT_EXECUTION_BUDGETS.max_line_bytes),
    maxStdoutBytesText: String(DEFAULT_EXECUTION_BUDGETS.max_stdout_bytes),
    maxStderrBytesText: String(DEFAULT_EXECUTION_BUDGETS.max_stderr_bytes),
    maxEventsText: String(DEFAULT_EXECUTION_BUDGETS.max_events),
    reviewConfirmed: false,
    confirmedSubjectSignature: null,
  };
}

function resolveAttachmentIdentity(attachment?: Attachment | null): string | null {
  if (attachment === undefined) {
    return 'runtime:connected:';
  }
  if (
    attachment === null ||
    attachment.mode !== 'runtime' ||
    attachment.connection !== 'connected'
  ) {
    return null;
  }
  const stateDir = attachment.state_dir?.trim() ?? '';
  if (!stateDir) {
    return null;
  }
  return `${attachment.mode}:${attachment.connection}:${stateDir}`;
}

export function computeApprovalSubjectSignature(
  draft: ApprovalFormDraft,
  authoritativeThreadUuid: string | null,
  attachment?: Attachment | null,
  selectedBindingId?: string | null
): string | null {
  if (!draft.preview || !authoritativeThreadUuid) {
    return null;
  }
  if (!isCanonicalNonNilThreadUuid(authoritativeThreadUuid)) {
    return null;
  }
  const attachmentIdentity = resolveAttachmentIdentity(attachment);
  if (attachmentIdentity === null) {
    return null;
  }

  const orderedSelectedTaskIds = draft.preview.tasks
    .filter((task) => draft.selectedTaskIds.includes(task.id))
    .map((task) => task.id);

  const canonicalPayload = {
    domain: 'threadlane-desktop-approval-review-subject-v1',
    plan_id: draft.preview.plan_id,
    plan_revision: draft.preview.plan_revision,
    binding_id: draft.preview.binding_id,
    profile_id: draft.preview.profile_id,
    registry_revision: draft.preview.registry_revision,
    plan_digest: draft.preview.plan_digest,
    tasks: draft.preview.tasks.map((task) => ({
      id: task.id,
      task_digest: task.task_digest,
      instruction_digest: task.instruction_digest,
      tool_id: task.tool_id,
      adapter: task.adapter,
      model_id: task.model_id,
      model_arg: task.model_arg,
      effort: task.effort,
      depends_on: task.depends_on,
      allowed_write_paths: task.allowed_write_paths,
    })),
    thread_uuid: authoritativeThreadUuid,
    attachment_identity: attachmentIdentity,
    selected_binding_id: selectedBindingId ?? null,
    repository_path: draft.repositoryPath.trim(),
    base_commit: draft.baseCommit.trim().toLowerCase(),
    approved_task_ids: orderedSelectedTaskIds,
    approved_write_paths: parseWritePathsInput(draft.approvedWritePathsText),
    max_parallel: draft.maxParallelText.trim(),
    budgets: {
      timeout_ms: draft.timeoutMsText.trim(),
      max_line_bytes: draft.maxLineBytesText.trim(),
      max_stdout_bytes: draft.maxStdoutBytesText.trim(),
      max_stderr_bytes: draft.maxStderrBytesText.trim(),
      max_events: draft.maxEventsText.trim(),
    },
  };

  return JSON.stringify(canonicalPayload);
}

export function applyLoadedExecutionPreview(
  draft: ApprovalFormDraft,
  preview: PlanExecutionPreview,
  isSampleFixture: boolean = false
): ApprovalFormDraft {
  const initialSelected = computeInitialSelectedTaskIds(preview);
  const existingValidSelected = preview.tasks
    .filter(
      (task) =>
        draft.selectedTaskIds.includes(task.id) &&
        classifyTaskApprovalDisposition(task).selectable
    )
    .map((task) => task.id);

  const selectedTaskIds =
    draft.preview !== null && existingValidSelected.length > 0
      ? existingValidSelected
      : initialSelected;

  const currentPaths = parseWritePathsInput(draft.approvedWritePathsText);
  const defaultPaths = deriveDefaultApprovedWritePaths(preview, selectedTaskIds);
  const nextWritePathsText =
    currentPaths.length > 0 ? draft.approvedWritePathsText : defaultPaths.join(', ');

  return {
    ...draft,
    preview,
    previewIsSampleFixture: isSampleFixture,
    previewLoading: false,
    errorMessage: null,
    selectedTaskIds,
    approvedWritePathsText: nextWritePathsText,
    reviewConfirmed: false,
    confirmedSubjectSignature: null,
  };
}

export type EditableApprovalDraftField =
  | 'repositoryPath'
  | 'baseCommit'
  | 'approvedWritePathsText'
  | 'maxParallelText'
  | 'timeoutMsText'
  | 'maxLineBytesText'
  | 'maxStdoutBytesText'
  | 'maxStderrBytesText'
  | 'maxEventsText';

export function updateApprovalDraftField(
  draft: ApprovalFormDraft,
  field: EditableApprovalDraftField,
  value: string
): ApprovalFormDraft {
  if (draft[field] === value) {
    return draft;
  }
  return {
    ...draft,
    [field]: value,
    reviewConfirmed: false,
    confirmedSubjectSignature: null,
    errorMessage: null,
  };
}

export function toggleApprovalTaskSelection(
  draft: ApprovalFormDraft,
  taskId: string,
  checked: boolean
): ApprovalFormDraft {
  if (!draft.preview) {
    return draft;
  }
  const targetTask = draft.preview.tasks.find((task) => task.id === taskId);
  if (!targetTask || !classifyTaskApprovalDisposition(targetTask).selectable) {
    return draft;
  }

  const prevSet = new Set(draft.selectedTaskIds);
  if (checked) {
    prevSet.add(taskId);
  } else {
    prevSet.delete(taskId);
  }

  const orderedSelected = draft.preview.tasks
    .filter((task) => prevSet.has(task.id))
    .map((task) => task.id);

  const existingPaths = parseWritePathsInput(draft.approvedWritePathsText);
  for (const pathEntry of targetTask.allowed_write_paths) {
    const trimmed = pathEntry.trim();
    if (checked && trimmed && !existingPaths.includes(trimmed)) {
      existingPaths.push(trimmed);
    }
  }

  return {
    ...draft,
    selectedTaskIds: orderedSelected,
    approvedWritePathsText: existingPaths.join(', '),
    reviewConfirmed: false,
    confirmedSubjectSignature: null,
    errorMessage: null,
  };
}

export function syncApprovalDraftWithContext(
  draft: ApprovalFormDraft,
  plan: PlanRevision,
  registry: RegistrySnapshot | null,
  bindings: readonly BindingSummary[],
  attachment?: Attachment | null,
  selectedBindingId?: string | null
): ApprovalFormDraft {
  const bindingRes = resolveAuthoritativeBindingForApproval(
    bindings,
    plan.binding_id,
    attachment
  );
  const authoritativeThreadUuid = bindingRes.ok ? bindingRes.threadUuid : null;

  let nextPreview = draft.preview;
  let nextPreviewIsSample = draft.previewIsSampleFixture;
  let staleNotice: string | null = null;

  if (nextPreview !== null) {
    const planActive = isActivePublishedPlanStatus(plan.status);
    const matchesPlanIdentity =
      nextPreview.plan_id === plan.plan_id &&
      nextPreview.plan_revision === plan.revision &&
      nextPreview.binding_id === plan.binding_id &&
      nextPreview.profile_id === plan.profile_id;
    const matchesRegistryRevision =
      registry !== null &&
      registry.revision === plan.profile_revision &&
      nextPreview.registry_revision === registry.revision;

    if (!planActive || !matchesPlanIdentity || !matchesRegistryRevision) {
      nextPreview = null;
      nextPreviewIsSample = false;
      staleNotice =
        'Prior execution preview was cleared because the plan status or registry revision changed. Preview execution again to review current settings.';
    }
  }

  const candidateDraft: ApprovalFormDraft = {
    ...draft,
    bindingId: plan.binding_id,
    preview: nextPreview,
    previewIsSampleFixture: nextPreviewIsSample,
  };

  const currentSig = computeApprovalSubjectSignature(
    candidateDraft,
    authoritativeThreadUuid,
    attachment,
    selectedBindingId
  );
  const confirmationStillValid =
    candidateDraft.reviewConfirmed &&
    currentSig !== null &&
    candidateDraft.confirmedSubjectSignature === currentSig;

  if (
    nextPreview === draft.preview &&
    confirmationStillValid === draft.reviewConfirmed &&
    draft.bindingId === plan.binding_id
  ) {
    return draft;
  }

  return {
    ...candidateDraft,
    reviewConfirmed: confirmationStillValid,
    confirmedSubjectSignature: confirmationStillValid ? currentSig : null,
    statusNotice: staleNotice ?? draft.statusNotice,
  };
}

function parseBoundedInteger(
  rawText: string,
  fieldLabel: string,
  min: number,
  max: number
): number {
  const trimmed = rawText.trim();
  if (!/^\d+$/.test(trimmed)) {
    throw new AppIpcError('validation', `${fieldLabel} must be a whole number.`);
  }
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    throw new AppIpcError(
      'validation',
      `${fieldLabel} must be between ${min.toLocaleString('en-US')} and ${max.toLocaleString('en-US')}.`
    );
  }
  return parsed;
}

function validateRepositoryPathInput(rawPath: string): string {
  const trimmed = rawPath.trim();
  if (!trimmed) {
    throw new AppIpcError(
      'invalid_repository',
      'Enter the canonical absolute Git repository path.'
    );
  }
  if (!trimmed.startsWith('/') || trimmed.includes('\0')) {
    throw new AppIpcError(
      'invalid_repository',
      'Repository path must be an absolute filesystem path starting with "/".'
    );
  }
  const segments = trimmed.split('/');
  if (segments.some((seg) => seg === '.' || seg === '..')) {
    throw new AppIpcError(
      'invalid_repository',
      'Repository path must be canonical and cannot contain "." or ".." segments.'
    );
  }
  return trimmed;
}

function validateWriteScopeEntryFormat(entry: string): string {
  const trimmed = entry.trim();
  return validateCanonicalWriteScopeEntry(trimmed, 'invalid_write_scope');
}

export function validateApprovalDraftInputs(
  draft: ApprovalFormDraft,
  bindings: readonly BindingSummary[],
  attachment?: Attachment | null
): {
  preview: PlanExecutionPreview;
  threadUuid: string;
  repositoryPath: string;
  baseCommit: string;
  approvedTaskIds: string[];
  approvedWritePaths: string[];
  maxParallel: number;
  budgets: ExecutionBudgets;
} {
  if (!draft.preview) {
    throw new AppIpcError(
      'preview_required',
      'Preview execution first to inspect resolved models, efforts, instructions, and digests.'
    );
  }
  if (draft.previewIsSampleFixture) {
    throw new AppIpcError(
      'preview_read_only',
      'Browser preview mode is a read-only fixture and cannot record operator execution approvals.'
    );
  }

  const preview = draft.preview;
  if (
    !Number.isSafeInteger(preview.plan_revision) ||
    preview.plan_revision <= 0 ||
    preview.plan_revision > MAX_SAFE_REVISION ||
    !Number.isSafeInteger(preview.registry_revision) ||
    preview.registry_revision < 0 ||
    preview.registry_revision > MAX_SAFE_REVISION
  ) {
    throw new AppIpcError('validation', 'Preview revision numbers are invalid.');
  }
  if (!isSha256HexHex(preview.plan_digest)) {
    throw new AppIpcError('validation', 'Preview plan digest is invalid.');
  }

  const bindingRes = resolveAuthoritativeBindingForApproval(
    bindings,
    preview.binding_id,
    attachment
  );
  if (!bindingRes.ok) {
    throw new AppIpcError(bindingRes.code, bindingRes.reason);
  }

  const repositoryPath = validateRepositoryPathInput(draft.repositoryPath);
  const rawCommit = draft.baseCommit.trim();
  if (!isExact40HexCommitSha(rawCommit)) {
    throw new AppIpcError(
      'invalid_base_commit',
      'Base commit must be an exact 40-character hexadecimal Git commit SHA.'
    );
  }
  const baseCommit = rawCommit.toLowerCase();

  const selectedSet = new Set(draft.selectedTaskIds);
  const selectedTasks = preview.tasks.filter((task) => selectedSet.has(task.id));
  if (selectedTasks.length === 0) {
    throw new AppIpcError(
      'validation',
      'Select at least one task from the execution preview to approve.'
    );
  }
  if (selectedTasks.length > MAX_APPROVED_TASKS) {
    throw new AppIpcError(
      'validation',
      `Cannot approve more than ${MAX_APPROVED_TASKS} tasks in a single request.`
    );
  }

  for (const task of selectedTasks) {
    const disposition = classifyTaskApprovalDisposition(task);
    if (!disposition.selectable) {
      throw new AppIpcError(
        'unsupported_task_adapter',
        `Task "${task.id}" uses unsupported adapter "${task.adapter}".`
      );
    }
  }

  const rawWritePaths = parseWritePathsInput(draft.approvedWritePathsText);
  if (rawWritePaths.length === 0) {
    throw new AppIpcError(
      'invalid_write_scope',
      'Specify at least one operator-approved repository-relative write path.'
    );
  }
  if (rawWritePaths.length > MAX_APPROVED_WRITE_PATHS) {
    throw new AppIpcError(
      'invalid_write_scope',
      `Cannot specify more than ${MAX_APPROVED_WRITE_PATHS} approved write paths.`
    );
  }
  const approvedWritePaths = rawWritePaths.map(validateWriteScopeEntryFormat);

  for (const task of selectedTasks) {
    for (const taskPath of task.allowed_write_paths) {
      if (!isTaskWriteScopeContainedInAllowlist(taskPath, approvedWritePaths)) {
        throw new AppIpcError(
          'unapproved_task_write_scope',
          `Selected task "${task.id}" write path "${taskPath}" is not covered by operator approved write paths.`
        );
      }
    }
  }

  const maxParallel = parseBoundedInteger(
    draft.maxParallelText,
    'Max parallel workers',
    MIN_APPROVAL_MAX_PARALLEL,
    MAX_APPROVAL_MAX_PARALLEL
  );

  const timeoutMs = parseBoundedInteger(
    draft.timeoutMsText,
    'Timeout (ms)',
    EXECUTION_BUDGET_BOUNDS.timeout_ms.min,
    EXECUTION_BUDGET_BOUNDS.timeout_ms.max
  );
  const maxLineBytes = parseBoundedInteger(
    draft.maxLineBytesText,
    'Max line bytes',
    EXECUTION_BUDGET_BOUNDS.max_line_bytes.min,
    EXECUTION_BUDGET_BOUNDS.max_line_bytes.max
  );
  const maxStdoutBytes = parseBoundedInteger(
    draft.maxStdoutBytesText,
    'Max stdout bytes',
    EXECUTION_BUDGET_BOUNDS.max_stdout_bytes.min,
    EXECUTION_BUDGET_BOUNDS.max_stdout_bytes.max
  );
  if (maxStdoutBytes < maxLineBytes) {
    throw new AppIpcError(
      'validation',
      'Max stdout bytes must be greater than or equal to max line bytes.'
    );
  }
  const maxStderrBytes = parseBoundedInteger(
    draft.maxStderrBytesText,
    'Max stderr bytes',
    EXECUTION_BUDGET_BOUNDS.max_stderr_bytes.min,
    EXECUTION_BUDGET_BOUNDS.max_stderr_bytes.max
  );
  const maxEvents = parseBoundedInteger(
    draft.maxEventsText,
    'Max events',
    EXECUTION_BUDGET_BOUNDS.max_events.min,
    EXECUTION_BUDGET_BOUNDS.max_events.max
  );

  return {
    preview,
    threadUuid: bindingRes.threadUuid,
    repositoryPath,
    baseCommit,
    approvedTaskIds: selectedTasks.map((task) => task.id),
    approvedWritePaths,
    maxParallel,
    budgets: {
      timeout_ms: timeoutMs,
      max_line_bytes: maxLineBytes,
      max_stdout_bytes: maxStdoutBytes,
      max_stderr_bytes: maxStderrBytes,
      max_events: maxEvents,
    },
  };
}

function isSha256HexHex(raw: string): boolean {
  return SHA256_HEX_RE.test(raw);
}

export function setApprovalReviewConfirmed(
  draft: ApprovalFormDraft,
  confirmed: boolean,
  bindings: readonly BindingSummary[],
  attachment?: Attachment | null,
  selectedBindingId?: string | null
): ApprovalFormDraft {
  if (!confirmed) {
    return {
      ...draft,
      reviewConfirmed: false,
      confirmedSubjectSignature: null,
    };
  }

  const validated = validateApprovalDraftInputs(draft, bindings, attachment);
  const signature = computeApprovalSubjectSignature(
    draft,
    validated.threadUuid,
    attachment,
    selectedBindingId
  );
  if (!signature) {
    throw new AppIpcError(
      'review_confirmation_required',
      'Complete all required approval fields and preview execution before confirming review.'
    );
  }

  return {
    ...draft,
    reviewConfirmed: true,
    confirmedSubjectSignature: signature,
    errorMessage: null,
  };
}

export function canSubmitApprovalDraft(
  draft: ApprovalFormDraft,
  bindings: readonly BindingSummary[],
  attachment?: Attachment | null,
  selectedBindingId?: string | null
): boolean {
  if (
    !draft.preview ||
    draft.previewIsSampleFixture ||
    draft.previewLoading ||
    draft.submitting ||
    !draft.reviewConfirmed ||
    !draft.confirmedSubjectSignature
  ) {
    return false;
  }
  try {
    const validated = validateApprovalDraftInputs(draft, bindings, attachment);
    const expectedSig = computeApprovalSubjectSignature(
      draft,
      validated.threadUuid,
      attachment,
      selectedBindingId
    );
    return expectedSig !== null && expectedSig === draft.confirmedSubjectSignature;
  } catch {
    return false;
  }
}

export function buildOperatorApprovalRequest(
  draft: ApprovalFormDraft,
  bindings: readonly BindingSummary[],
  attachment?: Attachment | null,
  selectedBindingId?: string | null
): OperatorApprovalRequest {
  const validated = validateApprovalDraftInputs(draft, bindings, attachment);
  const currentSignature = computeApprovalSubjectSignature(
    draft,
    validated.threadUuid,
    attachment,
    selectedBindingId
  );

  if (
    !draft.reviewConfirmed ||
    !draft.confirmedSubjectSignature ||
    currentSignature === null ||
    draft.confirmedSubjectSignature !== currentSignature
  ) {
    throw new AppIpcError(
      'review_confirmation_required',
      'Confirm the review checkbox for the current execution preview and form fields before approving.'
    );
  }

  return {
    binding_id: validated.preview.binding_id,
    thread_uuid: validated.threadUuid,
    plan_id: validated.preview.plan_id,
    plan_revision: validated.preview.plan_revision,
    expected_registry_revision: validated.preview.registry_revision,
    expected_plan_digest: validated.preview.plan_digest,
    repository_path: validated.repositoryPath,
    base_commit: validated.baseCommit,
    approved_task_ids: validated.approvedTaskIds,
    approved_write_paths: validated.approvedWritePaths,
    max_parallel: validated.maxParallel,
    budgets: validated.budgets,
  };
}
