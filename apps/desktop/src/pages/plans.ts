import { createButton, h } from '../ui/dom.ts';
import { formatRelativeTimestamp } from '../format.ts';
import type { Attachment, BindingSummary } from '../types.ts';
import {
  desktop_plan_change,
  DIFFICULTIES,
  formatAppError,
  hasTauriRuntime,
  isAppError,
  isDevPreviewFixtureMode,
  resolve_choice,
  validatePlanTasksDag,
  WORKFLOW_ROLES,
  type DeliveryReceipt,
  type Difficulty,
  type PlanRevision,
  type PlanTask,
  type RegistrySnapshot,
  type ToolAdapter,
  type WorkflowRole,
} from '../registry/ipc.ts';
import {
  applyLoadedExecutionPreview,
  buildOperatorApprovalRequest,
  canSubmitApprovalDraft,
  classifyTaskApprovalDisposition,
  createInitialApprovalFormDraft,
  desktop_approve_plan,
  desktop_plan_approvals,
  desktop_plan_execution_preview,
  EXECUTION_BUDGET_BOUNDS,
  isActivePublishedPlanStatus,
  MAX_APPROVAL_MAX_PARALLEL,
  MIN_APPROVAL_MAX_PARALLEL,
  resolveAuthoritativeBindingForApproval,
  setApprovalReviewConfirmed,
  syncApprovalDraftWithContext,
  toggleApprovalTaskSelection,
  updateApprovalDraftField,
  validateApprovalDraftInputs,
  type ApprovalFormDraft,
  type ApprovalSummary,
  type EditableApprovalDraftField,
  type PlanTaskExecutionPreview,
} from '../approval/ipc.ts';

if (typeof window !== 'undefined' && typeof import.meta.glob === 'function') {
  import.meta.glob('./plans.css', { eager: true });
}

const SVG_NS = 'http://www.w3.org/2000/svg';

function createInlineChevronIcon(expanded: boolean): SVGSVGElement {
  const svg = document.createElementNS(SVG_NS, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('fill', 'none');
  svg.setAttribute('stroke', 'currentColor');
  svg.setAttribute('stroke-width', '1.7');
  svg.setAttribute('stroke-linecap', 'round');
  svg.setAttribute('stroke-linejoin', 'round');
  svg.setAttribute('aria-hidden', 'true');
  const path = document.createElementNS(SVG_NS, 'path');
  path.setAttribute('d', expanded ? 'm6 9 6 6 6-6' : 'm9 18 6-6-6-6');
  svg.appendChild(path);
  return svg;
}

function formatAdapterProviderLabel(adapter: ToolAdapter): string {
  switch (adapter) {
    case 'antigravity':
      return 'Antigravity';
    case 'opencode_v2':
      return 'OpenCode v2';
    case 'codex_cli':
      return 'Native Codex';
    case 'jev_api':
      return 'Jev API';
    case 'openai_decisions_api':
      return 'OpenAI API';
    case 'git':
      return 'Git';
    case 'tmux':
      return 'tmux';
    case 'codegraph':
      return 'CodeGraph';
  }
}

export interface PlanTaskDraft {
  id: string;
  title: string;
  instruction: string;
  role: WorkflowRole;
  difficulty: Difficulty;
  dependsOnText: string;
  allowedWritePathsText: string;
}

export interface PlanFormDraft {
  planId: string;
  bindingId: string;
  profileId: string;
  summary: string;
  expectedRevision: number;
  tasks: PlanTaskDraft[];
  dirty: boolean;
}

function createDefaultTaskDraft(index: number): PlanTaskDraft {
  return {
    id: `task-${index}`,
    title: '',
    instruction: '',
    role: 'coder',
    difficulty: 'medium',
    dependsOnText: '',
    allowedWritePathsText: 'crates/',
  };
}

export function createInitialPlanDraft(
  defaultBindingId: string = '',
  defaultProfileId: string = ''
): PlanFormDraft {
  return {
    planId: 'plan-1',
    bindingId: defaultBindingId,
    profileId: defaultProfileId,
    summary: '',
    expectedRevision: 0,
    tasks: [createDefaultTaskDraft(1)],
    dirty: false,
  };
}

export function toPlanTaskDto(draft: PlanTaskDraft): PlanTask {
  return {
    id: draft.id.trim(),
    title: draft.title.trim(),
    instruction: draft.instruction.trim(),
    role: draft.role,
    difficulty: draft.difficulty,
    depends_on: draft.dependsOnText
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    allowed_write_paths: draft.allowedWritePathsText
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  };
}

export function deriveDeliveryReceipts(revisions: readonly PlanRevision[]): DeliveryReceipt[] {
  return [...revisions]
    .sort((a, b) => b.created_at_ms - a.created_at_ms || b.revision - a.revision)
    .map((rev) => ({
      plan_id: rev.plan_id,
      revision: rev.revision,
      state: rev.status,
    }));
}

function approvalDraftKey(planId: string, revision: number): string {
  return `${planId}:${revision}`;
}

export class PlansPage {
  private readonly onPlansChanged: () => void;
  private readonly container: HTMLElement;
  private draft: PlanFormDraft;
  private latestPlans: PlanRevision[] = [];
  private latestRegistry: RegistrySnapshot | null = null;
  private latestBindings: BindingSummary[] = [];
  private selectedBindingId: string | null = null;
  private latestAttachment: Attachment | null | undefined = undefined;
  private busyAction: string | null = null;
  private errorMessage: string | null = null;
  private hasCasConflict: boolean = false;
  private statusNotice: string | null = null;
  private readonly approvalDrafts = new Map<string, ApprovalFormDraft>();
  private readonly approvalsByBinding = new Map<string, ApprovalSummary[]>();
  private readonly loadingBindings = new Set<string>();
  private readonly loadedBindings = new Set<string>();

  constructor(onPlansChanged: () => void) {
    this.onPlansChanged = onPlansChanged;
    this.draft = createInitialPlanDraft();
    this.container = h('div', { className: 'aq-plans-page' });
  }

  public getDraft(): PlanFormDraft {
    return structuredClone(this.draft);
  }

  public getApprovalDraft(planId: string, revision: number): ApprovalFormDraft | null {
    const found = this.approvalDrafts.get(approvalDraftKey(planId, revision));
    return found ? structuredClone(found) : null;
  }

  public render(
    plans: PlanRevision[],
    registry: RegistrySnapshot,
    bindings: BindingSummary[],
    selectedBindingId: string | null,
    externalError: string | null = null,
    attachment?: Attachment | null
  ): HTMLElement {
    this.latestPlans = plans;
    this.latestRegistry = registry;
    this.latestBindings = bindings;
    this.selectedBindingId = selectedBindingId;
    this.latestAttachment = attachment;

    if (!this.draft.dirty) {
      if (!this.draft.bindingId && (selectedBindingId || bindings[0]?.binding_id)) {
        this.draft.bindingId = selectedBindingId ?? bindings[0]!.binding_id;
      }
      if (
        !this.draft.profileId &&
        (registry.default_profile_id || registry.profiles[0]?.id)
      ) {
        this.draft.profileId = registry.default_profile_id ?? registry.profiles[0]!.id;
      }
      const latestForPlan = this.findLatestRevision(this.draft.planId);
      if (latestForPlan) {
        this.draft.expectedRevision = latestForPlan.revision;
      }
    }

    this.syncAllApprovalDrafts();
    this.ensureBindingApprovalsLoaded(plans);
    this.rebuildDom(externalError);
    return this.container;
  }

  public syncRuntimeContext(
    bindings: BindingSummary[],
    selectedBindingId: string | null,
    attachment?: Attachment | null
  ): void {
    this.latestBindings = bindings;
    this.selectedBindingId = selectedBindingId;
    this.latestAttachment = attachment;

    if (!this.draft.dirty && !this.draft.bindingId && (selectedBindingId || bindings[0]?.binding_id)) {
      this.draft.bindingId = selectedBindingId ?? bindings[0]!.binding_id;
    }

    this.syncAllApprovalDrafts();
    this.patchLiveApprovalContextDom();
  }

  public dispose(): void {
    this.container.replaceChildren();
  }

  private syncAllApprovalDrafts(): void {
    for (const [key, draft] of this.approvalDrafts.entries()) {
      const plan = this.latestPlans.find(
        (candidate) =>
          candidate.plan_id === draft.planId && candidate.revision === draft.planRevision
      );
      if (!plan) {
        continue;
      }
      const synced = syncApprovalDraftWithContext(
        draft,
        plan,
        this.latestRegistry,
        this.latestBindings,
        this.latestAttachment,
        this.selectedBindingId
      );
      this.approvalDrafts.set(key, synced);
    }
  }

  private formatBindingResolutionText(
    bindingResolution: ReturnType<typeof resolveAuthoritativeBindingForApproval>
  ): string {
    return bindingResolution.ok
      ? `${bindingResolution.threadUuid} (${bindingResolution.binding.label})`
      : `Unavailable — ${bindingResolution.reason}`;
  }

  private formatReviewBoxInstructionText(
    draft: ApprovalFormDraft,
    inputsValidForCheckbox: boolean
  ): string {
    if (!draft.preview) {
      return '';
    }
    return inputsValidForCheckbox
      ? `I have reviewed the exact compiled digest (${draft.preview.plan_digest.slice(
          0,
          12
        )}…), resolved model/effort pins, task instructions, dependency boundaries, write scopes, repository path, base commit, and native thread UUID above. Any edit invalidates this confirmation.`
      : 'Complete the canonical repository path, 40-hex base commit SHA, selected tasks, and covering write paths above, then check this box to confirm your operator review.';
  }

  private patchLiveApprovalContextDom(): void {
    const pageBusy = this.busyAction !== null;
    for (const [key, draft] of this.approvalDrafts.entries()) {
      if (!draft.expanded) {
        continue;
      }
      const bindingResolution = resolveAuthoritativeBindingForApproval(
        this.latestBindings,
        draft.bindingId,
        this.latestAttachment
      );
      const panelBusy = pageBusy || draft.previewLoading || draft.submitting;

      const threadDd = this.container.querySelector<HTMLElement>(
        `[data-approval-thread-dd="${key}"]`
      );
      if (threadDd) {
        threadDd.textContent = this.formatBindingResolutionText(bindingResolution);
      }

      const warnEl = this.container.querySelector<HTMLElement>(
        `[data-approval-binding-warn="${key}"]`
      );
      if (warnEl) {
        if (bindingResolution.ok) {
          warnEl.style.display = 'none';
          warnEl.textContent = '';
        } else {
          warnEl.style.removeProperty('display');
          warnEl.textContent = `Cannot submit approval until a live non-demo runtime binding is connected for "${draft.bindingId}": ${bindingResolution.reason}`;
        }
      }

      const checkEl = this.container.querySelector<HTMLInputElement>(
        `input[data-approval-confirm-check="${key}"]`
      );
      if (checkEl) {
        checkEl.checked = draft.reviewConfirmed;
        checkEl.disabled =
          panelBusy || draft.previewIsSampleFixture || !bindingResolution.ok;
      }

      const reviewBoxEl = this.container.querySelector<HTMLElement>(
        `[data-approval-review-box="${key}"]`
      );
      if (reviewBoxEl) {
        reviewBoxEl.classList.toggle('is-confirmed', draft.reviewConfirmed);
      }

      const reviewTextEl = this.container.querySelector<HTMLElement>(
        `[data-approval-review-text="${key}"]`
      );
      if (reviewTextEl && draft.preview) {
        let inputsValidForCheckbox = false;
        try {
          validateApprovalDraftInputs(
            draft,
            this.latestBindings,
            this.latestAttachment
          );
          inputsValidForCheckbox = true;
        } catch {
          inputsValidForCheckbox = false;
        }
        reviewTextEl.textContent = this.formatReviewBoxInstructionText(
          draft,
          inputsValidForCheckbox
        );
      }

      const submitBtn = this.container.querySelector<HTMLButtonElement>(
        `button[data-approval-submit-btn="${key}"]`
      );
      if (submitBtn) {
        const canSubmit =
          !panelBusy &&
          canSubmitApprovalDraft(
            draft,
            this.latestBindings,
            this.latestAttachment,
            this.selectedBindingId
          );
        submitBtn.disabled = !canSubmit;
      }
    }
  }

  private ensureBindingApprovalsLoaded(plans: readonly PlanRevision[]): void {
    if (!hasTauriRuntime() && !isDevPreviewFixtureMode()) {
      return;
    }
    const bindingIds = new Set<string>();
    for (const rev of plans) {
      if (isActivePublishedPlanStatus(rev.status) && rev.binding_id.trim()) {
        bindingIds.add(rev.binding_id.trim());
      }
    }
    for (const bindingId of bindingIds) {
      if (this.loadedBindings.has(bindingId) || this.loadingBindings.has(bindingId)) {
        continue;
      }
      void this.loadApprovalsForBinding(bindingId);
    }
  }

  private async loadApprovalsForBinding(bindingId: string): Promise<void> {
    if (this.loadingBindings.has(bindingId)) {
      return;
    }
    this.loadingBindings.add(bindingId);
    try {
      const approvals = await desktop_plan_approvals({ bindingId });
      this.approvalsByBinding.set(bindingId, approvals);
      this.loadedBindings.add(bindingId);
      this.rebuildDom();
    } catch {
      // Non-fatal background fetch; explicit preview/approve actions surface errors directly.
    } finally {
      this.loadingBindings.delete(bindingId);
    }
  }

  private getOrCreateApprovalDraft(rev: PlanRevision): ApprovalFormDraft {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    let existing = this.approvalDrafts.get(key);
    if (!existing) {
      existing = createInitialApprovalFormDraft(rev.plan_id, rev.revision, rev.binding_id);
      this.approvalDrafts.set(key, existing);
    }
    const synced = syncApprovalDraftWithContext(
      existing,
      rev,
      this.latestRegistry,
      this.latestBindings,
      this.latestAttachment,
      this.selectedBindingId
    );
    this.approvalDrafts.set(key, synced);
    return synced;
  }

  private findApprovalsForRevision(planId: string, revision: number, bindingId: string): ApprovalSummary[] {
    const list = this.approvalsByBinding.get(bindingId.trim()) ?? [];
    return list.filter(
      (item) => item.plan_id === planId && item.plan_revision === revision
    );
  }

  private findLatestRevision(planId: string): PlanRevision | null {
    const trimmed = planId.trim();
    const matches = this.latestPlans
      .filter((p) => p.plan_id === trimmed)
      .sort((a, b) => b.revision - a.revision);
    return matches[0] ?? null;
  }

  private loadRevisionIntoDraft(rev: PlanRevision): void {
    const latest = this.findLatestRevision(rev.plan_id) ?? rev;
    this.draft = {
      planId: rev.plan_id,
      bindingId: rev.binding_id,
      profileId: rev.profile_id,
      summary: rev.summary,
      expectedRevision: latest.revision,
      tasks:
        rev.tasks.length > 0
          ? rev.tasks.map((t) => ({
              id: t.id,
              title: t.title,
              instruction: t.instruction,
              role: t.role,
              difficulty: t.difficulty,
              dependsOnText: t.depends_on.join(', '),
              allowedWritePathsText: t.allowed_write_paths.join(', '),
            }))
          : [createDefaultTaskDraft(1)],
      dirty: false,
    };
    this.hasCasConflict = false;
    this.errorMessage = null;
    this.statusNotice = `Loaded ${rev.plan_id} (rev ${rev.revision}) into draft editor.`;
    this.rebuildDom();
  }

  private handleStartNewDraft(): void {
    const nextIdx = new Set(this.latestPlans.map((p) => p.plan_id)).size + 1;
    const defaultBinding =
      this.selectedBindingId ?? this.latestBindings[0]?.binding_id ?? '';
    const defaultProfile =
      this.latestRegistry?.default_profile_id ??
      this.latestRegistry?.profiles[0]?.id ??
      '';
    this.draft = {
      planId: `plan-${nextIdx}`,
      bindingId: defaultBinding,
      profileId: defaultProfile,
      summary: '',
      expectedRevision: 0,
      tasks: [createDefaultTaskDraft(1)],
      dirty: false,
    };
    this.hasCasConflict = false;
    this.errorMessage = null;
    this.statusNotice = 'Initialized new empty plan draft.';
    this.rebuildDom();
  }

  private handleReloadLatestCas(): void {
    const latest = this.findLatestRevision(this.draft.planId);
    this.draft.expectedRevision = latest ? latest.revision : 0;
    this.hasCasConflict = false;
    this.errorMessage = null;
    this.statusNotice = `Synced expected revision to r${this.draft.expectedRevision}. Local form edits were preserved.`;
    this.onPlansChanged();
    this.rebuildDom();
  }

  private async handleSaveDraft(): Promise<void> {
    if (this.busyAction !== null) {
      return;
    }
    const planId = this.draft.planId.trim();
    const bindingId = this.draft.bindingId.trim();
    const profileId = this.draft.profileId.trim();
    const summary = this.draft.summary.trim();

    if (!planId) {
      this.errorMessage = 'Enter a plan ID before saving a draft.';
      this.rebuildDom();
      return;
    }
    if (!bindingId) {
      this.errorMessage = 'Enter or select a conversation binding ID.';
      this.rebuildDom();
      return;
    }
    if (!profileId) {
      this.errorMessage = 'Select or enter a workflow profile ID.';
      this.rebuildDom();
      return;
    }
    if (!summary) {
      this.errorMessage = 'Enter a concise plan summary.';
      this.rebuildDom();
      return;
    }

    const tasks = this.draft.tasks.map(toPlanTaskDto);
    try {
      validatePlanTasksDag(tasks);
    } catch (err) {
      this.errorMessage = formatAppError(err, 'Invalid plan task DAG.');
      this.rebuildDom();
      return;
    }

    const profileRevision = this.latestRegistry?.revision ?? 0;
    this.busyAction = 'save_draft';
    this.errorMessage = null;
    this.statusNotice = null;
    this.rebuildDom();

    try {
      const updated = await desktop_plan_change({
        expectedRevision: this.draft.expectedRevision,
        change: {
          op: 'save_draft',
          plan_id: planId,
          binding_id: bindingId,
          profile_id: profileId,
          profile_revision: profileRevision,
          summary,
          tasks,
        },
      });
      this.draft.expectedRevision = updated.revision;
      this.draft.dirty = false;
      this.hasCasConflict = false;
      this.statusNotice = `Saved draft ${updated.plan_id} at revision r${updated.revision} (not automatically executed).`;
      this.onPlansChanged();
    } catch (err) {
      const code = isAppError(err) ? err.code : '';
      if (code === 'revision_conflict' || /revision|conflict|stale/i.test(String(err))) {
        this.hasCasConflict = true;
      }
      this.errorMessage = formatAppError(err, 'Failed to save plan draft.');
    } finally {
      this.busyAction = null;
      this.rebuildDom();
    }
  }

  private async handleTransition(
    op: 'publish' | 'receive' | 'apply',
    planId: string,
    revision: number
  ): Promise<void> {
    if (this.busyAction !== null) {
      return;
    }
    this.busyAction = `${op}:${planId}:${revision}`;
    this.errorMessage = null;
    this.statusNotice = null;
    this.rebuildDom();

    try {
      const updated = await desktop_plan_change({
        expectedRevision: revision,
        change: {
          op,
          plan_id: planId,
          revision,
        },
      });
      if (this.draft.planId.trim() === planId) {
        this.draft.expectedRevision = updated.revision;
      }
      this.hasCasConflict = false;
      this.statusNotice = `Transitioned ${updated.plan_id} r${updated.revision} to "${updated.status}". No worker processes were spawned automatically.`;
      this.onPlansChanged();
    } catch (err) {
      const code = isAppError(err) ? err.code : '';
      if (code === 'revision_conflict' || /revision|conflict|stale/i.test(String(err))) {
        this.hasCasConflict = true;
      }
      this.errorMessage = formatAppError(err, `Failed to ${op} plan revision.`);
    } finally {
      this.busyAction = null;
      this.rebuildDom();
    }
  }

  private handleToggleApprovalPanel(rev: PlanRevision): void {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const current = this.getOrCreateApprovalDraft(rev);
    this.approvalDrafts.set(key, {
      ...current,
      expanded: !current.expanded,
    });
    if (!current.expanded && rev.binding_id.trim()) {
      void this.loadApprovalsForBinding(rev.binding_id.trim());
    }
    this.rebuildDom();
  }

  private async handlePreviewExecution(rev: PlanRevision): Promise<void> {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const current = this.getOrCreateApprovalDraft(rev);
    if (current.previewLoading || current.submitting) {
      return;
    }

    this.approvalDrafts.set(key, {
      ...current,
      expanded: true,
      previewLoading: true,
      errorMessage: null,
      statusNotice: null,
      reviewConfirmed: false,
      confirmedSubjectSignature: null,
    });
    this.rebuildDom();

    try {
      const preview = await desktop_plan_execution_preview({
        planId: rev.plan_id,
        revision: rev.revision,
      });
      const isSample = !hasTauriRuntime() && isDevPreviewFixtureMode();
      const latestDraft = this.getOrCreateApprovalDraft(rev);
      const updated = applyLoadedExecutionPreview(latestDraft, preview, isSample);
      this.approvalDrafts.set(key, {
        ...updated,
        statusNotice: isSample
          ? 'Loaded read-only sample execution preview fixture. Approval submission requires the native Tauri desktop bridge.'
          : `Compiled execution preview against registry r${preview.registry_revision} (digest ${preview.plan_digest.slice(0, 12)}…). Review fields below to approve.`,
      });
    } catch (err) {
      const latestDraft = this.getOrCreateApprovalDraft(rev);
      this.approvalDrafts.set(key, {
        ...latestDraft,
        preview: null,
        previewIsSampleFixture: false,
        previewLoading: false,
        reviewConfirmed: false,
        confirmedSubjectSignature: null,
        errorMessage: formatAppError(err, 'Failed to compile execution preview.'),
      });
    } finally {
      this.rebuildDom();
    }
  }

  private async handleApproveSelectedTasks(rev: PlanRevision): Promise<void> {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const current = this.getOrCreateApprovalDraft(rev);
    if (current.submitting || current.previewLoading) {
      return;
    }

    let request;
    try {
      request = buildOperatorApprovalRequest(
        current,
        this.latestBindings,
        this.latestAttachment,
        this.selectedBindingId
      );
    } catch (err) {
      this.approvalDrafts.set(key, {
        ...current,
        errorMessage: formatAppError(err, 'Approval form validation failed.'),
      });
      this.rebuildDom();
      return;
    }

    this.approvalDrafts.set(key, {
      ...current,
      submitting: true,
      errorMessage: null,
      statusNotice: null,
    });
    this.rebuildDom();

    try {
      const summary = await desktop_approve_plan({ request });
      const bindingId = summary.binding_id.trim();
      const existingList = this.approvalsByBinding.get(bindingId) ?? [];
      const nextList = [
        summary,
        ...existingList.filter((item) => item.approval_id !== summary.approval_id),
      ];
      this.approvalsByBinding.set(bindingId, nextList);
      this.loadedBindings.add(bindingId);

      const latestDraft = this.getOrCreateApprovalDraft(rev);
      this.approvalDrafts.set(key, {
        ...latestDraft,
        submitting: false,
        errorMessage: null,
        statusNotice: `Approved · Not started (${summary.approval_id}). Frozen operator approval artifact recorded; no worker or host process was started.`,
      });
    } catch (err) {
      const latestDraft = this.getOrCreateApprovalDraft(rev);
      const code = isAppError(err) ? err.code : '';
      const clearPreview =
        code === 'stale_registry_revision' ||
        code === 'plan_digest_mismatch' ||
        code === 'unpublished_or_superseded_plan';
      this.approvalDrafts.set(key, {
        ...latestDraft,
        submitting: false,
        preview: clearPreview ? null : latestDraft.preview,
        reviewConfirmed: false,
        confirmedSubjectSignature: null,
        errorMessage: formatAppError(err, 'Failed to record operator plan approval.'),
      });
    } finally {
      this.rebuildDom();
    }
  }

  private rebuildDom(externalError: string | null = null): void {
    const activeEl =
      typeof document !== 'undefined' ? (document.activeElement as HTMLElement | null) : null;
    const focusKey =
      activeEl && this.container.contains(activeEl) ? activeEl.dataset.focus : undefined;
    const selStart = activeEl instanceof HTMLInputElement || activeEl instanceof HTMLTextAreaElement
      ? activeEl.selectionStart
      : null;
    const selEnd = activeEl instanceof HTMLInputElement || activeEl instanceof HTMLTextAreaElement
      ? activeEl.selectionEnd
      : null;

    const registry = this.latestRegistry;
    const busy = this.busyAction !== null;

    const nodes: Node[] = [];

    nodes.push(
      h(
        'div',
        { className: 'aq-section', style: 'margin-top: 4px;' },
        h('span', undefined, 'Durable plan ledger & draft editor'),
        h(
          'div',
          { className: 'aq-actions', style: 'margin-top: 0;' },
          createButton('New draft', () => this.handleStartNewDraft(), busy),
          createButton('Refresh ledger', () => this.onPlansChanged(), busy)
        )
      )
    );

    nodes.push(
      h(
        'section',
        { className: 'aq-banner', 'aria-label': 'Plan execution policy' },
        h(
          'div',
          { className: 'aq-banner-head' },
          h('span', undefined, 'Explicit publication & operator approval'),
          h('span', { className: 'aq-badge' }, 'No automatic execution')
        ),
        h(
          'p',
          { className: 'aq-banner-sub' },
          'Saving or publishing a plan records an immutable revision in the local ledger. Publishing never grants execution authority or spawns workers. Execution approval separately freezes a verified operator artifact without starting a host or worker.'
        )
      )
    );

    const activeError = this.errorMessage ?? externalError;
    if (activeError) {
      nodes.push(
        h(
          'div',
          { className: 'aq-banner aq-banner-error', role: 'alert' },
          h(
            'div',
            undefined,
            h('span', undefined, activeError),
            this.hasCasConflict
              ? h(
                  'div',
                  { className: 'aq-actions' },
                  createButton(
                    'Reload latest revision (keep draft)',
                    () => this.handleReloadLatestCas(),
                    busy
                  )
                )
              : null
          )
        )
      );
    }

    if (this.statusNotice) {
      nodes.push(
        h(
          'div',
          { className: 'aq-banner', role: 'status' },
          h('span', undefined, this.statusNotice)
        )
      );
    }

    nodes.push(this.buildDraftEditorSection(registry, busy));
    nodes.push(this.buildLedgerReceiptsSection(registry, busy));

    this.container.replaceChildren(...nodes);

    if (focusKey) {
      const target = this.container.querySelector<HTMLElement>(`[data-focus="${focusKey}"]`);
      if (target) {
        target.focus({ preventScroll: true });
        if (
          (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) &&
          selStart !== null &&
          selEnd !== null
        ) {
          try {
            target.setSelectionRange(selStart, selEnd);
          } catch {
            // Non-text inputs do not support setSelectionRange.
          }
        }
      }
    }
  }

  private buildDraftEditorSection(
    registry: RegistrySnapshot | null,
    busy: boolean
  ): HTMLElement {
    const planIdInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: this.draft.planId,
      placeholder: 'plan-1',
      'aria-label': 'Plan ID',
      'data-focus': 'plan-draft-id',
      disabled: busy,
    });
    planIdInput.addEventListener('input', () => {
      this.draft.planId = planIdInput.value;
      const latest = this.findLatestRevision(planIdInput.value);
      this.draft.expectedRevision = latest ? latest.revision : 0;
      this.draft.dirty = true;
    });

    const bindingInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: this.draft.bindingId,
      placeholder: 'binding-id',
      'aria-label': 'Binding ID',
      'data-focus': 'plan-draft-binding',
      disabled: busy,
    });
    bindingInput.addEventListener('input', () => {
      this.draft.bindingId = bindingInput.value;
      this.draft.dirty = true;
    });

    const bindingQuickSelect =
      this.latestBindings.length > 0
        ? h(
            'select',
            {
              className: 'aq-select-input',
              'aria-label': 'Select known binding',
              'data-focus': 'plan-draft-binding-select',
              disabled: busy,
            },
            h('option', { value: '' }, 'Pick bound conversation…'),
            ...this.latestBindings.map((b) =>
              h(
                'option',
                {
                  value: b.binding_id,
                  selected: b.binding_id === this.draft.bindingId,
                },
                `${b.label} (${b.binding_id})`
              )
            )
          )
        : null;
    if (bindingQuickSelect) {
      bindingQuickSelect.addEventListener('change', () => {
        if (bindingQuickSelect.value) {
          this.draft.bindingId = bindingQuickSelect.value;
          this.draft.dirty = true;
          this.rebuildDom();
        }
      });
    }

    const profiles = registry?.profiles ?? [];
    const profileControl =
      profiles.length > 0
        ? h(
            'select',
            {
              className: 'aq-select-input',
              'aria-label': 'Workflow profile',
              'data-focus': 'plan-draft-profile',
              disabled: busy,
            },
            h('option', { value: '' }, 'Select workflow profile…'),
            ...profiles.map((p) =>
              h(
                'option',
                {
                  value: p.id,
                  selected: p.id === this.draft.profileId,
                },
                `${p.name} (${p.id})`
              )
            )
          )
        : h('input', {
            type: 'text',
            className: 'aq-text-input',
            value: this.draft.profileId,
            placeholder: 'default',
            'aria-label': 'Workflow profile ID',
            'data-focus': 'plan-draft-profile',
            disabled: busy,
          });

    profileControl.addEventListener(
      profileControl instanceof HTMLSelectElement ? 'change' : 'input',
      () => {
        this.draft.profileId = profileControl.value;
        this.draft.dirty = true;
        if (profileControl instanceof HTMLSelectElement) {
          this.rebuildDom();
        }
      }
    );

    const summaryInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: this.draft.summary,
      placeholder: 'Scoped milestone objective and verification summary',
      'aria-label': 'Plan summary',
      'data-focus': 'plan-draft-summary',
      disabled: busy,
    });
    summaryInput.addEventListener('input', () => {
      this.draft.summary = summaryInput.value;
      this.draft.dirty = true;
    });

    const taskCards = this.draft.tasks.map((task, idx) =>
      this.buildTaskDraftCard(task, idx, registry, busy)
    );

    const addTaskBtn = createButton(
      'Add task',
      () => {
        this.draft.tasks.push(createDefaultTaskDraft(this.draft.tasks.length + 1));
        this.draft.dirty = true;
        this.rebuildDom();
      },
      busy || this.draft.tasks.length >= 32
    );

    const saveDraftBtn = createButton(
      this.busyAction === 'save_draft'
        ? 'Saving draft…'
        : `Save draft (CAS r${this.draft.expectedRevision})`,
      () => {
        void this.handleSaveDraft();
      },
      busy
    );
    saveDraftBtn.classList.add('aq-btn-primary');

    const latestForCurrentPlan = this.findLatestRevision(this.draft.planId);
    const canPublishLatest =
      latestForCurrentPlan !== null &&
      latestForCurrentPlan.status === 'draft' &&
      !this.draft.dirty;

    const publishLatestBtn = canPublishLatest
      ? createButton(
          `Publish r${latestForCurrentPlan.revision}`,
          () => {
            void this.handleTransition(
              'publish',
              latestForCurrentPlan.plan_id,
              latestForCurrentPlan.revision
            );
          },
          busy
        )
      : null;

    return h(
      'section',
      { className: 'aq-card', 'aria-label': 'Plan draft editor' },
      h(
        'div',
        { className: 'aq-row-head' },
        h('span', { className: 'aq-task-name' }, 'Draft editor'),
        h(
          'span',
          { className: 'aq-badge', 'data-status': this.draft.dirty ? 'warn' : 'default' },
          this.draft.dirty
            ? `Unsaved draft · base r${this.draft.expectedRevision}`
            : `Base r${this.draft.expectedRevision}`
        )
      ),
      h(
        'div',
        { className: 'aq-form-grid' },
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Plan ID'),
          planIdInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h(
            'span',
            { className: 'aq-field-label' },
            `Profile (registry r${registry?.revision ?? 0})`
          ),
          profileControl
        ),
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h('span', { className: 'aq-field-label' }, 'Binding ID'),
          h('div', { className: 'aq-input-row' }, bindingInput, bindingQuickSelect)
        ),
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h('span', { className: 'aq-field-label' }, 'Plan summary'),
          summaryInput
        )
      ),
      h(
        'div',
        { className: 'aq-section' },
        h('span', undefined, `Tasks (${this.draft.tasks.length})`),
        addTaskBtn
      ),
      ...taskCards,
      h('div', { className: 'aq-actions' }, saveDraftBtn, publishLatestBtn)
    );
  }

  private buildTaskDraftCard(
    task: PlanTaskDraft,
    idx: number,
    registry: RegistrySnapshot | null,
    busy: boolean
  ): HTMLElement {
    const idInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: task.id,
      placeholder: `task-${idx + 1}`,
      'aria-label': `Task ${idx + 1} ID`,
      'data-focus': `plan-task-${idx}-id`,
      disabled: busy,
    });
    idInput.addEventListener('input', () => {
      task.id = idInput.value;
      this.draft.dirty = true;
    });

    const titleInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: task.title,
      placeholder: 'Task title',
      'aria-label': `Task ${idx + 1} title`,
      'data-focus': `plan-task-${idx}-title`,
      disabled: busy,
    });
    titleInput.addEventListener('input', () => {
      task.title = titleInput.value;
      this.draft.dirty = true;
    });

    const roleSelect = h(
      'select',
      {
        className: 'aq-select-input',
        'aria-label': `Task ${idx + 1} role`,
        'data-focus': `plan-task-${idx}-role`,
        disabled: busy,
      },
      ...WORKFLOW_ROLES.map((r) =>
        h('option', { value: r, selected: r === task.role }, r)
      )
    );
    roleSelect.addEventListener('change', () => {
      task.role = roleSelect.value as WorkflowRole;
      this.draft.dirty = true;
      this.rebuildDom();
    });

    const diffSelect = h(
      'select',
      {
        className: 'aq-select-input',
        'aria-label': `Task ${idx + 1} difficulty`,
        'data-focus': `plan-task-${idx}-difficulty`,
        disabled: busy,
      },
      ...DIFFICULTIES.map((d) =>
        h('option', { value: d, selected: d === task.difficulty }, d)
      )
    );
    diffSelect.addEventListener('change', () => {
      task.difficulty = diffSelect.value as Difficulty;
      this.draft.dirty = true;
      this.rebuildDom();
    });

    const depsInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: task.dependsOnText,
      placeholder: 'Comma-separated task IDs (optional)',
      'aria-label': `Task ${idx + 1} dependencies`,
      'data-focus': `plan-task-${idx}-deps`,
      disabled: busy,
    });
    depsInput.addEventListener('input', () => {
      task.dependsOnText = depsInput.value;
      this.draft.dirty = true;
    });

    const pathsInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: task.allowedWritePathsText,
      placeholder: 'crates/threadlane-plans/, docs/ROADMAP.md',
      'aria-label': `Task ${idx + 1} allowed write paths`,
      'data-focus': `plan-task-${idx}-paths`,
      disabled: busy,
    });
    pathsInput.addEventListener('input', () => {
      task.allowedWritePathsText = pathsInput.value;
      this.draft.dirty = true;
    });

    const instructionArea = h(
      'textarea',
      {
        className: 'aq-textarea-input',
        rows: 2,
        placeholder: 'Scoped task instruction and verification criteria',
        'aria-label': `Task ${idx + 1} instruction`,
        'data-focus': `plan-task-${idx}-instruction`,
        disabled: busy,
      },
      task.instruction
    );
    instructionArea.addEventListener('input', () => {
      task.instruction = instructionArea.value;
      this.draft.dirty = true;
    });

    let resolutionText = 'Select a profile to preview resolved model.';
    let resolutionWarn = false;
    if (registry && this.draft.profileId.trim()) {
      try {
        const choice = resolve_choice(
          registry,
          this.draft.profileId.trim(),
          task.role,
          task.difficulty
        );
        const modelDef = registry.models.find((m) => m.id === choice.model_id);
        resolutionText = `Resolved: ${modelDef?.label ?? choice.model_id} (${
          modelDef?.model_arg ?? choice.model_id
        }) · effort: ${choice.effort ?? 'none'}`;
      } catch (err) {
        resolutionWarn = true;
        resolutionText = isAppError(err) ? err.message : 'Unresolved role/difficulty assignment.';
      }
    }

    const removeBtn =
      this.draft.tasks.length > 1
        ? createButton(
            'Remove',
            () => {
              this.draft.tasks.splice(idx, 1);
              this.draft.dirty = true;
              this.rebuildDom();
            },
            busy
          )
        : null;
    if (removeBtn) {
      removeBtn.classList.add('aq-btn-danger');
    }

    return h(
      'div',
      { className: 'aq-task-editor-card' },
      h(
        'div',
        { className: 'aq-row-head' },
        h('span', { className: 'aq-task-name' }, `Task #${idx + 1}`),
        removeBtn
      ),
      h(
        'div',
        { className: 'aq-form-grid' },
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Task ID'),
          idInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Title'),
          titleInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Role'),
          roleSelect
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Difficulty'),
          diffSelect
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Depends on'),
          depsInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Allowed write paths'),
          pathsInput
        ),
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h('span', { className: 'aq-field-label' }, 'Instruction'),
          instructionArea
        )
      ),
      h(
        'p',
        {
          className: 'aq-sub',
          style: resolutionWarn ? 'color: var(--aq-warn);' : undefined,
        },
        resolutionText
      )
    );
  }

  private buildLedgerReceiptsSection(
    registry: RegistrySnapshot | null,
    busy: boolean
  ): HTMLElement {
    const receipts = deriveDeliveryReceipts(this.latestPlans);
    const sortedRevisions = [...this.latestPlans].sort(
      (a, b) => b.created_at_ms - a.created_at_ms || b.revision - a.revision
    );

    if (sortedRevisions.length === 0) {
      return h(
        'section',
        { className: 'aq-card', 'aria-label': 'Recorded plan revisions and receipts' },
        h('div', { className: 'aq-section' }, h('span', undefined, 'Ledger revisions & delivery receipts')),
        h('p', { className: 'aq-note' }, 'No plan revisions saved in the local ledger yet.')
      );
    }

    const latestByPlan = new Map<string, number>();
    for (const rev of sortedRevisions) {
      const prev = latestByPlan.get(rev.plan_id) ?? 0;
      if (rev.revision > prev) {
        latestByPlan.set(rev.plan_id, rev.revision);
      }
    }

    return h(
      'section',
      { className: 'aq-card', 'aria-label': 'Recorded plan revisions and receipts' },
      h(
        'div',
        { className: 'aq-section' },
        h('span', undefined, `Ledger revisions & delivery receipts (${receipts.length})`)
      ),
      ...sortedRevisions.map((rev) => {
        const isLatest = latestByPlan.get(rev.plan_id) === rev.revision;
        const isActivePublished = isActivePublishedPlanStatus(rev.status);
        const badgeTone = isActivePublished
          ? 'active'
          : rev.status === 'superseded'
            ? 'warn'
            : 'default';

        const loadBtn = createButton(
          'Load into editor',
          () => this.loadRevisionIntoDraft(rev),
          busy
        );

        const transitionBtn =
          isLatest && rev.status === 'draft'
            ? createButton(
                'Publish',
                () => void this.handleTransition('publish', rev.plan_id, rev.revision),
                busy
              )
            : isLatest && rev.status === 'published'
              ? createButton(
                  'Mark received',
                  () => void this.handleTransition('receive', rev.plan_id, rev.revision),
                  busy
                )
              : isLatest && rev.status === 'received'
                ? createButton(
                    'Mark applied',
                    () => void this.handleTransition('apply', rev.plan_id, rev.revision),
                    busy
                  )
                : null;

        if (transitionBtn && rev.status === 'draft') {
          transitionBtn.classList.add('aq-btn-primary');
        }

        const profileStale =
          registry !== null && rev.profile_revision !== registry.revision;

        return h(
          'div',
          { className: 'aq-role' },
          h(
            'div',
            { className: 'aq-role-head' },
            h(
              'span',
              undefined,
              `${rev.plan_id} · r${rev.revision} (${rev.binding_id})`
            ),
            h(
              'span',
              { className: 'aq-badge', 'data-status': badgeTone },
              `Receipt: ${rev.status}`
            )
          ),
          h('div', { className: 'aq-role-line' }, h('span', undefined, rev.summary)),
          h(
            'div',
            { className: 'aq-role-line' },
            h(
              'span',
              undefined,
              `Profile: ${rev.profile_id} @ r${rev.profile_revision}${
                profileStale ? ` (current registry r${registry?.revision ?? 0})` : ''
              } · ${rev.tasks.length} task${rev.tasks.length === 1 ? '' : 's'} · ${formatRelativeTimestamp(
                rev.created_at_ms
              )}`
            )
          ),
          h('div', { className: 'aq-actions' }, loadBtn, transitionBtn),
          isActivePublished ? this.buildExecutionApprovalSection(rev, registry, busy) : null
        );
      })
    );
  }

  private buildExecutionApprovalSection(
    rev: PlanRevision,
    registry: RegistrySnapshot | null,
    pageBusy: boolean
  ): HTMLElement {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const draft = this.getOrCreateApprovalDraft(rev);
    const recordedApprovals = this.findApprovalsForRevision(
      rev.plan_id,
      rev.revision,
      rev.binding_id
    );
    const latestApproval = recordedApprovals[0] ?? null;

    const statusBadgeText = latestApproval
      ? 'Approved · Not started'
      : draft.preview
        ? draft.previewIsSampleFixture
          ? 'Sample preview · Read-only'
          : 'Preview ready · Unapproved'
        : 'Not approved';

    const statusBadgeTone = latestApproval
      ? 'active'
      : draft.preview
        ? draft.previewIsSampleFixture
          ? 'warn'
          : 'default'
        : 'default';

    const toggleBtn = h(
      'button',
      {
        type: 'button',
        className: 'aq-plan-approval-toggle',
        'aria-expanded': String(draft.expanded),
        'aria-label': `Execution approval for ${rev.plan_id} revision ${rev.revision}`,
        'data-focus': `approval-toggle-${key}`,
      },
      h(
        'span',
        { className: 'aq-plan-approval-toggle-left' },
        h('span', { className: 'aq-plan-approval-toggle-title' }, 'Execution approval'),
        h(
          'span',
          { className: 'aq-badge', 'data-status': statusBadgeTone },
          statusBadgeText
        )
      ),
      h(
        'span',
        { className: 'aq-plan-approval-toggle-right' },
        createInlineChevronIcon(draft.expanded)
      )
    );
    toggleBtn.addEventListener('click', () => {
      this.handleToggleApprovalPanel(rev);
    });

    if (!draft.expanded) {
      return h('div', { className: 'aq-plan-approval-panel' }, toggleBtn);
    }

    const panelBusy = pageBusy || draft.previewLoading || draft.submitting;
    const bindingResolution = resolveAuthoritativeBindingForApproval(
      this.latestBindings,
      rev.binding_id,
      this.latestAttachment
    );
    const registryMismatch =
      registry !== null && registry.revision !== rev.profile_revision;

    const previewBtn = createButton(
      draft.previewLoading
        ? 'Compiling preview…'
        : draft.preview
          ? 'Refresh execution preview'
          : 'Preview execution',
      () => {
        void this.handlePreviewExecution(rev);
      },
      panelBusy
    );

    const bodyNodes: Node[] = [];

    bodyNodes.push(
      h(
        'p',
        { className: 'aq-plan-approval-note' },
        'Operator-only approval freezes the compiled task models, efforts, instructions, write scopes, repository, base commit, and native thread UUID into a local authority artifact. It never starts workers, providers, or host processes and never changes plan receipt state.'
      )
    );

    if (recordedApprovals.length > 0) {
      bodyNodes.push(this.buildRecordedApprovalsList(recordedApprovals));
    }

    if (draft.errorMessage) {
      bodyNodes.push(
        h(
          'div',
          { className: 'aq-plan-approval-note aq-plan-approval-note--danger', role: 'alert' },
          draft.errorMessage
        )
      );
    }

    if (draft.statusNotice) {
      bodyNodes.push(
        h(
          'div',
          { className: 'aq-plan-approval-note aq-plan-approval-note--active', role: 'status' },
          draft.statusNotice
        )
      );
    }

    if (registryMismatch) {
      bodyNodes.push(
        h(
          'div',
          { className: 'aq-plan-approval-note aq-plan-approval-note--warn' },
          `Plan revision r${rev.revision} pins registry r${rev.profile_revision}, but the current registry is at r${registry?.revision ?? 0}. Publish a new plan revision against the current registry to approve execution.`
        )
      );
    }

    bodyNodes.push(
      h(
        'dl',
        { className: 'aq-plan-approval-kv', 'aria-label': 'Authoritative binding identity' },
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'Binding ID'),
          h('dd', undefined, rev.binding_id)
        ),
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'Native thread UUID'),
          h(
            'dd',
            { 'data-approval-thread-dd': key },
            this.formatBindingResolutionText(bindingResolution)
          )
        ),
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'Pinned registry'),
          h(
            'dd',
            undefined,
            `${rev.profile_id} @ r${rev.profile_revision}`
          )
        )
      )
    );

    bodyNodes.push(h('div', { className: 'aq-actions' }, previewBtn));

    if (draft.preview) {
      bodyNodes.push(
        this.buildPreviewAndApprovalForm(rev, draft, bindingResolution, panelBusy)
      );
    }

    return h(
      'div',
      { className: 'aq-plan-approval-panel' },
      toggleBtn,
      h('div', { className: 'aq-plan-approval-body' }, ...bodyNodes)
    );
  }

  private buildRecordedApprovalsList(approvals: readonly ApprovalSummary[]): HTMLElement {
    return h(
      'div',
      {
        className: 'aq-plan-approval-recorded-list',
        'aria-label': 'Recorded operator approvals for this plan revision',
      },
      ...approvals.map((summary) =>
        h(
          'div',
          { className: 'aq-plan-approval-recorded-card' },
          h(
            'div',
            { className: 'aq-row-head' },
            h('span', { className: 'aq-task-name' }, `Approval ${summary.approval_id}`),
            h(
              'span',
              { className: 'aq-badge', 'data-status': 'active' },
              'Approved · Not started'
            )
          ),
          h(
            'dl',
            { className: 'aq-plan-approval-kv' },
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Subject digest'),
              h('dd', undefined, summary.subject_digest)
            ),
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Plan digest'),
              h('dd', undefined, `${summary.plan_digest} (registry r${summary.registry_revision})`)
            ),
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Thread UUID'),
              h('dd', undefined, summary.thread_uuid)
            ),
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Repository'),
              h('dd', undefined, `${summary.repository_path} @ ${summary.base_commit}`)
            ),
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Approved tasks'),
              h(
                'dd',
                undefined,
                `${summary.approved_task_ids.join(', ')} (${summary.worker_profile_count} worker profile${
                  summary.worker_profile_count === 1 ? '' : 's'
                }, ${summary.native_route_count} native route${
                  summary.native_route_count === 1 ? '' : 's'
                } · max parallel ${summary.max_parallel})`
              )
            ),
            h(
              'div',
              { className: 'aq-plan-approval-kv-row' },
              h('dt', undefined, 'Write scope'),
              h('dd', undefined, summary.approved_write_paths.join(', '))
            )
          )
        )
      )
    );
  }

  private buildPreviewAndApprovalForm(
    rev: PlanRevision,
    draft: ApprovalFormDraft,
    bindingResolution: ReturnType<typeof resolveAuthoritativeBindingForApproval>,
    panelBusy: boolean
  ): HTMLElement {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const preview = draft.preview!;

    const updateField = (field: EditableApprovalDraftField, value: string, rerender = false): void => {
      const current = this.getOrCreateApprovalDraft(rev);
      const next = updateApprovalDraftField(current, field, value);
      this.approvalDrafts.set(key, next);
      if (rerender) {
        this.rebuildDom();
      } else {
        this.patchLiveApprovalContextDom();
      }
    };

    const repoInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: draft.repositoryPath,
      placeholder: '/canonical/path/to/git/repository',
      'aria-label': 'Canonical Git repository path',
      'data-focus': `approval-repo-${key}`,
      disabled: panelBusy,
    });
    repoInput.addEventListener('input', () => {
      updateField('repositoryPath', repoInput.value);
    });

    const commitInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: draft.baseCommit,
      placeholder: '40-character hexadecimal Git commit SHA',
      'aria-label': 'Exact 40-character base commit SHA',
      'data-focus': `approval-commit-${key}`,
      disabled: panelBusy,
    });
    commitInput.addEventListener('input', () => {
      updateField('baseCommit', commitInput.value);
    });

    const writePathsInput = h('input', {
      type: 'text',
      className: 'aq-text-input',
      value: draft.approvedWritePathsText,
      placeholder: 'src/lib.rs, crates/threadlane-authority/',
      'aria-label': 'Operator approved write paths',
      'data-focus': `approval-write-paths-${key}`,
      disabled: panelBusy,
    });
    writePathsInput.addEventListener('input', () => {
      updateField('approvedWritePathsText', writePathsInput.value);
    });

    const maxParallelInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(MIN_APPROVAL_MAX_PARALLEL),
      max: String(MAX_APPROVAL_MAX_PARALLEL),
      value: draft.maxParallelText,
      'aria-label': 'Max parallel workers',
      'data-focus': `approval-max-parallel-${key}`,
      disabled: panelBusy,
    });
    maxParallelInput.addEventListener('input', () => {
      updateField('maxParallelText', maxParallelInput.value);
    });

    const timeoutInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_BUDGET_BOUNDS.timeout_ms.min),
      max: String(EXECUTION_BUDGET_BOUNDS.timeout_ms.max),
      value: draft.timeoutMsText,
      'aria-label': 'Execution timeout (ms)',
      'data-focus': `approval-timeout-${key}`,
      disabled: panelBusy,
    });
    timeoutInput.addEventListener('input', () => {
      updateField('timeoutMsText', timeoutInput.value);
    });

    const maxLineInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_BUDGET_BOUNDS.max_line_bytes.min),
      max: String(EXECUTION_BUDGET_BOUNDS.max_line_bytes.max),
      value: draft.maxLineBytesText,
      'aria-label': 'Max line bytes',
      'data-focus': `approval-max-line-${key}`,
      disabled: panelBusy,
    });
    maxLineInput.addEventListener('input', () => {
      updateField('maxLineBytesText', maxLineInput.value);
    });

    const maxStdoutInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_BUDGET_BOUNDS.max_stdout_bytes.min),
      max: String(EXECUTION_BUDGET_BOUNDS.max_stdout_bytes.max),
      value: draft.maxStdoutBytesText,
      'aria-label': 'Max stdout bytes',
      'data-focus': `approval-max-stdout-${key}`,
      disabled: panelBusy,
    });
    maxStdoutInput.addEventListener('input', () => {
      updateField('maxStdoutBytesText', maxStdoutInput.value);
    });

    const maxStderrInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_BUDGET_BOUNDS.max_stderr_bytes.min),
      max: String(EXECUTION_BUDGET_BOUNDS.max_stderr_bytes.max),
      value: draft.maxStderrBytesText,
      'aria-label': 'Max stderr bytes',
      'data-focus': `approval-max-stderr-${key}`,
      disabled: panelBusy,
    });
    maxStderrInput.addEventListener('input', () => {
      updateField('maxStderrBytesText', maxStderrInput.value);
    });

    const maxEventsInput = h('input', {
      type: 'number',
      className: 'aq-text-input',
      min: String(EXECUTION_BUDGET_BOUNDS.max_events.min),
      max: String(EXECUTION_BUDGET_BOUNDS.max_events.max),
      value: draft.maxEventsText,
      'aria-label': 'Max durable progress events',
      'data-focus': `approval-max-events-${key}`,
      disabled: panelBusy,
    });
    maxEventsInput.addEventListener('input', () => {
      updateField('maxEventsText', maxEventsInput.value);
    });

    let inputsValidForCheckbox = false;
    try {
      validateApprovalDraftInputs(draft, this.latestBindings, this.latestAttachment);
      inputsValidForCheckbox = true;
    } catch {
      inputsValidForCheckbox = false;
    }

    const reviewCheckbox = h('input', {
      type: 'checkbox',
      checked: draft.reviewConfirmed,
      'aria-label': 'Confirm operator review of exact execution preview and approval parameters',
      'data-focus': `approval-confirm-check-${key}`,
      'data-approval-confirm-check': key,
      disabled: panelBusy || draft.previewIsSampleFixture || !bindingResolution.ok,
    });
    reviewCheckbox.addEventListener('change', () => {
      const current = this.getOrCreateApprovalDraft(rev);
      try {
        const next = setApprovalReviewConfirmed(
          current,
          reviewCheckbox.checked,
          this.latestBindings,
          this.latestAttachment,
          this.selectedBindingId
        );
        this.approvalDrafts.set(key, next);
      } catch (err) {
        reviewCheckbox.checked = false;
        this.approvalDrafts.set(key, {
          ...current,
          reviewConfirmed: false,
          confirmedSubjectSignature: null,
          errorMessage: formatAppError(err, 'Complete all approval fields before confirming review.'),
        });
      }
      this.rebuildDom();
    });

    const canSubmit =
      !panelBusy &&
      canSubmitApprovalDraft(
        draft,
        this.latestBindings,
        this.latestAttachment,
        this.selectedBindingId
      );
    const approveBtn = createButton(
      draft.submitting ? 'Recording approval…' : 'Approve selected tasks',
      () => {
        void this.handleApproveSelectedTasks(rev);
      },
      !canSubmit
    );
    approveBtn.classList.add('aq-btn-primary');
    approveBtn.dataset.approvalSubmitBtn = key;

    return h(
      'div',
      { className: 'aq-plan-approval-task-list' },
      draft.previewIsSampleFixture
        ? h(
            'div',
            { className: 'aq-plan-approval-note aq-plan-approval-note--warn' },
            'Browser DEV preview fixture (read-only). This sample preview demonstrates resolved models and scopes, but persistent operator approval is disabled outside the native Tauri desktop application.'
          )
        : null,
      h(
        'div',
        {
          className: 'aq-plan-approval-note aq-plan-approval-note--warn',
          'data-approval-binding-warn': key,
          style: bindingResolution.ok ? 'display: none;' : undefined,
        },
        bindingResolution.ok
          ? ''
          : `Cannot submit approval until a live non-demo runtime binding is connected for "${rev.binding_id}": ${bindingResolution.reason}`
      ),
      h(
        'dl',
        { className: 'aq-plan-approval-kv', 'aria-label': 'Compiled execution preview summary' },
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'Compiled digest'),
          h('dd', undefined, preview.plan_digest)
        ),
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'Pinned profile'),
          h('dd', undefined, `${preview.profile_id} @ registry r${preview.registry_revision}`)
        ),
        h(
          'div',
          { className: 'aq-plan-approval-kv-row' },
          h('dt', undefined, 'DAG order'),
          h('dd', undefined, preview.topological_order.join(' → '))
        )
      ),
      h(
        'div',
        { className: 'aq-section' },
        h(
          'span',
          undefined,
          `Compiled tasks (${draft.selectedTaskIds.length} of ${preview.tasks.length} selected)`
        )
      ),
      ...preview.tasks.map((task) =>
        this.buildApprovalTaskPreviewCard(rev, draft, task, panelBusy)
      ),
      h(
        'div',
        { className: 'aq-section' },
        h('span', undefined, 'Repository, write allowlist & execution budgets')
      ),
      h(
        'div',
        { className: 'aq-form-grid' },
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h('span', { className: 'aq-field-label' }, 'Canonical Git repository path'),
          repoInput
        ),
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h('span', { className: 'aq-field-label' }, 'Exact base commit SHA (40-hex)'),
          commitInput
        ),
        h(
          'label',
          { className: 'aq-field aq-field-full' },
          h(
            'span',
            { className: 'aq-field-label' },
            'Operator approved write paths (comma-separated, trailing "/" for directories)'
          ),
          writePathsInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h(
            'span',
            { className: 'aq-field-label' },
            `Max parallel (${MIN_APPROVAL_MAX_PARALLEL}–${MAX_APPROVAL_MAX_PARALLEL})`
          ),
          maxParallelInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Timeout (ms)'),
          timeoutInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max line bytes'),
          maxLineInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max stdout bytes'),
          maxStdoutInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max stderr bytes'),
          maxStderrInput
        ),
        h(
          'label',
          { className: 'aq-field' },
          h('span', { className: 'aq-field-label' }, 'Max events'),
          maxEventsInput
        )
      ),
      h(
        'label',
        {
          className: `aq-plan-approval-review-box${
            draft.reviewConfirmed ? ' is-confirmed' : ''
          }`,
          'data-approval-review-box': key,
        },
        reviewCheckbox,
        h(
          'span',
          { 'data-approval-review-text': key },
          this.formatReviewBoxInstructionText(draft, inputsValidForCheckbox)
        )
      ),
      h('div', { className: 'aq-actions' }, approveBtn)
    );
  }

  private buildApprovalTaskPreviewCard(
    rev: PlanRevision,
    draft: ApprovalFormDraft,
    task: PlanTaskExecutionPreview,
    panelBusy: boolean
  ): HTMLElement {
    const key = approvalDraftKey(rev.plan_id, rev.revision);
    const disposition = classifyTaskApprovalDisposition(task);
    const isSelected = draft.selectedTaskIds.includes(task.id);

    const taskCheckbox = h('input', {
      type: 'checkbox',
      checked: isSelected,
      disabled: panelBusy || !disposition.selectable,
      'aria-label': `Select task ${task.id} (${task.title}) for approval`,
      'data-focus': `approval-task-check-${key}-${task.id}`,
    });
    taskCheckbox.addEventListener('change', () => {
      const current = this.getOrCreateApprovalDraft(rev);
      const next = toggleApprovalTaskSelection(current, task.id, taskCheckbox.checked);
      this.approvalDrafts.set(key, next);
      this.rebuildDom();
    });

    const providerLabel = formatAdapterProviderLabel(task.adapter);
    const effortLabel = task.effort ?? 'none';

    return h(
      'div',
      {
        className: `aq-plan-approval-task-card${isSelected ? ' is-selected' : ''}`,
      },
      h(
        'div',
        { className: 'aq-plan-approval-task-head' },
        h(
          'label',
          { className: 'aq-plan-approval-task-check' },
          taskCheckbox,
          h('span', undefined, `${task.id} · ${task.title}`)
        ),
        h(
          'span',
          { className: 'aq-badge', 'data-status': disposition.badgeTone },
          disposition.badgeLabel
        )
      ),
      h(
        'div',
        { className: 'aq-plan-approval-task-provider' },
        h(
          'span',
          undefined,
          `${providerLabel} (${task.tool_label}) · ${task.model_label} [${task.model_arg}] · effort: ${effortLabel} · ${task.role}/${task.difficulty}`
        )
      ),
      h('pre', { className: 'aq-plan-approval-task-instruction' }, task.instruction),
      h(
        'div',
        { className: 'aq-plan-approval-task-meta' },
        `Write scope: ${task.allowed_write_paths.join(', ') || 'none'} · Depends on: ${
          task.depends_on.length > 0 ? task.depends_on.join(', ') : 'none (root)'
        } · Task digest: ${task.task_digest.slice(0, 16)}… · Instruction digest: ${task.instruction_digest.slice(0, 16)}…`
      ),
      h(
        'p',
        {
          className: `aq-plan-approval-note${
            disposition.badgeTone === 'warn' ? ' aq-plan-approval-note--warn' : ''
          }`,
        },
        disposition.note
      )
    );
  }
}
