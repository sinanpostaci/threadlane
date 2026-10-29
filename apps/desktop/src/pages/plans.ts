import { createButton, h } from '../ui/dom.ts';
import { formatRelativeTimestamp } from '../format.ts';
import type { BindingSummary } from '../types.ts';
import {
  desktop_plan_change,
  DIFFICULTIES,
  formatAppError,
  isAppError,
  resolve_choice,
  validatePlanTasksDag,
  WORKFLOW_ROLES,
  type DeliveryReceipt,
  type Difficulty,
  type PlanRevision,
  type PlanTask,
  type RegistrySnapshot,
  type WorkflowRole,
} from '../registry/ipc.ts';

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

export class PlansPage {
  private readonly onPlansChanged: () => void;
  private readonly container: HTMLElement;
  private draft: PlanFormDraft;
  private latestPlans: PlanRevision[] = [];
  private latestRegistry: RegistrySnapshot | null = null;
  private latestBindings: BindingSummary[] = [];
  private selectedBindingId: string | null = null;
  private busyAction: string | null = null;
  private errorMessage: string | null = null;
  private hasCasConflict: boolean = false;
  private statusNotice: string | null = null;

  constructor(onPlansChanged: () => void) {
    this.onPlansChanged = onPlansChanged;
    this.draft = createInitialPlanDraft();
    this.container = h('div', { className: 'aq-plans-page' });
  }

  public getDraft(): PlanFormDraft {
    return structuredClone(this.draft);
  }

  public render(
    plans: PlanRevision[],
    registry: RegistrySnapshot,
    bindings: BindingSummary[],
    selectedBindingId: string | null,
    externalError: string | null = null
  ): HTMLElement {
    this.latestPlans = plans;
    this.latestRegistry = registry;
    this.latestBindings = bindings;
    this.selectedBindingId = selectedBindingId;

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

    this.rebuildDom(externalError);
    return this.container;
  }

  public dispose(): void {
    this.container.replaceChildren();
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

  private rebuildDom(externalError: string | null = null): void {
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
          h('span', undefined, 'Explicit publication only'),
          h('span', { className: 'aq-badge' }, 'No automatic execution')
        ),
        h(
          'p',
          { className: 'aq-banner-sub' },
          'Saving or publishing a plan records an immutable revision in the local ledger. Plans never classify transcripts or spawn workers automatically, and model output is never treated as user approval.'
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
      placeholder: 'crates/threadlane-plans, docs/',
      'aria-label': `Task ${idx + 1} allowed write paths`,
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
        const badgeTone =
          rev.status === 'published' || rev.status === 'received' || rev.status === 'applied'
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
          h('div', { className: 'aq-actions' }, loadBtn, transitionBtn)
        );
      })
    );
  }
}
