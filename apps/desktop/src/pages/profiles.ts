import './profiles.css';

import { desktop_registry_apply, desktop_registry_snapshot } from '../registry/ipc.ts';

import type {
  Difficulty,
  ModelDefinition,
  RegistryChange,
  RegistrySnapshot,
  RoleAssignment,
  ToolDefinition,
  WorkflowProfile,
  WorkflowRole,
} from '../registry/types.ts';
import { createButton, h } from '../ui/dom.ts';

const ROLES: readonly WorkflowRole[] = [
  'follower',
  'orchestrator',
  'architect',
  'coder',
  'reviewer',
  'closure',
];

const DIFFICULTIES: readonly Difficulty[] = ['easy', 'medium', 'hard', 'extreme'];

const ROLE_LABELS: Record<WorkflowRole, string> = {
  follower: 'Follower',
  orchestrator: 'Orchestrator',
  architect: 'Architect',
  coder: 'Coder',
  reviewer: 'Reviewer',
  closure: 'Closure',
};

const DIFFICULTY_LABELS: Record<Difficulty, string> = {
  easy: 'Easy',
  medium: 'Medium',
  hard: 'Hard',
  extreme: 'Extreme',
};

export type Slot = 'default' | Difficulty;

const SLOTS: readonly Slot[] = ['default', ...DIFFICULTIES];

export interface Choice {
  model_id: string;
  effort: string | null;
}

export type AssignmentTable = Record<WorkflowRole, Record<Slot, Choice>>;

export interface AssignmentIssue {
  role: WorkflowRole;
  slot: Slot;
  message: string;
}

export interface DraftAnalysis {
  errors: string[];
  issues: AssignmentIssue[];
  missingDefaults: WorkflowRole[];
  unassignedRoles: WorkflowRole[];
  assignmentCount: number;
  defaultCount: number;
  overrideCount: number;
}

export interface ProfileSummary {
  assignmentCount: number;
  defaultCount: number;
  overrideCount: number;
  missingDefaults: WorkflowRole[];
  issues: AssignmentIssue[];
}

export interface ProfileDraft {
  mode: 'create' | 'edit';
  id: string;
  name: string;
  sourceId: string | null;
  sourceRevision: number;
  sourceSignature: string | null;
  cloneOfName: string | null;
  assignments: AssignmentTable;
  externalChanged: boolean;
  externalDeleted: boolean;
  overwriteAcknowledged: boolean;
  acknowledgedSignature: string | null;
}

interface ModelGroup {
  tool: ToolDefinition;
  models: ModelDefinition[];
}

interface RegistryIndex {
  tools: Map<string, ToolDefinition>;
  models: Map<string, ModelDefinition>;
  groups: ModelGroup[];
}

export interface SafeError {
  code: string | null;
  message: string;
}

interface PendingState {
  kind: 'save' | 'delete' | 'default' | 'reload';
  targetId?: string;
}

interface Notice {
  tone: 'info' | 'success' | 'warn';
  text: string;
}

interface FocusState {
  key: string;
  start: number | null;
  end: number | null;
}

interface ButtonOptions {
  className?: string;
  disabled?: boolean;
  focusKey?: string;
  ariaLabel?: string;
}

const MAX_PROFILE_ID_LENGTH = 64;
const MAX_PROFILE_NAME_LENGTH = 120;
const DELETED_SIGNATURE = '__deleted__';

let nextPageId = 1;

function createEmptyTable(): AssignmentTable {
  const table = {} as AssignmentTable;
  for (const role of ROLES) {
    const slots = {} as Record<Slot, Choice>;
    for (const slot of SLOTS) {
      slots[slot] = { model_id: '', effort: null };
    }
    table[role] = slots;
  }
  return table;
}

function cloneTable(table: AssignmentTable): AssignmentTable {
  const next = createEmptyTable();
  for (const role of ROLES) {
    for (const slot of SLOTS) {
      next[role][slot] = { ...table[role][slot] };
    }
  }
  return next;
}

function tableFromProfile(profile: WorkflowProfile): AssignmentTable {
  const table = createEmptyTable();
  for (const assignment of profile.assignments) {
    if (!ROLES.includes(assignment.role)) {
      continue;
    }
    const slot: Slot = assignment.difficulty ?? 'default';
    if (!SLOTS.includes(slot)) {
      continue;
    }
    table[assignment.role][slot] = {
      model_id: assignment.choice.model_id,
      effort: assignment.choice.effort ?? null,
    };
  }
  return table;
}

function assignmentsFromTable(table: AssignmentTable): RoleAssignment[] {
  const assignments: RoleAssignment[] = [];
  for (const role of ROLES) {
    for (const slot of SLOTS) {
      const cell = table[role][slot];
      if (!cell.model_id) {
        continue;
      }
      assignments.push({
        role,
        difficulty: slot === 'default' ? null : slot,
        choice: { model_id: cell.model_id, effort: cell.effort },
      });
    }
  }
  return assignments;
}

function profileFromDraft(draft: ProfileDraft): WorkflowProfile {
  return {
    id: draft.id,
    name: draft.name.trim(),
    assignments: assignmentsFromTable(draft.assignments),
  };
}

function buildIndex(snapshot: RegistrySnapshot): RegistryIndex {
  const tools = new Map(snapshot.tools.map((tool) => [tool.id, tool]));
  const models = new Map(snapshot.models.map((model) => [model.id, model]));
  const groups: ModelGroup[] = [];

  for (const tool of snapshot.tools) {
    if (!tool.enabled) {
      continue;
    }
    groups.push({
      tool,
      models: snapshot.models.filter((model) => model.tool_id === tool.id),
    });
  }

  return { tools, models, groups };
}

function validateTable(index: RegistryIndex, table: AssignmentTable): AssignmentIssue[] {
  const issues: AssignmentIssue[] = [];

  for (const role of ROLES) {
    for (const slot of SLOTS) {
      const cell = table[role][slot];
      if (!cell.model_id) {
        if (cell.effort !== null) {
          issues.push({ role, slot, message: 'An effort is set without a model.' });
        }
        continue;
      }

      const model = index.models.get(cell.model_id);
      if (!model) {
        issues.push({
          role,
          slot,
          message: `Model "${cell.model_id}" is not in the registry.`,
        });
        continue;
      }

      const modelLabel = model.label || model.id;
      const tool = index.tools.get(model.tool_id);
      if (!tool) {
        issues.push({
          role,
          slot,
          message: `Model "${modelLabel}" refers to a missing tool.`,
        });
      } else if (!tool.enabled) {
        issues.push({
          role,
          slot,
          message: `Model "${modelLabel}" belongs to disabled tool "${tool.label}".`,
        });
      }

      if (cell.effort === null) {
        if (!model.allow_no_effort) {
          issues.push({
            role,
            slot,
            message: `Model "${modelLabel}" requires an explicit effort.`,
          });
        }
      } else if (!model.efforts.includes(cell.effort)) {
        issues.push({
          role,
          slot,
          message: `Effort "${cell.effort}" is not supported by model "${modelLabel}".`,
        });
      }
    }
  }

  return issues;
}

function slotLabel(slot: Slot): string {
  return slot === 'default' ? 'role default' : `${DIFFICULTY_LABELS[slot]} override`;
}

function analyzeWithIndex(index: RegistryIndex, draft: ProfileDraft): DraftAnalysis {
  const issues = validateTable(index, draft.assignments);
  const errors = issues.map(
    (issue) => `${ROLE_LABELS[issue.role]} ${slotLabel(issue.slot)}: ${issue.message}`
  );
  const trimmedName = draft.name.trim();

  if (!trimmedName) {
    errors.unshift('Profile name is required.');
  } else if (Array.from(trimmedName).length > MAX_PROFILE_NAME_LENGTH) {
    errors.unshift(`Profile name must be ${MAX_PROFILE_NAME_LENGTH} characters or fewer.`);
  }
  if (!draft.id.trim()) {
    errors.unshift('Profile identifier is missing.');
  } else if (draft.id.length > MAX_PROFILE_ID_LENGTH) {
    errors.unshift(`Profile identifier must be ${MAX_PROFILE_ID_LENGTH} characters or fewer.`);
  }

  const missingDefaults = ROLES.filter((role) => !draft.assignments[role].default.model_id);
  const unassignedRoles = ROLES.filter((role) =>
    SLOTS.every((slot) => !draft.assignments[role][slot].model_id)
  );
  let assignmentCount = 0;
  let overrideCount = 0;

  for (const role of ROLES) {
    for (const slot of SLOTS) {
      if (draft.assignments[role][slot].model_id) {
        assignmentCount += 1;
        if (slot !== 'default') {
          overrideCount += 1;
        }
      }
    }
  }

  return {
    errors,
    issues,
    missingDefaults,
    unassignedRoles,
    assignmentCount,
    defaultCount: ROLES.length - missingDefaults.length,
    overrideCount,
  };
}

export function analyzeDraft(snapshot: RegistrySnapshot, draft: ProfileDraft): DraftAnalysis {
  return analyzeWithIndex(buildIndex(snapshot), draft);
}

export function summarizeProfile(
  snapshot: RegistrySnapshot,
  profile: WorkflowProfile
): ProfileSummary {
  const index = buildIndex(snapshot);
  const table = tableFromProfile(profile);
  const missingDefaults = ROLES.filter((role) => !table[role].default.model_id);
  let assignmentCount = 0;
  let overrideCount = 0;

  for (const role of ROLES) {
    for (const slot of SLOTS) {
      if (table[role][slot].model_id) {
        assignmentCount += 1;
        if (slot !== 'default') {
          overrideCount += 1;
        }
      }
    }
  }

  return {
    assignmentCount,
    defaultCount: ROLES.length - missingDefaults.length,
    overrideCount,
    missingDefaults,
    issues: validateTable(index, table),
  };
}

export function profileSignature(profile: WorkflowProfile): string {
  const assignments = profile.assignments
    .map((assignment) => ({
      role: assignment.role,
      difficulty: assignment.difficulty ?? null,
      model_id: assignment.choice.model_id,
      effort: assignment.choice.effort ?? null,
    }))
    .sort((left, right) => {
      const roleDelta = ROLES.indexOf(left.role) - ROLES.indexOf(right.role);
      if (roleDelta !== 0) {
        return roleDelta;
      }
      const leftSlot: Slot = left.difficulty ?? 'default';
      const rightSlot: Slot = right.difficulty ?? 'default';
      return SLOTS.indexOf(leftSlot) - SLOTS.indexOf(rightSlot);
    });

  return JSON.stringify({ name: profile.name, assignments });
}

export function normalizeError(error: unknown): SafeError {
  const objectError =
    error !== null && typeof error === 'object'
      ? (error as { code?: unknown; message?: unknown })
      : null;
  const code = objectError && typeof objectError.code === 'string'
    ? objectError.code.trim().slice(0, 64)
    : null;
  let message =
    objectError && typeof objectError.message === 'string' ? objectError.message : null;

  if (!message && error instanceof Error) {
    message = error.message;
  }
  if (!message && typeof error === 'string') {
    message = error;
  }

  return {
    code: code || null,
    message: (message ?? 'Registry request failed.').replace(/\s+/g, ' ').trim().slice(0, 240) ||
      'Registry request failed.',
  };
}

function isRevisionConflict(error: SafeError): boolean {
  const text = `${error.code ?? ''} ${error.message}`.toLowerCase();
  return text.includes('revision') || text.includes('conflict') || text.includes('stale');
}

function profileId(snapshot: RegistrySnapshot): string {
  if (typeof globalThis.crypto?.randomUUID !== 'function') {
    throw new Error('Secure profile identifiers are unavailable in this environment.');
  }
  const existing = new Set(snapshot.profiles.map((profile) => profile.id));
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const id = `profile-${globalThis.crypto.randomUUID()}`;
    if (!existing.has(id)) {
      return id;
    }
  }
  throw new Error('Could not allocate a unique profile identifier.');
}

function copyName(name: string): string {
  const characters = Array.from(`${name.trim()} copy`);
  return characters.slice(0, MAX_PROFILE_NAME_LENGTH).join('');
}

function button(
  text: string,
  onClick: () => void,
  options: ButtonOptions = {}
): HTMLButtonElement {
  const element = createButton(text, onClick, options.disabled);
  if (options.className) {
    element.className = options.className;
  }
  if (options.focusKey) {
    element.dataset.focusKey = options.focusKey;
  }
  if (options.ariaLabel) {
    element.setAttribute('aria-label', options.ariaLabel);
  }
  return element;
}

function pill(text: string, tone: 'muted' | 'accent' | 'warn' | 'error'): HTMLElement {
  return h('span', { class: 'aq-profiles-pill', 'data-tone': tone }, text);
}

function banner(
  tone: 'error' | 'warn' | 'info' | 'success',
  title: string,
  text: string,
  actions: HTMLElement[] = []
): HTMLElement {
  return h(
    'div',
    {
      class: 'aq-profiles-banner',
      'data-tone': tone,
      role: tone === 'error' || tone === 'warn' ? 'alert' : 'status',
      'aria-live': tone === 'error' ? 'assertive' : 'polite',
    },
    h(
      'div',
      { class: 'aq-profiles-banner-copy' },
      h('strong', {}, title),
      h('p', {}, text)
    ),
    actions.length > 0
      ? h('div', { class: 'aq-profiles-banner-actions' }, ...actions)
      : null
  );
}

function buildDraftStatus(analysis: DraftAnalysis): HTMLElement {
  const status = h(
    'div',
    { class: 'aq-profiles-status' },
    h(
      'div',
      { class: 'aq-profiles-stats' },
      pill(`${analysis.defaultCount}/${ROLES.length} role defaults`, 'muted'),
      pill(`${analysis.overrideCount} difficulty overrides`, 'muted'),
      pill(`${analysis.assignmentCount} assignments`, 'muted'),
      analysis.missingDefaults.length > 0
        ? pill('Partial profile', 'warn')
        : pill('Role defaults complete', 'accent')
    )
  );

  if (analysis.missingDefaults.length > 0) {
    status.append(
      h(
        'p',
        { class: 'aq-profiles-note' },
        h('strong', {}, 'Missing role defaults: '),
        analysis.missingDefaults.map((role) => ROLE_LABELS[role]).join(', ')
      )
    );
  }
  if (analysis.unassignedRoles.length > 0) {
    status.append(
      h(
        'p',
        { class: 'aq-profiles-note', 'data-tone': 'muted' },
        h('strong', {}, 'No assignments yet: '),
        analysis.unassignedRoles.map((role) => ROLE_LABELS[role]).join(', ')
      )
    );
  }
  if (analysis.errors.length > 0) {
    status.append(
      h(
        'div',
        { class: 'aq-profiles-errors', role: 'alert' },
        h('p', {}, 'Fix these before saving:'),
        h('ul', {}, ...analysis.errors.map((message) => h('li', {}, message)))
      )
    );
  }

  return status;
}

function needsAcknowledgement(draft: ProfileDraft): boolean {
  return draft.externalChanged && !draft.overwriteAcknowledged;
}

function roleState(
  analysis: DraftAnalysis,
  table: AssignmentTable,
  role: WorkflowRole
): { tone: 'accent' | 'warn' | 'error' | 'muted'; label: string } {
  const issues = analysis.issues.filter((issue) => issue.role === role).length;
  if (issues > 0) {
    return { tone: 'error', label: `${issues} ${issues === 1 ? 'issue' : 'issues'}` };
  }
  const hasAny = SLOTS.some((slot) => table[role][slot].model_id);
  if (!hasAny) {
    return { tone: 'muted', label: 'No assignment' };
  }
  if (!table[role].default.model_id) {
    return { tone: 'warn', label: 'Overrides only' };
  }
  return { tone: 'accent', label: 'Default set' };
}

export class ProfilesPage {
  private readonly onRegistryChanged: () => void;
  private readonly instanceId: number;
  private root: HTMLElement | null = null;
  private snapshot: RegistrySnapshot | null = null;
  private index: RegistryIndex | null = null;
  private renderedRevision: number | null = null;
  private draft: ProfileDraft | null = null;
  private confirmDeleteId: string | null = null;
  private pending: PendingState | null = null;
  private notice: Notice | null = null;
  private error: SafeError | null = null;
  private conflict = false;
  private disposed = false;
  private focusTimer: ReturnType<typeof setTimeout> | null = null;
  private reloadGeneration = 0;
  private overridesOpen = new Set<string>();

  constructor(onRegistryChanged: () => void) {
    this.onRegistryChanged = onRegistryChanged;
    this.instanceId = nextPageId;
    nextPageId += 1;
  }

  render(snapshot: RegistrySnapshot): HTMLElement {
    const focus = this.captureFocus();
    this.snapshot = snapshot;
    if (this.root && snapshot.revision === this.renderedRevision) {
      return this.root;
    }

    this.detectExternalChange(snapshot);
    this.index = buildIndex(snapshot);
    this.root = this.build(snapshot);
    this.renderedRevision = snapshot.revision;
    if (focus) {
      this.queueFocusRestore(this.root, focus);
    }
    return this.root;
  }

  dispose(): void {
    this.disposed = true;
    if (this.focusTimer !== null) {
      clearTimeout(this.focusTimer);
      this.focusTimer = null;
    }
    this.root = null;
    this.snapshot = null;
    this.index = null;
    this.draft = null;
    this.overridesOpen.clear();
  }

  private build(snapshot: RegistrySnapshot): HTMLElement {
    this.index = buildIndex(snapshot);
    const headingId = `aq-profiles-heading-${this.instanceId}`;
    const analysis = this.draft && this.index
      ? analyzeWithIndex(this.index, this.draft)
      : null;

    return h(
      'section',
      {
        class: 'aq-profiles',
        'aria-labelledby': headingId,
        'aria-busy': this.pending ? 'true' : undefined,
      },
      this.buildHeader(snapshot, headingId),
      ...this.buildBanners(),
      this.draft && analysis
        ? this.buildEditor(snapshot, this.draft, analysis)
        : null,
      this.buildList(snapshot)
    );
  }

  private buildHeader(snapshot: RegistrySnapshot, headingId: string): HTMLElement {
    const defaultProfile = snapshot.default_profile_id
      ? snapshot.profiles.find((profile) => profile.id === snapshot.default_profile_id)
      : null;
    const busy = this.pending !== null;

    return h(
      'header',
      { class: 'aq-profiles-head' },
      h(
        'div',
        { class: 'aq-profiles-heading' },
        h('p', { class: 'aq-profiles-kicker' }, 'Registry'),
        h('h2', { class: 'aq-profiles-title', id: headingId }, 'Profiles'),
        h(
          'p',
          { class: 'aq-profiles-subtitle' },
          'Map workflow roles and difficulties to enabled tools and models. Changes affect future task resolution only.'
        ),
        h(
          'p',
          { class: 'aq-profiles-meta' },
          h('span', {}, `Revision ${snapshot.revision}`),
          h(
            'span',
            {},
            `${snapshot.profiles.length} ${snapshot.profiles.length === 1 ? 'profile' : 'profiles'}`
          ),
          h(
            'span',
            {},
            defaultProfile ? `Default: ${defaultProfile.name}` : 'No default selected'
          )
        )
      ),
      h(
        'div',
        { class: 'aq-profiles-toolbar' },
        button('New profile', () => this.beginCreate(), {
          className: 'aq-btn aq-btn-primary',
          disabled: busy || this.draft !== null,
          focusKey: 'new-profile',
        }),
        defaultProfile
          ? button(
              this.pending?.kind === 'default' && !this.pending?.targetId
                ? 'Clearing...'
                : 'Clear default',
              () => void this.clearDefault(),
              {
                className: 'aq-btn',
                disabled: busy,
                focusKey: 'clear-default',
              }
            )
          : null,
        button(
          this.pending?.kind === 'reload' ? 'Reloading...' : 'Reload latest',
          () => void this.reloadLatest(),
          {
            className: 'aq-btn',
            disabled: busy,
            focusKey: 'reload-latest',
          }
        )
      )
    );
  }

  private buildBanners(): (HTMLElement | null)[] {
    const banners: (HTMLElement | null)[] = [];

    if (this.notice) {
      banners.push(
        banner(
          this.notice.tone,
          this.notice.tone === 'success' ? 'Saved' : 'Notice',
          this.notice.text,
          [
            button(
              'Dismiss',
              () => {
                this.notice = null;
                this.rebuild();
              },
              { className: 'aq-profiles-link', disabled: this.pending !== null }
            ),
          ]
        )
      );
    }

    if (this.error) {
      banners.push(
        banner(
          this.conflict ? 'warn' : 'error',
          this.conflict
            ? 'Registry changed before this action completed'
            : 'Registry action failed',
          this.error.code
            ? `${this.error.code}: ${this.error.message}`
            : this.error.message,
          [
            button(
              'Reload latest',
              () => void this.reloadLatest(),
              {
                className: 'aq-btn',
                disabled: this.pending !== null,
                focusKey: 'reload-latest',
              }
            ),
            button(
              'Dismiss',
              () => {
                this.error = null;
                this.conflict = false;
                this.rebuild();
              },
              { className: 'aq-profiles-link', disabled: this.pending !== null }
            ),
          ]
        )
      );
    }

    return banners;
  }

  private buildList(snapshot: RegistrySnapshot): HTMLElement {
    const listId = `aq-profiles-list-${this.instanceId}`;
    const children: (HTMLElement | null)[] = [
      h(
        'div',
        { class: 'aq-profiles-list-head' },
        h('h3', { id: listId }, 'Saved profiles'),
        h(
          'span',
          { class: 'aq-profiles-count' },
          `${snapshot.profiles.length} ${snapshot.profiles.length === 1 ? 'entry' : 'entries'}`
        )
      ),
    ];

    if (snapshot.profiles.length === 0) {
      children.push(
        h(
          'div',
          { class: 'aq-profiles-empty' },
          h('p', {}, 'No profiles saved yet.'),
          h(
            'p',
            {},
            'Create a profile to map roles and difficulties. Partial profiles can be saved and completed later.'
          )
        )
      );
    } else {
      for (const profile of snapshot.profiles) {
        children.push(this.buildProfileCard(snapshot, profile));
      }
    }

    return h(
      'section',
      { class: 'aq-profiles-list', 'aria-labelledby': listId },
      ...children
    );
  }

  private buildProfileCard(
    snapshot: RegistrySnapshot,
    profile: WorkflowProfile
  ): HTMLElement {
    const summary = summarizeProfile(snapshot, profile);
    const busy = this.pending !== null;
    const isDefault = snapshot.default_profile_id === profile.id;
    const isConfirming = this.confirmDeleteId === profile.id;
    const missing = summary.missingDefaults.map((role) => ROLE_LABELS[role]).join(', ');
    const visibleIssues = summary.issues.slice(0, 2);

    const badges: HTMLElement[] = [];
    if (isDefault) {
      badges.push(pill('Default', 'accent'));
    }
    if (summary.assignmentCount === 0) {
      badges.push(pill('Empty profile', 'muted'));
    } else if (summary.missingDefaults.length > 0) {
      badges.push(pill('Partial profile', 'warn'));
    }
    if (summary.issues.length > 0) {
      badges.push(
        pill(
          `${summary.issues.length} ${summary.issues.length === 1 ? 'issue' : 'issues'}`,
          'error'
        )
      );
    }

    return h(
      'article',
      { class: 'aq-profiles-card', 'aria-label': profile.name || 'Unnamed profile' },
      h(
        'div',
        { class: 'aq-profiles-card-head' },
        h(
          'div',
          { class: 'aq-profiles-card-title' },
          h('h4', {}, profile.name || 'Unnamed profile'),
          h('p', { class: 'aq-profiles-id' }, profile.id)
        ),
        h('div', { class: 'aq-profiles-badges' }, ...badges)
      ),
      h(
        'p',
        { class: 'aq-profiles-summary' },
        `${summary.defaultCount}/${ROLES.length} role defaults · ` +
          `${summary.overrideCount} difficulty overrides · ${summary.assignmentCount} assignments`
      ),
      summary.missingDefaults.length > 0
        ? h(
            'p',
            { class: 'aq-profiles-note', 'data-tone': 'warn' },
            h('strong', {}, 'Missing role defaults: '),
            missing
          )
        : null,
      visibleIssues.length > 0
        ? h(
            'ul',
            { class: 'aq-profiles-issues' },
            ...visibleIssues.map((issue) =>
              h(
                'li',
                {},
                `${ROLE_LABELS[issue.role]} ${slotLabel(issue.slot)}: ${issue.message}`
              )
            ),
            summary.issues.length > visibleIssues.length
              ? h('li', {}, `${summary.issues.length - visibleIssues.length} more`)
              : null
          )
        : null,
      h(
        'div',
        { class: 'aq-profiles-card-actions' },
        button('Edit', () => this.beginEdit(profile), {
          className: 'aq-btn',
          disabled: busy,
          focusKey: `edit-${profile.id}`,
          ariaLabel: `Edit ${profile.name}`,
        }),
        button('Clone', () => this.beginClone(profile), {
          className: 'aq-btn',
          disabled: busy,
          focusKey: `clone-${profile.id}`,
          ariaLabel: `Clone ${profile.name}`,
        }),
        button(
          this.pending?.kind === 'default' && this.pending?.targetId === profile.id
            ? 'Setting default...'
            : 'Set default',
          () => void this.setDefault(profile),
          {
            className: 'aq-btn',
            disabled: busy || isDefault,
            focusKey: `default-${profile.id}`,
            ariaLabel: `Set ${profile.name} as default`,
          }
        ),
        button('Delete', () => this.requestDelete(profile), {
          className: 'aq-btn aq-profiles-danger-button',
          disabled: busy,
          focusKey: `delete-${profile.id}`,
          ariaLabel: `Delete ${profile.name}`,
        })
      ),
      isConfirming
        ? h(
            'div',
            {
              class: 'aq-profiles-confirm',
              role: 'alertdialog',
              'aria-label': `Confirm deletion of ${profile.name}`,
            },
            h('p', {}, `Delete "${profile.name}" permanently?`),
            h(
              'p',
              {},
              'This cannot be undone. Running tasks keep their resolved choices; future tasks will not use this profile.'
            ),
            isDefault
              ? h(
                  'p',
                  { class: 'aq-profiles-note', 'data-tone': 'warn' },
                  'This profile is currently the default.'
                )
              : null,
            h(
              'div',
              { class: 'aq-profiles-confirm-actions' },
              button(
                this.pending?.kind === 'delete' && this.pending?.targetId === profile.id
                  ? 'Deleting...'
                  : 'Delete profile',
                () => void this.deleteProfile(profile),
                {
                  className: 'aq-btn aq-btn-danger',
                  disabled: busy,
                  focusKey: `confirm-delete-${profile.id}`,
                }
              ),
              button(
                'Cancel',
                () => {
                  this.confirmDeleteId = null;
                  this.rebuild(`delete-${profile.id}`);
                },
                {
                  className: 'aq-btn',
                  disabled: busy,
                  focusKey: `cancel-delete-${profile.id}`,
                }
              )
            )
          )
        : null
    );
  }

  private buildEditor(
    snapshot: RegistrySnapshot,
    draft: ProfileDraft,
    analysis: DraftAnalysis
  ): HTMLElement {
    const editorId = `aq-profiles-editor-${this.instanceId}`;
    const nameId = `aq-profiles-name-${this.instanceId}`;
    const busy = this.pending !== null;
    const title = draft.mode === 'edit'
      ? 'Edit profile'
      : draft.cloneOfName
        ? 'Clone profile'
        : 'New profile';
    const context = draft.mode === 'edit'
      ? 'Editing saved profile'
      : draft.cloneOfName
        ? `Cloned from "${draft.cloneOfName}"`
        : 'Unsaved draft';

    const nameInput = h('input', {
      id: nameId,
      class: 'aq-profiles-input',
      type: 'text',
      maxlength: MAX_PROFILE_NAME_LENGTH,
      autocomplete: 'off',
      spellcheck: 'false',
      'data-focus-key': 'profile-name',
    });
    nameInput.value = draft.name;
    nameInput.disabled = busy;
    nameInput.addEventListener('input', () => {
      if (this.draft) {
        this.draft.name = nameInput.value;
        this.refreshDraftStatus();
      }
    });

    return h(
      'section',
      { class: 'aq-profiles-editor', 'aria-labelledby': editorId },
      h(
        'div',
        { class: 'aq-profiles-editor-head' },
        h(
          'div',
          {},
          h('h3', { id: editorId }, title),
          h(
            'p',
            { class: 'aq-profiles-meta' },
            context,
            ' · ',
            `Draft revision ${draft.sourceRevision}`
          )
        ),
        pill('Draft', 'accent')
      ),
      ...this.buildExternalChangeBanner(draft),
      h(
        'div',
        { class: 'aq-profiles-fields' },
        h(
          'label',
          { class: 'aq-profiles-field', for: nameId },
          h('span', {}, 'Profile name'),
          nameInput
        ),
        h(
          'p',
          { class: 'aq-profiles-id' },
          `${draft.id} · Saves against registry revision ${snapshot.revision}.`
        )
      ),
      h('div', { 'data-draft-status': 'true' }, buildDraftStatus(analysis)),
      h(
        'div',
        { class: 'aq-profiles-roles' },
        ...ROLES.map((role) => this.buildRoleEditor(role, draft, analysis, busy))
      ),
      h(
        'div',
        { class: 'aq-profiles-editor-actions' },
        button(
          this.pending?.kind === 'save'
            ? 'Saving...'
            : draft.mode === 'edit'
              ? 'Save changes'
              : 'Create profile',
          () => void this.saveDraft(),
          {
            className: 'aq-btn aq-btn-primary',
            disabled:
              busy ||
              this.conflict ||
              analysis.errors.length > 0 ||
              needsAcknowledgement(draft),
            focusKey: 'save-profile',
          }
        ),
        button('Cancel', () => this.cancelDraft(), {
          className: 'aq-btn',
          disabled: busy,
          focusKey: 'cancel-profile',
        })
      ),
      h(
        'p',
        { class: 'aq-profiles-footnote' },
        'Profiles are preferences. Running tasks keep their resolved model choices until they finish.'
      )
    );
  }

  private buildExternalChangeBanner(draft: ProfileDraft): (HTMLElement | null)[] {
    if (!draft.externalChanged) {
      return [];
    }

    const actions: HTMLElement[] = [
      button(
        draft.externalDeleted ? 'Discard draft and close' : 'Load latest into editor',
        () => this.loadLatestDraft(),
        {
          className: 'aq-btn',
          disabled: this.pending !== null,
          focusKey: 'load-latest-draft',
        }
      ),
    ];
    if (!draft.overwriteAcknowledged) {
      actions.push(
        button(
          draft.externalDeleted ? 'Keep draft and recreate' : 'Keep my draft',
          () => this.acknowledgeOverwrite(),
          {
            className: 'aq-btn aq-btn-primary',
            disabled: this.pending !== null,
            focusKey: 'keep-draft',
          }
        )
      );
    }

    return [
      banner(
        'warn',
        draft.externalDeleted
          ? 'This profile was removed from the latest registry'
          : draft.overwriteAcknowledged
            ? 'Your draft will replace the newer saved version'
            : 'This profile changed after your draft started',
        draft.externalDeleted
          ? 'Your input is preserved. Choose whether to close the draft or recreate the profile.'
          : 'Your input is preserved. Load the latest version, or keep the draft and save explicitly.',
        actions
      ),
    ];
  }

  private buildRoleEditor(
    role: WorkflowRole,
    draft: ProfileDraft,
    analysis: DraftAnalysis,
    busy: boolean
  ): HTMLElement {
    const state = roleState(analysis, draft.assignments, role);
    const overrideCount = DIFFICULTIES.filter(
      (slot) => draft.assignments[role][slot].model_id
    ).length;
    const overrides = h('details', {
      class: 'aq-profiles-overrides',
    }) as HTMLDetailsElement;
    overrides.open = this.overridesOpen.has(role);
    overrides.append(
      h(
        'summary',
        { class: 'aq-profiles-overrides-summary' },
        h('span', {}, 'Difficulty overrides'),
        h(
          'span',
          {
            class: 'aq-profiles-overrides-count',
            'data-override-count': role,
            'data-tone': overrideCount > 0 ? 'accent' : 'muted',
          },
          `${overrideCount} assigned`
        )
      ),
      h(
        'div',
        { class: 'aq-profiles-overrides-body' },
        ...DIFFICULTIES.map((slot) => this.buildChoiceRow(role, slot, busy))
      )
    );
    overrides.addEventListener('toggle', () => {
      if (overrides.open) this.overridesOpen.add(role);
      else this.overridesOpen.delete(role);
    });
    return h(
      'fieldset',
      {
        class: 'aq-profiles-role',
        'data-profile-role': role,
        'data-role-state': state.tone,
      },
      h(
        'legend',
        {},
        h('span', {}, ROLE_LABELS[role]),
        h(
          'span',
          {
            class: 'aq-profiles-role-status',
            'data-role-status': role,
            'data-tone': state.tone,
          },
          state.label
        )
      ),
      this.buildChoiceRow(role, 'default', busy),
      overrides
    );
  }

  private buildChoiceRow(role: WorkflowRole, slot: Slot, busy: boolean): HTMLElement {
    if (!this.index || !this.draft) {
      return h('div', {});
    }
    const cell = this.draft.assignments[role][slot];
    const label = slot === 'default' ? 'Role default' : DIFFICULTY_LABELS[slot];
    const key = `${role}:${slot}`;

    return h(
      'div',
      { class: 'aq-profiles-row', 'data-row-key': key },
      h('span', { class: 'aq-profiles-row-label' }, label),
      h(
        'label',
        { class: 'aq-profiles-select-field' },
        h('span', {}, 'Model'),
        this.buildModelSelect(role, slot, cell, busy)
      ),
      h(
        'label',
        { class: 'aq-profiles-select-field' },
        h('span', {}, 'Effort'),
        this.buildEffortSelect(role, slot, cell, busy)
      ),
      button(
        'Clear',
        () => this.clearRow(role, slot),
        {
          className: 'aq-profiles-link',
          disabled: busy,
          focusKey: `clear-${role}-${slot}`,
          ariaLabel: `Clear ${ROLE_LABELS[role]} ${label}`,
        }
      )
    );
  }

  private buildModelSelect(
    role: WorkflowRole,
    slot: Slot,
    cell: Choice,
    busy: boolean
  ): HTMLSelectElement {
    const index = this.index;
    const key = `${role}:${slot}`;
    const select = h('select', {
      class: 'aq-profiles-select',
      'aria-label': `${ROLE_LABELS[role]} ${slotLabel(slot)} model`,
      'data-model-key': key,
      'data-focus-key': `model-${role}-${slot}`,
    });
    select.append(h('option', { value: '' }, 'Not assigned'));

    if (index) {
      let selected = false;
      for (const group of index.groups) {
        const optgroup = h('optgroup', { label: group.tool.label });
        for (const model of group.models) {
          const option = h('option', { value: model.id }, model.label || model.id);
          option.title = model.id;
          if (model.id === cell.model_id) {
            option.selected = true;
            selected = true;
          }
          optgroup.append(option);
        }
        if (group.models.length > 0) {
          select.append(optgroup);
        }
      }

      if (cell.model_id && !selected) {
        const model = index.models.get(cell.model_id);
        const unavailable = h(
          'option',
          { value: cell.model_id },
          `${model?.label || cell.model_id} (unavailable)`
        );
        unavailable.disabled = true;
        unavailable.selected = true;
        select.append(unavailable);
      }
    }

    select.value = cell.model_id;
    select.disabled = busy;
    select.addEventListener('change', () => {
      if (!this.draft) {
        return;
      }
      const current = this.draft.assignments[role][slot];
      current.model_id = select.value;
      if (!current.model_id) {
        current.effort = null;
      }
      this.syncEffort(role, slot);
      this.refreshDraftStatus();
    });
    return select;
  }

  private buildEffortSelect(
    role: WorkflowRole,
    slot: Slot,
    cell: Choice,
    busy: boolean
  ): HTMLSelectElement {
    const model = cell.model_id && this.index
      ? this.index.models.get(cell.model_id)
      : null;
    const select = h('select', {
      class: 'aq-profiles-select',
      'aria-label': `${ROLE_LABELS[role]} ${slotLabel(slot)} effort`,
      'data-effort-key': `${role}:${slot}`,
      'data-focus-key': `effort-${role}-${slot}`,
    });

    if (!model) {
      const option = h('option', { value: '' }, 'Select a model first');
      option.selected = true;
      select.append(option);
      select.disabled = true;
      return select;
    }

    let selected = false;
    if (model.allow_no_effort) {
      const option = h('option', { value: '' }, 'No effort');
      if (cell.effort === null) {
        option.selected = true;
        selected = true;
      }
      select.append(option);
    } else if (cell.effort === null) {
      const placeholder = h('option', { value: '' }, 'Select an effort');
      placeholder.disabled = true;
      placeholder.selected = true;
      select.append(placeholder);
      selected = true;
    }

    for (const effort of model.efforts) {
      const option = h('option', { value: effort }, effort);
      if (effort === cell.effort) {
        option.selected = true;
        selected = true;
      }
      select.append(option);
    }

    if (cell.effort !== null && !selected) {
      const unsupported = h(
        'option',
        { value: cell.effort },
        `${cell.effort} (unsupported)`
      );
      unsupported.disabled = true;
      unsupported.selected = true;
      select.append(unsupported);
    }

    select.value = cell.effort ?? '';
    select.disabled = busy;
    select.addEventListener('change', () => {
      if (!this.draft) {
        return;
      }
      const current = this.draft.assignments[role][slot];
      const currentModel = this.index?.models.get(current.model_id);
      const next = select.value === '' ? null : select.value;
      if (!currentModel) {
        return;
      }
      if (next === null && !currentModel.allow_no_effort) {
        return;
      }
      if (next !== null && !currentModel.efforts.includes(next)) {
        return;
      }
      current.effort = next;
      this.refreshDraftStatus();
    });
    return select;
  }

  private syncEffort(role: WorkflowRole, slot: Slot): void {
    if (!this.root || !this.draft) {
      return;
    }
    const current = this.root.querySelector<HTMLSelectElement>(
      `[data-effort-key="${role}:${slot}"]`
    );
    if (!current) {
      return;
    }
    current.replaceWith(
      this.buildEffortSelect(
        role,
        slot,
        this.draft.assignments[role][slot],
        this.pending !== null
      )
    );
  }

  private refreshDraftStatus(): void {
    if (!this.root || !this.snapshot || !this.index || !this.draft) {
      return;
    }
    const analysis = analyzeWithIndex(this.index, this.draft);
    const status = this.root.querySelector<HTMLElement>('[data-draft-status]');
    if (status) {
      status.replaceChildren(buildDraftStatus(analysis));
    }

    const save = this.root.querySelector<HTMLButtonElement>(
      '[data-focus-key="save-profile"]'
    );
    if (save) {
      save.disabled =
        this.pending !== null ||
        this.conflict ||
        analysis.errors.length > 0 ||
        needsAcknowledgement(this.draft);
    }

    for (const role of ROLES) {
      const state = roleState(analysis, this.draft.assignments, role);
      const assignments = this.draft.assignments;
      const overrideCount = DIFFICULTIES.filter(
        (slot) => assignments[role][slot].model_id
      ).length;
      const fieldset = this.root.querySelector<HTMLElement>(
        `[data-profile-role="${role}"]`
      );
      if (fieldset) {
        fieldset.dataset.roleState = state.tone;
      }
      const badge = this.root.querySelector<HTMLElement>(
        `[data-role-status="${role}"]`
      );
      if (badge) {
        badge.dataset.tone = state.tone;
        badge.textContent = state.label;
      }
      const count = this.root.querySelector<HTMLElement>(
        `[data-override-count="${role}"]`
      );
      if (count) {
        count.textContent = `${overrideCount} assigned`;
        count.dataset.tone = overrideCount > 0 ? 'accent' : 'muted';
      }
    }
  }

  private clearRow(role: WorkflowRole, slot: Slot): void {
    if (!this.root || !this.draft || this.pending !== null) {
      return;
    }
    const cell = this.draft.assignments[role][slot];
    cell.model_id = '';
    cell.effort = null;
    const select = this.root.querySelector<HTMLSelectElement>(
      `[data-model-key="${role}:${slot}"]`
    );
    if (select) {
      select.value = '';
    }
    this.syncEffort(role, slot);
    this.refreshDraftStatus();
  }

  private beginCreate(): void {
    if (!this.snapshot || this.pending !== null) {
      return;
    }
    if (this.draft) {
      this.notice = {
        tone: 'info',
        text: 'Save or cancel the current draft before starting another profile.',
      };
      this.rebuild();
      return;
    }
    try {
      this.draft = {
        mode: 'create',
        id: profileId(this.snapshot),
        name: '',
        sourceId: null,
        sourceRevision: this.snapshot.revision,
        sourceSignature: null,
        cloneOfName: null,
        assignments: createEmptyTable(),
        externalChanged: false,
        externalDeleted: false,
        overwriteAcknowledged: false,
        acknowledgedSignature: null,
      };
      this.resetTransientState();
      this.rebuild('profile-name');
    } catch (error) {
      this.error = normalizeError(error);
      this.rebuild();
    }
  }

  private beginEdit(profile: WorkflowProfile): void {
    if (!this.snapshot || this.pending !== null) {
      return;
    }
    if (this.draft) {
      this.notice = {
        tone: 'info',
        text: 'Save or cancel the current draft before editing another profile.',
      };
      this.rebuild();
      return;
    }
    this.draft = {
      mode: 'edit',
      id: profile.id,
      name: profile.name,
      sourceId: profile.id,
      sourceRevision: this.snapshot.revision,
      sourceSignature: profileSignature(profile),
      cloneOfName: null,
      assignments: tableFromProfile(profile),
      externalChanged: false,
      externalDeleted: false,
      overwriteAcknowledged: false,
      acknowledgedSignature: null,
    };
    this.resetTransientState();
    this.rebuild('profile-name');
  }

  private beginClone(profile: WorkflowProfile): void {
    if (!this.snapshot || this.pending !== null) {
      return;
    }
    if (this.draft) {
      this.notice = {
        tone: 'info',
        text: 'Save or cancel the current draft before cloning another profile.',
      };
      this.rebuild();
      return;
    }
    try {
      this.draft = {
        mode: 'create',
        id: profileId(this.snapshot),
        name: copyName(profile.name),
        sourceId: null,
        sourceRevision: this.snapshot.revision,
        sourceSignature: null,
        cloneOfName: profile.name,
        assignments: cloneTable(tableFromProfile(profile)),
        externalChanged: false,
        externalDeleted: false,
        overwriteAcknowledged: false,
        acknowledgedSignature: null,
      };
      this.resetTransientState();
      this.rebuild('profile-name');
    } catch (error) {
      this.error = normalizeError(error);
      this.rebuild();
    }
  }

  private resetTransientState(): void {
    this.confirmDeleteId = null;
    this.notice = null;
    this.error = null;
    this.conflict = false;
    this.overridesOpen.clear();
  }

  private cancelDraft(): void {
    if (this.pending !== null || !this.draft) {
      return;
    }
    this.draft = null;
    this.resetTransientState();
    this.rebuild('new-profile');
  }

  private async saveDraft(): Promise<void> {
    if (!this.snapshot || !this.draft || this.pending !== null) {
      return;
    }
    if (!this.index || needsAcknowledgement(this.draft)) {
      return;
    }
    const analysis = analyzeWithIndex(this.index, this.draft);
    if (analysis.errors.length > 0) {
      return;
    }

    const profile = profileFromDraft(this.draft);
    await this.runChange(
      { op: 'upsert_profile', profile },
      { kind: 'save', targetId: profile.id },
      `Profile "${profile.name}" saved.`,
      `edit-${profile.id}`,
      () => {
        this.draft = null;
        this.confirmDeleteId = null;
      }
    );
  }

  private requestDelete(profile: WorkflowProfile): void {
    if (this.pending !== null) {
      return;
    }
    this.confirmDeleteId = profile.id;
    this.notice = null;
    this.error = null;
    this.conflict = false;
    this.rebuild(`confirm-delete-${profile.id}`);
  }

  private async deleteProfile(profile: WorkflowProfile): Promise<void> {
    await this.runChange(
      { op: 'delete_profile', profile_id: profile.id },
      { kind: 'delete', targetId: profile.id },
      `Profile "${profile.name}" deleted.`,
      'new-profile',
      () => {
        if (this.draft?.sourceId === profile.id) {
          this.draft = null;
        }
        this.confirmDeleteId = null;
      }
    );
  }

  private async setDefault(profile: WorkflowProfile): Promise<void> {
    await this.runChange(
      { op: 'set_default_profile', profile_id: profile.id },
      { kind: 'default', targetId: profile.id },
      `Default profile set to "${profile.name}".`,
      `edit-${profile.id}`
    );
  }

  private async clearDefault(): Promise<void> {
    await this.runChange(
      { op: 'set_default_profile', profile_id: null },
      { kind: 'default' },
      'Default profile cleared.',
      'new-profile'
    );
  }

  private async runChange(
    change: RegistryChange,
    pending: PendingState,
    successText: string,
    focusKey: string,
    onSuccess?: () => void
  ): Promise<void> {
    if (!this.snapshot || this.pending !== null) {
      return;
    }
    const expectedRevision = this.snapshot.revision;
    this.pending = pending;
    this.notice = null;
    this.error = null;
    this.conflict = false;
    this.rebuild();

    try {
      const updated = await desktop_registry_apply({
        expectedRevision,
        change,
      });
      if (this.disposed) {
        return;
      }
      this.pending = null;
      this.snapshot = updated;
      this.renderedRevision = updated.revision;
      onSuccess?.();
      this.detectExternalChange(updated);
      this.notice = { tone: 'success', text: successText };
      this.error = null;
      this.conflict = false;
      this.onRegistryChanged();
      this.rebuild(focusKey);
    } catch (error) {
      if (this.disposed) {
        return;
      }
      this.pending = null;
      this.error = normalizeError(error);
      this.conflict = isRevisionConflict(this.error);
      this.rebuild(this.draft ? 'save-profile' : 'reload-latest');
    }
  }

  private async reloadLatest(): Promise<void> {
    if (this.pending !== null) {
      return;
    }
    this.reloadGeneration += 1;
    const generation = this.reloadGeneration;
    this.pending = { kind: 'reload' };
    this.notice = null;
    this.error = null;
    this.conflict = false;
    this.rebuild();

    try {
      const latest = await desktop_registry_snapshot();
      if (this.disposed || generation !== this.reloadGeneration) {
        return;
      }
      this.pending = null;
      this.snapshot = latest;
      this.renderedRevision = latest.revision;
      this.index = buildIndex(latest);
      this.detectExternalChange(latest);
      this.notice = {
        tone: 'info',
        text: 'Loaded the latest registry. Your draft was preserved.',
      };
      this.error = null;
      this.conflict = false;
      this.onRegistryChanged();
      this.rebuild(this.draft ? 'save-profile' : 'new-profile');
    } catch (error) {
      if (this.disposed || generation !== this.reloadGeneration) {
        return;
      }
      this.pending = null;
      this.error = normalizeError(error);
      this.conflict = false;
      this.rebuild(this.draft ? 'save-profile' : 'reload-latest');
    }
  }

  private acknowledgeOverwrite(): void {
    if (!this.snapshot || !this.draft || this.pending !== null) {
      return;
    }
    const latest = this.latestSourceProfile(this.snapshot);
    this.draft.overwriteAcknowledged = true;
    this.draft.acknowledgedSignature = latest
      ? profileSignature(latest)
      : DELETED_SIGNATURE;
    this.notice = null;
    this.rebuild('save-profile');
  }

  private loadLatestDraft(): void {
    if (!this.snapshot || !this.draft || this.pending !== null || !this.draft.sourceId) {
      return;
    }
    const latest = this.latestSourceProfile(this.snapshot);
    if (!latest) {
      this.draft = null;
      this.notice = {
        tone: 'warn',
        text: 'The saved profile no longer exists in the latest registry. The draft was closed.',
      };
      this.error = null;
      this.conflict = false;
      this.rebuild('new-profile');
      return;
    }

    this.draft.name = latest.name;
    this.draft.assignments = tableFromProfile(latest);
    this.draft.sourceRevision = this.snapshot.revision;
    this.draft.sourceSignature = profileSignature(latest);
    this.draft.externalChanged = false;
    this.draft.externalDeleted = false;
    this.draft.overwriteAcknowledged = false;
    this.draft.acknowledgedSignature = null;
    this.notice = {
      tone: 'info',
      text: 'Loaded the latest saved version into the editor.',
    };
    this.error = null;
    this.conflict = false;
    this.rebuild('profile-name');
  }

  private latestSourceProfile(snapshot: RegistrySnapshot): WorkflowProfile | null {
    if (!this.draft?.sourceId) {
      return null;
    }
    return snapshot.profiles.find((profile) => profile.id === this.draft?.sourceId) ?? null;
  }

  private detectExternalChange(snapshot: RegistrySnapshot): void {
    const draft = this.draft;
    if (!draft || draft.mode !== 'edit' || !draft.sourceId) {
      return;
    }

    const latest = snapshot.profiles.find((profile) => profile.id === draft.sourceId);
    if (!latest) {
      draft.externalChanged = true;
      draft.externalDeleted = true;
      if (draft.acknowledgedSignature !== DELETED_SIGNATURE) {
        draft.overwriteAcknowledged = false;
        draft.acknowledgedSignature = null;
      }
      return;
    }

    const signature = profileSignature(latest);
    if (signature !== draft.sourceSignature) {
      draft.externalChanged = true;
      draft.externalDeleted = false;
      if (draft.acknowledgedSignature !== signature) {
        draft.overwriteAcknowledged = false;
        draft.acknowledgedSignature = null;
      }
      return;
    }

    draft.externalChanged = false;
    draft.externalDeleted = false;
    draft.overwriteAcknowledged = false;
    draft.acknowledgedSignature = null;
  }

  private rebuild(focusKey?: string | FocusState | null): void {
    if (this.disposed || !this.snapshot) {
      return;
    }
    const focus = focusKey ?? this.captureFocus();
    const next = this.build(this.snapshot);
    const previous = this.root;
    this.root = next;
    this.renderedRevision = this.snapshot.revision;
    if (previous?.parentNode) {
      previous.parentNode.replaceChild(next, previous);
    }
    if (focus) {
      this.queueFocusRestore(
        next,
        typeof focus === 'string' ? { key: focus, start: null, end: null } : focus
      );
    }
  }

  private captureFocus(): FocusState | null {
    if (!this.root || typeof document === 'undefined') {
      return null;
    }
    const active = document.activeElement as HTMLElement | null;
    if (!active || !this.root.contains(active)) {
      return null;
    }
    const key = active.dataset.focusKey;
    if (!key) {
      return null;
    }
    if (active instanceof HTMLInputElement && active.type === 'text') {
      return { key, start: active.selectionStart, end: active.selectionEnd };
    }
    return { key, start: null, end: null };
  }

  private queueFocusRestore(root: HTMLElement, focus: FocusState): void {
    if (this.focusTimer !== null) {
      clearTimeout(this.focusTimer);
      this.focusTimer = null;
    }

    const attempt = (): boolean => {
      if (this.disposed || !root.isConnected) {
        return false;
      }
      const target = Array.from(root.querySelectorAll<HTMLElement>('[data-focus-key]'))
        .find((element) => element.dataset.focusKey === focus.key);
      if (!target) {
        return false;
      }
      if (
        (target instanceof HTMLButtonElement ||
          target instanceof HTMLInputElement ||
          target instanceof HTMLSelectElement) &&
        target.disabled
      ) {
        return false;
      }
      target.focus({ preventScroll: true });
      if (
        target instanceof HTMLInputElement &&
        target.type === 'text' &&
        focus.start !== null &&
        focus.end !== null
      ) {
        target.setSelectionRange(focus.start, focus.end);
      }
      return true;
    };

    queueMicrotask(() => {
      if (this.disposed || attempt()) {
        return;
      }
      this.focusTimer = setTimeout(() => {
        this.focusTimer = null;
        attempt();
      }, 0);
    });
  }
}
