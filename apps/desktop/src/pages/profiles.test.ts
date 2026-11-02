import assert from 'node:assert/strict';
import Module from 'node:module';
import { describe, it } from 'node:test';

import {
  EXECUTION_ROLES,
  WORKFLOW_ROLES,
  createEmptyRegistrySnapshot,
  type ModelDefinition,
  type RegistrySnapshot,
  type RoleAssignment,
  type ToolDefinition,
  type WorkflowProfile,
} from '../registry/types.ts';

if (typeof (Module as { registerHooks?: unknown }).registerHooks === 'function') {
  (
    Module as unknown as {
      registerHooks: (hooks: {
        load?: (
          url: string,
          context: unknown,
          nextLoad: (url: string, context: unknown) => unknown
        ) => unknown;
      }) => void;
    }
  ).registerHooks({
    load(url, context, nextLoad) {
      if (url.endsWith('.css')) {
        return { format: 'module', shortCircuit: true, source: 'export default {};' };
      }
      return nextLoad(url, context);
    },
  });
} else if (typeof Module.register === 'function') {
  const loaderSource = `
    export async function load(url, context, nextLoad) {
      if (url.endsWith('.css')) {
        return { format: 'module', shortCircuit: true, source: 'export default {};' };
      }
      return nextLoad(url, context);
    }
  `;
  Module.register(
    `data:text/javascript,${encodeURIComponent(loaderSource)}`,
    import.meta.url
  );
}

class FakeNode {
  parentNode: FakeElement | null = null;
  parentElement: FakeElement | null = null;
  protected _textContent = '';

  get textContent(): string {
    return this._textContent;
  }

  set textContent(value: string) {
    this._textContent = value;
  }
}

class FakeTextNode extends FakeNode {
  constructor(text: string) {
    super();
    this._textContent = text;
  }
}

class FakeElement extends FakeNode {
  readonly tagName: string;
  className = '';
  id = '';
  title = '';
  value = '';
  type = '';
  checked = false;
  selected = false;
  disabled = false;
  open = false;
  scrollTop = 0;
  scrollLeft = 0;
  selectionStart: number | null = null;
  selectionEnd: number | null = null;
  readonly isConnected = true;
  private readonly datasetStore: Record<string, string> = {};
  readonly dataset: Record<string, string>;
  readonly attributes = new Map<string, string>();
  readonly children: FakeNode[] = [];
  private readonly listeners = new Map<string, Array<() => void>>();

  constructor(tagName: string) {
    super();
    this.tagName = tagName.toLowerCase();
    this.dataset = new Proxy(this.datasetStore, {
      set: (target, prop, value): boolean => {
        if (typeof prop === 'string') {
          const strValue = String(value);
          target[prop] = strValue;
          const attrName = `data-${prop.replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)}`;
          this.attributes.set(attrName, strValue);
        }
        return true;
      },
      deleteProperty: (target, prop): boolean => {
        if (typeof prop === 'string') {
          delete target[prop];
          const attrName = `data-${prop.replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`)}`;
          this.attributes.delete(attrName);
        }
        return true;
      },
    });
  }

  setAttribute(name: string, value: string): void {
    this.attributes.set(name, value);
    if (name === 'id') {
      this.id = value;
    } else if (name === 'class') {
      this.className = value;
    } else if (name.startsWith('data-')) {
      const camelKey = name
        .slice(5)
        .replace(/-([a-z])/g, (_, letter: string) => letter.toUpperCase());
      this.datasetStore[camelKey] = value;
    }
  }

  getAttribute(name: string): string | null {
    return this.attributes.get(name) ?? null;
  }

  appendChild<T extends FakeNode>(child: T): T {
    child.parentNode = this;
    child.parentElement = this;
    this.children.push(child);
    return child;
  }

  append(...nodes: Array<FakeNode | string | null | undefined>): void {
    for (const node of nodes) {
      if (node === null || node === undefined) {
        continue;
      }
      if (typeof node === 'string') {
        this.appendChild(new FakeTextNode(node));
      } else {
        this.appendChild(node);
      }
    }
  }

  replaceChildren(...nodes: FakeNode[]): void {
    this.children.length = 0;
    for (const node of nodes) {
      this.appendChild(node);
    }
  }

  replaceChild(newChild: FakeNode, oldChild: FakeNode): void {
    const idx = this.children.indexOf(oldChild);
    if (idx !== -1) {
      newChild.parentNode = this;
      newChild.parentElement = this;
      oldChild.parentNode = null;
      oldChild.parentElement = null;
      this.children[idx] = newChild;
    }
  }

  replaceWith(nextNode: FakeNode): void {
    if (this.parentNode) {
      this.parentNode.replaceChild(nextNode, this);
    }
  }

  addEventListener(event: string, handler: () => void): void {
    const list = this.listeners.get(event) ?? [];
    list.push(handler);
    this.listeners.set(event, list);
  }

  click(): void {
    this.dispatchEvent('click');
  }

  dispatchEvent(event: string): void {
    if (this.disabled) {
      return;
    }
    for (const handler of this.listeners.get(event) ?? []) {
      handler();
    }
  }

  focus(_options?: unknown): void {}

  setSelectionRange(start: number, end: number): void {
    this.selectionStart = start;
    this.selectionEnd = end;
  }

  contains(target: FakeNode | null): boolean {
    if (!target) {
      return false;
    }
    if (target === this) {
      return true;
    }
    for (const child of this.children) {
      if (child instanceof FakeElement && child.contains(target)) {
        return true;
      }
    }
    return false;
  }

  querySelectorAll<T extends FakeElement = FakeElement>(selector: string): T[] {
    const results: T[] = [];
    const visit = (node: FakeElement): void => {
      for (const child of node.children) {
        if (child instanceof FakeElement) {
          if (matchesSelector(child, selector)) {
            results.push(child as T);
          }
          visit(child);
        }
      }
    };
    visit(this);
    return results;
  }

  querySelector<T extends FakeElement = FakeElement>(selector: string): T | null {
    return this.querySelectorAll<T>(selector)[0] ?? null;
  }

  override get textContent(): string {
    return this.children
      .map((child) => (child instanceof FakeElement ? child.textContent : child.textContent))
      .join('');
  }

  override set textContent(value: string) {
    this.children.length = 0;
    this.appendChild(new FakeTextNode(value));
  }
}

class FakeButtonElement extends FakeElement {}
class FakeInputElement extends FakeElement {}
class FakeSelectElement extends FakeElement {}
class FakeTextAreaElement extends FakeElement {}
class FakeOptionElement extends FakeElement {}
class FakeFieldSetElement extends FakeElement {}
class FakeDetailsElement extends FakeElement {}

function matchesSelector(el: FakeElement, selector: string): boolean {
  const exactAttr = selector.match(/^\[([a-z0-9-]+)="([^"]*)"\]$/i);
  if (exactAttr) {
    return el.getAttribute(exactAttr[1]) === exactAttr[2];
  }
  const hasAttr = selector.match(/^\[([a-z0-9-]+)\]$/i);
  if (hasAttr) {
    return el.attributes.has(hasAttr[1]);
  }
  return el.tagName === selector.toLowerCase();
}

function installFakeDom(): void {
  const g = globalThis as Record<string, unknown>;
  g.Node = FakeNode;
  g.HTMLElement = FakeElement;
  g.HTMLButtonElement = FakeButtonElement;
  g.HTMLInputElement = FakeInputElement;
  g.HTMLSelectElement = FakeSelectElement;
  g.HTMLTextAreaElement = FakeTextAreaElement;
  g.HTMLOptionElement = FakeOptionElement;
  g.HTMLFieldSetElement = FakeFieldSetElement;
  g.HTMLDetailsElement = FakeDetailsElement;
  g.document = {
    activeElement: null,
    createElement(tag: string): FakeElement {
      switch (tag.toLowerCase()) {
        case 'button':
          return new FakeButtonElement(tag);
        case 'input':
          return new FakeInputElement(tag);
        case 'select':
          return new FakeSelectElement(tag);
        case 'textarea':
          return new FakeTextAreaElement(tag);
        case 'option':
          return new FakeOptionElement(tag);
        case 'fieldset':
          return new FakeFieldSetElement(tag);
        case 'details':
          return new FakeDetailsElement(tag);
        default:
          return new FakeElement(tag);
      }
    },
    createTextNode(text: string): FakeTextNode {
      return new FakeTextNode(text);
    },
  };
}

installFakeDom();

const {
  PROFILE_EXECUTION_ROLES,
  ProfilesPage,
  analyzeDraft,
  assignmentsFromTable,
  createEmptyTable,
  profileFromDraft,
  profileSignature,
  summarizeProfile,
  tableFromProfile,
} = await import('./profiles.ts');

function makeTool(overrides: Partial<ToolDefinition> = {}): ToolDefinition {
  return {
    id: 'agy',
    label: 'Antigravity CLI',
    adapter: 'antigravity',
    executable_path: null,
    enabled: true,
    ...overrides,
  };
}

function makeModel(overrides: Partial<ModelDefinition> = {}): ModelDefinition {
  return {
    id: 'gemini-argon',
    tool_id: 'agy',
    label: 'Gemini 4 Argon High',
    model_arg: 'gemini-4-argon-high',
    efforts: ['low', 'medium', 'high'],
    allow_no_effort: false,
    ...overrides,
  };
}

function makeSnapshot(profiles: WorkflowProfile[] = []): RegistrySnapshot {
  return {
    ...createEmptyRegistrySnapshot(),
    revision: 3,
    tools: [makeTool()],
    models: [makeModel()],
    profiles,
    default_profile_id: profiles[0]?.id ?? null,
  };
}

function makeFiveRoleAssignments(
  modelId: string = 'gemini-argon',
  effort: string | null = 'high'
): RoleAssignment[] {
  return EXECUTION_ROLES.map((role) => ({
    role,
    difficulty: null,
    choice: { model_id: modelId, effort },
  }));
}

function makeLegacySixRoleAssignments(
  modelId: string = 'gemini-argon',
  effort: string | null = 'high'
): RoleAssignment[] {
  return WORKFLOW_ROLES.map((role) => ({
    role,
    difficulty: null,
    choice: { model_id: modelId, effort },
  }));
}

describe('profiles coworker native model simplification', () => {
  it('exposes only the five configurable execution roles in profile tables', () => {
    assert.deepEqual(PROFILE_EXECUTION_ROLES, [
      'orchestrator',
      'architect',
      'coder',
      'reviewer',
      'closure',
    ]);
    const empty = createEmptyTable();
    assert.deepEqual(Object.keys(empty), [
      'orchestrator',
      'architect',
      'coder',
      'reviewer',
      'closure',
    ]);
    assert.equal('follower' in empty, false);
  });

  it('treats five execution role defaults as complete without requiring a follower or coworker assignment', () => {
    const profile: WorkflowProfile = {
      id: 'profile-five-role',
      name: 'Five Role Complete',
      assignments: makeFiveRoleAssignments(),
    };
    const snapshot = makeSnapshot([profile]);
    const summary = summarizeProfile(snapshot, profile);
    assert.equal(summary.defaultCount, 5);
    assert.equal(summary.assignmentCount, 5);
    assert.equal(summary.overrideCount, 0);
    assert.deepEqual(summary.missingDefaults, []);
    assert.deepEqual(summary.issues, []);

    const draft = {
      mode: 'edit' as const,
      id: profile.id,
      name: profile.name,
      sourceId: profile.id,
      sourceRevision: snapshot.revision,
      sourceSignature: profileSignature(profile),
      cloneOfName: null,
      assignments: tableFromProfile(profile),
      decisionAdvisor: null,
      externalChanged: false,
      externalDeleted: false,
      overwriteAcknowledged: false,
      acknowledgedSignature: null,
    };
    const analysis = analyzeDraft(snapshot, draft);
    assert.equal(analysis.defaultCount, 5);
    assert.equal(analysis.assignmentCount, 5);
    assert.deepEqual(analysis.missingDefaults, []);
    assert.deepEqual(analysis.unassignedRoles, []);
    assert.deepEqual(analysis.errors, []);
  });

  it('decodes legacy profiles with follower assignments cleanly and omits follower when saving drafts', () => {
    const legacyProfile: WorkflowProfile = {
      id: 'profile-legacy-six',
      name: 'Legacy Six Role',
      assignments: [
        ...makeLegacySixRoleAssignments(),
        {
          role: 'coder',
          difficulty: 'hard',
          choice: { model_id: 'gemini-argon', effort: 'high' },
        },
      ],
    };
    const snapshot = makeSnapshot([legacyProfile]);
    const summary = summarizeProfile(snapshot, legacyProfile);
    assert.equal(summary.defaultCount, 5);
    assert.equal(summary.overrideCount, 1);
    assert.equal(summary.assignmentCount, 6);
    assert.deepEqual(summary.missingDefaults, []);
    assert.deepEqual(summary.issues, []);

    const table = tableFromProfile(legacyProfile);
    assert.equal('follower' in table, false);
    assert.equal(table.coder.hard.model_id, 'gemini-argon');
    assert.equal(table.coder.hard.effort, 'high');

    const roundTripAssignments = assignmentsFromTable(table);
    assert.equal(
      roundTripAssignments.some((assignment) => assignment.role === 'follower'),
      false
    );
    assert.equal(roundTripAssignments.length, 6);

    const savedProfile = profileFromDraft({
      mode: 'edit',
      id: legacyProfile.id,
      name: legacyProfile.name,
      sourceId: legacyProfile.id,
      sourceRevision: snapshot.revision,
      sourceSignature: profileSignature(legacyProfile),
      cloneOfName: null,
      assignments: table,
      decisionAdvisor: null,
      externalChanged: false,
      externalDeleted: false,
      overwriteAcknowledged: false,
      acknowledgedSignature: null,
    });
    assert.deepEqual(
      savedProfile.assignments.map((a) => `${a.role}:${a.difficulty ?? 'default'}`),
      [
        'orchestrator:default',
        'architect:default',
        'coder:default',
        'coder:hard',
        'reviewer:default',
        'closure:default',
      ]
    );
  });

  it('hides the legacy follower editor in the DOM while preserving the five execution role editors and effort choices', () => {
    const legacyProfile: WorkflowProfile = {
      id: 'profile-dom-legacy',
      name: 'Legacy DOM Profile',
      assignments: makeLegacySixRoleAssignments('gemini-argon', 'medium'),
    };
    const snapshot = makeSnapshot([legacyProfile]);
    const page = new ProfilesPage(() => {});
    const root = page.render(snapshot) as unknown as FakeElement;

    assert.equal(root.textContent.includes('5/5 role defaults'), true);

    const editBtn = root.querySelector(`[data-focus-key="edit-${legacyProfile.id}"]`);
    assert.ok(editBtn, 'expected edit button on profile card');
    editBtn.click();

    const renderedRoot = page.render(snapshot) as unknown as FakeElement;
    const draftStatus = renderedRoot.querySelector('[data-draft-status]');
    assert.ok(draftStatus, 'expected draft status in rendered editor');
    assert.equal(draftStatus.textContent.includes('5/5 role defaults'), true);
    assert.equal(draftStatus.textContent.includes('Role defaults complete'), true);
    assert.equal(draftStatus.textContent.includes('Follower'), false);

    const roleFieldsets = renderedRoot.querySelectorAll('[data-profile-role]');
    assert.deepEqual(
      roleFieldsets.map((el) => el.getAttribute('data-profile-role')),
      ['orchestrator', 'architect', 'coder', 'reviewer', 'closure']
    );
    assert.equal(
      renderedRoot.querySelector('[data-profile-role="follower"]'),
      null,
      'legacy follower role editor must be hidden'
    );

    const coderDefaultModel = renderedRoot.querySelector(
      '[data-model-key="coder:default"]'
    );
    assert.ok(coderDefaultModel, 'expected coder default model selector');
    assert.equal(coderDefaultModel.value, 'gemini-argon');

    const coderDefaultEffort = renderedRoot.querySelector(
      '[data-effort-key="coder:default"]'
    );
    assert.ok(coderDefaultEffort, 'expected coder default effort selector');
    assert.equal(coderDefaultEffort.value, 'medium');
    const effortValues = coderDefaultEffort
      .querySelectorAll('option')
      .map((opt) => opt.getAttribute('value'));
    assert.deepEqual(effortValues, ['low', 'medium', 'high']);

    const coderHardModel = renderedRoot.querySelector('[data-model-key="coder:hard"]');
    assert.ok(coderHardModel, 'expected coder hard override model selector');
    coderHardModel.value = 'gemini-argon';
    coderHardModel.dispatchEvent('change');

    const coderHardEffort = renderedRoot.querySelector('[data-effort-key="coder:hard"]');
    assert.ok(coderHardEffort, 'expected synced coder hard override effort selector');
    coderHardEffort.value = 'high';
    coderHardEffort.dispatchEvent('change');

    assert.equal(draftStatus.textContent.includes('5/5 role defaults'), true);
    assert.equal(draftStatus.textContent.includes('1 difficulty overrides'), true);
    assert.equal(draftStatus.textContent.includes('6 assignments'), true);

    page.dispose();
  });
});
