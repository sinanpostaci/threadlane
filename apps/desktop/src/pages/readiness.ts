import { h } from '../ui/dom.ts';
import type { DesktopSnapshot, NavPage } from '../types.ts';
import {
  WORKFLOW_ROLES,
  isApiToolAdapter,
  type Difficulty,
  type ModelDefinition,
  type RegistrySnapshot,
  type RoleAssignment,
  type ToolAdapter,
  type ToolDefinition,
  type ToolVersionInspection,
  type WorkflowProfile,
  type WorkflowRole,
} from '../registry/types.ts';
import type { ToolVersionScanState } from '../registry/tool-versions.ts';



const AGENT_TOOL_ADAPTERS: ReadonlySet<ToolAdapter> = new Set([
  'codex_cli',
  'opencode_v2',
  'antigravity',
]);

const REQUIRED_ROLE_COUNT = WORKFLOW_ROLES.length;

const ROLE_LABELS: Record<WorkflowRole, string> = {
  follower: 'Follower',
  orchestrator: 'Orchestrator',
  architect: 'Architect',
  coder: 'Coder',
  reviewer: 'Reviewer',
  closure: 'Closure',
};

export type ReadinessCheckId =
  | 'agent-tools'
  | 'executables'
  | 'models'
  | 'profiles'
  | 'attachment';

export type ReadinessCheckState = 'ready' | 'attention' | 'blocked' | 'loading' | 'unknown';

export type ReadinessProfileState = 'complete' | 'partial' | 'invalid';

export interface ReadinessAction {
  page: NavPage;
  label: string;
}

export interface ReadinessCheck {
  id: ReadinessCheckId;
  label: string;
  state: ReadinessCheckState;
  summary: string;
  detail: string;
  action: ReadinessAction;
}

export interface ReadinessProfileDiagnostic {
  name: string;
  isDefault: boolean;
  state: ReadinessProfileState;
  defaultCount: number;
  requiredDefaultCount: number;
  missingDefaults: string[];
  issueCount: number;
}

export interface ReadinessProjection {
  checks: ReadinessCheck[];
  profiles: ReadinessProfileDiagnostic[];
  complete: boolean;
  readyCount: number;
  totalCount: number;
  defaultProfileName: string | null;
  headline: string;
  summary: string;
  limitations: string[];
}

export interface ReadinessChecklistInput {
  registry: RegistrySnapshot;
  toolVersions: ToolVersionScanState;
  snapshot: DesktopSnapshot;
  registryLoading: boolean;
  registryError: string | null;
}

export interface ReadinessChecklistOptions extends ReadinessChecklistInput {
  onNavigate: (page: NavPage) => void;
}

interface ModelChoiceIssue {
  profileName: string;
  role: string;
  difficulty: Difficulty | null;
  message: string;
}

interface ProfileEvaluation {
  profile: WorkflowProfile;
  name: string;
  isDefault: boolean;
  missingDefaults: WorkflowRole[];
  issues: ModelChoiceIssue[];
  state: ReadinessProfileState;
}

const CHECK_LABELS: Record<ReadinessCheckId, string> = {
  'agent-tools': 'Agent tools',
  executables: 'Installed executables',
  models: 'Models and efforts',
  profiles: 'Profiles',
  attachment: 'Runtime attachment',
};

const CHECK_ACTIONS: Record<ReadinessCheckId, ReadinessAction> = {
  'agent-tools': { page: 'tools', label: 'Open Tools' },
  executables: { page: 'tools', label: 'Open Tools' },
  models: { page: 'tools', label: 'Choose models' },
  profiles: { page: 'profiles', label: 'Review Profiles' },
  attachment: { page: 'chats', label: 'Review Chats' },
};

const LIMITATIONS: readonly string[] = [
  'Work continues in Codex. This checklist is read-only and starts no worker, host, or form submission.',
  'A concrete approved execution preview is still required before dispatch; local setup is not execution approval.',
  'Native notification delivery is not verified by registry setup or by a connected attachment.',
  'Decision API keys, entitlements, and paid model access are optional and are not verified here.',
];

function safeText(value: unknown, fallback: string, maxLength: number = 120): string {
  const raw = typeof value === 'string' ? value : '';
  const clean = raw
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!clean) {
    return fallback;
  }
  const characters = Array.from(clean);
  if (characters.length <= maxLength) {
    return clean;
  }
  return `${characters.slice(0, Math.max(1, maxLength - 1)).join('')}…`;
}

function safeList(values: readonly string[], maxVisible: number = 3): string {
  const visible = values.slice(0, maxVisible);
  const hiddenCount = values.length - visible.length;
  const suffix = hiddenCount > 0 ? ` and ${hiddenCount} more` : '';
  return `${visible.join(', ')}${suffix}`;
}

function isSafeRevision(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function isWorkflowRole(value: string): value is WorkflowRole {
  return (WORKFLOW_ROLES as readonly string[]).includes(value);
}

function roleLabel(value: string): string {
  return isWorkflowRole(value) ? ROLE_LABELS[value] : 'Unknown role';
}

function slotLabel(difficulty: Difficulty | null | undefined): string {
  if (difficulty === null || difficulty === undefined) {
    return 'role default';
  }
  const label = safeText(difficulty, 'difficulty', 24);
  return `${label.slice(0, 1).toUpperCase()}${label.slice(1)} override`;
}

function toolLabel(tool: ToolDefinition): string {
  return safeText(tool.label, 'Unnamed tool', 48);
}

function modelLabel(model: ModelDefinition): string {
  return safeText(model.label, 'Assigned model', 48);
}

function enabledAgentTools(registry: RegistrySnapshot): ToolDefinition[] {
  return registry.tools.filter(
    (tool) => tool.enabled && AGENT_TOOL_ADAPTERS.has(tool.adapter)
  );
}

function makeCheck(
  id: ReadinessCheckId,
  state: ReadinessCheckState,
  summary: string,
  detail: string
): ReadinessCheck {
  return {
    id,
    label: CHECK_LABELS[id],
    state,
    summary,
    detail,
    action: CHECK_ACTIONS[id],
  };
}

function validateAssignment(
  registry: RegistrySnapshot,
  assignment: RoleAssignment
): string | null {
  if (!isWorkflowRole(assignment.role)) {
    return 'has an unknown workflow role.';
  }

  const choice = assignment.choice;
  if (!choice || typeof choice.model_id !== 'string' || choice.model_id.trim() === '') {
    return 'has no model selected.';
  }

  const model = registry.models.find((candidate) => candidate.id === choice.model_id);
  if (!model) {
    return 'uses a model that is not in the registry.';
  }

  const tool = registry.tools.find((candidate) => candidate.id === model.tool_id);
  if (!tool) {
    return 'uses a model whose tool is missing.';
  }
  if (!tool.enabled) {
    return `uses ${toolLabel(tool)}, which is disabled.`;
  }
  if (isApiToolAdapter(tool.adapter)) {
    return `uses ${toolLabel(tool)}, a decision API that cannot fill a workflow role.`;
  }
  if (!AGENT_TOOL_ADAPTERS.has(tool.adapter)) {
    return `uses ${toolLabel(tool)}, which is not an agent worker tool.`;
  }

  const efforts = Array.isArray(model.efforts) ? model.efforts : [];
  if (choice.effort === null) {
    if (!model.allow_no_effort) {
      return `requires an explicit effort for ${modelLabel(model)}.`;
    }
  } else if (typeof choice.effort !== 'string' || !efforts.includes(choice.effort)) {
    return `uses an effort that ${modelLabel(model)} does not list.`;
  }

  return null;
}

function evaluateProfiles(registry: RegistrySnapshot): ProfileEvaluation[] {
  return registry.profiles.map((profile) => {
    const profileId = typeof profile.id === 'string' ? profile.id : '';
    const name = safeText(profile.name, 'Unnamed profile', 64);
    const assignments = Array.isArray(profile.assignments) ? profile.assignments : [];
    const missingDefaults = WORKFLOW_ROLES.filter(
      (role) =>
        !assignments.some(
          (assignment) => assignment.role === role && assignment.difficulty === null
        )
    );
    const issues: ModelChoiceIssue[] = [];

    for (const assignment of assignments) {
      const message = validateAssignment(registry, assignment);
      if (message) {
        issues.push({
          profileName: name,
          role: assignment.role,
          difficulty: assignment.difficulty,
          message,
        });
      }
    }

    const state: ReadinessProfileState =
      issues.length > 0 ? 'invalid' : missingDefaults.length > 0 ? 'partial' : 'complete';

    return {
      profile,
      name,
      isDefault:
        registry.default_profile_id !== null && registry.default_profile_id === profileId,
      missingDefaults,
      issues,
      state,
    };
  });
}

function describeChoiceIssue(issue: ModelChoiceIssue): string {
  return safeText(
    `${issue.profileName} · ${roleLabel(issue.role)} ${slotLabel(issue.difficulty)}: ${issue.message}`,
    'A saved role choice needs repair.',
    180
  );
}

function inspectionProblem(
  tool: ToolDefinition,
  inspection: ToolVersionInspection | undefined
): string | null {
  const label = toolLabel(tool);
  if (!inspection) {
    return `${label}: no result for this revision`;
  }
  switch (inspection.availability) {
    case 'found':
      return null;
    case 'not_found':
      return `${label}: executable not found`;
    case 'disabled':
      return `${label}: inspection still reports disabled`;
    case 'api_only':
      return `${label}: API-only result cannot verify a local CLI`;
    default:
      return `${label}: inspection result is unknown`;
  }
}

function projectAgentTools(input: ReadinessChecklistInput): ReadinessCheck {
  if (input.registryLoading) {
    return makeCheck(
      'agent-tools',
      'loading',
      'Loading saved tools',
      'Waiting for the local registry snapshot before checking agent tools.'
    );
  }
  if (input.registryError) {
    return makeCheck(
      'agent-tools',
      'blocked',
      'Registry unavailable',
      'The saved tool registry could not be loaded, so agent tool setup cannot be checked.'
    );
  }

  const tools = enabledAgentTools(input.registry);
  if (tools.length === 0) {
    return makeCheck(
      'agent-tools',
      'blocked',
      'No agent tool enabled',
      'Enable an agent CLI in Tools. Git, tmux, CodeGraph, and decision APIs do not replace an agent worker.'
    );
  }

  return makeCheck(
    'agent-tools',
    'ready',
    `${tools.length} agent tool${tools.length === 1 ? '' : 's'} enabled`,
    `${safeList(tools.map(toolLabel))}. Executable and model checks below verify the rest; decision API tools and their keys stay optional.`
  );
}

function projectExecutables(input: ReadinessChecklistInput): ReadinessCheck {
  if (input.registryLoading) {
    return makeCheck(
      'executables',
      'loading',
      'Loading executable checks',
      'Waiting for the local registry snapshot before matching version inspections.'
    );
  }
  if (input.registryError) {
    return makeCheck(
      'executables',
      'blocked',
      'Registry unavailable',
      'Executable inspections cannot be matched until the saved registry loads.'
    );
  }

  const tools = enabledAgentTools(input.registry);
  if (tools.length === 0) {
    return makeCheck(
      'executables',
      'blocked',
      'No agent executable to inspect',
      'Enable an agent CLI in Tools, then run the version-only check.'
    );
  }

  const scan = input.toolVersions;
  if (scan.status === 'loading') {
    return makeCheck(
      'executables',
      'loading',
      'Checking local executables',
      'A version-only check is still running. An earlier success is not reused while this check is pending.'
    );
  }
  if (scan.status === 'idle') {
    return makeCheck(
      'executables',
      'unknown',
      'Executable check has not run',
      'Open Tools and run the installed version check for the enabled agent CLIs.'
    );
  }
  if (scan.status === 'stale') {
    return makeCheck(
      'executables',
      'blocked',
      'Executable check is stale',
      'The registry or executable selection changed. Recheck versions in Tools; an older success is not reused.'
    );
  }
  if (scan.status === 'error') {
    return makeCheck(
      'executables',
      'blocked',
      'Executable check failed',
      `${safeText(
        scan.error,
        'Version checks could not complete.',
        180
      )} Recheck versions in Tools.`
    );
  }
  if (scan.status !== 'ready') {
    return makeCheck(
      'executables',
      'blocked',
      'Executable check unavailable',
      'Recheck versions in Tools before relying on local executables.'
    );
  }

  const scannedRevision = scan.inspectedRegistryRevision;
  if (
    !isSafeRevision(input.registry.revision) ||
    scannedRevision === null ||
    !isSafeRevision(scannedRevision) ||
    scannedRevision !== input.registry.revision
  ) {
    return makeCheck(
      'executables',
      'blocked',
      'Executable check is stale',
      'The last successful scan does not match the current registry revision. Recheck versions in Tools.'
    );
  }

  const problems: string[] = [];
  for (const tool of tools) {
    const inspection = scan.inspections.find((entry) => entry.tool_id === tool.id);
    const problem = inspectionProblem(tool, inspection);
    if (problem) {
      problems.push(problem);
    }
  }

  if (problems.length > 0) {
    return makeCheck(
      'executables',
      'blocked',
      `${problems.length} executable check${problems.length === 1 ? '' : 's'} need attention`,
      `${safeList(problems)}. Recheck versions in Tools.`
    );
  }

  return makeCheck(
    'executables',
    'ready',
    'Executables found',
    'Version-only checks for this registry revision found each enabled agent CLI. Login, catalog access, approval, and paid access are not verified.'
  );
}

function projectModels(
  input: ReadinessChecklistInput,
  evaluations: readonly ProfileEvaluation[]
): ReadinessCheck {
  if (input.registryLoading) {
    return makeCheck(
      'models',
      'loading',
      'Loading saved role choices',
      'Waiting for the local registry snapshot before checking models and efforts.'
    );
  }
  if (input.registryError) {
    return makeCheck(
      'models',
      'blocked',
      'Registry unavailable',
      'Saved role choices cannot be checked until the registry loads.'
    );
  }

  const assignmentCount = evaluations.reduce(
    (total, evaluation) =>
      total + (Array.isArray(evaluation.profile.assignments) ? evaluation.profile.assignments.length : 0),
    0
  );
  if (assignmentCount === 0) {
    return makeCheck(
      'models',
      'blocked',
      'No saved role choices',
      'A profile name or profile presence alone is not valid setup. Save role model choices in Profiles.'
    );
  }

  const issues = evaluations.flatMap((evaluation) => evaluation.issues);
  if (issues.length > 0) {
    return makeCheck(
      'models',
      'blocked',
      `${issues.length} model or effort choice${issues.length === 1 ? '' : 's'} need repair`,
      `${safeList(issues.map(describeChoiceIssue), 2)}. Open Profiles to repair the saved choices.`
    );
  }

  return makeCheck(
    'models',
    'ready',
    `${assignmentCount} role choice${assignmentCount === 1 ? '' : 's'} resolve`,
    'Every saved role assignment points to a model on an enabled agent tool and uses a permitted effort.'
  );
}

function projectProfiles(
  input: ReadinessChecklistInput,
  evaluations: readonly ProfileEvaluation[]
): ReadinessCheck {
  if (input.registryLoading) {
    return makeCheck(
      'profiles',
      'loading',
      'Loading saved profiles',
      'Waiting for the local registry snapshot before checking role defaults.'
    );
  }
  if (input.registryError) {
    return makeCheck(
      'profiles',
      'blocked',
      'Registry unavailable',
      'Saved profiles cannot be checked until the registry loads.'
    );
  }

  if (evaluations.length === 0) {
    return makeCheck(
      'profiles',
      'blocked',
      'No saved profile',
      'A profile holds the role defaults used by a full workflow. Create one in Profiles.'
    );
  }

  const defaultId = input.registry.default_profile_id;
  if (!defaultId) {
    return makeCheck(
      'profiles',
      'blocked',
      'No default profile selected',
      'Saved profiles are not silently chosen. Select one explicitly as the default in Profiles.'
    );
  }

  const defaultEvaluation = evaluations.find((evaluation) => evaluation.isDefault);
  if (!defaultEvaluation) {
    return makeCheck(
      'profiles',
      'blocked',
      'Default profile unavailable',
      'The saved default does not match a current profile. Choose a default explicitly in Profiles.'
    );
  }

  const otherInvalid = evaluations.filter(
    (evaluation) => !evaluation.isDefault && evaluation.state === 'invalid'
  );
  const otherPartial = evaluations.filter(
    (evaluation) => !evaluation.isDefault && evaluation.state === 'partial'
  );

  if (
    defaultEvaluation.missingDefaults.length > 0 ||
    defaultEvaluation.issues.length > 0
  ) {
    const missingText =
      defaultEvaluation.missingDefaults.length > 0
        ? `${defaultEvaluation.name} is missing role defaults: ${defaultEvaluation.missingDefaults
            .map((role) => ROLE_LABELS[role])
            .join(', ')}.`
        : '';
    const issueText =
      defaultEvaluation.issues.length > 0
        ? `${defaultEvaluation.name} has ${defaultEvaluation.issues.length} invalid role choice${
            defaultEvaluation.issues.length === 1 ? '' : 's'
          }.`
        : '';
    return makeCheck(
      'profiles',
      defaultEvaluation.issues.length > 0 ? 'blocked' : 'attention',
      defaultEvaluation.issues.length > 0
        ? 'Default profile needs repair'
        : 'Default profile is partial',
      `${[missingText, issueText].filter(Boolean).join(' ')} Difficulty overrides do not replace a role default.`
    );
  }

  if (otherInvalid.length > 0) {
    const descriptions = otherInvalid.map(
      (evaluation) =>
        `${evaluation.name} (${evaluation.issues.length} issue${
          evaluation.issues.length === 1 ? '' : 's'
        })`
    );
    return makeCheck(
      'profiles',
      'blocked',
      `${otherInvalid.length} saved profile${otherInvalid.length === 1 ? '' : 's'} need repair`,
      `Default ${defaultEvaluation.name} is complete. ${safeList(
        descriptions,
        2
      )} still need valid role choices.`
    );
  }

  if (otherPartial.length > 0) {
    const descriptions = otherPartial.map(
      (evaluation) =>
        `${evaluation.name} (missing ${evaluation.missingDefaults
          .map((role) => ROLE_LABELS[role])
          .join(', ')})`
    );
    return makeCheck(
      'profiles',
      'attention',
      `${otherPartial.length} saved profile${otherPartial.length === 1 ? '' : 's'} partial`,
      `Default ${defaultEvaluation.name} is complete. ${safeList(
        descriptions,
        2
      )} still need all six role defaults.`
    );
  }

  return makeCheck(
    'profiles',
    'ready',
    'Profiles complete',
    `Default: ${defaultEvaluation.name}. Every saved profile has all six role defaults; difficulty overrides do not replace them.`
  );
}

function projectAttachment(snapshot: DesktopSnapshot): ReadinessCheck {
  const attachment = snapshot.attachment;

  if (attachment.mode === 'demo') {
    return makeCheck(
      'attachment',
      'blocked',
      'Local demo attached',
      'Demo activity does not verify the runtime. Attach a runtime in Settings, then select a non-demo conversation in Chats.'
    );
  }
  if (attachment.mode !== 'runtime') {
    return makeCheck(
      'attachment',
      'blocked',
      'Runtime not attached',
      'No runtime attachment is configured. Attach one in Settings; this checklist will not start a host.'
    );
  }
  if (attachment.connection !== 'connected') {
    const connectionDetail =
      attachment.connection === 'blocked'
        ? 'The runtime connection is blocked. Review the attachment in Settings.'
        : attachment.connection === 'disconnected'
          ? 'The runtime connection is disconnected. Reattach in Settings.'
          : 'The runtime connection is not configured. Attach a runtime in Settings, then select a conversation in Chats.';
    return makeCheck(
      'attachment',
      'blocked',
      `Runtime ${attachment.connection}`,
      connectionDetail
    );
  }

  const selectedId = snapshot.selected_binding_id;
  if (!selectedId) {
    return makeCheck(
      'attachment',
      'blocked',
      'No runtime binding selected',
      'The runtime is connected, but no conversation is selected. Choose a non-demo binding in Chats.'
    );
  }

  const binding = snapshot.bindings.find((candidate) => candidate.binding_id === selectedId);
  if (!binding) {
    return makeCheck(
      'attachment',
      'blocked',
      'Selected binding unavailable',
      'The selected conversation is not in the current runtime list. Choose one in Chats.'
    );
  }
  if (binding.is_local_demo) {
    return makeCheck(
      'attachment',
      'blocked',
      'Demo binding selected',
      'The selected conversation is a local demo. Choose a non-demo runtime binding in Chats.'
    );
  }

  return makeCheck(
    'attachment',
    'ready',
    'Runtime connected',
    `Selected conversation: ${safeText(
      binding.label,
      'Unnamed binding',
      64
    )}. Attachment is observed only; no host or worker is started by this checklist.`
  );
}

export function projectReadinessChecklist(
  input: ReadinessChecklistInput
): ReadinessProjection {
  const evaluations = evaluateProfiles(input.registry);
  const attachmentCheck = projectAttachment(input.snapshot);
  if (input.snapshot.attachment.mode !== 'runtime' || input.snapshot.attachment.connection !== 'connected') {
    attachmentCheck.action = { page: 'settings', label: 'Attach runtime' };
  }
  const checks: ReadinessCheck[] = [
    projectAgentTools(input),
    projectExecutables(input),
    projectModels(input, evaluations),
    projectProfiles(input, evaluations),
    attachmentCheck,
  ];
  const readyCount = checks.filter((check) => check.state === 'ready').length;
  const complete = readyCount === checks.length;
  const profiles = evaluations.map<ReadinessProfileDiagnostic>((evaluation) => ({
    name: evaluation.name,
    isDefault: evaluation.isDefault,
    state: evaluation.state,
    defaultCount: REQUIRED_ROLE_COUNT - evaluation.missingDefaults.length,
    requiredDefaultCount: REQUIRED_ROLE_COUNT,
    missingDefaults: evaluation.missingDefaults.map((role) => ROLE_LABELS[role]),
    issueCount: evaluation.issues.length,
  }));
  const defaultEvaluation = evaluations.find((evaluation) => evaluation.isDefault) ?? null;

  return {
    checks,
    profiles,
    complete,
    readyCount,
    totalCount: checks.length,
    defaultProfileName: defaultEvaluation?.name ?? null,
    headline: complete ? 'Local setup complete' : 'Finish local setup',
    summary: complete
      ? 'Saved tools, role models, profiles, and the selected runtime conversation agree. Work continues in Codex; an approved execution preview is still required before any run.'
      : 'This read-only view compares saved registry choices with the current executable scan and runtime attachment. Resolve open items, then continue work in Codex.',
    limitations: [...LIMITATIONS],
  };
}

function checkStateLabel(state: ReadinessCheckState): string {
  switch (state) {
    case 'ready':
      return 'Ready';
    case 'attention':
      return 'Partial';
    case 'blocked':
      return 'Needs setup';
    case 'loading':
      return 'Checking';
    case 'unknown':
      return 'Not checked';
  }
}

function profileStateLabel(state: ReadinessProfileState): string {
  if (state === 'complete') {
    return 'Complete';
  }
  return state === 'partial' ? 'Partial' : 'Needs repair';
}

function profileMeta(profile: ReadinessProfileDiagnostic): string {
  if (profile.state === 'invalid') {
    return `${profile.issueCount} choice issue${
      profile.issueCount === 1 ? '' : 's'
    } · ${profile.defaultCount}/${profile.requiredDefaultCount} role defaults`;
  }
  if (profile.state === 'partial') {
    return `${profile.defaultCount}/${profile.requiredDefaultCount} role defaults · missing ${profile.missingDefaults.join(
      ', '
    )}`;
  }
  return `${profile.defaultCount}/${profile.requiredDefaultCount} role defaults`;
}

function overallState(checks: readonly ReadinessCheck[]): ReadinessCheckState {
  if (checks.every((check) => check.state === 'ready')) {
    return 'ready';
  }
  if (checks.some((check) => check.state === 'blocked')) {
    return 'blocked';
  }
  if (checks.some((check) => check.state === 'loading')) {
    return 'loading';
  }
  if (checks.some((check) => check.state === 'attention')) {
    return 'attention';
  }
  return 'unknown';
}

function actionButton(
  action: ReadinessAction,
  context: string,
  onNavigate: (page: NavPage) => void
): HTMLButtonElement {
  const button = h(
    'button',
    {
      type: 'button',
      className: 'aq-readiness-action',
      'data-readiness-page': action.page,
      'aria-label': `${action.label} for ${context}`,
    },
    action.label
  );
  button.addEventListener('click', (event) => {
    event.preventDefault();
    onNavigate(action.page);
  });
  return button;
}

function buildCheckItem(
  check: ReadinessCheck,
  onNavigate: (page: NavPage) => void
): HTMLElement {
  return h(
    'li',
    { className: 'aq-readiness-item', 'data-state': check.state },
    h('span', { className: 'aq-readiness-dot', 'aria-hidden': 'true' }),
    h(
      'div',
      { className: 'aq-readiness-item-main' },
      h(
        'div',
        { className: 'aq-readiness-item-head' },
        h('span', { className: 'aq-readiness-item-label' }, check.label),
        h(
          'span',
          { className: 'aq-readiness-state', 'data-state': check.state },
          checkStateLabel(check.state)
        )
      ),
      h('p', { className: 'aq-readiness-item-summary' }, check.summary),
      h('p', { className: 'aq-readiness-item-detail' }, check.detail),
      actionButton(check.action, check.label, onNavigate)
    )
  );
}

function buildProfileDiagnostics(
  profiles: readonly ReadinessProfileDiagnostic[]
): HTMLElement | null {
  if (profiles.length === 0) {
    return null;
  }

  return h(
    'div',
    { className: 'aq-readiness-diagnostics' },
    h('p', { className: 'aq-readiness-diagnostics-title' }, 'Saved profiles'),
    h(
      'ul',
      { className: 'aq-readiness-profiles' },
      ...profiles.map((profile) =>
        h(
          'li',
          {
            className: 'aq-readiness-profile',
            'data-state': profile.state,
          },
          h(
            'span',
            { className: 'aq-readiness-profile-name' },
            profile.name,
            profile.isDefault
              ? h('span', { className: 'aq-readiness-profile-default' }, 'Default')
              : null
          ),
          h(
            'span',
            { className: 'aq-readiness-profile-meta' },
            `${profileStateLabel(profile.state)} · ${profileMeta(profile)}`
          )
        )
      )
    )
  );
}

export function renderReadinessChecklist(
  options: ReadinessChecklistOptions
): HTMLElement {
  const projection = projectReadinessChecklist({
    registry: options.registry,
    toolVersions: options.toolVersions,
    snapshot: options.snapshot,
    registryLoading: options.registryLoading,
    registryError: options.registryError,
  });
  const state = overallState(projection.checks);
  const root = h('section', {
    className: 'aq-readiness',
    'aria-label': 'Getting started local readiness',
  });
  const body = h('div', { className: 'aq-readiness-body' });

  body.appendChild(h('p', { className: 'aq-readiness-lede' }, projection.summary));

  if (options.registryError) {
    body.appendChild(
      h(
        'div',
        { className: 'aq-readiness-alert', role: 'alert' },
        h('strong', {}, 'Registry error'),
        h(
          'p',
          {},
          safeText(
            options.registryError,
            'The saved registry could not be loaded.',
            180
          )
        )
      )
    );
  }

  body.appendChild(
    h(
      'ol',
      { className: 'aq-readiness-list' },
      ...projection.checks.map((check) => buildCheckItem(check, options.onNavigate))
    )
  );

  const diagnostics = buildProfileDiagnostics(projection.profiles);
  if (diagnostics) {
    body.appendChild(diagnostics);
  }

  body.appendChild(
    h(
      'div',
      { className: 'aq-readiness-boundaries' },
      h('strong', {}, 'What this does not verify'),
      h(
        'ul',
        {},
        ...projection.limitations.map((limitation) => h('li', {}, limitation))
      ),
      h(
        'div',
        { className: 'aq-readiness-boundary-actions' },
        actionButton(
          { page: 'settings', label: 'Open Settings' },
          'execution and notification settings',
          options.onNavigate
        ),
        h('span', { className: 'aq-readiness-boundary-note' }, 'Read-only · no host or worker starts')
      )
    )
  );

  const details = h(
    'details',
    {
      className: 'aq-readiness-panel',
      'data-complete': String(projection.complete),
      'data-state': state,
      open: !projection.complete,
    },
    h(
      'summary',
      { className: 'aq-readiness-summary' },
      h('span', {
        className: 'aq-readiness-mark',
        'data-state': state,
        'aria-hidden': 'true',
      }),
      h(
        'span',
        { className: 'aq-readiness-summary-copy' },
        h('span', { className: 'aq-readiness-kicker' }, 'Getting started'),
        h('span', { className: 'aq-readiness-title' }, projection.headline),
        h(
          'span',
          { className: 'aq-readiness-caption' },
          projection.complete
            ? 'Local checks pass. Approval and native delivery remain separate.'
            : `${projection.readyCount} of ${projection.totalCount} local checks ready.`
        )
      ),
      h(
        'span',
        { className: 'aq-readiness-progress', 'aria-hidden': 'true' },
        `${projection.readyCount}/${projection.totalCount}`
      )
    ),
    body
  );

  root.appendChild(details);
  return root;
}
