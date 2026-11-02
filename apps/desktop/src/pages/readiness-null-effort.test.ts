import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  projectReadinessChecklist,
  type ReadinessCheck,
  type ReadinessChecklistInput,
  type ReadinessProjection,
} from './readiness.ts';
import {
  EXECUTION_ROLES,
  WORKFLOW_ROLES,
  type ModelDefinition,
  type RegistrySnapshot,
  type RoleAssignment,
  type ToolDefinition,
  type ToolVersionInspection,
  type WorkflowProfile,
} from '../registry/types.ts';
import type { ToolVersionScanState } from '../registry/tool-versions.ts';
import type {
  Attachment,
  BindingSummary,
  DesktopSnapshot,
} from '../types.ts';

const NOW = 1730000000000;

function makeTool(overrides: Partial<ToolDefinition> = {}): ToolDefinition {
  return {
    id: 'opencode',
    label: 'OpenCode v2',
    adapter: 'opencode_v2',
    executable_path: null,
    enabled: true,
    ...overrides,
  };
}

function makeModel(overrides: Partial<ModelDefinition> = {}): ModelDefinition {
  return {
    id: 'model-null-effort',
    tool_id: 'opencode',
    label: 'Null effort model',
    model_arg: 'opencode-go/deepseek-v4.1-flash',
    efforts: [],
    allow_no_effort: true,
    ...overrides,
  };
}

function fullAssignments(
  modelId: string = 'model-null-effort',
  effort: string | null = null
): RoleAssignment[] {
  return EXECUTION_ROLES.map((role) => ({
    role,
    difficulty: null,
    choice: { model_id: modelId, effort },
  }));
}

function legacySixRoleAssignments(
  modelId: string = 'model-null-effort',
  effort: string | null = null
): RoleAssignment[] {
  return WORKFLOW_ROLES.map((role) => ({
    role,
    difficulty: null,
    choice: { model_id: modelId, effort },
  }));
}

function makeProfile(
  id: string = 'profile-main',
  name: string = 'Everyday',
  assignments: RoleAssignment[] = fullAssignments()
): WorkflowProfile {
  return { id, name, assignments };
}

function makeBinding(overrides: Partial<BindingSummary> = {}): BindingSummary {
  return {
    binding_id: 'bind-main',
    thread_uuid: '00000000-0000-0000-0000-000000000001',
    label: 'Main conversation',
    is_local_demo: false,
    ...overrides,
  };
}

function makeAttachment(overrides: Partial<Attachment> = {}): Attachment {
  return {
    mode: 'runtime',
    connection: 'connected',
    state_dir: '/tmp/threadlane-readiness-null-effort-test',
    message: null,
    updated_at_ms: NOW,
    ...overrides,
  };
}

function makeSnapshot(overrides: Partial<DesktopSnapshot> = {}): DesktopSnapshot {
  return {
    attachment: makeAttachment(),
    bindings: [makeBinding()],
    selected_binding_id: 'bind-main',
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
    ...overrides,
  };
}

function makeInspection(
  toolId: string,
  overrides: Partial<ToolVersionInspection> = {}
): ToolVersionInspection {
  return {
    tool_id: toolId,
    availability: 'found',
    selection_mode: 'system',
    configured_path: null,
    resolved_path: `/usr/bin/${toolId}`,
    system_path: `/usr/bin/${toolId}`,
    version: `${toolId} 1.0.0`,
    system_version: `${toolId} 1.0.0`,
    system_path_differs: false,
    message: 'Version-only check completed.',
    error_code: null,
    ...overrides,
  };
}

function makeScan(
  registryRevision: number,
  inspections: ToolVersionInspection[]
): ToolVersionScanState {
  return {
    status: 'ready',
    inspections,
    inspectedRegistryRevision: registryRevision,
    error: null,
    startupStarted: true,
    nextTicketId: 1,
    activeTicketId: null,
  };
}

function makeRegistry(overrides: Partial<RegistrySnapshot> = {}): RegistrySnapshot {
  return {
    schema_version: 1,
    revision: 7,
    tools: [makeTool()],
    models: [makeModel()],
    profiles: [makeProfile()],
    default_profile_id: 'profile-main',
    ...overrides,
  };
}

function makeInput(
  overrides: Partial<ReadinessChecklistInput> = {}
): ReadinessChecklistInput {
  const registry = overrides.registry ?? makeRegistry();
  const agentTools = registry.tools.filter(
    (tool) =>
      tool.enabled &&
      tool.adapter !== 'jev_api' &&
      tool.adapter !== 'openai_decisions_api'
  );
  return {
    registry,
    toolVersions:
      overrides.toolVersions ??
      makeScan(
        registry.revision,
        agentTools.map((tool) => makeInspection(tool.id))
      ),
    snapshot: overrides.snapshot ?? makeSnapshot(),
    registryLoading: overrides.registryLoading ?? false,
    registryError: overrides.registryError ?? null,
  };
}

function checkById(
  projection: ReadinessProjection,
  id: ReadinessCheck['id']
): ReadinessCheck {
  const check = projection.checks.find((candidate) => candidate.id === id);
  assert.ok(check, `missing readiness check: ${id}`);
  return check;
}

describe('readiness null effort choices', () => {
  it('accepts a complete role default profile with null effort when the model allows it', () => {
    const registry = makeRegistry();
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');

    assert.deepEqual(registry.models[0].efforts, []);
    assert.equal(registry.models[0].allow_no_effort, true);
    assert.equal(models.state, 'ready');
    assert.match(models.summary, /5 role choices resolve/);
    assert.match(models.detail, /permitted effort/);
    assert.equal(checkById(projection, 'profiles').state, 'ready');
    assert.equal(projection.profiles[0].state, 'complete');
    assert.equal(projection.profiles[0].defaultCount, EXECUTION_ROLES.length);
    assert.equal(projection.profiles[0].requiredDefaultCount, EXECUTION_ROLES.length);
    assert.deepEqual(projection.profiles[0].missingDefaults, []);
    assert.equal(projection.complete, true);
  });

  it('blocks null effort when the model requires an explicit effort', () => {
    const registry = makeRegistry({
      models: [makeModel({ allow_no_effort: false })],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');
    const profiles = checkById(projection, 'profiles');

    assert.equal(models.state, 'blocked');
    assert.match(models.summary, /5 model or effort choices need repair/);
    assert.match(models.detail, /requires an explicit effort/);
    assert.match(models.detail, /Null effort model/);
    assert.doesNotMatch(models.detail, /does not list/);
    assert.equal(profiles.state, 'blocked');
    assert.equal(projection.profiles[0].state, 'invalid');
    assert.equal(projection.profiles[0].issueCount, EXECUTION_ROLES.length);
    assert.equal(projection.readyCount, 3);
    assert.equal(projection.complete, false);
  });

  it('keeps decision API tools optional without changing null-effort role choices', () => {
    const apiTool = makeTool({
      id: 'jev',
      label: 'Jev decisions',
      adapter: 'jev_api',
      enabled: true,
    });
    const registry = makeRegistry({
      tools: [makeTool(), apiTool],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const agentTools = checkById(projection, 'agent-tools');
    const models = checkById(projection, 'models');

    assert.equal(agentTools.state, 'ready');
    assert.match(agentTools.summary, /1 agent tool enabled/);
    assert.match(agentTools.detail, /decision API tools and their keys stay optional/);
    assert.equal(models.state, 'ready');
    assert.match(models.summary, /5 role choices resolve/);
    assert.equal(projection.profiles[0].state, 'complete');
    assert.equal(projection.complete, true);
  });

  it('decodes legacy six-role profiles with follower and null effort without gating five-role readiness or mutating saved data', () => {
    const cleanLegacyRegistry = makeRegistry({
      profiles: [
        makeProfile('profile-legacy-clean', 'Legacy Six Clean Null Effort', legacySixRoleAssignments()),
      ],
      default_profile_id: 'profile-legacy-clean',
    });
    const cleanBefore = JSON.stringify(cleanLegacyRegistry);
    const cleanProjection = projectReadinessChecklist(makeInput({ registry: cleanLegacyRegistry }));
    assert.equal(checkById(cleanProjection, 'models').state, 'ready');
    assert.match(checkById(cleanProjection, 'models').summary, /5 role choices resolve/);
    assert.equal(cleanProjection.complete, true);
    assert.equal(JSON.stringify(cleanLegacyRegistry), cleanBefore);

    const registry = makeRegistry({
      models: [
        makeModel(),
        makeModel({
          id: 'model-explicit-effort',
          label: 'Explicit effort model',
          efforts: ['high'],
          allow_no_effort: false,
        }),
      ],
      profiles: [
        makeProfile('profile-legacy-six', 'Legacy Six Null Effort', [
          {
            role: 'follower',
            difficulty: null,
            choice: { model_id: 'model-explicit-effort', effort: null },
          },
          ...fullAssignments('model-null-effort', null),
        ]),
      ],
      default_profile_id: 'profile-legacy-six',
    });
    const before = JSON.stringify(registry);
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');

    assert.equal(models.state, 'ready');
    assert.match(models.summary, /5 role choices resolve/);
    assert.equal(checkById(projection, 'profiles').state, 'ready');
    assert.equal(projection.profiles[0].state, 'complete');
    assert.equal(projection.profiles[0].defaultCount, EXECUTION_ROLES.length);
    assert.equal(projection.profiles[0].requiredDefaultCount, EXECUTION_ROLES.length);
    assert.deepEqual(projection.profiles[0].missingDefaults, []);
    assert.equal(projection.profiles[0].issueCount, 0);
    assert.equal(projection.complete, true);
    assert.equal(registry.profiles[0].assignments.length, WORKFLOW_ROLES.length);
    assert.equal(registry.profiles[0].assignments[0].role, 'follower');
    assert.equal(JSON.stringify(registry), before);
  });
});
