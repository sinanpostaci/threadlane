import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  projectReadinessChecklist,
  type ReadinessCheck,
  type ReadinessChecklistInput,
  type ReadinessProjection,
} from './readiness.ts';
import {
  WORKFLOW_ROLES,
  createEmptyRegistrySnapshot,
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
    id: 'model-main',
    tool_id: 'opencode',
    label: 'DeepSeek 4.1',
    model_arg: 'opencode-go/deepseek-v4.1-flash',
    efforts: ['low', 'high'],
    allow_no_effort: false,
    ...overrides,
  };
}

function fullAssignments(
  modelId: string = 'model-main',
  effort: string | null = 'high'
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
    state_dir: '/tmp/threadlane-readiness-test',
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
  inspections: ToolVersionInspection[] = [],
  overrides: Partial<ToolVersionScanState> = {}
): ToolVersionScanState {
  return {
    status: 'ready',
    inspections,
    inspectedRegistryRevision: registryRevision,
    error: null,
    startupStarted: true,
    nextTicketId: 1,
    activeTicketId: null,
    ...overrides,
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

function defaultInspections(registry: RegistrySnapshot): ToolVersionInspection[] {
  return registry.tools
    .filter(
      (tool) =>
        tool.enabled &&
        tool.adapter !== 'jev_api' &&
        tool.adapter !== 'openai_decisions_api'
    )
    .map((tool) => makeInspection(tool.id));
}

function makeInput(
  overrides: Partial<ReadinessChecklistInput> = {}
): ReadinessChecklistInput {
  const registry = overrides.registry ?? makeRegistry();
  return {
    registry,
    toolVersions:
      overrides.toolVersions ?? makeScan(registry.revision, defaultInspections(registry)),
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

describe('local readiness projection', () => {
  it('marks a complete current setup ready without claiming approval or native delivery', () => {
    const input = makeInput();
    const before = JSON.stringify(input);
    const projection = projectReadinessChecklist(input);

    assert.equal(projection.complete, true);
    assert.equal(projection.readyCount, 5);
    assert.equal(projection.totalCount, 5);
    assert.deepEqual(
      projection.checks.map((check) => check.id),
      ['agent-tools', 'executables', 'models', 'profiles', 'attachment']
    );
    for (const check of projection.checks) {
      assert.equal(check.state, 'ready', check.id);
    }
    assert.equal(projection.headline, 'Local setup complete');
    assert.equal(projection.defaultProfileName, 'Everyday');
    assert.equal(projection.profiles.length, 1);
    assert.deepEqual(projection.profiles[0], {
      name: 'Everyday',
      isDefault: true,
      state: 'complete',
      defaultCount: 6,
      requiredDefaultCount: 6,
      missingDefaults: [],
      issueCount: 0,
    });

    assert.match(projection.summary, /Codex/);
    assert.ok(
      projection.limitations.some((line) => /approved execution preview/i.test(line))
    );
    assert.ok(projection.limitations.some((line) => /notification/i.test(line)));
    assert.ok(
      projection.limitations.some((line) => /entitlement|paid model access/i.test(line))
    );
    assert.doesNotMatch(JSON.stringify(projection), /ready to run/i);
    assert.equal(JSON.stringify(input), before);
    assert.doesNotThrow(() => JSON.stringify(projection));
  });

  it('keeps an empty setup incomplete and links each check to an allowed page', () => {
    const projection = projectReadinessChecklist(
      makeInput({
        registry: createEmptyRegistrySnapshot(),
        toolVersions: makeScan(0, [], {
          status: 'idle',
          inspectedRegistryRevision: null,
          startupStarted: false,
          nextTicketId: 0,
        }),
        snapshot: makeSnapshot({
          attachment: makeAttachment({
            mode: 'unconfigured',
            connection: 'unconfigured',
            state_dir: null,
          }),
          bindings: [],
          selected_binding_id: null,
        }),
      })
    );

    assert.equal(projection.complete, false);
    assert.equal(projection.readyCount, 0);
    assert.equal(projection.profiles.length, 0);
    assert.equal(projection.defaultProfileName, null);
    assert.deepEqual(
      projection.checks.map((check) => check.state),
      ['blocked', 'blocked', 'blocked', 'blocked', 'blocked']
    );
    assert.deepEqual(
      projection.checks.map((check) => check.action.page),
      ['tools', 'tools', 'tools', 'profiles', 'settings']
    );
  });

  it('rejects a successful scan from an older registry revision', () => {
    const registry = makeRegistry({ revision: 7 });
    const projection = projectReadinessChecklist(
      makeInput({
        registry,
        toolVersions: makeScan(6, [makeInspection('opencode')]),
      })
    );
    const executables = checkById(projection, 'executables');

    assert.equal(executables.state, 'blocked');
    assert.match(executables.summary, /stale/i);
    assert.match(executables.detail, /revision/i);
    assert.equal(projection.complete, false);
    assert.equal(projection.readyCount, 4);
  });

  it('does not reuse a stale status or a pending scan as a current success', () => {
    const stale = projectReadinessChecklist(
      makeInput({
        toolVersions: makeScan(7, [makeInspection('opencode')], {
          status: 'stale',
        }),
      })
    );
    assert.equal(checkById(stale, 'executables').state, 'blocked');
    assert.match(checkById(stale, 'executables').summary, /stale/i);
    assert.equal(stale.complete, false);

    const pending = projectReadinessChecklist(
      makeInput({
        toolVersions: makeScan(7, [makeInspection('opencode')], {
          status: 'loading',
        }),
      })
    );
    assert.equal(checkById(pending, 'executables').state, 'loading');
    assert.equal(pending.complete, false);
  });

  it('reports an enabled agent executable that the current scan did not find', () => {
    const projection = projectReadinessChecklist(
      makeInput({
        toolVersions: makeScan(7, [
          makeInspection('opencode', { availability: 'not_found' }),
        ]),
      })
    );
    const executables = checkById(projection, 'executables');

    assert.equal(executables.state, 'blocked');
    assert.match(executables.detail, /not found/i);
    assert.equal(projection.complete, false);
  });

  it('blocks a saved model tied to a disabled tool', () => {
    const registry = makeRegistry({
      tools: [
        makeTool(),
        makeTool({
          id: 'agy',
          label: 'Antigravity',
          adapter: 'antigravity',
          enabled: false,
        }),
      ],
      models: [
        makeModel(),
        makeModel({
          id: 'model-disabled',
          tool_id: 'agy',
          label: 'Argon',
          model_arg: 'antigravity/argon',
        }),
      ],
      profiles: [
        makeProfile(
          'profile-disabled',
          'Disabled choice',
          fullAssignments('model-disabled')
        ),
      ],
      default_profile_id: 'profile-disabled',
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');

    assert.equal(models.state, 'blocked');
    assert.match(models.detail, /Antigravity/);
    assert.match(models.detail, /disabled/i);
    assert.equal(checkById(projection, 'profiles').state, 'blocked');
    assert.equal(projection.profiles[0].state, 'invalid');
    assert.equal(projection.complete, false);
  });

  it('blocks an effort that the assigned model does not list', () => {
    const registry = makeRegistry({
      models: [makeModel({ efforts: ['low'], allow_no_effort: false })],
      profiles: [
        makeProfile('profile-main', 'Everyday', fullAssignments('model-main', 'max')),
      ],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');

    assert.equal(models.state, 'blocked');
    assert.match(models.detail, /effort/i);
    assert.equal(checkById(projection, 'profiles').state, 'blocked');
    assert.equal(projection.complete, false);
  });

  it('blocks an empty model choice without substituting another model', () => {
    const registry = makeRegistry({
      profiles: [
        makeProfile('profile-main', 'Everyday', fullAssignments('', 'high')),
      ],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const models = checkById(projection, 'models');

    assert.equal(models.state, 'blocked');
    assert.match(models.detail, /no model selected/i);
    assert.equal(projection.complete, false);
  });

  it('requires every role default and does not let an override replace one', () => {
    const assignments: RoleAssignment[] = WORKFLOW_ROLES.filter(
      (role) => role !== 'coder'
    ).map((role) => ({
      role,
      difficulty: null,
      choice: { model_id: 'model-main', effort: 'high' },
    }));
    assignments.push({
      role: 'coder',
      difficulty: 'hard',
      choice: { model_id: 'model-main', effort: 'high' },
    });
    const registry = makeRegistry({
      profiles: [makeProfile('profile-main', 'Everyday', assignments)],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const profiles = checkById(projection, 'profiles');

    assert.equal(checkById(projection, 'models').state, 'ready');
    assert.equal(profiles.state, 'attention');
    assert.match(profiles.summary, /partial/i);
    assert.match(profiles.detail, /Coder/);
    assert.match(profiles.detail, /overrides do not replace/i);
    assert.equal(projection.complete, false);
    assert.equal(projection.readyCount, 4);
    assert.deepEqual(projection.profiles[0].missingDefaults, ['Coder']);
    assert.equal(projection.profiles[0].defaultCount, 5);
    assert.equal(projection.profiles[0].issueCount, 0);
  });

  it('does not silently choose a default profile', () => {
    const noDefault = projectReadinessChecklist(
      makeInput({ registry: makeRegistry({ default_profile_id: null }) })
    );
    const noDefaultCheck = checkById(noDefault, 'profiles');
    assert.equal(noDefaultCheck.state, 'blocked');
    assert.match(noDefaultCheck.summary, /no default/i);
    assert.match(noDefaultCheck.detail, /silently chosen|explicitly/i);
    assert.equal(noDefault.defaultProfileName, null);
    assert.equal(noDefault.complete, false);

    const missingDefault = projectReadinessChecklist(
      makeInput({
        registry: makeRegistry({ default_profile_id: 'profile-missing' }),
      })
    );
    assert.equal(checkById(missingDefault, 'profiles').state, 'blocked');
    assert.match(checkById(missingDefault, 'profiles').summary, /unavailable/i);
    assert.equal(missingDefault.complete, false);
  });

  it('treats a profile name or presence alone as incomplete setup', () => {
    const registry = makeRegistry({
      profiles: [makeProfile('profile-empty', 'Empty draft', [])],
      default_profile_id: 'profile-empty',
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));

    assert.equal(checkById(projection, 'models').state, 'blocked');
    assert.match(checkById(projection, 'models').summary, /no saved role choices/i);
    assert.equal(checkById(projection, 'profiles').state, 'attention');
    assert.match(checkById(projection, 'profiles').summary, /partial/i);
    assert.equal(projection.profiles[0].state, 'partial');
    assert.equal(projection.profiles[0].defaultCount, 0);
    assert.equal(projection.complete, false);
  });

  it('shows a partial saved profile beside a complete default instead of ignoring it', () => {
    const registry = makeRegistry({
      profiles: [
        makeProfile(),
        makeProfile('profile-draft', 'Draft profile', [
          {
            role: 'follower',
            difficulty: null,
            choice: { model_id: 'model-main', effort: 'high' },
          },
        ]),
      ],
    });
    const projection = projectReadinessChecklist(makeInput({ registry }));
    const profiles = checkById(projection, 'profiles');
    const draft = projection.profiles.find(
      (profile) => profile.name === 'Draft profile'
    );

    assert.equal(profiles.state, 'attention');
    assert.match(profiles.detail, /Draft profile/);
    assert.ok(draft);
    assert.equal(draft.state, 'partial');
    assert.equal(draft.defaultCount, 1);
    assert.equal(draft.missingDefaults.length, 5);
    assert.equal(projection.complete, false);
  });

  it('requires a real runtime attachment and a non-demo selected binding', () => {
    const real = projectReadinessChecklist(makeInput());
    assert.equal(checkById(real, 'attachment').state, 'ready');
    assert.equal(real.complete, true);

    const demo = projectReadinessChecklist(
      makeInput({
        snapshot: makeSnapshot({
          attachment: makeAttachment({ mode: 'demo', connection: 'connected' }),
          bindings: [
            makeBinding({
              binding_id: 'bind-demo',
              label: 'Local demo',
              is_local_demo: true,
            }),
          ],
          selected_binding_id: 'bind-demo',
        }),
      })
    );
    assert.equal(checkById(demo, 'attachment').state, 'blocked');
    assert.match(checkById(demo, 'attachment').summary, /demo/i);
    assert.match(checkById(demo, 'attachment').detail, /runtime/i);
    assert.equal(demo.readyCount, 4);
    assert.equal(demo.complete, false);

    const noSelection = projectReadinessChecklist(
      makeInput({
        snapshot: makeSnapshot({ selected_binding_id: null }),
      })
    );
    assert.equal(checkById(noSelection, 'attachment').state, 'blocked');
    assert.match(checkById(noSelection, 'attachment').summary, /selected/i);

    const demoBinding = projectReadinessChecklist(
      makeInput({
        snapshot: makeSnapshot({
          bindings: [
            makeBinding({
              binding_id: 'bind-main',
              label: 'Local demo',
              is_local_demo: true,
            }),
          ],
        }),
      })
    );
    assert.equal(checkById(demoBinding, 'attachment').state, 'blocked');
    assert.match(checkById(demoBinding, 'attachment').summary, /demo binding/i);
  });

  it('keeps decision API tools optional and never claims key or entitlement verification', () => {
    const apiTool = makeTool({
      id: 'jev',
      label: 'Jev decisions',
      adapter: 'jev_api',
      enabled: true,
    });
    const apiInspection = makeInspection('jev', {
      availability: 'api_only',
      resolved_path: null,
      system_path: null,
      version: null,
      system_version: null,
    });
    const optionalRegistry = makeRegistry({
      tools: [makeTool(), apiTool],
    });
    const optional = projectReadinessChecklist(
      makeInput({
        registry: optionalRegistry,
        toolVersions: makeScan(7, [makeInspection('opencode'), apiInspection]),
      })
    );

    assert.equal(optional.complete, true);
    assert.match(checkById(optional, 'agent-tools').detail, /optional/i);
    assert.ok(
      optional.limitations.some((line) => /keys|entitlement/i.test(line))
    );

    const apiOnly = projectReadinessChecklist(
      makeInput({
        registry: makeRegistry({
          tools: [apiTool],
          models: [],
          profiles: [],
          default_profile_id: null,
        }),
        toolVersions: makeScan(7, [apiInspection]),
      })
    );
    assert.equal(checkById(apiOnly, 'agent-tools').state, 'blocked');
    assert.equal(apiOnly.complete, false);
  });

  it('holds registry-derived checks open while loading or after a registry error', () => {
    const loading = projectReadinessChecklist(makeInput({ registryLoading: true }));
    for (const id of ['agent-tools', 'executables', 'models', 'profiles'] as const) {
      assert.equal(checkById(loading, id).state, 'loading', id);
    }
    assert.equal(checkById(loading, 'attachment').state, 'ready');
    assert.equal(loading.complete, false);

    const failed = projectReadinessChecklist(
      makeInput({ registryError: 'Registry read failed.' })
    );
    for (const id of ['agent-tools', 'executables', 'models', 'profiles'] as const) {
      assert.equal(checkById(failed, id).state, 'blocked', id);
    }
    assert.equal(failed.complete, false);
  });
});
