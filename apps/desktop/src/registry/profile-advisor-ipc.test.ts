import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyRegistryChangeLocally,
  type RegistryChange,
  type RegistrySnapshot,
  type WorkflowProfile,
} from './ipc.ts';

function makeBaseSnapshot(): RegistrySnapshot {
  return {
    schema_version: 1,
    revision: 7,
    tools: [
      {
        id: 'codex-tool',
        label: 'Codex CLI',
        adapter: 'codex_cli',
        executable_path: '/usr/bin/codex',
        enabled: true,
      },
      {
        id: 'jev-api',
        label: 'Jev API',
        adapter: 'jev_api',
        executable_path: null,
        enabled: true,
      },
      {
        id: 'openai-api',
        label: 'OpenAI Decisions API',
        adapter: 'openai_decisions_api',
        executable_path: null,
        enabled: true,
      },
      {
        id: 'jev-offline',
        label: 'Jev offline',
        adapter: 'jev_api',
        executable_path: null,
        enabled: false,
      },
    ],
    models: [
      {
        id: 'codex-role',
        tool_id: 'codex-tool',
        label: 'Codex role model',
        model_arg: 'gpt-6-astra',
        efforts: ['high'],
        allow_no_effort: false,
      },
      {
        id: 'jev-shadow',
        tool_id: 'jev-api',
        label: 'Jev shadow',
        model_arg: 'jev-shadow-v1',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'openai-shadow',
        tool_id: 'openai-api',
        label: 'OpenAI shadow',
        model_arg: 'gpt-shadow',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'jev-effort',
        tool_id: 'jev-api',
        label: 'Jev effort',
        model_arg: 'jev-effort-v1',
        efforts: ['low'],
        allow_no_effort: false,
      },
      {
        id: 'codex-noeffort',
        tool_id: 'codex-tool',
        label: 'Codex no effort',
        model_arg: 'gpt-no-effort',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'jev-disabled',
        tool_id: 'jev-offline',
        label: 'Jev disabled',
        model_arg: 'jev-disabled-v1',
        efforts: [],
        allow_no_effort: true,
      },
    ],
    profiles: [
      {
        id: 'base',
        name: 'Base profile',
        assignments: [
          {
            role: 'coder',
            difficulty: null,
            choice: { model_id: 'codex-role', effort: 'high' },
          },
        ],
      },
    ],
    default_profile_id: 'base',
  };
}

function makeProfile(id: string, name: string, decisionAdvisor?: unknown): WorkflowProfile {
  const profile: WorkflowProfile = {
    id,
    name,
    assignments: [
      {
        role: 'coder',
        difficulty: null,
        choice: { model_id: 'codex-role', effort: 'high' },
      },
    ],
  };
  if (decisionAdvisor !== undefined) {
    profile.decision_advisor = decisionAdvisor as WorkflowProfile['decision_advisor'];
  }
  return profile;
}

function apply(snapshot: RegistrySnapshot, change: RegistryChange): RegistrySnapshot {
  return applyRegistryChangeLocally(snapshot, snapshot.revision, change);
}

function profileById(snapshot: RegistrySnapshot, id: string): WorkflowProfile {
  const profile = snapshot.profiles.find((candidate) => candidate.id === id);
  assert.ok(profile, `expected profile "${id}" to exist`);
  return profile;
}

function modelById(snapshot: RegistrySnapshot, id: string): RegistrySnapshot['models'][number] {
  const model = snapshot.models.find((candidate) => candidate.id === id);
  assert.ok(model, `expected model "${id}" to exist`);
  return model;
}

function toolById(snapshot: RegistrySnapshot, id: string): RegistrySnapshot['tools'][number] {
  const tool = snapshot.tools.find((candidate) => candidate.id === id);
  assert.ok(tool, `expected tool "${id}" to exist`);
  return tool;
}

function throwsCode(fn: () => unknown, code: string): void {
  assert.throws(fn, (err: unknown) => (err as { code?: string }).code === code);
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value as unknown as Record<string, unknown>)) {
      deepFreeze(child);
    }
    Object.freeze(value);
  }
  return value;
}

describe('DEV preview registry decision advisor adapter', () => {
  it('persists valid advisors through save, edit, clone, and reads while omitting off values', () => {
    const base = makeBaseSnapshot();
    const sourceBefore = structuredClone(base);

    const saved = apply(base, {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile', {
        model_id: 'jev-shadow',
        mode: 'shadow',
      }),
    });
    const savedProfile = profileById(saved, 'advisor');
    assert.deepEqual(savedProfile.decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });

    const edited = apply(saved, {
      op: 'upsert_profile',
      profile: { ...savedProfile, name: 'Advisor profile renamed' },
    });
    const editedProfile = profileById(edited, 'advisor');
    assert.equal(editedProfile.name, 'Advisor profile renamed');
    assert.deepEqual(editedProfile.decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });

    const cloned = apply(edited, {
      op: 'upsert_profile',
      profile: {
        ...editedProfile,
        id: 'advisor-copy',
        name: 'Advisor profile copy',
      },
    });
    const clonedProfile = profileById(cloned, 'advisor-copy');
    assert.deepEqual(clonedProfile.decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });
    assert.notEqual(clonedProfile.decision_advisor, editedProfile.decision_advisor);

    const read = apply(cloned, { op: 'set_default_profile', profile_id: 'advisor-copy' });
    assert.equal(read.default_profile_id, 'advisor-copy');
    assert.deepEqual(profileById(read, 'advisor-copy').decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });

    const openAiAdvisor = apply(read, {
      op: 'upsert_profile',
      profile: makeProfile('advisor-openai', 'OpenAI advisor', {
        model_id: 'openai-shadow',
        mode: 'shadow',
      }),
    });
    assert.deepEqual(profileById(openAiAdvisor, 'advisor-openai').decision_advisor, {
      model_id: 'openai-shadow',
      mode: 'shadow',
    });

    const off = apply(openAiAdvisor, {
      op: 'upsert_profile',
      profile: makeProfile('off', 'Off profile'),
    });
    assert.equal(Object.hasOwn(profileById(off, 'off'), 'decision_advisor'), false);

    const cleared = apply(off, {
      op: 'upsert_profile',
      profile: { ...profileById(off, 'advisor-copy'), decision_advisor: null },
    });
    assert.equal(Object.hasOwn(profileById(cleared, 'advisor-copy'), 'decision_advisor'), false);
    assert.deepEqual(base, sourceBefore);
  });

  it('refuses malformed advisors, unsupported modes, and extra fields instead of normalizing them off', () => {
    const base = makeBaseSnapshot();
    const sourceBefore = structuredClone(base);
    const malformed: unknown[] = [
      'jev-shadow',
      [],
      { model_id: 'jev-shadow' },
      { mode: 'shadow' },
      { model_id: '', mode: 'shadow' },
      { model_id: 'jev-shadow', mode: 'active' },
      { model_id: 'jev-shadow', mode: 'shadow', effort: null },
      { model_id: 'jev-shadow', mode: 'shadow', effort: 'max' },
      { model_id: 'jev-shadow', mode: 'shadow', provider: 'jev' },
    ];

    for (const [index, advisor] of malformed.entries()) {
      throwsCode(
        () =>
          apply(base, {
            op: 'upsert_profile',
            profile: makeProfile(`malformed-${index}`, `Malformed ${index}`, advisor),
          }),
        'invalid_advisor'
      );
    }
    assert.deepEqual(base, sourceBefore);
  });

  it('refuses disabled, wrong-adapter, and effort-requiring advisor references', () => {
    const base = makeBaseSnapshot();
    const sourceBefore = structuredClone(base);
    const invalid: Array<{ advisor: unknown; code: string }> = [
      { advisor: { model_id: 'missing-model', mode: 'shadow' }, code: 'missing_model' },
      { advisor: { model_id: 'jev-disabled', mode: 'shadow' }, code: 'disabled_tool' },
      { advisor: { model_id: 'codex-noeffort', mode: 'shadow' }, code: 'wrong_adapter' },
      { advisor: { model_id: 'jev-effort', mode: 'shadow' }, code: 'effort_required' },
    ];

    for (const [index, testCase] of invalid.entries()) {
      throwsCode(
        () =>
          apply(base, {
            op: 'upsert_profile',
            profile: makeProfile(`invalid-${index}`, `Invalid ${index}`, testCase.advisor),
          }),
        testCase.code
      );
    }
    assert.deepEqual(base, sourceBefore);
  });

  it('preserves CAS revision conflicts before advisor validation and leaves the source unchanged', () => {
    const seeded = apply(makeBaseSnapshot(), {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile', {
        model_id: 'jev-shadow',
        mode: 'shadow',
      }),
    });
    const sourceBefore = structuredClone(seeded);

    throwsCode(
      () =>
        applyRegistryChangeLocally(seeded, seeded.revision - 1, {
          op: 'upsert_profile',
          profile: { ...profileById(seeded, 'advisor'), decision_advisor: null },
        }),
      'revision_conflict'
    );
    throwsCode(
      () =>
        applyRegistryChangeLocally(seeded, seeded.revision - 1, {
          op: 'upsert_profile',
          profile: makeProfile('malformed', 'Malformed', {
            model_id: 'jev-shadow',
            mode: 'unsupported',
          }),
        }),
      'revision_conflict'
    );
    throwsCode(
      () =>
        applyRegistryChangeLocally(seeded, seeded.revision - 1, {
          op: 'delete_model',
          model_id: 'jev-shadow',
        }),
      'revision_conflict'
    );
    assert.deepEqual(seeded, sourceBefore);
  });

  it('protects advisor references from model deletion until the advisor is cleared', () => {
    const seeded = apply(makeBaseSnapshot(), {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile', {
        model_id: 'jev-shadow',
        mode: 'shadow',
      }),
    });
    const sourceBefore = structuredClone(seeded);

    throwsCode(
      () => apply(seeded, { op: 'delete_model', model_id: 'jev-shadow' }),
      'model_in_use'
    );
    assert.deepEqual(seeded, sourceBefore);

    const cleared = apply(seeded, {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile'),
    });
    assert.equal(Object.hasOwn(profileById(cleared, 'advisor'), 'decision_advisor'), false);

    const deleted = apply(cleared, { op: 'delete_model', model_id: 'jev-shadow' });
    assert.equal(
      deleted.models.some((model) => model.id === 'jev-shadow'),
      false
    );
  });

  it('revalidates advisor references when tools or models are edited', () => {
    const seeded = apply(makeBaseSnapshot(), {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile', {
        model_id: 'jev-shadow',
        mode: 'shadow',
      }),
    });
    const sourceBefore = structuredClone(seeded);
    const jevTool = toolById(seeded, 'jev-api');
    const advisorModel = modelById(seeded, 'jev-shadow');

    throwsCode(
      () =>
        apply(seeded, {
          op: 'upsert_tool',
          tool: { ...jevTool, enabled: false },
        }),
      'disabled_tool'
    );
    throwsCode(
      () =>
        apply(seeded, {
          op: 'upsert_tool',
          tool: { ...jevTool, adapter: 'codex_cli' },
        }),
      'wrong_adapter'
    );
    throwsCode(
      () =>
        apply(seeded, {
          op: 'upsert_model',
          model: { ...advisorModel, efforts: ['low'], allow_no_effort: false },
        }),
      'effort_required'
    );
    throwsCode(
      () =>
        apply(seeded, {
          op: 'upsert_models',
          models: [{ ...advisorModel, efforts: ['low'], allow_no_effort: false }],
        }),
      'effort_required'
    );
    throwsCode(
      () =>
        apply(seeded, {
          op: 'upsert_model',
          model: { ...advisorModel, tool_id: 'codex-tool' },
        }),
      'wrong_adapter'
    );
    assert.deepEqual(seeded, sourceBefore);

    const switchedApiAdapter = apply(seeded, {
      op: 'upsert_tool',
      tool: { ...jevTool, adapter: 'openai_decisions_api' },
    });
    assert.deepEqual(profileById(switchedApiAdapter, 'advisor').decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });

    const relabeled = apply(seeded, {
      op: 'upsert_model',
      model: { ...advisorModel, label: 'Jev shadow renamed' },
    });
    assert.deepEqual(profileById(relabeled, 'advisor').decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });

    const openAiTool = toolById(relabeled, 'openai-api');
    const unrelatedToolEdit = apply(relabeled, {
      op: 'upsert_tool',
      tool: { ...openAiTool, enabled: false },
    });
    assert.deepEqual(profileById(unrelatedToolEdit, 'advisor').decision_advisor, {
      model_id: 'jev-shadow',
      mode: 'shadow',
    });
  });

  it('keeps legacy role behavior and does not mutate source or cloned advisor objects', () => {
    const legacyBase = makeBaseSnapshot();
    const legacyToolDisable = apply(legacyBase, {
      op: 'upsert_tool',
      tool: { ...toolById(legacyBase, 'codex-tool'), enabled: false },
    });
    assert.equal(toolById(legacyToolDisable, 'codex-tool').enabled, false);

    const seeded = apply(makeBaseSnapshot(), {
      op: 'upsert_profile',
      profile: makeProfile('advisor', 'Advisor profile', {
        model_id: 'jev-shadow',
        mode: 'shadow',
      }),
    });
    const frozen = deepFreeze(structuredClone(seeded));
    const sourceBefore = JSON.stringify(frozen);

    const updated = apply(frozen, {
      op: 'upsert_model',
      model: { ...modelById(frozen, 'jev-shadow'), label: 'Jev shadow renamed' },
    });
    assert.equal(JSON.stringify(frozen), sourceBefore);

    const sourceAdvisor = profileById(frozen, 'advisor').decision_advisor as {
      model_id: string;
    };
    const resultAdvisor = profileById(updated, 'advisor').decision_advisor as {
      model_id: string;
    };
    assert.notEqual(resultAdvisor, sourceAdvisor);
    resultAdvisor.model_id = 'openai-shadow';
    assert.equal(sourceAdvisor.model_id, 'jev-shadow');

    throwsCode(
      () =>
        apply(frozen, {
          op: 'upsert_profile',
          profile: makeProfile('malformed', 'Malformed', {
            model_id: 'jev-shadow',
            mode: 'shadow',
            effort: 'max',
          }),
        }),
      'invalid_advisor'
    );
    assert.equal(JSON.stringify(frozen), sourceBefore);
  });
});
