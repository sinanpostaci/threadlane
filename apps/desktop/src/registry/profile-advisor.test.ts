import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  cloneDecisionAdvisor,
  decisionAdvisorFromProfile,
  decisionAdvisorSignature,
  eligibleDecisionAdvisors,
  normalizeDecisionAdvisor,
  profileSignatureWithDecisionAdvisor,
  profileWithDecisionAdvisor,
  validateDecisionAdvisor,
  type DecisionAdvisorRegistry,
} from './profile-advisor.ts';
import type {
  ModelDefinition,
  ToolDefinition,
  WorkflowProfile,
} from './types.ts';

const TOOLS: readonly ToolDefinition[] = [
  {
    id: 'jev-main',
    label: 'Jev System-One API',
    adapter: 'jev_api',
    executable_path: null,
    enabled: true,
  },
  {
    id: 'openai-main',
    label: 'OpenAI decisions API',
    adapter: 'openai_decisions_api',
    executable_path: null,
    enabled: true,
  },
  {
    id: 'jev-disabled',
    label: 'Disabled Jev API',
    adapter: 'jev_api',
    executable_path: null,
    enabled: false,
  },
  {
    id: 'codex-main',
    label: 'Codex CLI',
    adapter: 'codex_cli',
    executable_path: '/usr/bin/codex',
    enabled: true,
  },
];

function model(
  overrides: Partial<ModelDefinition> & Pick<ModelDefinition, 'id'>
): ModelDefinition {
  return {
    tool_id: 'jev-main',
    label: overrides.id,
    model_arg: overrides.id,
    efforts: [],
    allow_no_effort: true,
    ...overrides,
  };
}

const MODELS: readonly ModelDefinition[] = [
  model({ id: 'jev-1.14.0', label: 'Jev 1.14.0' }),
  model({
    id: 'gpt-6-luna',
    tool_id: 'openai-main',
    label: 'GPT-6 Luna Decisions',
  }),
  model({
    id: 'jev-disabled-model',
    tool_id: 'jev-disabled',
    label: 'Disabled Jev model',
  }),
  model({
    id: 'codex-astra',
    tool_id: 'codex-main',
    label: 'GPT-6 Astra',
    model_arg: 'gpt-6-astra',
    efforts: ['high'],
    allow_no_effort: false,
  }),
  model({
    id: 'jev-requires-effort',
    label: 'Jev effort model',
    efforts: ['high'],
    allow_no_effort: false,
  }),
  model({
    id: 'removed-tool-model',
    tool_id: 'removed-tool',
    label: 'Removed API model',
  }),
];

const REGISTRY: DecisionAdvisorRegistry = {
  tools: TOOLS,
  models: MODELS,
};

function baseProfile(): WorkflowProfile {
  return {
    id: 'profile-1',
    name: 'Standard profile',
    assignments: [
      {
        role: 'coder',
        difficulty: null,
        choice: { model_id: 'codex-astra', effort: 'high' },
      },
    ],
  };
}

describe('profile decision advisor', () => {
  it('treats legacy omission and null as the same off state', () => {
    assert.equal(normalizeDecisionAdvisor(undefined), null);
    assert.equal(normalizeDecisionAdvisor(null), null);
    assert.equal(decisionAdvisorFromProfile({}), null);
    assert.equal(decisionAdvisorFromProfile({ decision_advisor: null }), null);
    assert.equal(
      decisionAdvisorSignature(undefined),
      decisionAdvisorSignature(null)
    );

    const omitted = profileWithDecisionAdvisor(baseProfile(), undefined);
    const nullable = profileWithDecisionAdvisor(
      { ...baseProfile(), decision_advisor: null },
      null
    );

    assert.equal(Object.hasOwn(omitted, 'decision_advisor'), false);
    assert.equal(Object.hasOwn(nullable, 'decision_advisor'), false);
    assert.equal(
      JSON.stringify(omitted).includes('decision_advisor'),
      false
    );
    assert.equal(decisionAdvisorSignature(omitted.decision_advisor), 'off');
  });

  it('offers enabled Jev and OpenAI decision models that allow no effort', () => {
    const choices = eligibleDecisionAdvisors(REGISTRY);
    assert.deepEqual(
      choices.map((choice) => [choice.tool.id, choice.model.id]),
      [
        ['jev-main', 'jev-1.14.0'],
        ['openai-main', 'gpt-6-luna'],
      ]
    );

    for (const choice of choices) {
      const validation = validateDecisionAdvisor(REGISTRY, {
        model_id: choice.model.id,
        mode: 'shadow',
      });
      assert.equal(validation.state, 'ready');
      if (validation.state === 'ready') {
        assert.equal(validation.tool.id, choice.tool.id);
        assert.equal(validation.model.id, choice.model.id);
        assert.equal(validation.advisor.mode, 'shadow');
      }
    }
  });

  it('refuses missing, disabled, wrong-adapter, and effort-required advisors', () => {
    const missing = validateDecisionAdvisor(REGISTRY, {
      model_id: 'not-registered',
      mode: 'shadow',
    });
    assert.equal(missing.state, 'invalid');
    if (missing.state === 'invalid') {
      assert.equal(missing.issue.code, 'missing_model');
      assert.match(missing.issue.message, /not-registered/);
      assert.match(missing.issue.message, /clear the advisor/i);
    }

    const missingTool = validateDecisionAdvisor(REGISTRY, {
      model_id: 'removed-tool-model',
      mode: 'shadow',
    });
    assert.equal(missingTool.state, 'invalid');
    if (missingTool.state === 'invalid') {
      assert.equal(missingTool.issue.code, 'missing_tool');
      assert.match(missingTool.issue.message, /removed-tool/);
    }

    const disabled = validateDecisionAdvisor(REGISTRY, {
      model_id: 'jev-disabled-model',
      mode: 'shadow',
    });
    assert.equal(disabled.state, 'invalid');
    if (disabled.state === 'invalid') {
      assert.equal(disabled.issue.code, 'disabled_tool');
      assert.match(disabled.issue.message, /Disabled Jev API/);
    }

    const wrongAdapter = validateDecisionAdvisor(REGISTRY, {
      model_id: 'codex-astra',
      mode: 'shadow',
    });
    assert.equal(wrongAdapter.state, 'invalid');
    if (wrongAdapter.state === 'invalid') {
      assert.equal(wrongAdapter.issue.code, 'wrong_adapter');
      assert.match(wrongAdapter.issue.message, /Codex CLI/);
      assert.match(wrongAdapter.issue.message, /Jev or OpenAI/);
    }

    const effortRequired = validateDecisionAdvisor(REGISTRY, {
      model_id: 'jev-requires-effort',
      mode: 'shadow',
    });
    assert.equal(effortRequired.state, 'invalid');
    if (effortRequired.state === 'invalid') {
      assert.equal(effortRequired.issue.code, 'effort_required');
      assert.match(effortRequired.issue.message, /no effort setting/);
    }

    assert.equal(
      normalizeDecisionAdvisor({ model_id: 'jev-1.14.0', mode: 'live' }),
      null
    );
  });

  it('clones and clears a selection without changing the source advisor', () => {
    const source = { model_id: 'gpt-6-luna', mode: 'shadow' as const };
    const clone = cloneDecisionAdvisor(source);

    assert.deepEqual(clone, source);
    assert.notEqual(clone, source);
    assert.ok(clone);
    clone.model_id = 'jev-1.14.0';
    assert.equal(source.model_id, 'gpt-6-luna');

    const configured = profileWithDecisionAdvisor(baseProfile(), source);
    assert.deepEqual(configured.decision_advisor, source);

    const copiedProfile = profileWithDecisionAdvisor(
      { ...configured, id: 'profile-2', name: 'Cloned profile' },
      cloneDecisionAdvisor(configured.decision_advisor)
    );
    assert.deepEqual(copiedProfile.decision_advisor, source);
    assert.equal(copiedProfile.id, 'profile-2');

    const cleared = profileWithDecisionAdvisor(copiedProfile, null);
    assert.equal(Object.hasOwn(cleared, 'decision_advisor'), false);
    assert.equal(
      Object.hasOwn(
        JSON.parse(JSON.stringify(cleared)) as Record<string, unknown>,
        'decision_advisor'
      ),
      false
    );
  });

  it('serializes a configured advisor as shadow-only metadata with no effort', () => {
    const profile = profileWithDecisionAdvisor(baseProfile(), {
      model_id: 'jev-1.14.0',
      mode: 'shadow',
    });
    const serialized = JSON.parse(JSON.stringify(profile)) as {
      decision_advisor?: Record<string, unknown>;
    };

    assert.deepEqual(serialized.decision_advisor, {
      model_id: 'jev-1.14.0',
      mode: 'shadow',
    });
    assert.deepEqual(
      Object.keys(serialized.decision_advisor ?? {}).sort(),
      ['mode', 'model_id']
    );
    assert.equal(
      normalizeDecisionAdvisor({
        model_id: ' jev-1.14.0 ',
        mode: 'shadow',
        effort: 'high',
      })?.model_id,
      'jev-1.14.0'
    );
  });

  it('changes the profile signature only when the advisor changes', () => {
    const off = baseProfile();
    const nullOff = { ...baseProfile(), decision_advisor: null };
    const jev = profileWithDecisionAdvisor(baseProfile(), {
      model_id: 'jev-1.14.0',
      mode: 'shadow',
    });
    const openai = profileWithDecisionAdvisor(baseProfile(), {
      model_id: 'gpt-6-luna',
      mode: 'shadow',
    });

    const offSignature = profileSignatureWithDecisionAdvisor(off, off.assignments);
    const offAdvisorSignature = decisionAdvisorSignature(off.decision_advisor);
    assert.equal(
      offAdvisorSignature,
      decisionAdvisorSignature(nullOff.decision_advisor)
    );
    assert.equal(
      offSignature,
      profileSignatureWithDecisionAdvisor(nullOff, nullOff.assignments)
    );
    assert.notEqual(
      offSignature,
      profileSignatureWithDecisionAdvisor(jev, jev.assignments)
    );
    assert.notEqual(
      decisionAdvisorSignature(jev.decision_advisor),
      decisionAdvisorSignature(openai.decision_advisor)
    );
  });
});
