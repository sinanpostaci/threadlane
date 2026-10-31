import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  findModelFamily,
  groupModelFamilies,
  modelChoiceForFamilyEffort,
  selectedFamilyEffort,
  visibleModelCount,
} from './model-families.ts';
import type { ModelDefinition, ToolDefinition } from './types.ts';

const AGY_TOOL: ToolDefinition = Object.freeze({
  id: 'agy-main',
  label: 'Antigravity CLI',
  adapter: 'antigravity',
  executable_path: '/usr/bin/agy',
  enabled: true,
});

function freezeModel(model: ModelDefinition): ModelDefinition {
  Object.freeze(model.efforts);
  return Object.freeze({ ...model });
}

const REAL_EIGHT_AGY_MODELS: readonly ModelDefinition[] = Object.freeze([
  freezeModel({
    id: 'gemini-4-argon-low',
    tool_id: 'agy-main',
    label: 'Gemini 4 Argon (Low)',
    model_arg: 'gemini-4-argon-low',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'gemini-4-argon-medium',
    tool_id: 'agy-main',
    label: 'Gemini 4 Argon (Medium)',
    model_arg: 'gemini-4-argon-medium',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'gemini-4-argon-high',
    tool_id: 'agy-main',
    label: 'Gemini 4 Argon (High)',
    model_arg: 'gemini-4-argon-high',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'claude-opus-5-5-low',
    tool_id: 'agy-main',
    label: 'Claude Opus 5.5 (Low)',
    model_arg: 'claude-opus-5-5-low',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'claude-opus-5-5-medium',
    tool_id: 'agy-main',
    label: 'Claude Opus 5.5 (Medium)',
    model_arg: 'claude-opus-5-5-medium',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'claude-opus-5-5-high',
    tool_id: 'agy-main',
    label: 'Claude Opus 5.5 (High)',
    model_arg: 'claude-opus-5-5-high',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'claude-sonnet-5-5-medium',
    tool_id: 'agy-main',
    label: 'Claude Sonnet 5.5 (Medium)',
    model_arg: 'claude-sonnet-5-5-medium',
    efforts: [],
    allow_no_effort: true,
  }),
  freezeModel({
    id: 'claude-sonnet-5-5-high',
    tool_id: 'agy-main',
    label: 'Claude Sonnet 5.5 (High)',
    model_arg: 'claude-sonnet-5-5-high',
    efforts: [],
    allow_no_effort: true,
  }),
]);

describe('model-families helper', () => {
  it('groups the real 8 Antigravity CLI records into 3 visible model families without mutating inputs', () => {
    const families = groupModelFamilies(REAL_EIGHT_AGY_MODELS, [AGY_TOOL]);
    assert.equal(families.length, 3);
    assert.equal(visibleModelCount(REAL_EIGHT_AGY_MODELS, [AGY_TOOL]), 3);

    assert.deepEqual(
      families.map((family) => ({
        label: family.label,
        tool_id: family.tool_id,
        encodedEfforts: family.encodedEfforts,
        efforts: family.variants.map((variant) => variant.effort),
        memberIds: family.members.map((member) => member.id),
      })),
      [
        {
          label: 'Gemini 4 Argon',
          tool_id: 'agy-main',
          encodedEfforts: true,
          efforts: ['low', 'medium', 'high'],
          memberIds: [
            'gemini-4-argon-low',
            'gemini-4-argon-medium',
            'gemini-4-argon-high',
          ],
        },
        {
          label: 'Claude Opus 5.5',
          tool_id: 'agy-main',
          encodedEfforts: true,
          efforts: ['low', 'medium', 'high'],
          memberIds: [
            'claude-opus-5-5-low',
            'claude-opus-5-5-medium',
            'claude-opus-5-5-high',
          ],
        },
        {
          label: 'Claude Sonnet 5.5',
          tool_id: 'agy-main',
          encodedEfforts: true,
          efforts: ['medium', 'high'],
          memberIds: ['claude-sonnet-5-5-medium', 'claude-sonnet-5-5-high'],
        },
      ]
    );
  });

  it('orders encoded family efforts canonically from low through ultra regardless of input order', () => {
    const unordered: ModelDefinition[] = [
      {
        id: 'agy-ultra',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Ultra)',
        model_arg: 'gemini-4-argon-ultra',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'agy-high',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (High)',
        model_arg: 'gemini-4-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'agy-low',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Low)',
        model_arg: 'gemini-4-argon-low',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'agy-max',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Max)',
        model_arg: 'gemini-4-argon-max',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'agy-xhigh',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Xhigh)',
        model_arg: 'gemini-4-argon-xhigh',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'agy-medium',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Medium)',
        model_arg: 'gemini-4-argon-medium',
        efforts: [],
        allow_no_effort: true,
      },
    ];

    const families = groupModelFamilies(unordered, [AGY_TOOL]);
    assert.equal(families.length, 1);
    assert.deepEqual(
      families[0]!.variants.map((variant) => variant.effort),
      ['low', 'medium', 'high', 'xhigh', 'max', 'ultra']
    );
    assert.deepEqual(
      families[0]!.members.map((member) => member.id),
      ['agy-low', 'agy-medium', 'agy-high', 'agy-xhigh', 'agy-max', 'agy-ultra']
    );
  });

  it('preserves existing concrete ModelChoice round-trip and maps effort changes without first-member fallback', () => {
    const families = groupModelFamilies(REAL_EIGHT_AGY_MODELS, [AGY_TOOL]);
    const existingChoice = { model_id: 'gemini-4-argon-high', effort: null };

    const argonFamily = findModelFamily(families, existingChoice.model_id);
    assert.ok(argonFamily);
    assert.equal(argonFamily.label, 'Gemini 4 Argon');

    const derivedEffort = selectedFamilyEffort(argonFamily, existingChoice);
    assert.equal(derivedEffort, 'high');

    const roundTrip = modelChoiceForFamilyEffort(argonFamily, derivedEffort);
    assert.deepEqual(roundTrip, {
      model_id: 'gemini-4-argon-high',
      effort: null,
    });

    assert.deepEqual(modelChoiceForFamilyEffort(argonFamily, 'low'), {
      model_id: 'gemini-4-argon-low',
      effort: null,
    });
    assert.deepEqual(modelChoiceForFamilyEffort(argonFamily, 'medium'), {
      model_id: 'gemini-4-argon-medium',
      effort: null,
    });

    // Null or unknown effort on encoded family must return null, never falling back to first member.
    assert.equal(modelChoiceForFamilyEffort(argonFamily, null), null);
    assert.equal(modelChoiceForFamilyEffort(argonFamily, ''), null);
    assert.equal(modelChoiceForFamilyEffort(argonFamily, 'max'), null);

    // Sonnet only has medium and high; low must be rejected.
    const sonnetFamily = findModelFamily(families, 'claude-sonnet-5-5-high');
    assert.ok(sonnetFamily);
    assert.equal(modelChoiceForFamilyEffort(sonnetFamily, 'low'), null);
    assert.deepEqual(modelChoiceForFamilyEffort(sonnetFamily, 'medium'), {
      model_id: 'claude-sonnet-5-5-medium',
      effort: null,
    });

    // Non-null raw effort on an encoded variant is invalid.
    assert.equal(
      selectedFamilyEffort(argonFamily, {
        model_id: 'gemini-4-argon-high',
        effort: 'high',
      }),
      null
    );
  });

  it('does not merge across distinct tools, non-Antigravity adapters, versions, or nonmatching label/arg/metadata', () => {
    const tools: ToolDefinition[] = [
      AGY_TOOL,
      {
        id: 'agy-secondary',
        label: 'Second Antigravity',
        adapter: 'antigravity',
        executable_path: '/usr/bin/agy2',
        enabled: true,
      },
      {
        id: 'codex-main',
        label: 'Codex CLI',
        adapter: 'codex_cli',
        executable_path: '/usr/bin/codex',
        enabled: true,
      },
      {
        id: 'opencode-main',
        label: 'OpenCode',
        adapter: 'opencode_v2',
        executable_path: '/usr/bin/opencode',
        enabled: true,
      },
    ];

    const models: ModelDefinition[] = [
      // Valid singleton on agy-main
      {
        id: 'gemini-4-argon-low',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Low)',
        model_arg: 'gemini-4-argon-low',
        efforts: [],
        allow_no_effort: true,
      },
      // Different tool_id (agy-secondary) -> must not merge with agy-main
      {
        id: 'gemini-4-argon-high-other-tool',
        tool_id: 'agy-secondary',
        label: 'Gemini 4 Argon (High)',
        model_arg: 'gemini-4-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      // Different version in label and model_arg
      {
        id: 'gemini-4-5-argon-high',
        tool_id: 'agy-main',
        label: 'Gemini 4.5 Argon (High)',
        model_arg: 'gemini-4-5-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      // Non-Antigravity adapter even with matching naming pattern
      {
        id: 'codex-argon-low',
        tool_id: 'codex-main',
        label: 'Gemini 4 Argon (Low)',
        model_arg: 'gemini-4-argon-low',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'codex-argon-high',
        tool_id: 'codex-main',
        label: 'Gemini 4 Argon (High)',
        model_arg: 'gemini-4-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      // Mismatched label effort vs model_arg suffix
      {
        id: 'mismatch-effort',
        tool_id: 'agy-main',
        label: 'Claude Opus 5.5 (Low)',
        model_arg: 'claude-opus-5-5-high',
        efforts: [],
        allow_no_effort: true,
      },
      // Non-parenthesized label
      {
        id: 'no-paren-label',
        tool_id: 'agy-main',
        label: 'Claude Opus 5.5 High',
        model_arg: 'claude-opus-5-5-high',
        efforts: [],
        allow_no_effort: true,
      },
      // Unrecognized suffix token (Fast / Thinking)
      {
        id: 'unknown-fast',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Fast)',
        model_arg: 'gemini-4-argon-fast',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'unknown-thinking',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Thinking)',
        model_arg: 'gemini-4-argon-thinking',
        efforts: [],
        allow_no_effort: true,
      },
      // Custom non-empty efforts metadata
      {
        id: 'custom-efforts',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Medium)',
        model_arg: 'gemini-4-argon-medium',
        efforts: ['medium'],
        allow_no_effort: true,
      },
      // allow_no_effort: false
      {
        id: 'disallow-no-effort',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Max)',
        model_arg: 'gemini-4-argon-max',
        efforts: [],
        allow_no_effort: false,
      },
      // Provider-prefixed model_arg
      {
        id: 'provider-prefixed',
        tool_id: 'opencode-main',
        label: 'DeepSeek v4.1 Flash',
        model_arg: 'opencode-go/deepseek-v4.1-flash',
        efforts: ['max'],
        allow_no_effort: false,
      },
    ];

    const families = groupModelFamilies(models, tools);
    assert.equal(families.length, models.length);

    // Only the first three valid Antigravity singleton variants are encodedEfforts=true
    const encoded = families.filter((family) => family.encodedEfforts);
    assert.equal(encoded.length, 3);
    assert.deepEqual(
      encoded.map((family) => ({
        tool_id: family.tool_id,
        label: family.label,
        efforts: family.variants.map((v) => v.effort),
      })),
      [
        { tool_id: 'agy-main', label: 'Gemini 4 Argon', efforts: ['low'] },
        { tool_id: 'agy-secondary', label: 'Gemini 4 Argon', efforts: ['high'] },
        { tool_id: 'agy-main', label: 'Gemini 4.5 Argon', efforts: ['high'] },
      ]
    );

    // Ordinary provider model preserves ordinary effort validation
    const ordinary = findModelFamily(families, 'provider-prefixed');
    assert.ok(ordinary);
    assert.equal(ordinary.encodedEfforts, false);
    assert.equal(
      selectedFamilyEffort(ordinary, {
        model_id: 'provider-prefixed',
        effort: 'max',
      }),
      'max'
    );
    assert.deepEqual(modelChoiceForFamilyEffort(ordinary, 'max'), {
      model_id: 'provider-prefixed',
      effort: 'max',
    });
    assert.equal(modelChoiceForFamilyEffort(ordinary, null), null);
    assert.equal(modelChoiceForFamilyEffort(ordinary, 'low'), null);
  });

  it('keeps all involved candidates ordinary when duplicate effort collisions occur', () => {
    const models: ModelDefinition[] = [
      {
        id: 'gemini-4-argon-low',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (Low)',
        model_arg: 'gemini-4-argon-low',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'gemini-4-argon-high-a',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (High)',
        model_arg: 'gemini-4-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'gemini-4-argon-high-b',
        tool_id: 'agy-main',
        label: 'Gemini 4 Argon (High)',
        model_arg: 'gemini-4-argon-high',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'claude-sonnet-5-5-medium',
        tool_id: 'agy-main',
        label: 'Claude Sonnet 5.5 (Medium)',
        model_arg: 'claude-sonnet-5-5-medium',
        efforts: [],
        allow_no_effort: true,
      },
      {
        id: 'claude-sonnet-5-5-high',
        tool_id: 'agy-main',
        label: 'Claude Sonnet 5.5 (High)',
        model_arg: 'claude-sonnet-5-5-high',
        efforts: [],
        allow_no_effort: true,
      },
    ];

    const families = groupModelFamilies(models, [AGY_TOOL]);
    // The 3 colliding Gemini 4 Argon candidates remain 3 ordinary entries + 1 grouped Claude Sonnet 5.5 family = 4
    assert.equal(families.length, 4);

    for (const id of [
      'gemini-4-argon-low',
      'gemini-4-argon-high-a',
      'gemini-4-argon-high-b',
    ]) {
      const fam = findModelFamily(families, id);
      assert.ok(fam);
      assert.equal(fam.encodedEfforts, false);
      assert.deepEqual(fam.variants, []);
      assert.equal(fam.members.length, 1);
    }

    const sonnet = findModelFamily(families, 'claude-sonnet-5-5-high');
    assert.ok(sonnet);
    assert.equal(sonnet.encodedEfforts, true);
    assert.deepEqual(
      sonnet.variants.map((variant) => variant.effort),
      ['medium', 'high']
    );
  });
});
