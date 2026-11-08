import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { findEquivalentTool, projectToolRegistrations } from './tool-duplicates.ts';
import type { ModelDefinition, ToolDefinition } from './types.ts';

function makeTool(overrides: Partial<ToolDefinition> & Pick<ToolDefinition, 'id'>): ToolDefinition {
  return {
    id: overrides.id,
    label: overrides.label ?? 'Antigravity',
    adapter: overrides.adapter ?? 'antigravity',
    executable_path: overrides.executable_path ?? null,
    enabled: overrides.enabled ?? true,
  };
}

function makeModel(
  overrides: Partial<ModelDefinition> & Pick<ModelDefinition, 'id' | 'tool_id'>,
): ModelDefinition {
  return {
    id: overrides.id,
    tool_id: overrides.tool_id,
    label: overrides.label ?? overrides.id,
    model_arg: overrides.model_arg ?? overrides.id,
    efforts: overrides.efforts ?? ['low', 'medium', 'high'],
    allow_no_effort: overrides.allow_no_effort ?? false,
  };
}

const ANTIGRAVITY_RAW_MODELS: readonly ModelDefinition[] = Object.freeze([
  makeModel({ id: 'agy-claude-sonnet-4', tool_id: 'antigravity', model_arg: 'claude-sonnet-4' }),
  makeModel({ id: 'agy-claude-opus-4', tool_id: 'antigravity', model_arg: 'claude-opus-4' }),
  makeModel({ id: 'agy-gemini-2.5-pro', tool_id: 'antigravity', model_arg: 'gemini-2.5-pro' }),
  makeModel({ id: 'agy-gemini-2.5-flash', tool_id: 'antigravity', model_arg: 'gemini-2.5-flash' }),
  makeModel({ id: 'agy-gemini-4-argon', tool_id: 'antigravity', model_arg: 'gemini-4-argon' }),
  makeModel({ id: 'agy-gpt-5', tool_id: 'antigravity', model_arg: 'gpt-5' }),
  makeModel({ id: 'agy-gpt-5-mini', tool_id: 'antigravity', model_arg: 'gpt-5-mini' }),
  makeModel({ id: 'agy-gpt-6-astra', tool_id: 'antigravity', model_arg: 'gpt-6-astra' }),
]);

describe('projectToolRegistrations and findEquivalentTool', () => {
  it('collapses the empty antigravity-2 replica into populated antigravity', () => {
    const antigravity = makeTool({ id: 'antigravity' });
    const antigravityReplica = makeTool({ id: 'antigravity-2' });
    const codex = makeTool({ id: 'codex', label: 'Codex CLI', adapter: 'codex_cli' });

    const projection = projectToolRegistrations(
      [antigravity, codex, antigravityReplica],
      ANTIGRAVITY_RAW_MODELS,
    );

    assert.deepEqual(
      projection.tools.map((tool) => tool.id),
      ['antigravity', 'codex'],
    );
    assert.equal(projection.duplicates.length, 1);
    assert.equal(projection.duplicates[0]?.primary, antigravity);
    assert.deepEqual(projection.duplicates[0]?.duplicates, [antigravityReplica]);
  });

  it('selects the populated tool as primary even when it appears later in the array', () => {
    const emptyFirst = makeTool({ id: 'antigravity-2' });
    const codex = makeTool({ id: 'codex', label: 'Codex CLI', adapter: 'codex_cli' });
    const populatedLater = makeTool({ id: 'antigravity' });

    const projection = projectToolRegistrations(
      [emptyFirst, codex, populatedLater],
      ANTIGRAVITY_RAW_MODELS,
    );

    assert.deepEqual(
      projection.tools.map((tool) => tool.id),
      ['codex', 'antigravity'],
    );
    assert.equal(projection.duplicates.length, 1);
    assert.equal(projection.duplicates[0]?.primary, populatedLater);
    assert.deepEqual(projection.duplicates[0]?.duplicates, [emptyFirst]);
  });

  it('keeps the first stable record when all equivalent tools have zero models', () => {
    const first = makeTool({ id: 'antigravity' });
    const second = makeTool({ id: 'antigravity-2' });
    const third = makeTool({ id: 'antigravity-3' });

    const projection = projectToolRegistrations([first, second, third], []);

    assert.deepEqual(projection.tools, [first]);
    assert.equal(projection.duplicates.length, 1);
    assert.equal(projection.duplicates[0]?.primary, first);
    assert.deepEqual(projection.duplicates[0]?.duplicates, [second, third]);
  });

  it('leaves the entire equivalence set visible when two records own models', () => {
    const first = makeTool({ id: 'antigravity' });
    const second = makeTool({ id: 'antigravity-2' });
    const emptyThird = makeTool({ id: 'antigravity-3' });
    const models = [
      makeModel({ id: 'm1', tool_id: 'antigravity' }),
      makeModel({ id: 'm2', tool_id: 'antigravity-2' }),
    ];

    const projection = projectToolRegistrations([first, second, emptyThird], models);

    assert.deepEqual(projection.tools, [first, second, emptyThird]);
    assert.deepEqual(projection.duplicates, []);
  });

  it('keeps tools with different adapters, custom paths, labels, or enabled states separate', () => {
    const base = makeTool({ id: 'antigravity' });
    const diffAdapter = makeTool({ id: 'opencode', label: 'Antigravity', adapter: 'opencode_v2' });
    const customPathA = makeTool({ id: 'agy-custom-a', executable_path: '/opt/agy-a/bin/agy' });
    const customPathB = makeTool({ id: 'agy-custom-b', executable_path: '/opt/agy-b/bin/agy' });
    const diffLabel = makeTool({ id: 'agy-canary', label: 'Antigravity Canary' });
    const disabled = makeTool({ id: 'agy-disabled', enabled: false });

    const tools = [base, diffAdapter, customPathA, customPathB, diffLabel, disabled];
    const projection = projectToolRegistrations(tools, ANTIGRAVITY_RAW_MODELS);

    assert.deepEqual(projection.tools, tools);
    assert.deepEqual(projection.duplicates, []);
    assert.equal(findEquivalentTool(diffAdapter, [base]), null);
    assert.equal(findEquivalentTool(customPathA, [base, customPathB]), null);
    assert.equal(findEquivalentTool(diffLabel, [base]), null);
    assert.equal(findEquivalentTool(disabled, [base]), null);
  });

  it('normalizes label case/whitespace and equates null with blank executable_path as system selection', () => {
    const systemNull = makeTool({
      id: 'antigravity',
      label: 'Antigravity',
      executable_path: null,
    });
    const systemBlank = makeTool({
      id: 'antigravity-2',
      label: '  antigravity  ',
      executable_path: '   ',
    });

    const projection = projectToolRegistrations([systemNull, systemBlank], ANTIGRAVITY_RAW_MODELS);

    assert.deepEqual(projection.tools, [systemNull]);
    assert.equal(projection.duplicates.length, 1);
    assert.equal(projection.duplicates[0]?.primary, systemNull);
    assert.deepEqual(projection.duplicates[0]?.duplicates, [systemBlank]);
    assert.equal(findEquivalentTool(systemBlank, [systemNull, systemBlank]), systemNull);
  });

  it('never groups API adapters or returns an equivalent match for API tools', () => {
    const jevA = makeTool({ id: 'jev-1', label: 'JEV API', adapter: 'jev_api' });
    const jevB = makeTool({ id: 'jev-2', label: 'JEV API', adapter: 'jev_api' });
    const openAiA = makeTool({
      id: 'openai-1',
      label: 'OpenAI Decisions',
      adapter: 'openai_decisions_api',
    });
    const openAiB = makeTool({
      id: 'openai-2',
      label: 'OpenAI Decisions',
      adapter: 'openai_decisions_api',
    });

    const tools = [jevA, jevB, openAiA, openAiB];
    const projection = projectToolRegistrations(tools, []);

    assert.deepEqual(projection.tools, tools);
    assert.deepEqual(projection.duplicates, []);
    assert.equal(findEquivalentTool(jevB, tools), null);
    assert.equal(findEquivalentTool(openAiB, tools), null);
  });

  it('excludes candidate.id when finding an equivalent tool for validation', () => {
    const existing = makeTool({ id: 'antigravity' });
    const draft = makeTool({ id: 'antigravity-new', label: ' antigravity ', executable_path: '' });

    assert.equal(findEquivalentTool(existing, [existing]), null);
    assert.equal(findEquivalentTool(draft, [existing]), existing);
  });

  it('preserves object identity, never mutates inputs, and produces deterministic projections', () => {
    const antigravity = Object.freeze(makeTool({ id: 'antigravity' }));
    const replica = Object.freeze(makeTool({ id: 'antigravity-2' }));
    const codex = Object.freeze(
      makeTool({ id: 'codex', label: 'Codex CLI', adapter: 'codex_cli' }),
    );
    const tools = Object.freeze([antigravity, replica, codex]);
    const models = ANTIGRAVITY_RAW_MODELS;

    const firstRun = projectToolRegistrations(tools, models);
    const secondRun = projectToolRegistrations(tools, models);

    assert.deepEqual(firstRun, secondRun);
    assert.equal(firstRun.tools[0], antigravity);
    assert.equal(firstRun.tools[1], codex);
    assert.equal(firstRun.duplicates[0]?.primary, antigravity);
    assert.equal(firstRun.duplicates[0]?.duplicates[0], replica);
  });
});
