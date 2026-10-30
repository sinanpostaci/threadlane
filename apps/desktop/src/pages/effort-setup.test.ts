import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  MAX_EFFORT_OPTIONS,
  normalizeDetectedEfforts,
  planDetectedEffortUpdates,
} from './effort-setup.ts';
import type { ModelDefinition } from '../registry/types.ts';

function makeModel(overrides: Partial<ModelDefinition> = {}): ModelDefinition {
  return {
    id: 'model-a',
    tool_id: 'tool-a',
    label: 'Model A',
    model_arg: 'provider/model-a',
    efforts: [],
    allow_no_effort: true,
    ...overrides,
  };
}

describe('detected effort setup', () => {
  it('fills empty saved effort metadata from an exact trimmed catalog ID match', () => {
    const saved = makeModel({
      id: 'model-a',
      label: 'Saved label',
      model_arg: 'provider/model-a',
      allow_no_effort: true,
    });
    const catalog = makeModel({
      id: ' model-a ',
      label: 'Catalog label',
      model_arg: 'provider/model-a',
      efforts: ['low', 'high', 'max'],
      allow_no_effort: false,
    });

    const plan = planDetectedEffortUpdates([saved], [catalog], 'tool-a');

    assert.equal(plan.updates.length, 1);
    assert.deepEqual(plan.updates[0], {
      id: 'model-a',
      tool_id: 'tool-a',
      label: 'Saved label',
      model_arg: 'provider/model-a',
      efforts: ['low', 'high', 'max'],
      allow_no_effort: true,
    });
  });

  it('preserves a saved disallow-no-effort choice', () => {
    const saved = makeModel({ allow_no_effort: false });
    const catalog = makeModel({ efforts: ['high'], allow_no_effort: true });

    const plan = planDetectedEffortUpdates([saved], [catalog], 'tool-a');

    assert.equal(plan.updates.length, 1);
    assert.equal(plan.updates[0].allow_no_effort, false);
  });

  it('never overwrites a nonempty customized effort list', () => {
    const saved = makeModel({ efforts: ['custom-high'] });
    const catalog = makeModel({ efforts: ['low', 'high', 'max'] });

    const plan = planDetectedEffortUpdates([saved], [catalog], 'tool-a');

    assert.deepEqual(plan.updates, []);
  });

  it('excludes foreign tools, foreign catalog entries, and model argument mismatches', () => {
    const savedForeignTool = makeModel({ id: 'foreign-saved', tool_id: 'tool-b' });
    const catalogForeignTool = makeModel({
      id: 'foreign-saved',
      tool_id: 'tool-b',
      efforts: ['high'],
    });
    const savedAlias = makeModel({ id: 'alias', model_arg: 'provider/model-alias' });
    const catalogAlias = makeModel({
      id: 'alias',
      model_arg: 'provider/model-a',
      efforts: ['high'],
    });
    const savedSameId = makeModel({ id: 'same-id', model_arg: 'provider/model-a' });
    const catalogOtherTool = makeModel({
      id: 'same-id',
      tool_id: 'tool-b',
      model_arg: 'provider/model-a',
      efforts: ['high'],
    });

    const plan = planDetectedEffortUpdates(
      [savedForeignTool, savedAlias, savedSameId],
      [catalogForeignTool, catalogAlias, catalogOtherTool],
      'tool-a'
    );

    assert.deepEqual(plan.updates, []);
  });

  it('drops invalid and duplicate detected effort tokens', () => {
    const saved = makeModel();
    const catalog = makeModel({
      efforts: [
        ' high ',
        'high',
        '',
        '   ',
        'bad\u0007token',
        'x'.repeat(65),
        'low',
        'max',
        'max',
      ],
    });

    const plan = planDetectedEffortUpdates([saved], [catalog], 'tool-a');

    assert.deepEqual(normalizeDetectedEfforts([42, null, undefined, 'high']), ['high']);
    assert.equal(plan.updates.length, 1);
    assert.deepEqual(plan.updates[0].efforts, ['high', 'low', 'max']);
  });

  it('caps normalized options at the registry per-model bound', () => {
    const detected = Array.from(
      { length: MAX_EFFORT_OPTIONS + 3 },
      (_, index) => `effort-${index}`
    );

    assert.deepEqual(
      normalizeDetectedEfforts(detected),
      detected.slice(0, MAX_EFFORT_OPTIONS)
    );
  });

  it('returns no updates and leaves inputs unchanged when nothing is eligible', () => {
    const savedCustom = makeModel({ id: 'custom', efforts: ['high'] });
    const savedEmpty = makeModel({ id: 'empty' });
    const catalogCustom = makeModel({ id: 'custom', efforts: ['low', 'high'] });
    const catalogNoEfforts = makeModel({ id: 'empty', efforts: [] });
    const savedBefore = JSON.stringify([savedCustom, savedEmpty]);
    const catalogBefore = JSON.stringify([catalogCustom, catalogNoEfforts]);

    const plan = planDetectedEffortUpdates(
      [savedCustom, savedEmpty],
      [catalogCustom, catalogNoEfforts],
      'tool-a'
    );

    assert.deepEqual(plan.updates, []);
    assert.equal(JSON.stringify([savedCustom, savedEmpty]), savedBefore);
    assert.equal(JSON.stringify([catalogCustom, catalogNoEfforts]), catalogBefore);
  });
});
