import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { catalogProvider, catalogProviderOptions, matchesCatalogProvider, modelCatalogProvider } from './catalog-providers.ts';

describe('OpenCode model catalog providers', () => {
  it('distinguishes identical model names by their exact execution routes', () => {
    assert.equal(modelCatalogProvider('opencode-go/deepseek-v4.1-flash'), 'opencode-go');
    assert.equal(modelCatalogProvider('opencode/deepseek-v4.1-flash'), 'opencode');
    assert.equal(matchesCatalogProvider('opencode/deepseek-v4.1-flash', 'opencode-go'), false);
    assert.equal(matchesCatalogProvider('opencode-go/deepseek-v4.1-flash', 'opencode-go'), true);
  });

  it('keeps lookalike provider prefixes and invalid references outside Go', () => {
    for (const argument of ['opencode-go-fake/model', 'openai/opencode-go/model', 'opencode-go', 'opencode-go/', ' opencode-go/model']) {
      assert.equal(modelCatalogProvider(argument), 'other');
    }
  });

  it('counts each route separately without rewriting the source models', () => {
    const models = [
      { model_arg: 'opencode-go/shared-model' },
      { model_arg: 'opencode/shared-model' },
      { model_arg: 'opencode-go/second-model' },
      { model_arg: 'openai/third-model' },
    ];
    const before = JSON.stringify(models);
    assert.deepEqual(catalogProviderOptions(models), [
      { value: 'all', label: 'All providers (4)' },
      { value: 'opencode-go', label: 'OpenCode Go (2)' },
      { value: 'opencode', label: 'OpenCode API (1)' },
      { value: 'other', label: 'Other providers (1)' },
    ]);
    assert.equal(JSON.stringify(models), before);
  });

  it('shows a missing Go catalog explicitly rather than substituting API models', () => {
    const models = [{ model_arg: 'opencode/shared-model' }];
    assert.equal(catalogProviderOptions(models)[1].label, 'OpenCode Go (0)');
    assert.deepEqual(models.filter(m => matchesCatalogProvider(m.model_arg, 'opencode-go')), []);
    assert.deepEqual(catalogProviderOptions([]), [
      { value: 'all', label: 'All providers (0)' },
      { value: 'opencode-go', label: 'OpenCode Go (0)' },
      { value: 'opencode', label: 'OpenCode API (0)' },
    ]);
  });

  it('defaults an unknown filter to the complete catalog', () => {
    assert.equal(catalogProvider('unknown'), 'all');
    assert.equal(catalogProvider('opencode-go'), 'opencode-go');
    assert.equal(matchesCatalogProvider('openai/model', 'all'), true);
  });
});
