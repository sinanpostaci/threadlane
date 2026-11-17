import { expect, test } from '@playwright/test';
import type { ModelDefinition, ToolProbe } from '../src/registry/types.ts';
import {
  buildDefaultRegistryFixture,
  installWorkspaceTauriFixture,
  sidebar,
} from './fixtures/workspace.ts';

const codexTool = 'tool-codex';

function model(id: string, label: string): ModelDefinition {
  return {
    id,
    tool_id: codexTool,
    label,
    model_arg: id,
    efforts: ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
    allow_no_effort: true,
  };
}

function probe(models: ModelDefinition[]): ToolProbe {
  return {
    tool_id: codexTool,
    availability: 'found',
    version: '0.169.0',
    auth_status: 'authenticated',
    models,
    message: 'Catalog metadata loaded.',
  };
}

test('Refresh tools replaces an open stale or failed catalog without changing saved profiles', async ({
  page,
}) => {
  const registry = buildDefaultRegistryFixture();
  const olderModel = model('gpt-6.1-sol', 'GPT-6.1-Sol');
  const sol = model('gpt-6.2-sol', 'GPT-6.2-Sol');
  const astra = model('gpt-6.2-astra', 'GPT-6.2-Astra');
  const controller = await installWorkspaceTauriFixture(page, {
    registrySnapshot: registry,
    toolProbeReplies: {
      [codexTool]: [
        { error: { code: 'probe_failed', message: 'Codex model catalog command failed.' } },
        probe([olderModel]),
        probe([olderModel, sol, astra]),
      ],
    },
  });
  await page.setViewportSize({ width: 1040, height: 760 });
  await page.goto('/');
  await sidebar(page).getByRole('button', { name: 'Tools', exact: true }).click();

  const tools = page.getByRole('region', { name: 'Tools', exact: true });
  const openCodex = tools.getByRole('button', { name: 'Choose models', exact: true }).first();
  await expect(openCodex).toBeVisible();
  const catalogCalls = async () =>
    (await controller.getInvocations())
      .filter((invocation) => invocation.cmd === 'desktop_probe_tool')
      .map((invocation) => invocation.args.toolId);
  expect(await catalogCalls()).toEqual([]);

  await openCodex.click();
  await expect(tools.getByRole('button', { name: 'Retry catalog' })).toBeVisible();
  const refresh = tools.locator('[data-focus="discovery-refresh"]');
  await refresh.click();
  const checked = tools.getByRole('checkbox', { name: 'Select GPT-6.1-Sol for import' });
  await expect(checked).toBeVisible();
  await checked.check();
  await refresh.click();

  await expect(tools.getByRole('checkbox', { name: 'Select GPT-6.2-Sol for import' })).toBeVisible();
  await expect(tools.getByRole('checkbox', { name: 'Select GPT-6.2-Astra for import' })).toBeVisible();
  await expect(checked).toBeChecked();
  expect(await catalogCalls()).toEqual([codexTool, codexTool, codexTool]);
  expect(await controller.getRegistrySnapshot()).toEqual(registry);
});
