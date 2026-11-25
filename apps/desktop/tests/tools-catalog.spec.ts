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

test('Refresh model catalog on expanded tool card recovers from error, loads 6.2 options, and keeps pending selections and saved models', async ({
  page,
}) => {
  const registry = buildDefaultRegistryFixture();
  const olderModel = model('gpt-6.1-sol', 'GPT-6.1-Sol');
  const vanishedModel = model('gpt-6.1-temp', 'GPT-6.1-Temp');
  const sol = model('gpt-6.2-sol', 'GPT-6.2-Sol');
  const astra = model('gpt-6.2-astra', 'GPT-6.2-Astra');
  const controller = await installWorkspaceTauriFixture(page, {
    registrySnapshot: registry,
    toolProbeReplies: {
      [codexTool]: [
        { error: { code: 'probe_failed', message: 'Codex model catalog command failed.' } },
        probe([olderModel, vanishedModel]),
        probe([olderModel, sol, astra]),
      ],
    },
  });
  await page.setViewportSize({ width: 1040, height: 760 });
  await page.goto('/');
  await sidebar(page).getByRole('button', { name: 'Tools', exact: true }).click();

  const tools = page.getByRole('region', { name: 'Tools', exact: true });
  await expect(
    page.getByRole('button', { name: 'Refresh saved registry', exact: true })
  ).toBeVisible();
  await expect(
    tools.getByRole('button', { name: 'Refresh tool discovery', exact: true })
  ).toBeVisible();
  await expect(
    tools.getByRole('button', { name: /^(?:Recheck|Check) installed tool versions$/ })
  ).toBeVisible();

  const openCodex = tools.getByRole('button', { name: 'Choose models', exact: true }).first();
  await expect(openCodex).toBeVisible();
  const catalogCalls = async () =>
    (await controller.getInvocations())
      .filter((invocation) => invocation.cmd === 'desktop_probe_tool')
      .map((invocation) => invocation.args.toolId);
  expect(await catalogCalls()).toEqual([]);

  await openCodex.click();
  const picker = tools.getByRole('region', { name: 'Choose models for Codex CLI' });
  await expect(picker).toBeVisible();
  await expect(picker.getByText('Codex model catalog command failed.')).toBeVisible();

  // Single explicit Refresh model catalog action on the expanded card (no duplicate header refresh)
  const refreshCatalog = picker.getByRole('button', { name: 'Refresh model catalog', exact: true });
  await expect(refreshCatalog).toHaveCount(1);
  await expect(tools.getByRole('button', { name: 'Refresh models', exact: true })).toHaveCount(0);

  // Trigger Refresh model catalog via keyboard after error
  await refreshCatalog.focus();
  await page.keyboard.press('Enter');

  const keptSelection = picker.getByRole('checkbox', { name: 'Select GPT-6.1-Sol for import' });
  const droppedSelection = picker.getByRole('checkbox', { name: 'Select GPT-6.1-Temp for import' });
  await expect(keptSelection).toBeVisible();
  await expect(droppedSelection).toBeVisible();
  await keptSelection.check();
  await droppedSelection.check();
  await expect(keptSelection).toBeChecked();
  await expect(droppedSelection).toBeChecked();

  // Single Refresh model catalog action remains available after success and works via keyboard
  await expect(refreshCatalog).toHaveCount(1);
  await refreshCatalog.focus();
  await page.keyboard.press('Enter');

  await expect(picker.getByRole('checkbox', { name: 'Select GPT-6.2-Sol for import' })).toBeVisible();
  await expect(picker.getByRole('checkbox', { name: 'Select GPT-6.2-Astra for import' })).toBeVisible();
  await expect(keptSelection).toBeChecked();
  await expect(refreshCatalog).toBeFocused();
  await expect(droppedSelection).toHaveCount(0);

  // Saved models and profile overrides remain intact
  await expect(page.getByRole('region', { name: 'Saved models', exact: true }).getByRole('heading', { name: 'GPT-5.4', exact: true })).toBeVisible();
  expect(await catalogCalls()).toEqual([codexTool, codexTool, codexTool]);
  expect(await controller.getRegistrySnapshot()).toEqual(registry);
});
