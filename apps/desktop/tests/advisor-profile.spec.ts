import { expect, test, type Page } from '@playwright/test';

import type {
  ModelChoice,
  RegistryChange,
  RegistrySnapshot,
  RoleAssignment,
  WorkflowProfile,
} from '../src/registry/types.ts';
import {
  buildDefaultRegistryFixture,
  buildMinimalChatWorkspaceSnapshot,
  chatHeader,
  installWorkspaceTauriFixture,
  selectChat,
  sidebar,
  type WorkspaceTauriController,
} from './fixtures/workspace.ts';

const OTHER_ROLE_ORDER = ['orchestrator', 'coder', 'reviewer', 'closure'] as const;

const OTHER_ROLE_CHOICES: Record<(typeof OTHER_ROLE_ORDER)[number], ModelChoice> = {
  orchestrator: { model_id: 'model-gpt-5-4', effort: 'xhigh' },
  coder: { model_id: 'model-deepseek-flash', effort: 'max' },
  reviewer: { model_id: 'model-argon-high', effort: 'high' },
  closure: { model_id: 'model-gpt-5-4', effort: 'medium' },
};

const MODEL_LABELS: Record<string, string> = {
  'model-argon-high': 'Gemini 4 Argon High',
  'model-deepseek-flash': 'DeepSeek v4.1 Flash',
  'model-gpt-5-4': 'GPT-5.4',
};

interface ExpectedEdit {
  defaultChoice: ModelChoice;
  hardChoice: ModelChoice;
}

const EXPECTED_EDITS: Record<'legacy' | 'advisor', ExpectedEdit> = {
  legacy: {
    defaultChoice: { model_id: 'model-deepseek-flash', effort: 'max' },
    hardChoice: { model_id: 'model-gpt-5-4', effort: 'xhigh' },
  },
  advisor: {
    defaultChoice: { model_id: 'model-gpt-5-4', effort: 'high' },
    hardChoice: { model_id: 'model-deepseek-flash', effort: 'max' },
  },
};

function otherRoleDefaults(): RoleAssignment[] {
  return OTHER_ROLE_ORDER.map((role) => ({
    role,
    difficulty: null,
    choice: { ...OTHER_ROLE_CHOICES[role] },
  }));
}

function buildAdvisorRegistryFixture(): {
  registry: RegistrySnapshot;
  legacyProfile: WorkflowProfile;
  advisorProfile: WorkflowProfile;
} {
  const legacyProfile: WorkflowProfile = {
    id: 'profile-legacy-advisor-role',
    name: 'Legacy Architect Profile',
    assignments: [
      ...otherRoleDefaults(),
      {
        role: 'architect',
        difficulty: null,
        choice: { model_id: 'model-argon-high', effort: 'high' },
      },
      {
        role: 'architect',
        difficulty: 'hard',
        choice: { model_id: 'model-deepseek-flash', effort: 'max' },
      },
    ],
  };
  const advisorProfile: WorkflowProfile = {
    id: 'profile-current-advisor-role',
    name: 'Current Advisor Profile',
    assignments: [
      ...otherRoleDefaults(),
      {
        role: 'advisor',
        difficulty: null,
        choice: { model_id: 'model-deepseek-flash', effort: 'max' },
      },
      {
        role: 'advisor',
        difficulty: 'hard',
        choice: { model_id: 'model-argon-high', effort: 'high' },
      },
    ],
  };
  return {
    registry: {
      ...buildDefaultRegistryFixture(),
      profiles: [legacyProfile, advisorProfile],
      default_profile_id: legacyProfile.id,
    },
    legacyProfile,
    advisorProfile,
  };
}

function profileSection(page: Page) {
  return page.locator('section[aria-label="Profiles"]').first();
}

function profileRow(page: Page, profileName: string) {
  return profileSection(page)
    .locator('button.tl-profiles-role-row-btn')
    .filter({ hasText: profileName })
    .first();
}

async function selectProfile(page: Page, profileName: string): Promise<void> {
  await profileRow(page, profileName).click();
}

async function selectAdvisorRole(page: Page, compact: boolean): Promise<void> {
  if (compact) {
    const roleSelect = page.getByRole('combobox', { name: 'Workflow role' });
    await roleSelect.click();
    const options = page.getByRole('option');
    await expect(options).toHaveCount(5);
    await expect(page.getByRole('option', { name: 'Advisor', exact: true })).toHaveCount(1);
    await page.getByRole('option', { name: 'Advisor', exact: true }).click();
    await expect(roleSelect).toHaveText('Advisor');
    return;
  }

  const roleGroup = profileSection(page).getByRole('group', {
    name: 'Execution roles',
  });
  const roleButtons = roleGroup.getByRole('button');
  await expect(roleButtons).toHaveCount(5);
  await expect(roleGroup.getByText('Advisor', { exact: true })).toHaveCount(1);
  await roleGroup.getByRole('button', { name: /Advisor/ }).click();
}

async function chooseSelectOption(
  page: Page,
  triggerName: string,
  optionName: string,
): Promise<void> {
  const trigger = page.getByRole('combobox', { name: triggerName });
  await trigger.click();
  const option = page.getByRole('option', { name: optionName, exact: true });
  await expect(option).toBeVisible();
  await option.click();
  await expect(trigger).toHaveText(optionName);
}

async function openHardOverride(page: Page, compact: boolean): Promise<void> {
  if (!compact) {
    return;
  }
  const trigger = page.getByRole('button', { name: 'Hard difficulty' });
  if ((await trigger.getAttribute('aria-expanded')) !== 'true') {
    await trigger.click();
  }
  await expect(trigger).toHaveAttribute('aria-expanded', 'true');
}

async function expectChoice(
  page: Page,
  triggerName: string,
  label: ModelChoice,
): Promise<void> {
  const modelLabel = MODEL_LABELS[label.model_id];
  if (!modelLabel) {
    throw new Error(`Missing fixture label for model ${label.model_id}`);
  }
  await expect(page.getByRole('combobox', { name: triggerName })).toHaveText(modelLabel);
  const effortName =
    triggerName === 'Default model'
      ? 'Reasoning effort'
      : triggerName.replace(/model$/, 'effort');
  await expect(page.getByRole('combobox', { name: effortName })).toHaveText(
    label.effort ?? 'No effort',
  );
}

async function editAdvisorChoices(
  page: Page,
  compact: boolean,
  expected: ExpectedEdit,
): Promise<void> {
  const defaultModelLabel = MODEL_LABELS[expected.defaultChoice.model_id]!;
  await chooseSelectOption(page, 'Default model', defaultModelLabel);
  await chooseSelectOption(page, 'Reasoning effort', expected.defaultChoice.effort!);

  await openHardOverride(page, compact);
  const hardModelLabel = MODEL_LABELS[expected.hardChoice.model_id]!;
  await chooseSelectOption(page, 'Hard override model', hardModelLabel);
  await chooseSelectOption(page, 'Hard override effort', expected.hardChoice.effort!);
}

function assignmentsBySlot(
  assignments: readonly RoleAssignment[],
): Map<string, ModelChoice> {
  return new Map(
    assignments.map((assignment) => [
      `${assignment.role}:${assignment.difficulty ?? 'default'}`,
      assignment.choice,
    ]),
  );
}

async function latestRegistryChange(
  controller: WorkspaceTauriController,
): Promise<RegistryChange> {
  await expect
    .poll(async () => {
      const invocations = await controller.getInvocations();
      return invocations.filter((invocation) => invocation.cmd === 'desktop_registry_apply')
        .length;
    })
    .toBeGreaterThan(0);
  const invocations = await controller.getInvocations();
  const apply = [...invocations]
    .reverse()
    .find((invocation) => invocation.cmd === 'desktop_registry_apply');
  if (!apply) {
    throw new Error('Expected a captured desktop_registry_apply invocation.');
  }
  return apply.args.change as RegistryChange;
}

async function expectAdvisorEditorChoices(
  page: Page,
  compact: boolean,
  expected: ExpectedEdit,
): Promise<void> {
  await selectAdvisorRole(page, compact);
  await expectChoice(page, 'Default model', expected.defaultChoice);
  await openHardOverride(page, compact);
  await expectChoice(page, 'Hard override model', expected.hardChoice);
}

async function reloadProfiles(
  page: Page,
  compact: boolean,
  profileName: string,
): Promise<void> {
  if (compact) {
    await profileSection(page)
      .getByRole('button', { name: 'Back to profiles' })
      .click();
  }
  await profileSection(page).getByRole('button', { name: 'Reload', exact: true }).click();
  await expect(
    profileSection(page).getByText('Loaded the latest registry.'),
  ).toBeVisible();
  if (compact) {
    await selectProfile(page, profileName);
  }
}

async function expectRawSeedPreserved(
  controller: WorkspaceTauriController,
  profile: WorkflowProfile,
): Promise<void> {
  const rawRegistry = await controller.getRegistrySnapshot();
  expect(rawRegistry).not.toBeNull();
  const rawProfile = rawRegistry!.profiles.find((item) => item.id === profile.id);
  expect(rawProfile).toBeDefined();
  expect(rawProfile!.assignments).toEqual(profile.assignments);
}

async function exerciseProfile(
  page: Page,
  controller: WorkspaceTauriController,
  compact: boolean,
  profile: WorkflowProfile,
  profileKind: 'legacy' | 'advisor',
): Promise<void> {
  const expected = EXPECTED_EDITS[profileKind];
  if (compact) {
    const back = profileSection(page).getByRole('button', {
      name: 'Back to profiles',
    });
    if ((await back.count()) > 0) {
      await back.click();
      await expect(profileRow(page, profile.name)).toBeVisible();
    }
  }
  await selectProfile(page, profile.name);

  // Merely opening and editing the profile must not rewrite the incoming raw
  // record, including its frozen legacy architect spelling.
  await expectRawSeedPreserved(controller, profile);
  await expectAdvisorEditorChoices(page, compact, {
    defaultChoice:
      profileKind === 'legacy'
        ? { model_id: 'model-argon-high', effort: 'high' }
        : { model_id: 'model-deepseek-flash', effort: 'max' },
    hardChoice:
      profileKind === 'legacy'
        ? { model_id: 'model-deepseek-flash', effort: 'max' }
        : { model_id: 'model-argon-high', effort: 'high' },
  });
  await expectRawSeedPreserved(controller, profile);

  await editAdvisorChoices(page, compact, expected);
  await expectRawSeedPreserved(controller, profile);

  const save = profileSection(page).getByRole('button', { name: 'Save changes' });
  await expect(save).toBeEnabled();
  await save.click();
  await expect(
    profileSection(page).getByText(`"${profile.name}" saved.`),
  ).toBeVisible();

  const change = await latestRegistryChange(controller);
  if (change.op !== 'upsert_profile') {
    throw new Error(`Expected upsert_profile, received ${change.op}`);
  }
  if (profileKind === 'legacy') {
    expect(profile.assignments.some((assignment) => assignment.role === 'architect')).toBe(
      true,
    );
  }
  expect(change.profile.id).toBe(profile.id);
  const outgoing = assignmentsBySlot(change.profile.assignments);
  expect(change.profile.assignments.some((assignment) => assignment.role === 'architect')).toBe(
    false,
  );
  expect([...new Set(change.profile.assignments.map((assignment) => assignment.role))]).toEqual([
    'orchestrator',
    'advisor',
    'coder',
    'reviewer',
    'closure',
  ]);
  expect(
    change.profile.assignments.filter((assignment) => assignment.role === 'advisor'),
  ).toHaveLength(2);
  expect(outgoing.get('advisor:default')).toEqual(expected.defaultChoice);
  expect(outgoing.get('advisor:hard')).toEqual(expected.hardChoice);
  for (const role of OTHER_ROLE_ORDER) {
    expect(outgoing.get(`${role}:default`)).toEqual(OTHER_ROLE_CHOICES[role]);
  }

  const savedRegistry = await controller.getRegistrySnapshot();
  const savedProfile = savedRegistry!.profiles.find((item) => item.id === profile.id);
  expect(savedProfile).toBeDefined();
  expect(savedProfile!.assignments.some((assignment) => assignment.role === 'architect')).toBe(
    false,
  );
  expect(assignmentsBySlot(savedProfile!.assignments).get('advisor:default')).toEqual(
    expected.defaultChoice,
  );
  expect(assignmentsBySlot(savedProfile!.assignments).get('advisor:hard')).toEqual(
    expected.hardChoice,
  );

  await reloadProfiles(page, compact, profile.name);
  await expect(profileSection(page)).not.toContainText('Unsaved changes');
  await expect(profileSection(page)).not.toContainText(
    'This profile changed after your draft started',
  );
  await expect(profileSection(page)).not.toContainText(
    'This profile was removed from the latest registry',
  );
  await expectAdvisorEditorChoices(page, compact, expected);
}

test.describe('Advisor profile workflow', () => {
  test('edits legacy and current advisor profiles at 1440px and saves advisor IPC roles', async ({
    page,
  }) => {
    const { registry, legacyProfile, advisorProfile } = buildAdvisorRegistryFixture();
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshot(),
      registrySnapshot: registry,
    });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await sidebar(page).getByRole('button', { name: 'Profiles', exact: true }).click();
    await expect(profileSection(page)).toBeVisible();
    await expect(profileRow(page, legacyProfile.name)).toBeVisible();

    await expectRawSeedPreserved(controller, legacyProfile);
    await expectRawSeedPreserved(controller, advisorProfile);
    await exerciseProfile(page, controller, false, legacyProfile, 'legacy');
    await expectRawSeedPreserved(controller, advisorProfile);
    await exerciseProfile(page, controller, false, advisorProfile, 'advisor');
  });

  test('edits legacy and current advisor profiles at 390px and reloads without false warnings', async ({
    page,
  }) => {
    const { registry, legacyProfile, advisorProfile } = buildAdvisorRegistryFixture();
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshot(),
      registrySnapshot: registry,
    });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await page.setViewportSize({ width: 390, height: 740 });
    await page.getByRole('button', { name: 'Open navigation drawer' }).click();
    await page
      .getByRole('dialog', { name: /navigation|projects|workspace/i })
      .getByRole('button', { name: 'Profiles', exact: true })
      .click();
    await expect(profileSection(page)).toBeVisible();
    await expect(profileRow(page, legacyProfile.name)).toBeVisible();

    await expectRawSeedPreserved(controller, legacyProfile);
    await expectRawSeedPreserved(controller, advisorProfile);
    await exerciseProfile(page, controller, true, legacyProfile, 'legacy');
    await expectRawSeedPreserved(controller, advisorProfile);
    await exerciseProfile(page, controller, true, advisorProfile, 'advisor');
  });
});

test.describe('Profile readiness and multi-role separation', () => {
  test('separates single-Worker readiness from optional multi-role completeness and preserves draft and decision selection', async ({
    page,
  }) => {
    const base = buildDefaultRegistryFixture();
    const singleWorkerProfile: WorkflowProfile = {
      id: 'profile-single-worker',
      name: 'Single Worker Profile',
      assignments: [
        {
          role: 'coder',
          difficulty: null,
          choice: { model_id: 'model-gpt-5-4', effort: 'high' },
        },
      ],
      decision_advisor: { model_id: 'model-jev-1-14', mode: 'shadow' },
    };
    const noWorkerProfile: WorkflowProfile = {
      id: 'profile-no-worker',
      name: 'Orchestrator Only Profile',
      assignments: [
        {
          role: 'orchestrator',
          difficulty: null,
          choice: { model_id: 'model-gpt-5-4', effort: 'xhigh' },
        },
      ],
    };
    const completeProfile: WorkflowProfile = {
      id: 'profile-complete',
      name: 'Complete Multi-Role Profile',
      assignments: [
        ...otherRoleDefaults(),
        {
          role: 'advisor',
          difficulty: null,
          choice: { model_id: 'model-argon-high', effort: 'high' },
        },
      ],
    };

    const registry: RegistrySnapshot = {
      ...base,
      tools: [
        ...base.tools,
        {
          id: 'tool-jev',
          label: 'Jev Decisions API',
          adapter: 'jev_api',
          executable_path: null,
          enabled: true,
        },
      ],
      models: [
        ...base.models,
        {
          id: 'model-jev-1-14',
          tool_id: 'tool-jev',
          label: 'Jev 1.14.0',
          model_arg: 'jev-1.14.0',
          efforts: [],
          allow_no_effort: true,
        },
      ],
      profiles: [singleWorkerProfile, noWorkerProfile, completeProfile],
      default_profile_id: singleWorkerProfile.id,
    };

    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshot(),
      registrySnapshot: registry,
    });
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/');
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await sidebar(page).getByRole('button', { name: 'Profiles', exact: true }).click();

    const profiles = profileSection(page);
    await expect(profiles).toBeVisible();

    const singleRow = profileRow(page, singleWorkerProfile.name);
    await expect(singleRow).toContainText('Single-Worker ready');
    await expect(singleRow).toContainText('Multi-role optional (1/5)');
    await expect(singleRow).not.toContainText('Needs repair');
    await expect(singleRow).not.toContainText('Missing defaults');

    const noWorkerRow = profileRow(page, noWorkerProfile.name);
    await expect(noWorkerRow).toContainText('Worker not configured');
    await expect(noWorkerRow).toContainText('Multi-role optional (1/5)');

    const completeRow = profileRow(page, completeProfile.name);
    await expect(completeRow).toContainText('Single-Worker ready');
    await expect(completeRow).toContainText('Multi-role complete (5/5)');

    // Keyboard select the single-Worker profile and verify separate readiness cards & advisor distinction
    await singleRow.focus();
    await page.keyboard.press('Enter');
    const readiness = profiles.getByRole('region', { name: 'Profile readiness' });
    await expect(readiness).toContainText('Ordinary single-Worker use');
    await expect(readiness).toContainText('Single-Worker ready');
    await expect(readiness).toContainText('does not verify live');
    await expect(readiness).toContainText('Optional multi-role workflows');
    await expect(readiness).toContainText('Multi-role optional (1/5)');

    const decisionSection = profiles.getByRole('region', { name: 'Decision advisor' });
    await expect(decisionSection).toContainText('Task-difficulty advice');
    await expect(decisionSection).toContainText('Separate from the technical Advisor execution role');
    await expect(
      profiles.getByRole('combobox', { name: 'Decision advisor API model' })
    ).toHaveText('Jev 1.14.0');

    // Unsaved draft and decision selection stay unaltered when cancelling a switch
    const nameInput = profiles.getByLabel('Profile name');
    await nameInput.fill('Single Worker Unsaved Draft');
    await noWorkerRow.click();
    await expect(
      profiles.getByRole('alertdialog', { name: 'Unsaved profile changes' })
    ).toBeVisible();
    await profiles.getByRole('button', { name: 'Stay here' }).click();
    await expect(nameInput).toHaveValue('Single Worker Unsaved Draft');
    await expect(
      profiles.getByRole('combobox', { name: 'Decision advisor API model' })
    ).toHaveText('Jev 1.14.0');
    await expect(
      profiles.getByRole('combobox', { name: 'Default model' })
    ).toHaveText('GPT-5.4');

    // Discard and inspect the no-Worker profile
    await noWorkerRow.click();
    await profiles.getByRole('button', { name: 'Discard changes' }).click();
    await expect(readiness).toContainText('Worker not configured');
    await expect(readiness).toContainText('Multi-role optional (1/5)');
  });
});
