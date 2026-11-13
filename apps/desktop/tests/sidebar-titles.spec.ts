import { expect, test, type Locator, type Page } from '@playwright/test';

import {
  buildInteractiveWorkspaceSnapshot,
  CHAT_ALPHA_IDENTITY,
  CHAT_ALPHA_KEY,
  CHAT_BETA_SAME_PROJECT_IDENTITY,
  installWorkspaceTauriFixture,
} from './fixtures/workspace.ts';
import type {
  WorkspaceChatSummary,
  WorkspaceNativeSample,
  WorkspaceSnapshot,
} from '../src/workspace-contract.ts';

const ALPHA_TITLE =
  'Investigate truncated sidebar titles across wide and compact drawer layouts';
const BETA_TITLE =
  'Verify archive restore keyboard and touch access without collapsing labels';

/**
 * Alpha intentionally has no native sample, job history, or messages so its
 * updatedAtMs is genuinely unavailable. Beta keeps one real idle native sample
 * so the known-age placement is exercised without fabricating waiting state.
 */
function buildSidebarTitleSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  const alphaChat = base.chats.find(
    (chat) => chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id
  );
  const betaChat = base.chats.find(
    (chat) =>
      chat.identity.binding_id === CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id
  );
  const betaSample = base.native_samples.find(
    (sample) =>
      sample.chat_identity.binding_id ===
      CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id
  );
  if (!alphaChat || !betaChat || !betaSample) {
    throw new Error(
      'Expected Alpha and Beta chats plus a Beta native sample in the interactive workspace fixture.'
    );
  }

  const idleChat = (
    chat: WorkspaceChatSummary,
    threadLabel: string
  ): WorkspaceChatSummary => ({
    ...chat,
    thread_label: threadLabel,
    availability: 'available',
    activity: 'idle',
    active_job_count: 0,
    attention_count: 0,
    updated_at_ms: null,
  });

  const idleSample = (
    sample: WorkspaceNativeSample
  ): WorkspaceNativeSample => ({
    ...sample,
    availability: 'available',
    activity: 'idle',
  });

  return {
    ...base,
    observation: {
      ...base.observation,
      jobs: [],
      requests: [],
      next_cursor: null,
      has_more: false,
      jobs_has_more: false,
    },
    chats: [
      idleChat(alphaChat, ALPHA_TITLE),
      idleChat(betaChat, BETA_TITLE),
    ],
    native_samples: [idleSample(betaSample)],
    job_histories: [],
    messages: {
      ...base.messages,
      items: [],
      omitted_count: 0,
      has_more: false,
      truncated: false,
    },
    chat_work: [],
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
  };
}

function chatLabel(container: Locator, title: string): Locator {
  return container
    .locator('.tl-chat-item-label')
    .filter({ hasText: title })
    .first();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function expectReadableTitle(
  container: Locator,
  title: string
): Promise<Locator> {
  const label = chatLabel(container, title);
  await expect(label).toBeVisible();
  await expect(label).toHaveText(title);
  await expect
    .poll(async () => (await label.boundingBox())?.width ?? 0)
    .toBeGreaterThanOrEqual(120);
  return label;
}

async function expectAgeInMeta(label: Locator): Promise<void> {
  const placement = await label.evaluate((labelEl) => {
    const item = labelEl.closest('.tl-chat-item');
    const top = item?.querySelector('.tl-chat-item-top');
    const meta = item?.querySelector('.tl-chat-item-meta');
    const age = item?.querySelector('.tl-chat-item-age');
    return {
      ageText: age?.textContent?.trim() ?? null,
      ageInTop: Boolean(age && top?.contains(age)),
      ageInMeta: Boolean(age && meta?.contains(age)),
    };
  });
  expect(placement.ageText).not.toBeNull();
  expect(placement.ageText).not.toBe('Time not reported');
  expect(placement.ageInTop).toBe(false);
  expect(placement.ageInMeta).toBe(true);
}

async function expectNoUnknownTimeAge(label: Locator): Promise<void> {
  const placement = await label.evaluate((labelEl) => {
    const item = labelEl.closest('.tl-chat-item');
    return {
      hasAge: Boolean(item?.querySelector('.tl-chat-item-age')),
      text: item?.textContent ?? '',
    };
  });
  expect(placement.hasAge).toBe(false);
  expect(placement.text).not.toContain('Time not reported');
}

async function expectFullChatIdentity(
  container: Locator,
  title: string
): Promise<void> {
  const chatButton = container
    .locator('.tl-chat-item')
    .filter({ hasText: title })
    .first();
  await expect(chatButton).toHaveAccessibleName(
    new RegExp(escapeRegExp(title))
  );

  const archiveButton = container.getByRole('button', {
    name: `Archive ${title}`,
    exact: true,
  });
  await expect(archiveButton).toHaveCount(1);
  await expect(archiveButton).toHaveAccessibleName(`Archive ${title}`);
  await expect(chatLabel(container, title)).not.toHaveAttribute('title', /.+/);
}

async function expectNoHorizontalOverflow(
  page: Page,
  container: Locator
): Promise<void> {
  const documentMetrics = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    innerWidth: window.innerWidth,
  }));
  expect(documentMetrics.scrollWidth).toBeLessThanOrEqual(
    documentMetrics.innerWidth
  );

  const containerMetrics = await container.evaluate((el) => ({
    scrollWidth: el.scrollWidth,
    clientWidth: el.clientWidth,
    rowsFit: Array.from(el.querySelectorAll('.tl-chat-item-row')).every(
      (row) => row.scrollWidth <= row.clientWidth + 1
    ),
  }));
  expect(containerMetrics.scrollWidth).toBeLessThanOrEqual(
    containerMetrics.clientWidth
  );
  expect(containerMetrics.rowsFit).toBe(true);
}

test.describe('wide sidebar chat titles', () => {
  test('keeps long titles readable, places known age outside the title row, and preserves keyboard Archive/Restore', async ({
    page,
  }) => {
    const snapshot = buildSidebarTitleSnapshot();
    expect(snapshot.chats.map((chat) => chat.thread_label)).toEqual([
      ALPHA_TITLE,
      BETA_TITLE,
    ]);
    expect(snapshot.chats.map((chat) => chat.updated_at_ms)).toEqual([
      null,
      null,
    ]);
    expect(snapshot.chats.map((chat) => chat.activity)).toEqual([
      'idle',
      'idle',
    ]);
    expect(snapshot.native_samples.map((sample) => sample.activity)).toEqual([
      'idle',
    ]);
    expect(snapshot.native_samples[0]?.availability).toBe('available');
    expect(snapshot.native_samples[0]?.last_success_at_ms).not.toBeNull();
    expect(snapshot.native_samples[0]?.chat_identity.binding_id).toBe(
      CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id
    );
    expect(snapshot.observation.jobs).toHaveLength(0);
    expect(snapshot.observation.requests).toHaveLength(0);
    expect(snapshot.job_histories).toHaveLength(0);

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: snapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await expect(
      page.locator('#app .tl-app-shell, #app .tl-app').first()
    ).toBeVisible();

    const sidebar = page.locator('aside.tl-sidebar').first();
    await expect(sidebar).toBeVisible();

    const alphaLabel = await expectReadableTitle(sidebar, ALPHA_TITLE);
    const betaLabel = await expectReadableTitle(sidebar, BETA_TITLE);
    await expectNoUnknownTimeAge(alphaLabel);
    await expectAgeInMeta(betaLabel);
    await expect(
      page.getByText('Time not reported', { exact: true })
    ).toHaveCount(0);
    await expectNoHorizontalOverflow(page, sidebar);
    await expectFullChatIdentity(sidebar, ALPHA_TITLE);
    await expectFullChatIdentity(sidebar, BETA_TITLE);

    const archiveAlpha = sidebar.getByRole('button', {
      name: `Archive ${ALPHA_TITLE}`,
      exact: true,
    });
    await expect(archiveAlpha).toBeVisible();
    await archiveAlpha.focus();
    await page.keyboard.press('Enter');
    await expect(chatLabel(sidebar, ALPHA_TITLE)).toHaveCount(0);

    const archivedRaw = await controller.readArchivePreferencesRaw();
    const archivedMap: Record<string, { lifecycle?: string }> = archivedRaw
      ? (JSON.parse(archivedRaw) as Record<string, { lifecycle?: string }>)
      : {};
    expect(archivedMap[CHAT_ALPHA_KEY]?.lifecycle).toBe('archived');

    const archiveFilter = sidebar
      .getByRole('group', { name: 'Projects views' })
      .getByRole('button', { name: /^archive\b/i });
    await archiveFilter.focus();
    await page.keyboard.press('Enter');

    const archivedLabel = await expectReadableTitle(sidebar, ALPHA_TITLE);
    await expectNoUnknownTimeAge(archivedLabel);
    const restoreAlpha = sidebar.getByRole('button', {
      name: `Restore ${ALPHA_TITLE}`,
      exact: true,
    });
    await expect(restoreAlpha).toHaveAccessibleName(`Restore ${ALPHA_TITLE}`);
    await restoreAlpha.focus();
    await page.keyboard.press('Enter');
    await expect(chatLabel(sidebar, ALPHA_TITLE)).toHaveCount(0);

    const restoredRaw = await controller.readArchivePreferencesRaw();
    const restoredMap: Record<string, unknown> = restoredRaw
      ? (JSON.parse(restoredRaw) as Record<string, unknown>)
      : {};
    expect(restoredMap[CHAT_ALPHA_KEY]).toBeUndefined();

    await sidebar
      .getByRole('group', { name: 'Projects views' })
      .getByRole('button', { name: /^active\b/i })
      .click();
    await expectReadableTitle(sidebar, ALPHA_TITLE);
  });
});

test.describe('narrow drawer chat titles', () => {
  test.use({ hasTouch: true });

  test('keeps long titles readable and Archive/Restore touch actions reachable at 390x740', async ({
    page,
  }) => {
    const snapshot = buildSidebarTitleSnapshot();
    expect(snapshot.chats).toHaveLength(2);
    expect(snapshot.native_samples).toHaveLength(1);
    expect(snapshot.native_samples[0]?.chat_identity.binding_id).toBe(
      CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id
    );

    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: snapshot,
    });
    await page.setViewportSize({ width: 390, height: 740 });
    await page.goto('/');
    await expect(
      page.locator('#app .tl-app-shell, #app .tl-app').first()
    ).toBeVisible();

    await expect(page.locator('aside.tl-sidebar')).toHaveCount(0);
    const drawerTrigger = page
      .getByRole('button', { name: /open navigation drawer/i })
      .first();
    await expect(drawerTrigger).toBeVisible();
    await drawerTrigger.tap();

    const drawer = page.getByRole('dialog', {
      name: /navigation|projects|workspace/i,
    });
    await expect(drawer).toBeVisible();

    const alphaLabel = await expectReadableTitle(drawer, ALPHA_TITLE);
    const betaLabel = await expectReadableTitle(drawer, BETA_TITLE);
    await expectNoUnknownTimeAge(alphaLabel);
    await expectAgeInMeta(betaLabel);
    await expect(
      page.getByText('Time not reported', { exact: true })
    ).toHaveCount(0);
    await expectNoHorizontalOverflow(page, drawer);
    await expectFullChatIdentity(drawer, ALPHA_TITLE);
    await expectFullChatIdentity(drawer, BETA_TITLE);

    const archiveAlpha = drawer.getByRole('button', {
      name: `Archive ${ALPHA_TITLE}`,
      exact: true,
    });
    const actionBox = await archiveAlpha.boundingBox();
    expect(actionBox).not.toBeNull();
    expect(actionBox!.width).toBeGreaterThanOrEqual(32);
    expect(actionBox!.width).toBeLessThanOrEqual(44);
    expect(actionBox!.height).toBeGreaterThanOrEqual(32);
    await archiveAlpha.tap();
    await expect(chatLabel(drawer, ALPHA_TITLE)).toHaveCount(0);

    await drawer
      .getByRole('group', { name: 'Projects views' })
      .getByRole('button', { name: /^archive\b/i })
      .tap();
    const restoreAlpha = drawer.getByRole('button', {
      name: `Restore ${ALPHA_TITLE}`,
      exact: true,
    });
    await expect(restoreAlpha).toBeVisible();
    await restoreAlpha.tap();
    await expect(chatLabel(drawer, ALPHA_TITLE)).toHaveCount(0);

    await drawer
      .getByRole('group', { name: 'Projects views' })
      .getByRole('button', { name: /^active\b/i })
      .tap();
    await expectReadableTitle(drawer, ALPHA_TITLE);
  });
});
