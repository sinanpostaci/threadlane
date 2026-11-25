import { expect, test, type Locator, type Page } from '@playwright/test';

import {
  advanceWorkspaceGeneratedAtFixture,
  buildInteractiveWorkspaceSnapshot,
  buildWorkerMetadataWorkspaceSnapshot,
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

const APPEARANCE_STORAGE_KEY = 'threadlane.desktop.appearance.v1';

function minimalAppearanceStorage(theme: 'light' | 'dark'): string {
  return JSON.stringify({
    theme,
    surface: 'glass',
    opacity: 88,
    accent: 'sage',
    rememberStateDir: false,
    savedStateDir: '',
  });
}

/**
 * Alpha has no content timestamp. Beta has a recorded content timestamp and
 * an idle native sample. This exercises the age label independently of polling.
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
    threadLabel: string,
    contentUpdatedAtMs: number | null
  ): WorkspaceChatSummary => ({
    ...chat,
    thread_label: threadLabel,
    availability: 'available',
    activity: 'idle',
    active_job_count: 0,
    attention_count: 0,
    updated_at_ms: contentUpdatedAtMs,
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
      idleChat(alphaChat, ALPHA_TITLE, null),
      idleChat(betaChat, BETA_TITLE, betaChat.updated_at_ms),
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

async function waitForFiniteAnimations(scope: Locator): Promise<void> {
  await scope.evaluate(async () => {
    const finiteAnimations = document.getAnimations().filter((animation) => {
      const timing = animation.effect?.getTiming();
      return !timing || timing.iterations !== Infinity;
    });
    await Promise.race([
      Promise.all(
        finiteAnimations.map((animation) =>
          animation.finished.catch(() => undefined)
        )
      ),
      new Promise<void>((resolve) => {
        setTimeout(() => resolve(), 2_000);
      }),
    ]);
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(() => resolve()))
    );
  });
}

async function expectOpaqueDrawerContrast(label: Locator): Promise<void> {
  const metrics = await label.evaluate((labelEl) => {
    const canvas = document.createElement('canvas');
    canvas.width = 1;
    canvas.height = 1;
    const context = canvas.getContext('2d');
    if (!context) {
      throw new Error('Expected a 2D canvas context for color parsing.');
    }
    const parse = (value: string): number[] => {
      context.clearRect(0, 0, 1, 1);
      context.fillStyle = 'rgba(0, 0, 0, 0)';
      context.fillStyle = value;
      context.fillRect(0, 0, 1, 1);
      const data = context.getImageData(0, 0, 1, 1).data;
      return [data[0], data[1], data[2], data[3] / 255];
    };
    const composite = (
      foreground: number[],
      background: number[]
    ): number[] => [
      foreground[0] * foreground[3] + background[0] * (1 - foreground[3]),
      foreground[1] * foreground[3] + background[1] * (1 - foreground[3]),
      foreground[2] * foreground[3] + background[2] * (1 - foreground[3]),
      1,
    ];
    const channel = (value: number): number => {
      const normalized = value / 255;
      return normalized <= 0.04045
        ? normalized / 12.92
        : ((normalized + 0.055) / 1.055) ** 2.4;
    };
    const luminance = (color: number[]): number =>
      0.2126 * channel(color[0]) +
      0.7152 * channel(color[1]) +
      0.0722 * channel(color[2]);

    const dialog = labelEl.closest('.tl-drawer-dialog');
    if (!dialog) {
      throw new Error('Expected the navigation label inside .tl-drawer-dialog.');
    }
    const dialogBackground = parse(getComputedStyle(dialog).backgroundColor);
    let background = dialogBackground;
    const layers: Element[] = [];
    for (
      let node: Element | null = labelEl;
      node !== null && node !== dialog;
      node = node.parentElement
    ) {
      layers.push(node);
    }
    for (const node of layers.reverse()) {
      const layer = parse(getComputedStyle(node).backgroundColor);
      if (layer[3] > 0) {
        background = composite(layer, background);
      }
    }

    const text = composite(parse(getComputedStyle(labelEl).color), background);
    const textLuminance = luminance(text);
    const backgroundLuminance = luminance(background);
    const dot = dialog.querySelector('.tl-chat-item .tl-status-dot');
    const dotBackground = dot
      ? parse(getComputedStyle(dot).backgroundColor)
      : [0, 0, 0, 0];
    return {
      backgroundAlpha: dialogBackground[3],
      contrast:
        (Math.max(textLuminance, backgroundLuminance) + 0.05) /
        (Math.min(textLuminance, backgroundLuminance) + 0.05),
      dotAlpha: dotBackground[3],
    };
  });

  expect(metrics.backgroundAlpha).toBeGreaterThanOrEqual(0.99);
  expect(metrics.contrast).toBeGreaterThanOrEqual(4.5);
  expect(metrics.dotAlpha).toBeGreaterThanOrEqual(0.99);
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
    expect(snapshot.chats[0]?.updated_at_ms).toBeNull();
    expect(snapshot.chats[1]?.updated_at_ms).not.toBeNull();
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

  // The Radix portal sits outside .tl-app-shell, so the local gray bridge must
  // keep the drawer surface opaque and readable in both themes.
  test('keeps the navigation drawer opaque and titles readable in light and dark themes', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildSidebarTitleSnapshot(),
      initialLocalStorage: {
        [APPEARANCE_STORAGE_KEY]: minimalAppearanceStorage('light'),
      },
    });
    await page.setViewportSize({ width: 390, height: 740 });
    await page.goto('/');
    await expect(
      page.locator('#app .tl-app-shell, #app .tl-app').first()
    ).toBeVisible();

    for (const theme of ['light', 'dark'] as const) {
      if (theme === 'dark') {
        await page.evaluate(
          ({ key, value }) => window.localStorage.setItem(key, value),
          {
            key: APPEARANCE_STORAGE_KEY,
            value: minimalAppearanceStorage('dark'),
          }
        );
        await page.reload();
        await expect(
          page.locator('#app .tl-app-shell, #app .tl-app').first()
        ).toBeVisible();
      }
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      const trigger = page
        .getByRole('button', { name: /open navigation drawer/i })
        .first();
      await expect(trigger).toBeVisible();
      await trigger.tap();

      const drawer = page.getByRole('dialog', {
        name: /navigation|projects|workspace/i,
      });
      await expect(drawer).toBeVisible();
      const alphaLabel = await expectReadableTitle(drawer, ALPHA_TITLE);
      const betaLabel = await expectReadableTitle(drawer, BETA_TITLE);
      await waitForFiniteAnimations(drawer);

      await expectOpaqueDrawerContrast(alphaLabel);
      await expectOpaqueDrawerContrast(betaLabel);
      await expectNoHorizontalOverflow(page, drawer);

      await page.keyboard.press('Escape');
      await expect(drawer).toHaveCount(0);
      await expect(trigger).toBeFocused();
    }
  });
});

async function measureElementContrast(locator: Locator): Promise<{
  contrast: number;
  color: string;
  background: string;
  text: string;
}> {
  await expect(locator).toBeVisible();
  return locator.evaluate((element) => {
    const canvas = document.createElement('canvas');
    canvas.width = 1;
    canvas.height = 1;
    const context = canvas.getContext('2d');
    if (!context) {
      throw new Error('Expected a 2D canvas context for contrast calculation.');
    }
    const parse = (value: string): [number, number, number, number] => {
      context.clearRect(0, 0, 1, 1);
      context.fillStyle = 'rgba(0, 0, 0, 0)';
      context.fillStyle = value;
      context.fillRect(0, 0, 1, 1);
      const data = context.getImageData(0, 0, 1, 1).data;
      return [data[0] ?? 0, data[1] ?? 0, data[2] ?? 0, (data[3] ?? 0) / 255];
    };
    const composite = (
      fg: [number, number, number, number],
      bg: [number, number, number, number]
    ): [number, number, number, number] => [
      fg[0] * fg[3] + bg[0] * (1 - fg[3]),
      fg[1] * fg[3] + bg[1] * (1 - fg[3]),
      fg[2] * fg[3] + bg[2] * (1 - fg[3]),
      1,
    ];
    const channel = (value: number): number => {
      const normalized = Math.max(0, Math.min(255, value)) / 255;
      return normalized <= 0.04045
        ? normalized / 12.92
        : ((normalized + 0.055) / 1.055) ** 2.4;
    };
    const luminance = (color: [number, number, number, number]): number =>
      0.2126 * channel(color[0]) +
      0.7152 * channel(color[1]) +
      0.0722 * channel(color[2]);

    const resolvedTheme = document.documentElement.dataset.theme ?? 'light';
    let background: [number, number, number, number] =
      resolvedTheme === 'dark' ? [11, 11, 14, 1] : [244, 244, 246, 1];
    const chain: Element[] = [];
    for (
      let node: Element | null = element;
      node !== null;
      node = node.parentElement
    ) {
      chain.push(node);
    }
    for (const node of chain.reverse()) {
      const style = getComputedStyle(node);
      const parsed = parse(style.backgroundColor);
      const opacity = Number(style.opacity);
      const alpha =
        parsed[3] *
        (Number.isFinite(opacity) ? Math.max(0, Math.min(1, opacity)) : 1);
      if (alpha > 0) {
        background = composite([parsed[0], parsed[1], parsed[2], alpha], background);
      }
    }

    let accumulatedOpacity = 1;
    for (
      let node: Element | null = element;
      node !== null;
      node = node.parentElement
    ) {
      const nodeOpacity = Number(getComputedStyle(node).opacity);
      if (Number.isFinite(nodeOpacity)) {
        accumulatedOpacity *= nodeOpacity;
      }
    }
    const style = getComputedStyle(element);
    const parsedFg = parse(style.color);
    const fg = composite(
      [
        parsedFg[0],
        parsedFg[1],
        parsedFg[2],
        Math.max(0, Math.min(1, parsedFg[3] * accumulatedOpacity)),
      ],
      background
    );
    const l1 = Math.max(luminance(fg), luminance(background));
    const l2 = Math.min(luminance(fg), luminance(background));
    return {
      contrast: (l1 + 0.05) / (l2 + 0.05),
      color: style.color,
      background: `rgb(${Math.round(background[0])}, ${Math.round(background[1])}, ${Math.round(background[2])})`,
      text: (element.textContent ?? '').trim(),
    };
  });
}

test.describe('sidebar and stream metadata contrast and keyboard archive undo', () => {
  test('maintains >= 4.5:1 contrast for age, active observed model, and event timestamps across light, dark, and selected states', async ({
    page,
  }) => {
    const snapshot = buildWorkerMetadataWorkspaceSnapshot();
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: snapshot,
      initialLocalStorage: {
        [APPEARANCE_STORAGE_KEY]: minimalAppearanceStorage('light'),
      },
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await expect(
      page.locator('#app .tl-app-shell, #app .tl-app').first()
    ).toBeVisible();

    for (const theme of ['light', 'dark'] as const) {
      if (theme === 'dark') {
        await page.evaluate(
          ({ key, value }) => window.localStorage.setItem(key, value),
          {
            key: APPEARANCE_STORAGE_KEY,
            value: minimalAppearanceStorage('dark'),
          }
        );
        await page.reload();
        await expect(
          page.locator('#app .tl-app-shell, #app .tl-app').first()
        ).toBeVisible();
      }
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      const sidebar = page.locator('aside.tl-sidebar').first();
      const alphaRow = sidebar
        .locator('.tl-chat-item-row')
        .filter({ hasText: 'Workspace UI Contract' })
        .first();
      await alphaRow.locator('button.tl-chat-item').click();
      await expect(alphaRow).toHaveAttribute('data-selected', 'true');

      const betaRow = sidebar
        .locator('.tl-chat-item-row')
        .filter({ hasText: 'Release Verification Gate' })
        .first();
      await expect(betaRow).toHaveAttribute('data-selected', 'false');

      const selectedModel = await measureElementContrast(
        alphaRow.locator('.tl-chat-model-text').first()
      );
      expect(selectedModel.text).toBe('Worker · Argon');
      expect(
        selectedModel.contrast,
        `${theme} selected observed model contrast (${selectedModel.color} on ${selectedModel.background})`
      ).toBeGreaterThanOrEqual(4.5);

      const selectedAge = await measureElementContrast(
        alphaRow.locator('.tl-chat-item-age').first()
      );
      expect(
        selectedAge.contrast,
        `${theme} selected age contrast (${selectedAge.color} on ${selectedAge.background})`
      ).toBeGreaterThanOrEqual(4.5);

      const unselectedAge = await measureElementContrast(
        betaRow.locator('.tl-chat-item-age').first()
      );
      expect(
        unselectedAge.contrast,
        `${theme} unselected age contrast (${unselectedAge.color} on ${unselectedAge.background})`
      ).toBeGreaterThanOrEqual(4.5);

      const eventTime = await measureElementContrast(
        page.locator('.tl-cw-event-time').first()
      );
      expect(
        eventTime.contrast,
        `${theme} event timestamp contrast (${eventTime.color} on ${eventTime.background})`
      ).toBeGreaterThanOrEqual(4.5);

      const executionModel = await measureElementContrast(
        page.locator('.tl-cw-execution-model').first()
      );
      expect(
        executionModel.contrast,
        `${theme} execution model contrast (${executionModel.color} on ${executionModel.background})`
      ).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('supports keyboard Archive and reversible Undo without stealing focus on new observations', async ({
    page,
  }) => {
    const snapshot = buildSidebarTitleSnapshot();
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: snapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await expect(
      page.locator('#app .tl-app-shell, #app .tl-app').first()
    ).toBeVisible();

    const sidebar = page.locator('aside.tl-sidebar').first();
    const archiveAlpha = sidebar.getByRole('button', {
      name: `Archive ${ALPHA_TITLE}`,
      exact: true,
    });
    await archiveAlpha.focus();
    await page.keyboard.press('Enter');

    const undoBanner = page.locator('[data-archive-undo="true"]');
    await expect(undoBanner).toBeVisible();
    await expect(undoBanner).toContainText(`Archived ${ALPHA_TITLE}.`);
    await expect(chatLabel(sidebar, ALPHA_TITLE)).toHaveCount(0);

    // Focus another control and push a fresh observation: Undo banner must not steal focus.
    const activeTab = sidebar
      .getByRole('group', { name: 'Projects views' })
      .getByRole('button', { name: /^active\b/i });
    await activeTab.focus();
    await expect(activeTab).toBeFocused();

    const nextSnapshot = advanceWorkspaceGeneratedAtFixture(
      snapshot,
      snapshot.generated_at_ms + 1_000
    );
    await controller.setWorkspaceSnapshot(nextSnapshot);
    await page.evaluate(() => window.dispatchEvent(new Event('focus')));
    await expect(activeTab).toBeFocused();

    // Activate reversible Undo via keyboard to restore the exact named chat.
    const undoBtn = undoBanner.getByRole('button', {
      name: `Undo archiving ${ALPHA_TITLE}`,
      exact: true,
    });
    await undoBtn.focus();
    await page.keyboard.press('Enter');

    await expect(undoBanner).toHaveCount(0);
    await expectReadableTitle(sidebar, ALPHA_TITLE);
    const restoredRaw = await controller.readArchivePreferencesRaw();
    const restoredMap: Record<string, unknown> = restoredRaw
      ? (JSON.parse(restoredRaw) as Record<string, unknown>)
      : {};
    expect(restoredMap[CHAT_ALPHA_KEY]).toBeUndefined();
  });
});
