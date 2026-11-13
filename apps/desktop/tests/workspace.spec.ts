import { expect, test, type Locator, type Page } from '@playwright/test';

import {
  appendChatWorkEventFixture,
  backlogButton,
  BETA_CHAT_ONLY_SENTINEL,
  buildExternalResolutionWorkspaceSnapshot,
  buildFourPartScopeCollisionWorkspaceSnapshot,
  buildHistoricalFailureWorkspaceSnapshot,
  buildMinimalChatWorkspaceSnapshot,
  buildMinimalChatWorkspaceSnapshotWithMalformedWork,
  buildPendingAcceptanceWorkspaceSnapshot,
  CHAT_ALPHA_IDENTITY,
  CHAT_BETA_SAME_PROJECT_IDENTITY,
  CHAT_GAMMA_IDENTITY,
  CHAT_GAMMA_KEY,
  chatHeader,
  closeBacklogWithEscape,
  createBacklogEvent,
  createBacklogWorkerPresence,
  EXTERNAL_RESOLUTION_FULL_SUMMARY,
  EXTERNAL_RESOLUTION_UNRELATED_QUESTION,
  expectNoHorizontalOverflow,
  expectNoNeedsYou,
  expectSingleNeedsYou,
  expectStreamEventOrder,
  FIXTURE_BASE_NOW_MS,
  FOUR_PART_BINDING_ONLY_IDENTITY,
  FOUR_PART_BINDING_ONLY_SENTINEL,
  FOUR_PART_PROJECT_ONLY_IDENTITY,
  FOUR_PART_PROJECT_ONLY_SENTINEL,
  FOUR_PART_SOURCE_ONLY_IDENTITY,
  FOUR_PART_SOURCE_ONLY_SENTINEL,
  getChatWorkFixture,
  installWorkspaceTauriFixture,
  MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
  MINIMAL_CHAT_APPENDED_EVENT_TITLE,
  MINIMAL_CHAT_AUTHORED_LANGUAGE_TITLE,
  MINIMAL_CHAT_BETA_BACKLOG_SENTINEL,
  MINIMAL_CHAT_BLOCKED_REASON,
  MINIMAL_CHAT_BLOCKED_TITLE,
  MINIMAL_CHAT_COWORKER_DOING_TITLE,
  MINIMAL_CHAT_DOING_TITLE,
  MINIMAL_CHAT_DONE_TITLE,
  MINIMAL_CHAT_FAILED_TITLE,
  MINIMAL_CHAT_GAMMA_BACKLOG_SENTINEL,
  MINIMAL_CHAT_LEGACY_MESSAGE_SENTINEL,
  MINIMAL_CHAT_MALFORMED_WORK_SENTINEL,
  MINIMAL_CHAT_PREPARING_TITLE,
  MINIMAL_CHAT_READING_ANCHOR_TITLE,
  MINIMAL_CHAT_TODO_TITLE,
  MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL,
  openBacklog,
  resolveBacklogItemRow,
  resolveStreamEventRow,
  scrollStreamToBottom,
  selectChat,
  sidebar,
  streamBottomDistance,
  streamScroller,
  triggerWorkspaceRefresh,
  updateChatWorkFixture,
  updateChatWorkPresenceFixture,
  updateNativeActivityFixture,
  workspaceSection,
  WRONG_SCOPE_QUARANTINE_SENTINEL,
} from './fixtures/workspace.ts';
import {
  parseWorkspaceSnapshot,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  workspaceChatIdentitiesEqual,
} from '../src/workspace-contract.ts';

// The minimal chat contract replaces ordinary Messages/Jobs/Activity tabs with
// one event stream and a read-only backlog drawer. These browser checks adapt
// the meaningful old isolation, archive, navigation, liveness, and safety
// coverage to that product surface.
const STALE_ADVANCE_MS = WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 1_001;

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

async function installMinimal(
  page: Page,
  viewport: { width: number; height: number } = { width: 1040, height: 760 },
  options: { appearanceTheme?: 'light' | 'dark' } = {},
) {
  const controller = await installWorkspaceTauriFixture(page, {
    workspaceSnapshot: buildMinimalChatWorkspaceSnapshot(),
    ...(options.appearanceTheme
      ? {
          initialLocalStorage: {
            [APPEARANCE_STORAGE_KEY]: minimalAppearanceStorage(options.appearanceTheme),
          },
        }
      : {}),
  });
  // Select Alpha while the inline sidebar is available, then apply the
  // requested viewport. At 760px and below the sidebar becomes a closed drawer.
  await page.setViewportSize({ width: 1040, height: 760 });
  await page.goto('/');
  await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
  // The workspace independently orders chats, so Alpha is not guaranteed to be
  // selected merely because it appears first in the fixture chat list.
  await selectChat(page, 'Workspace UI Contract');
  await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
  await page.setViewportSize(viewport);
  return controller;
}

type CommittedBacklogStatus = 'todo' | 'doing' | 'done' | 'blocked';

const COMMITTED_BACKLOG_GROUP_LABEL: Record<CommittedBacklogStatus, string> = {
  todo: 'Todo',
  doing: 'Doing',
  done: 'Done',
  blocked: 'Blocked',
};

function backlogStatusGroup(dialog: Locator, status: CommittedBacklogStatus): Locator {
  const labels =
    status === 'todo'
      ? ['Todo', 'To do', 'To Do']
      : [COMMITTED_BACKLOG_GROUP_LABEL[status]];
  return dialog
    .locator(
      labels
        .map((label) => `section[aria-label="${label} backlog items" i]`)
        .join(', '),
    )
    .first();
}

function backlogStatusGroupPattern(status: CommittedBacklogStatus): RegExp {
  const label =
    status === 'todo' ? 'to[ -]?do' : COMMITTED_BACKLOG_GROUP_LABEL[status];
  return new RegExp(`^${label} backlog items$`, 'i');
}

async function expectCommittedBacklogStatus(
  dialog: Locator,
  title: string,
  status: CommittedBacklogStatus,
): Promise<void> {
  // The committed state is the enclosing named group plus the row's exact
  // data-status. The row intentionally has no duplicated visible state text.
  const group = backlogStatusGroup(dialog, status);
  await expect(group).toBeVisible();
  await expect(group).toHaveAttribute(
    'aria-label',
    backlogStatusGroupPattern(status),
  );
  const row = resolveBacklogItemRow(group, title);
  await expect(row).toBeVisible();
  await expect(row).toHaveAttribute('data-status', status);
}

async function expectNoInventedBacklogRowStatus(row: Locator): Promise<void> {
  await expect(row).not.toHaveAttribute(
    'data-status',
    /^(?:working|preparing|awaiting check|failed)$/i,
  );
  await expect(row.getByText(/^(?:working|preparing|awaiting check|failed)$/i)).toHaveCount(0);
}

type LiveAgentRole = 'Worker' | 'Coworker';

function liveAgentList(scope: Locator): Locator {
  // Live agents is the accessible list itself, not a heading above the rows.
  return scope.locator('ul[aria-label="Live agents"]').first();
}

function liveAgentRow(scope: Locator, agent: LiveAgentRole): Locator {
  return liveAgentList(scope)
    .locator('li[data-agent-role]')
    .filter({ hasText: new RegExp(`^\\s*${agent}`, 'i') })
    .first();
}

function liveAgentSpinner(row: Locator): Locator {
  return row.locator(
    '[data-activity-spinner="true"], [data-agent-spinner="true"], [class*="spinner" i]',
  );
}

async function visibleCount(locator: Locator): Promise<number> {
  return locator.evaluateAll(
    (nodes) => nodes.filter((node) => node.checkVisibility()).length,
  );
}

async function expectLiveAgentWorking(
  scope: Locator,
  agent: LiveAgentRole,
): Promise<void> {
  await expect(liveAgentList(scope)).toBeVisible();
  const row = liveAgentRow(scope, agent);
  await expect(row).toBeVisible();
  await expect(row.getByText(new RegExp(`^${agent}$`, 'i')).first()).toBeVisible();
  const detail = (await row.innerText())
    .replace(new RegExp(`^\\s*${agent}`, 'i'), '')
    .trim();
  expect(detail, `${agent} live agent row must include its detail text`).not.toBe('');
  await expect.poll(() => visibleCount(liveAgentSpinner(row))).toBeGreaterThan(0);
}

async function expectNoLiveAgentWorking(
  scope: Locator,
  agent: LiveAgentRole,
): Promise<void> {
  await expect
    .poll(async () => {
      const row = liveAgentRow(scope, agent);
      if ((await row.count()) === 0) {
        return 0;
      }
      const spinners = await visibleCount(liveAgentSpinner(row));
      const workingDetails = await visibleCount(row.getByText(/\bworking\b/i));
      return spinners + workingDetails;
    })
    .toBe(0);
}

async function streamEventOffset(scroller: Locator, row: Locator): Promise<number> {
  const [scrollerBox, rowBox] = await Promise.all([
    scroller.boundingBox(),
    row.boundingBox(),
  ]);
  if (!scrollerBox || !rowBox) {
    throw new Error('Expected both the stream scroller and event row to have geometry.');
  }
  return rowBox.y - scrollerBox.y;
}

async function positionStreamEvent(
  scroller: Locator,
  row: Locator,
  offsetPx: number,
): Promise<void> {
  await row.scrollIntoViewIfNeeded();
  const currentOffset = await streamEventOffset(scroller, row);
  await scroller.evaluate((element, delta) => {
    element.scrollTop = Math.max(0, element.scrollTop + delta);
    element.dispatchEvent(new Event('scroll', { bubbles: true }));
  }, currentOffset - offsetPx);
  await expect
    .poll(async () => Math.abs((await streamEventOffset(scroller, row)) - offsetPx))
    .toBeLessThanOrEqual(1);
}

async function firstIntersectingStreamEventId(scroller: Locator): Promise<string | null> {
  return scroller.evaluate((element) => {
    const viewport = element.getBoundingClientRect();
    const first = [...element.querySelectorAll('[data-chat-event-id]')].find((row) => {
      const rect = row.getBoundingClientRect();
      return rect.bottom > viewport.top && rect.top < viewport.bottom;
    });
    return first?.getAttribute('data-chat-event-id') ?? null;
  });
}

async function waitForFiniteAnimations(scope: Locator): Promise<void> {
  await scope.evaluate(async () => {
    const finiteAnimations = document.getAnimations().filter((animation) => {
      const timing = animation.effect?.getTiming();
      return !timing || timing.iterations !== Infinity;
    });
    await Promise.race([
      Promise.all(finiteAnimations.map((animation) => animation.finished.catch(() => undefined))),
      new Promise<void>((resolve) => {
        setTimeout(() => resolve(), 2_000);
      }),
    ]);
    await new Promise<void>((resolve) => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => resolve());
      });
    });
  });
}

async function expectReadableDrawerTitle(dialog: Locator, title: string): Promise<void> {
  const titleElement = dialog.getByText(title, { exact: true }).first();
  await expect(titleElement).toBeVisible();
  const metrics = await titleElement.evaluate((element) => {
    interface RgbaColor {
      r: number;
      g: number;
      b: number;
      a: number;
    }

    const parseColor = (value: string): RgbaColor => {
      const match = value.match(
        /rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:[,\s/]+([\d.]+))?\s*\)/i,
      );
      if (match) {
        return {
          r: Number(match[1]),
          g: Number(match[2]),
          b: Number(match[3]),
          a: match[4] === undefined ? 1 : Number(match[4]),
        };
      }
      if (value === 'transparent') {
        return { r: 0, g: 0, b: 0, a: 0 };
      }
      const canvas = document.createElement('canvas');
      canvas.width = 1;
      canvas.height = 1;
      const context = canvas.getContext('2d');
      if (!context) {
        return { r: 0, g: 0, b: 0, a: 0 };
      }
      context.clearRect(0, 0, 1, 1);
      context.fillStyle = 'rgba(0, 0, 0, 0)';
      context.fillStyle = value;
      context.fillRect(0, 0, 1, 1);
      const data = context.getImageData(0, 0, 1, 1).data;
      return {
        r: data[0] ?? 0,
        g: data[1] ?? 0,
        b: data[2] ?? 0,
        a: (data[3] ?? 0) / 255,
      };
    };

    const compositeOver = (foreground: RgbaColor, background: RgbaColor): RgbaColor => ({
      r: foreground.r * foreground.a + background.r * (1 - foreground.a),
      g: foreground.g * foreground.a + background.g * (1 - foreground.a),
      b: foreground.b * foreground.a + background.b * (1 - foreground.a),
      a: 1,
    });

    const linearChannel = (value: number): number => {
      const normalized = Math.max(0, Math.min(255, value)) / 255;
      return normalized <= 0.04045
        ? normalized / 12.92
        : ((normalized + 0.055) / 1.055) ** 2.4;
    };

    const luminance = (color: RgbaColor): number =>
      0.2126 * linearChannel(color.r) +
      0.7152 * linearChannel(color.g) +
      0.0722 * linearChannel(color.b);

    const style = getComputedStyle(element);
    const resolvedTheme = document.documentElement.dataset.theme ?? 'light';
    let background: RgbaColor =
      resolvedTheme === 'dark'
        ? { r: 0, g: 0, b: 0, a: 1 }
        : { r: 255, g: 255, b: 255, a: 1 };
    const ancestors: Element[] = [];
    for (let node = element.parentElement; node !== null; node = node.parentElement) {
      ancestors.push(node);
    }
    for (const ancestor of ancestors.reverse()) {
      const ancestorStyle = getComputedStyle(ancestor);
      const ancestorBackground = parseColor(ancestorStyle.backgroundColor);
      const ancestorOpacity = Number(ancestorStyle.opacity);
      const effectiveAlpha =
        ancestorBackground.a *
        (Number.isFinite(ancestorOpacity)
          ? Math.max(0, Math.min(1, ancestorOpacity))
          : 1);
      if (effectiveAlpha > 0) {
        background = compositeOver(
          { ...ancestorBackground, a: effectiveAlpha },
          background,
        );
      }
    }
    const ownBackground = parseColor(style.backgroundColor);
    const ownOpacity = Number(style.opacity);
    const ownAlpha =
      ownBackground.a *
      (Number.isFinite(ownOpacity) ? Math.max(0, Math.min(1, ownOpacity)) : 1);
    if (ownAlpha > 0) {
      background = compositeOver({ ...ownBackground, a: ownAlpha }, background);
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

    const textColor = parseColor(style.color);
    const compositedText = compositeOver(
      {
        ...textColor,
        a: Math.max(0, Math.min(1, textColor.a * accumulatedOpacity)),
      },
      background,
    );
    const textLuminance = luminance(compositedText);
    const backgroundLuminance = luminance(background);
    const contrast =
      (Math.max(textLuminance, backgroundLuminance) + 0.05) /
      (Math.min(textLuminance, backgroundLuminance) + 0.05);
    const rect = element.getBoundingClientRect();

    return {
      contrast,
      fontSize: Number.parseFloat(style.fontSize),
      fontWeight: Number.parseInt(style.fontWeight, 10),
      color: style.color,
      backgroundColor: `${background.r},${background.g},${background.b}`,
      text: (element.textContent ?? '').replace(/\s+/g, ' ').trim(),
      width: rect.width,
      height: rect.height,
      visible: element.checkVisibility(),
      overflows:
        element.clientWidth > 0 && element.scrollWidth > element.clientWidth + 1,
    };
  });

  expect(metrics.visible, `${title} must be visible`).toBe(true);
  expect(metrics.text, `${title} must render its complete title text`).toBe(title);
  expect(metrics.width, `${title} must have a readable title box`).toBeGreaterThan(40);
  expect(metrics.height, `${title} must have a readable title height`).toBeGreaterThanOrEqual(12);
  expect(metrics.overflows, `${title} must not overflow horizontally`).toBe(false);
  const minimumContrast =
    metrics.fontSize >= 24 || (metrics.fontSize >= 18.66 && metrics.fontWeight >= 700)
      ? 3
      : 4.5;
  expect(
    metrics.contrast,
    `${title} contrast ${metrics.contrast.toFixed(2)} (text ${metrics.color}, background rgb(${metrics.backgroundColor}))`,
  ).toBeGreaterThanOrEqual(minimumContrast);
}

async function expectVisibleWorking(scope: Locator): Promise<void> {
  await expect
    .poll(async () =>
      scope
        .getByText(/working/i)
        .evaluateAll((nodes) => nodes.filter((node) => node.checkVisibility()).length),
    )
    .toBeGreaterThan(0);
}

async function expectNoVisibleWorking(scope: Locator): Promise<void> {
  await expect
    .poll(async () =>
      scope
        .getByText(/working/i)
        .evaluateAll((nodes) => nodes.filter((node) => node.checkVisibility()).length),
    )
    .toBe(0);
}

async function expectNoNeedsYouInScope(scope: Locator): Promise<void> {
  await expect
    .poll(async () => {
      const visibleTexts = await scope
        .getByText(/waiting for you(?: in codex)?|needs\s*you/i)
        .evaluateAll((nodes) => nodes.filter((node) => node.checkVisibility()).length);
      const visibleAttentionLabels = await scope
        .locator('[aria-label]')
        .evaluateAll(
          (nodes) =>
            nodes
              .filter((node) => node.checkVisibility())
              .map((node) => node.getAttribute('aria-label') ?? '')
              .filter((label) => /\b\d+\s+(?:item|items|question|questions)?\s*needs?\s+attention/i.test(label))
              .length,
        );
      return visibleTexts + visibleAttentionLabels;
    })
    .toBe(0);
}

function lifecycleFilterControl(page: Page, kind: 'active' | 'archived'): Locator {
  const pattern = kind === 'active' ? /^active\b/i : /^archive\b/i;
  return sidebar(page)
    .getByRole('group', { name: 'Projects views' })
    .getByRole('button', { name: pattern })
    .first();
}

function matchesExactOpenCodexInvocation(
  rawArgs: Record<string, unknown>,
  expected: { source_id: string; thread_uuid: string; job_id?: string | null },
): boolean {
  const topKeys = Object.keys(rawArgs);
  if (topKeys.length !== 1 || topKeys[0] !== 'request') {
    return false;
  }
  const rawRequest = rawArgs.request;
  if (!rawRequest || typeof rawRequest !== 'object' || Array.isArray(rawRequest)) {
    return false;
  }
  const request = rawRequest as Record<string, unknown>;
  const allowedKeys = new Set(['source_id', 'thread_uuid', 'job_id']);
  for (const key of Object.keys(request)) {
    if (!allowedKeys.has(key)) {
      return false;
    }
  }
  const expectedJobId = expected.job_id ?? null;
  const actualJobId = request.job_id === undefined ? null : request.job_id;
  return (
    request.source_id === expected.source_id &&
    request.thread_uuid === expected.thread_uuid &&
    actualJobId === expectedJobId
  );
}

async function expectUsableStream(page: Page): Promise<Locator> {
  const scroller = streamScroller(page);
  await expect(scroller).toBeVisible();
  await expect(scroller).toHaveAttribute('data-chat-stream-scroller', 'true');
  await expect(scroller).toHaveAttribute('aria-label', 'Chat activity stream');
  await expect
    .poll(() => scroller.evaluate((el) => el.clientHeight))
    .toBeGreaterThanOrEqual(120);
  const metrics = await scroller.evaluate((el) => ({
    clientHeight: el.clientHeight,
    scrollHeight: el.scrollHeight,
  }));
  expect(metrics.clientHeight).toBeGreaterThanOrEqual(120);
  expect(metrics.scrollHeight).toBeGreaterThanOrEqual(metrics.clientHeight);
  return scroller;
}

async function expectInViewportWithinScroller(
  scroller: Locator,
  target: Locator,
): Promise<void> {
  await target.scrollIntoViewIfNeeded();
  await expect(target).toBeVisible();
  await expect
    .poll(async () => {
      const [scrollerBox, targetBox] = await Promise.all([
        scroller.boundingBox(),
        target.boundingBox(),
      ]);
      if (scrollerBox === null || targetBox === null) {
        return false;
      }
      return (
        scrollerBox.height >= 120 &&
        targetBox.height > 0 &&
        targetBox.y >= scrollerBox.y - 1 &&
        targetBox.y + targetBox.height <= scrollerBox.y + scrollerBox.height + 1
      );
    })
    .toBe(true);
}

test.describe('Production entry path and Tauri IPC boundary', () => {
  test('opens production index.html -> main.ts -> app.tsx without fake success fallback when IPC is absent', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    const hasTauriGlobals = await page.evaluate(() => {
      const win = window as unknown as Record<string, unknown>;
      return Boolean(win.__TAURI_INTERNALS__ || win.__TAURI__);
    });
    expect(hasTauriGlobals).toBe(false);

    // Without injected Tauri IPC or ?preview=1, production code must never fabricate live chats.
    await expect(page.getByText('Workspace UI Contract')).toHaveCount(0);
    await expect(
      page
        .getByText(
          /not connected in this browser window|observation.*unavailable|runtime.*unavailable|not available|unavailable|disconnected|offline/i,
        )
        .first(),
    ).toBeVisible();
  });

  test('mounts compact tray surface via index.html?surface=tray and dispatches Open/Hide/Quit IPC actions', async ({
    page,
  }) => {
    const controller = await installWorkspaceTauriFixture(page, {
      windowLabel: 'tray',
      workspaceSnapshot: buildPendingAcceptanceWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 360, height: 320 });
    await page.goto('/?surface=tray');

    await expect(page.locator('#app .tl-tray-shell, #app .tl-tray-root').first()).toBeVisible();
    await expect(
      page.getByRole('region', { name: /Threadlane (system tray|status)/i }),
    ).toBeVisible();

    const rows = page.locator('.tl-tray-row');
    expect(await rows.count()).toBeLessThanOrEqual(3);

    const openBtn = page.getByRole('button', { name: 'Open Threadlane' });
    const quitBtn = page.getByRole('button', { name: 'Quit Threadlane' });
    await expect(openBtn).toBeVisible();
    await expect(quitBtn).toBeVisible();

    const trayOverflow = await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    );
    expect(trayOverflow).toBe(true);

    await controller.clearInvocations();
    await openBtn.click();
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.some(
          (call) => call.cmd === 'desktop_tray_action' && call.args.action === 'show_main',
        );
      })
      .toBe(true);

    await page.keyboard.press('Escape');
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.some(
          (call) => call.cmd === 'desktop_tray_action' && call.args.action === 'hide',
        );
      })
      .toBe(true);

    await quitBtn.click();
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.some(
          (call) => call.cmd === 'desktop_tray_action' && call.args.action === 'quit',
        );
      })
      .toBe(true);
  });
});

test.describe('Responsive shell geometry: wide, 760px drawer boundary, and narrow stream', () => {
  test('toggles wide titlebar sidebar rail, switches at exact 760px drawer boundary, traps focus, and restores focus on Escape', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const titlebar = page.locator('header.tl-titlebar, [role="banner"]').first();
    await expect(titlebar).toBeVisible();

    const inlineSidebar = sidebar(page);
    await expect(inlineSidebar).toBeVisible();
    const expandedBox = await inlineSidebar.boundingBox();
    expect(expandedBox).not.toBeNull();
    expect(expandedBox!.width).toBeGreaterThanOrEqual(220);
    expect(expandedBox!.width).toBeLessThanOrEqual(320);

    const collapseBtn = titlebar
      .getByRole('button', { name: /collapse sidebar|hide sidebar|toggle sidebar/i })
      .first();
    await expect(collapseBtn).toBeVisible();
    await collapseBtn.click();

    const expandBtn = titlebar
      .getByRole('button', { name: /expand sidebar|show sidebar|toggle sidebar/i })
      .first();
    await expect(expandBtn).toBeVisible();
    await expect
      .poll(async () => {
        const collapsedBox = await inlineSidebar.boundingBox();
        return collapsedBox === null ? 0 : collapsedBox.width;
      })
      .toBeLessThanOrEqual(72);

    await expandBtn.click();
    await expect(
      titlebar
        .getByRole('button', { name: /collapse sidebar|hide sidebar|toggle sidebar/i })
        .first(),
    ).toBeVisible();
    await expect
      .poll(async () => (await inlineSidebar.boundingBox())?.width ?? 0)
      .toBeGreaterThanOrEqual(220);

    // 761px remains desktop inline mode; 760px crosses into compact drawer mode.
    await page.setViewportSize({ width: 761, height: 760 });
    await expect(inlineSidebar).toBeVisible();
    await expect(page.getByRole('button', { name: /open navigation drawer/i })).toHaveCount(0);

    await page.setViewportSize({ width: 760, height: 760 });
    await expect(inlineSidebar).toHaveCount(0);

    const drawerTrigger = titlebar
      .getByRole('button', { name: /open navigation drawer|navigation drawer/i })
      .first();
    await expect(drawerTrigger).toBeVisible();
    const drawerDialog = page.getByRole('dialog', { name: /navigation|projects|workspace/i });
    await expect(drawerDialog).toHaveCount(0);

    await drawerTrigger.click();
    await expect(drawerDialog).toBeVisible();

    await expect
      .poll(() =>
        page.evaluate(() => {
          const dialog = document.querySelector('[role="dialog"]');
          return Boolean(dialog && document.activeElement && dialog.contains(document.activeElement));
        }),
      )
      .toBe(true);

    await page.keyboard.press('Escape');
    await expect(drawerDialog).toHaveCount(0);
    await expect(drawerTrigger).toBeFocused();
  });

  test('renders one usable event stream and an accessible Backlog control inside 390x740 without horizontal overflow', async ({
    page,
  }) => {
    await installMinimal(page, { width: 390, height: 740 });
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expectUsableStream(page);
    await expect(backlogButton(page)).toBeVisible();
    await expectNoHorizontalOverflow(page, '390x740 stream');

    const workspace = workspaceSection(page);
    await expect(workspace.getByRole('tab')).toHaveCount(0);
    await expect(workspace.locator('[role="tablist"]')).toHaveCount(0);
    await expect(
      workspace.getByRole('tab', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(
      workspace.getByRole('button', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(workspace.getByText(/load older/i)).toHaveCount(0);
    await expect(workspace.getByText(/filter/i)).toHaveCount(0);
    await expect(workspace.getByRole('button', { name: /filter/i })).toHaveCount(0);

    const dialog = await openBacklog(page);
    const dialogBox = await dialog.boundingBox();
    expect(dialogBox).not.toBeNull();
    expect(dialogBox!.width).toBeLessThanOrEqual(390);
    await expectNoHorizontalOverflow(page, '390x740 backlog drawer');
    await closeBacklogWithEscape(page);
    await expect(backlogButton(page)).toBeFocused();
  });

  test('narrow event rows preserve readable titles and remain inside the stream viewport', async ({
    page,
  }) => {
    await installMinimal(page, { width: 390, height: 740 });
    const scroller = await expectUsableStream(page);

    const titles = [
      MINIMAL_CHAT_READING_ANCHOR_TITLE,
      `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_DOING_TITLE}`,
      `Blocked: ${MINIMAL_CHAT_BLOCKED_TITLE}`,
    ];
    for (const title of titles) {
      const row = await resolveStreamEventRow(page, title);
      await expect(row).toBeVisible();
      await expectInViewportWithinScroller(scroller, row);
      const box = await row.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.width).toBeGreaterThan(80);
      expect(box!.height).toBeGreaterThanOrEqual(14);
    }

    await expectNoHorizontalOverflow(page, 'narrow event rows');
  });
});

test.describe('Minimal chat stream, archive, scroll, and read-only backlog drawer', () => {
  test('renders a verified zero-job chat as one chronological event stream without legacy tabs at 1040x760 and 390x740', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    const snapshot = await controller.getWorkspaceSnapshot();
    expect(snapshot).not.toBeNull();
    expect(snapshot!.observation.jobs).toHaveLength(0);
    expect(snapshot!.observation.requests).toHaveLength(0);
    expect(snapshot!.job_histories).toHaveLength(0);

    await expect(
      sidebar(page).getByText('Workspace UI Contract', { exact: true }),
    ).toBeVisible();
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    const scroller = await expectUsableStream(page);
    const workspace = workspaceSection(page);
    await expect(workspace.getByRole('tab')).toHaveCount(0);
    await expect(workspace.locator('[role="tablist"]')).toHaveCount(0);
    await expect(
      workspace.getByRole('tab', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(
      workspace.getByRole('button', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(workspace.getByText(/load older/i)).toHaveCount(0);
    await expect(workspace.getByText(/filter/i)).toHaveCount(0);
    await expect(workspace.getByRole('button', { name: /filter/i })).toHaveCount(0);

    await expectStreamEventOrder(page, [
      MINIMAL_CHAT_READING_ANCHOR_TITLE,
      `Backlog item created: ${MINIMAL_CHAT_TODO_TITLE}`,
      `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_DOING_TITLE}`,
    ]);

    // Historical message fields and unaligned work never replace or leak into the stream.
    await expect(workspace.getByText(MINIMAL_CHAT_LEGACY_MESSAGE_SENTINEL)).toHaveCount(0);
    await expect(workspace.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);
    await expect(workspace.getByText(MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL)).toHaveCount(0);
    await expect(workspace.getByText(MINIMAL_CHAT_MALFORMED_WORK_SENTINEL)).toHaveCount(0);

    await page.setViewportSize({ width: 390, height: 740 });
    await expect(scroller).toBeVisible();
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(backlogButton(page)).toBeVisible();
    await expectNoHorizontalOverflow(page, 'narrow zero-job stream');
  });

  test('appends events chronologically, deduplicates stable event IDs, and follows only while near the bottom', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    const scroller = streamScroller(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await scrollStreamToBottom(scroller);

    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const alphaWork = getChatWorkFixture(baseSnapshot!, CHAT_ALPHA_IDENTITY);
    const lastEvent = alphaWork.events[alphaWork.events.length - 1]!;
    const lastItem = alphaWork.items.find((item) => item.item_id === lastEvent.item_id);
    expect(lastItem).toBeDefined();

    const appendedEvent = createBacklogEvent({
      item: lastItem!,
      sequence: lastEvent.sequence + 1,
      revision: lastEvent.revision + 1,
      action: 'edited',
      title: MINIMAL_CHAT_APPENDED_EVENT_TITLE,
      recordedAtMs: FIXTURE_BASE_NOW_MS + 5_000,
    });
    const appendedSnapshot = appendChatWorkEventFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      appendedEvent,
    );
    await controller.setWorkspaceSnapshot(appendedSnapshot);
    await triggerWorkspaceRefresh(page);

    const appendedRow = await resolveStreamEventRow(page, MINIMAL_CHAT_APPENDED_EVENT_TITLE);
    await expect(appendedRow).toBeVisible();
    await expect(appendedRow).toBeInViewport({ ratio: 0.5 });
    const appendedText = await scroller.innerText();
    expect(appendedText.indexOf(MINIMAL_CHAT_APPENDED_EVENT_TITLE)).toBeGreaterThan(
      appendedText.indexOf(lastEvent.title),
    );
    await expect.poll(() => streamBottomDistance(scroller)).toBeLessThanOrEqual(64);

    // A refresh repeating the same durable event ID must not add a second row.
    const duplicateSnapshot = appendChatWorkEventFixture(
      appendedSnapshot,
      CHAT_ALPHA_IDENTITY,
      {
        ...appendedEvent,
        note: 'Duplicate refresh payload for the same durable event ID.',
      },
    );
    await controller.setWorkspaceSnapshot(duplicateSnapshot);
    await triggerWorkspaceRefresh(page);
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_APPENDED_EVENT_TITLE, { exact: true }),
    ).toHaveCount(1);

    // Distinct durable event IDs may share display text; they must not be
    // collapsed by title alone.
    const sameTitle = 'Same display title, distinct durable event IDs.';
    const sameTitleEvents = [1, 2].map((offset) =>
      createBacklogEvent({
        item: lastItem!,
        sequence: appendedEvent.sequence + offset,
        revision: appendedEvent.revision + offset,
        eventId: `${lastItem!.item_id}:${appendedEvent.revision + offset}`,
        action: 'edited',
        title: sameTitle,
        recordedAtMs: FIXTURE_BASE_NOW_MS + 5_000 + offset,
      }),
    );
    let sameTitleSnapshot = duplicateSnapshot;
    for (const sameTitleEvent of sameTitleEvents) {
      sameTitleSnapshot = appendChatWorkEventFixture(
        sameTitleSnapshot,
        CHAT_ALPHA_IDENTITY,
        sameTitleEvent,
      );
    }
    await controller.setWorkspaceSnapshot(sameTitleSnapshot);
    await triggerWorkspaceRefresh(page);
    await expect(
      workspaceSection(page).getByText(sameTitle, { exact: true }),
    ).toHaveCount(2);

    // Reading above the bottom must not be forced to follow the append.
    await scroller.evaluate((el) => {
      el.scrollTop = Math.min(96, Math.max(0, el.scrollHeight - el.clientHeight));
      el.dispatchEvent(new Event('scroll', { bubbles: true }));
    });
    const anchor = await resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE);
    const before = await scroller.evaluate((el) => ({
      scrollTop: el.scrollTop,
      distanceFromBottom: el.scrollHeight - el.scrollTop - el.clientHeight,
    }));
    expect(before.distanceFromBottom).toBeGreaterThan(80);
    const beforeBox = await anchor.boundingBox();
    expect(beforeBox).not.toBeNull();

    const readingEvent = createBacklogEvent({
      item: lastItem!,
      sequence: appendedEvent.sequence + 3,
      revision: appendedEvent.revision + 3,
      action: 'edited',
      title: 'Reading anchor append event: the viewport must stay on the anchor.',
      recordedAtMs: FIXTURE_BASE_NOW_MS + 6_000,
    });
    const readingSnapshot = appendChatWorkEventFixture(
      sameTitleSnapshot,
      CHAT_ALPHA_IDENTITY,
      readingEvent,
    );
    await controller.setWorkspaceSnapshot(readingSnapshot);
    await triggerWorkspaceRefresh(page);

    const newEventsButton = page
      .getByRole('button', {
        name: /new (?:event|events|message|messages|activity|update)|jump to latest|latest/i,
      })
      .first();
    await expect(newEventsButton).toBeVisible();

    const afterScrollTop = await scroller.evaluate((el) => el.scrollTop);
    const afterBox = await anchor.boundingBox();
    expect(afterBox).not.toBeNull();
    expect(Math.abs(afterScrollTop - before.scrollTop)).toBeLessThanOrEqual(6);
    expect(Math.abs(afterBox!.y - beforeBox!.y)).toBeLessThanOrEqual(6);

    await newEventsButton.click();
    await expect.poll(() => streamBottomDistance(scroller)).toBeLessThanOrEqual(64);
    await expect(
      resolveStreamEventRow(page, readingEvent.title),
    ).toBeInViewport({ ratio: 0.5 });
    await expect(newEventsButton).toHaveCount(0);
  });

  test('preserves the visible event and pixel offset when a refresh trims the stream head', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    const scroller = await expectUsableStream(page);
    await expect
      .poll(() => scroller.evaluate((el) => el.scrollHeight - el.clientHeight))
      .toBeGreaterThan(160);

    const anchorTitle = `Backlog item created: ${MINIMAL_CHAT_TODO_TITLE}`;
    const anchor = await resolveStreamEventRow(page, anchorTitle);
    await positionStreamEvent(scroller, anchor, 72);
    const beforeOffset = await streamEventOffset(scroller, anchor);
    const beforeScrollTop = await scroller.evaluate((el) => el.scrollTop);
    expect(beforeScrollTop).toBeGreaterThan(0);

    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const alphaWork = getChatWorkFixture(baseSnapshot!, CHAT_ALPHA_IDENTITY);
    expect(alphaWork.events.length).toBeGreaterThan(6);
    // Keep the first intersecting row (sequence 3). The named row below it is
    // not itself the reading anchor; deleting that predecessor tests eviction.
    const removedTitles = alphaWork.events.slice(0, 2).map((event) => event.title);
    await expect(resolveStreamEventRow(page, removedTitles[0]!)).toHaveCount(1);
    const trimmedSnapshot = updateChatWorkFixture(baseSnapshot!, CHAT_ALPHA_IDENTITY, {
      events: alphaWork.events.slice(2),
    });
    await controller.setWorkspaceSnapshot(trimmedSnapshot);
    await triggerWorkspaceRefresh(page);

    await expect(resolveStreamEventRow(page, removedTitles[0]!)).toHaveCount(0);
    await expect(anchor).toBeVisible();
    await expect(anchor).toBeInViewport({ ratio: 0.4 });
    await expect
      .poll(async () => Math.abs((await streamEventOffset(scroller, anchor)) - beforeOffset))
      .toBeLessThanOrEqual(2);
    await expect
      .poll(() => scroller.evaluate((el) => el.scrollTop))
      .toBeLessThan(beforeScrollTop);
  });

  test('falls back to the nearest surviving event when the reading anchor is evicted without jumping to the end', async ({ page }) => {
    const controller = await installMinimal(page);
    const scroller = await expectUsableStream(page);
    const survivor = resolveStreamEventRow(page, `Backlog item created: ${MINIMAL_CHAT_TODO_TITLE}`);
    await positionStreamEvent(scroller, survivor, 72);
    const previousAnchor = await firstIntersectingStreamEventId(scroller);
    const base = (await controller.getWorkspaceSnapshot())!;
    const work = getChatWorkFixture(base, CHAT_ALPHA_IDENTITY);
    expect(work.events.slice(0, 3).map((event) => event.event_id)).toContain(previousAnchor);
    await controller.setWorkspaceSnapshot(updateChatWorkFixture(base, CHAT_ALPHA_IDENTITY, {
      events: work.events.slice(3),
    }));
    await triggerWorkspaceRefresh(page);
    await expect.poll(() => firstIntersectingStreamEventId(scroller)).toBe(work.events[3]!.event_id);
    await expect.poll(async () => Math.abs(await streamEventOffset(scroller, survivor))).toBeLessThanOrEqual(2);
    await expect(page.getByRole('button', { name: 'Jump to latest', exact: true })).toBeVisible();
    await expect.poll(() => streamBottomDistance(scroller)).toBeGreaterThan(64);
  });

  test('preserves reading position through text reflow and stays pinned to the end while following', async ({
    page,
  }) => {
    await installMinimal(page);
    const scroller = await expectUsableStream(page);
    const anchorTitle = `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`;
    const anchor = await resolveStreamEventRow(page, anchorTitle);
    // Anchor the actual first intersecting row. A later row can move within
    // the viewport when its predecessor wraps without losing reading state.
    await positionStreamEvent(scroller, anchor, -12);
    const anchorId = await anchor.getAttribute('data-chat-event-id');
    expect(await firstIntersectingStreamEventId(scroller)).toBe(anchorId);
    const beforeOffset = await streamEventOffset(scroller, anchor);
    expect(await streamBottomDistance(scroller)).toBeGreaterThan(64);

    await page.setViewportSize({ width: 390, height: 740 });
    await expect(scroller).toBeVisible();
    await expectNoHorizontalOverflow(page, 'reflowed stream while reading');
    await expect
      .poll(async () => Math.abs((await streamEventOffset(scroller, anchor)) - beforeOffset))
      .toBeLessThanOrEqual(6);
    await expect(anchor).toBeInViewport({ ratio: 0.4 });
    expect(await firstIntersectingStreamEventId(scroller)).toBe(anchorId);
    await expect.poll(() => streamBottomDistance(scroller)).toBeGreaterThan(64);

    await scrollStreamToBottom(scroller);
    await expect.poll(() => streamBottomDistance(scroller)).toBeLessThanOrEqual(32);
    const latestEvent = await resolveStreamEventRow(
      page,
      `Coworker started ${MINIMAL_CHAT_COWORKER_DOING_TITLE}`,
    );
    await expect(latestEvent).toBeInViewport({ ratio: 0.5 });

    await page.setViewportSize({ width: 1040, height: 620 });
    await expect(scroller).toBeVisible();
    await expectNoHorizontalOverflow(page, 'reflowed stream at the end');
    await expect.poll(() => streamBottomDistance(scroller)).toBeLessThanOrEqual(64);
    await expect(latestEvent).toBeInViewport({ ratio: 0.5 });
  });

  test('switching chats restores canonical event scope and each chat scroll position', async ({
    page,
  }) => {
    await installMinimal(page);
    const scroller = streamScroller(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await scrollStreamToBottom(scroller);
    await scroller.evaluate((el) => {
      el.scrollTop = 0;
      el.dispatchEvent(new Event('scroll', { bubbles: true }));
    });
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    const alphaScrollTop = await scroller.evaluate((el) => el.scrollTop);

    await selectChat(page, 'Release Verification Gate');
    await expect(chatHeader(page, 'Release Verification Gate')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toBeVisible();
    await expect(workspaceSection(page).getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    // Authored event language is retained verbatim; the surrounding UI stays English.
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_AUTHORED_LANGUAGE_TITLE),
    ).toBeVisible();
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toHaveCount(0);
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL),
    ).toHaveCount(0);
    await expect(workspaceSection(page).getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(
      0,
    );

    await selectChat(page, 'Workspace UI Contract');
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toHaveCount(0);
    await expect
      .poll(() => scroller.evaluate((el) => el.scrollTop))
      .toBeLessThanOrEqual(alphaScrollTop + 8);
  });

  test('read-only backlog drawer exposes four committed states, blocked reason on demand, Escape focus restore, and no overflow', async ({
    page,
  }) => {
    await installMinimal(page);
    const trigger = backlogButton(page);
    const dialog = await openBacklog(page);
    await expect(dialog.getByText(MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL)).toHaveCount(0);
    await expect(dialog.getByText(MINIMAL_CHAT_MALFORMED_WORK_SENTINEL)).toHaveCount(0);
    await expect(dialog.getByText(MINIMAL_CHAT_BETA_BACKLOG_SENTINEL)).toHaveCount(0);
    await expect(dialog.getByText('Beta committed delivery')).toHaveCount(0);
    await expect(dialog.getByText(MINIMAL_CHAT_GAMMA_BACKLOG_SENTINEL)).toHaveCount(0);
    await expect(dialog.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);
    await expect(dialog.getByText(MINIMAL_CHAT_LEGACY_MESSAGE_SENTINEL)).toHaveCount(0);

    const expectedStates: Array<[string, CommittedBacklogStatus]> = [
      [MINIMAL_CHAT_TODO_TITLE, 'todo'],
      [MINIMAL_CHAT_COWORKER_DOING_TITLE, 'doing'],
      [MINIMAL_CHAT_DONE_TITLE, 'done'],
      [MINIMAL_CHAT_BLOCKED_TITLE, 'blocked'],
    ];
    for (const [title, status] of expectedStates) {
      await expectCommittedBacklogStatus(dialog, title, status);
      await expectNoInventedBacklogRowStatus(await resolveBacklogItemRow(dialog, title));
    }
    const committedStatuses = await dialog.locator('[data-status]').evaluateAll((nodes) =>
      Array.from(
        new Set(
          nodes
            .map((node) => (node.getAttribute('data-status') ?? '').toLowerCase())
            .filter((status) => status.length > 0),
        ),
      ).sort(),
    );
    expect(committedStatuses).toEqual(['blocked', 'doing', 'done', 'todo']);
    await expect(
      dialog.getByText(/^(?:working|preparing|awaiting check|failed)$/i),
    ).toHaveCount(0);

    await expect(
      dialog.getByRole('button', {
        name: /^(start|complete|edit|delete|block|unblock|mark done|create|add item)\b/i,
      }),
    ).toHaveCount(0);

    const blockedRow = await resolveBacklogItemRow(dialog, MINIMAL_CHAT_BLOCKED_TITLE);
    const reason = blockedRow.getByText(MINIMAL_CHAT_BLOCKED_REASON);
    if ((await reason.count()) > 0) {
      await expect(reason).toBeHidden();
    }
    const reasonSummary = blockedRow.locator('summary').first();
    await expect(reasonSummary).toBeVisible();
    await reasonSummary.click();
    await expect(blockedRow.getByText(MINIMAL_CHAT_BLOCKED_REASON)).toBeVisible();

    expect(await dialog.evaluate((el) => el.scrollWidth <= el.clientWidth + 1)).toBe(true);
    await expectNoHorizontalOverflow(page, 'wide backlog drawer');

    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
    await expect(trigger).toBeFocused();

    await page.setViewportSize({ width: 390, height: 740 });
    const narrowDialog = await openBacklog(page);
    await expect(
      resolveBacklogItemRow(narrowDialog, MINIMAL_CHAT_DONE_TITLE),
    ).toBeVisible();
    const narrowBox = await narrowDialog.boundingBox();
    expect(narrowBox).not.toBeNull();
    expect(narrowBox!.width).toBeLessThanOrEqual(390);
    expect(await narrowDialog.evaluate((el) => el.scrollWidth <= el.clientWidth + 1)).toBe(
      true,
    );
    await expectNoHorizontalOverflow(page, 'narrow backlog drawer');
    await closeBacklogWithEscape(page);
    await expect(backlogButton(page)).toBeFocused();
  });

  test('keeps drawer item titles readable in light and dark themes after finite animation, restores focus, and avoids overflow', async ({
    page,
  }) => {
    const drawerTitles = [
      MINIMAL_CHAT_TODO_TITLE,
      MINIMAL_CHAT_COWORKER_DOING_TITLE,
      MINIMAL_CHAT_DONE_TITLE,
      MINIMAL_CHAT_BLOCKED_TITLE,
    ];

    await installMinimal(
      page,
      { width: 1040, height: 760 },
      { appearanceTheme: 'light' },
    );
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
    let trigger = backlogButton(page);
    let dialog = await openBacklog(page);
    await waitForFiniteAnimations(dialog);
    for (const title of drawerTitles) {
      await expectReadableDrawerTitle(dialog, title);
    }
    expect(await dialog.evaluate((el) => el.scrollWidth <= el.clientWidth + 1)).toBe(true);
    await expectNoHorizontalOverflow(page, 'light theme backlog drawer');
    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
    await expect(dialog).toBeHidden();
    await expect(trigger).toBeFocused();

    await page.evaluate(
      ({ key, value }) => window.localStorage.setItem(key, value),
      { key: APPEARANCE_STORAGE_KEY, value: minimalAppearanceStorage('dark') },
    );
    await page.reload();
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

    trigger = backlogButton(page);
    dialog = await openBacklog(page);
    await waitForFiniteAnimations(dialog);
    for (const title of drawerTitles) {
      await expectReadableDrawerTitle(dialog, title);
    }
    expect(await dialog.evaluate((el) => el.scrollWidth <= el.clientWidth + 1)).toBe(true);
    await expectNoHorizontalOverflow(page, 'dark theme backlog drawer');
    await page.keyboard.press('Escape');
    await expect(dialog).toHaveCount(0);
    await expect(dialog).toBeHidden();
    await expect(trigger).toBeFocused();
  });

  test('resizes between wide and narrow while reading without forcing the bottom or horizontal overflow', async ({
    page,
  }) => {
    await installMinimal(page);
    const scroller = streamScroller(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    const anchor = await resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE);
    await positionStreamEvent(scroller, anchor, -12);
    const anchorId = await anchor.getAttribute('data-chat-event-id');
    expect(await firstIntersectingStreamEventId(scroller)).toBe(anchorId);
    const beforeBox = await anchor.boundingBox();
    const beforeOffset = await streamEventOffset(scroller, anchor);
    expect(beforeBox).not.toBeNull();

    await page.setViewportSize({ width: 390, height: 740 });
    await expect(scroller).toBeVisible();
    await expectNoHorizontalOverflow(page, 'resized narrow stream');
    await expect(anchor).toBeVisible();
    // Reflow may legitimately change scrollTop to keep the same event at the
    // same pixel offset. Assert that anchor invariant instead of a frozen
    // absolute scrollTop, and prove the resize did not force the bottom.
    await expect
      .poll(async () =>
        Math.abs((await streamEventOffset(scroller, anchor)) - beforeOffset),
      )
      .toBeLessThanOrEqual(6);
    await expect(anchor).toBeInViewport({ ratio: 0.5 });
    expect(await firstIntersectingStreamEventId(scroller)).toBe(anchorId);
    await expect.poll(() => streamBottomDistance(scroller)).toBeGreaterThan(64);

    await page.setViewportSize({ width: 1040, height: 760 });
    await expect(scroller).toBeVisible();
    await expectNoHorizontalOverflow(page, 'resized wide stream');
    await expect(anchor).toBeVisible();
    await expect
      .poll(async () =>
        Math.abs((await streamEventOffset(scroller, anchor)) - beforeOffset),
      )
      .toBeLessThanOrEqual(6);
    await expect(anchor).toBeInViewport({ ratio: 0.5 });
    expect(await firstIntersectingStreamEventId(scroller)).toBe(anchorId);
    await expect.poll(() => streamBottomDistance(scroller)).toBeGreaterThan(64);
  });

  test('archives and restores individual chats by canonical four-part WorkspaceChatIdentity and ignores forged keys', async ({
    page,
  }) => {
    const forgedStoragePayload = JSON.stringify({
      job_gamma_01: {
        chat_key: 'job_gamma_01',
        identity: CHAT_GAMMA_IDENTITY,
        lifecycle: 'archived',
        updated_at_ms: FIXTURE_BASE_NOW_MS,
      },
      'Archived Migration Audit': {
        chat_key: 'Archived Migration Audit',
        identity: CHAT_GAMMA_IDENTITY,
        lifecycle: 'archived',
        updated_at_ms: FIXTURE_BASE_NOW_MS,
      },
    });

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshot(),
      initialLocalStorage: {
        [WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY]: forgedStoragePayload,
      },
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const sidebarEl = sidebar(page);
    await expect(sidebarEl).toBeVisible();

    // Forged keys keyed by job_id or chat title must not archive the chat.
    await expect(sidebarEl.getByText('Archived Migration Audit').first()).toBeVisible();
    await expect(sidebarEl.getByText('Workspace UI Contract').first()).toBeVisible();
    await expect(sidebarEl.getByText('Release Verification Gate').first()).toBeVisible();

    await sidebarEl.getByText('Archived Migration Audit').first().click();
    await expect(
      resolveStreamEventRow(page, `Backlog item created: ${MINIMAL_CHAT_GAMMA_BACKLOG_SENTINEL}`),
    ).toBeVisible();

    const archiveActionBtn = sidebarEl.getByRole('button', {
      name: 'Archive Archived Migration Audit',
      exact: true,
    });
    await expect(archiveActionBtn).toBeVisible();
    await archiveActionBtn.click();

    // Gamma leaves Active while Alpha and Beta remain in Active. Archiving the
    // selected chat may move selection, so require a real selected chat and
    // explicitly select Alpha before continuing with lifecycle navigation.
    await expect(sidebarEl.getByText('Archived Migration Audit')).toHaveCount(0);
    await expect(sidebarEl.getByText('Workspace UI Contract').first()).toBeVisible();
    await expect(sidebarEl.getByText('Release Verification Gate').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    const storedRaw = await controller.readArchivePreferencesRaw();
    expect(storedRaw).not.toBeNull();
    const storedMap = JSON.parse(storedRaw!) as Record<string, { lifecycle?: string }>;
    expect(storedMap[CHAT_GAMMA_KEY]?.lifecycle).toBe('archived');

    // Switch Projects/Archive subnav separately and verify Gamma appears there with Restore control.
    const archiveFilter = lifecycleFilterControl(page, 'archived');
    await expect(archiveFilter).toBeVisible();
    await archiveFilter.click();

    await expect(sidebarEl.getByText('Archived Migration Audit').first()).toBeVisible();
    await expect(sidebarEl.getByText('Workspace UI Contract')).toHaveCount(0);

    await sidebarEl.getByText('Archived Migration Audit').first().click();
    const restoreBtn = sidebarEl.getByRole('button', {
      name: 'Restore Archived Migration Audit',
      exact: true,
    });
    await expect(restoreBtn).toBeVisible();
    await restoreBtn.click();

    await expect(sidebarEl.getByText('Archived Migration Audit')).toHaveCount(0);

    const activeFilter = lifecycleFilterControl(page, 'active');
    await activeFilter.click();
    await expect(sidebarEl.getByText('Archived Migration Audit').first()).toBeVisible();

    const restoredRaw = await controller.readArchivePreferencesRaw();
    const restoredMap = restoredRaw ? (JSON.parse(restoredRaw) as Record<string, unknown>) : {};
    expect(restoredMap[CHAT_GAMMA_KEY]).toBeUndefined();

    // Lifecycle navigation is allowed to change selection. Require an actual
    // selected chat and prove Alpha still opens its canonical stream.
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(workspaceSection(page).getByText(MINIMAL_CHAT_GAMMA_BACKLOG_SENTINEL)).toHaveCount(
      0,
    );
  });
});

test.describe('Workspace liveness, attention, and committed completion', () => {
  test('shows actual fresh Worker and Coworker Working in live agents, clears Worker on expiry and failed refresh, and keeps drawer rows committed', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    const workspace = workspaceSection(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const workingSnapshot = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      'working',
    );
    await controller.setWorkspaceSnapshot(workingSnapshot);
    await triggerWorkspaceRefresh(page);

    const header = chatHeader(page, 'Workspace UI Contract');
    await expectVisibleWorking(header);
    await expect(liveAgentList(workspace)).toBeVisible();
    await expect(liveAgentList(workspace).locator('li[data-agent-role]')).toHaveCount(2);
    await expectLiveAgentWorking(workspace, 'Worker');
    await expectLiveAgentWorking(workspace, 'Coworker');

    let dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_PREPARING_TITLE, 'doing');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_FAILED_TITLE, 'blocked');
    await expectCommittedBacklogStatus(
      dialog,
      MINIMAL_CHAT_COWORKER_DOING_TITLE,
      'doing',
    );
    for (const title of [
      MINIMAL_CHAT_DOING_TITLE,
      MINIMAL_CHAT_PREPARING_TITLE,
      MINIMAL_CHAT_FAILED_TITLE,
      MINIMAL_CHAT_COWORKER_DOING_TITLE,
    ]) {
      await expectNoInventedBacklogRowStatus(await resolveBacklogItemRow(dialog, title));
    }
    await closeBacklogWithEscape(page);

    // The fresh Worker presence expires at a frozen wall-clock instant. The
    // owned UI age clock must clear Worker Working without manufacturing a read.
    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 6_000);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectLiveAgentWorking(workspace, 'Coworker');
    // The Coworker remains working because its native sample is still fresh.
    await expectVisibleWorking(header);

    // Reset to a fresh Worker sample at the fixture instant, then fail the next
    // refresh: the failed read must clear Worker live rather than retain proof.
    await controller.setWorkspaceSnapshot(workingSnapshot);
    await triggerWorkspaceRefresh(page);
    await expectLiveAgentWorking(workspace, 'Worker');
    const readsBeforeFailedRefresh = (await controller.getInvocations()).filter(
      (call) => call.cmd === 'desktop_workspace_snapshot',
    ).length;
    await controller.failNextWorkspaceSnapshotRead();
    await triggerWorkspaceRefresh(page);
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.filter((call) => call.cmd === 'desktop_workspace_snapshot').length;
      })
      .toBeGreaterThan(readsBeforeFailedRefresh);
    await expectNoLiveAgentWorking(workspace, 'Worker');

    dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await closeBacklogWithEscape(page);
    await expectNoNeedsYou(page);
  });

  test('clears Worker live when a refresh omits the exact current execution presence', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    const workspace = workspaceSection(page);
    const header = chatHeader(page, 'Workspace UI Contract');
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const workingSnapshot = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      'working',
    );
    await controller.setWorkspaceSnapshot(workingSnapshot);
    await triggerWorkspaceRefresh(page);

    await expectVisibleWorking(header);
    await expectLiveAgentWorking(workspace, 'Worker');
    let dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await closeBacklogWithEscape(page);

    // A fresh live sample for a different execution reference must not prove
    // the current item execution is running.
    const mismatchedPresence = updateChatWorkPresenceFixture(
      workingSnapshot,
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: 'exec_mismatched_current_99',
        }),
      ],
    );
    await controller.setWorkspaceSnapshot(mismatchedPresence);
    await triggerWorkspaceRefresh(page);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectLiveAgentWorking(workspace, 'Coworker');
    await expectVisibleWorking(header);
    dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await closeBacklogWithEscape(page);

    await controller.setWorkspaceSnapshot(workingSnapshot);
    await triggerWorkspaceRefresh(page);
    await expectLiveAgentWorking(workspace, 'Worker');
    await expectVisibleWorking(header);
    dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await closeBacklogWithEscape(page);

    const unavailablePresence = updateChatWorkPresenceFixture(
      workingSnapshot,
      CHAT_ALPHA_IDENTITY,
      [],
    );
    await controller.setWorkspaceSnapshot(unavailablePresence);
    await triggerWorkspaceRefresh(page);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectLiveAgentWorking(workspace, 'Coworker');
    await expectVisibleWorking(header);
    dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await closeBacklogWithEscape(page);
    await expectNoNeedsYou(page);
  });

  test('derives one NeedsYou only from actual native waiting and never from Blocked, Doing, failed, or awaiting-check work', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    const initialDialog = await openBacklog(page);
    await expect(
      resolveBacklogItemRow(initialDialog, MINIMAL_CHAT_BLOCKED_TITLE),
    ).toBeVisible();
    await closeBacklogWithEscape(page);
    await expectNoNeedsYou(page);

    // Legacy attention counts are compatibility data, not an actual open request.
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const legacyAttentionSnapshot = parseWorkspaceSnapshot({
      ...baseSnapshot!,
      chats: baseSnapshot!.chats.map((chat) =>
        workspaceChatIdentitiesEqual(chat.identity, CHAT_ALPHA_IDENTITY)
          ? {
              ...chat,
              thread_label: 'Legacy Attention Compatibility Chat',
              active_job_count: 3,
              attention_count: 5,
            }
          : chat,
      ),
    });
    await controller.setWorkspaceSnapshot(legacyAttentionSnapshot);
    const readsBeforeLegacyAttentionRefresh = (await controller.getInvocations()).filter(
      (call) => call.cmd === 'desktop_workspace_snapshot',
    ).length;
    await triggerWorkspaceRefresh(page);
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.filter((call) => call.cmd === 'desktop_workspace_snapshot').length;
      })
      .toBeGreaterThan(readsBeforeLegacyAttentionRefresh);
    await expect(
      chatHeader(page, 'Legacy Attention Compatibility Chat'),
    ).toBeVisible();
    await expectNoNeedsYou(page);
    expect(
      await page
        .getByText(/needs attention/i)
        .evaluateAll((nodes) => nodes.filter((node) => node.checkVisibility()).length),
    ).toBe(0);

    const waitingApproval = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      'waiting_for_approval',
    );
    await controller.setWorkspaceSnapshot(waitingApproval);
    await triggerWorkspaceRefresh(page);
    await expectSingleNeedsYou(page);
    await expect(workspaceSection(page).getByText(/q_[A-Za-z0-9_]+/)).toHaveCount(0);

    const waitingInput = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      'waiting_for_input',
    );
    await controller.setWorkspaceSnapshot(waitingInput);
    await triggerWorkspaceRefresh(page);
    await expectSingleNeedsYou(page);

    // A stale native sample is not an actual current request.
    await page.clock.setFixedTime(
      FIXTURE_BASE_NOW_MS + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 1_001,
    );
    await expect(chatHeader(page, 'Workspace UI Contract').locator('.tl-cw-status')).toHaveCount(0);
    await expectNoLiveAgentWorking(workspaceSection(page), 'Coworker');
    await expectNoNeedsYou(page);

    const working = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      'working',
    );
    await controller.setWorkspaceSnapshot(working);
    await triggerWorkspaceRefresh(page);
    await expectVisibleWorking(chatHeader(page, 'Workspace UI Contract'));
    await expectNoNeedsYou(page);
  });

  test('marks only actual committed Done while reservation is Preparing and successful outcome is Awaiting check', async ({
    page,
  }) => {
    await installMinimal(page);
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    // The live Worker presence makes the chat header and live-agent list Working,
    // while the drawer below still shows only committed workflow states.
    await expectVisibleWorking(chatHeader(page, 'Workspace UI Contract'));
    await expectLiveAgentWorking(workspaceSection(page), 'Worker');
    await expectNoLiveAgentWorking(workspaceSection(page), 'Coworker');
    const dialog = await openBacklog(page);

    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DONE_TITLE, 'done');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_TODO_TITLE, 'todo');
    await expectCommittedBacklogStatus(
      dialog,
      MINIMAL_CHAT_COWORKER_DOING_TITLE,
      'doing',
    );
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_PREPARING_TITLE, 'doing');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_FAILED_TITLE, 'blocked');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_BLOCKED_TITLE, 'blocked');

    // Preparing, Awaiting check, and Failed are stream execution actions, never
    // invented drawer row states. Only committed Todo/Doing/Done/Blocked appear.
    for (const title of [
      MINIMAL_CHAT_DOING_TITLE,
      MINIMAL_CHAT_PREPARING_TITLE,
      MINIMAL_CHAT_FAILED_TITLE,
    ]) {
      const row = await resolveBacklogItemRow(dialog, title);
      await expectNoInventedBacklogRowStatus(row);
      await expect(row.getByText(/^done$/i)).toHaveCount(0);
    }
    await expect(dialog.getByText(/^(?:working|preparing|awaiting check|failed)$/i)).toHaveCount(0);

    await expect(
      resolveStreamEventRow(
        page,
        `Preparing Worker execution for ${MINIMAL_CHAT_PREPARING_TITLE}`,
      ),
    ).toBeVisible();
    await expect(
      resolveStreamEventRow(
        page,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_DOING_TITLE}`,
      ),
    ).toBeVisible();
    await expect(
      resolveStreamEventRow(
        page,
        `Worker result recorded: Failed for ${MINIMAL_CHAT_FAILED_TITLE}`,
      ),
    ).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await closeBacklogWithEscape(page);
  });

  test('withholds live native indicators for Stale and Disconnected samples without resetting sample age or hiding durable events', async ({
    page,
  }) => {
    const base = buildMinimalChatWorkspaceSnapshot();
    const staleSnapshot = parseWorkspaceSnapshot({
      ...base,
      generated_at_ms: base.generated_at_ms + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 15_000,
    });
    const baseAlphaSample = base.native_samples.find((sample) =>
      workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY),
    );
    expect(baseAlphaSample).toBeDefined();
    const disconnectedAtMs = staleSnapshot.generated_at_ms + 5_000;
    const disconnectedSnapshot = parseWorkspaceSnapshot({
      ...base,
      generated_at_ms: disconnectedAtMs,
      native_samples: base.native_samples.map((sample) =>
        workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY)
          ? {
              ...sample,
              availability: 'disconnected',
              activity: 'unknown',
              connection: {
                ...sample.connection,
                state: 'disconnected',
                freshness: 'stale',
                diagnostic: 'Native Codex bridge socket closed; durable events remain readable.',
              },
              configured_selection: {
                ...sample.configured_selection,
                evidence: 'unavailable',
              },
            }
          : sample,
      ),
    });

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: staleSnapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');

    expect(await page.evaluate(() => Date.now())).toBe(staleSnapshot.generated_at_ms);
    const header = chatHeader(page, 'Workspace UI Contract');
    await expect(header.locator('.tl-cw-status')).toHaveCount(0);
    await expectNoVisibleWorking(header);
    await expectNoNeedsYou(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    await controller.setWorkspaceSnapshot(disconnectedSnapshot);
    await triggerWorkspaceRefresh(page);

    await expect(header.locator('.tl-cw-status')).toHaveCount(0);
    await expectNoVisibleWorking(header);
    await expectNoNeedsYou(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    const storedAfterDisconnect = await controller.getWorkspaceSnapshot();
    const storedAlphaSample = storedAfterDisconnect?.native_samples.find((sample) =>
      workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY),
    );
    expect(storedAlphaSample?.last_success_at_ms).toBe(baseAlphaSample!.last_success_at_ms);

    const dialog = await openBacklog(page);
    await expect(resolveBacklogItemRow(dialog, MINIMAL_CHAT_DONE_TITLE)).toBeVisible();
    await closeBacklogWithEscape(page);
  });

  test('transitions a fresh native working chat to Stale via the owned 1s UI age clock when one snapshot poll stalls', async ({
    page,
  }) => {
    const workingSnapshot = updateNativeActivityFixture(
      buildMinimalChatWorkspaceSnapshot(),
      CHAT_ALPHA_IDENTITY,
      'working',
    );
    const baseAlphaSample = workingSnapshot.native_samples.find((sample) =>
      workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY),
    );
    expect(baseAlphaSample).toBeDefined();

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: workingSnapshot,
    });

    try {
      await page.setViewportSize({ width: 1040, height: 760 });
      await page.goto('/');
      await selectChat(page, 'Workspace UI Contract');

      const header = chatHeader(page, 'Workspace UI Contract');
      await expect(header).toBeVisible();
      await expectVisibleWorking(header);

      const initialSnapshotReads = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      expect(initialSnapshotReads).toBe(1);

      await controller.stallNextWorkspaceSnapshotRead();
      await expect
        .poll(() => controller.getStalledWorkspaceSnapshotCount(), { timeout: 5_000 })
        .toBe(1);

      const snapshotReadsAtStall = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      expect(snapshotReadsAtStall).toBe(2);
      await expect(page.locator('[role="alert"]')).toHaveCount(0);

      await page.clock.setFixedTime(workingSnapshot.generated_at_ms + STALE_ADVANCE_MS);

      await expect(header.locator('.tl-cw-status')).toHaveCount(0, { timeout: 4_000 });
      await expectNoVisibleWorking(header);
      await expect(
        resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
      ).toBeVisible();
      await expect(page.locator('[role="alert"]')).toHaveCount(0);

      expect(await controller.getStalledWorkspaceSnapshotCount()).toBe(1);
      const invocationsAfterAgeOut = await controller.getInvocations();
      expect(
        invocationsAfterAgeOut.filter((call) => call.cmd === 'desktop_workspace_snapshot').length,
      ).toBe(snapshotReadsAtStall);
      expect(
        invocationsAfterAgeOut.filter((call) => call.cmd === 'desktop_observation_snapshot').length,
      ).toBe(0);

      const storedAfterAgeOut = await controller.getWorkspaceSnapshot();
      expect(storedAfterAgeOut?.generated_at_ms).toBe(workingSnapshot.generated_at_ms);
      const storedAlphaSample = storedAfterAgeOut?.native_samples.find((sample) =>
        workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY),
      );
      expect(storedAlphaSample?.observed_at_ms).toBe(baseAlphaSample!.observed_at_ms);
      expect(storedAlphaSample?.last_success_at_ms).toBe(baseAlphaSample!.last_success_at_ms);
    } finally {
      await controller.releaseStalledWorkspaceSnapshotReads().catch(() => {});
    }
  });

  test('distinguishes historical failure, Blocked, Doing, and awaiting-check work from actual native waiting attention', async ({
    page,
  }) => {
    const controller = await installMinimal(page);
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();

    const workspace = workspaceSection(page);
    await expect(workspace.getByText(`Blocked: ${MINIMAL_CHAT_BLOCKED_TITLE}`)).toBeVisible();
    await expect(
      workspace.getByText(`Worker result recorded: Failed for ${MINIMAL_CHAT_FAILED_TITLE}`),
    ).toBeVisible();
    await expect(
      workspace.getByText(`Worker result recorded: Awaiting check for ${MINIMAL_CHAT_DOING_TITLE}`),
    ).toBeVisible();

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_FAILED_TITLE, 'blocked');
    const failedRow = await resolveBacklogItemRow(dialog, MINIMAL_CHAT_FAILED_TITLE);
    await expectNoInventedBacklogRowStatus(failedRow);
    await expect(failedRow.getByText(/^done$/i)).toHaveCount(0);
    await closeBacklogWithEscape(page);
    await expectNoNeedsYou(page);

    const betaWaiting = updateNativeActivityFixture(
      baseSnapshot!,
      CHAT_BETA_SAME_PROJECT_IDENTITY,
      'waiting_for_approval',
    );
    await controller.setWorkspaceSnapshot(betaWaiting);
    await triggerWorkspaceRefresh(page);
    await selectChat(page, 'Release Verification Gate');
    await expectSingleNeedsYou(page);
  });

  test('does not report legacy pending acceptance as Done and keeps actual committed Done separate', async ({
    page,
  }) => {
    await installMinimal(page);
    const workspace = workspaceSection(page);
    await expect(workspace.getByText(/verified accepted/i)).toHaveCount(0);

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DOING_TITLE, 'doing');
    const doingRow = await resolveBacklogItemRow(dialog, MINIMAL_CHAT_DOING_TITLE);
    await expectNoInventedBacklogRowStatus(doingRow);
    await expect(doingRow.getByText(/^done$/i)).toHaveCount(0);
    await expect(
      resolveStreamEventRow(
        page,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_DOING_TITLE}`,
      ),
    ).toBeVisible();

    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_DONE_TITLE, 'done');
    await closeBacklogWithEscape(page);
  });
});

test.describe('Workspace identity isolation, navigation, and archive usability', () => {
  test('enforces four-part WorkspaceChatIdentity scope isolation and passes exact Open in Codex targets', async ({
    page,
  }) => {
    const controller = await installMinimal(page);

    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(workspaceSection(page).getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(workspaceSection(page).getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL),
    ).toHaveCount(0);

    const header = chatHeader(page, 'Workspace UI Contract');
    const openCodexBtn = header.getByRole('button', { name: /^open in codex$/i });
    await expect(openCodexBtn).toBeVisible();
    await expect(openCodexBtn).toBeEnabled();

    await controller.clearInvocations();
    await openCodexBtn.click();
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.some(
          (call) =>
            call.cmd === 'desktop_open_codex_chat' &&
            matchesExactOpenCodexInvocation(call.args, {
              source_id: CHAT_ALPHA_IDENTITY.source_id,
              thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
              job_id: null,
            }),
        );
      })
      .toBe(true);
    await expect(page.locator('[role="alert"]')).toHaveCount(0);

    await selectChat(page, 'Release Verification Gate');
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toBeVisible();
    await expect(workspaceSection(page).getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    // Authored event language is retained verbatim; the surrounding UI stays English.
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_AUTHORED_LANGUAGE_TITLE),
    ).toBeVisible();
    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toHaveCount(0);
    await expect(workspaceSection(page).getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await controller.clearInvocations();
    await chatHeader(page, 'Release Verification Gate')
      .getByRole('button', { name: /^open in codex$/i })
      .click();
    await expect
      .poll(async () => {
        const calls = await controller.getInvocations();
        return calls.some(
          (call) =>
            call.cmd === 'desktop_open_codex_chat' &&
            matchesExactOpenCodexInvocation(call.args, {
              source_id: CHAT_BETA_SAME_PROJECT_IDENTITY.source_id,
              thread_uuid: CHAT_BETA_SAME_PROJECT_IDENTITY.thread_uuid,
              job_id: null,
            }),
        );
      })
      .toBe(true);
    await expect(page.locator('[role="alert"]')).toHaveCount(0);
  });

  test('quarantines same-thread work that differs by only source, binding, or project scope', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildFourPartScopeCollisionWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    // Prove the three probes are valid identities that vary exactly one part.
    expect(FOUR_PART_SOURCE_ONLY_IDENTITY.source_id).not.toBe(
      CHAT_ALPHA_IDENTITY.source_id,
    );
    expect(FOUR_PART_SOURCE_ONLY_IDENTITY.binding_id).toBe(CHAT_ALPHA_IDENTITY.binding_id);
    expect(FOUR_PART_SOURCE_ONLY_IDENTITY.thread_uuid).toBe(CHAT_ALPHA_IDENTITY.thread_uuid);
    expect(FOUR_PART_SOURCE_ONLY_IDENTITY.canonical_project_scope).toBe(
      CHAT_ALPHA_IDENTITY.canonical_project_scope,
    );

    expect(FOUR_PART_BINDING_ONLY_IDENTITY.source_id).toBe(CHAT_ALPHA_IDENTITY.source_id);
    expect(FOUR_PART_BINDING_ONLY_IDENTITY.binding_id).not.toBe(
      CHAT_ALPHA_IDENTITY.binding_id,
    );
    expect(FOUR_PART_BINDING_ONLY_IDENTITY.thread_uuid).toBe(
      CHAT_ALPHA_IDENTITY.thread_uuid,
    );
    expect(FOUR_PART_BINDING_ONLY_IDENTITY.canonical_project_scope).toBe(
      CHAT_ALPHA_IDENTITY.canonical_project_scope,
    );

    expect(FOUR_PART_PROJECT_ONLY_IDENTITY.source_id).toBe(CHAT_ALPHA_IDENTITY.source_id);
    expect(FOUR_PART_PROJECT_ONLY_IDENTITY.binding_id).toBe(
      CHAT_ALPHA_IDENTITY.binding_id,
    );
    expect(FOUR_PART_PROJECT_ONLY_IDENTITY.thread_uuid).toBe(
      CHAT_ALPHA_IDENTITY.thread_uuid,
    );
    expect(FOUR_PART_PROJECT_ONLY_IDENTITY.canonical_project_scope).not.toBe(
      CHAT_ALPHA_IDENTITY.canonical_project_scope,
    );

    const workspace = workspaceSection(page);
    const sentinels = [
      FOUR_PART_SOURCE_ONLY_SENTINEL,
      FOUR_PART_BINDING_ONLY_SENTINEL,
      FOUR_PART_PROJECT_ONLY_SENTINEL,
    ];
    for (const sentinel of sentinels) {
      await expect(workspace.getByText(sentinel)).toHaveCount(0);
    }
    await expectLiveAgentWorking(workspace, 'Worker');
    await expectNoLiveAgentWorking(workspace, 'Coworker');
    // Native waiting samples for valid one-part variants may normalize as their
    // own chats, but none of that attention may leak into the Alpha selection.
    await expectNoNeedsYouInScope(workspace);
    await expectNoNeedsYouInScope(chatHeader(page, 'Workspace UI Contract'));

    const dialog = await openBacklog(page);
    for (const sentinel of sentinels) {
      await expect(dialog.getByText(sentinel)).toHaveCount(0);
    }
    await closeBacklogWithEscape(page);

    await selectChat(page, 'Release Verification Gate');
    await expect(chatHeader(page, 'Release Verification Gate')).toBeVisible();
    const betaWorkspace = workspaceSection(page);
    for (const sentinel of sentinels) {
      await expect(betaWorkspace.getByText(sentinel)).toHaveCount(0);
    }
    await expectNoLiveAgentWorking(betaWorkspace, 'Worker');
    await expectNoLiveAgentWorking(betaWorkspace, 'Coworker');
    await expectNoNeedsYouInScope(betaWorkspace);
    await expectNoNeedsYouInScope(chatHeader(page, 'Release Verification Gate'));
  });

  test('keeps Active/Archive, sidebar collapse, settings/tools/profiles usable, and quarantines wrong-scope/malformed work', async ({
    page,
  }) => {
    await installMinimal(page);
    const workspace = workspaceSection(page);
    await expect(workspace.getByText(MINIMAL_CHAT_WRONG_SCOPE_WORK_SENTINEL)).toHaveCount(0);
    await expect(workspace.getByText(MINIMAL_CHAT_MALFORMED_WORK_SENTINEL)).toHaveCount(0);
    await expect(workspace.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    const sidebarEl = sidebar(page);
    await sidebarEl
      .getByRole('button', { name: 'Archive Release Verification Gate', exact: true })
      .click();
    await expect(sidebarEl.getByText('Release Verification Gate')).toHaveCount(0);

    await lifecycleFilterControl(page, 'archived').click();
    await expect(sidebarEl.getByText('Release Verification Gate').first()).toBeVisible();
    await sidebarEl
      .getByRole('button', { name: 'Restore Release Verification Gate', exact: true })
      .click();
    await lifecycleFilterControl(page, 'active').click();
    await expect(sidebarEl.getByText('Release Verification Gate').first()).toBeVisible();

    await page.getByRole('button', { name: /collapse sidebar/i }).first().click();
    await expect
      .poll(async () => (await sidebarEl.boundingBox())?.width ?? 0)
      .toBeLessThanOrEqual(72);
    await page.getByRole('button', { name: /expand sidebar/i }).first().click();
    await expect
      .poll(async () => (await sidebarEl.boundingBox())?.width ?? 0)
      .toBeGreaterThanOrEqual(220);

    for (const label of ['Settings', 'Tools', 'Profiles'] as const) {
      await sidebarEl.getByRole('button', { name: label, exact: true }).click();
      const sectionSelector =
        label === 'Tools'
          ? 'section[aria-label="Tools and models"], section[aria-label="Tools"]'
          : `section[aria-label="${label}"]`;
      await expect(page.locator(sectionSelector).first()).toBeVisible();
    }
    await sidebarEl.getByRole('button', { name: 'Projects', exact: true }).click();
    // Returning from settings/tools/profiles does not promise which chat the
    // lifecycle view left selected. Explicitly select Alpha and require its
    // real stream and drawer control rather than assuming prior selection.
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(backlogButton(page)).toBeVisible();
  });
});

test.describe('Legacy compatibility safety', () => {
  test('quarantines malformed chat_work without hiding the selected chat stream or drawer', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshotWithMalformedWork(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    await expect(
      workspaceSection(page).getByText(MINIMAL_CHAT_MALFORMED_WORK_SENTINEL),
    ).toHaveCount(0);
    await expectNoNeedsYou(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    const dialog = await openBacklog(page);
    await expect(dialog.getByText(MINIMAL_CHAT_MALFORMED_WORK_SENTINEL)).toHaveCount(0);
    await expect(resolveBacklogItemRow(dialog, MINIMAL_CHAT_DONE_TITLE)).toBeVisible();
    await closeBacklogWithEscape(page);
  });

  test('legacy external resolution receipts do not create normal-chat events, backlog items, or NeedsYou', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildExternalResolutionWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'External Resolution Chat');
    await expect(chatHeader(page, 'External Resolution Chat')).toBeVisible();

    const workspace = workspaceSection(page);
    await expect(workspace.getByRole('tab')).toHaveCount(0);
    await expect(workspace.locator('[role="tablist"]')).toHaveCount(0);
    await expect(
      workspace.getByRole('tab', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(
      workspace.getByRole('button', { name: /^(messages|jobs|activity)\b/i }),
    ).toHaveCount(0);
    await expect(workspace.getByText(EXTERNAL_RESOLUTION_FULL_SUMMARY)).toHaveCount(0);
    await expect(workspace.getByText(EXTERNAL_RESOLUTION_UNRELATED_QUESTION)).toHaveCount(0);
    await expect(workspace.getByText(/resolved externally/i)).toHaveCount(0);
    await expect(workspace.getByText(/external resolution receipts/i)).toHaveCount(0);
    await expectNoNeedsYou(page);

    const dialog = await openBacklog(page);
    await expect(dialog.locator('[data-backlog-item-id], [data-item-id]')).toHaveCount(0);
    await expect(dialog.getByText(/Externally Resolved Full Job/)).toHaveCount(0);
    await expect(dialog.getByText(EXTERNAL_RESOLUTION_FULL_SUMMARY)).toHaveCount(0);
    await expect(dialog.getByText(EXTERNAL_RESOLUTION_UNRELATED_QUESTION)).toHaveCount(0);
    await closeBacklogWithEscape(page);
  });

  test('legacy historical failure and job attention fields never become NeedsYou or Done', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildHistoricalFailureWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    await expectNoNeedsYou(page);
    await expect(workspaceSection(page).getByRole('tab')).toHaveCount(0);
    await expect(workspaceSection(page).getByText(/^done$/i)).toHaveCount(0);
    await expect(workspaceSection(page).getByText(/Alpha message #36/)).toHaveCount(0);

    const dialog = await openBacklog(page);
    await expect(dialog.locator('[data-backlog-item-id], [data-item-id]')).toHaveCount(0);
    await expect(dialog.getByText(/Recovered Retry Pipeline|Plan Approval Gate/)).toHaveCount(0);
    await closeBacklogWithEscape(page);
  });

  test('legacy pending acceptance fields never fabricate Done or a human request', async ({
    page,
  }) => {
    const pendingSnapshot = buildPendingAcceptanceWorkspaceSnapshot();
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: pendingSnapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    await expectNoNeedsYou(page);
    await expect(workspaceSection(page).getByRole('tab')).toHaveCount(0);
    await expect(workspaceSection(page).getByText(/^done$/i)).toHaveCount(0);
    await expect(workspaceSection(page).getByText(/verified accepted/i)).toHaveCount(0);
    await expect(
      workspaceSection(page).getByText(/awaiting review and Closure acceptance/i),
    ).toHaveCount(0);

    const dialog = await openBacklog(page);
    await expect(dialog.locator('[data-backlog-item-id], [data-item-id]')).toHaveCount(0);
    await expect(
      dialog.getByText(/Worker Finished Pending Acceptance Slice|Closure Verified Package Slice/),
    ).toHaveCount(0);
    await closeBacklogWithEscape(page);

    // Positive control: actual native waiting on the same legacy snapshot does
    // create exactly one real request, proving the no-NeedsYou result above
    // came from the controlled idle sample rather than a broken detector.
    const nativeWaiting = updateNativeActivityFixture(
      pendingSnapshot,
      CHAT_ALPHA_IDENTITY,
      'waiting_for_input',
    );
    await controller.setWorkspaceSnapshot(nativeWaiting);
    await triggerWorkspaceRefresh(page);
    await expectSingleNeedsYou(page);
    await expect(workspaceSection(page).getByText(/^done$/i)).toHaveCount(0);
    await expect(workspaceSection(page).getByText(/verified accepted/i)).toHaveCount(0);
  });
});
