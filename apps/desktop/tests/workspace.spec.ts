import { expect, type Locator, type Page, test } from '@playwright/test';
import {
  BETA_CHAT_ONLY_SENTINEL,
  buildDisconnectedWorkspaceSnapshot,
  buildExternalResolutionWorkspaceSnapshot,
  buildHistoricalFailureWorkspaceSnapshot,
  buildInteractiveWorkspaceSnapshot,
  buildPendingAcceptanceWorkspaceSnapshot,
  buildStaleWorkspaceSnapshot,
  CHAT_ALPHA_IDENTITY,
  CHAT_BETA_SAME_PROJECT_IDENTITY,
  CHAT_GAMMA_IDENTITY,
  CHAT_GAMMA_KEY,
  createChatMessageItem,
  EXTERNAL_RESOLUTION_COVERED_JOB_REASON,
  EXTERNAL_RESOLUTION_COVERED_TASK_REASON,
  EXTERNAL_RESOLUTION_FULL_RECEIPT_ID,
  EXTERNAL_RESOLUTION_FULL_SUMMARY,
  EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID,
  EXTERNAL_RESOLUTION_PARTIAL_SUMMARY,
  EXTERNAL_RESOLUTION_STALE_RECEIPT_ID,
  EXTERNAL_RESOLUTION_UNRELATED_QUESTION,
  EXTERNAL_RESOLUTION_UNVERIFIED_RECEIPT_ID,
  FIXTURE_BASE_NOW_MS,
  HISTORICAL_FAILURE_REASON,
  installWorkspaceTauriFixture,
  PENDING_ACCEPTANCE_SUMMARY,
  VERIFIED_ACCEPTED_SUMMARY,
  WRONG_SCOPE_QUARANTINE_SENTINEL,
} from './fixtures/workspace.ts';
import {
  parseWorkspaceSnapshot,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
} from '../src/workspace-contract.ts';

const ACTIVITY_SPINNER_SELECTOR = '[data-activity-spinner="true"], .tl-wp-spinner, .tl-spin';

function chatSurfaceTab(page: Page, tabName: 'Messages' | 'Jobs' | 'Activity'): Locator {
  const pattern = new RegExp(`^${tabName}\\b`, 'i');
  return page.getByRole('tab', { name: pattern }).first();
}

function visibleJobsPanel(page: Page): Locator {
  return page.locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden]) .tl-wp-jobs-panel').first();
}

function visibleActivityPanel(page: Page): Locator {
  return page
    .locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden]) .tl-wp-activity-panel')
    .first();
}

async function ensureDetailsOpen(details: Locator): Promise<void> {
  await expect(details).toBeVisible();
  const isOpen = await details.evaluate((el) => (el as HTMLDetailsElement).open);
  if (!isOpen) {
    await details.locator('summary').first().click();
  }
  await expect(details).toHaveJSProperty('open', true);
}

async function expectUsableMessagesScroller(messagesPanel: Locator): Promise<Locator> {
  const scroller = messagesPanel.locator('[data-messages-scroller="true"]').first();
  await expect(scroller).toBeVisible();
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
  const [scrollerBox, targetBox] = await Promise.all([
    scroller.boundingBox(),
    target.boundingBox(),
  ]);
  expect(scrollerBox).not.toBeNull();
  expect(targetBox).not.toBeNull();
  expect(scrollerBox!.height).toBeGreaterThanOrEqual(120);
  expect(targetBox!.height).toBeGreaterThan(0);
  expect(targetBox!.y).toBeGreaterThanOrEqual(scrollerBox!.y - 1);
  expect(targetBox!.y + targetBox!.height).toBeLessThanOrEqual(
    scrollerBox!.y + scrollerBox!.height + 1,
  );
}

function lifecycleFilterControl(page: Page, kind: 'active' | 'archived'): Locator {
  const pattern = kind === 'active' ? /^active\b/i : /^archive\b/i;
  return page
    .locator('aside.tl-sidebar')
    .first()
    .getByRole('group', { name: 'Projects views' })
    .getByRole('button', { name: pattern })
    .first();
}

async function expandOtherActivityDetails(page: Page, activityPanel: Locator): Promise<Locator> {
  const details = activityPanel
    .locator('details')
    .filter({ has: page.locator('summary', { hasText: /other activity/i }) })
    .first();
  if ((await details.count()) > 0) {
    const isOpen = await details.evaluate((el) => (el as HTMLDetailsElement).open);
    if (!isOpen) {
      await details.locator('summary').first().click();
    }
    await expect(details).toHaveJSProperty('open', true);
  }
  return details;
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

async function triggerWorkspaceRefresh(page: Page): Promise<void> {
  const refreshBtn = page.getByRole('button', { name: /refresh|reload/i }).first();
  if ((await refreshBtn.count()) > 0 && (await refreshBtn.isVisible())) {
    await refreshBtn.click();
    return;
  }
  await page.evaluate(() => {
    window.dispatchEvent(new Event('focus'));
  });
}

async function findScrollableMessagesViewport(
  page: Page,
  anchorText: string,
): Promise<Locator> {
  const handle = await page.evaluateHandle((needle) => {
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let targetEl: HTMLElement | null = null;
    while (walker.nextNode()) {
      const node = walker.currentNode;
      if (node.nodeValue && node.nodeValue.includes(needle)) {
        targetEl = node.parentElement;
        break;
      }
    }
    let current: HTMLElement | null = targetEl;
    while (current && current !== document.body) {
      const style = window.getComputedStyle(current);
      const overflowY = style.overflowY;
      if (
        (overflowY === 'auto' || overflowY === 'scroll' || overflowY === 'overlay') &&
        current.scrollHeight > current.clientHeight + 40
      ) {
        return current;
      }
      current = current.parentElement;
    }
    return null;
  }, anchorText);

  try {
    const element = handle.asElement();
    expect(
      element,
      `Expected a scrollable Messages viewport ancestor containing "${anchorText}"`,
    ).not.toBeNull();

    await page.evaluate((el) => {
      if (el) {
        el.setAttribute('data-pw-messages-scroll-probe', 'true');
      }
    }, element);
  } finally {
    await handle.dispose();
  }

  return page.locator('[data-pw-messages-scroll-probe="true"]').first();
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
          /not connected in this browser window|observation.*unavailable|runtime.*unavailable|unavailable|disconnected/i,
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

test.describe('Responsive shell geometry: wide, 760px drawer boundary, and 360x500 minimum window', () => {
  test('toggles wide titlebar sidebar rail, switches at exact 760px drawer boundary, traps focus, and restores focus on Escape', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const titlebar = page.locator('header.tl-titlebar, [role="banner"]').first();
    await expect(titlebar).toBeVisible();

    const inlineSidebar = page.locator('aside.tl-sidebar').first();
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
    // Wait for the 140ms CSS width transition on `.tl-sidebar` to settle at <= 72px.
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

    // Focus must move inside the modal navigation drawer on open.
    await expect
      .poll(() =>
        page.evaluate(() => {
          const dialog = document.querySelector('[role="dialog"]');
          return Boolean(dialog && document.activeElement && dialog.contains(document.activeElement));
        }),
      )
      .toBe(true);

    // Escape must dismiss the drawer and restore focus to the titlebar trigger button.
    await page.keyboard.press('Escape');
    await expect(drawerDialog).toHaveCount(0);
    await expect(drawerTrigger).toBeFocused();
  });

  test('renders tabs, drawer navigation, and chat surfaces cleanly inside 360x500 narrow minimum window without horizontal overflow', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 360, height: 500 });
    await page.goto('/');

    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();

    const messagesTab = chatSurfaceTab(page, 'Messages');
    const jobsTab = chatSurfaceTab(page, 'Jobs');
    const activityTab = chatSurfaceTab(page, 'Activity');

    await expect(messagesTab).toBeVisible();
    await expect(messagesTab).toHaveAttribute('aria-selected', 'true');
    await expect(jobsTab).toBeVisible();
    await expect(activityTab).toBeVisible();

    for (const tab of [messagesTab, jobsTab, activityTab]) {
      const rect = await tab.boundingBox();
      expect(rect).not.toBeNull();
      expect(rect!.x).toBeGreaterThanOrEqual(0);
      expect(rect!.x + rect!.width).toBeLessThanOrEqual(360);
    }

    const checkNoHorizontalOverflow = async () => {
      const metrics = await page.evaluate(() => ({
        scrollWidth: document.documentElement.scrollWidth,
        innerWidth: window.innerWidth,
      }));
      expect(metrics.scrollWidth).toBeLessThanOrEqual(metrics.innerWidth);
    };

    await checkNoHorizontalOverflow();

    await jobsTab.click();
    await expect(jobsTab).toHaveAttribute('aria-selected', 'true');
    await expect(
      visibleJobsPanel(page).getByText('Workspace Contract Acceptance Slice').first(),
    ).toBeVisible();
    await checkNoHorizontalOverflow();

    await activityTab.click();
    await expect(activityTab).toHaveAttribute('aria-selected', 'true');
    await expect(
      visibleActivityPanel(page).getByText('Run isolated Worker implementation pass').first(),
    ).toBeVisible();
    await checkNoHorizontalOverflow();

    const drawerTrigger = page
      .getByRole('button', { name: /open navigation drawer|navigation drawer/i })
      .first();
    await drawerTrigger.click();
    const drawerDialog = page.getByRole('dialog', { name: /navigation|projects|workspace/i });
    await expect(drawerDialog).toBeVisible();
    const drawerRect = await drawerDialog.boundingBox();
    expect(drawerRect).not.toBeNull();
    expect(drawerRect!.width).toBeLessThanOrEqual(360);
    await checkNoHorizontalOverflow();

    await page.keyboard.press('Escape');
    await expect(drawerDialog).toHaveCount(0);
  });

  test('narrow activity cards preserve readable titles', async ({ page }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 360, height: 500 });
    await page.goto('/');

    const activityTab = chatSurfaceTab(page, 'Activity');
    await activityTab.click();
    await expect(activityTab).toHaveAttribute('aria-selected', 'true');

    const activityPanel = visibleActivityPanel(page);
    await expect(activityPanel).toBeVisible();

    const scroller = activityPanel.locator('.tl-wp-activity-scroller').first();
    await expect(scroller).toBeVisible();

    const cards = scroller.locator('article.tl-wp-activity-card');
    await expect(cards).toHaveCount(3);

    const expectedTitles = [
      'Run isolated Worker implementation pass',
      'Complete contract schema validation pass',
      'Validate shared contract types',
    ];

    const measureCardTitleGeometry = async (index: number) =>
      cards.nth(index).evaluate((cardEl) => {
        const titleEl = cardEl.querySelector('.tl-wp-activity-row-title');
        const buttonEl = cardEl.querySelector('button.tl-wp-activity-row-btn');
        const scrollerEl = cardEl.closest('.tl-wp-activity-scroller');
        const panelEl = cardEl.closest('.tl-wp-activity-panel');
        if (!titleEl || !buttonEl || !scrollerEl || !panelEl) {
          return null;
        }

        const cardRect = cardEl.getBoundingClientRect();
        const buttonRect = buttonEl.getBoundingClientRect();
        const titleRect = titleEl.getBoundingClientRect();
        const scrollerRect = scrollerEl.getBoundingClientRect();
        const panelRect = panelEl.getBoundingClientRect();

        const cardClipLeft = cardRect.left + cardEl.clientLeft;
        const cardClipTop = cardRect.top + cardEl.clientTop;
        const cardClipRight = cardClipLeft + cardEl.clientWidth;
        const cardClipBottom = cardClipTop + cardEl.clientHeight;

        const scrollerClipLeft = scrollerRect.left + scrollerEl.clientLeft;
        const scrollerClipTop = scrollerRect.top + scrollerEl.clientTop;
        const scrollerClipRight = scrollerClipLeft + scrollerEl.clientWidth;
        const scrollerClipBottom = scrollerClipTop + scrollerEl.clientHeight;

        const visibleLeft = Math.max(
          0,
          panelRect.left,
          scrollerClipLeft,
          cardClipLeft,
          titleRect.left,
        );
        const visibleTop = Math.max(
          0,
          panelRect.top,
          scrollerClipTop,
          cardClipTop,
          titleRect.top,
        );
        const visibleRight = Math.min(
          window.innerWidth,
          panelRect.right,
          scrollerClipRight,
          cardClipRight,
          titleRect.right,
        );
        const visibleBottom = Math.min(
          window.innerHeight,
          panelRect.bottom,
          scrollerClipBottom,
          cardClipBottom,
          titleRect.bottom,
        );
        const visibleWidth = Math.max(0, visibleRight - visibleLeft);
        const visibleHeight = Math.max(0, visibleBottom - visibleTop);

        const centerX = visibleLeft + visibleWidth / 2;
        const centerY = visibleTop + visibleHeight / 2;
        const hitEl =
          visibleWidth > 0 && visibleHeight > 0
            ? document.elementFromPoint(centerX, centerY)
            : null;

        return {
          titleText: (titleEl.textContent ?? '').trim(),
          titleWidth: titleRect.width,
          titleHeight: titleRect.height,
          titleWithinCard:
            titleRect.left >= cardClipLeft - 1.5 &&
            titleRect.right <= cardClipRight + 1.5 &&
            titleRect.top >= cardClipTop - 1.5 &&
            titleRect.bottom <= cardClipBottom + 1.5,
          buttonWithinCard:
            buttonRect.top >= cardClipTop - 1.5 && buttonRect.bottom <= cardClipBottom + 1.5,
          visibleWidth,
          visibleHeight,
          hitWithinCard: Boolean(hitEl && cardEl.contains(hitEl)),
        };
      });

    for (let idx = 0; idx < expectedTitles.length; idx += 1) {
      const geometry = await measureCardTitleGeometry(idx);
      expect(geometry).not.toBeNull();
      expect(geometry!.titleText).toBe(expectedTitles[idx]);
      expect(geometry!.titleWidth).toBeGreaterThan(80);
      expect(geometry!.titleHeight).toBeGreaterThanOrEqual(14);
      expect(geometry!.titleWithinCard).toBe(true);
      expect(geometry!.buttonWithinCard).toBe(true);
    }

    const firstTitle = cards.nth(0).locator('.tl-wp-activity-row-title').first();
    await expect(firstTitle).toHaveText(expectedTitles[0]!);
    await expect(firstTitle).toBeInViewport({ ratio: 0.95 });

    const firstGeometry = await measureCardTitleGeometry(0);
    expect(firstGeometry).not.toBeNull();
    expect(firstGeometry!.visibleWidth).toBeGreaterThanOrEqual(firstGeometry!.titleWidth - 1.5);
    expect(firstGeometry!.visibleHeight).toBeGreaterThanOrEqual(firstGeometry!.titleHeight - 1.5);
    expect(firstGeometry!.hitWithinCard).toBe(true);

    const initialScroll = await scroller.evaluate((pane) => {
      const paneRect = pane.getBoundingClientRect();
      const cardNodes = pane.querySelectorAll('article.tl-wp-activity-card');
      const lastCard = cardNodes[cardNodes.length - 1];
      const lastRect = lastCard ? lastCard.getBoundingClientRect() : null;
      return {
        scrollTop: pane.scrollTop,
        clientHeight: pane.clientHeight,
        scrollHeight: pane.scrollHeight,
        lastCardExtendsBelowPane: lastRect ? lastRect.bottom > paneRect.bottom + 4 : false,
        docScrollWidth: document.documentElement.scrollWidth,
        innerWidth: window.innerWidth,
        windowScrollY: window.scrollY,
      };
    });
    expect(initialScroll.scrollTop).toBe(0);
    expect(initialScroll.scrollHeight).toBeGreaterThan(initialScroll.clientHeight + 20);
    expect(initialScroll.lastCardExtendsBelowPane).toBe(true);
    expect(initialScroll.docScrollWidth).toBeLessThanOrEqual(initialScroll.innerWidth);
    expect(initialScroll.windowScrollY).toBe(0);

    let previousScrollTop = initialScroll.scrollTop;
    for (let idx = 1; idx < expectedTitles.length; idx += 1) {
      const nextScrollTop = await scroller.evaluate((pane, cardIndex) => {
        const cardEl = pane.querySelectorAll('article.tl-wp-activity-card')[cardIndex];
        if (!cardEl) {
          throw new Error(`Missing activity card at index ${cardIndex}`);
        }
        const paneRect = pane.getBoundingClientRect();
        const cardRect = cardEl.getBoundingClientRect();
        const desiredTop = pane.scrollTop + (cardRect.top - paneRect.top) - 8;
        const maxScrollTop = Math.max(0, pane.scrollHeight - pane.clientHeight);
        pane.scrollTop = Math.min(maxScrollTop, Math.max(0, desiredTop));
        pane.dispatchEvent(new Event('scroll', { bubbles: true }));
        return pane.scrollTop;
      }, idx);

      expect(nextScrollTop).toBeGreaterThan(0);
      expect(nextScrollTop).toBeGreaterThanOrEqual(previousScrollTop);
      previousScrollTop = nextScrollTop;
      expect(await page.evaluate(() => window.scrollY)).toBe(0);

      const cardTitle = cards.nth(idx).locator('.tl-wp-activity-row-title').first();
      await expect(cardTitle).toHaveText(expectedTitles[idx]!);
      await expect(cardTitle).toBeInViewport({ ratio: 0.95 });

      const scrolledGeometry = await measureCardTitleGeometry(idx);
      expect(scrolledGeometry).not.toBeNull();
      expect(scrolledGeometry!.titleWithinCard).toBe(true);
      expect(scrolledGeometry!.buttonWithinCard).toBe(true);
      expect(scrolledGeometry!.visibleWidth).toBeGreaterThanOrEqual(
        scrolledGeometry!.titleWidth - 1.5,
      );
      expect(scrolledGeometry!.visibleHeight).toBeGreaterThanOrEqual(
        scrolledGeometry!.titleHeight - 1.5,
      );
      expect(scrolledGeometry!.hitWithinCard).toBe(true);
    }
  });
});

test.describe('Workspace chat tabs, Active/Archive lifecycle, scroll preservation, and Activity spinner geometry', () => {
  test('separates Messages, Jobs, and Activity tabs with Messages selected by default', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const messagesTab = chatSurfaceTab(page, 'Messages');
    const jobsTab = chatSurfaceTab(page, 'Jobs');
    const activityTab = chatSurfaceTab(page, 'Activity');
    const messagesPanel = page.locator('.tl-wp-messages-panel').first();

    await expect(messagesTab).toBeVisible();
    await expect(messagesTab).toHaveAttribute('aria-selected', 'true');
    await expect(jobsTab).toHaveAttribute('aria-selected', 'false');
    await expect(activityTab).toHaveAttribute('aria-selected', 'false');

    // All three chat tabpanels stay mounted to preserve scroll, with exactly one visible tabpanel.
    await expect(page.locator('.tl-wp-tabpanel[role="tabpanel"]')).toHaveCount(3);
    await expect(page.locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden])')).toHaveCount(1);
    await expect(page.locator('[role="tabpanel"]:visible')).toHaveCount(1);
    await expect(messagesPanel).toBeVisible();
    await expect(page.getByText(/Alpha message #36/)).toBeVisible();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await jobsTab.click();
    await expect(jobsTab).toHaveAttribute('aria-selected', 'true');
    await expect(messagesTab).toHaveAttribute('aria-selected', 'false');
    await expect(page.locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden])')).toHaveCount(1);
    await expect(page.locator('[role="tabpanel"]:visible')).toHaveCount(1);
    await expect(messagesPanel).toHaveCount(1);
    await expect(messagesPanel).toBeHidden();
    await expect(page.getByText(/Alpha message #36/)).toBeHidden();
    await expect(
      visibleJobsPanel(page).getByText('Workspace Contract Acceptance Slice').first(),
    ).toBeVisible();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await activityTab.click();
    await expect(activityTab).toHaveAttribute('aria-selected', 'true');
    await expect(jobsTab).toHaveAttribute('aria-selected', 'false');
    await expect(messagesTab).toHaveAttribute('aria-selected', 'false');
    await expect(page.locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden])')).toHaveCount(1);
    await expect(page.locator('[role="tabpanel"]:visible')).toHaveCount(1);
    await expect(messagesPanel).toBeHidden();
    await expect(page.getByText(/Alpha message #36/)).toBeHidden();

    const activityPanel = visibleActivityPanel(page);
    await expect(
      activityPanel.getByText('Run isolated Worker implementation pass').first(),
    ).toBeVisible();
    await expandOtherActivityDetails(page, activityPanel);
    await expect(activityPanel.getByText('Validate shared contract types').first()).toBeVisible();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);
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
      initialLocalStorage: {
        [WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY]: forgedStoragePayload,
      },
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const sidebar = page.locator('aside.tl-sidebar').first();
    await expect(sidebar).toBeVisible();

    // Forged keys keyed by job_id or chat title must not archive the chat.
    await expect(sidebar.getByText('Archived Migration Audit').first()).toBeVisible();
    await expect(sidebar.getByText('Workspace UI Contract').first()).toBeVisible();
    await expect(sidebar.getByText('Release Verification Gate').first()).toBeVisible();

    await sidebar.getByText('Archived Migration Audit').first().click();

    const archiveActionBtn = sidebar.getByRole('button', {
      name: 'Archive Archived Migration Audit',
      exact: true,
    });
    await expect(archiveActionBtn).toBeVisible();
    await archiveActionBtn.click();

    // Gamma leaves Active while Alpha and Beta remain in Active.
    await expect(sidebar.getByText('Archived Migration Audit')).toHaveCount(0);
    await expect(sidebar.getByText('Workspace UI Contract').first()).toBeVisible();
    await expect(sidebar.getByText('Release Verification Gate').first()).toBeVisible();

    const storedRaw = await controller.readArchivePreferencesRaw();
    expect(storedRaw).not.toBeNull();
    const storedMap = JSON.parse(storedRaw!) as Record<string, { lifecycle?: string }>;
    expect(storedMap[CHAT_GAMMA_KEY]?.lifecycle).toBe('archived');

    // Switch Projects/Archive subnav separately and verify Gamma appears there with Restore control.
    const archiveFilter = lifecycleFilterControl(page, 'archived');
    await expect(archiveFilter).toBeVisible();
    await archiveFilter.click();

    await expect(sidebar.getByText('Archived Migration Audit').first()).toBeVisible();
    await expect(sidebar.getByText('Workspace UI Contract')).toHaveCount(0);

    await sidebar.getByText('Archived Migration Audit').first().click();
    const restoreBtn = sidebar.getByRole('button', {
      name: 'Restore Archived Migration Audit',
      exact: true,
    });
    await expect(restoreBtn).toBeVisible();
    await restoreBtn.click();

    await expect(sidebar.getByText('Archived Migration Audit')).toHaveCount(0);

    const activeFilter = lifecycleFilterControl(page, 'active');
    await activeFilter.click();
    await expect(sidebar.getByText('Archived Migration Audit').first()).toBeVisible();

    const restoredRaw = await controller.readArchivePreferencesRaw();
    const restoredMap = restoredRaw ? (JSON.parse(restoredRaw) as Record<string, unknown>) : {};
    expect(restoredMap[CHAT_GAMMA_KEY]).toBeUndefined();
  });

  test('appends messages at bottom when anchored, preserves viewport when reading older history, and jumps via new-events control', async ({
    page,
  }) => {
    const controller = await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    await expect(chatSurfaceTab(page, 'Messages')).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByText(/Alpha message #36/)).toBeVisible();

    const viewport = await findScrollableMessagesViewport(page, 'Alpha message #36');

    // 1. Anchored at bottom -> appending message #37 keeps scroll anchored at bottom.
    await viewport.evaluate((el) => {
      el.scrollTop = el.scrollHeight;
    });

    await controller.appendMessageItem(
      createChatMessageItem({
        identity: CHAT_ALPHA_IDENTITY,
        seq: 37,
        recordId: 'rec_alpha_msg_37',
        summary: 'Alpha message #37: Appended while anchored at bottom of viewport.',
        recordedAtMs: FIXTURE_BASE_NOW_MS + 5_000,
      }),
    );
    await triggerWorkspaceRefresh(page);

    await expect(page.getByText(/Alpha message #37/)).toBeVisible();
    const bottomDistanceAfterAnchoredAppend = await viewport.evaluate(
      (el) => el.scrollHeight - el.scrollTop - el.clientHeight,
    );
    expect(bottomDistanceAfterAnchoredAppend).toBeLessThanOrEqual(36);

    // 2. Scroll up to read older messages -> appending message #38 preserves viewport and shows new-events button.
    await viewport.evaluate((el) => {
      el.scrollTop = 96;
      el.dispatchEvent(new Event('scroll', { bubbles: true }));
    });

    await expect(page.getByText(/Alpha message #05/)).toBeVisible();
    const anchorMessage = page.getByText(/Alpha message #05/).first();

    const beforeMetrics = await viewport.evaluate((el) => ({
      scrollTop: el.scrollTop,
      distanceFromBottom: el.scrollHeight - el.scrollTop - el.clientHeight,
    }));
    expect(beforeMetrics.distanceFromBottom).toBeGreaterThan(120);
    const beforeAnchorBox = await anchorMessage.boundingBox();
    expect(beforeAnchorBox).not.toBeNull();

    await controller.appendMessageItem(
      createChatMessageItem({
        identity: CHAT_ALPHA_IDENTITY,
        seq: 38,
        recordId: 'rec_alpha_msg_38',
        summary: 'Alpha message #38: Appended while operator reads older message history.',
        recordedAtMs: FIXTURE_BASE_NOW_MS + 10_000,
      }),
    );
    await triggerWorkspaceRefresh(page);

    const newEventsButton = page
      .locator('button, [role="button"]')
      .filter({ hasText: /new (event|message|item|update)|jump to latest|latest/i })
      .first();
    await expect(newEventsButton).toBeVisible();

    const afterScrollTop = await viewport.evaluate((el) => el.scrollTop);
    const afterAnchorBox = await anchorMessage.boundingBox();
    expect(afterAnchorBox).not.toBeNull();
    expect(Math.abs(afterScrollTop - beforeMetrics.scrollTop)).toBeLessThanOrEqual(4);
    expect(Math.abs(afterAnchorBox!.y - beforeAnchorBox!.y)).toBeLessThanOrEqual(4);

    // 3. Clicking the new-events control jumps to bottom and clears the indicator.
    await newEventsButton.click();
    await expect(page.getByText(/Alpha message #38/)).toBeVisible();
    await expect
      .poll(() =>
        viewport.evaluate((el) => el.scrollHeight - el.scrollTop - el.clientHeight <= 36),
      )
      .toBe(true);
    await expect(newEventsButton).toHaveCount(0);
  });

  test('aligns Activity running spinner cleanly with status badge and omits spinners on completed or local MCP rows', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    await chatSurfaceTab(page, 'Activity').click();
    const activityPanel = visibleActivityPanel(page);

    const runningWorkerCard = activityPanel
      .locator('article.tl-wp-activity-card')
      .filter({ hasText: 'Run isolated Worker implementation pass' })
      .first();
    await expect(runningWorkerCard).toBeVisible();

    // Running Worker card must carry a single centered spinner aligned with its status badge.
    const runningWorkerSpinners = runningWorkerCard.locator(ACTIVITY_SPINNER_SELECTOR);
    await expect(runningWorkerSpinners).toHaveCount(1);
    const runningSpinner = runningWorkerSpinners.first();
    await expect(runningSpinner).toBeVisible();

    const alignment = await runningSpinner.evaluate((spinEl) => {
      const spinRect = spinEl.getBoundingClientRect();
      const container = spinEl.parentElement ?? spinEl;
      const containerRect = container.getBoundingClientRect();
      const spinCenterX = spinRect.left + spinRect.width / 2;
      const spinCenterY = spinRect.top + spinRect.height / 2;
      const containerCenterY = containerRect.top + containerRect.height / 2;
      const svg = spinEl.querySelector('svg');
      const svgRect = svg ? svg.getBoundingClientRect() : spinRect;
      const svgCenterX = svgRect.left + svgRect.width / 2;
      const svgCenterY = svgRect.top + svgRect.height / 2;
      return {
        width: spinRect.width,
        height: spinRect.height,
        centerDeltaY: Math.abs(spinCenterY - containerCenterY),
        svgCenterDeltaX: Math.abs(svgCenterX - spinCenterX),
        svgCenterDeltaY: Math.abs(svgCenterY - spinCenterY),
        withinHorizontalBounds:
          spinRect.left >= containerRect.left - 1 && spinRect.right <= containerRect.right + 1,
      };
    });

    expect(alignment.width).toBeGreaterThanOrEqual(8);
    expect(alignment.height).toBeGreaterThanOrEqual(8);
    expect(Math.abs(alignment.width - alignment.height)).toBeLessThanOrEqual(2);
    expect(alignment.centerDeltaY).toBeLessThanOrEqual(3.5);
    expect(alignment.svgCenterDeltaX).toBeLessThanOrEqual(2);
    expect(alignment.svgCenterDeltaY).toBeLessThanOrEqual(2);
    expect(alignment.withinHorizontalBounds).toBe(true);

    // Completed Worker row must not render a running spinner.
    const completedWorkerCard = activityPanel
      .locator('article.tl-wp-activity-card')
      .filter({ hasText: 'Complete contract schema validation pass' })
      .first();
    await expect(completedWorkerCard).toBeVisible();
    await expect(completedWorkerCard.locator(ACTIVITY_SPINNER_SELECTOR)).toHaveCount(0);

    // Local MCP content is grouped under collapsed "Other activity" details; expand first and verify zero spinners.
    const otherActivityDetails = await expandOtherActivityDetails(page, activityPanel);
    if ((await otherActivityDetails.count()) > 0) {
      await expect(otherActivityDetails.locator(ACTIVITY_SPINNER_SELECTOR)).toHaveCount(0);
    }

    const localMcpRow = activityPanel
      .locator('article, [role="listitem"], li, .tl-req-row')
      .filter({ hasText: 'Validate shared contract types' })
      .first();
    await expect(localMcpRow).toBeVisible();
    await expect(localMcpRow.locator(ACTIVITY_SPINNER_SELECTOR)).toHaveCount(0);
  });
});

test.describe('Workspace semantic contract fixtures', () => {
  test('keeps live Coworker chat availability/selection separate from background Worker execution telemetry', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const chatHeader = page.locator('header.tl-wp-chat-header').first();
    await expect(chatHeader).toBeVisible();

    // Native Coworker header state is waiting for user input with configured GPT-5.4 (never Worker telemetry).
    await expect(
      chatHeader.getByText(/waiting for (?:user )?input|awaiting (?:user )?input/i).first(),
    ).toBeVisible();
    await expect(chatHeader.getByText(/gpt-5\.4/i).first()).toBeVisible();
    await expect(chatHeader.getByText(/argon|antigravity/i)).toHaveCount(0);

    // Meanwhile Worker execution on the same chat is Running with gemini-4-argon-high.
    await chatSurfaceTab(page, 'Activity').click();
    const activityPanel = visibleActivityPanel(page);
    const runningWorkerCard = activityPanel
      .locator('article.tl-wp-activity-card')
      .filter({ hasText: 'Run isolated Worker implementation pass' })
      .first();
    await expect(runningWorkerCard).toBeVisible();
    await expect(runningWorkerCard.getByText(/^running$/i).first()).toBeVisible();
    await expect(runningWorkerCard.getByText(/argon/i).first()).toBeVisible();

    await runningWorkerCard
      .locator('button.tl-wp-activity-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(runningWorkerCard.getByText(/gemini-4-argon-high/i).first()).toBeVisible();

    // Native header continues reporting Coworker state and configured selection rather than Worker model.
    await expect(
      chatHeader.getByText(/waiting for (?:user )?input|awaiting (?:user )?input/i).first(),
    ).toBeVisible();
    await expect(chatHeader.getByText(/gpt-5\.4/i).first()).toBeVisible();
    await expect(chatHeader.getByText(/argon|antigravity/i)).toHaveCount(0);
  });

  test('surfaces Stale and Disconnected native chat states without resetting sample age or hiding durable history', async ({
    page,
  }) => {
    const staleSnapshot = buildStaleWorkspaceSnapshot();
    const disconnectedSnapshot = buildDisconnectedWorkspaceSnapshot();

    // Verify fixture envelope and monotonic sequence / preserved last_success_at_ms preconditions.
    expect(staleSnapshot.generated_at_ms).toBe(FIXTURE_BASE_NOW_MS + 25_000);
    expect(disconnectedSnapshot.generated_at_ms).toBeGreaterThan(staleSnapshot.generated_at_ms);

    const staleAlphaSample = staleSnapshot.native_samples.find(
      (sample) => sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
    );
    const disconnectedAlphaSample = disconnectedSnapshot.native_samples.find(
      (sample) => sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
    );
    expect(staleAlphaSample).toBeDefined();
    expect(disconnectedAlphaSample).toBeDefined();
    expect(disconnectedAlphaSample!.sequence).toBeGreaterThan(staleAlphaSample!.sequence);
    expect(disconnectedAlphaSample!.last_success_at_ms).toBe(staleAlphaSample!.last_success_at_ms);

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: staleSnapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    expect(await page.evaluate(() => Date.now())).toBe(staleSnapshot.generated_at_ms);

    const chatHeader = page.locator('header.tl-wp-chat-header').first();
    // Stale native sample must surface stale status and withhold configured model while durable messages remain readable.
    await expect(chatHeader.getByText(/^stale$/i).first()).toBeVisible();
    await expect(chatHeader.getByText(/Configured:\s*Unavailable/i)).toBeVisible();
    await expect(page.getByText(/Alpha message #36/)).toBeVisible();

    // Transition monotonically to Disconnected native bridge state without faking last_success_at_ms.
    await controller.setWorkspaceSnapshot(disconnectedSnapshot);
    await triggerWorkspaceRefresh(page);

    await expect(chatHeader.getByText(/^disconnected$/i).first()).toBeVisible();
    await expect(page.getByText(/Alpha message #36/)).toBeVisible();

    const storedAfterDisconnect = await controller.getWorkspaceSnapshot();
    const storedAlphaSample = storedAfterDisconnect?.native_samples.find(
      (sample) => sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
    );
    expect(storedAlphaSample?.last_success_at_ms).toBe(staleAlphaSample!.last_success_at_ms);

    await chatSurfaceTab(page, 'Jobs').click();
    await expect(
      visibleJobsPanel(page).getByText('Workspace Contract Acceptance Slice').first(),
    ).toBeVisible();
  });

  test('enforces four-part WorkspaceChatIdentity scope isolation against same-project sibling chats and unaligned records', async ({
    page,
  }) => {
    const controller = await installWorkspaceTauriFixture(page);
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    // Alpha is selected: neither Beta's sentinel nor the unaligned wrong-scope sentinel may exist anywhere in DOM.
    await expect(page.getByText(/Alpha message #36/)).toBeVisible();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await chatSurfaceTab(page, 'Jobs').click();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await chatSurfaceTab(page, 'Activity').click();
    await expandOtherActivityDetails(page, visibleActivityPanel(page));
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    // Open in Codex must pass exact { request: { source_id, thread_uuid, job_id? } } for Alpha with no URL/path/executable fields.
    const chatHeader = page.locator('header.tl-wp-chat-header').first();
    const openCodexBtn = chatHeader.getByRole('button', { name: /^open in codex$/i });
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

    // Selecting same-project sibling Beta scopes DOM and Open in Codex strictly to Beta's thread_uuid while still quarantining unaligned records.
    const sidebar = page.locator('aside.tl-sidebar').first();
    await sidebar.getByText('Release Verification Gate').first().click();
    await chatSurfaceTab(page, 'Messages').click();
    await expect(page.getByText(BETA_CHAT_ONLY_SENTINEL).first()).toBeVisible();
    await expect(page.getByText(/Alpha message #36/)).toHaveCount(0);
    await expect(page.getByText(WRONG_SCOPE_QUARANTINE_SENTINEL)).toHaveCount(0);

    await controller.clearInvocations();
    await openCodexBtn.click();
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

  test('distinguishes superseded historical attempt failures from active human approval attention', async ({
    page,
  }) => {
    const baseSnapshot = buildHistoricalFailureWorkspaceSnapshot();
    const fixtureSourceId = baseSnapshot.job_histories[0]?.job.source_id ?? 'default';
    const retiredPlanningHistory = {
      job: {
        source_id: fixtureSourceId,
        job_id: 'job_retired_plan_draft_01',
        binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
        chat_identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
        title: 'Retired Initial Plan Draft',
        job_revision: 2,
        state: 'unknown' as const,
        phase: 'held',
        approval_status: 'superseded',
        execution_status: 'not_started',
        review_state: null,
        created_at_ms: FIXTURE_BASE_NOW_MS - 90_000,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 45_000,
        summary: 'Initial unstarted planning brief superseded before approval.',
      },
      tasks: [],
      runs: [],
      outbox: [],
      combined_candidates: [],
      result: null,
      links: {
        source_plan_id: 'plan_attn_01',
        source_plan_revision: 1,
        source_approval_id: null,
        source_work_brief_id: 'brf_attn_01',
        planning_grant_id: null,
        planning_reservation_id: null,
        supersedes_job_id: null,
        superseded_by_job_id: 'job_active_attention_02',
        parent_job_id: null,
        follow_up_job_ids: ['job_active_attention_02'],
        architect_advice_ids: [],
        source_request_ids: [],
      },
      historical_failure: null,
      current_attention: null,
      acceptance_proof: {
        status: 'unknown' as const,
        accepted_candidate_id: null,
        combined_candidate_id: null,
        review_verdict_id: null,
        review_receipt_id: null,
        integration_receipt_id: null,
        closure_reservation_id: null,
        closure_verdict_id: null,
        verified_at_ms: null,
      },
      truncation: {
        tasks_omitted: 0,
        attempts_omitted: 0,
        candidates_omitted: 0,
        runs_omitted: 0,
        outbox_omitted: 0,
        combined_candidates_omitted: 0,
      },
    };
    const genuineFailedHistory = {
      ...retiredPlanningHistory,
      job: {
        ...retiredPlanningHistory.job,
        job_id: 'job_cancelled_legacy_03',
        title: 'Cancelled Legacy Experiment',
        state: 'failed' as const,
        phase: 'cancelled',
        approval_status: 'superseded',
        execution_status: 'stopped',
        updated_at_ms: FIXTURE_BASE_NOW_MS - 50_000,
        summary: 'Explicitly cancelled job remains truthfully Failed.',
      },
    };
    const augmentedSnapshot = parseWorkspaceSnapshot({
      ...baseSnapshot,
      job_histories: [
        ...baseSnapshot.job_histories,
        retiredPlanningHistory,
        genuineFailedHistory,
      ],
    });

    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: augmentedSnapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const sidebar = page.locator('aside.tl-sidebar').first();
    await expect(sidebar).toBeVisible();

    // Historical fixture prioritizes Beta initially due to active attention; explicitly select Alpha first.
    await sidebar.getByText('Workspace UI Contract').first().click();
    await chatSurfaceTab(page, 'Jobs').click();

    const jobsPanel = visibleJobsPanel(page);
    const recoveredJobCard = jobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Recovered Retry Pipeline' })
      .first();
    await expect(recoveredJobCard).toBeVisible();
    await expect(
      recoveredJobCard.getByText(/awaiting.*approval|needs approval/i),
    ).toHaveCount(0);

    // Expand job detail to inspect superseded historical failure context without surfacing active approval blockers.
    await recoveredJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(
      recoveredJobCard.getByText(new RegExp(HISTORICAL_FAILURE_REASON, 'i')).first(),
    ).toBeVisible();
    await expect(recoveredJobCard.getByText(/superseded/i).first()).toBeVisible();
    await expect(
      recoveredJobCard.getByText(/awaiting.*approval|needs approval/i),
    ).toHaveCount(0);

    // Switch to sibling chat Beta where active approval attention is genuinely pending alongside a retired planning draft.
    await sidebar.getByText('Release Verification Gate').first().click();
    await chatSurfaceTab(page, 'Jobs').click();

    const betaJobsPanel = visibleJobsPanel(page);
    const retiredPlanCard = betaJobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Retired Initial Plan Draft' })
      .first();
    await expect(retiredPlanCard).toBeVisible();
    await expect(
      retiredPlanCard.locator('.tl-wp-pill[data-tone="neutral"] .tl-wp-pill-label', {
        hasText: /^Superseded$/,
      }),
    ).toHaveCount(1);
    await expect(
      retiredPlanCard.locator('.tl-wp-pill .tl-wp-pill-label', {
        hasText: /^(Failed|Unknown|Needs user|Needs approval|On hold)$/i,
      }),
    ).toHaveCount(0);

    const genuineFailedCard = betaJobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Cancelled Legacy Experiment' })
      .first();
    await expect(genuineFailedCard).toBeVisible();
    await expect(
      genuineFailedCard.locator('.tl-wp-pill[data-tone="danger"] .tl-wp-pill-label', {
        hasText: /^Failed$/,
      }),
    ).toHaveCount(1);

    const approvalJobCard = betaJobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Plan Approval Gate' })
      .first();
    await expect(approvalJobCard).toBeVisible();
    await expect(
      approvalJobCard.getByText(/awaiting.*approval|needs approval/i).first(),
    ).toBeVisible();
  });

  test('does not report completed Worker candidates as Accepted/Done while Closure acceptance is pending', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildPendingAcceptanceWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    await chatSurfaceTab(page, 'Jobs').click();
    await expect(page.locator('.tl-wp-messages-panel').first()).toBeHidden();
    await expect(page.locator('[role="tabpanel"]:visible')).toHaveCount(1);

    const jobsPanel = visibleJobsPanel(page);
    await expect(jobsPanel).toBeVisible();

    const pendingJobCard = jobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Worker Finished Pending Acceptance Slice' })
      .first();
    await expect(pendingJobCard).toBeVisible();
    await expect(pendingJobCard.getByText(PENDING_ACCEPTANCE_SUMMARY).first()).toBeVisible();
    await expect(
      pendingJobCard
        .getByText(
          /ready for review|pending review|awaiting review|pending acceptance|requires review/i,
        )
        .first(),
    ).toBeVisible();
    await expect(pendingJobCard.getByText(/^verified accepted$|^accepted$|^done$/i)).toHaveCount(
      0,
    );
    await expect(pendingJobCard.getByText(VERIFIED_ACCEPTED_SUMMARY)).toHaveCount(0);

    // Expand pending job card and verify finished Worker candidate still never claims Accepted/Done.
    await pendingJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(pendingJobCard.getByText('cand_pending_01').first()).toBeVisible();
    await expect(pendingJobCard.getByText(PENDING_ACCEPTANCE_SUMMARY).first()).toBeVisible();
    await expect(pendingJobCard.getByText(/^verified accepted$|^accepted$|^done$/i)).toHaveCount(
      0,
    );

    const acceptedJobCard = jobsPanel
      .locator('article.tl-wp-job-card')
      .filter({ hasText: 'Closure Verified Package Slice' })
      .first();
    await expect(acceptedJobCard).toBeVisible();
    await expect(acceptedJobCard.getByText(/^verified accepted$/i).first()).toBeVisible();
    await expect(acceptedJobCard.getByText(VERIFIED_ACCEPTED_SUMMARY).first()).toBeVisible();
    await expect(acceptedJobCard.getByText(PENDING_ACCEPTANCE_SUMMARY)).toHaveCount(0);
  });

  test('transitions fresh native working chat to Stale via owned 1s UI age clock when one snapshot poll stalls without manufacturing reads or timestamps', async ({
    page,
  }) => {
    const base = buildInteractiveWorkspaceSnapshot();
    const baseAlphaChat = base.chats.find(
      (chat) => chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
    )!;
    const baseAlphaSample = base.native_samples.find(
      (sample) => sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
    )!;

    const workingSnapshot = parseWorkspaceSnapshot({
      ...base,
      observation: {
        ...base.observation,
        sources: base.observation.sources.map((src) => ({
          ...src,
          availability: 'available',
          message: null,
        })),
        jobs: [],
        requests: [],
        next_cursor: null,
        has_more: false,
        jobs_has_more: false,
        warnings: [],
        warning_contexts: [],
      },
      chats: [
        {
          ...baseAlphaChat,
          availability: 'available',
          activity: 'working',
          active_job_count: 0,
          attention_count: 0,
        },
      ],
      native_samples: [
        {
          ...baseAlphaSample,
          availability: 'available',
          activity: 'working',
          connection: {
            ...baseAlphaSample.connection,
            state: 'connected',
            freshness: 'fresh',
          },
        },
      ],
      job_histories: [],
      messages: {
        ...base.messages,
        availability: 'available',
        items: [],
        omitted_count: 0,
        has_more: false,
        truncated: false,
      },
    });

    // Producer timestamps must remain untouched from the base interactive snapshot.
    expect(workingSnapshot.generated_at_ms).toBe(base.generated_at_ms);
    expect(workingSnapshot.native_samples[0]!.observed_at_ms).toBe(baseAlphaSample.observed_at_ms);
    expect(workingSnapshot.native_samples[0]!.last_success_at_ms).toBe(
      baseAlphaSample.last_success_at_ms,
    );

    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: workingSnapshot,
    });

    try {
      await page.setViewportSize({ width: 1040, height: 760 });
      await page.goto('/');

      const sidebar = page.locator('aside.tl-sidebar').first();
      const chatHeader = page.locator('header.tl-wp-chat-header').first();
      await expect(sidebar).toBeVisible();
      await expect(chatHeader).toBeVisible();

      // Initially fresh native working Alpha renders two sidebar working badges and header working pill.
      await expect(sidebar.locator('.tl-mini-pill[data-tone="working"]')).toHaveCount(2);
      await expect(chatHeader.locator('.tl-wp-pill[data-tone="working"]')).toHaveCount(1);
      await expect(
        chatHeader.locator('.tl-wp-pill[data-tone="working"]').getByText(/\bworking\b/i),
      ).toBeVisible();
      await expect(chatHeader.getByText(/Configured:\s*GPT-5\.4/i)).toBeVisible();
      await expect(sidebar.getByText(/GPT-5\.4/i).first()).toBeVisible();

      const initialSnapshotReads = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      expect(initialSnapshotReads).toBe(1);

      // Arm a single stalled desktop_workspace_snapshot read (pending without error or reply).
      await controller.stallNextWorkspaceSnapshotRead();
      await expect
        .poll(() => controller.getStalledWorkspaceSnapshotCount(), { timeout: 5_000 })
        .toBe(1);

      const snapshotReadsAtStall = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      expect(snapshotReadsAtStall).toBe(2);
      await expect(page.locator('[role="alert"]')).toHaveCount(0);

      // Advance controlled Date by 11,001ms while real browser interval timers continue running.
      await page.clock.setFixedTime(workingSnapshot.generated_at_ms + 11_001);

      // Owned 1s UI age clock must age out the native working sample to Stale with zero working badges.
      await expect(chatHeader.getByText(/^stale$/i).first()).toBeVisible({ timeout: 4_000 });
      await expect(chatHeader.getByText(/Configured:\s*Unavailable/i)).toBeVisible();
      await expect(chatHeader.getByText(/GPT-5\.4/i)).toHaveCount(0);
      await expect(chatHeader.locator('.tl-wp-pill[data-tone="working"]')).toHaveCount(0);
      await expect(chatHeader.locator(ACTIVITY_SPINNER_SELECTOR)).toHaveCount(0);

      await expect(sidebar.locator('.tl-mini-pill[data-tone="working"]')).toHaveCount(0);
      await expect(
        sidebar.getByText(/native state stale|coworker state stale|\bstale\b/i).first(),
      ).toBeVisible();
      await expect(sidebar.getByText(/GPT-5\.4/i)).toHaveCount(0);
      // Hidden section slots (such as static Profiles override pills under hidden `.tl-panel-slot`)
      // reuse `data-tone="working"` for accent styling rather than live status; assert zero visible working claims.
      await expect(page.locator('[data-tone="working"]:visible')).toHaveCount(0);
      expect(
        await page
          .locator('[data-tone="working"]')
          .evaluateAll((nodes) => nodes.filter((node) => node.checkVisibility()).length),
      ).toBe(0);
      await expect(page.locator('[role="alert"]')).toHaveCount(0);

      // Still stalled on that one pending read: zero additional backend reads or fresh producer timestamps manufactured.
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
      expect(storedAfterAgeOut?.native_samples[0]?.observed_at_ms).toBe(
        baseAlphaSample.observed_at_ms,
      );
      expect(storedAfterAgeOut?.native_samples[0]?.last_success_at_ms).toBe(
        baseAlphaSample.last_success_at_ms,
      );
    } finally {
      await controller.releaseStalledWorkspaceSnapshotReads().catch(() => {});
    }
  });
});

test.describe('Workspace external resolution receipts', () => {
  test('renders full, partial, stale, unverified, covered attention, and receipt navigation in wide layout', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildExternalResolutionWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');

    const messagesPanel = page
      .locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden]) .tl-wp-messages-panel')
      .first();
    await expect(messagesPanel).toBeVisible();
    const messagesReceipts = messagesPanel.locator(
      '[data-external-resolution-surface="messages"]',
    );
    await expect(messagesReceipts).toBeVisible();
    const messagesScroller = await expectUsableMessagesScroller(messagesPanel);
    const fullMessageReceiptButton = messagesPanel
      .locator(`[data-message-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`)
      .first();
    await expectInViewportWithinScroller(messagesScroller, fullMessageReceiptButton);
    const initialMessagesScrollerHeight = await messagesScroller.evaluate(
      (el) => el.clientHeight,
    );

    const fullReceipt = messagesReceipts.locator(
      `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
    );
    await expect(fullReceipt).toBeVisible();
    await expect(fullReceipt.getByText('Resolved externally').first()).toBeVisible();
    await expect(fullReceipt.getByText('Full job').first()).toBeVisible();
    await expect(fullReceipt.getByText(EXTERNAL_RESOLUTION_FULL_SUMMARY).first()).toBeVisible();
    const fullReceiptDetails = fullReceipt
      .locator('details[data-external-resolution-details]')
      .first();
    await ensureDetailsOpen(fullReceiptDetails);
    await expect(
      fullReceipt.getByText(`Receipt: ${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}`).first(),
    ).toBeVisible();
    await expect(fullReceipt.getByText('ev_ext_fixture_full_01').first()).toBeVisible();
    await expect(
      fullReceipt.getByText(EXTERNAL_RESOLUTION_COVERED_JOB_REASON).first(),
    ).toBeVisible();
    await expect(
      fullReceipt.getByText(EXTERNAL_RESOLUTION_COVERED_TASK_REASON).first(),
    ).toBeVisible();

    const partialReceipt = messagesReceipts.locator(
      `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID}"]`,
    );
    await expect(partialReceipt).toBeVisible();
    await expect(
      partialReceipt.getByText('Partially resolved externally').first(),
    ).toBeVisible();
    await expect(partialReceipt.getByText('Partial').first()).toBeVisible();
    await expect(partialReceipt.getByText('task_ext_partial_01').first()).toBeVisible();

    const messagesHistory = messagesReceipts.locator(
      '[data-external-resolution-surface="messages-history"]',
    );
    await ensureDetailsOpen(messagesHistory);
    await expect(messagesHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      messagesHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();
    await expect(
      messagesHistory.locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_STALE_RECEIPT_ID}"]`,
      ),
    ).toBeVisible();
    await expect(
      messagesHistory.locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_UNVERIFIED_RECEIPT_ID}"]`,
      ),
    ).toBeVisible();
    await expect(messagesHistory.getByText('External resolution history (2)')).toBeVisible();
    await expect(
      messagesPanel.getByText(EXTERNAL_RESOLUTION_UNRELATED_QUESTION).first(),
    ).toBeVisible();
    const unrelatedMessageItem = messagesPanel
      .locator('[data-message-item-id]')
      .filter({ hasText: EXTERNAL_RESOLUTION_UNRELATED_QUESTION })
      .first();
    await expect(unrelatedMessageItem).toBeVisible();
    await expect(unrelatedMessageItem.locator('[data-message-receipt-id]')).toHaveCount(0);
    expect(
      await messagesScroller.evaluate((el) => el.clientHeight),
    ).toBe(initialMessagesScrollerHeight);
    await expectUsableMessagesScroller(messagesPanel);
    await expectInViewportWithinScroller(messagesScroller, fullMessageReceiptButton);

    await fullMessageReceiptButton.click();
    await expect(chatSurfaceTab(page, 'Jobs')).toHaveAttribute('aria-selected', 'true');

    const jobsPanel = visibleJobsPanel(page);
    const fullJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_full_01"]',
    );
    await expect(fullJobCard).toBeVisible();
    await expect(fullJobCard).toHaveAttribute('data-expanded', 'true');
    await expect(fullJobCard.getByText('Resolved externally').first()).toBeVisible();
    await expect(fullJobCard.getByText(EXTERNAL_RESOLUTION_FULL_SUMMARY).first()).toBeVisible();
    const jobFullReceipt = fullJobCard
      .locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
      )
      .first();
    await ensureDetailsOpen(
      jobFullReceipt.locator('details[data-external-resolution-details]').first(),
    );
    await expect(
      fullJobCard.getByText(`Receipt: ${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}`).first(),
    ).toBeVisible();
    await expect(
      fullJobCard.getByText(/Phase:\s*external_resolution/).first(),
    ).toBeVisible();
    await expect(fullJobCard.getByText('Acceptance unverified').first()).toBeVisible();
    await expect(fullJobCard.getByText('Status: completed').first()).toBeVisible();
    await expect(
      fullJobCard
        .getByText('Real recorded job result remains visible alongside the external receipt.')
        .first(),
    ).toBeVisible();
    await expect(
      fullJobCard
        .getByText(/Historical failure remains visible even after external resolution/)
        .first(),
    ).toBeVisible();
    await expect(
      fullJobCard.getByText(EXTERNAL_RESOLUTION_COVERED_JOB_REASON).first(),
    ).toBeVisible();

    const partialJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_partial_02"]',
    );
    await expect(partialJobCard).toBeVisible();
    await partialJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(
      partialJobCard.getByText('Partially resolved externally').first(),
    ).toBeVisible();
    await expect(
      partialJobCard.getByText(EXTERNAL_RESOLUTION_PARTIAL_SUMMARY).first(),
    ).toBeVisible();
    await expect(
      partialJobCard.getByText('Unrelated task remains visible').first(),
    ).toBeVisible();

    const blockedJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_blocked_03"]',
    );
    await expect(blockedJobCard).toBeVisible();
    await blockedJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(
      blockedJobCard
        .getByText('Uncovered job approval remains active while stale and unverified receipts exist.')
        .first(),
    ).toBeVisible();
    const blockedHistory = blockedJobCard.locator(
      '[data-external-resolution-surface="jobs-history"]',
    );
    await ensureDetailsOpen(blockedHistory);
    await expect(blockedHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      blockedHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();
    const blockedUnverifiedReceipt = blockedHistory
      .locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_UNVERIFIED_RECEIPT_ID}"]`,
      )
      .first();
    await ensureDetailsOpen(
      blockedUnverifiedReceipt
        .locator('details[data-external-resolution-details]')
        .first(),
    );
    await expect(
      blockedHistory.getByText(EXTERNAL_RESOLUTION_COVERED_JOB_REASON).first(),
    ).toBeVisible();

    const unknownJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_unknown_04"]',
    );
    await expect(unknownJobCard).toBeVisible();
    await unknownJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(
      unknownJobCard.locator('.tl-wp-pill-label', { hasText: /^Unknown$/ }),
    ).toHaveCount(1);
    await expect(unknownJobCard.getByText('Resolved externally').first()).toBeVisible();

    await chatSurfaceTab(page, 'Activity').click();
    const activityPanel = visibleActivityPanel(page);
    const activityReceipts = activityPanel.locator(
      '[data-external-resolution-surface="activity"]',
    );
    await expect(activityReceipts).toBeVisible();
    await expect(
      activityReceipts.locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
      ),
    ).toBeVisible();
    await expect(
      activityReceipts.locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID}"]`,
      ),
    ).toBeVisible();
    const activityHistory = activityPanel.locator(
      '[data-external-resolution-surface="activity-history"]',
    );
    await ensureDetailsOpen(activityHistory);
    await expect(activityHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      activityHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();
    await expect(activityPanel.locator('article.tl-wp-activity-card')).toHaveCount(0);
    await expect(
      activityPanel.getByText('External resolution receipts are recorded in job history.'),
    ).toBeVisible();

    await activityReceipts
      .locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
      )
      .getByRole('button', { name: 'Open in Jobs' })
      .click();
    await expect(chatSurfaceTab(page, 'Jobs')).toHaveAttribute('aria-selected', 'true');
    await expect(
      visibleJobsPanel(page).locator(
        'article.tl-wp-job-card[data-job-id="job_ext_full_01"]',
      ),
    ).toHaveAttribute('data-focused', 'true');
  });

  test('renders the same receipt coverage, blocker history, and unknown boundary in narrow layout', async ({
    page,
  }) => {
    await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildExternalResolutionWorkspaceSnapshot(),
    });
    await page.setViewportSize({ width: 360, height: 500 });
    await page.goto('/');

    const messagesPanel = page
      .locator('.tl-wp-tabpanel[role="tabpanel"]:not([hidden]) .tl-wp-messages-panel')
      .first();
    await expect(messagesPanel).toBeVisible();
    const messagesScroller = await expectUsableMessagesScroller(messagesPanel);
    const fullMessageReceiptButton = messagesPanel
      .locator(`[data-message-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`)
      .first();
    await expectInViewportWithinScroller(messagesScroller, fullMessageReceiptButton);
    const initialMessagesScrollerHeight = await messagesScroller.evaluate(
      (el) => el.clientHeight,
    );

    const messagesReceipts = messagesPanel.locator(
      '[data-external-resolution-surface="messages"]',
    );
    await expect(messagesReceipts).toBeVisible();
    await expect(messagesReceipts.getByText('Resolved externally').first()).toBeVisible();
    await expect(
      messagesReceipts.getByText('Partially resolved externally').first(),
    ).toBeVisible();
    const narrowFullReceipt = messagesReceipts
      .locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
      )
      .first();
    await ensureDetailsOpen(
      narrowFullReceipt.locator('details[data-external-resolution-details]').first(),
    );
    await expect(
      messagesReceipts.getByText(EXTERNAL_RESOLUTION_COVERED_JOB_REASON).first(),
    ).toBeVisible();
    const messagesHistory = messagesReceipts.locator(
      '[data-external-resolution-surface="messages-history"]',
    );
    await ensureDetailsOpen(messagesHistory);
    await expect(messagesHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      messagesHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();
    await expect(
      messagesPanel.getByText(EXTERNAL_RESOLUTION_UNRELATED_QUESTION).first(),
    ).toBeVisible();
    expect(
      await messagesScroller.evaluate((el) => el.clientHeight),
    ).toBe(initialMessagesScrollerHeight);
    await expectUsableMessagesScroller(messagesPanel);
    await expectInViewportWithinScroller(messagesScroller, fullMessageReceiptButton);

    await fullMessageReceiptButton.click();
    await expect(chatSurfaceTab(page, 'Jobs')).toHaveAttribute('aria-selected', 'true');

    const jobsPanel = visibleJobsPanel(page);
    const fullJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_full_01"]',
    );
    await expect(fullJobCard).toHaveAttribute('data-expanded', 'true');
    await expect(fullJobCard.getByText('Resolved externally').first()).toBeVisible();
    const narrowJobFullReceipt = fullJobCard
      .locator(
        `[data-external-resolution-receipt-id="${EXTERNAL_RESOLUTION_FULL_RECEIPT_ID}"]`,
      )
      .first();
    await ensureDetailsOpen(
      narrowJobFullReceipt.locator('details[data-external-resolution-details]').first(),
    );
    await expect(
      fullJobCard.getByText(EXTERNAL_RESOLUTION_COVERED_TASK_REASON).first(),
    ).toBeVisible();

    const blockedJobCard = jobsPanel.locator(
      'article.tl-wp-job-card[data-job-id="job_ext_blocked_03"]',
    );
    await blockedJobCard
      .locator('button.tl-wp-job-row-btn, button[aria-expanded]')
      .first()
      .click();
    await expect(
      blockedJobCard
        .getByText('Uncovered job approval remains active while stale and unverified receipts exist.')
        .first(),
    ).toBeVisible();
    const blockedHistory = blockedJobCard.locator(
      '[data-external-resolution-surface="jobs-history"]',
    );
    await ensureDetailsOpen(blockedHistory);
    await expect(blockedHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      blockedHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();

    await chatSurfaceTab(page, 'Activity').click();
    const activityPanel = visibleActivityPanel(page);
    await expect(
      activityPanel.locator('[data-external-resolution-surface="activity"]'),
    ).toBeVisible();
    const activityHistory = activityPanel.locator(
      '[data-external-resolution-surface="activity-history"]',
    );
    await ensureDetailsOpen(activityHistory);
    await expect(activityHistory.getByText('Stale external receipt').first()).toBeVisible();
    await expect(
      activityHistory.getByText('Unverified external receipt').first(),
    ).toBeVisible();
    await expect(activityPanel.locator('article.tl-wp-activity-card')).toHaveCount(0);
  });
});
