import { expect, test, type Locator, type Page } from '@playwright/test';

import type {
  ObservationJob,
  ObservationRequest,
  ObservationSource,
} from '../src/observation/types.ts';
import {
  advanceWorkspaceGeneratedAtFixture,
  appendChatWorkEventFixture,
  backlogButton,
  BETA_CHAT_ONLY_SENTINEL,
  buildExternalResolutionWorkspaceSnapshot,
  buildFourPartScopeCollisionWorkspaceSnapshot,
  buildHistoricalFailureWorkspaceSnapshot,
  buildMinimalChatWorkspaceSnapshot,
  buildMinimalChatWorkspaceSnapshotWithMalformedWork,
  buildMinimalChatWorkspaceSnapshotWithoutWorkerExecutions,
  buildPendingAcceptanceWorkspaceSnapshot,
  buildWorkerMetadataWorkspaceSnapshot,
  CHAT_ALPHA_IDENTITY,
  CHAT_BETA_SAME_PROJECT_IDENTITY,
  CHAT_GAMMA_IDENTITY,
  CHAT_GAMMA_KEY,
  chatHeader,
  closeBacklogWithEscape,
  createBacklogEvent,
  createBacklogWorkerPresence,
  createWorkerExecution,
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
  getChatWorkWorkerExecutionsFixture,
  installWorkspaceTauriFixture,
  MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
  MINIMAL_CHAT_ACCEPTED_COMPLETION_NOTE,
  MINIMAL_CHAT_ACCEPTED_TITLE,
  MINIMAL_CHAT_ALPHA_ACCEPTED_ITEM_ID,
  MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_CANCELLED_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_CANCELLED_PREVIOUS_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_CHECKING_ITEM_ID,
  MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
  MINIMAL_CHAT_ALPHA_FAILED_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
  MINIMAL_CHAT_ALPHA_UNKNOWN_EXECUTION_REF,
  MINIMAL_CHAT_APPENDED_EVENT_TITLE,
  MINIMAL_CHAT_AUTHORED_LANGUAGE_TITLE,
  MINIMAL_CHAT_BETA_BACKLOG_SENTINEL,
  MINIMAL_CHAT_BETA_WORKER_COMPLETED_EVENT_TITLE,
  MINIMAL_CHAT_BETA_WORKER_EXECUTION_REF,
  MINIMAL_CHAT_BETA_WORKER_TITLE,
  MINIMAL_CHAT_BLOCKED_REASON,
  MINIMAL_CHAT_BLOCKED_TITLE,
  MINIMAL_CHAT_CANCELLED_EVENT_TITLE,
  MINIMAL_CHAT_CANCELLED_PREVIOUS_EVENT_TITLE,
  MINIMAL_CHAT_CANCELLED_TITLE,
  MINIMAL_CHAT_CHECKING_EVENT_TITLE,
  MINIMAL_CHAT_CHECKING_TITLE,
  MINIMAL_CHAT_COWORKER_DOING_TITLE,
  MINIMAL_CHAT_DISPATCH_EVENT_TITLE,
  MINIMAL_CHAT_DOING_TITLE,
  MINIMAL_CHAT_DONE_TITLE,
  MINIMAL_CHAT_FAILED_TITLE,
  MINIMAL_CHAT_GAMMA_BACKLOG_SENTINEL,
  MINIMAL_CHAT_LEGACY_MESSAGE_SENTINEL,
  MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
  MINIMAL_CHAT_MALFORMED_WORK_SENTINEL,
  MINIMAL_CHAT_PREPARING_TITLE,
  MINIMAL_CHAT_READING_ANCHOR_TITLE,
  MINIMAL_CHAT_TODO_TITLE,
  MINIMAL_CHAT_UNKNOWN_EVENT_TITLE,
  MINIMAL_CHAT_UNKNOWN_TITLE,
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
  updateChatWorkWorkerExecutionFixture,
  updateChatWorkWorkerExecutionsFixture,
  updateNativeActivityFixture,
  workspaceSection,
  WRONG_SCOPE_QUARANTINE_SENTINEL,
} from './fixtures/workspace.ts';
import {
  CHAT_WORK_EXECUTION_BRIEF_GAP_MS,
} from '../src/message-scroll.ts';
import {
  parseWorkspaceSnapshot,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  workspaceChatIdentitiesEqual,
  type WorkspaceSnapshot,
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
  options: {
    appearanceTheme?: 'light' | 'dark';
    workspaceSnapshot?: WorkspaceSnapshot;
  } = {},
) {
  const controller = await installWorkspaceTauriFixture(page, {
    workspaceSnapshot: options.workspaceSnapshot ?? buildMinimalChatWorkspaceSnapshot(),
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

async function installWorkerMetadata(
  page: Page,
  viewport: { width: number; height: number } = { width: 1040, height: 760 },
) {
  const controller = await installWorkspaceTauriFixture(page, {
    workspaceSnapshot: buildWorkerMetadataWorkspaceSnapshot(),
  });
  await page.setViewportSize({ width: 1040, height: 760 });
  await page.goto('/');
  await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
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

function workerExecutionFooter(eventRow: Locator, executionRef: string): Locator {
  // Anchor to the exact expected event row. A surface-wide lookup could pass
  // while the footer is attached to an older event or duplicated elsewhere.
  return eventRow.locator(
    `[data-worker-execution-summary="${executionRef}"]`,
  );
}

async function expectSingleWorkerExecutionFooter(
  eventRow: Locator,
  executionRef: string,
  context: string,
): Promise<Locator> {
  const footer = workerExecutionFooter(eventRow, executionRef);
  await expect(
    footer,
    `${context}: expected exactly one execution footer under the latest event row`,
  ).toHaveCount(1);
  await expect(footer).toBeVisible();
  return footer;
}

async function expectNoWorkerExecutionFooter(
  eventRow: Locator,
  executionRef: string,
  context: string,
): Promise<void> {
  await expect(
    workerExecutionFooter(eventRow, executionRef),
    `${context}: an earlier event for the same execution must not carry the footer`,
  ).toHaveCount(0);
}

function resolveAllStreamEventRows(page: Page, title: string): Locator {
  // Two distinct attempts can share display text. The event-row list keeps
  // earlier-placement assertions anchored to every exact row, not only the first.
  return streamScroller(page)
    .locator('[data-chat-event-id]')
    .filter({ hasText: title });
}

async function expectWorkerFooterAtLatestEvent(
  page: Page,
  latestTitle: string,
  earlierTitles: readonly string[],
  executionRef: string,
): Promise<Locator> {
  const latestRow = await resolveStreamEventRow(page, latestTitle);
  await expect(latestRow).toBeVisible();
  for (const title of earlierTitles) {
    const earlierRow = await resolveStreamEventRow(page, title);
    await expect(earlierRow).toBeVisible();
    await expectNoWorkerExecutionFooter(earlierRow, executionRef, latestTitle);
  }
  const footer = await expectSingleWorkerExecutionFooter(
    latestRow,
    executionRef,
    latestTitle,
  );
  await expect(
    workspaceSection(page).locator(
      `[data-worker-execution-summary="${executionRef}"]`,
    ),
    `${latestTitle}: the selected chat must expose one matching footer in total`,
  ).toHaveCount(1);
  return footer;
}

async function readWorkerFooterStatus(footer: Locator): Promise<string | null> {
  return footer.getAttribute('data-worker-status');
}

async function expectWorkerFooterStatus(
  footer: Locator,
  expectedStatus: string,
  expectedLabel: string,
): Promise<string> {
  await expect(footer).toHaveCount(1);
  await expect(footer).toBeVisible();
  await expect(footer).toHaveAttribute('data-worker-status', expectedStatus);
  const status = await readWorkerFooterStatus(footer);
  expect(status, 'Worker execution footer must expose its exact status').toBe(
    expectedStatus,
  );
  const statusLabel = footer.locator('[data-worker-execution-status]');
  await expect(statusLabel).toHaveCount(1);
  await expect(statusLabel).toBeVisible();
  await expect(statusLabel).toHaveText(expectedLabel);
  expect((await footer.innerText()).trim().length).toBeGreaterThan(0);
  return status ?? '';
}

type WorkerFooterDurationExpectation =
  | {
      readonly kind: 'final' | 'elapsed' | 'last_confirmed';
      readonly text: RegExp;
    }
  | null;

interface WorkerFooterExpectation {
  readonly brand: 'gemini' | 'openai' | 'generic';
  readonly model: RegExp | null;
  readonly effort: RegExp | null;
  /** Omit to leave duration unasserted; null requires the duration to be absent. */
  readonly duration?: WorkerFooterDurationExpectation;
  readonly status: string;
  readonly label: string;
}

async function expectWorkerFooterMetadata(
  footer: Locator,
  expected: WorkerFooterExpectation,
): Promise<string> {
  const brand = footer.locator(`[data-brand="${expected.brand}"]`);
  await expect(brand).toHaveCount(1);
  await expect(brand).toBeVisible();

  const model = footer.locator('[data-worker-execution-model="true"]');
  if (expected.model === null) {
    await expect(model).toHaveCount(0);
  } else {
    await expect(model).toHaveCount(1);
    await expect(model).toBeVisible();
    expect((await model.innerText()).trim()).toMatch(expected.model);
  }

  const effort = footer.locator('[data-worker-execution-effort="true"]');
  if (expected.effort === null) {
    await expect(effort).toHaveCount(0);
  } else {
    await expect(effort).toHaveCount(1);
    await expect(effort).toBeVisible();
    expect((await effort.innerText()).trim()).toMatch(expected.effort);
  }

  const status = await expectWorkerFooterStatus(
    footer,
    expected.status,
    expected.label,
  );

  const duration = footer.locator('[data-worker-execution-duration]');
  if (expected.duration === undefined) {
    return status;
  }
  if (expected.duration === null) {
    await expect(duration).toHaveCount(0);
    return status;
  }
  await expect(duration).toHaveCount(1);
  await expect(duration).toBeVisible();
  await expect(duration).toHaveAttribute(
    'data-worker-execution-duration',
    expected.duration.kind,
  );
  expect((await duration.innerText()).replace(/\s+/g, ' ').trim()).toMatch(
    expected.duration.text,
  );
  return status;
}

function elapsedSecondsFromWorkerFooter(text: string): number | null {
  const clockMatch = text.match(/\b(\d+):(\d{2})\b/);
  if (clockMatch) {
    return Number(clockMatch[1]) * 60 + Number(clockMatch[2]);
  }
  const secondsMatch = text.match(/\b(\d+(?:\.\d+)?)\s*s\b/i);
  return secondsMatch ? Number(secondsMatch[1]) : null;
}

async function openDesktopBuildRow(page: Page): Promise<Locator> {
  const settings = page.locator('section[aria-label="Settings"]').first();
  await expect(settings).toBeVisible();
  // The product renders the static compile-time identity as one named group.
  // There is no runtime build-info command or window override to seed.
  const row = settings.getByRole('group', {
    name: 'Desktop build',
    exact: true,
  });
  await expect(row).toHaveCount(1);
  await expect(row).toBeVisible();
  return row;
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

  test('keeps selected chat stable with temporary-unavailable feedback across incomplete refresh and restores on wide and narrow viewports', async ({
    page,
  }) => {
    const controller = await installMinimal(page, { width: 1040, height: 760 });
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();

    const workspace = workspaceSection(page);
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();

    const dropAlphaAt = (generatedAtMs: number): WorkspaceSnapshot =>
      parseWorkspaceSnapshot({
        ...baseSnapshot!,
        generated_at_ms: generatedAtMs,
        observation: {
          ...baseSnapshot!.observation,
          generated_at_ms: generatedAtMs,
          jobs: baseSnapshot!.observation.jobs.filter(
            (job) =>
              !(
                job.source_id === CHAT_ALPHA_IDENTITY.source_id &&
                job.thread_uuid?.toLowerCase() ===
                  CHAT_ALPHA_IDENTITY.thread_uuid
              ),
          ),
          requests: baseSnapshot!.observation.requests.filter(
            (req) =>
              !(
                req.source_id === CHAT_ALPHA_IDENTITY.source_id &&
                req.thread_uuid?.toLowerCase() ===
                  CHAT_ALPHA_IDENTITY.thread_uuid
              ),
          ),
        },
        chats: baseSnapshot!.chats
          .filter(
            (chat) =>
              !workspaceChatIdentitiesEqual(chat.identity, CHAT_ALPHA_IDENTITY),
          )
          .map((chat) =>
            workspaceChatIdentitiesEqual(
              chat.identity,
              CHAT_BETA_SAME_PROJECT_IDENTITY,
            )
              ? { ...chat, updated_at_ms: generatedAtMs - 100 }
              : chat,
          ),
        native_samples: baseSnapshot!.native_samples.filter(
          (sample) =>
            !workspaceChatIdentitiesEqual(
              sample.chat_identity,
              CHAT_ALPHA_IDENTITY,
            ),
        ),
        chat_work: (baseSnapshot!.chat_work ?? []).filter(
          (entry) =>
            !workspaceChatIdentitiesEqual(entry.identity, CHAT_ALPHA_IDENTITY),
        ),
        job_histories: baseSnapshot!.job_histories.filter(
          (history) =>
            history.job.chat_identity === null ||
            !workspaceChatIdentitiesEqual(
              history.job.chat_identity,
              CHAT_ALPHA_IDENTITY,
            ),
        ),
        messages: {
          ...baseSnapshot!.messages,
          items: baseSnapshot!.messages.items.filter(
            (item) =>
              item.scope.chat_identity === null ||
              !workspaceChatIdentitiesEqual(
                item.scope.chat_identity,
                CHAT_ALPHA_IDENTITY,
              ),
          ),
        },
        bounds: {
          ...baseSnapshot!.bounds,
          native_samples_omitted:
            baseSnapshot!.bounds.native_samples_omitted + 1,
        },
      });

    const restoreReorderedAt = (generatedAtMs: number): WorkspaceSnapshot =>
      parseWorkspaceSnapshot({
        ...baseSnapshot!,
        generated_at_ms: generatedAtMs,
        observation: {
          ...baseSnapshot!.observation,
          generated_at_ms: generatedAtMs,
        },
        chats: baseSnapshot!.chats.map((chat) =>
          workspaceChatIdentitiesEqual(
            chat.identity,
            CHAT_BETA_SAME_PROJECT_IDENTITY,
          )
            ? { ...chat, updated_at_ms: generatedAtMs - 100 }
            : chat,
        ),
      });

    // 1. Wide viewport (1040x760): incomplete refresh omits Alpha while Beta is newer.
    await controller.clearInvocations();
    await controller.setWorkspaceSnapshot(
      dropAlphaAt(baseSnapshot!.generated_at_ms + 1_000),
    );
    await triggerWorkspaceRefresh(page);

    const unavailableSurface = workspace.locator(
      '[data-selected-chat-unavailable="true"]',
    );
    await expect(unavailableSurface).toBeVisible();
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      workspace.getByText('Temporarily unavailable', { exact: true }),
    ).toBeVisible();
    await expect(
      workspace.getByText(/temporarily unavailable in the latest refresh/i),
    ).toBeVisible();
    await expect(
      workspace.getByText(/\b(?:archived?|deleted?|removed?)\b/i),
    ).toHaveCount(0);

    // Never show Beta's data or mark Beta selected while Alpha is preserved.
    await expect(chatHeader(page, 'Release Verification Gate')).toHaveCount(0);
    await expect(
      workspace.getByText(MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toHaveCount(0);
    await expect(workspace.getByText(BETA_CHAT_ONLY_SENTINEL)).toHaveCount(0);
    await expect(
      sidebar(page)
        .locator('.tl-chat-item')
        .filter({ hasText: 'Release Verification Gate' })
        .first(),
    ).toHaveAttribute('data-selected', 'false');

    // Actions depending on missing chat evidence stay disabled.
    const backlogBtn = backlogButton(page);
    const openCodexBtn = workspace.getByRole('button', {
      name: 'Open in Codex',
      exact: true,
    });
    await expect(backlogBtn).toBeDisabled();
    await expect(openCodexBtn).toBeDisabled();
    expect(await controller.readArchivePreferencesRaw()).toBeNull();
    await expectNoNeedsYou(page);
    await expectNoHorizontalOverflow(page, 'wide unavailable selected chat');

    // 2. Narrow viewport (390x740): temporary-unavailable feedback and disabled actions stay usable.
    await page.setViewportSize({ width: 390, height: 740 });
    await expect(unavailableSurface).toBeVisible();
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(backlogBtn).toBeDisabled();
    await expect(openCodexBtn).toBeDisabled();
    await expectNoHorizontalOverflow(page, 'narrow unavailable selected chat');

    // 3. Returning snapshot (with Beta still newer/reordered first) restores Alpha automatically.
    await controller.setWorkspaceSnapshot(
      restoreReorderedAt(baseSnapshot!.generated_at_ms + 2_000),
    );
    await triggerWorkspaceRefresh(page);

    await expect(unavailableSurface).toHaveCount(0);
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(
      workspace.getByText(MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toHaveCount(0);
    await expect(backlogBtn).toBeEnabled();
    await expect(openCodexBtn).toBeEnabled();
    await expectNoHorizontalOverflow(page, 'narrow automatically restored chat');

    // 4. If the user manually selects Beta while Alpha is unavailable, Beta stays selected when Alpha returns.
    await page.setViewportSize({ width: 1040, height: 760 });
    await controller.setWorkspaceSnapshot(
      dropAlphaAt(baseSnapshot!.generated_at_ms + 3_000),
    );
    await triggerWorkspaceRefresh(page);
    await expect(unavailableSurface).toBeVisible();

    await selectChat(page, 'Release Verification Gate');
    await expect(unavailableSurface).toHaveCount(0);
    await expect(chatHeader(page, 'Release Verification Gate')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toBeVisible();

    await controller.setWorkspaceSnapshot(
      restoreReorderedAt(baseSnapshot!.generated_at_ms + 4_000),
    );
    await triggerWorkspaceRefresh(page);
    await expect(chatHeader(page, 'Release Verification Gate')).toBeVisible();
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_BETA_BACKLOG_SENTINEL),
    ).toBeVisible();
    await expect(
      workspace.getByText(MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toHaveCount(0);

    const openCalls = (await controller.getInvocations()).filter(
      (call) => call.cmd === 'desktop_open_codex_chat',
    );
    expect(openCalls).toHaveLength(0);
  });
});

test('keeps scoped planning rows and Active chats consistent across refresh and reopen', async ({ page }) => {
  const base = buildMinimalChatWorkspaceSnapshot();
  const startedAt = FIXTURE_BASE_NOW_MS - 3_000;
  const advisor: ObservationRequest = {
    source_id: CHAT_ALPHA_IDENTITY.source_id,
    id: 'obsreq_advisor_fixture',
    job_id: null,
    parent_request_id: null,
    title: 'Advisor consultation',
    operation: 'coworker.planning.advisor',
    project_label: 'Threadlane',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: null,
    state: 'unknown',
    started_at_ms: startedAt,
    finished_at_ms: null,
    updated_at_ms: startedAt,
    duration_ms: null,
    actor: { label: null, role: 'coworker', source: null, model: null, effort: null, evidence: 'unknown' },
    executor: {
      tool: 'codex-cli', provider: 'codex_cli', role: 'advisor',
      requested_model: 'gpt-6.2-astra', observed_model: null,
      requested_effort: 'max', observed_effort: null,
    },
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: null,
  };
  const orchestrator: ObservationRequest = {
    ...advisor,
    id: 'obsreq_orchestrator_fixture',
    operation: 'coworker.planning.orchestrator',
    executor: { ...advisor.executor!, role: 'orchestrator', requested_model: 'gpt-6.2-sol', requested_effort: 'xhigh' },
    started_at_ms: startedAt + 1_000,
    updated_at_ms: startedAt + 1_000,
    state: 'running',
  };
  const snapshotWithRequests = (atMs: number, requests: ObservationRequest[]) => ({
    ...base,
    generated_at_ms: atMs,
    observation: { ...base.observation, generated_at_ms: atMs, requests },
  });
  const started = snapshotWithRequests(FIXTURE_BASE_NOW_MS, [advisor, orchestrator]);
  const controller = await installMinimal(page, undefined, { workspaceSnapshot: started });
  const stream = streamScroller(page);
  const planning = stream.locator('[data-planning-role]');
  const advisorRow = planning.filter({ hasText: 'Advisor consultation' });
  const advisorKey = await advisorRow.getAttribute('data-chat-event-key');
  expect(advisorKey).toBeTruthy();
  await expect(planning.locator('.tl-cw-event-action')).toHaveText(['Started', 'Started']);
  await expect(planning.locator('.tl-cw-execution-duration')).toHaveCount(0);
  await expect(workspaceSection(page).locator('[data-agent-role="advisor"], [data-agent-role="orchestrator"]')).toHaveCount(0);

  const finishedAt = FIXTURE_BASE_NOW_MS + 3_000;
  const finished = snapshotWithRequests(finishedAt, [
    advisor,
    { ...advisor, state: 'completed', finished_at_ms: finishedAt, updated_at_ms: finishedAt, duration_ms: 6_000 },
    { ...orchestrator, state: 'failed', finished_at_ms: finishedAt, updated_at_ms: finishedAt },
    {
      ...orchestrator,
      id: 'obsreq_cancelled_fixture',
      state: 'cancelled',
      operation: 'coworker.planning.architect',
      started_at_ms: finishedAt - 2_000,
      finished_at_ms: finishedAt,
      updated_at_ms: finishedAt,
      executor: null,
    },
  ]);
  await controller.setWorkspaceSnapshot(finished);
  await triggerWorkspaceRefresh(page);
  await expect(planning.locator('.tl-cw-event-action')).toHaveText(['Completed', 'Failed', 'Cancelled']);
  await expect(planning.locator('.tl-cw-execution-duration')).toHaveText(['6s', '5s', '2s']);
  await expect(planning.first()).toHaveAttribute('data-chat-event-key', advisorKey!);
  await expect(planning.first().getByText('gpt-6.2-astra', { exact: true })).toBeVisible();
  await expect(planning.nth(1).getByText('xhigh', { exact: true })).toBeVisible();
  await expectNoNeedsYouInScope(workspaceSection(page));

  await selectChat(page, 'Release Verification Gate');
  await expect(planning).toHaveCount(0);
  await lifecycleFilterControl(page, 'archived').click();
  await expect(sidebar(page).getByText('Workspace UI Contract', { exact: true })).toHaveCount(0);
  await lifecycleFilterControl(page, 'active').click();
  await selectChat(page, 'Workspace UI Contract');
  await expect(planning).toHaveCount(3);
  expect(await controller.readArchivePreferencesRaw()).toBeNull();

  const reopened = await page.context().newPage();
  const reloadedController = await installMinimal(reopened, { width: 520, height: 750 }, {
    workspaceSnapshot: finished,
  });
  const reopenedRows = streamScroller(reopened).locator('[data-planning-role]');
  await expect(reopenedRows.locator('.tl-cw-event-action')).toHaveText(['Completed', 'Failed', 'Cancelled']);
  await expect(reopenedRows.first()).toHaveAttribute('data-chat-event-key', advisorKey!);
  await expectNoHorizontalOverflow(reopened);
  expect(await reloadedController.readArchivePreferencesRaw()).toBeNull();
  await reopened.close();
});

test('keeps jobless Advisor completion on default source (project_path=null) in matching chat and refuses wrong-project or conflicting scopes', async ({
  page,
}) => {
  const base = buildMinimalChatWorkspaceSnapshot();
  const baseSource = base.observation.sources[0]!;
  const nullPathSource: ObservationSource = {
    ...baseSource,
    project_path: null,
  };
  const wrongProject = '/workspace/threadlane-other';
  const startedAt = FIXTURE_BASE_NOW_MS - 6_000;
  const finishedAt = FIXTURE_BASE_NOW_MS;

  const conflictingOtherJob: ObservationJob = {
    source_id: CHAT_ALPHA_IDENTITY.source_id,
    id: 'job_conflicting_other_project',
    project_label: 'threadlane-other',
    project_path: wrongProject,
    title: 'Wrong Project Job',
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: 'Other Project Scope',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    plan_id: 'plan_other_project',
    plan_revision: 1,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: 'accepted',
    updated_at_ms: finishedAt - 1_000,
    active_call_count: 0,
    tasks: [],
    summary: null,
  };

  const legitimateAdvisor: ObservationRequest = {
    source_id: CHAT_ALPHA_IDENTITY.source_id,
    id: 'obsreq_advisor_jobless_exact_scope',
    job_id: null,
    parent_request_id: null,
    title: 'Advisor consultation',
    operation: 'coworker.planning.advisor',
    project_label: 'threadlane-alpha',
    project_path: CHAT_ALPHA_IDENTITY.canonical_project_scope,
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: null,
    state: 'completed',
    started_at_ms: startedAt,
    finished_at_ms: finishedAt,
    updated_at_ms: finishedAt,
    duration_ms: 6_000,
    actor: {
      label: null,
      role: 'coworker',
      source: null,
      model: null,
      effort: null,
      evidence: 'unknown',
    },
    executor: {
      tool: 'codex-cli',
      provider: 'codex_cli',
      role: 'advisor',
      requested_model: 'gpt-6.2-astra',
      observed_model: null,
      requested_effort: 'max',
      observed_effort: null,
    },
    usage: null,
    delivery: [],
    children: [],
    children_count: 0,
    summary: null,
  };

  const wrongProjectSameBindingThread: ObservationRequest = {
    ...legitimateAdvisor,
    id: 'obsreq_wrong_project_same_binding_thread',
    operation: 'coworker.planning.orchestrator',
    project_path: wrongProject,
    executor: {
      ...legitimateAdvisor.executor!,
      role: 'orchestrator',
      requested_model: 'gpt-6.2-sol',
      requested_effort: 'xhigh',
    },
  };

  const invalidProjectScope: ObservationRequest = {
    ...legitimateAdvisor,
    id: 'obsreq_invalid_relative_project_scope',
    operation: 'coworker.planning.architect',
    project_path: 'relative/invalid-path',
    executor: null,
  };

  const conflictingJobScope: ObservationRequest = {
    ...wrongProjectSameBindingThread,
    id: 'obsreq_conflicting_job_scope',
    job_id: conflictingOtherJob.id,
    project_path: CHAT_ALPHA_IDENTITY.canonical_project_scope,
  };

  const buildSnapshot = (
    source: ObservationSource,
    requests: ObservationRequest[],
    jobs: ObservationJob[] = [conflictingOtherJob],
  ) => ({
    ...base,
    generated_at_ms: finishedAt,
    observation: {
      ...base.observation,
      generated_at_ms: finishedAt,
      sources: [source],
      jobs,
      requests,
    },
  });

  const controller = await installMinimal(page, undefined, {
    workspaceSnapshot: buildSnapshot(nullPathSource, [
      legitimateAdvisor,
      wrongProjectSameBindingThread,
      invalidProjectScope,
      conflictingJobScope,
    ]),
  });
  await selectChat(page, 'Workspace UI Contract');

  const stream = streamScroller(page);
  const planning = stream.locator('[data-planning-role]');
  await expect(planning).toHaveCount(1);
  await expect(planning.first()).toBeVisible();
  await expect(planning.first()).toHaveAttribute('data-planning-role', 'advisor');
  await expect(planning.first().locator('.tl-cw-event-action')).toBeVisible();
  await expect(planning.first().locator('.tl-cw-event-action')).toHaveText('Completed');
  await expect(planning.first().locator('.tl-cw-execution-duration')).toBeVisible();
  await expect(planning.first().locator('.tl-cw-execution-duration')).toHaveText('6s');
  await expect(planning.first().getByText('gpt-6.2-astra', { exact: true })).toBeVisible();
  await expect(
    stream.locator('[data-planning-role="orchestrator"], [data-planning-role="architect"]'),
  ).toHaveCount(0);
  await expect(stream.getByText('Orchestrator technical planning', { exact: true })).toHaveCount(0);
  await expect(stream.getByText('gpt-6.2-sol', { exact: true })).toHaveCount(0);
  await expect(
    workspaceSection(page).locator('[data-agent-role="advisor"], [data-agent-role="orchestrator"]'),
  ).toHaveCount(0);
  await expectNoNeedsYouInScope(workspaceSection(page));
  await expectNoHorizontalOverflow(page);

  // Verify isolation across chats and refresh persistence.
  await selectChat(page, 'Release Verification Gate');
  await expect(planning).toHaveCount(0);
  await selectChat(page, 'Workspace UI Contract');
  await expect(planning).toHaveCount(1);
  await expect(planning.first()).toHaveAttribute('data-planning-role', 'advisor');
  await triggerWorkspaceRefresh(page);
  await expect(planning).toHaveCount(1);
  await expect(planning.first()).toHaveAttribute('data-planning-role', 'advisor');
  await expect(planning.first().locator('.tl-cw-event-action')).toHaveText('Completed');
  await expect(planning.first().locator('.tl-cw-execution-duration')).toHaveText('6s');
  await expect(planning.first().getByText('gpt-6.2-astra', { exact: true })).toBeVisible();

  // Shared source project_path hint must not override explicit canonical project_path or verified job,
  // while still refusing omitted-project_path jobless requests.
  const sharedHintSource: ObservationSource = {
    ...nullPathSource,
    project_path: wrongProject,
  };
  const alphaVerifiedJob: ObservationJob = {
    ...conflictingOtherJob,
    id: 'job_alpha_verified',
    project_label: 'threadlane-alpha',
    project_path: CHAT_ALPHA_IDENTITY.canonical_project_scope,
    plan_id: 'plan_alpha_verified',
  };
  const verifiedAdvisorWithSharedSource: ObservationRequest = {
    ...legitimateAdvisor,
    id: 'obsreq_advisor_verified_job_shared_source',
    job_id: alphaVerifiedJob.id,
  };
  const omittedScopeJoblessOrchestrator: ObservationRequest = {
    ...wrongProjectSameBindingThread,
    id: 'obsreq_omitted_scope_jobless',
    job_id: null,
    project_path: null,
  };
  await controller.setWorkspaceSnapshot(
    buildSnapshot(
      sharedHintSource,
      [verifiedAdvisorWithSharedSource, omittedScopeJoblessOrchestrator],
      [alphaVerifiedJob, conflictingOtherJob],
    ),
  );
  await triggerWorkspaceRefresh(page);
  await expect(planning).toHaveCount(1);
  await expect(planning.first()).toBeVisible();
  await expect(planning.first()).toHaveAttribute('data-planning-role', 'advisor');
  await expect(planning.first().locator('.tl-cw-event-action')).toHaveText('Completed');
  await expect(planning.first().locator('.tl-cw-execution-duration')).toHaveText('6s');
  await expect(planning.first().getByText('gpt-6.2-astra', { exact: true })).toBeVisible();
  await expect(stream.locator('[data-planning-role="orchestrator"]')).toHaveCount(0);
  await expect(stream.getByText('gpt-6.2-sol', { exact: true })).toHaveCount(0);
  await expectNoHorizontalOverflow(page);
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

test.describe('Sidebar status presentation and navigation counters', () => {
  function escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function sidebarChatButton(scope: Locator, threadLabel: string): Locator {
    const exactLabel = new RegExp(`^\\s*${escapeRegExp(threadLabel)}\\s*$`);
    return scope
      .locator('button.tl-chat-item')
      .filter({
        has: scope.page().locator('.tl-chat-item-label', { hasText: exactLabel }),
      });
  }

  function projectHeaderButton(scope: Locator, projectLabel: string): Locator {
    return scope
      .locator(`section.tl-project-group[aria-label="${projectLabel}"] > button.tl-project-header`)
      .first();
  }

  async function expectUnclippedPill(
    pill: Locator,
    expectedText: string,
    expectedAriaLabel: string,
    maxRightX?: number,
  ): Promise<void> {
    await expect(pill).toHaveCount(1);
    await expect(pill).toBeVisible();
    await expect(pill).toHaveText(expectedText);
    await expect(pill).toHaveAttribute('aria-label', expectedAriaLabel);
    await expect(pill).not.toHaveAttribute('title');
    expect(
      await pill.evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
    ).toBe(true);
    if (maxRightX !== undefined) {
      const box = await pill.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.x).toBeGreaterThanOrEqual(0);
      expect(box!.x + box!.width).toBeLessThanOrEqual(maxRightX + 1);
    }
  }

  test('distinguishes confirmed idle and terminal samples from missing, unknown, and stale observations while preserving working, waiting, and passive events', async ({
    page,
  }) => {
    const base = buildMinimalChatWorkspaceSnapshot();
    const missingGammaSampleSnapshot = parseWorkspaceSnapshot({
      ...base,
      native_samples: base.native_samples.filter(
        (sample) => !workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_GAMMA_IDENTITY),
      ),
    });

    const controller = await installMinimal(page, { width: 1040, height: 760 }, {
      workspaceSnapshot: missingGammaSampleSnapshot,
    });
    const sidebarEl = sidebar(page);

    // Alpha has live Worker presence (Working); passive events stay readable.
    const alphaRow = sidebarChatButton(sidebarEl, 'Workspace UI Contract');
    await expect(alphaRow).toHaveCount(1);
    await expect(alphaRow.locator('[data-sidebar-status="working"]')).toHaveText('Working');
    await expect(alphaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'working');
    await expect(
      resolveStreamEventRow(page, `Blocked: ${MINIMAL_CHAT_BLOCKED_TITLE}`),
    ).toBeVisible();
    await expect(
      resolveStreamEventRow(
        page,
        `Worker result recorded: Failed for ${MINIMAL_CHAT_FAILED_TITLE}`,
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
        `Preparing Worker execution for ${MINIMAL_CHAT_PREPARING_TITLE}`,
      ),
    ).toBeVisible();

    // Beta has a real fresh idle native sample -> confirmed Idle (neutral).
    const betaRow = sidebarChatButton(sidebarEl, 'Release Verification Gate');
    await expect(betaRow).toHaveCount(1);
    await expect(betaRow.locator('[data-sidebar-status="idle"]')).toHaveText('Idle');
    await expect(betaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');

    // Gamma has no native sample and no worker presence -> Status unavailable,
    // never fabricating "No live agent" or any human-attention warning.
    const gammaRow = sidebarChatButton(sidebarEl, 'Archived Migration Audit');
    await expect(gammaRow).toHaveCount(1);
    await expect(gammaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(gammaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(gammaRow.locator('[data-tone="attention"]')).toHaveCount(0);
    await expect(gammaRow.getByText('No live agent')).toHaveCount(0);
    await expectNoNeedsYou(page);

    // Current explicit terminal worker presence (non_live, no native sample, no
    // unconfirmed Doing worker) renders No live agent alongside real confirmed
    // Idle (Beta) and missing observation (Gamma).
    const alphaWork = getChatWorkFixture(missingGammaSampleSnapshot, CHAT_ALPHA_IDENTITY);
    const alphaTerminalPresence = alphaWork.worker_presence.filter(
      (entry) => entry.availability === 'non_live' && entry.live === false,
    );
    expect(alphaTerminalPresence.length).toBeGreaterThan(0);
    const terminalAlphaSnapshot = advanceWorkspaceGeneratedAtFixture(
      parseWorkspaceSnapshot({
        ...updateChatWorkFixture(missingGammaSampleSnapshot, CHAT_ALPHA_IDENTITY, {
          items: alphaWork.items.filter((item) => item.status !== 'doing'),
          worker_presence: alphaTerminalPresence,
        }),
        native_samples: missingGammaSampleSnapshot.native_samples.filter(
          (sample) => !workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY),
        ),
      }),
      FIXTURE_BASE_NOW_MS + 1_000,
    );
    await controller.setWorkspaceSnapshot(terminalAlphaSnapshot);
    await triggerWorkspaceRefresh(page);

    await expect(alphaRow.locator('[data-sidebar-status="terminal"]')).toHaveText('No live agent');
    await expect(alphaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(alphaRow.locator('[data-tone="attention"]')).toHaveCount(0);
    await expect(betaRow.locator('[data-sidebar-status="idle"]')).toHaveText('Idle');
    await expect(betaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(gammaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(gammaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expectNoNeedsYou(page);

    // Waiting presentation is preserved while unknown native activity stays unavailable.
    const waitingBetaBase = updateNativeActivityFixture(
      updateNativeActivityFixture(base, CHAT_BETA_SAME_PROJECT_IDENTITY, 'waiting_for_approval'),
      CHAT_GAMMA_IDENTITY,
      'unknown',
    );
    const waitingBetaAndUnknownGamma = advanceWorkspaceGeneratedAtFixture(
      parseWorkspaceSnapshot({
        ...waitingBetaBase,
        native_samples: waitingBetaBase.native_samples.map((sample) =>
          workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_BETA_SAME_PROJECT_IDENTITY)
            ? { ...sample, sequence: sample.sequence + 1 }
            : sample,
        ),
      }),
      FIXTURE_BASE_NOW_MS + 2_000,
    );
    await controller.setWorkspaceSnapshot(waitingBetaAndUnknownGamma);
    await triggerWorkspaceRefresh(page);

    await expect(alphaRow.locator('[data-sidebar-status="working"]')).toHaveText('Working');
    await expect(alphaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'working');
    await expect(betaRow.locator('[data-sidebar-status="waiting"]')).toHaveText(
      'Waiting for you in Codex',
    );
    await expect(betaRow.locator('[data-sidebar-status="waiting"]')).toHaveAttribute(
      'data-tone',
      'attention',
    );
    await expect(betaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'attention');
    await expect(gammaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(gammaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(gammaRow.locator('[data-tone="attention"]')).toHaveCount(0);
    await expect(gammaRow.getByText('No live agent')).toHaveCount(0);
    await expectSingleNeedsYou(page);

    // Aging past the stale threshold clears working/waiting and switches all
    // sidebar rows to neutral Status unavailable without warnings.
    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + STALE_ADVANCE_MS);
    await expect(alphaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(alphaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(alphaRow.getByText('No live agent')).toHaveCount(0);
    await expect(betaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(betaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(gammaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(gammaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(sidebarEl.locator('[data-tone="attention"]')).toHaveCount(0);
    await expect(sidebarEl.locator('[data-nav-counter], [data-project-counter]')).toHaveCount(0);
    await expectNoNeedsYou(page);
  });

  test('counts working agents separately from waiting chats with singular and plural visible and accessible units', async ({
    page,
  }) => {
    const base = buildMinimalChatWorkspaceSnapshot();
    // One chat (Alpha) with two working agents: live Worker + working Coworker.
    const twoWorkingAgentsOneChat = updateNativeActivityFixture(
      base,
      CHAT_ALPHA_IDENTITY,
      'working',
    );

    const controller = await installMinimal(page, { width: 1040, height: 760 }, {
      workspaceSnapshot: twoWorkingAgentsOneChat,
    });
    const sidebarEl = sidebar(page);
    const projectsNavBtn = sidebarEl.getByRole('button', { name: 'Projects', exact: true });
    const alphaProjectBtn = projectHeaderButton(sidebarEl, 'threadlane-alpha');
    const legacyProjectBtn = projectHeaderButton(sidebarEl, 'threadlane-legacy');

    // Global and project working counters report 2 working agents for 1 chat.
    await expectUnclippedPill(
      projectsNavBtn.locator('[data-nav-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
    );
    await expect(projectsNavBtn.locator('[data-nav-counter="waiting-chats"]')).toHaveCount(0);
    await expectUnclippedPill(
      alphaProjectBtn.locator('[data-project-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
    );
    await expect(alphaProjectBtn.locator('[data-project-counter="waiting-chats"]')).toHaveCount(0);
    await expect(legacyProjectBtn.locator('[data-project-counter]')).toHaveCount(0);

    // Single-counter sidebar rows stay compact on one row.
    const singleNavBox = await projectsNavBtn.boundingBox();
    const singleProjectBox = await alphaProjectBtn.boundingBox();
    expect(singleNavBox).not.toBeNull();
    expect(singleProjectBox).not.toBeNull();
    expect(singleNavBox!.height).toBeLessThanOrEqual(52);
    expect(singleProjectBox!.height).toBeLessThanOrEqual(48);
    await expectNoHorizontalOverflow(page, 'desktop sidebar with 2 working agents');

    // Adding one waiting chat in the same project keeps working agents (2) and
    // waiting chats (1) separate without mixing units.
    const twoWorkingOneWaiting = updateNativeActivityFixture(
      twoWorkingAgentsOneChat,
      CHAT_BETA_SAME_PROJECT_IDENTITY,
      'waiting_for_approval',
    );
    await controller.setWorkspaceSnapshot(twoWorkingOneWaiting);
    await triggerWorkspaceRefresh(page);

    await expectUnclippedPill(
      projectsNavBtn.locator('[data-nav-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
    );
    await expectUnclippedPill(
      projectsNavBtn.locator('[data-nav-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
    );
    await expect(
      lifecycleFilterControl(page, 'active').locator('.tl-status-dot[data-tone="attention"]'),
    ).toHaveAttribute('aria-label', '1 active chat waiting for you');
    await expectUnclippedPill(
      alphaProjectBtn.locator('[data-project-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
    );
    await expectUnclippedPill(
      alphaProjectBtn.locator('[data-project-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
    );
    const projectNameBox = await alphaProjectBtn.locator('.tl-project-name').boundingBox();
    expect(projectNameBox).not.toBeNull();
    expect(projectNameBox!.width).toBeGreaterThanOrEqual(72);
    const twoCounterProjectBox = await alphaProjectBtn.boundingBox();
    expect(twoCounterProjectBox).not.toBeNull();
    expect(twoCounterProjectBox!.height).toBeLessThanOrEqual(76);
    await expectSingleNeedsYou(page);
    await expectNoHorizontalOverflow(page, 'desktop sidebar with mixed working and waiting counters');

    // Singular working agent (1 live Worker on Alpha) + plural waiting chats
    // (Beta in threadlane-alpha and Gamma in threadlane-legacy).
    const oneWorkingTwoWaiting = updateNativeActivityFixture(
      updateNativeActivityFixture(base, CHAT_BETA_SAME_PROJECT_IDENTITY, 'waiting_for_approval'),
      CHAT_GAMMA_IDENTITY,
      'waiting_for_input',
    );
    await controller.setWorkspaceSnapshot(oneWorkingTwoWaiting);
    await triggerWorkspaceRefresh(page);

    await expectUnclippedPill(
      projectsNavBtn.locator('[data-nav-counter="working-agents"]'),
      '1 working agent',
      '1 working agent',
    );
    await expectUnclippedPill(
      projectsNavBtn.locator('[data-nav-counter="waiting-chats"]'),
      '2 waiting chats',
      '2 chats waiting for you',
    );
    await expect(
      lifecycleFilterControl(page, 'active').locator('.tl-status-dot[data-tone="attention"]'),
    ).toHaveAttribute('aria-label', '2 active chats waiting for you');
    await expectUnclippedPill(
      alphaProjectBtn.locator('[data-project-counter="working-agents"]'),
      '1 working agent',
      '1 working agent',
    );
    await expectUnclippedPill(
      alphaProjectBtn.locator('[data-project-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
    );
    await expect(
      legacyProjectBtn.locator('[data-project-counter="working-agents"]'),
    ).toHaveCount(0);
    await expectUnclippedPill(
      legacyProjectBtn.locator('[data-project-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
    );

    // Collapsed desktop sidebar surfaces the exact same units in the titlebar.
    await page.getByRole('button', { name: /collapse sidebar/i }).first().click();
    const titlebar = page.locator('header.tl-titlebar').first();
    await expectUnclippedPill(
      titlebar.locator('[data-nav-counter="working-agents"]'),
      '1 working agent',
      '1 working agent',
      1040,
    );
    await expectUnclippedPill(
      titlebar.locator('[data-nav-counter="waiting-chats"]'),
      '2 waiting chats',
      '2 chats waiting for you',
      1040,
    );
    await expectNoHorizontalOverflow(page, 'collapsed sidebar titlebar counters');
  });

  test('keeps working-agent and waiting-chat units readable and unclipped in narrow navigation at 760px and 390px', async ({
    page,
  }) => {
    const base = buildMinimalChatWorkspaceSnapshot();
    const narrowBase = updateNativeActivityFixture(
      updateNativeActivityFixture(base, CHAT_ALPHA_IDENTITY, 'working'),
      CHAT_BETA_SAME_PROJECT_IDENTITY,
      'waiting_for_approval',
    );
    const narrowSnapshot = parseWorkspaceSnapshot({
      ...narrowBase,
      native_samples: narrowBase.native_samples.filter(
        (sample) => !workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_GAMMA_IDENTITY),
      ),
    });

    await installMinimal(page, { width: 760, height: 760 }, {
      workspaceSnapshot: narrowSnapshot,
    });

    const titlebar = page.locator('header.tl-titlebar').first();
    const titlebarPills = titlebar.locator('.tl-titlebar-pills');
    await expectUnclippedPill(
      titlebarPills.locator('[data-nav-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
      760,
    );
    await expectUnclippedPill(
      titlebarPills.locator('[data-nav-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
      760,
    );
    await expectNoHorizontalOverflow(page, '760px narrow titlebar navigation counters');

    await page.setViewportSize({ width: 390, height: 740 });
    await expectUnclippedPill(
      titlebarPills.locator('[data-nav-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
      390,
    );
    await expectUnclippedPill(
      titlebarPills.locator('[data-nav-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
      390,
    );
    expect(
      await titlebarPills.evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
    ).toBe(true);
    await expectNoHorizontalOverflow(page, '390px narrow titlebar navigation counters');

    const drawerTrigger = titlebar.getByRole('button', { name: /open navigation drawer/i }).first();
    await drawerTrigger.click();
    const drawer = page.locator('#tl-navigation-drawer');
    await expect(drawer).toBeVisible();

    const drawerProjectsBtn = drawer.getByRole('button', { name: 'Projects', exact: true });
    await expectUnclippedPill(
      drawerProjectsBtn.locator('[data-nav-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
      390,
    );
    await expectUnclippedPill(
      drawerProjectsBtn.locator('[data-nav-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
      390,
    );

    const drawerAlphaProject = projectHeaderButton(drawer, 'threadlane-alpha');
    await expectUnclippedPill(
      drawerAlphaProject.locator('[data-project-counter="working-agents"]'),
      '2 working agents',
      '2 working agents',
      390,
    );
    await expectUnclippedPill(
      drawerAlphaProject.locator('[data-project-counter="waiting-chats"]'),
      '1 waiting chat',
      '1 chat waiting for you',
      390,
    );

    const drawerAlphaRow = sidebarChatButton(drawer, 'Workspace UI Contract');
    await expect(drawerAlphaRow).toHaveCount(1);
    await expect(drawerAlphaRow.locator('[data-sidebar-status="working"]')).toHaveText('Working');
    await expect(drawerAlphaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'working');

    const drawerBetaRow = sidebarChatButton(drawer, 'Release Verification Gate');
    await expect(drawerBetaRow).toHaveCount(1);
    await expect(drawerBetaRow.locator('[data-sidebar-status="waiting"]')).toHaveText(
      'Waiting for you in Codex',
    );
    await expect(drawerBetaRow.locator('[data-sidebar-status="waiting"]')).toHaveAttribute(
      'data-tone',
      'attention',
    );
    await expect(drawerBetaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'attention');

    const drawerGammaRow = sidebarChatButton(drawer, 'Archived Migration Audit');
    await expect(drawerGammaRow).toHaveCount(1);
    await expect(drawerGammaRow.locator('[data-sidebar-status="unknown"]')).toHaveText(
      'Status unavailable',
    );
    await expect(drawerGammaRow.locator('.tl-status-dot')).toHaveAttribute('data-tone', 'neutral');
    await expect(drawerGammaRow.locator('[data-tone="attention"]')).toHaveCount(0);
    await expect(drawerGammaRow.getByText('No live agent')).toHaveCount(0);

    expect(
      await drawer.evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
    ).toBe(true);
    await expectNoHorizontalOverflow(page, '390px narrow navigation drawer');

    await page.keyboard.press('Escape');
    await expect(drawer).toHaveCount(0);
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

test.describe('Compact Worker execution metadata footer', () => {
  test('keeps the fresh Working badge, Coworker/Worker live strip, and elapsed timer, then drops liveness for expired and stale samples', async ({
    page,
  }) => {
    const liveSnapshot = updateNativeActivityFixture(
      buildWorkerMetadataWorkspaceSnapshot(),
      CHAT_ALPHA_IDENTITY,
      'working',
    );
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: liveSnapshot,
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    const workspace = workspaceSection(page);
    const header = chatHeader(page, 'Workspace UI Contract');
    await expect(header.getByText(/^Working$/i).first()).toBeVisible();
    await expectLiveAgentWorking(workspace, 'Coworker');
    await expectLiveAgentWorking(workspace, 'Worker');

    const liveFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
        `Worker execution attached for ${MINIMAL_CHAT_DOING_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
    );
    // Exact current execution presence is the only proof that the durable
    // dispatch metadata is confirmed live (`working` internally, durable `Started` visibly).
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: {
        kind: 'elapsed',
        text: /^Elapsed\s+\d+(?:\.\d+)?s$/i,
      },
    });
    const initialElapsed = elapsedSecondsFromWorkerFooter(await liveFooter.innerText());
    expect(initialElapsed).not.toBeNull();
    expect(initialElapsed!).toBeGreaterThanOrEqual(14);

    // The timer comes from the actual start timestamp and keeps advancing while
    // the original sample is still inside its freshness window. Advance the
    // browser clock only; no new snapshot or sample is injected.
    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 3_000);
    await expectWorkerFooterStatus(liveFooter, 'working', 'Started');
    await expect(header.getByText(/^Working$/i).first()).toBeVisible();
    await expectLiveAgentWorking(workspace, 'Worker');
    await expectLiveAgentWorking(workspace, 'Coworker');
    await expect
      .poll(
        async () =>
          elapsedSecondsFromWorkerFooter(await liveFooter.innerText()) ?? -1,
        { timeout: 5_000 },
      )
      .toBeGreaterThanOrEqual(initialElapsed! + 2);

    // The original presence deadline is sampled_at_ms + max_sample_age_ms, so
    // advancing the same browser clock past it expires the only proof without
    // injecting any fresh snapshot. The footer keeps its durable item/execution
    // scope but renders Status unavailable and no ticking elapsed timer.
    const expiredClockMs =
      FIXTURE_BASE_NOW_MS + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 1_000;
    await page.clock.setFixedTime(expiredClockMs);
    await expect
      .poll(
        async () => readWorkerFooterStatus(liveFooter),
        { timeout: 5_000 },
      )
      .toBe('unavailable');
    await expect(liveFooter).toBeVisible();
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });
    await expectNoVisibleWorking(workspace);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectNoLiveAgentWorking(workspace, 'Coworker');

    const stalePresence = createBacklogWorkerPresence({
      itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
      executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
      availability: 'live',
      freshness: 'stale',
      live: false,
      sampledAtMs: FIXTURE_BASE_NOW_MS - 10_000,
      sampleAgeMs: 10_000,
      maxSampleAgeMs: 5_000,
      expiresAtMs: FIXTURE_BASE_NOW_MS - 5_000,
    });
    const staleSnapshot = updateChatWorkPresenceFixture(
      updateNativeActivityFixture(
        advanceWorkspaceGeneratedAtFixture(liveSnapshot, expiredClockMs),
        CHAT_ALPHA_IDENTITY,
        'idle',
      ),
      CHAT_ALPHA_IDENTITY,
      [stalePresence],
    );
    await controller.setWorkspaceSnapshot(staleSnapshot);
    await triggerWorkspaceRefresh(page);

    await expect(liveFooter).toBeVisible();
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });
    await expect
      .poll(async () => /working/i.test(await liveFooter.innerText()))
      .toBe(false);
    await expectNoVisibleWorking(workspace);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectNoNeedsYou(page);
  });

  test('treats dispatch without live presence as non-Working and keeps failure, cancellation, and unknown distinct without NeedsYou', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page);
    const workspace = workspaceSection(page);

    const dispatchFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_DISPATCH_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_PREPARING_TITLE}`],
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
    );
    const dispatchStatus = await expectWorkerFooterMetadata(dispatchFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'claimed',
      label: 'Claimed',
      duration: null,
    });
    expect(dispatchStatus).toBe('claimed');
    await expect
      .poll(async () => /working/i.test(await dispatchFooter.innerText()))
      .toBe(false);

    // The only live Worker row belongs to the fresh Doing execution, never to
    // the dispatch receipt above.
    const liveWorkerRows = liveAgentList(workspace)
      .locator('li[data-agent-role="worker"]')
      .filter({ hasText: MINIMAL_CHAT_DOING_TITLE });
    await expect(liveWorkerRows).toHaveCount(1);
    await expect(
      liveAgentList(workspace)
        .locator('li[data-agent-role="worker"]')
        .filter({ hasText: MINIMAL_CHAT_PREPARING_TITLE }),
    ).toHaveCount(0);

    // A running execution record without live presence is still dispatch-only:
    // it renders Dispatched, never Working, and carries no fabricated timer.
    const dispatchOnlyBase = await controller.getWorkspaceSnapshot();
    expect(dispatchOnlyBase).not.toBeNull();
    const dispatchOnlySnapshot = updateChatWorkWorkerExecutionFixture(
      dispatchOnlyBase!,
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
      { worker_status: 'running', outcome: null, acceptance: null },
    );
    await controller.setWorkspaceSnapshot(dispatchOnlySnapshot);
    await triggerWorkspaceRefresh(page);
    const dispatchedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_DISPATCH_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_PREPARING_TITLE}`],
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
    );
    const dispatchedStatus = await expectWorkerFooterMetadata(dispatchedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'dispatched',
      label: 'Dispatched',
      duration: null,
    });
    await expect
      .poll(async () => /working/i.test(await dispatchedFooter.innerText()))
      .toBe(false);

    const failedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      `Worker result recorded: Failed for ${MINIMAL_CHAT_FAILED_TITLE}`,
      [`Preparing Worker execution for ${MINIMAL_CHAT_FAILED_TITLE}`],
      MINIMAL_CHAT_ALPHA_FAILED_EXECUTION_REF,
    );
    const failedStatus = await expectWorkerFooterMetadata(failedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'failed',
      label: 'Failed',
      duration: { kind: 'final', text: /^1s$/i },
    });

    const cancelledFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CANCELLED_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CANCELLED_TITLE}`],
      MINIMAL_CHAT_ALPHA_CANCELLED_EXECUTION_REF,
    );
    const cancelledStatus = await expectWorkerFooterMetadata(cancelledFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'cancelled',
      label: 'Cancelled',
      duration: { kind: 'final', text: /^10s$/i },
    });

    const unknownFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_UNKNOWN_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_UNKNOWN_TITLE}`],
      MINIMAL_CHAT_ALPHA_UNKNOWN_EXECUTION_REF,
    );
    const unknownStatus = await expectWorkerFooterMetadata(unknownFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unknown',
      label: 'Unknown',
      duration: null,
    });

    expect(
      new Set([
        dispatchStatus,
        dispatchedStatus,
        failedStatus,
        cancelledStatus,
        unknownStatus,
      ]).size,
    ).toBe(5);
    await expectNoNeedsYou(page);

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_FAILED_TITLE, 'blocked');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_CANCELLED_TITLE, 'blocked');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_UNKNOWN_TITLE, 'blocked');
    await closeBacklogWithEscape(page);
  });

  test('shows technical success as Awaiting check and only accepted guarded completion as Completed with the recorded duration and model', async ({
    page,
  }) => {
    await installWorkerMetadata(page);
    const workspace = workspaceSection(page);

    const checkingRow = await resolveStreamEventRow(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
    );
    const checkingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CHECKING_TITLE}`],
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
    );
    const checkingStatus = await expectWorkerFooterMetadata(checkingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'checking',
      label: 'Awaiting check',
      duration: { kind: 'final', text: /^55s$/i },
    });
    expect(checkingStatus).toBe('checking');

    // Awaiting check is neutral pending work, never an attention warning color.
    const checkingStatusEl = checkingFooter.locator(
      '[data-worker-execution-status="checking"]',
    );
    await expect(checkingStatusEl).toHaveCount(1);
    const checkingStatusColors = await checkingStatusEl.evaluate((element) => {
      const actualColor = getComputedStyle(element).color;
      const surfaceRoot = element.closest('.tl-cw-root') ?? document.body;
      const probe = document.createElement('span');
      probe.setAttribute('aria-hidden', 'true');
      probe.style.position = 'fixed';
      probe.style.visibility = 'hidden';
      probe.style.pointerEvents = 'none';
      probe.style.color = 'var(--cw-fg-secondary)';
      surfaceRoot.appendChild(probe);
      try {
        const neutralSecondaryColor = getComputedStyle(probe).color;
        probe.style.color = 'var(--cw-attention)';
        const attentionColor = getComputedStyle(probe).color;
        return {
          actualColor,
          neutralSecondaryColor,
          attentionColor,
        };
      } finally {
        probe.remove();
      }
    });
    expect(checkingStatusColors.neutralSecondaryColor).not.toBe('');
    expect(checkingStatusColors.neutralSecondaryColor).not.toBe(
      checkingStatusColors.attentionColor,
    );
    expect(checkingStatusColors.actualColor).toBe(
      checkingStatusColors.neutralSecondaryColor,
    );

    const acceptedRow = await resolveStreamEventRow(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
    );
    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    const completedStatus = await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });
    expect(completedStatus).toBe('completed');
    await expect(
      acceptedRow.getByText(MINIMAL_CHAT_ACCEPTED_COMPLETION_NOTE, { exact: true }),
    ).toHaveCount(1);

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_CHECKING_TITLE, 'doing');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_ACCEPTED_TITLE, 'done');
    const acceptedDrawerItem = await resolveBacklogItemRow(
      dialog,
      MINIMAL_CHAT_ACCEPTED_TITLE,
    );
    await acceptedDrawerItem.locator('summary').first().click();
    await expect(
      dialog.getByText(MINIMAL_CHAT_ACCEPTED_COMPLETION_NOTE, { exact: true }),
    ).toBeVisible();
    await closeBacklogWithEscape(page);
    await expectNoNeedsYou(page);

    // The accepted Done item stays terminal: the Awaiting check attempt is a separate
    // item and is not rendered as a Done or accepted result.
    await expect(
      checkingRow.getByText(MINIMAL_CHAT_ACCEPTED_COMPLETION_NOTE, { exact: true }),
    ).toHaveCount(0);
  });

  test('defaults old payloads to no execution summaries and omits unproven model and timing metadata', async ({
    page,
  }) => {
    const controller = await installWorkspaceTauriFixture(page, {
      workspaceSnapshot: buildMinimalChatWorkspaceSnapshotWithoutWorkerExecutions(),
    });
    await page.setViewportSize({ width: 1040, height: 760 });
    await page.goto('/');
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    const workspace = workspaceSection(page);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_READING_ANCHOR_TITLE),
    ).toBeVisible();
    await expect(workspace.locator('[data-worker-execution-summary]')).toHaveCount(0);
    // Presence, not the additive metadata array, remains the liveness proof.
    await expectLiveAgentWorking(workspace, 'Worker');
    await expectNoNeedsYou(page);

    const missingMetadataSnapshot = buildWorkerMetadataWorkspaceSnapshot();
    const withoutTelemetry = updateChatWorkWorkerExecutionFixture(
      missingMetadataSnapshot,
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
      {
        provider: null,
        requested_model: null,
        observed_model: null,
        requested_effort: null,
        observed_effort: null,
        started_at_ms: null,
        finished_at_ms: null,
        duration_ms: null,
      },
    );
    await controller.setWorkspaceSnapshot(withoutTelemetry);
    await triggerWorkspaceRefresh(page);
    await selectChat(page, 'Workspace UI Contract');

    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'generic',
      model: null,
      effort: null,
      status: 'completed',
      label: 'Completed',
      duration: null,
    });
    await expect
      .poll(async () => /argon/i.test(await acceptedFooter.innerText()))
      .toBe(false);
    await expect
      .poll(async () =>
        /\b45(?:\.0)?s\b/.test(
          (await acceptedFooter.innerText()).replace(/\s+/g, ' '),
        ),
      )
      .toBe(false);
    await expectNoNeedsYou(page);
  });

  test('isolates exact attempts and chats, and deduplicates repeated snapshots, events, and execution records', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const workspace = workspaceSection(page);

    const executions = getChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
    );
    expect(executions.length).toBeGreaterThan(0);
    for (const execution of executions) {
      expect(execution.session_binding_id).not.toBe(
        CHAT_ALPHA_IDENTITY.binding_id,
      );
      expect(execution.session_binding_id.trim().length).toBeGreaterThan(0);
      expect(execution.item_id.trim().length).toBeGreaterThan(0);
      expect(execution.run_id.trim().length).toBeGreaterThan(0);
      expect(execution.worker_id.trim().length).toBeGreaterThan(0);
    }
    expect(
      new Set(executions.map((execution) => execution.session_binding_id)).size,
    ).toBe(executions.length);
    const betaExecutions = getChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_BETA_SAME_PROJECT_IDENTITY,
    );
    expect(betaExecutions).toHaveLength(1);
    expect(betaExecutions[0]!.execution_ref).toBe(
      MINIMAL_CHAT_BETA_WORKER_EXECUTION_REF,
    );
    expect(betaExecutions[0]!.session_binding_id).not.toBe(
      CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
    );
    const alphaSharedRefExecution = executions.find(
      (execution) =>
        execution.execution_ref === MINIMAL_CHAT_BETA_WORKER_EXECUTION_REF,
    );
    expect(alphaSharedRefExecution).toBeDefined();
    expect(betaExecutions[0]!.session_binding_id).not.toBe(
      alphaSharedRefExecution!.session_binding_id,
    );
    expect(betaExecutions[0]!.item_id).not.toBe(
      alphaSharedRefExecution!.item_id,
    );
    expect(betaExecutions[0]!.run_id).not.toBe(alphaSharedRefExecution!.run_id);
    expect(betaExecutions[0]!.worker_id).not.toBe(
      alphaSharedRefExecution!.worker_id,
    );

    const previousFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CANCELLED_PREVIOUS_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CANCELLED_TITLE}`],
      MINIMAL_CHAT_ALPHA_CANCELLED_PREVIOUS_EXECUTION_REF,
    );
    const previousStatus = await expectWorkerFooterMetadata(previousFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'failed',
      label: 'Failed',
      duration: { kind: 'final', text: /^5s$/i },
    });

    const currentFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CANCELLED_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CANCELLED_TITLE}`],
      MINIMAL_CHAT_ALPHA_CANCELLED_EXECUTION_REF,
    );
    const currentStatus = await expectWorkerFooterMetadata(currentFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'cancelled',
      label: 'Cancelled',
      duration: { kind: 'final', text: /^10s$/i },
    });
    expect(previousStatus).not.toBe(currentStatus);

    // The two cancellation attempts share the same preparation title. Every
    // earlier row for the current execution must stay footer-free.
    const sharedPreparationRows = resolveAllStreamEventRows(
      page,
      `Preparing Worker execution for ${MINIMAL_CHAT_CANCELLED_TITLE}`,
    );
    await expect(sharedPreparationRows).toHaveCount(2);
    for (let index = 0; index < 2; index += 1) {
      await expectNoWorkerExecutionFooter(
        sharedPreparationRows.nth(index),
        MINIMAL_CHAT_ALPHA_CANCELLED_EXECUTION_REF,
        'shared cancellation preparation title',
      );
    }

    const acceptedEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
    ];
    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });

    // The sibling chat reuses the same execution reference with different
    // runtime metadata. The selected Alpha footer must stay Alpha-scoped.
    await expect(workspace.getByText(MINIMAL_CHAT_BETA_WORKER_TITLE)).toHaveCount(0);
    await expect(
      workspace.getByText(MINIMAL_CHAT_BETA_WORKER_COMPLETED_EVENT_TITLE),
    ).toHaveCount(0);

    await controller.setWorkspaceSnapshot(baseSnapshot!);
    await triggerWorkspaceRefresh(page);
    await controller.setWorkspaceSnapshot(baseSnapshot!);
    await triggerWorkspaceRefresh(page);
    await expect(
      workspace.getByText(MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE, { exact: true }),
    ).toHaveCount(1);
    const stableAcceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterStatus(stableAcceptedFooter, 'completed', 'Completed');

    const alphaWork = getChatWorkFixture(baseSnapshot!, CHAT_ALPHA_IDENTITY);
    const acceptedEvent = alphaWork.events.find(
      (event) => event.title === MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
    );
    expect(acceptedEvent).toBeDefined();
    const duplicateEventSnapshot = appendChatWorkEventFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      {
        ...acceptedEvent!,
        note: 'Duplicate event ID payload must not add a second footer.',
      },
    );
    await controller.setWorkspaceSnapshot(duplicateEventSnapshot);
    await triggerWorkspaceRefresh(page);
    await expect(
      workspace.getByText(MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE, { exact: true }),
    ).toHaveCount(1);
    const duplicateEventFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterStatus(duplicateEventFooter, 'completed', 'Completed');

    const acceptedExecution = executions.find(
      (execution) =>
        execution.execution_ref === MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    expect(acceptedExecution).toBeDefined();
    const duplicateExecutionSnapshot = updateChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      [...executions, acceptedExecution!],
    );
    await controller.setWorkspaceSnapshot(duplicateExecutionSnapshot);
    await triggerWorkspaceRefresh(page);
    const duplicateExecutionFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterStatus(
      duplicateExecutionFooter,
      'completed',
      'Completed',
    );

    await selectChat(page, 'Release Verification Gate');
    const betaWorkspace = workspaceSection(page);
    const betaFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_BETA_WORKER_COMPLETED_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_BETA_WORKER_TITLE}`,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_BETA_WORKER_TITLE}`,
      ],
      MINIMAL_CHAT_BETA_WORKER_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(betaFooter, {
      brand: 'openai',
      model: /^GPT-5\.4$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^12s$/i },
    });
    await expect(betaWorkspace.getByText(MINIMAL_CHAT_ACCEPTED_TITLE)).toHaveCount(0);
    await expect
      .poll(async () =>
        /\b45(?:\.0)?s\b/.test(
          (await betaFooter.innerText()).replace(/\s+/g, ' '),
        ),
      )
      .toBe(false);
  });

  test('quarantines conflicting Worker execution identities instead of transferring Completed metadata', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const workspace = workspaceSection(page);
    const acceptedEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
    ];

    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });

    const executions = getChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
    );
    const acceptedExecution = executions.find(
      (execution) =>
        execution.execution_ref === MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    expect(acceptedExecution).toBeDefined();
    const conflictingExecution = createWorkerExecution({
      identity: CHAT_ALPHA_IDENTITY,
      itemId: acceptedExecution!.item_id,
      executionRef: acceptedExecution!.execution_ref,
      approvalId: 'apr_conflicting_worker_identity',
      taskId: acceptedExecution!.task_id,
      sessionBindingId: acceptedExecution!.session_binding_id,
      runId: acceptedExecution!.run_id,
      workerId: acceptedExecution!.worker_id,
      provider: 'google',
      requestedModel: 'gemini-4-argon-high',
      observedModel: 'gemini-4-argon-high',
      requestedEffort: 'high',
      observedEffort: 'high',
      workerStatus: 'completed',
      startedAtMs: FIXTURE_BASE_NOW_MS - 70_000,
      finishedAtMs: FIXTURE_BASE_NOW_MS - 20_000,
      durationMs: 50_000,
      outcome: 'success',
      acceptance: 'accepted',
    });
    const conflictSnapshot = updateChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      [...executions, conflictingExecution],
    );
    await controller.setWorkspaceSnapshot(conflictSnapshot);
    await triggerWorkspaceRefresh(page);

    // The same item/execution ref now carries contradicting immutable approval
    // identity, so the whole group is quarantined: no Completed label, model,
    // effort, or final duration may survive on the latest event row.
    await expect(
      workspace.locator(
        `[data-worker-execution-summary="${MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF}"]`,
      ),
    ).toHaveCount(0);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE),
    ).toBeVisible();

    // Unrelated exact executions remain rendered; the rejection is scoped to the
    // conflicting identity group only.
    const checkingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CHECKING_TITLE}`],
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(checkingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'checking',
      label: 'Awaiting check',
      duration: { kind: 'final', text: /^55s$/i },
    });
    await expectNoNeedsYou(page);

    const dialog = await openBacklog(page);
    await expect(
      dialog.getByText('2 worker execution records are outside this bounded window.', {
        exact: true,
      }),
    ).toBeVisible();
    await closeBacklogWithEscape(page);
  });

  test('quarantines a Worker identity conflict split across duplicate chat_work entries', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const workspace = workspaceSection(page);
    const acceptedEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
    ];
    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });

    const executions = getChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
    );
    const acceptedExecution = executions.find(
      (execution) =>
        execution.execution_ref === MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    expect(acceptedExecution).toBeDefined();
    const conflictingExecution = createWorkerExecution({
      identity: CHAT_ALPHA_IDENTITY,
      itemId: acceptedExecution!.item_id,
      executionRef: acceptedExecution!.execution_ref,
      approvalId: 'apr_cross_entry_conflict',
      taskId: acceptedExecution!.task_id,
      sessionBindingId: acceptedExecution!.session_binding_id,
      runId: acceptedExecution!.run_id,
      workerId: acceptedExecution!.worker_id,
      provider: 'google',
      requestedModel: 'gemini-4-argon-high',
      observedModel: 'gemini-4-argon-high',
      requestedEffort: 'high',
      observedEffort: 'high',
      workerStatus: 'completed',
      startedAtMs: FIXTURE_BASE_NOW_MS - 70_000,
      finishedAtMs: FIXTURE_BASE_NOW_MS - 20_000,
      durationMs: 50_000,
      outcome: 'success',
      acceptance: 'accepted',
    });

    const work = getChatWorkFixture(baseSnapshot!, CHAT_ALPHA_IDENTITY);
    const crossEntrySnapshot = parseWorkspaceSnapshot({
      ...baseSnapshot!,
      chat_work: [
        ...baseSnapshot!.chat_work.filter(
          (entry) => !workspaceChatIdentitiesEqual(entry.identity, CHAT_ALPHA_IDENTITY),
        ),
        { ...work },
        {
          ...work,
          items: [],
          events: [],
          worker_presence: [],
          worker_executions: [acceptedExecution!, conflictingExecution],
          bounds: { ...work.bounds, executions_omitted: 0 },
        },
      ],
    });
    await controller.setWorkspaceSnapshot(crossEntrySnapshot);
    await triggerWorkspaceRefresh(page);

    // The first entry is valid on its own; the contradicting identity arrives in
    // a later entry with the same exact canonical chat identity. The composed
    // chat must quarantine all three conflicting rows, never keep Completed.
    await expect(
      workspace.locator(
        `[data-worker-execution-summary="${MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF}"]`,
      ),
    ).toHaveCount(0);
    await expect(
      resolveStreamEventRow(page, MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE),
    ).toBeVisible();
    const checkingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_CHECKING_TITLE}`],
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(checkingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'checking',
      label: 'Awaiting check',
      duration: { kind: 'final', text: /^55s$/i },
    });
    await expectNoNeedsYou(page);

    const dialog = await openBacklog(page);
    await expect(
      dialog.getByText('3 worker execution records are outside this bounded window.', {
        exact: true,
      }),
    ).toBeVisible();
    await closeBacklogWithEscape(page);
  });

  test('preserves the final Completed status, model, and fixed duration across a browser reload', async ({
    page,
  }) => {
    await installWorkerMetadata(page);
    const acceptedEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
    ];
    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    const beforeStatus = await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });

    await page.reload();
    await expect(page.locator('#app .tl-app-shell, #app .tl-app').first()).toBeVisible();
    await selectChat(page, 'Workspace UI Contract');
    await expect(chatHeader(page, 'Workspace UI Contract')).toBeVisible();

    const reloadedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    const reloadedStatus = await expectWorkerFooterMetadata(reloadedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });
    expect(reloadedStatus).toBe(beforeStatus);

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_ACCEPTED_TITLE, 'done');
    await closeBacklogWithEscape(page);
  });

  test('applies partial cancellation to one exact execution without changing another completed execution', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();

    const acceptedEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
    ];
    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      acceptedEarlierTitles,
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    const acceptedStatus = await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });

    let cancelledSnapshot = updateChatWorkWorkerExecutionFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
      { worker_status: 'cancel_requested', acceptance: null },
    );
    await controller.setWorkspaceSnapshot(cancelledSnapshot);
    await triggerWorkspaceRefresh(page);

    const checkingEarlierTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_CHECKING_TITLE}`,
    ];
    const checkingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
      checkingEarlierTitles,
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(checkingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'cancelling',
      label: 'Cancelling',
    });

    cancelledSnapshot = updateChatWorkWorkerExecutionFixture(
      cancelledSnapshot,
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
      {
        worker_status: 'cancelled',
        outcome: null,
        finished_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
        duration_ms: 57_000,
      },
    );
    const currentWork = getChatWorkFixture(cancelledSnapshot, CHAT_ALPHA_IDENTITY);
    cancelledSnapshot = updateChatWorkFixture(
      cancelledSnapshot,
      CHAT_ALPHA_IDENTITY,
      {
        items: currentWork.items.map((item) =>
          item.item_id === MINIMAL_CHAT_ALPHA_CHECKING_ITEM_ID
            ? {
                ...item,
                status: 'blocked',
                blocked_reason:
                  'Cancellation verified without creating a human request.',
              }
            : item,
        ),
      },
    );
    await controller.setWorkspaceSnapshot(cancelledSnapshot);
    await triggerWorkspaceRefresh(page);

    const cancelledCheckingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_CHECKING_EVENT_TITLE,
      checkingEarlierTitles,
      MINIMAL_CHAT_ALPHA_CHECKING_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(cancelledCheckingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'cancelled',
      label: 'Cancelled',
      duration: { kind: 'final', text: /^57s$/i },
    });
    const retainedAcceptedStatus = await expectWorkerFooterMetadata(
      acceptedFooter,
      {
        brand: 'gemini',
        model: /^Argon$/i,
        effort: /^High$/i,
        status: 'completed',
        label: 'Completed',
        duration: { kind: 'final', text: /^45s$/i },
      },
    );
    expect(retainedAcceptedStatus).toBe(acceptedStatus);
    await expectNoNeedsYou(page);

    const dialog = await openBacklog(page);
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_CHECKING_TITLE, 'blocked');
    await expectCommittedBacklogStatus(dialog, MINIMAL_CHAT_ACCEPTED_TITLE, 'done');
    await closeBacklogWithEscape(page);
  });

  test('keeps the compact footer readable without horizontal overflow at 1440px and 390px', async ({
    page,
  }) => {
    await installWorkerMetadata(page, { width: 1440, height: 900 });
    const workspace = workspaceSection(page);

    const acceptedFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_ACCEPTED_COMPLETED_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
        `Worker result recorded: Awaiting check for ${MINIMAL_CHAT_ACCEPTED_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_ACCEPTED_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(acceptedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^45s$/i },
    });
    await acceptedFooter.scrollIntoViewIfNeeded();
    await expect(acceptedFooter).toBeVisible();
    const wideBox = await acceptedFooter.boundingBox();
    expect(wideBox).not.toBeNull();
    expect(wideBox!.width).toBeGreaterThan(80);
    expect(wideBox!.height).toBeGreaterThanOrEqual(12);
    expect(
      await acceptedFooter.evaluate(
        (element) => element.scrollWidth <= element.clientWidth + 1,
      ),
    ).toBe(true);
    await expectNoHorizontalOverflow(page, '1440px Worker metadata footer');

    await page.setViewportSize({ width: 390, height: 740 });
    await expect(workspace).toBeVisible();
    await acceptedFooter.scrollIntoViewIfNeeded();
    await expect(acceptedFooter).toBeVisible();
    const narrowBox = await acceptedFooter.boundingBox();
    expect(narrowBox).not.toBeNull();
    expect(narrowBox!.width).toBeGreaterThan(80);
    expect(narrowBox!.height).toBeGreaterThanOrEqual(12);
    expect(
      await acceptedFooter.evaluate(
        (element) => element.scrollWidth <= element.clientWidth + 1,
      ),
    ).toBe(true);
    await expectNoHorizontalOverflow(page, '390px Worker metadata footer');

    // The exact current execution still has a fresh presence sample at the
    // fixed fixture clock, so the compact footer renders confirmed live Started
    // (`working` internally) with its elapsed duration at the narrow width.
    const liveFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
        `Worker execution attached for ${MINIMAL_CHAT_DOING_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
    );
    await liveFooter.scrollIntoViewIfNeeded();
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: {
        kind: 'elapsed',
        text: /^Elapsed\s+\d+(?:\.\d+)?s$/i,
      },
    });
    await expectNoHorizontalOverflow(page, '390px live Worker metadata footer');

    await page.setViewportSize({ width: 1440, height: 900 });
    await acceptedFooter.scrollIntoViewIfNeeded();
    await expect(acceptedFooter).toBeVisible();
    await expectNoHorizontalOverflow(page, 'restored 1440px Worker metadata footer');
  });

  test('reconciles regressed Worker samples, keeps stable neutral Started and Elapsed across three late 4s/3s renewals in wide and narrow viewports, expires genuinely, and preserves header/sidebar semantics and accessibility', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page, {
      width: 1440,
      height: 900,
    });
    const workspace = workspaceSection(page);
    const header = chatHeader(page, 'Workspace UI Contract');
    const alphaSidebarRow = sidebar(page)
      .locator('button.tl-chat-item')
      .filter({ hasText: 'Workspace UI Contract' })
      .first();

    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    let nativeSeq = 16;
    const withFreshAlphaNativeAt = (
      snapshot: WorkspaceSnapshot,
      generatedAtMs: number,
    ): WorkspaceSnapshot => {
      nativeSeq += 1;
      const advanced = advanceWorkspaceGeneratedAtFixture(snapshot, generatedAtMs);
      return parseWorkspaceSnapshot({
        ...advanced,
        native_samples: advanced.native_samples.map((sample) =>
          workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY)
            ? {
                ...sample,
                sequence: nativeSeq,
                observed_at_ms: generatedAtMs - 500,
                last_success_at_ms: generatedAtMs - 500,
                activity: 'idle',
              }
            : sample,
        ),
      });
    };

    const pushAndRefresh = async (nextSnapshot: WorkspaceSnapshot): Promise<void> => {
      await controller.setWorkspaceSnapshot(nextSnapshot);
      const beforeCount = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      await triggerWorkspaceRefresh(page);
      await expect
        .poll(async () =>
          (await controller.getInvocations()).filter(
            (call) => call.cmd === 'desktop_workspace_snapshot',
          ).length,
        )
        .toBeGreaterThan(beforeCount);
      await page.evaluate(
        () =>
          new Promise<void>((resolve) => {
            requestAnimationFrame(() => {
              requestAnimationFrame(() => resolve());
            });
          }),
      );
    };

    const measureFooterPresentation = async (footer: Locator) =>
      footer.evaluate((element) => {
        const statusEl = element.querySelector('[data-worker-execution-status]');
        const durationEl = element.querySelector('[data-worker-execution-duration]');
        const footerRect = element.getBoundingClientRect();
        const statusRect = statusEl?.getBoundingClientRect() ?? null;
        const durationRect = durationEl?.getBoundingClientRect() ?? null;

        const surfaceRoot = element.closest('.tl-cw-root') ?? document.body;
        const probe = document.createElement('span');
        probe.setAttribute('aria-hidden', 'true');
        probe.style.position = 'fixed';
        probe.style.visibility = 'hidden';
        probe.style.pointerEvents = 'none';
        probe.style.color = 'var(--cw-fg-secondary)';
        surfaceRoot.appendChild(probe);
        let neutralSecondaryColor = '';
        let workingAccentColor = '';
        try {
          neutralSecondaryColor = getComputedStyle(probe).color;
          probe.style.color = 'var(--cw-working)';
          workingAccentColor = getComputedStyle(probe).color;
        } finally {
          probe.remove();
        }

        const parseRgb = (raw: string): [number, number, number] => {
          const match = raw.match(/rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)/i);
          return match
            ? [Number(match[1]), Number(match[2]), Number(match[3])]
            : [0, 0, 0];
        };
        const lin = (c: number): number => {
          const v = Math.max(0, Math.min(255, c)) / 255;
          return v <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
        };
        const lum = ([r, g, b]: [number, number, number]): number =>
          0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);

        const statusColor = statusEl ? getComputedStyle(statusEl).color : '';
        const durationColor = durationEl ? getComputedStyle(durationEl).color : '';
        let statusContrast = 0;
        if (statusEl) {
          const fg = parseRgb(statusColor);
          let bg: [number, number, number] = [24, 24, 28];
          for (
            let node: Element | null = statusEl;
            node !== null;
            node = node.parentElement
          ) {
            const rawBg = getComputedStyle(node).backgroundColor;
            const alphaMatch = rawBg.match(
              /rgba\(\s*[\d.]+[,\s]+[\d.]+[,\s]+[\d.]+[,\s/]+([\d.]+)\s*\)/i,
            );
            const alpha = alphaMatch
              ? Number(alphaMatch[1])
              : rawBg === 'transparent'
                ? 0
                : 1;
            if (alpha >= 0.99) {
              bg = parseRgb(rawBg);
              break;
            }
          }
          const l1 = Math.max(lum(fg), lum(bg));
          const l2 = Math.min(lum(fg), lum(bg));
          statusContrast = (l1 + 0.05) / (l2 + 0.05);
        }

        return {
          eventKey:
            element
              .closest('[data-chat-event-key]')
              ?.getAttribute('data-chat-event-key') ?? null,
          statusKey: element.getAttribute('data-worker-status'),
          statusText: (statusEl?.textContent ?? '').replace(/\s+/g, ' ').trim(),
          statusAriaLabel: statusEl?.getAttribute('aria-label') ?? null,
          statusTitle: statusEl?.getAttribute('title') ?? null,
          statusColor,
          neutralSecondaryColor,
          workingAccentColor,
          statusContrast,
          durationKind:
            durationEl?.getAttribute('data-worker-execution-duration') ?? null,
          durationText: (durationEl?.textContent ?? '').replace(/\s+/g, ' ').trim(),
          durationAriaLabel: durationEl?.getAttribute('aria-label') ?? null,
          durationTitle: durationEl?.getAttribute('title') ?? null,
          durationColor,
          statusRelX: statusRect ? statusRect.left - footerRect.left : null,
          statusRelY: statusRect ? statusRect.top - footerRect.top : null,
          statusWidth: statusRect ? statusRect.width : null,
          durationRelX: durationRect ? durationRect.left - footerRect.left : null,
          durationRelY: durationRect ? durationRect.top - footerRect.top : null,
          durationRightInset: durationRect
            ? footerRect.right - durationRect.right
            : null,
          durationGapFromStatus:
            statusRect && durationRect
              ? durationRect.left - statusRect.right
              : null,
          overflows: element.scrollWidth > element.clientWidth + 1,
        };
      });

    type FooterPresentation = Awaited<ReturnType<typeof measureFooterPresentation>>;
    const assertStablePresentation = (
      current: FooterPresentation,
      baseline: FooterPresentation,
      layout: 'wide' | 'narrow',
      expected: {
        statusKey: 'working' | 'started';
        durationKind: 'elapsed' | 'last_confirmed';
      },
    ): void => {
      expect(current.eventKey).not.toBeNull();
      expect(current.eventKey).toBe(baseline.eventKey);
      expect(current.statusKey).toBe(expected.statusKey);
      expect(current.statusText).toBe('Started');
      expect(current.durationKind).toBe(expected.durationKind);
      expect(current.durationText).toMatch(/^Elapsed\s+\d+(?:\.\d+)?s$/i);
      expect(current.durationText).not.toMatch(/last at/i);
      expect(current.neutralSecondaryColor).not.toBe('');
      expect(current.neutralSecondaryColor).not.toBe(current.workingAccentColor);
      expect(current.statusColor).toBe(current.neutralSecondaryColor);
      expect(current.statusColor).toBe(baseline.statusColor);
      expect(current.durationColor).toBe(baseline.durationColor);
      expect(current.statusContrast).toBeGreaterThanOrEqual(4.5);
      expect(current.overflows).toBe(false);

      expect(current.statusAriaLabel).not.toBeNull();
      expect(current.statusTitle).toBe(current.statusAriaLabel);
      expect(current.durationAriaLabel).not.toBeNull();
      expect(current.durationTitle).toBe(current.durationAriaLabel);
      if (expected.statusKey === 'working') {
        expect(current.statusAriaLabel).toMatch(
          /^Started, live evidence fresh \(last confirmed (?:just now|\d+(?:\.\d+)?(?:ms|s) ago)\)$/i,
        );
        expect(current.durationAriaLabel).toMatch(
          /^Elapsed \d+(?:\.\d+)?s, live evidence fresh \(last confirmed (?:just now|\d+(?:\.\d+)?(?:ms|s) ago)\)$/i,
        );
      } else {
        expect(current.statusAriaLabel).toMatch(
          /^Started, last confirmed status \(confirmed \d+(?:\.\d+)?(?:ms|s) ago\)$/i,
        );
        expect(current.durationAriaLabel).toMatch(
          /^Last confirmed elapsed \d+(?:\.\d+)?s \(confirmed \d+(?:\.\d+)?(?:ms|s) ago\)$/i,
        );
      }

      expect(Math.abs(current.statusRelX! - baseline.statusRelX!)).toBeLessThanOrEqual(1);
      expect(Math.abs(current.statusRelY! - baseline.statusRelY!)).toBeLessThanOrEqual(1);
      expect(Math.abs(current.statusWidth! - baseline.statusWidth!)).toBeLessThanOrEqual(1);
      expect(Math.abs(current.durationRelY! - baseline.durationRelY!)).toBeLessThanOrEqual(1);
      if (layout === 'wide') {
        expect(
          Math.abs(current.durationRightInset! - baseline.durationRightInset!),
        ).toBeLessThanOrEqual(1);
      } else {
        expect(Math.abs(current.durationRelX! - baseline.durationRelX!)).toBeLessThanOrEqual(1);
        expect(
          Math.abs(current.durationGapFromStatus! - baseline.durationGapFromStatus!),
        ).toBeLessThanOrEqual(1);
      }
    };

    const earlierLiveTitles = [
      `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
      `Worker execution attached for ${MINIMAL_CHAT_DOING_TITLE}`,
    ];
    const eventRow = await resolveStreamEventRow(
      page,
      MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
    );
    const initialEventId = await eventRow.getAttribute('data-chat-event-id');
    const initialEventSequence = await eventRow.getAttribute(
      'data-chat-event-sequence',
    );
    const initialEventKey = await eventRow.getAttribute('data-chat-event-key');
    expect(initialEventId).not.toBeNull();
    expect(initialEventSequence).not.toBeNull();
    expect(initialEventKey).not.toBeNull();
    await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
      earlierLiveTitles,
      MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
    );
    const keyedRow = workspace.locator(
      `[data-chat-event-id="${initialEventId}"][data-chat-event-sequence="${initialEventSequence}"]`,
    );
    await expect(keyedRow).toHaveCount(1);
    const keyedFooter = keyedRow.locator(
      `[data-worker-execution-summary="${MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF}"]`,
    );
    await expect(keyedFooter).toHaveCount(1);
    await expect(keyedFooter).not.toHaveAttribute('role', 'status');
    await expect(keyedFooter).not.toHaveAttribute('aria-live');

    await expectWorkerFooterMetadata(keyedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: { kind: 'elapsed', text: /^Elapsed\s+14s$/i },
    });
    await expect(header.getByText(/^Working$/i).first()).toBeVisible();
    await expectLiveAgentWorking(workspace, 'Worker');
    await expect(
      alphaSidebarRow.locator('[data-sidebar-status="working"]'),
    ).toHaveText('Working');

    const wideBaseline = await measureFooterPresentation(keyedFooter);
    expect(wideBaseline.eventKey).toBe(initialEventKey);
    assertStablePresentation(wideBaseline, wideBaseline, 'wide', {
      statusKey: 'working',
      durationKind: 'elapsed',
    });

    // 1. A newer overall refresh carrying an older regressed Worker sample
    // (seq 11, already expired at +1_500ms) must NOT replace still-valid seq 12
    // (expires at +4_000ms). Row, header, and sidebar stay live without flicker.
    const regressedSnapshot = updateChatWorkPresenceFixture(
      withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 1_500),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          sequence: 11,
          sampledAtMs: FIXTURE_BASE_NOW_MS - 3_500,
          sampleAgeMs: 5_000,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 1_500,
        }),
      ],
    );
    await pushAndRefresh(regressedSnapshot);
    await expectWorkerFooterMetadata(keyedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: { kind: 'elapsed', text: /^Elapsed\s+16s$/i },
    });
    await expect(header.getByText(/^Working$/i).first()).toBeVisible();
    await expectLiveAgentWorking(workspace, 'Worker');
    await expect(
      alphaSidebarRow.locator('[data-sidebar-status="working"]'),
    ).toHaveText('Working');

    // Advance within seq 12 (+3_000ms -> 17s), enter Gap #0 (+4_500ms), and verify
    // delayed stale refresh and same-host unavailable probe never grant live
    // Working or advance frozen elapsed.
    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 3_000);
    await expect
      .poll(
        async () =>
          elapsedSecondsFromWorkerFooter(await keyedFooter.innerText()) ?? -1,
        { timeout: 5_000 },
      )
      .toBeGreaterThanOrEqual(17);
    await expectWorkerFooterStatus(keyedFooter, 'working', 'Started');

    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 4_500);
    await expect
      .poll(async () => readWorkerFooterStatus(keyedFooter), { timeout: 5_000 })
      .toBe('started');
    const frozenElapsedGap0 = elapsedSecondsFromWorkerFooter(
      await keyedFooter.innerText(),
    );
    expect(frozenElapsedGap0).not.toBeNull();
    expect(frozenElapsedGap0!).toBeGreaterThanOrEqual(17);
    expect(frozenElapsedGap0!).toBeLessThanOrEqual(18);
    assertStablePresentation(
      await measureFooterPresentation(keyedFooter),
      wideBaseline,
      'wide',
      { statusKey: 'started', durationKind: 'last_confirmed' },
    );
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectNoVisibleWorking(workspace);
    await expect(
      alphaSidebarRow.locator('[data-sidebar-status="unknown"]'),
    ).toHaveText('Status unavailable');
    await expectNoNeedsYou(page);

    const staleDuringGap0 = updateChatWorkPresenceFixture(
      withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 6_500),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          sequence: 12,
          availability: 'live',
          freshness: 'stale',
          live: false,
          sampledAtMs: FIXTURE_BASE_NOW_MS - 1_000,
          sampleAgeMs: 7_500,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 4_000,
        }),
      ],
    );
    await pushAndRefresh(staleDuringGap0);
    expect(elapsedSecondsFromWorkerFooter(await keyedFooter.innerText())).toBe(
      frozenElapsedGap0,
    );
    await expectNoVisibleWorking(workspace);

    const sameHostUnavailableGap0 = updateChatWorkPresenceFixture(
      withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 7_500),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          hostInstanceId: 'host_alpha_01',
          invocationId: null,
          claimId: null,
          sequence: null,
          availability: 'unavailable',
          freshness: 'not_applicable',
          live: false,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 7_000,
          sampleAgeMs: null,
          maxSampleAgeMs: 5_000,
          expiresAtMs: null,
        }),
      ],
    );
    await pushAndRefresh(sameHostUnavailableGap0);
    await expectWorkerFooterStatus(keyedFooter, 'started', 'Started');
    expect(elapsedSecondsFromWorkerFooter(await keyedFooter.innerText())).toBe(
      frozenElapsedGap0,
    );

    // 2. Three successive late renewals (4-second renewals with a 3-second TTL)
    // on the identical DOM record in wide (1440px) and narrow (390px) viewports.
    const runThreeLateRenewalCycles = async (
      layout: 'wide' | 'narrow',
      startOffsetMs: number,
      startSeq: number,
      baseline: FooterPresentation,
      firstCycleAlreadyLive: boolean,
    ): Promise<number> => {
      let lastFrozen = -1;
      for (let cycle = 0; cycle < 3; cycle += 1) {
        const cycleStartOffset = startOffsetMs + cycle * 4_000;
        const cycleStartMs = FIXTURE_BASE_NOW_MS + cycleStartOffset;
        const expiresAtMs = cycleStartMs + 3_000;
        const seq = startSeq + cycle;

        if (!(cycle === 0 && firstCycleAlreadyLive)) {
          await pushAndRefresh(
            updateChatWorkPresenceFixture(
              withFreshAlphaNativeAt(baseSnapshot!, cycleStartMs),
              CHAT_ALPHA_IDENTITY,
              [
                createBacklogWorkerPresence({
                  itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
                  executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
                  sequence: seq,
                  sampledAtMs: cycleStartMs,
                  sampleAgeMs: 0,
                  maxSampleAgeMs: 3_000,
                  expiresAtMs,
                }),
              ],
            ),
          );
        }

        await expect(keyedFooter).toHaveCount(1);
        const liveStartMetrics = await measureFooterPresentation(keyedFooter);
        assertStablePresentation(liveStartMetrics, baseline, layout, {
          statusKey: 'working',
          durationKind: 'elapsed',
        });
        const liveStartElapsed = elapsedSecondsFromWorkerFooter(
          liveStartMetrics.durationText,
        );
        expect(liveStartElapsed).not.toBeNull();
        expect(liveStartElapsed!).toBeGreaterThan(lastFrozen);
        await expect(header.getByText(/^Working$/i).first()).toBeVisible();
        await expectLiveAgentWorking(workspace, 'Worker');
        if (layout === 'wide') {
          await expect(
            alphaSidebarRow.locator('[data-sidebar-status="working"]'),
          ).toHaveText('Working');
        }

        // Advance within the 3s TTL (+2_000ms) and record the exact live elapsed.
        const liveTickMs = cycleStartMs + 2_000;
        if (cycle === 0) {
          await page.clock.setFixedTime(liveTickMs);
          await expect
            .poll(
              async () =>
                elapsedSecondsFromWorkerFooter(await keyedFooter.innerText()) ?? -1,
              { timeout: 5_000 },
            )
            .toBeGreaterThanOrEqual(liveStartElapsed! + 2);
        } else {
          await pushAndRefresh(
            updateChatWorkPresenceFixture(
              withFreshAlphaNativeAt(baseSnapshot!, liveTickMs),
              CHAT_ALPHA_IDENTITY,
              [
                createBacklogWorkerPresence({
                  itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
                  executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
                  sequence: seq,
                  sampledAtMs: cycleStartMs,
                  sampleAgeMs: 2_000,
                  maxSampleAgeMs: 3_000,
                  expiresAtMs,
                }),
              ],
            ),
          );
        }
        const liveBeforeGap = elapsedSecondsFromWorkerFooter(
          await keyedFooter.innerText(),
        );
        expect(liveBeforeGap).not.toBeNull();
        expect(liveBeforeGap!).toBeGreaterThanOrEqual(liveStartElapsed! + 2);

        // Cross the 3s TTL (+3_400ms): cycle 0 exercises pure wall-clock expiry,
        // cycle 1 exercises an omitted host probe, cycle 2 exercises a stale refresh.
        const gapMs = cycleStartMs + 3_400;
        if (cycle === 0) {
          await page.clock.setFixedTime(gapMs);
          await expect
            .poll(async () => readWorkerFooterStatus(keyedFooter), {
              timeout: 5_000,
            })
            .toBe('started');
        } else if (cycle === 1) {
          await pushAndRefresh(
            updateChatWorkPresenceFixture(
              withFreshAlphaNativeAt(baseSnapshot!, gapMs),
              CHAT_ALPHA_IDENTITY,
              [],
            ),
          );
        } else {
          await pushAndRefresh(
            updateChatWorkPresenceFixture(
              withFreshAlphaNativeAt(baseSnapshot!, gapMs),
              CHAT_ALPHA_IDENTITY,
              [
                createBacklogWorkerPresence({
                  itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
                  executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
                  sequence: seq,
                  availability: 'live',
                  freshness: 'stale',
                  live: false,
                  sampledAtMs: cycleStartMs,
                  sampleAgeMs: 3_400,
                  maxSampleAgeMs: 3_000,
                  expiresAtMs,
                }),
              ],
            ),
          );
        }

        await expect(keyedFooter).toHaveCount(1);
        const gapMetrics = await measureFooterPresentation(keyedFooter);
        assertStablePresentation(gapMetrics, baseline, layout, {
          statusKey: 'started',
          durationKind: 'last_confirmed',
        });
        const frozenElapsed = elapsedSecondsFromWorkerFooter(gapMetrics.durationText);
        expect(frozenElapsed).toBe(liveBeforeGap);
        await expectNoLiveAgentWorking(workspace, 'Worker');
        await expectNoVisibleWorking(workspace);
        await expectNoNeedsYou(page);
        if (layout === 'wide') {
          await expect(
            alphaSidebarRow.locator('[data-sidebar-status="unknown"]'),
          ).toHaveText('Status unavailable');
        } else {
          await expectNoHorizontalOverflow(page, `390px gap cycle ${cycle + 1}`);
        }

        // Advance later inside the same brief gap (+3_800ms): elapsed remains frozen.
        await pushAndRefresh(
          updateChatWorkPresenceFixture(
            withFreshAlphaNativeAt(baseSnapshot!, cycleStartMs + 3_800),
            CHAT_ALPHA_IDENTITY,
            [
              createBacklogWorkerPresence({
                itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
                executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
                sequence: seq,
                availability: 'live',
                freshness: 'stale',
                live: false,
                sampledAtMs: cycleStartMs,
                sampleAgeMs: 3_800,
                maxSampleAgeMs: 3_000,
                expiresAtMs,
              }),
            ],
          ),
        );
        expect(elapsedSecondsFromWorkerFooter(await keyedFooter.innerText())).toBe(
          frozenElapsed,
        );
        lastFrozen = frozenElapsed!;
      }

      // Live recovery at the 4th renewal (+12_000ms from startOffsetMs).
      const recoveryMs = FIXTURE_BASE_NOW_MS + startOffsetMs + 12_000;
      const recoveryExpiresAtMs = recoveryMs + 3_000;
      await pushAndRefresh(
        updateChatWorkPresenceFixture(
          withFreshAlphaNativeAt(baseSnapshot!, recoveryMs),
          CHAT_ALPHA_IDENTITY,
          [
            createBacklogWorkerPresence({
              itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
              executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
              sequence: startSeq + 3,
              sampledAtMs: recoveryMs,
              sampleAgeMs: 0,
              maxSampleAgeMs: 3_000,
              expiresAtMs: recoveryExpiresAtMs,
            }),
          ],
        ),
      );
      const recoveredMetrics = await measureFooterPresentation(keyedFooter);
      assertStablePresentation(recoveredMetrics, baseline, layout, {
        statusKey: 'working',
        durationKind: 'elapsed',
      });
      const recoveredElapsed = elapsedSecondsFromWorkerFooter(
        recoveredMetrics.durationText,
      );
      expect(recoveredElapsed).not.toBeNull();
      expect(recoveredElapsed!).toBeGreaterThan(lastFrozen);
      await expect(header.getByText(/^Working$/i).first()).toBeVisible();
      await expectLiveAgentWorking(workspace, 'Worker');
      return recoveryExpiresAtMs;
    };

    await runThreeLateRenewalCycles('wide', 8_000, 13, wideBaseline, false);

    // 3. Narrow 390px: verify three successive late renewals on the exact same
    // DOM record keep stable caption, elapsed prefix, neutral color, and fixed
    // timer placement without horizontal overflow.
    await page.setViewportSize({ width: 390, height: 740 });
    await keyedFooter.scrollIntoViewIfNeeded();
    await expect(keyedFooter).toBeVisible();
    await expectNoHorizontalOverflow(page, '390px live Started Worker footer');
    const narrowBaseline = await measureFooterPresentation(keyedFooter);
    expect(narrowBaseline.eventKey).toBe(initialEventKey);
    assertStablePresentation(narrowBaseline, narrowBaseline, 'narrow', {
      statusKey: 'working',
      durationKind: 'elapsed',
    });
    const lastExpiresAtMs = await runThreeLateRenewalCycles(
      'narrow',
      20_000,
      16,
      narrowBaseline,
      true,
    );

    // 4. Real expiration past expires_at_ms + brief gap: after worker_presence
    // becomes empty, genuine sustained loss becomes explicit Status unavailable
    // with duration removed instead of reverting to Dispatched.
    await page.setViewportSize({ width: 1440, height: 900 });
    await pushAndRefresh(
      updateChatWorkPresenceFixture(
        withFreshAlphaNativeAt(baseSnapshot!, lastExpiresAtMs + 100),
        CHAT_ALPHA_IDENTITY,
        [],
      ),
    );
    await expectWorkerFooterStatus(keyedFooter, 'started', 'Started');
    await page.clock.setFixedTime(
      lastExpiresAtMs + CHAT_WORK_EXECUTION_BRIEF_GAP_MS + 500,
    );
    await expect
      .poll(async () => readWorkerFooterStatus(keyedFooter), { timeout: 5_000 })
      .toBe('unavailable');
    await expectWorkerFooterMetadata(keyedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });
    await pushAndRefresh(
      updateChatWorkPresenceFixture(
        withFreshAlphaNativeAt(
          baseSnapshot!,
          lastExpiresAtMs + CHAT_WORK_EXECUTION_BRIEF_GAP_MS + 1_500,
        ),
        CHAT_ALPHA_IDENTITY,
        [],
      ),
    );
    await expectWorkerFooterMetadata(keyedFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });
    await expectNoVisibleWorking(workspace);
    await expectNoLiveAgentWorking(workspace, 'Worker');
    await expectNoNeedsYou(page);
  });

  test('never carries continuity across restarted hosts or foreign runs, and lets terminal failure and success immediately win', async ({
    page,
  }) => {
    const controller = await installWorkerMetadata(page, {
      width: 1280,
      height: 840,
    });
    const workspace = workspaceSection(page);
    const baseSnapshot = await controller.getWorkspaceSnapshot();
    expect(baseSnapshot).not.toBeNull();
    const preparingExecution = getChatWorkWorkerExecutionsFixture(
      baseSnapshot!,
      CHAT_ALPHA_IDENTITY,
    ).find(
      (execution) =>
        execution.execution_ref === MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
    );
    expect(preparingExecution).toBeDefined();
    const preparingItemId = preparingExecution!.item_id;

    let nativeSeq = 16;
    const withFreshAlphaNativeAt = (
      snapshot: WorkspaceSnapshot,
      generatedAtMs: number,
    ): WorkspaceSnapshot => {
      nativeSeq += 1;
      const advanced = advanceWorkspaceGeneratedAtFixture(snapshot, generatedAtMs);
      return parseWorkspaceSnapshot({
        ...advanced,
        native_samples: advanced.native_samples.map((sample) =>
          workspaceChatIdentitiesEqual(sample.chat_identity, CHAT_ALPHA_IDENTITY)
            ? {
                ...sample,
                sequence: nativeSeq,
                observed_at_ms: generatedAtMs - 500,
                last_success_at_ms: generatedAtMs - 500,
                activity: 'idle',
              }
            : sample,
        ),
      });
    };

    const pushAndRefresh = async (nextSnapshot: WorkspaceSnapshot): Promise<void> => {
      await controller.setWorkspaceSnapshot(nextSnapshot);
      const beforeCount = (await controller.getInvocations()).filter(
        (call) => call.cmd === 'desktop_workspace_snapshot',
      ).length;
      await triggerWorkspaceRefresh(page);
      await expect
        .poll(async () =>
          (await controller.getInvocations()).filter(
            (call) => call.cmd === 'desktop_workspace_snapshot',
          ).length,
        )
        .toBeGreaterThan(beforeCount);
      await page.evaluate(
        () =>
          new Promise<void>((resolve) => {
            requestAnimationFrame(() => {
              requestAnimationFrame(() => resolve());
            });
          }),
      );
    };

    const liveFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_LIVE_PROGRESS_EVENT_TITLE,
      [
        `Preparing Worker execution for ${MINIMAL_CHAT_DOING_TITLE}`,
        `Worker execution attached for ${MINIMAL_CHAT_DOING_TITLE}`,
      ],
      MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
    );
    await expectWorkerFooterStatus(liveFooter, 'working', 'Started');

    // Enter brief gap at +4_500ms -> Started with frozen elapsed.
    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 4_500);
    await expect
      .poll(async () => readWorkerFooterStatus(liveFooter), { timeout: 5_000 })
      .toBe('started');
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'started',
      label: 'Started',
      duration: { kind: 'last_confirmed', text: /^Elapsed\s+14s$/i },
    });

    // 1. Restarted host (new host_instance_id / invocation_id) immediately clears
    // Started instead of carrying prior host state.
    const restartedHostSnapshot = updateChatWorkPresenceFixture(
      withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 5_000),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          hostInstanceId: 'host_alpha_restarted_02',
          invocationId: 'inv_alpha_worker_02',
          claimId: 'clm_alpha_worker_02',
          sequence: 1,
          availability: 'unavailable',
          freshness: 'not_applicable',
          live: false,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 4_800,
          sampleAgeMs: null,
          maxSampleAgeMs: 5_000,
          expiresAtMs: null,
        }),
      ],
    );
    await pushAndRefresh(restartedHostSnapshot);
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });

    // 2. Re-seed live on host_alpha_restarted_02, enter brief gap, then switch
    // to a foreign run (different run_id/worker_id) with stale presence: foreign
    // run never inherits Started or frozen elapsed.
    const restartedLiveSnapshot = updateChatWorkPresenceFixture(
      withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 6_000),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          hostInstanceId: 'host_alpha_restarted_02',
          invocationId: 'inv_alpha_worker_02',
          claimId: 'clm_alpha_worker_02',
          sequence: 2,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 5_500,
          sampleAgeMs: 500,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 10_500,
        }),
      ],
    );
    await pushAndRefresh(restartedLiveSnapshot);
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: { kind: 'elapsed', text: /^Elapsed\s+20s$/i },
    });

    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 11_000);
    await expect
      .poll(async () => readWorkerFooterStatus(liveFooter), { timeout: 5_000 })
      .toBe('started');

    const foreignRunSnapshot = updateChatWorkPresenceFixture(
      updateChatWorkWorkerExecutionFixture(
        withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 11_500),
        CHAT_ALPHA_IDENTITY,
        MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
        {
          run_id: 'run_alpha_foreign_02',
          worker_id: 'wrk_alpha_foreign_02',
        },
      ),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          runId: 'run_alpha_foreign_02',
          workerId: 'wrk_alpha_foreign_02',
          hostInstanceId: 'host_alpha_restarted_02',
          invocationId: 'inv_alpha_worker_02',
          claimId: 'clm_alpha_worker_02',
          sequence: 2,
          availability: 'live',
          freshness: 'stale',
          live: false,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 5_500,
          sampleAgeMs: 6_000,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 10_500,
        }),
      ],
    );
    await pushAndRefresh(foreignRunSnapshot);
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'unavailable',
      label: 'Status unavailable',
      duration: null,
    });

    // 3. Terminal FAILURE immediately wins during a brief gap.
    const foreignLiveSnapshot = updateChatWorkPresenceFixture(
      updateChatWorkWorkerExecutionFixture(
        withFreshAlphaNativeAt(baseSnapshot!, FIXTURE_BASE_NOW_MS + 12_500),
        CHAT_ALPHA_IDENTITY,
        MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
        {
          run_id: 'run_alpha_foreign_02',
          worker_id: 'wrk_alpha_foreign_02',
        },
      ),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: MINIMAL_CHAT_ALPHA_DOING_ITEM_ID,
          executionRef: MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
          runId: 'run_alpha_foreign_02',
          workerId: 'wrk_alpha_foreign_02',
          hostInstanceId: 'host_alpha_restarted_02',
          invocationId: 'inv_alpha_worker_02',
          claimId: 'clm_alpha_worker_02',
          sequence: 3,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 12_000,
          sampleAgeMs: 500,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 17_000,
        }),
      ],
    );
    await pushAndRefresh(foreignLiveSnapshot);
    await expectWorkerFooterStatus(liveFooter, 'working', 'Started');

    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 17_500);
    await expect
      .poll(async () => readWorkerFooterStatus(liveFooter), { timeout: 5_000 })
      .toBe('started');

    const failedDuringGapBase = updateChatWorkWorkerExecutionFixture(
      withFreshAlphaNativeAt(foreignLiveSnapshot, FIXTURE_BASE_NOW_MS + 17_800),
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_LIVE_EXECUTION_REF,
      {
        worker_status: 'failed',
        outcome: 'failure',
        finished_at_ms: FIXTURE_BASE_NOW_MS + 17_800,
        duration_ms: 31_800,
      },
    );
    const failedWork = getChatWorkFixture(failedDuringGapBase, CHAT_ALPHA_IDENTITY);
    const failedDuringGapSnapshot = updateChatWorkFixture(
      failedDuringGapBase,
      CHAT_ALPHA_IDENTITY,
      {
        items: failedWork.items.map((item) =>
          item.item_id === MINIMAL_CHAT_ALPHA_DOING_ITEM_ID
            ? {
                ...item,
                status: 'blocked',
                blocked_reason: 'Worker failed during brief sampling gap.',
              }
            : item,
        ),
      },
    );
    await pushAndRefresh(failedDuringGapSnapshot);
    await expectWorkerFooterMetadata(liveFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'failed',
      label: 'Failed',
      duration: { kind: 'final', text: /^32s$/i },
    });
    await expectNoNeedsYou(page);

    // 4. Terminal SUCCESS (Awaiting check and guarded Completed) immediately
    // wins during a brief gap on an independent Worker attempt.
    const preparingLiveSnapshot = updateChatWorkPresenceFixture(
      updateChatWorkWorkerExecutionFixture(
        withFreshAlphaNativeAt(
          failedDuringGapSnapshot,
          FIXTURE_BASE_NOW_MS + 18_500,
        ),
        CHAT_ALPHA_IDENTITY,
        MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
        {
          worker_status: 'running',
          outcome: null,
          acceptance: null,
          started_at_ms: FIXTURE_BASE_NOW_MS + 10_000,
        },
      ),
      CHAT_ALPHA_IDENTITY,
      [
        createBacklogWorkerPresence({
          itemId: preparingItemId,
          executionRef: MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
          approvalId: preparingExecution!.approval_id,
          taskId: preparingExecution!.task_id,
          runId: preparingExecution!.run_id,
          workerId: preparingExecution!.worker_id,
          claimId: 'clm_alpha_preparing_01',
          hostInstanceId: 'host_alpha_preparing_01',
          invocationId: 'inv_alpha_preparing_01',
          sequence: 5,
          sampledAtMs: FIXTURE_BASE_NOW_MS + 18_000,
          sampleAgeMs: 500,
          maxSampleAgeMs: 5_000,
          expiresAtMs: FIXTURE_BASE_NOW_MS + 23_000,
        }),
      ],
    );
    await pushAndRefresh(preparingLiveSnapshot);
    const preparingFooter = await expectWorkerFooterAtLatestEvent(
      page,
      MINIMAL_CHAT_DISPATCH_EVENT_TITLE,
      [`Preparing Worker execution for ${MINIMAL_CHAT_PREPARING_TITLE}`],
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
    );
    await expectWorkerFooterMetadata(preparingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'working',
      label: 'Started',
      duration: { kind: 'elapsed', text: /^Elapsed\s+8\.5s$/i },
    });

    await page.clock.setFixedTime(FIXTURE_BASE_NOW_MS + 23_500);
    await expect
      .poll(async () => readWorkerFooterStatus(preparingFooter), {
        timeout: 5_000,
      })
      .toBe('started');
    await expectWorkerFooterMetadata(preparingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'started',
      label: 'Started',
      duration: { kind: 'last_confirmed', text: /^Elapsed\s+8\.5s$/i },
    });

    const awaitingCheckSnapshot = updateChatWorkWorkerExecutionFixture(
      withFreshAlphaNativeAt(
        preparingLiveSnapshot,
        FIXTURE_BASE_NOW_MS + 24_200,
      ),
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
      {
        worker_status: 'completed',
        outcome: 'success',
        acceptance: 'pending_review',
        finished_at_ms: FIXTURE_BASE_NOW_MS + 24_000,
        duration_ms: 14_000,
      },
    );
    await pushAndRefresh(awaitingCheckSnapshot);
    await expectWorkerFooterMetadata(preparingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'checking',
      label: 'Awaiting check',
      duration: { kind: 'final', text: /^14s$/i },
    });

    const completedBase = updateChatWorkWorkerExecutionFixture(
      withFreshAlphaNativeAt(
        awaitingCheckSnapshot,
        FIXTURE_BASE_NOW_MS + 24_600,
      ),
      CHAT_ALPHA_IDENTITY,
      MINIMAL_CHAT_ALPHA_PREPARING_EXECUTION_REF,
      {
        acceptance: 'accepted',
      },
    );
    const completedWork = getChatWorkFixture(completedBase, CHAT_ALPHA_IDENTITY);
    const completedSnapshot = updateChatWorkFixture(
      completedBase,
      CHAT_ALPHA_IDENTITY,
      {
        items: completedWork.items.map((item) =>
          item.item_id === preparingItemId
            ? {
                ...item,
                status: 'done',
                completed_at_ms: FIXTURE_BASE_NOW_MS + 24_500,
              }
            : item,
        ),
      },
    );
    await pushAndRefresh(completedSnapshot);
    await expectWorkerFooterMetadata(preparingFooter, {
      brand: 'gemini',
      model: /^Argon$/i,
      effort: /^High$/i,
      status: 'completed',
      label: 'Completed',
      duration: { kind: 'final', text: /^14s$/i },
    });
    await expectNoVisibleWorking(workspace);
    await expectNoNeedsYou(page);
  });

  test('reports the compiled Desktop build identity from the actual Vite build input in Settings', async ({
    page,
  }) => {
    // The identity is a static compile-time string in build-info.ts. The test
    // worker sees the same VITE_ input that the separately built preview used;
    // there is no runtime build-info command or window override to seed.
    const rawBuildInput = process.env.VITE_THREADLANE_DESKTOP_BUILD_IDENTITY;
    const buildInput = rawBuildInput?.trim() ?? '';
    const expected =
      buildInput === ''
        ? { statusLabel: 'Unknown source', displayValue: 'unknown' }
        : buildInput === 'dirty'
          ? { statusLabel: 'Dirty source', displayValue: 'dirty' }
          : /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(buildInput)
            ? {
                statusLabel: 'Clean source',
                displayValue: buildInput.slice(0, 12),
              }
            : { statusLabel: 'Unknown source', displayValue: 'unknown' };

    const controller = await installMinimal(page, {
      width: 1040,
      height: 760,
    });
    await sidebar(page)
      .getByRole('button', { name: 'Settings', exact: true })
      .click();
    const row = await openDesktopBuildRow(page);
    expect(
      (await controller.getInvocations()).filter((call) =>
        /build|identit/i.test(call.cmd),
      ),
      'Desktop build identity must come from the static compile-time value, not runtime IPC',
    ).toEqual([]);

    await expect(row.getByText('Desktop build', { exact: true })).toHaveCount(1);
    await expect(
      row.getByText(expected.statusLabel, { exact: true }),
    ).toHaveCount(1);
    await expect(
      row.getByText(expected.displayValue, { exact: true }),
    ).toHaveCount(1);

    const text = (await row.innerText()).replace(/\s+/g, ' ').trim();
    expect(text).toContain(expected.statusLabel);
    expect(text).toContain(expected.displayValue);

    if (expected.statusLabel === 'Unknown source') {
      expect(text).not.toMatch(/\b[0-9a-f]{7,64}\b/i);
      expect(text).not.toMatch(/clean source|dirty source/i);
    } else if (expected.statusLabel === 'Dirty source') {
      // A dirty source has no validated revision to display.
      expect(text).not.toMatch(/\b[0-9a-f]{7,64}\b/i);
      expect(text).not.toMatch(/clean source/i);
    } else {
      expect(text).not.toMatch(/\bdirty\b/i);
      // Only the 12-character display revision is visible, not the full input.
      expect(text).toContain(expected.displayValue);
      expect(text).not.toContain(buildInput);
    }
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

for (const width of [390, 1040]) {
  test(`decision badges follow the correct task through concurrent responses at ${width}px`, async ({ page }) => {
    const base = buildWorkerMetadataWorkspaceSnapshot();
    const work = getChatWorkFixture(base, CHAT_ALPHA_IDENTITY);
    const jev = MINIMAL_CHAT_ALPHA_DOING_ITEM_ID;
    const openai = MINIMAL_CHAT_ALPHA_CHECKING_ITEM_ID;
    const skipped = MINIMAL_CHAT_ALPHA_ACCEPTED_ITEM_ID;

    function snapshot(stage: 'pending' | 'openai-ready' | 'both-ready') {
      const badges = ([jev, openai] as const).map((id) => {
        const execution = work.worker_executions!.find((record) => record.item_id === id)!;
        const ready = id === jev ? stage === 'both-ready' : stage !== 'pending';
        return {
          item_id: id,
          execution_ref: execution.execution_ref,
          approval_id: execution.approval_id,
          task_id: execution.task_id,
          subject_digest: (id === jev ? 'a' : 'b').repeat(64),
          provider: id === jev ? 'jev' as const : 'openai' as const,
          status: ready ? 'result' as const : 'pending' as const,
          coworker_difficulty: 'medium' as const,
          recommended_difficulty: !ready ? null : id === jev ? 'hard' as const : 'easy' as const,
          advice_source: !ready ? null : id === jev ? 'fresh' as const : 'cached' as const,
          cache_age_ms: ready && id === openai ? 120 : null,
        };
      });
      const items = work.items.map((item) => [jev, openai, skipped].includes(item.item_id)
        ? { ...item, title: 'Shared task name' } : item);
      return updateChatWorkFixture(base, CHAT_ALPHA_IDENTITY, {
        items: stage === 'pending' ? items : items.reverse(),
        decision_badges: stage === 'pending' ? badges : badges.reverse(),
      });
    }

    const controller = await installMinimal(page, { width, height: 760 }, {workspaceSnapshot: snapshot('pending')});
    for (const stage of ['pending', 'openai-ready', 'both-ready'] as const) {
      if (stage !== 'pending') {
        await controller.setWorkspaceSnapshot(snapshot(stage));
        await triggerWorkspaceRefresh(page);
      }
      for (const placement of ['stream', 'drawer'] as const) {
        if (placement === 'drawer') await openBacklog(page);
        const badge = (id: string) => page.locator(
          `[data-decision-placement="${placement}"][data-decision-item-id="${id}"]`,
        );
        await expect(badge(jev)).toHaveCount(1);
        await expect(badge(jev)).toHaveText(stage === 'both-ready' ? 'Jev · Hard' : 'Jev · Pending');
        await expect(badge(openai)).toHaveCount(1);
        await expect(badge(openai)).toHaveText(stage === 'pending' ? 'OpenAI · Pending' : 'OpenAI · Easy');
        await expect(badge(skipped)).toHaveCount(0);
        if (placement === 'drawer') {
          for (const id of [jev, openai]) {
            await expect(page.locator(`[data-backlog-item-id="${id}"] summary`)).toContainText('Shared task name');
            await expect(page.locator(`[data-backlog-item-id="${id}"] summary`)).toContainText(id === jev ? 'Jev' : 'OpenAI');
          }
          if (stage === 'both-ready') {
            await badge(openai).focus();
            await page.keyboard.press('Enter');
            const details = page.locator('[data-decision-popover]');
            await expect(details).toContainText('Cached');
            await expect(details).toContainText('Medium');
            await page.keyboard.press('Escape');
            await expect(details).toHaveCount(0);
          }
          await closeBacklogWithEscape(page);
        }
      }
    }
  });
}
