import { expect, test, type Page } from '@playwright/test';

import type { ObservationSource } from '../src/observation/types.ts';
import { TRAY_OPEN_EVENT } from '../src/tray-model.ts';
import {
  parseWorkspaceSnapshot,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  type WorkspaceSnapshot,
} from '../src/workspace-contract.ts';
import {
  buildInteractiveWorkspaceSnapshot,
  CHAT_ALPHA_IDENTITY,
  installWorkspaceTauriFixture,
} from './fixtures/workspace.ts';

const STALE_ADVANCE_MS = WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 1_001;

interface BrowserClockApi {
  advanceByMs: (deltaMs: number) => number;
  readNowMs: () => number;
}

interface StalledSnapshotInvokeStats {
  wrappedSnapshotCalls: number;
  pendingSnapshotReads: number;
  settledSnapshotReads: number;
  snapshotCallTimestampsMs: number[];
  delegatedCommands: Array<{ cmd: string; atMs: number }>;
}

interface BrowserStalledInvokeApi {
  readStats: () => StalledSnapshotInvokeStats;
  releaseAndRestore: (fallbackSnapshot: WorkspaceSnapshot) => void;
}

interface TauriInternalsBridge {
  invoke: (
    cmd: string,
    rawArgs?: Record<string, unknown>
  ) => Promise<unknown>;
}

interface TrayFreshnessWindow extends Window {
  __TAURI_INTERNALS__?: TauriInternalsBridge;
  __THREADLANE_TRAY_TEST_CLOCK__?: BrowserClockApi;
  __THREADLANE_TRAY_STALLED_INVOKE__?: BrowserStalledInvokeApi;
  __THREADLANE_TRAY_VISIBILITY__?: {
    setHidden: (hidden: boolean) => void;
    restore: () => void;
  };
}

interface ControlledClockHandle {
  advanceByMs: (deltaMs: number) => Promise<number>;
  readNowMs: () => Promise<number>;
}

interface StalledSnapshotProbeHandle {
  readStats: () => Promise<StalledSnapshotInvokeStats>;
  releaseAndRestore: (fallbackSnapshot: WorkspaceSnapshot) => Promise<void>;
}

function buildSingleWorkingCoworkerTraySnapshot(
  overrideNowMs?: number
): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  const fixedNowMs = overrideNowMs ?? base.generated_at_ms;

  const alphaChatBase = base.chats.find(
    (chat) => chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id
  );
  if (!alphaChatBase) {
    throw new Error('Expected Alpha chat in interactive workspace fixture.');
  }

  const alphaSampleBase = base.native_samples.find(
    (sample) =>
      sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id
  );
  if (!alphaSampleBase) {
    throw new Error(
      'Expected Alpha native sample in interactive workspace fixture.'
    );
  }

  const canonicalSource: ObservationSource = {
    id: CHAT_ALPHA_IDENTITY.source_id,
    label: 'Primary Desktop Store',
    project_path: CHAT_ALPHA_IDENTITY.canonical_project_scope,
    availability: 'available',
    message: null,
  };

  const candidate: WorkspaceSnapshot = {
    schema_version: base.schema_version,
    generated_at_ms: fixedNowMs,
    sampling_policy: { ...base.sampling_policy },
    observation: {
      schema_version: base.observation.schema_version,
      generated_at_ms: fixedNowMs,
      sources: [canonicalSource],
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
        ...alphaChatBase,
        identity: { ...CHAT_ALPHA_IDENTITY },
        availability: 'available',
        activity: 'working',
        active_job_count: 0,
        attention_count: 0,
        updated_at_ms: fixedNowMs,
      },
    ],
    native_samples: [
      {
        ...alphaSampleBase,
        chat_identity: { ...CHAT_ALPHA_IDENTITY },
        sequence:
          overrideNowMs !== undefined
            ? alphaSampleBase.sequence + 1
            : alphaSampleBase.sequence,
        availability: 'available',
        activity: 'working',
        observed_at_ms: fixedNowMs,
        last_success_at_ms: fixedNowMs,
        connection: {
          ...alphaSampleBase.connection,
          state: 'connected',
          freshness: 'fresh',
          diagnostic: null,
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
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
  };

  return parseWorkspaceSnapshot(candidate);
}

async function installControlledDateBeforeLoad(
  page: Page,
  initialNowMs: number
): Promise<ControlledClockHandle> {
  // Initialize Playwright's fixed clock before registering the manual Date Proxy init script
  // so `installWorkspaceTauriFixture` reuses the existing clock instead of overwriting `window.Date`.
  await page.clock.setFixedTime(initialNowMs);
  await page.addInitScript((fixedMs: number) => {
    const NativeDate = window.Date;
    let currentNowMs = fixedMs;

    const ControlledDate = new Proxy(NativeDate, {
      construct(target, argArray: unknown[], newTarget) {
        if (argArray.length === 0) {
          return Reflect.construct(target, [currentNowMs], newTarget) as Date;
        }
        return Reflect.construct(target, argArray, newTarget) as Date;
      },
      apply() {
        return new NativeDate(currentNowMs).toString();
      },
      get(target, prop) {
        if (prop === 'now') {
          return (): number => currentNowMs;
        }
        const value: unknown = Reflect.get(target, prop, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });

    window.Date = ControlledDate;

    (window as TrayFreshnessWindow).__THREADLANE_TRAY_TEST_CLOCK__ = {
      advanceByMs(deltaMs: number): number {
        currentNowMs += Math.trunc(deltaMs);
        return currentNowMs;
      },
      readNowMs(): number {
        return currentNowMs;
      },
    };
  }, initialNowMs);

  return {
    advanceByMs: (deltaMs: number) =>
      page.evaluate((delta) => {
        const api = (window as TrayFreshnessWindow).__THREADLANE_TRAY_TEST_CLOCK__;
        if (!api) {
          throw new Error('Controlled test Date clock is not installed.');
        }
        return api.advanceByMs(delta);
      }, deltaMs),
    readNowMs: () =>
      page.evaluate(() => {
        const api = (window as TrayFreshnessWindow).__THREADLANE_TRAY_TEST_CLOCK__;
        if (!api) {
          throw new Error('Controlled test Date clock is not installed.');
        }
        return api.readNowMs();
      }),
  };
}

async function wrapSnapshotInvokeWithSingleHungRead(
  page: Page
): Promise<StalledSnapshotProbeHandle> {
  await page.evaluate(() => {
    const win = window as TrayFreshnessWindow;
    const internals = win.__TAURI_INTERNALS__;
    if (!internals || typeof internals.invoke !== 'function') {
      throw new Error('Expected window.__TAURI_INTERNALS__.invoke to exist.');
    }

    const originalInvoke = internals.invoke.bind(internals);
    let wrappedSnapshotCalls = 0;
    let pendingSnapshotReads = 0;
    let settledSnapshotReads = 0;
    const snapshotCallTimestampsMs: number[] = [];
    const delegatedCommands: Array<{ cmd: string; atMs: number }> = [];
    const pendingResolvers: Array<(value: unknown) => void> = [];

    let hungPromise: Promise<unknown> | null = null;

    internals.invoke = (
      cmd: string,
      rawArgs: Record<string, unknown> = {}
    ): Promise<unknown> => {
      if (cmd === 'desktop_workspace_snapshot') {
        wrappedSnapshotCalls += 1;
        pendingSnapshotReads += 1;
        snapshotCallTimestampsMs.push(Date.now());

        if (hungPromise === null) {
          hungPromise = new Promise<unknown>((resolve) => {
            pendingResolvers.push((payload: unknown) => {
              pendingSnapshotReads = Math.max(0, pendingSnapshotReads - 1);
              settledSnapshotReads += 1;
              resolve(payload);
            });
          });
          return hungPromise;
        }

        return new Promise<unknown>((resolve) => {
          pendingResolvers.push((payload: unknown) => {
            pendingSnapshotReads = Math.max(0, pendingSnapshotReads - 1);
            settledSnapshotReads += 1;
            resolve(payload);
          });
        });
      }

      delegatedCommands.push({ cmd, atMs: Date.now() });
      return originalInvoke(cmd, rawArgs);
    };

    win.__THREADLANE_TRAY_STALLED_INVOKE__ = {
      readStats: (): StalledSnapshotInvokeStats => ({
        wrappedSnapshotCalls,
        pendingSnapshotReads,
        settledSnapshotReads,
        snapshotCallTimestampsMs: [...snapshotCallTimestampsMs],
        delegatedCommands: delegatedCommands.map((entry) => ({ ...entry })),
      }),
      releaseAndRestore: (fallbackSnapshot: WorkspaceSnapshot): void => {
        internals.invoke = originalInvoke;
        const resolvers = pendingResolvers.splice(0, pendingResolvers.length);
        const cloned = JSON.parse(JSON.stringify(fallbackSnapshot)) as unknown;
        for (const resolve of resolvers) {
          resolve(cloned);
        }
      },
    };
  });

  return {
    readStats: () =>
      page.evaluate(() => {
        const api = (window as TrayFreshnessWindow)
          .__THREADLANE_TRAY_STALLED_INVOKE__;
        if (!api) {
          throw new Error('Stalled snapshot invoke probe is not installed.');
        }
        return api.readStats();
      }),
    releaseAndRestore: (fallbackSnapshot: WorkspaceSnapshot) =>
      page.evaluate((snap) => {
        const api = (window as TrayFreshnessWindow)
          .__THREADLANE_TRAY_STALLED_INVOKE__;
        api?.releaseAndRestore(snap);
      }, fallbackSnapshot),
  };
}

async function installVisibilityController(page: Page): Promise<{
  setHidden: (hidden: boolean) => Promise<void>;
  restore: () => Promise<void>;
}> {
  await page.evaluate(() => {
    const win = window as TrayFreshnessWindow;
    const originalHiddenDescriptor = Object.getOwnPropertyDescriptor(
      Document.prototype,
      'hidden'
    );
    const originalVisibilityDescriptor = Object.getOwnPropertyDescriptor(
      Document.prototype,
      'visibilityState'
    );

    let forcedHidden: boolean | null = null;

    Object.defineProperty(document, 'hidden', {
      configurable: true,
      get(): boolean {
        if (forcedHidden !== null) {
          return forcedHidden;
        }
        return originalHiddenDescriptor?.get
          ? Boolean(originalHiddenDescriptor.get.call(document))
          : false;
      },
    });

    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get(): DocumentVisibilityState {
        if (forcedHidden !== null) {
          return forcedHidden ? 'hidden' : 'visible';
        }
        return originalVisibilityDescriptor?.get
          ? (originalVisibilityDescriptor.get.call(
              document
            ) as DocumentVisibilityState)
          : 'visible';
      },
    });

    win.__THREADLANE_TRAY_VISIBILITY__ = {
      setHidden(hidden: boolean): void {
        forcedHidden = hidden;
        document.dispatchEvent(new Event('visibilitychange'));
      },
      restore(): void {
        forcedHidden = null;
        delete (document as unknown as Record<string, unknown>).hidden;
        delete (document as unknown as Record<string, unknown>).visibilityState;
      },
    };
  });

  return {
    setHidden: (hidden: boolean) =>
      page.evaluate((nextHidden) => {
        (window as TrayFreshnessWindow).__THREADLANE_TRAY_VISIBILITY__?.setHidden(
          nextHidden
        );
      }, hidden),
    restore: () =>
      page.evaluate(() => {
        (window as TrayFreshnessWindow).__THREADLANE_TRAY_VISIBILITY__?.restore();
      }),
  };
}

async function runBoundedCleanup(
  step: () => Promise<void>,
  timeoutMs: number = 2_500
): Promise<void> {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  try {
    await Promise.race([
      step(),
      new Promise<void>((resolve) => {
        timeoutId = setTimeout(resolve, timeoutMs);
      }),
    ]);
  } catch {
    // Ignore teardown errors if the browser page is already closed.
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
  }
}

function waitRealMs(durationMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
}

test.describe('System tray passive sample staleness', () => {
  test('ages cached working Coworker state to stale when a 4-second poll read stalls', async ({
    page,
  }) => {
    test.setTimeout(25_000);

    const initialSnapshot = buildSingleWorkingCoworkerTraySnapshot();
    const fixedNowMs = initialSnapshot.generated_at_ms;
    const pageErrors: string[] = [];
    const onPageError = (err: Error) => {
      pageErrors.push(err.message);
    };
    page.on('pageerror', onPageError);

    let stalledProbe: StalledSnapshotProbeHandle | null = null;

    try {
      expect(STALE_ADVANCE_MS).toBe(11_001);
      expect(initialSnapshot.observation.sources).toEqual([
        {
          id: CHAT_ALPHA_IDENTITY.source_id,
          label: 'Primary Desktop Store',
          project_path: CHAT_ALPHA_IDENTITY.canonical_project_scope,
          availability: 'available',
          message: null,
        },
      ]);
      expect(initialSnapshot.observation.jobs).toHaveLength(0);
      expect(initialSnapshot.observation.requests).toHaveLength(0);
      expect(initialSnapshot.job_histories).toHaveLength(0);
      expect(initialSnapshot.chats).toHaveLength(1);
      expect(initialSnapshot.native_samples).toHaveLength(1);
      expect(initialSnapshot.native_samples[0]?.activity).toBe('working');
      expect(initialSnapshot.native_samples[0]?.observed_at_ms).toBe(fixedNowMs);
      expect(initialSnapshot.native_samples[0]?.last_success_at_ms).toBe(
        fixedNowMs
      );

      const clock = await installControlledDateBeforeLoad(page, fixedNowMs);
      const tauri = await installWorkspaceTauriFixture(page, {
        workspaceSnapshot: initialSnapshot,
        windowLabel: 'tray',
      });

      await page.goto('/?surface=tray');
      await expect(page.locator('#app')).toHaveAttribute(
        'data-desktop-surface',
        'tray'
      );

      const workingBadges = page.locator(
        '.tl-tray-counts [data-tone="working"]'
      );
      await expect(workingBadges).toHaveCount(1);
      await expect(workingBadges).toHaveText('1 working');

      const workingRows = page.locator('.tl-tray-row[data-category="working"]');
      await expect(workingRows).toHaveCount(1);
      await expect(workingRows).toBeVisible();
      await expect(workingRows.locator('.tl-tray-row-status')).toHaveText(
        'Coworker working'
      );

      const initialInvocations = await tauri.getInvocations();
      const initialSnapshotInvocations = initialInvocations.filter(
        (entry) => entry.cmd === 'desktop_workspace_snapshot'
      );
      expect(initialSnapshotInvocations).toHaveLength(1);
      expect(initialSnapshotInvocations[0]?.atMs).toBe(fixedNowMs);

      stalledProbe = await wrapSnapshotInvokeWithSingleHungRead(page);

      // Wait for the existing 4-second poll interval to issue one pending read.
      await expect
        .poll(
          async () => (await stalledProbe!.readStats()).pendingSnapshotReads,
          { timeout: 6_500, intervals: [100, 200, 250] }
        )
        .toBe(1);

      // Before Date advances past the 10s threshold, the retained sample is still fresh.
      await expect(workingBadges).toHaveCount(1);

      const advancedNowMs = await clock.advanceByMs(STALE_ADVANCE_MS);
      expect(advancedNowMs).toBe(fixedNowMs + 11_001);
      expect(await page.evaluate(() => Date.now())).toBe(advancedNowMs);

      // With real timers running and the snapshot read still stalled, the 1s visible
      // age clock must recompute projection staleness without any new producer reply.
      await expect(workingBadges).toHaveCount(0, { timeout: 3_500 });
      await expect(page.locator('.tl-tray-counts')).toContainText('0 working');
      await expect(workingRows).toHaveCount(0);

      const emptyStateBox = page.locator('.tl-tray-state-box');
      await expect(emptyStateBox).toBeVisible();
      await expect(emptyStateBox).toContainText('No active or recent work');
      await expect(emptyStateBox).toContainText(
        'Live chat indicators are withheld until a fresh sample arrives.'
      );
      await expect(page.locator('.tl-tray-shell')).not.toContainText(
        'Coworker stale'
      );
      await expect(page.locator('.tl-tray-shell')).not.toContainText(
        'Coworker working'
      );

      // Hold across another 4-second poll window (>4.2s after the stalled read began)
      // to prove neither the age clock nor subsequent poll ticks issue overlapping reads.
      await waitRealMs(3_400);

      const statsAfterHold = await stalledProbe.readStats();
      expect(statsAfterHold.pendingSnapshotReads).toBe(1);
      expect(statsAfterHold.wrappedSnapshotCalls).toBe(1);
      expect(statsAfterHold.settledSnapshotReads).toBe(0);
      expect(statsAfterHold.snapshotCallTimestampsMs).toEqual([fixedNowMs]);

      // Confirm non-snapshot Tauri commands still delegate cleanly while the poll hangs.
      await page.getByRole('button', { name: 'Open Threadlane' }).click();
      await expect
        .poll(
          async () => (await stalledProbe!.readStats()).delegatedCommands.length,
          { timeout: 2_500, intervals: [50, 100] }
        )
        .toBe(1);

      const finalStats = await stalledProbe.readStats();
      expect(finalStats.pendingSnapshotReads).toBe(1);
      expect(finalStats.wrappedSnapshotCalls).toBe(1);
      expect(finalStats.settledSnapshotReads).toBe(0);
      expect(finalStats.snapshotCallTimestampsMs).toEqual([fixedNowMs]);
      expect(finalStats.delegatedCommands).toEqual([
        {
          cmd: 'desktop_tray_action',
          atMs: fixedNowMs + 11_001,
        },
      ]);

      const underlyingInvocations = await tauri.getInvocations();
      expect(
        underlyingInvocations.filter(
          (entry) => entry.cmd === 'desktop_workspace_snapshot'
        )
      ).toHaveLength(1);
      expect(
        underlyingInvocations.filter(
          (entry) => entry.cmd === 'desktop_tray_action'
        )
      ).toEqual([
        {
          cmd: 'desktop_tray_action',
          args: { action: 'show_main' },
          atMs: fixedNowMs + 11_001,
        },
      ]);

      expect(await tauri.readArchivePreferencesRaw()).toBeNull();
      await expect(page.locator('.tl-tray-error')).toHaveCount(0);
      expect(pageErrors).toEqual([]);
    } finally {
      page.off('pageerror', onPageError);
      if (stalledProbe !== null && !page.isClosed()) {
        await runBoundedCleanup(() =>
          stalledProbe!.releaseAndRestore(initialSnapshot)
        );
      }
      if (!page.isClosed()) {
        await runBoundedCleanup(() => page.close());
      }
    }
  });

  test('suppresses tray age clock while hidden and recomputes immediately on visible, reopen, and response', async ({
    page,
  }) => {
    test.setTimeout(20_000);

    const initialSnapshot = buildSingleWorkingCoworkerTraySnapshot();
    const fixedNowMs = initialSnapshot.generated_at_ms;
    const pageErrors: string[] = [];
    const onPageError = (err: Error) => {
      pageErrors.push(err.message);
    };
    page.on('pageerror', onPageError);

    let stalledProbe: StalledSnapshotProbeHandle | null = null;
    let visibility: {
      setHidden: (hidden: boolean) => Promise<void>;
      restore: () => Promise<void>;
    } | null = null;

    try {
      const clock = await installControlledDateBeforeLoad(page, fixedNowMs);
      const tauri = await installWorkspaceTauriFixture(page, {
        workspaceSnapshot: initialSnapshot,
        windowLabel: 'tray',
      });

      await page.goto('/?surface=tray');

      const workingBadges = page.locator(
        '.tl-tray-counts [data-tone="working"]'
      );
      const workingRows = page.locator('.tl-tray-row[data-category="working"]');
      const emptyStateBox = page.locator('.tl-tray-state-box');
      await expect(workingBadges).toHaveCount(1);
      await expect(workingBadges).toHaveText('1 working');
      await expect(workingRows).toHaveCount(1);
      await expect(workingRows.locator('.tl-tray-row-status')).toHaveText(
        'Coworker working'
      );
      await expect(emptyStateBox).toHaveCount(0);

      visibility = await installVisibilityController(page);
      await visibility.setHidden(true);

      const hiddenAdvancedNowMs = await clock.advanceByMs(STALE_ADVANCE_MS);
      expect(hiddenAdvancedNowMs).toBe(fixedNowMs + 11_001);
      await tauri.clearInvocations();

      // Hold while hidden past two 1-second clock periods: browser Date is past the 10s stale
      // threshold, yet the suppressed UI age clock must retain the cached working projection
      // without advancing to Stale in the background and without issuing IPC reads.
      await waitRealMs(2_200);
      expect(await clock.readNowMs()).toBe(hiddenAdvancedNowMs);
      expect(await page.evaluate(() => Date.now())).toBe(hiddenAdvancedNowMs);
      await expect(workingBadges).toHaveCount(1);
      await expect(workingBadges).toHaveText('1 working');
      await expect(workingRows).toHaveCount(1);
      await expect(workingRows.locator('.tl-tray-row-status')).toHaveText(
        'Coworker working'
      );
      await expect(emptyStateBox).toHaveCount(0);
      await expect(page.locator('.tl-tray-shell')).not.toContainText(/stale/i);
      expect(await tauri.getInvocations()).toEqual([]);

      // Stall backend reads before becoming visible so only immediate local clock sync can update UI.
      stalledProbe = await wrapSnapshotInvokeWithSingleHungRead(page);
      await visibility.setHidden(false);

      await expect(workingBadges).toHaveCount(0, { timeout: 1_500 });
      await expect(workingRows).toHaveCount(0);
      await expect(emptyStateBox).toBeVisible();
      await expect(emptyStateBox).toContainText('No active or recent work');
      await expect(emptyStateBox).toContainText(
        'Live chat indicators are withheld until a fresh sample arrives.'
      );

      // Advance Date again and emit tray reopen while the snapshot read is still hung:
      // the minimal projection must retain the withheld stale state without waiting
      // for a response or exposing a detailed stale-age label.
      await clock.advanceByMs(4_000);
      await tauri.emitTauriEvent(TRAY_OPEN_EVENT, null);
      await expect(workingBadges).toHaveCount(0);
      await expect(workingRows).toHaveCount(0);
      await expect(emptyStateBox).toContainText(
        'Live chat indicators are withheld until a fresh sample arrives.'
      );
      await expect(page.locator('.tl-tray-shell')).not.toContainText(
        'Coworker stale'
      );

      // Supply a fresh snapshot and release the hung request so queued reopen refresh settles.
      const recoveredNowMs = fixedNowMs + 16_000;
      const freshSnapshot =
        buildSingleWorkingCoworkerTraySnapshot(recoveredNowMs);
      await clock.advanceByMs(recoveredNowMs - (fixedNowMs + 15_001));
      await tauri.setWorkspaceSnapshot(freshSnapshot);
      await stalledProbe.releaseAndRestore(initialSnapshot);
      stalledProbe = null;

      await expect(workingBadges).toHaveCount(1, { timeout: 2_500 });
      await expect(workingBadges).toHaveText('1 working');
      await expect(
        page.locator('.tl-tray-row[data-category="working"] .tl-tray-row-status')
      ).toHaveText('Coworker working');
      expect(pageErrors).toEqual([]);
    } finally {
      page.off('pageerror', onPageError);
      if (stalledProbe !== null && !page.isClosed()) {
        await runBoundedCleanup(() =>
          stalledProbe!.releaseAndRestore(initialSnapshot)
        );
      }
      if (visibility !== null && !page.isClosed()) {
        await runBoundedCleanup(() => visibility!.restore());
      }
      if (!page.isClosed()) {
        await runBoundedCleanup(() => page.close());
      }
    }
  });

  test('recomputes withheld stale state when a tray reopen crosses the producer stale boundary', async ({
    page,
  }) => {
    test.setTimeout(15_000);

    const initialSnapshot = buildSingleWorkingCoworkerTraySnapshot();
    const fixedNowMs = initialSnapshot.generated_at_ms;
    const nearExpiryAdvanceMs = WORKSPACE_SAMPLE_STALE_THRESHOLD_MS - 1;
    const pageErrors: string[] = [];
    const onPageError = (err: Error) => {
      pageErrors.push(err.message);
    };
    page.on('pageerror', onPageError);

    let stalledProbe: StalledSnapshotProbeHandle | null = null;
    let visibility: {
      setHidden: (hidden: boolean) => Promise<void>;
      restore: () => Promise<void>;
    } | null = null;

    try {
      const clock = await installControlledDateBeforeLoad(page, fixedNowMs);
      const tauri = await installWorkspaceTauriFixture(page, {
        workspaceSnapshot: initialSnapshot,
        windowLabel: 'tray',
      });

      await page.goto('/?surface=tray');

      const workingBadges = page.locator(
        '.tl-tray-counts [data-tone="working"]'
      );
      const workingRows = page.locator('.tl-tray-row[data-category="working"]');
      const emptyStateBox = page.locator('.tl-tray-state-box');
      await expect(workingBadges).toHaveCount(1);
      await expect(workingBadges).toHaveText('1 working');
      await expect(workingRows).toHaveCount(1);
      await expect(workingRows.locator('.tl-tray-row-status')).toHaveText(
        'Coworker working'
      );
      await expect(emptyStateBox).toHaveCount(0);

      visibility = await installVisibilityController(page);
      await visibility.setHidden(true);

      const nearExpiryNowMs = await clock.advanceByMs(nearExpiryAdvanceMs);
      expect(nearExpiryNowMs).toBe(
        fixedNowMs + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS - 1
      );

      stalledProbe = await wrapSnapshotInvokeWithSingleHungRead(page);
      await visibility.setHidden(false);

      // First visibility recalculates just before the boundary from the original
      // producer timestamp, so the fresh Working row must remain visible.
      await expect(workingBadges).toHaveCount(1, { timeout: 1_500 });
      await expect(workingRows).toHaveCount(1);
      await expect(workingRows.locator('.tl-tray-row-status')).toHaveText(
        'Coworker working'
      );
      await waitRealMs(1_200);
      expect(await clock.readNowMs()).toBe(nearExpiryNowMs);
      await expect(workingBadges).toHaveCount(1);
      await expect(workingRows).toHaveCount(1);
      await expect(emptyStateBox).toHaveCount(0);

      const visibleStats = await stalledProbe.readStats();
      expect(visibleStats.wrappedSnapshotCalls).toBe(1);
      expect(visibleStats.pendingSnapshotReads).toBe(1);
      expect(visibleStats.settledSnapshotReads).toBe(0);
      expect(visibleStats.snapshotCallTimestampsMs).toEqual([
        nearExpiryNowMs,
      ]);

      await visibility.setHidden(true);
      const expiredNowMs = await clock.advanceByMs(2);
      expect(expiredNowMs).toBe(
        fixedNowMs + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 1
      );

      // The hidden age clock stays suppressed while Date crosses the stale boundary:
      // no producer response renewed the sample, so the UI must still show Working.
      await waitRealMs(1_200);
      expect(await clock.readNowMs()).toBe(expiredNowMs);
      await expect(workingBadges).toHaveCount(1);
      await expect(workingRows).toHaveCount(1);
      await expect(emptyStateBox).toHaveCount(0);
      await expect(page.locator('.tl-tray-shell')).not.toContainText(/stale/i);

      // The explicit tray open recomputes from the original producer timestamp and
      // crosses the stale boundary immediately, without waiting for the hung read.
      await tauri.emitTauriEvent(TRAY_OPEN_EVENT, null);
      await expect(workingBadges).toHaveCount(0, { timeout: 1_000 });
      await expect(workingRows).toHaveCount(0);
      await expect(emptyStateBox).toBeVisible();
      await expect(emptyStateBox).toContainText('No active or recent work');
      await expect(emptyStateBox).toContainText(
        'Live chat indicators are withheld until a fresh sample arrives.'
      );
      await expect(page.locator('.tl-tray-shell')).not.toContainText(
        'Coworker stale'
      );

      const statsAfterReopen = await stalledProbe.readStats();
      expect(statsAfterReopen.wrappedSnapshotCalls).toBe(1);
      expect(statsAfterReopen.pendingSnapshotReads).toBe(1);
      expect(statsAfterReopen.settledSnapshotReads).toBe(0);
      expect(statsAfterReopen.snapshotCallTimestampsMs).toEqual([
        nearExpiryNowMs,
      ]);

      expect(await tauri.readArchivePreferencesRaw()).toBeNull();
      await expect(page.locator('.tl-tray-error')).toHaveCount(0);
      expect(pageErrors).toEqual([]);
    } finally {
      page.off('pageerror', onPageError);
      if (stalledProbe !== null && !page.isClosed()) {
        await runBoundedCleanup(() =>
          stalledProbe!.releaseAndRestore(initialSnapshot)
        );
      }
      if (visibility !== null && !page.isClosed()) {
        await runBoundedCleanup(() => visibility!.restore());
      }
      if (!page.isClosed()) {
        await runBoundedCleanup(() => page.close());
      }
    }
  });
});
