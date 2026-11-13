import '@radix-ui/themes/styles.css';
import './tray.css';

import { Button, Flex, IconButton, Text, Theme } from '@radix-ui/themes';
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import {
  AlertCircle,
  CheckCircle2,
  Clock,
  ExternalLink,
  HelpCircle,
  Loader2,
  Power,
  RefreshCw,
  XCircle,
} from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';

import threadlaneIconUrl from '../src-tauri/icons/icon.svg';
import {
  OPEN_CODEX_CHAT_COMMAND,
  type CodexChatTarget,
} from './app-contract.ts';
import { hasTauriRuntime } from './ipc.ts';
import { ipcWorkspaceSnapshot } from './observation/ipc.ts';
import {
  normalizeTrayErrorMessage,
  projectMinimalTrayModel,
  TRAY_ACTION_COMMAND,
  TRAY_OPEN_EVENT,
  TRAY_REFRESH_INTERVAL_MS,
  TRAY_SNAPSHOT_PAGE_LIMIT,
  TrayObservationCoordinator,
  type TrayAction,
  type TrayActionArgs,
  type TrayCoordinatorState,
  type TrayProjection,
  type TrayRowTone,
  type TrayRowView,
} from './tray-model.ts';

async function invokeTrayAction(action: TrayAction): Promise<void> {
  if (!hasTauriRuntime()) {
    throw new Error('Native desktop tray bridge is not connected.');
  }
  const args: TrayActionArgs = { action };
  await invoke<void>(TRAY_ACTION_COMMAND, { ...args });
}

async function invokeOpenCodexChat(target: CodexChatTarget): Promise<void> {
  if (!hasTauriRuntime()) {
    throw new Error(
      'Opening a conversation in Codex requires the native desktop runtime.'
    );
  }
  const args = { request: target };
  await invoke<void>(OPEN_CODEX_CHAT_COMMAND, { ...args });
}

function renderRowIcon(tone: TrayRowTone) {
  switch (tone) {
    case 'working':
      return <Loader2 size={16} className="tl-spin" aria-hidden="true" />;
    case 'attention':
      return <AlertCircle size={16} aria-hidden="true" />;
    case 'unknown':
      return <HelpCircle size={16} aria-hidden="true" />;
    case 'danger':
      return <XCircle size={16} aria-hidden="true" />;
    case 'success':
      return <CheckCircle2 size={16} aria-hidden="true" />;
    case 'neutral':
      return <Clock size={16} aria-hidden="true" />;
  }
}

function formatRowMetaLine(row: TrayRowView): string {
  const parts = [row.projectLabel, row.chatLabel];
  if (row.coworkerLabel && row.coworkerLabel !== row.statusLabel) {
    parts.push(row.coworkerLabel);
  }
  parts.push(row.ageLabel);
  return parts.join(' · ');
}

function resolveSubbarBanner(
  projection: TrayProjection
): { text: string; tone: string } | null {
  if (projection.status !== 'ready' && projection.status !== 'empty') {
    return null;
  }
  if (projection.refreshError) {
    return {
      text: `Showing retained state · ${projection.refreshError}`,
      tone: 'warning',
    };
  }
  if (projection.sourceNotice) {
    return {
      text: projection.sourceNotice,
      tone: 'warning',
    };
  }
  const coworker = projection.coworkerSummary;
  if (!coworker || projection.status !== 'ready') {
    return null;
  }
  const coveredOnVisibleRow = projection.rows.some(
    (row) =>
      row.statusLabel === coworker.label || row.coworkerLabel === coworker.label
  );
  if (
    coworker.staleCount > 0 ||
    coworker.disconnectedCount > 0 ||
    !coveredOnVisibleRow
  ) {
    return {
      text: coworker.label,
      tone: coworker.tone,
    };
  }
  return null;
}

export interface TrayPopoverCardProps {
  projection: TrayProjection;
  refreshing: boolean;
  openingChat?: boolean;
  actionError: string | null;
  onRefresh: () => void;
  onOpenMain: () => void;
  onQuit: () => void;
  onOpenChat?: (target: CodexChatTarget) => void;
}

export function TrayPopoverCard({
  projection,
  refreshing,
  openingChat = false,
  actionError,
  onRefresh,
  onOpenMain,
  onQuit,
  onOpenChat,
}: TrayPopoverCardProps) {
  const { status, workingCount, attentionCount, rows } = projection;
  const subbar = resolveSubbarBanner(projection);

  return (
    <section className="tl-tray-shell" aria-label="Threadlane system tray">
      <header className="tl-tray-header">
        <div className="tl-tray-brand">
          <img
            src={threadlaneIconUrl}
            alt=""
            aria-hidden="true"
            className="tl-tray-logo"
          />
          <Text size="2" weight="medium">
            Threadlane
          </Text>
        </div>

        <div className="tl-tray-counts" role="status" aria-live="polite">
          {status === 'unavailable' ? (
            <span className="tl-tray-pill" data-tone="unavailable">
              Status unavailable
            </span>
          ) : status === 'loading' ? (
            <span className="tl-tray-pill">Loading…</span>
          ) : (
            <>
              <span
                className="tl-tray-pill"
                data-tone={
                  workingCount && workingCount > 0 ? 'working' : 'idle'
                }
              >
                {workingCount ?? 0} working
              </span>
              <span
                className="tl-tray-pill"
                data-tone={
                  attentionCount && attentionCount > 0 ? 'attention' : 'idle'
                }
              >
                {attentionCount === 1
                  ? '1 needs you'
                  : `${attentionCount ?? 0} need you`}
              </span>
            </>
          )}
        </div>
      </header>

      {subbar ? (
        <div
          className="tl-tray-subbar"
          data-tone={subbar.tone}
          role="status"
          aria-live="polite"
        >
          <span className="tl-tray-subbar-text">{subbar.text}</span>
        </div>
      ) : null}

      <main className="tl-tray-body">
        {status === 'unavailable' ? (
          <div className="tl-tray-state-box" role="status">
            <AlertCircle size={18} aria-hidden="true" />
            <Text size="2" weight="medium" style={{ color: '#f4f4f5' }}>
              Status unavailable
            </Text>
            <Text size="1" style={{ color: '#a1a1aa' }}>
              {projection.unavailableReason ??
                'Workspace observation runtime is not reachable in this window.'}
            </Text>
            <Button
              size="1"
              variant="soft"
              color="gray"
              onClick={onRefresh}
              disabled={refreshing}
            >
              <RefreshCw size={13} aria-hidden="true" />
              Retry status check
            </Button>
          </div>
        ) : status === 'loading' ? (
          <div className="tl-tray-state-box" role="status">
            <Loader2 size={18} className="tl-spin" aria-hidden="true" />
            <Text size="2">Reading workspace snapshot…</Text>
          </div>
        ) : status === 'empty' || rows.length === 0 ? (
          <div className="tl-tray-state-box" role="status">
            <CheckCircle2 size={18} aria-hidden="true" />
            <Text size="2" weight="medium" style={{ color: '#e4e4e7' }}>
              No active or recent work
            </Text>
            <Text size="1" style={{ color: '#a1a1aa' }}>
              {projection.emptyMessage ??
                'Observed conversations have no pending questions or active workers.'}
            </Text>
          </div>
        ) : (
          rows.map((row) => {
            const target = row.codexTarget;
            const content = (
              <>
                <span className="tl-tray-row-icon" data-tone={row.tone}>
                  {renderRowIcon(row.tone)}
                </span>
                <div className="tl-tray-row-content">
                  <div className="tl-tray-row-top">
                    <span className="tl-tray-row-title">{row.title}</span>
                    <span className="tl-tray-row-status" data-tone={row.tone}>
                      {row.statusLabel}
                    </span>
                  </div>
                  <div className="tl-tray-row-meta">
                    {formatRowMetaLine(row)}
                  </div>
                  <div className="tl-tray-row-detail">{row.detail}</div>
                </div>
              </>
            );

            if (onOpenChat && target !== null) {
              return (
                <button
                  key={row.key}
                  type="button"
                  className="tl-tray-row tl-tray-row-interactive"
                  data-tone={row.tone}
                  data-category={row.category}
                  disabled={openingChat}
                  aria-label={`Open ${row.chatLabel} in Codex`}
                  onClick={() => onOpenChat(target)}
                >
                  {content}
                </button>
              );
            }

            return (
              <article
                key={row.key}
                className="tl-tray-row"
                data-tone={row.tone}
                data-category={row.category}
              >
                {content}
              </article>
            );
          })
        )}
      </main>

      {actionError ? (
        <div className="tl-tray-error" role="alert">
          {actionError}
        </div>
      ) : null}

      <footer className="tl-tray-footer">
        <Button
          size="1"
          variant="solid"
          color="gray"
          highContrast
          onClick={onOpenMain}
          style={{ flex: 1, justifyContent: 'center' }}
        >
          <ExternalLink size={14} aria-hidden="true" />
          Open Threadlane
        </Button>

        <Flex align="center" gap="1">
          <IconButton
            size="1"
            variant="soft"
            color="gray"
            aria-label="Refresh tray status"
            onClick={onRefresh}
            disabled={refreshing}
          >
            <RefreshCw size={14} aria-hidden="true" />
          </IconButton>
          <Button
            size="1"
            variant="soft"
            color="gray"
            aria-label="Quit Threadlane"
            onClick={onQuit}
          >
            <Power size={14} aria-hidden="true" />
            Quit
          </Button>
        </Flex>
      </footer>
    </section>
  );
}

const VISIBLE_UI_AGE_CLOCK_INTERVAL_MS = 1_000;

function isDocumentVisible(): boolean {
  return typeof document === 'undefined' || !document.hidden;
}

function readWindowVisibility(): { visible: boolean; focused: boolean } {
  const visible = isDocumentVisible();
  const focused =
    typeof document === 'undefined' || typeof document.hasFocus !== 'function'
      ? true
      : document.hasFocus();
  return { visible, focused };
}

function createTrayCoordinator(
  onChange: (state: TrayCoordinatorState) => void
): TrayObservationCoordinator {
  const initial = readWindowVisibility();
  return new TrayObservationCoordinator(
    (request) =>
      ipcWorkspaceSnapshot(request ?? { limit: TRAY_SNAPSHOT_PAGE_LIMIT }),
    onChange,
    {
      openCodexChat: invokeOpenCodexChat,
      visible: initial.visible,
      focused: initial.focused,
    }
  );
}

function LiveTrayPopoverApp() {
  const [currentNowMs, setCurrentNowMs] = useState<number>(() => Date.now());
  const lastSyncedSnapshotRef =
    useRef<TrayCoordinatorState['snapshot']>(null);
  const syncAgeClockRef = useRef<(() => void) | null>(null);

  const handleCoordinatorStateChange = useCallback(
    (nextState: TrayCoordinatorState) => {
      const snapshotChanged =
        nextState.snapshot !== lastSyncedSnapshotRef.current;
      lastSyncedSnapshotRef.current = nextState.snapshot;
      setCoordState(nextState);
      if (isDocumentVisible() || snapshotChanged) {
        setCurrentNowMs(Date.now());
      }
    },
    []
  );

  const coordinatorRef = useRef<TrayObservationCoordinator | null>(null);
  if (coordinatorRef.current === null) {
    coordinatorRef.current = createTrayCoordinator(
      handleCoordinatorStateChange
    );
  }

  const [coordState, setCoordState] = useState<TrayCoordinatorState>(() =>
    coordinatorRef.current!.getState()
  );
  const [actionError, setActionError] = useState<string | null>(null);

  const triggerRefresh = useCallback((force: boolean = false) => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    setActionError(null);
    coordinator.clearOpenError();
    void coordinator.refresh({ force });
  }, []);

  const handleRefresh = useCallback(() => {
    triggerRefresh(true);
  }, [triggerRefresh]);

  const handleOpenMain = useCallback(() => {
    setActionError(null);
    coordinatorRef.current?.clearOpenError();
    void invokeTrayAction('show_main').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to open the main window.')
      );
    });
  }, []);

  const handleHide = useCallback(() => {
    setActionError(null);
    coordinatorRef.current?.clearOpenError();
    void invokeTrayAction('hide').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to hide the tray popover.')
      );
    });
  }, []);

  const handleQuit = useCallback(() => {
    setActionError(null);
    coordinatorRef.current?.clearOpenError();
    void invokeTrayAction('quit').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to quit Threadlane.')
      );
    });
  }, []);

  const handleOpenChat = useCallback((target: CodexChatTarget) => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    setActionError(null);
    void coordinator.openCodexChat(target);
  }, []);

  useEffect(() => {
    if (
      coordinatorRef.current === null ||
      coordinatorRef.current.isDisposed()
    ) {
      coordinatorRef.current = createTrayCoordinator(
        handleCoordinatorStateChange
      );
    }
    const coordinator = coordinatorRef.current;

    const initial = readWindowVisibility();
    if (initial.visible) {
      void coordinator.open();
    } else {
      coordinator.close();
    }

    const onVisibilityChange = () => {
      const next = readWindowVisibility();
      if (!next.visible) {
        coordinator.close();
        return;
      }
      void coordinator.notifyVisibilityTransition(true, next.focused);
    };

    const onWindowFocus = () => {
      const next = readWindowVisibility();
      void coordinator.notifyVisibilityTransition(next.visible, true);
    };

    const onWindowBlur = () => {
      const next = readWindowVisibility();
      coordinator.setVisibility(next.visible, false);
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault();
        handleHide();
      }
    };

    document.addEventListener('visibilitychange', onVisibilityChange);
    window.addEventListener('focus', onWindowFocus);
    window.addEventListener('blur', onWindowBlur);
    window.addEventListener('keydown', onKeyDown);

    const intervalId = window.setInterval(() => {
      const current = readWindowVisibility();
      if (!current.visible) {
        coordinator.close();
        return;
      }
      coordinator.setVisibility(current.visible, current.focused);
      if (
        hasTauriRuntime() &&
        coordinator.canPoll() &&
        !coordinator.isInFlight()
      ) {
        void coordinator.refresh();
      }
    }, TRAY_REFRESH_INTERVAL_MS);

    let disposed = false;
    let unlistenOpen: UnlistenFn | null = null;
    if (hasTauriRuntime()) {
      void listen(TRAY_OPEN_EVENT, () => {
        if (disposed) {
          return;
        }
        setActionError(null);
        coordinator.clearOpenError();
        setCurrentNowMs(Date.now());
        syncAgeClockRef.current?.();
        void coordinator.open();
      })
        .then((fn) => {
          if (disposed) {
            fn();
          } else {
            unlistenOpen = fn;
          }
        })
        .catch(() => {
          // Ignore event subscription errors when native tray event is not active yet.
        });
    }

    return () => {
      disposed = true;
      window.clearInterval(intervalId);
      document.removeEventListener('visibilitychange', onVisibilityChange);
      window.removeEventListener('focus', onWindowFocus);
      window.removeEventListener('blur', onWindowBlur);
      window.removeEventListener('keydown', onKeyDown);
      if (unlistenOpen) {
        unlistenOpen();
      }
      coordinator.dispose();
      if (coordinatorRef.current === coordinator) {
        coordinatorRef.current = null;
      }
    };
  }, [handleCoordinatorStateChange, handleHide]);

  // Owned visible-UI age clock: advances tray projection nowMs independently of
  // backend poll completion, suppresses background ticks while hidden, and
  // synchronizes immediately when visible or reopened.
  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    let ageClockIntervalId: number | null = null;

    const stopAgeClock = () => {
      if (ageClockIntervalId !== null) {
        window.clearInterval(ageClockIntervalId);
        ageClockIntervalId = null;
      }
    };

    const tickAgeClock = () => {
      if (!isDocumentVisible()) {
        stopAgeClock();
        return;
      }
      setCurrentNowMs(Date.now());
    };

    const startAgeClockIfVisible = () => {
      if (!isDocumentVisible()) {
        stopAgeClock();
        return;
      }
      if (ageClockIntervalId === null) {
        ageClockIntervalId = window.setInterval(
          tickAgeClock,
          VISIBLE_UI_AGE_CLOCK_INTERVAL_MS
        );
      }
    };

    const syncAgeClock = () => {
      if (isDocumentVisible()) {
        setCurrentNowMs(Date.now());
        startAgeClockIfVisible();
      } else {
        stopAgeClock();
      }
    };

    syncAgeClockRef.current = syncAgeClock;
    syncAgeClock();

    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', syncAgeClock);
    }
    window.addEventListener('focus', syncAgeClock);

    return () => {
      if (syncAgeClockRef.current === syncAgeClock) {
        syncAgeClockRef.current = null;
      }
      stopAgeClock();
      if (typeof document !== 'undefined') {
        document.removeEventListener('visibilitychange', syncAgeClock);
      }
      window.removeEventListener('focus', syncAgeClock);
    };
  }, []);

  const projection = useMemo(
    () =>
      projectMinimalTrayModel(coordState.snapshot, {
        loading: coordState.loading,
        error: coordState.error,
        nowMs: currentNowMs,
      }),
    [coordState.snapshot, coordState.loading, coordState.error, currentNowMs]
  );

  const effectiveActionError = actionError ?? coordState.openError;

  return (
    <Theme
      appearance="dark"
      accentColor="gray"
      grayColor="gray"
      radius="medium"
      scaling="95%"
      className="tl-tray-theme"
    >
      <TrayPopoverCard
        projection={projection}
        refreshing={coordState.loading}
        openingChat={coordState.openingChat}
        actionError={effectiveActionError}
        onRefresh={handleRefresh}
        onOpenMain={handleOpenMain}
        onQuit={handleQuit}
        onOpenChat={handleOpenChat}
      />
    </Theme>
  );
}

const mountedTrayRoots = new WeakMap<HTMLElement, Root>();

export function mountTrayPopover(mountNode: HTMLElement): void {
  let root = mountedTrayRoots.get(mountNode);
  if (!root) {
    root = createRoot(mountNode);
    mountedTrayRoots.set(mountNode, root);
  }
  root.render(<LiveTrayPopoverApp />);
}
