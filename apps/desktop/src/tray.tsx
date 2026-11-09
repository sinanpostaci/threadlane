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
import { createRoot } from 'react-dom/client';

import threadlaneIconUrl from '../src-tauri/icons/icon.svg';
import { hasTauriRuntime } from './ipc.ts';
import { ipcObservationSnapshot } from './observation/ipc.ts';
import {
  normalizeTrayErrorMessage,
  projectTrayModel,
  TRAY_ACTION_COMMAND,
  TRAY_OPEN_EVENT,
  TRAY_REFRESH_INTERVAL_MS,
  TrayObservationCoordinator,
  type TrayAction,
  type TrayActionArgs,
  type TrayCoordinatorState,
  type TrayProjection,
  type TrayRowTone,
} from './tray-model.ts';

async function invokeTrayAction(action: TrayAction): Promise<void> {
  if (!hasTauriRuntime()) {
    throw new Error('Native desktop tray bridge is not connected.');
  }
  const args: TrayActionArgs = { action };
  await invoke<void>(TRAY_ACTION_COMMAND, { ...args });
}

function renderRowIcon(tone: TrayRowTone) {
  switch (tone) {
    case 'working':
      return <Loader2 size={20} className="tl-spin" aria-hidden="true" />;
    case 'attention':
      return <AlertCircle size={20} aria-hidden="true" />;
    case 'unknown':
      return <HelpCircle size={20} aria-hidden="true" />;
    case 'danger':
      return <XCircle size={20} aria-hidden="true" />;
    case 'success':
      return <CheckCircle2 size={20} aria-hidden="true" />;
    case 'neutral':
      return <Clock size={20} aria-hidden="true" />;
  }
}

export interface TrayPopoverCardProps {
  projection: TrayProjection;
  refreshing: boolean;
  actionError: string | null;
  onRefresh: () => void;
  onOpenMain: () => void;
  onQuit: () => void;
}

export function TrayPopoverCard({
  projection,
  refreshing,
  actionError,
  onRefresh,
  onOpenMain,
  onQuit,
}: TrayPopoverCardProps) {
  const { status, workingCount, attentionCount, rows } = projection;

  return (
    <section className="tl-tray-shell" aria-label="Threadlane system tray">
      <header className="tl-tray-header" data-tauri-drag-region="true">
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
                data-tone={workingCount && workingCount > 0 ? 'working' : 'idle'}
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

      <main className="tl-tray-body">
        {status === 'unavailable' ? (
          <div className="tl-tray-state-box" role="status">
            <AlertCircle size={20} aria-hidden="true" />
            <Text size="2" weight="medium" style={{ color: '#f4f4f5' }}>
              Status unavailable
            </Text>
            <Text size="1" style={{ color: '#a1a1aa' }}>
              {projection.unavailableReason ??
                'Observation runtime is not reachable in this window.'}
            </Text>
            <Button
              size="1"
              variant="soft"
              color="gray"
              onClick={onRefresh}
              disabled={refreshing}
            >
              <RefreshCw size={14} aria-hidden="true" />
              Retry status check
            </Button>
          </div>
        ) : status === 'loading' ? (
          <div className="tl-tray-state-box" role="status">
            <Loader2 size={20} className="tl-spin" aria-hidden="true" />
            <Text size="2">Reading workspace observation…</Text>
          </div>
        ) : status === 'empty' || rows.length === 0 ? (
          <div className="tl-tray-state-box" role="status">
            <CheckCircle2 size={20} aria-hidden="true" />
            <Text size="2" weight="medium" style={{ color: '#e4e4e7' }}>
              No active or recent work
            </Text>
            <Text size="1" style={{ color: '#a1a1aa' }}>
              {projection.emptyMessage ??
                'Observed conversations have no pending questions or active workers.'}
            </Text>
          </div>
        ) : (
          rows.map((row) => (
            <article
              key={row.key}
              className="tl-tray-row"
              data-tone={row.tone}
              data-category={row.category}
            >
              <span className="tl-tray-row-icon" data-tone={row.tone}>
                {renderRowIcon(row.tone)}
              </span>
              <div style={{ minWidth: 0 }}>
                <div className="tl-tray-row-top">
                  <span className="tl-tray-row-title">{row.title}</span>
                  <span className="tl-tray-row-status" data-tone={row.tone}>
                    {row.statusLabel}
                  </span>
                </div>
                <div className="tl-tray-row-meta">
                  {row.projectLabel} · {row.chatLabel} · {row.ageLabel}
                </div>
                <div className="tl-tray-row-detail">{row.detail}</div>
              </div>
            </article>
          ))
        )}
      </main>

      {actionError ? (
        <div className="tl-tray-error" role="alert">
          {actionError}
        </div>
      ) : null}

      <footer className="tl-tray-footer">
        <Button
          size="2"
          variant="solid"
          color="gray"
          highContrast
          onClick={onOpenMain}
          style={{ flex: 1, justifyContent: 'center' }}
        >
          <ExternalLink size={16} aria-hidden="true" />
          Open Threadlane
        </Button>

        <Flex align="center" gap="1">
          <IconButton
            size="2"
            variant="soft"
            color="gray"
            aria-label="Refresh tray status"
            onClick={onRefresh}
            disabled={refreshing}
          >
            <RefreshCw size={16} aria-hidden="true" />
          </IconButton>
          <Button
            size="2"
            variant="soft"
            color="gray"
            aria-label="Quit Threadlane"
            onClick={onQuit}
          >
            <Power size={16} aria-hidden="true" />
            Quit
          </Button>
        </Flex>
      </footer>
    </section>
  );
}

function LiveTrayPopoverApp() {
  const [coordState, setCoordState] = useState<TrayCoordinatorState>({
    snapshot: null,
    loading: true,
    error: null,
  });
  const [actionError, setActionError] = useState<string | null>(null);
  const coordinatorRef = useRef<TrayObservationCoordinator | null>(null);

  if (coordinatorRef.current === null) {
    coordinatorRef.current = new TrayObservationCoordinator(
      () => ipcObservationSnapshot({ limit: 50 }),
      (nextState) => {
        setCoordState(nextState);
      }
    );
  }

  const triggerRefresh = useCallback((force: boolean = false) => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    void coordinator.refresh({ force });
  }, []);

  const handleOpenMain = useCallback(() => {
    setActionError(null);
    void invokeTrayAction('show_main').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to open the main window.')
      );
    });
  }, []);

  const handleHide = useCallback(() => {
    setActionError(null);
    void invokeTrayAction('hide').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to hide the tray popover.')
      );
    });
  }, []);

  const handleQuit = useCallback(() => {
    setActionError(null);
    void invokeTrayAction('quit').catch((err: unknown) => {
      setActionError(
        normalizeTrayErrorMessage(err, 'Unable to quit Threadlane.')
      );
    });
  }, []);

  useEffect(() => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }

    const syncVisibility = (): boolean => {
      const visible = typeof document === 'undefined' ? true : !document.hidden;
      const focused =
        typeof document === 'undefined' || typeof document.hasFocus !== 'function'
          ? true
          : document.hasFocus();
      coordinator.setVisibility(visible, focused);
      return visible && focused;
    };

    syncVisibility();
    void coordinator.refresh({ force: true });

    const onVisibilityChange = () => {
      if (syncVisibility()) {
        void coordinator.refresh();
      }
    };

    const onWindowFocus = () => {
      coordinator.setVisibility(
        typeof document === 'undefined' ? true : !document.hidden,
        true
      );
      void coordinator.refresh();
    };

    const onWindowBlur = () => {
      coordinator.setVisibility(
        typeof document === 'undefined' ? true : !document.hidden,
        false
      );
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
      syncVisibility();
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
        coordinator.setVisibility(true, true);
        void coordinator.refresh({ force: true });
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
    };
  }, [handleHide]);

  const projection = useMemo(
    () =>
      projectTrayModel(coordState.snapshot, {
        loading: coordState.loading,
        error: coordState.error,
      }),
    [coordState.snapshot, coordState.loading, coordState.error]
  );

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
        actionError={actionError}
        onRefresh={() => triggerRefresh(true)}
        onOpenMain={handleOpenMain}
        onQuit={handleQuit}
      />
    </Theme>
  );
}

export function mountTrayPopover(mountNode: HTMLElement): void {
  const root = createRoot(mountNode);
  root.render(<LiveTrayPopoverApp />);
}
