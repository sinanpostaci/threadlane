import {
  Badge,
  Button,
  Callout,
  Card,
  Flex,
  Heading,
  IconButton,
  Switch,
  Text,
  TextField,
} from '@radix-ui/themes';
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import {
  AlertCircle,
  CheckCircle2,
  ChevronRight,
  FolderOpen,
  Link2,
  Play,
  RefreshCw,
  Shield,
  Sparkles,
} from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';

import {
  applyCodexSetup,
  fetchCodexSetupStatus,
  recheckCodexSetup,
} from './codex-setup/ipc.ts';
import {
  createBrowserPreviewCodexSetupReport,
  createInitialCodexSetupReport,
  projectCodexSetupViewModel,
  projectCodexSkillSetupView,
  type CodexSetupBadgeTone,
  type CodexSetupReport,
} from './codex-setup/types.ts';
import {
  sanitizeAppearancePreferences,
} from './format.ts';
import {
  hasTauriRuntime,
  ipcDesktopAnswerDemo,
  ipcDesktopAttach,
  ipcDesktopCancelDemo,
  ipcDesktopSnapshot,
  ipcDesktopStartDemo,
} from './ipc.ts';
import {
  ipcLinkObservationContext,
  ipcObservationSources,
} from './observation/ipc.ts';
import type { ObservationSource } from './observation/types.ts';
import type { ProjectDiagnosticRowView } from './pages/projects-model.ts';
import { formatAppError } from './registry/ipc.ts';
import {
  applyTrayPreferencesLoadError,
  applyTrayPreferencesLoadSuccess,
  applyTrayPreferencesSaveError,
  applyTrayPreferencesSaveResult,
  createInitialTrayPreferencesState,
  TRAY_GET_PREFERENCES_COMMAND,
  TRAY_PREFERENCES_EVENT,
  TRAY_SET_PREFERENCES_COMMAND,
  type SetTrayPreferencesArgs,
  type TrayCloseBehavior,
  type TrayPreferencesState,
} from './tray-model.ts';
import {
  ACCENT_PALETTES,
  DEFAULT_APPEARANCE,
  type AccentName,
  type AppearancePreferences,
  type DesktopSnapshot,
  type SurfaceMode,
  type ThemeMode,
} from './types.ts';

export const APPEARANCE_STORAGE_KEY = 'threadlane.desktop.appearance.v1';

export function loadSavedAppearance(): AppearancePreferences {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return { ...DEFAULT_APPEARANCE };
    }
    const raw = window.localStorage.getItem(APPEARANCE_STORAGE_KEY);
    if (!raw) {
      return { ...DEFAULT_APPEARANCE };
    }
    return sanitizeAppearancePreferences(JSON.parse(raw));
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

export function saveAppearance(prefs: AppearancePreferences): void {
  try {
    if (typeof window === 'undefined' || !window.localStorage) {
      return;
    }
    const persisted: AppearancePreferences = {
      theme: prefs.theme,
      surface: prefs.surface,
      opacity: prefs.opacity,
      accent: prefs.accent,
      rememberStateDir: prefs.rememberStateDir,
      savedStateDir: prefs.rememberStateDir
        ? prefs.savedStateDir.trim().slice(0, 512)
        : '',
    };
    window.localStorage.setItem(
      APPEARANCE_STORAGE_KEY,
      JSON.stringify(persisted)
    );
  } catch {
    // Ignore storage write errors in restricted webviews.
  }
}


function mapSetupBadgeColor(
  tone: CodexSetupBadgeTone
): 'green' | 'blue' | 'amber' | 'red' | 'gray' {
  switch (tone) {
    case 'ready':
      return 'green';
    case 'info':
      return 'blue';
    case 'warning':
      return 'amber';
    case 'danger':
      return 'red';
    case 'neutral':
      return 'gray';
  }
}

export interface SettingsPanelProps {
  appearance: AppearancePreferences;
  onAppearanceChange: (next: AppearancePreferences) => void;
  diagnostics: readonly ProjectDiagnosticRowView[];
  warnings: readonly string[];
  onObservationLinked: () => void;
}

export function SettingsPanel({
  appearance,
  onAppearanceChange,
  diagnostics,
  warnings,
  onObservationLinked,
}: SettingsPanelProps) {
  const nativeAvailable = hasTauriRuntime();

  // 1. Tray / Window close preferences (native command + event sync)
  const [trayPrefs, setTrayPrefs] = useState<TrayPreferencesState>(() =>
    createInitialTrayPreferencesState(nativeAvailable)
  );
  const traySeqRef = useRef(0);

  const loadTrayPreferences = useCallback(async () => {
    if (!hasTauriRuntime()) {
      setTrayPrefs(createInitialTrayPreferencesState(false));
      return;
    }
    const seq = ++traySeqRef.current;
    try {
      const raw = await invoke<unknown>(TRAY_GET_PREFERENCES_COMMAND);
      if (seq !== traySeqRef.current) {
        return;
      }
      setTrayPrefs(applyTrayPreferencesLoadSuccess(raw));
    } catch (err) {
      if (seq !== traySeqRef.current) {
        return;
      }
      setTrayPrefs(applyTrayPreferencesLoadError(err));
    }
  }, []);

  useEffect(() => {
    void loadTrayPreferences();
    if (!hasTauriRuntime()) {
      return;
    }
    let disposed = false;
    let unlisten: UnlistenFn | null = null;
    void listen(TRAY_PREFERENCES_EVENT, (event) => {
      if (disposed) {
        return;
      }
      if (
        event &&
        typeof event === 'object' &&
        'payload' in event &&
        event.payload
      ) {
        const next = applyTrayPreferencesLoadSuccess(event.payload);
        if (next.availability === 'available' || next.message !== null) {
          setTrayPrefs(next);
          return;
        }
      }
      void loadTrayPreferences();
    })
      .then((fn) => {
        if (disposed) {
          fn();
        } else {
          unlisten = fn;
        }
      })
      .catch(() => {
        // Ignore event subscription errors when native tray event is inactive.
      });

    return () => {
      disposed = true;
      if (unlisten) {
        unlisten();
      }
    };
  }, [loadTrayPreferences]);

  const handleSetCloseBehavior = async (closeBehavior: TrayCloseBehavior) => {
    if (
      !hasTauriRuntime() ||
      trayPrefs.availability !== 'available' ||
      trayPrefs.saving
    ) {
      return;
    }
    const seq = ++traySeqRef.current;
    const previous = trayPrefs;
    setTrayPrefs({
      ...previous,
      saving: true,
      error: null,
    });
    const args: SetTrayPreferencesArgs = {
      request: {
        close_behavior: closeBehavior,
      },
    };
    try {
      const raw = await invoke<unknown>(TRAY_SET_PREFERENCES_COMMAND, {
        ...args,
      });
      if (seq !== traySeqRef.current) {
        return;
      }
      setTrayPrefs(applyTrayPreferencesSaveResult(previous, raw));
    } catch (err) {
      if (seq !== traySeqRef.current) {
        return;
      }
      // A native reconciliation event may have arrived before the rejected call.
      // Preserve it instead of restoring the value captured before the write.
      setTrayPrefs((current) => applyTrayPreferencesSaveError(current, err));
      if (
        err &&
        typeof err === 'object' &&
        'code' in err &&
        err.code === 'preferences_durability_uncertain'
      ) {
        try {
          const raw = await invoke<unknown>(TRAY_GET_PREFERENCES_COMMAND);
          if (seq !== traySeqRef.current) return;
          setTrayPrefs(
            applyTrayPreferencesSaveError(applyTrayPreferencesLoadSuccess(raw), err)
          );
        } catch {
          if (seq !== traySeqRef.current) return;
          setTrayPrefs((current) => ({
            ...current,
            availability: 'unavailable',
            saving: false,
            message: 'Current close behavior could not be verified. Recheck before changing this setting.',
          }));
        }
      }
    }
  };

  // 2. Codex MCP & Skill setup status (read-only on load; explicit human actions for recheck/apply)
  const [codexReport, setCodexReport] = useState<CodexSetupReport>(() =>
    nativeAvailable
      ? createInitialCodexSetupReport()
      : createBrowserPreviewCodexSetupReport()
  );
  const [codexBusy, setCodexBusy] = useState(false);

  useEffect(() => {
    if (!hasTauriRuntime()) {
      setCodexReport(createBrowserPreviewCodexSetupReport());
      return;
    }
    let cancelled = false;
    setCodexBusy(true);
    void fetchCodexSetupStatus()
      .then((report) => {
        if (!cancelled) {
          setCodexReport(report);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setCodexBusy(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const handleRecheckCodex = async () => {
    if (codexBusy || !hasTauriRuntime()) {
      return;
    }
    setCodexBusy(true);
    try {
      const next = await recheckCodexSetup();
      setCodexReport(next);
    } finally {
      setCodexBusy(false);
    }
  };

  const handleApplyCodex = async () => {
    if (codexBusy || !hasTauriRuntime()) {
      return;
    }
    setCodexBusy(true);
    try {
      const next = await applyCodexSetup();
      setCodexReport(next);
    } finally {
      setCodexBusy(false);
    }
  };

  // 3. Linked observation sources
  const [sources, setSources] = useState<ObservationSource[]>([]);
  const [sourcesLoading, setSourcesLoading] = useState(false);
  const [sourcesError, setSourcesError] = useState<string | null>(null);
  const [contextPathDraft, setContextPathDraft] = useState('');
  const [linkingContext, setLinkingContext] = useState(false);
  const [linkError, setLinkError] = useState<string | null>(null);
  const [linkNotice, setLinkNotice] = useState<string | null>(null);
  const sourcesSeqRef = useRef(0);

  const refreshSources = useCallback(async () => {
    const seq = ++sourcesSeqRef.current;
    setSourcesLoading(true);
    setSourcesError(null);
    try {
      const list = await ipcObservationSources();
      if (seq !== sourcesSeqRef.current) {
        return;
      }
      setSources(list);
    } catch (err) {
      if (seq !== sourcesSeqRef.current) {
        return;
      }
      setSourcesError(
        formatAppError(err, 'Failed to load linked observation sources.')
      );
    } finally {
      if (seq === sourcesSeqRef.current) {
        setSourcesLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshSources();
  }, [refreshSources]);

  const handleLinkContext = async () => {
    const trimmed = contextPathDraft.trim();
    if (!trimmed) {
      setLinkError('Enter a local observation context path before linking.');
      setLinkNotice(null);
      return;
    }
    if (linkingContext) {
      return;
    }
    setLinkingContext(true);
    setLinkError(null);
    setLinkNotice(null);
    try {
      const linked = await ipcLinkObservationContext(trimmed);
      const label = linked.label.trim() || linked.id;
      setLinkNotice(`Linked ${label}.`);
      setContextPathDraft('');
      await refreshSources();
      onObservationLinked();
    } catch (err) {
      setLinkError(
        formatAppError(err, 'Failed to link the supplied observation context.')
      );
    } finally {
      setLinkingContext(false);
    }
  };

  // 5. Advanced runtime attachment & local demo controls
  const [runtimeSnapshot, setRuntimeSnapshot] =
    useState<DesktopSnapshot | null>(null);
  const [stateDirDraft, setStateDirDraft] = useState(
    () => appearance.savedStateDir || ''
  );
  const [runtimeBusy, setRuntimeBusy] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);
  const [runtimeNotice, setRuntimeNotice] = useState<string | null>(null);

  const loadRuntimeSnapshot = useCallback(async () => {
    try {
      const snap = await ipcDesktopSnapshot(null);
      setRuntimeSnapshot(snap);
    } catch {
      // Optional diagnostic surface; errors surfaced on explicit mutation.
    }
  }, []);

  useEffect(() => {
    void loadRuntimeSnapshot();
  }, [loadRuntimeSnapshot]);

  const handleAttachRuntime = async () => {
    const targetDir = stateDirDraft.trim();
    if (!targetDir) {
      setRuntimeError(
        'Provide a valid runtime state directory path before attaching.'
      );
      setRuntimeNotice(null);
      return;
    }
    if (runtimeBusy) {
      return;
    }
    setRuntimeBusy(true);
    setRuntimeError(null);
    setRuntimeNotice(null);
    try {
      const snap = await ipcDesktopAttach(targetDir);
      setRuntimeSnapshot(snap);
      setRuntimeNotice(`Attached runtime directory: ${targetDir}`);
      if (appearance.rememberStateDir) {
        const nextApp = sanitizeAppearancePreferences({
          ...appearance,
          savedStateDir: targetDir,
        });
        onAppearanceChange(nextApp);
      }
      onObservationLinked();
    } catch (err) {
      setRuntimeError(
        formatAppError(err, 'Failed to attach runtime state directory.')
      );
    } finally {
      setRuntimeBusy(false);
    }
  };

  const handleDemoAction = async (
    action: 'start' | 'answer' | 'cancel'
  ): Promise<void> => {
    if (runtimeBusy) {
      return;
    }
    setRuntimeBusy(true);
    setRuntimeError(null);
    setRuntimeNotice(null);
    try {
      let snap: DesktopSnapshot;
      if (action === 'start') {
        snap = await ipcDesktopStartDemo();
      } else {
        const rev = runtimeSnapshot?.demo_controls.revision;
        if (typeof rev !== 'number') {
          throw new Error('No active local demo revision available.');
        }
        snap =
          action === 'answer'
            ? await ipcDesktopAnswerDemo(rev)
            : await ipcDesktopCancelDemo(rev);
      }
      setRuntimeSnapshot(snap);
      onObservationLinked();
    } catch (err) {
      setRuntimeError(
        formatAppError(err, `Failed to ${action} local demo run.`)
      );
    } finally {
      setRuntimeBusy(false);
    }
  };

  const updateAppearance = (patch: Partial<AppearancePreferences>) => {
    const next = sanitizeAppearancePreferences({
      ...appearance,
      ...patch,
    });
    onAppearanceChange(next);
  };

  const setupVm = projectCodexSetupViewModel(codexReport, {
    busy: codexBusy,
    isBrowserPreview: !nativeAvailable,
  });
  const skillVm = projectCodexSkillSetupView(codexReport.skill_setup, {
    isBrowserPreview: !nativeAvailable,
    isChecking: codexReport.state === 'checking' || codexBusy,
  });

  const trayControlsDisabled =
    trayPrefs.availability !== 'available' || trayPrefs.saving;

  const swatchEntries = Object.entries(ACCENT_PALETTES) as Array<
    [AccentName, readonly [string, string]]
  >;

  return (
    <div className="tl-panel-scroll">
      <div className="tl-panel-inner tl-settings-stack">
        <div>
          <Heading size="4" weight="medium">
            Settings
          </Heading>
          <Text size="2" color="gray">
            Window behavior, neutral appearance, and local Codex integration.
          </Text>
        </div>

        {/* 1. Window close behavior (Tray preferences) */}
        <Card size="2" className="tl-settings-card" aria-label="Window close behavior">
          <Flex direction="column" gap="3">
            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <div>
                <Text as="div" size="2" weight="medium">
                  When closing the main window
                </Text>
                <Text as="div" size="1" color="gray">
                  Use Quit in the tray menu to fully exit when this window is hidden.
                </Text>
              </div>
              <div
                className="tl-segment-group"
                role="group"
                aria-label="When closing the main window"
              >
                {(
                  [
                    ['hide_to_tray', 'Hide in system tray'],
                    ['quit', 'Quit application'],
                  ] as const
                ).map(([value, label]) => {
                  const active = trayPrefs.closeBehavior === value;
                  return (
                    <button
                      key={value}
                      type="button"
                      className="tl-segment-btn"
                      data-active={active ? 'true' : 'false'}
                      aria-pressed={active}
                      disabled={trayControlsDisabled}
                      onClick={() => {
                        void handleSetCloseBehavior(value);
                      }}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            </Flex>

            {trayPrefs.availability === 'unavailable' ? (
              <Text size="1" color="gray">
                {trayPrefs.message ??
                  'System tray integration is not available in this environment.'}
              </Text>
            ) : trayPrefs.message ? (
              <Text size="1" color="gray">
                {trayPrefs.message}
              </Text>
            ) : null}

            {trayPrefs.error ? (
              <Callout.Root color="red" variant="surface" size="1" role="alert">
                <Callout.Icon>
                  <AlertCircle size={15} />
                </Callout.Icon>
                <Callout.Text>{trayPrefs.error}</Callout.Text>
              </Callout.Root>
            ) : null}
            {nativeAvailable &&
            (trayPrefs.availability === 'unavailable' || trayPrefs.error !== null) ? (
              <Button size="1" variant="soft" color="gray" onClick={() => void loadTrayPreferences()}>
                <RefreshCw size={13} aria-hidden="true" />
                Recheck close behavior
              </Button>
            ) : null}
          </Flex>
        </Card>

        {/* 2. Appearance preferences */}
        <Card size="2" className="tl-settings-card" aria-label="Appearance">
          <Flex direction="column" gap="3">
            <Flex align="center" gap="2">
              <Sparkles size={16} aria-hidden="true" />
              <Text size="2" weight="medium">
                Appearance
              </Text>
            </Flex>

            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <div>
                <Text as="div" size="2">
                  Theme
                </Text>
                <Text as="div" size="1" color="gray">
                  Neutral zinc palette in dark, light, or system mode.
                </Text>
              </div>
              <div className="tl-segment-group" role="group" aria-label="Theme">
                {(
                  [
                    ['system', 'System'],
                    ['dark', 'Dark'],
                    ['light', 'Light'],
                  ] as ReadonlyArray<readonly [ThemeMode, string]>
                ).map(([mode, label]) => {
                  const active = appearance.theme === mode;
                  return (
                    <button
                      key={mode}
                      type="button"
                      className="tl-segment-btn"
                      data-active={active ? 'true' : 'false'}
                      aria-pressed={active}
                      onClick={() => updateAppearance({ theme: mode })}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            </Flex>

            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <div>
                <Text as="div" size="2">
                  Surface
                </Text>
                <Text as="div" size="1" color="gray">
                  Solid neutral panels or subtle translucent surface.
                </Text>
              </div>
              <div
                className="tl-segment-group"
                role="group"
                aria-label="Surface"
              >
                {(
                  [
                    ['solid', 'Solid'],
                    ['glass', 'Translucent'],
                  ] as ReadonlyArray<readonly [SurfaceMode, string]>
                ).map(([surface, label]) => {
                  const active = appearance.surface === surface;
                  return (
                    <button
                      key={surface}
                      type="button"
                      className="tl-segment-btn"
                      data-active={active ? 'true' : 'false'}
                      aria-pressed={active}
                      onClick={() => updateAppearance({ surface })}
                    >
                      {label}
                    </button>
                  );
                })}
              </div>
            </Flex>

            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <label htmlFor="tl-opacity-slider" className="tl-field-label">
                <Text as="div" size="2">
                  Background opacity ({appearance.opacity}%)
                </Text>
                <Text as="div" size="1" color="gray">
                  Applies when translucent surface is active.
                </Text>
              </label>
              <input
                id="tl-opacity-slider"
                type="range"
                min={65}
                max={100}
                value={appearance.opacity}
                disabled={appearance.surface === 'solid'}
                aria-label="Background opacity"
                className="tl-range-input"
                onChange={(e) =>
                  updateAppearance({ opacity: Number(e.target.value) })
                }
              />
            </Flex>

            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <div>
                <Text as="div" size="2">
                  Accent color
                </Text>
                <Text as="div" size="1" color="gray">
                  Used only for active highlights and primary badges.
                </Text>
              </div>
              <div
                className="tl-swatch-row"
                role="group"
                aria-label="Accent color"
              >
                {swatchEntries.map(([name, [lightHex, darkHex]]) => {
                  const active = appearance.accent === name;
                  const label = name.charAt(0).toUpperCase() + name.slice(1);
                  return (
                    <button
                      key={name}
                      type="button"
                      className="tl-swatch-btn"
                      data-active={active ? 'true' : 'false'}
                      aria-label={label}
                      aria-pressed={active}
                      style={{
                        backgroundColor:
                          appearance.theme === 'light' ? lightHex : darkHex,
                      }}
                      onClick={() => updateAppearance({ accent: name })}
                    />
                  );
                })}
              </div>
            </Flex>
          </Flex>
        </Card>

        {/* 3. Codex Native Setup */}
        <Card
          size="2"
          className="tl-settings-card"
          aria-label="Codex native integration"
        >
          <Flex direction="column" gap="3">
            <Flex align="center" justify="between" gap="3" wrap="wrap">
              <Flex align="center" gap="2" wrap="wrap">
                <Text size="2" weight="medium">
                  Codex integration
                </Text>
                <Badge
                  size="1"
                  variant="soft"
                  color={mapSetupBadgeColor(setupVm.registrationBadgeTone)}
                >
                  {setupVm.registrationBadgeLabel}
                </Badge>
                <Badge
                  size="1"
                  variant="soft"
                  color={mapSetupBadgeColor(skillVm.badgeTone)}
                >
                  {skillVm.badgeLabel}
                </Badge>
              </Flex>

              <Flex align="center" gap="2" wrap="wrap">
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  disabled={!setupVm.canRecheck}
                  onClick={() => {
                    void handleRecheckCodex();
                  }}
                >
                  <RefreshCw size={14} aria-hidden="true" />
                  {codexBusy ? 'Checking…' : 'Recheck'}
                </Button>
                <Button
                  size="2"
                  variant="solid"
                  color="gray"
                  highContrast
                  disabled={!setupVm.canRetry}
                  onClick={() => {
                    void handleApplyCodex();
                  }}
                >
                  {codexReport.registered ? 'Retry setup' : 'Apply setup'}
                </Button>
              </Flex>
            </Flex>

            <Text size="2" color="gray">
              {setupVm.summaryMessage}
            </Text>

            {setupVm.actionHint ? (
              <Text size="1" color="amber">
                {setupVm.actionHint}
              </Text>
            ) : null}

            {setupVm.restartNotice ? (
              <Callout.Root color="amber" variant="surface" size="1">
                <Callout.Icon>
                  <AlertCircle size={15} />
                </Callout.Icon>
                <Callout.Text>{setupVm.restartNotice}</Callout.Text>
              </Callout.Root>
            ) : null}

            <details className="tl-inline-disclosure">
              <summary className="tl-inline-summary">
                <ChevronRight
                  size={14}
                  className="tl-disclosure-chevron"
                  aria-hidden="true"
                />
                <span>Setup & skill paths</span>
              </summary>
              <dl className="tl-kv-grid">
                <dt>Config path</dt>
                <dd>{setupVm.configPathLabel}</dd>
                <dt>Pairing policy</dt>
                <dd>{setupVm.pairingBadgeLabel}</dd>
                <dt>Skill status</dt>
                <dd>{skillVm.message}</dd>
                <dt>Skill path</dt>
                <dd>{skillVm.pathLabel}</dd>
                {skillVm.invocationHint ? (
                  <>
                    <dt>Usage</dt>
                    <dd>{skillVm.invocationHint}</dd>
                  </>
                ) : null}
              </dl>
            </details>
          </Flex>
        </Card>

        {/* 4. Collapsed section: Linked observation sources */}
        <details
          className="tl-disclosure-card"
          aria-label="Linked observation sources"
        >
          <summary className="tl-disclosure-card-summary">
            <Flex align="center" gap="2">
              <ChevronRight
                size={16}
                className="tl-disclosure-chevron"
                aria-hidden="true"
              />
              <Link2 size={16} aria-hidden="true" />
              <Text size="2" weight="medium">
                Linked observation sources ({sources.length})
              </Text>
            </Flex>
            <Badge size="1" variant="soft" color="gray">
              Read-only bridge
            </Badge>
          </summary>

          <div className="tl-disclosure-card-body">
            <Flex align="center" justify="between" gap="2" wrap="wrap">
              <Text size="1" color="gray">
                Explicitly linked local observation contexts. Nothing is scanned
                or linked automatically.
              </Text>
              <IconButton
                size="2"
                variant="soft"
                color="gray"
                aria-label="Refresh linked observation sources"
                disabled={sourcesLoading}
                onClick={() => {
                  void refreshSources();
                }}
              >
                <RefreshCw size={15} aria-hidden="true" />
              </IconButton>
            </Flex>

            {sourcesError ? (
              <Callout.Root color="red" variant="surface" size="1" role="alert">
                <Callout.Icon>
                  <AlertCircle size={15} />
                </Callout.Icon>
                <Callout.Text>{sourcesError}</Callout.Text>
              </Callout.Root>
            ) : null}

            {linkNotice ? (
              <Callout.Root
                color="green"
                variant="surface"
                size="1"
                role="status"
              >
                <Callout.Icon>
                  <CheckCircle2 size={15} />
                </Callout.Icon>
                <Callout.Text>{linkNotice}</Callout.Text>
              </Callout.Root>
            ) : null}

            {linkError ? (
              <Callout.Root color="red" variant="surface" size="1" role="alert">
                <Callout.Icon>
                  <AlertCircle size={15} />
                </Callout.Icon>
                <Callout.Text>{linkError}</Callout.Text>
              </Callout.Root>
            ) : null}

            {sources.length === 0 ? (
              <Text size="2" color="gray">
                {sourcesLoading
                  ? 'Loading linked observation sources…'
                  : 'No linked observation sources yet. Enter a local context path below to link one.'}
              </Text>
            ) : (
              <div className="tl-source-list">
                {sources.map((src) => (
                  <div key={src.id} className="tl-source-row">
                    <Flex align="center" justify="between" gap="2" wrap="wrap">
                      <Text size="2" weight="medium">
                        {src.label.trim() || 'Unnamed source'}
                      </Text>
                      <Badge
                        size="1"
                        variant="soft"
                        color={
                          src.availability === 'available' ? 'green' : 'amber'
                        }
                      >
                        {src.availability === 'available'
                          ? 'Available'
                          : 'Unavailable'}
                      </Badge>
                    </Flex>
                    {src.project_path ? (
                      <Text size="1" color="gray">
                        {src.project_path}
                      </Text>
                    ) : null}
                    {src.message ? (
                      <Text size="1" color="amber">
                        {src.message}
                      </Text>
                    ) : null}
                    <details className="tl-inline-disclosure">
                      <summary className="tl-inline-summary">
                        <ChevronRight
                          size={13}
                          className="tl-disclosure-chevron"
                          aria-hidden="true"
                        />
                        <span>Source details</span>
                      </summary>
                      <dl className="tl-kv-grid">
                        <dt>Project path</dt>
                        <dd>{src.project_path?.trim() || 'Not recorded'}</dd>
                        <dt>Source ID</dt>
                        <dd>{src.id}</dd>
                      </dl>
                    </details>
                  </div>
                ))}
              </div>
            )}

            <div className="tl-field-block">
              <label
                htmlFor="tl-observation-context-input"
                className="tl-field-label"
              >
                <Text size="1" weight="medium">
                  Observation context path
                </Text>
              </label>
              <Flex gap="2" wrap="wrap">
                <TextField.Root
                  id="tl-observation-context-input"
                  style={{ flex: 1, minWidth: 200 }}
                  placeholder="/path/to/local/observation/context"
                  value={contextPathDraft}
                  disabled={linkingContext}
                  aria-label="Observation context path"
                  onChange={(e) => setContextPathDraft(e.target.value)}
                />
                <Button
                  size="2"
                  variant="solid"
                  color="gray"
                  highContrast
                  disabled={
                    linkingContext || contextPathDraft.trim().length === 0
                  }
                  onClick={() => {
                    void handleLinkContext();
                  }}
                >
                  {linkingContext ? 'Linking…' : 'Link context'}
                </Button>
              </Flex>
            </div>
          </div>
        </details>

        {/* 6. Collapsed section: Advanced runtime attachment & diagnostics */}
        <details
          className="tl-disclosure-card"
          aria-label="Advanced runtime and diagnostics"
        >
          <summary className="tl-disclosure-card-summary">
            <Flex align="center" gap="2">
              <ChevronRight
                size={16}
                className="tl-disclosure-chevron"
                aria-hidden="true"
              />
              <Shield size={16} aria-hidden="true" />
              <Text size="2" weight="medium">
                Advanced runtime & diagnostics
              </Text>
            </Flex>
            {diagnostics.length > 0 ? (
              <Badge size="1" variant="soft" color="amber">
                {diagnostics.length} diagnostic
                {diagnostics.length === 1 ? '' : 's'}
              </Badge>
            ) : (
              <Badge size="1" variant="soft" color="gray">
                Optional
              </Badge>
            )}
          </summary>

          <div className="tl-disclosure-card-body">
            {runtimeError ? (
              <Callout.Root color="red" variant="surface" size="1" role="alert">
                <Callout.Icon>
                  <AlertCircle size={15} />
                </Callout.Icon>
                <Callout.Text>{runtimeError}</Callout.Text>
              </Callout.Root>
            ) : null}

            {runtimeNotice ? (
              <Callout.Root
                color="green"
                variant="surface"
                size="1"
                role="status"
              >
                <Callout.Icon>
                  <CheckCircle2 size={15} />
                </Callout.Icon>
                <Callout.Text>{runtimeNotice}</Callout.Text>
              </Callout.Root>
            ) : null}

            <div className="tl-field-block">
              <label
                htmlFor="tl-runtime-state-dir-input"
                className="tl-field-label"
              >
                <Flex align="center" gap="1">
                  <FolderOpen size={14} aria-hidden="true" />
                  <Text size="1" weight="medium">
                    Attach runtime folder (read-only observer)
                  </Text>
                </Flex>
              </label>
              <Flex gap="2" wrap="wrap">
                <TextField.Root
                  id="tl-runtime-state-dir-input"
                  style={{ flex: 1, minWidth: 200 }}
                  placeholder="/path/to/threadlane/state-dir"
                  value={stateDirDraft}
                  disabled={runtimeBusy}
                  aria-label="Runtime state directory path"
                  onChange={(e) => {
                    const nextVal = e.target.value;
                    setStateDirDraft(nextVal);
                    if (appearance.rememberStateDir) {
                      updateAppearance({ savedStateDir: nextVal });
                    }
                  }}
                />
                <Button
                  size="2"
                  variant="soft"
                  color="gray"
                  disabled={runtimeBusy || stateDirDraft.trim().length === 0}
                  onClick={() => {
                    void handleAttachRuntime();
                  }}
                >
                  {runtimeBusy ? 'Attaching…' : 'Attach'}
                </Button>
              </Flex>

              <Flex align="center" justify="between" gap="3" mt="1">
                <Text size="1" color="gray">
                  Remember folder path on this device
                </Text>
                <Switch
                  checked={appearance.rememberStateDir}
                  aria-label="Remember folder path on this device"
                  onCheckedChange={(checked) =>
                    updateAppearance({
                      rememberStateDir: checked,
                      savedStateDir: checked ? stateDirDraft : '',
                    })
                  }
                />
              </Flex>
            </div>

            {runtimeSnapshot?.demo_controls.available ? (
              <div className="tl-field-block">
                <Text size="1" weight="medium">
                  Local zero-cost demo
                </Text>
                <Flex gap="2" wrap="wrap">
                  <Button
                    size="2"
                    variant="soft"
                    color="gray"
                    disabled={
                      runtimeBusy || runtimeSnapshot.demo_controls.can_cancel
                    }
                    onClick={() => {
                      void handleDemoAction('start');
                    }}
                  >
                    <Play size={14} aria-hidden="true" />
                    {runtimeSnapshot.demo_controls.can_cancel
                      ? 'Demo active'
                      : 'Run local demo'}
                  </Button>
                  {runtimeSnapshot.demo_controls.can_answer ? (
                    <Button
                      size="2"
                      variant="soft"
                      color="gray"
                      disabled={runtimeBusy}
                      onClick={() => {
                        void handleDemoAction('answer');
                      }}
                    >
                      Continue demo
                    </Button>
                  ) : null}
                  {runtimeSnapshot.demo_controls.can_cancel ? (
                    <Button
                      size="2"
                      variant="soft"
                      color="red"
                      disabled={runtimeBusy}
                      onClick={() => {
                        void handleDemoAction('cancel');
                      }}
                    >
                      Cancel demo
                    </Button>
                  ) : null}
                </Flex>
              </div>
            ) : null}

            {warnings.length > 0 ? (
              <div className="tl-field-block">
                <Text size="1" weight="medium">
                  Observation warnings ({warnings.length})
                </Text>
                <ul className="tl-plain-list">
                  {warnings.map((w, idx) => (
                    <li key={idx}>
                      <Text size="1" color="gray">
                        {w}
                      </Text>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}

            {diagnostics.length > 0 ? (
              <div className="tl-field-block">
                <Text size="1" weight="medium">
                  Observation diagnostics ({diagnostics.length})
                </Text>
                <div className="tl-source-list">
                  {diagnostics.map((diag) => (
                    <div key={diag.key} className="tl-source-row">
                      <Flex
                        align="center"
                        justify="between"
                        gap="2"
                        wrap="wrap"
                      >
                        <Text size="2" weight="medium">
                          {diag.title}
                        </Text>
                        <Badge
                          size="1"
                          variant="soft"
                          color={diag.severity === 'warning' ? 'amber' : 'gray'}
                        >
                          {diag.area}
                        </Badge>
                      </Flex>
                      <Text size="1" color="gray">
                        {diag.impact}
                      </Text>
                      <Text size="1" color="gray">
                        Next step: {diag.nextStep}
                      </Text>
                    </div>
                  ))}
                </div>
              </div>
            ) : null}
          </div>
        </details>
      </div>
    </div>
  );
}
