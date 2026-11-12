import '@radix-ui/themes/styles.css';
import './pages/tools.css';
import './app.css';

import { Button, Dialog, IconButton, Theme } from '@radix-ui/themes';
import { invoke } from '@tauri-apps/api/core';
import {
  AlertCircle,
  Archive,
  Blocks,
  ChevronDown,
  ChevronRight,
  FolderGit2,
  FolderOpen,
  Inbox,
  Loader2,
  Menu,
  Minus,
  PanelLeftClose,
  PanelLeftOpen,
  RefreshCw,
  RotateCcw,
  Settings2,
  SlidersHorizontal,
  X,
} from 'lucide-react';
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
} from 'react';
import { createRoot, type Root } from 'react-dom/client';

import threadlaneIconUrl from '../src-tauri/icons/icon.svg';
import {
  OPEN_CODEX_CHAT_COMMAND,
  type CodexChatTarget,
  type WorkspaceChatIdentity,
  type WorkspaceChatLifecycle,
  type WorkspaceChatSurfaceTab,
} from './app-contract.ts';
import {
  DesktopAppCoordinator,
  OBSERVATION_POLL_INTERVAL_MS,
  type DesktopCoordinatorState,
} from './app-data.ts';
import {
  workspaceChatIdentitiesEqual,
  type StatusDotTone,
  type WorkspaceChatPreferenceStorage,
  type WorkspaceChatSidebarItem,
  type WorkspaceChatSummary,
  type WorkspaceNativeSample,
  type WorkspaceProductionProjection,
  type WorkspaceProjectGroupView,
} from './app-model.ts';
import { ProfilesPanel } from './app-profiles.tsx';
import {
  loadSavedAppearance,
  saveAppearance,
  SettingsPanel,
} from './app-settings.tsx';
import { ToolsPanel } from './app-tools.tsx';
import {
  hasTauriRuntime,
  ipcDesktopToolStatus,
  ipcDesktopWindowAction,
} from './ipc.ts';
import { ipcWorkspaceSnapshot } from './observation/ipc.ts';
import {
  desktop_registry_snapshot,
  desktop_tool_versions,
} from './registry/ipc.ts';
import type { RegistrySnapshot } from './registry/types.ts';
import type { AccentName, AppearancePreferences } from './types.ts';
import { WorkspaceChatPanels } from './workspace-panels.tsx';

export type AppSection = 'projects' | 'tools' | 'profiles' | 'settings';

type ShellSurfaceStyle = CSSProperties & {
  '--tl-surface-opacity': string;
};

const COMPACT_BREAKPOINT_PX = 760;
const VISIBLE_UI_AGE_CLOCK_INTERVAL_MS = 1_000;

function isDocumentVisible(): boolean {
  return typeof document === 'undefined' || !document.hidden;
}

const COMPACT_NAV_SECTIONS: ReadonlyArray<readonly [AppSection, string]> = [
  ['projects', 'Projects'],
  ['tools', 'Tools'],
  ['profiles', 'Profiles'],
  ['settings', 'Settings'],
];

function getBrowserStorage(): WorkspaceChatPreferenceStorage | undefined {
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      return window.localStorage;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function mapAccentToRadixColor(
  accent: AccentName
): 'cyan' | 'blue' | 'iris' | 'amber' {
  switch (accent) {
    case 'sage':
      return 'cyan';
    case 'blue':
      return 'blue';
    case 'violet':
      return 'iris';
    case 'sand':
      return 'amber';
  }
}

function StatusDots({
  primary,
  secondary,
}: {
  primary: StatusDotTone;
  secondary?: StatusDotTone | null;
}) {
  return (
    <span className="tl-status-dots" aria-hidden="true">
      <span className="tl-status-dot" data-tone={primary} />
      {secondary ? (
        <span className="tl-status-dot" data-tone={secondary} />
      ) : null}
    </span>
  );
}

interface SidebarContentProps {
  activeSection: AppSection;
  collapsed: boolean;
  workspace: WorkspaceProductionProjection;
  visibleProjects: readonly WorkspaceProjectGroupView[];
  selectedChatKey: string | null;
  collapsedProjects: ReadonlySet<string>;
  onSelectSection: (section: AppSection) => void;
  onSelectLifecycle: (lifecycle: WorkspaceChatLifecycle) => void;
  onSelectChat: (chatKey: string) => void;
  onToggleProject: (projectKey: string) => void;
  onSetChatArchived: (
    identity: WorkspaceChatIdentity,
    archived: boolean
  ) => void;
}

function SidebarContent({
  activeSection,
  collapsed,
  workspace,
  visibleProjects,
  selectedChatKey,
  collapsedProjects,
  onSelectSection,
  onSelectLifecycle,
  onSelectChat,
  onToggleProject,
  onSetChatArchived,
}: SidebarContentProps) {
  const navItems: ReadonlyArray<{
    id: AppSection;
    label: string;
    icon: typeof FolderOpen;
  }> = [
    { id: 'projects', label: 'Projects', icon: FolderOpen },
    { id: 'tools', label: 'Tools', icon: Blocks },
    { id: 'profiles', label: 'Profiles', icon: SlidersHorizontal },
    { id: 'settings', label: 'Settings', icon: Settings2 },
  ];

  const totalWorkingCount =
    workspace.totalWorkingCoworkerCount + workspace.totalActiveWorkerCount;

  return (
    <div
      className="tl-sidebar-inner"
      data-collapsed={collapsed ? 'true' : 'false'}
    >
      <nav className="tl-nav-list" aria-label="Main navigation">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isCurrent = activeSection === item.id;
          return (
            <div key={item.id} className="tl-nav-group">
              <button
                type="button"
                className="tl-nav-btn"
                data-active={isCurrent ? 'true' : 'false'}
                aria-current={isCurrent ? 'page' : undefined}
                aria-label={item.label}
                onClick={() => onSelectSection(item.id)}
              >
                <Icon size={17} aria-hidden="true" />
                {!collapsed ? (
                  <>
                    <span className="tl-nav-btn-label">{item.label}</span>
                    {item.id === 'projects' ? (
                      <span className="tl-nav-badges">
                        {totalWorkingCount > 0 ? (
                          <span
                            className="tl-mini-pill tl-nav-count"
                            data-tone="working"
                            aria-label={`${totalWorkingCount} working`}
                          >
                            <span
                              className="tl-status-dot"
                              data-tone="working"
                              aria-hidden="true"
                            />
                            <span aria-hidden="true">{totalWorkingCount}</span>
                          </span>
                        ) : null}
                        {workspace.totalNeedsHumanCount > 0 ? (
                          <span
                            className="tl-mini-pill tl-nav-count"
                            data-tone="attention"
                            aria-label={
                              workspace.totalNeedsHumanCount === 1
                                ? '1 needs attention'
                                : `${workspace.totalNeedsHumanCount} need attention`
                            }
                          >
                            <span
                              className="tl-status-dot"
                              data-tone="attention"
                              aria-hidden="true"
                            />
                            <span aria-hidden="true">
                              {workspace.totalNeedsHumanCount}
                            </span>
                          </span>
                        ) : null}
                      </span>
                    ) : null}
                  </>
                ) : null}
              </button>

              {item.id === 'projects' && !collapsed ? (
                <div
                  className="tl-projects-subnav"
                  role="group"
                  aria-label="Projects views"
                >
                  <button
                    type="button"
                    className="tl-projects-subnav-btn"
                    data-active={
                      activeSection === 'projects' &&
                      workspace.lifecycleFilter === 'active'
                        ? 'true'
                        : 'false'
                    }
                    aria-current={
                      activeSection === 'projects' &&
                      workspace.lifecycleFilter === 'active'
                        ? 'page'
                        : undefined
                    }
                    onClick={() => onSelectLifecycle('active')}
                  >
                    <Inbox size={14} aria-hidden="true" />
                    <span className="tl-projects-subnav-label">Active</span>
                    <span className="tl-projects-subnav-badges">
                      {workspace.activeNeedsHumanCount > 0 ? (
                        <span
                          className="tl-status-dot"
                          data-tone="attention"
                          aria-label={`${workspace.activeNeedsHumanCount} active attention item(s)`}
                        />
                      ) : null}
                      <span className="tl-projects-subnav-count">
                        {workspace.activeChatCount}
                      </span>
                    </span>
                  </button>

                  <button
                    type="button"
                    className="tl-projects-subnav-btn"
                    data-active={
                      activeSection === 'projects' &&
                      workspace.lifecycleFilter === 'archived'
                        ? 'true'
                        : 'false'
                    }
                    aria-current={
                      activeSection === 'projects' &&
                      workspace.lifecycleFilter === 'archived'
                        ? 'page'
                        : undefined
                    }
                    onClick={() => onSelectLifecycle('archived')}
                  >
                    <Archive size={14} aria-hidden="true" />
                    <span className="tl-projects-subnav-label">Archive</span>
                    <span className="tl-projects-subnav-badges">
                      {workspace.archivedBlockerCount > 0 ? (
                        <span
                          className="tl-status-dot"
                          data-tone="attention"
                          aria-label={`${workspace.archivedBlockerCount} archived attention item(s)`}
                        />
                      ) : null}
                      <span className="tl-projects-subnav-count">
                        {workspace.archivedChatCount}
                      </span>
                    </span>
                  </button>
                </div>
              ) : null}
            </div>
          );
        })}
      </nav>

      {!collapsed ? (
        <div className="tl-sidebar-projects" aria-label="Projects and chats">
          <div className="tl-sidebar-section-bar">
            <span className="tl-sidebar-kicker">
              {workspace.lifecycleFilter === 'archived'
                ? 'Projects / Archive'
                : 'Projects / Active'}
            </span>
          </div>

          <div className="tl-project-tree">
            {workspace.status === 'loading' ? (
              <div className="tl-sidebar-empty" role="status">
                <Loader2 size={15} className="tl-spin" aria-hidden="true" />
                <span>Reading workspace…</span>
              </div>
            ) : workspace.status === 'unavailable' ? (
              <div className="tl-sidebar-empty" role="status">
                <AlertCircle size={15} aria-hidden="true" />
                <span>Observation unavailable</span>
              </div>
            ) : visibleProjects.length === 0 ? (
              <div className="tl-sidebar-empty" role="status">
                <span>
                  {workspace.emptyMessage ??
                    (workspace.lifecycleFilter === 'archived'
                      ? 'No archived chats in Projects/Archive.'
                      : 'No active chats in Projects/Active.')}
                </span>
              </div>
            ) : (
              visibleProjects.map((project: WorkspaceProjectGroupView) => {
                const isProjectCollapsed = collapsedProjects.has(
                  project.projectKey
                );
                const projectWorkingCount =
                  project.workingCoworkerCount + project.activeWorkerCount;
                return (
                  <section
                    key={project.projectKey}
                    className="tl-project-group"
                    aria-label={project.label}
                  >
                    <button
                      type="button"
                      className="tl-project-header"
                      aria-expanded={!isProjectCollapsed}
                      onClick={() => onToggleProject(project.projectKey)}
                    >
                      {isProjectCollapsed ? (
                        <ChevronRight size={14} aria-hidden="true" />
                      ) : (
                        <ChevronDown size={14} aria-hidden="true" />
                      )}
                      <FolderGit2 size={14} aria-hidden="true" />
                      <span className="tl-project-name">{project.label}</span>
                      <span className="tl-project-counts">
                        {projectWorkingCount > 0 ? (
                          <span
                            className="tl-mini-pill"
                            data-tone="working"
                            aria-label={`${projectWorkingCount} working`}
                          >
                            {projectWorkingCount}
                          </span>
                        ) : null}
                        {project.hasNeedsHuman ? (
                          <span
                            className="tl-mini-pill"
                            data-tone="attention"
                            aria-label={`${project.needsHumanCount} need attention`}
                          >
                            {project.needsHumanCount}
                          </span>
                        ) : null}
                      </span>
                    </button>

                    {!isProjectCollapsed ? (
                      <div className="tl-chat-list">
                        {project.chats.map((chat: WorkspaceChatSidebarItem) => {
                          const selected =
                            activeSection === 'projects' &&
                            selectedChatKey === chat.chatKey;
                          const chatIsWorking =
                            (chat.nativeState.availability === 'available' &&
                              chat.nativeState.activity === 'working') ||
                            chat.workerSummary.hasActiveWorkers ||
                            chat.jobSummary.activeJobCount > 0;
                          const configuredModelLabel =
                            chat.configuredSelection.evidence ===
                            'verified_configuration'
                              ? (chat.configuredSelection.friendlyModelLabel ??
                                chat.configuredSelection.displayLabel)
                              : null;

                          return (
                            <div
                              key={chat.chatKey}
                              className="tl-chat-item-row"
                              data-selected={selected ? 'true' : 'false'}
                            >
                              <button
                                type="button"
                                className="tl-chat-item"
                                data-selected={selected ? 'true' : 'false'}
                                aria-current={selected ? 'true' : undefined}
                                onClick={() => onSelectChat(chat.chatKey)}
                              >
                                <div className="tl-chat-item-top">
                                  <StatusDots
                                    primary={chat.primaryDotTone}
                                    secondary={
                                      chat.hasCurrentBlocker && chatIsWorking
                                        ? 'working'
                                        : null
                                    }
                                  />
                                  <span className="tl-chat-item-label">
                                    {chat.threadLabel}
                                  </span>
                                  <span className="tl-chat-item-age">
                                    {chat.ageLabel}
                                  </span>
                                </div>

                                <div className="tl-chat-item-meta">
                                  {chat.hasCurrentBlocker &&
                                  chat.needsHumanBadgeLabel ? (
                                    <span
                                      className="tl-mini-pill"
                                      data-tone="attention"
                                    >
                                      {chat.needsHumanBadgeLabel}
                                    </span>
                                  ) : null}
                                  <span className="tl-chat-status-text">
                                    {chat.statusSummaryLabel}
                                  </span>
                                  {configuredModelLabel ? (
                                    <span className="tl-chat-model-text">
                                      {configuredModelLabel}
                                    </span>
                                  ) : null}
                                </div>
                              </button>

                              {chat.canArchive ? (
                                <button
                                  type="button"
                                  className="tl-chat-archive-btn"
                                  aria-label={`Archive ${chat.threadLabel}`}
                                  title="Archive chat"
                                  onClick={() =>
                                    onSetChatArchived(chat.identity, true)
                                  }
                                >
                                  <Archive size={12} aria-hidden="true" />
                                  <span>Archive</span>
                                </button>
                              ) : chat.canRestore ? (
                                <button
                                  type="button"
                                  className="tl-chat-archive-btn"
                                  aria-label={`Restore ${chat.threadLabel}`}
                                  title="Restore chat"
                                  onClick={() =>
                                    onSetChatArchived(chat.identity, false)
                                  }
                                >
                                  <RotateCcw size={12} aria-hidden="true" />
                                  <span>Restore</span>
                                </button>
                              ) : null}
                            </div>
                          );
                        })}
                      </div>
                    ) : null}
                  </section>
                );
              })
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function ThreadlaneDesktopShell() {
  const [appearance, setAppearance] = useState<AppearancePreferences>(() =>
    loadSavedAppearance()
  );
  const [systemPrefersDark, setSystemPrefersDark] = useState<boolean>(() => {
    if (
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function'
    ) {
      return window.matchMedia('(prefers-color-scheme: dark)').matches;
    }
    return true;
  });

  const [compact, setCompact] = useState<boolean>(() => {
    if (typeof window !== 'undefined') {
      return window.innerWidth <= COMPACT_BREAKPOINT_PX;
    }
    return false;
  });
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mobileDrawerOpen, setMobileDrawerOpen] = useState(false);
  const navigationTriggerRef = useRef<HTMLButtonElement | null>(null);
  const [activeSection, setActiveSection] = useState<AppSection>('projects');
  const [archiveErrorDismissed, setArchiveErrorDismissed] = useState(false);
  const [collapsedProjects, setCollapsedProjects] = useState<Set<string>>(
    () => new Set()
  );

  const [currentNowMs, setCurrentNowMs] = useState<number>(() => Date.now());
  const lastSyncedSnapshotRef =
    useRef<DesktopCoordinatorState['workspaceSnapshot']>(null);

  const coordinatorRef = useRef<DesktopAppCoordinator | null>(null);
  if (coordinatorRef.current === null) {
    coordinatorRef.current = new DesktopAppCoordinator(
      {
        fetchWorkspaceSnapshot: (req) => ipcWorkspaceSnapshot(req),
        fetchRegistrySnapshot: () => desktop_registry_snapshot(),
        fetchToolVersions: () => desktop_tool_versions(),
        fetchToolDiscovery: () => ipcDesktopToolStatus(),
        openCodexChat: async (target: CodexChatTarget) => {
          if (!hasTauriRuntime()) {
            throw new Error(
              'Opening a conversation in Codex requires the native desktop runtime.'
            );
          }
          const args = { request: target };
          await invoke<void>(OPEN_CODEX_CHAT_COMMAND, { ...args });
        },
        preferenceStorage: getBrowserStorage(),
      },
      (nextState) => {
        const snapshotChanged =
          nextState.workspaceSnapshot !== lastSyncedSnapshotRef.current;
        lastSyncedSnapshotRef.current = nextState.workspaceSnapshot;
        setCoordState(nextState);
        if (isDocumentVisible() || snapshotChanged) {
          setCurrentNowMs(Date.now());
        }
      },
      {
        visible: isDocumentVisible(),
        focused:
          typeof document === 'undefined' ||
          typeof document.hasFocus !== 'function'
            ? true
            : document.hasFocus(),
      }
    );
  }

  const [coordState, setCoordState] = useState<DesktopCoordinatorState>(() =>
    coordinatorRef.current!.getState()
  );

  useEffect(() => {
    if (coordState.archiveError !== null) {
      setArchiveErrorDismissed(false);
    }
  }, [coordState.archiveError]);

  // Listen to OS color scheme and viewport resize
  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }
    const media =
      typeof window.matchMedia === 'function'
        ? window.matchMedia('(prefers-color-scheme: dark)')
        : null;
    const onMediaChange = (e: MediaQueryListEvent) => {
      setSystemPrefersDark(e.matches);
    };
    media?.addEventListener('change', onMediaChange);

    const onResize = () => {
      const nextCompact = window.innerWidth <= COMPACT_BREAKPOINT_PX;
      setCompact(nextCompact);
      if (!nextCompact) {
        setMobileDrawerOpen(false);
      }
    };
    window.addEventListener('resize', onResize);

    return () => {
      media?.removeEventListener('change', onMediaChange);
      window.removeEventListener('resize', onResize);
    };
  }, []);

  // Initialize coordinator + visibility/focus polling lifecycle
  useEffect(() => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    void coordinator.initialize();

    const syncVisibility = () => {
      const visible = typeof document === 'undefined' ? true : !document.hidden;
      const focused =
        typeof document === 'undefined' ||
        typeof document.hasFocus !== 'function'
          ? true
          : document.hasFocus();
      coordinator.setVisibility(visible, focused);
    };

    const onVisibilityChange = () => {
      syncVisibility();
    };
    const onFocus = () => {
      coordinator.setVisibility(
        typeof document === 'undefined' ? true : !document.hidden,
        true
      );
    };
    const onBlur = () => {
      coordinator.setVisibility(
        typeof document === 'undefined' ? true : !document.hidden,
        false
      );
    };

    document.addEventListener('visibilitychange', onVisibilityChange);
    window.addEventListener('focus', onFocus);
    window.addEventListener('blur', onBlur);

    const intervalId = window.setInterval(() => {
      syncVisibility();
      if (coordinator.canPoll() && !coordinator.isObservationInFlight()) {
        void coordinator.pollTick();
      }
    }, OBSERVATION_POLL_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
      document.removeEventListener('visibilitychange', onVisibilityChange);
      window.removeEventListener('focus', onFocus);
      window.removeEventListener('blur', onBlur);
      coordinator.dispose();
    };
  }, []);

  // Owned visible-UI age clock: advances root referenceNowMs independently of
  // backend poll completion, suppresses background ticks while hidden, and
  // synchronizes immediately when visible again.
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

    const onVisibilityChange = () => {
      if (isDocumentVisible()) {
        setCurrentNowMs(Date.now());
        startAgeClockIfVisible();
      } else {
        stopAgeClock();
      }
    };

    startAgeClockIfVisible();
    if (typeof document !== 'undefined') {
      document.addEventListener('visibilitychange', onVisibilityChange);
    }

    return () => {
      stopAgeClock();
      if (typeof document !== 'undefined') {
        document.removeEventListener('visibilitychange', onVisibilityChange);
      }
    };
  }, []);

  const resolvedThemeAppearance: 'dark' | 'light' =
    appearance.theme === 'system'
      ? systemPrefersDark
        ? 'dark'
        : 'light'
      : appearance.theme;

  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.documentElement.style.colorScheme = resolvedThemeAppearance;
      document.documentElement.dataset.theme = resolvedThemeAppearance;
    }
  }, [resolvedThemeAppearance]);

  const workspace: WorkspaceProductionProjection = useMemo(
    () => coordinatorRef.current!.projectWorkspace(currentNowMs),
    [
      coordState.workspaceSnapshot,
      coordState.observationLoading,
      coordState.observationError,
      coordState.selectedChatKey,
      coordState.selectedTab,
      coordState.workspaceLifecycle,
      coordState.archivePreferences,
      currentNowMs,
    ]
  );

  const visibleProjects: WorkspaceProjectGroupView[] =
    workspace.lifecycleFilter === 'archived'
      ? workspace.archivedProjects
      : workspace.activeProjects;

  // Keep coordinator's selectedChatKey synchronized with the active lifecycle view
  useEffect(() => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    if (
      workspace.selectedChat !== null &&
      workspace.selectedChat.lifecycle !== workspace.lifecycleFilter
    ) {
      const fallbackKey = visibleProjects[0]?.chats[0]?.chatKey ?? null;
      if (coordState.selectedChatKey !== fallbackKey) {
        coordinator.selectChat(fallbackKey);
      }
      return;
    }
    if (
      coordState.selectedChatKey === null &&
      workspace.selectedChatKey !== null
    ) {
      coordinator.selectChat(workspace.selectedChatKey);
    }
  }, [
    coordState.selectedChatKey,
    visibleProjects,
    workspace.lifecycleFilter,
    workspace.selectedChat,
    workspace.selectedChatKey,
  ]);

  const activeSelectedChat =
    workspace.selectedChat !== null &&
    workspace.selectedChat.lifecycle === workspace.lifecycleFilter
      ? workspace.selectedChat
      : null;
  const selectedIdentity: WorkspaceChatIdentity | null =
    activeSelectedChat?.identity ?? null;

  const selectedChatSummary: WorkspaceChatSummary | null = useMemo(() => {
    if (!selectedIdentity || !coordState.workspaceSnapshot) {
      return null;
    }
    return (
      coordState.workspaceSnapshot.chats.find((entry) =>
        workspaceChatIdentitiesEqual(entry.identity, selectedIdentity)
      ) ?? null
    );
  }, [coordState.workspaceSnapshot, selectedIdentity]);

  const selectedNativeSample: WorkspaceNativeSample | null = useMemo(() => {
    if (!selectedIdentity || !coordState.workspaceSnapshot) {
      return null;
    }
    return (
      coordState.workspaceSnapshot.native_samples.find((sample) =>
        workspaceChatIdentitiesEqual(sample.chat_identity, selectedIdentity)
      ) ?? null
    );
  }, [coordState.workspaceSnapshot, selectedIdentity]);

  const handleAppearanceChange = useCallback(
    (next: AppearancePreferences) => {
      setAppearance(next);
      saveAppearance(next);
    },
    []
  );

  const handleSelectSection = useCallback((section: AppSection) => {
    setActiveSection(section);
    setMobileDrawerOpen(false);
  }, []);

  const handleSelectLifecycle = useCallback(
    (lifecycle: WorkspaceChatLifecycle) => {
      coordinatorRef.current?.setWorkspaceLifecycle(lifecycle);
      setActiveSection('projects');
    },
    []
  );

  const handleSelectChat = useCallback((chatKey: string) => {
    coordinatorRef.current?.selectChat(chatKey);
    setActiveSection('projects');
    setMobileDrawerOpen(false);
  }, []);

  const handleToggleProject = useCallback((projectKey: string) => {
    setCollapsedProjects((prev) => {
      const next = new Set(prev);
      if (next.has(projectKey)) {
        next.delete(projectKey);
      } else {
        next.add(projectKey);
      }
      return next;
    });
  }, []);

  const handleSetChatArchived = useCallback(
    (identity: WorkspaceChatIdentity, archived: boolean) => {
      setArchiveErrorDismissed(false);
      coordinatorRef.current?.setChatArchived(identity, archived);
    },
    []
  );

  const handleTabChange = useCallback((tab: WorkspaceChatSurfaceTab) => {
    coordinatorRef.current?.setWorkspaceTab(tab);
  }, []);

  const handleWorkspaceRefresh = useCallback(() => {
    void coordinatorRef.current?.refreshObservation();
  }, []);

  const handleManualRefresh = useCallback(() => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    void coordinator.refreshObservation();
    if (activeSection === 'tools' || activeSection === 'profiles') {
      void coordinator.refreshRegistry({ force: true });
    }
  }, [activeSection]);

  const handleOpenCodex = useCallback((target: CodexChatTarget) => {
    void coordinatorRef.current?.openCodexChat(target);
  }, []);

  const handleProfileRegistryChanged = useCallback(
    (snapshot: RegistrySnapshot) => {
      coordinatorRef.current?.applyExternalRegistrySnapshot(snapshot);
    },
    []
  );

  const handleOpenToolsFromProfiles = useCallback(() => {
    setActiveSection('tools');
  }, []);

  const handleOpenProfilesFromTools = useCallback(() => {
    setActiveSection('profiles');
  }, []);

  const effectiveOpacity =
    appearance.surface === 'solid' ? 100 : appearance.opacity;
  const shellStyle: ShellSurfaceStyle = {
    '--tl-surface-opacity': `${effectiveOpacity}%`,
  };
  const totalWorkingCount =
    workspace.totalWorkingCoworkerCount + workspace.totalActiveWorkerCount;
  const visibleArchiveError =
    coordState.archiveError && !archiveErrorDismissed
      ? coordState.archiveError
      : null;

  return (
    <Theme
      appearance={resolvedThemeAppearance}
      accentColor={mapAccentToRadixColor(appearance.accent)}
      grayColor="gray"
      panelBackground={appearance.surface === 'glass' ? 'translucent' : 'solid'}
      radius="medium"
      scaling="100%"
    >
      <div
        className="tl-app-shell"
        data-theme={resolvedThemeAppearance}
        data-surface={appearance.surface}
        style={shellStyle}
      >
        {/* Custom Desktop Titlebar — single home for sidebar toggle */}
        <header className="tl-titlebar" data-tauri-drag-region="true">
          <div className="tl-titlebar-left" data-tauri-drag-region="true">
            <IconButton
              size="2"
              variant="ghost"
              color="gray"
              className="tl-titlebar-sidebar-btn"
              aria-label={
                compact
                  ? mobileDrawerOpen
                    ? 'Close navigation drawer'
                    : 'Open navigation drawer'
                  : sidebarCollapsed
                    ? 'Expand sidebar'
                    : 'Collapse sidebar'
              }
              aria-expanded={compact ? mobileDrawerOpen : !sidebarCollapsed}
              aria-controls={
                compact ? 'tl-navigation-drawer' : 'tl-projects-sidebar'
              }
              ref={navigationTriggerRef}
              onClick={() => {
                if (compact) {
                  setMobileDrawerOpen((prev) => !prev);
                } else {
                  setSidebarCollapsed((prev) => !prev);
                }
              }}
            >
              {compact ? (
                <Menu size={17} aria-hidden="true" />
              ) : sidebarCollapsed ? (
                <PanelLeftOpen size={17} aria-hidden="true" />
              ) : (
                <PanelLeftClose size={17} aria-hidden="true" />
              )}
            </IconButton>

            <img
              src={threadlaneIconUrl}
              alt=""
              aria-hidden="true"
              className="tl-titlebar-logo"
            />
            <span className="tl-titlebar-brand" data-tauri-drag-region="true">
              Threadlane
            </span>

            {(compact || sidebarCollapsed) ? (
              <span className="tl-titlebar-pills">
                {totalWorkingCount > 0 ? (
                  <span
                    className="tl-mini-pill tl-nav-count"
                    data-tone="working"
                    aria-label={`${totalWorkingCount} working`}
                  >
                    <span
                      className="tl-status-dot"
                      data-tone="working"
                      aria-hidden="true"
                    />
                    <span aria-hidden="true">{totalWorkingCount}</span>
                  </span>
                ) : null}
                {workspace.totalNeedsHumanCount > 0 ? (
                  <span
                    className="tl-mini-pill tl-nav-count"
                    data-tone="attention"
                    aria-label={`${workspace.totalNeedsHumanCount} need attention`}
                  >
                    <span
                      className="tl-status-dot"
                      data-tone="attention"
                      aria-hidden="true"
                    />
                    <span aria-hidden="true">
                      {workspace.totalNeedsHumanCount}
                    </span>
                  </span>
                ) : null}
              </span>
            ) : null}
          </div>

          <div className="tl-titlebar-right">
            <IconButton
              size="2"
              variant="ghost"
              color="gray"
              aria-label="Refresh workspace"
              onClick={handleManualRefresh}
            >
              <RefreshCw size={15} aria-hidden="true" />
            </IconButton>

            <div className="tl-window-controls">
              <button
                type="button"
                className="tl-win-btn"
                aria-label="Minimize window"
                onClick={() => {
                  void ipcDesktopWindowAction('minimize');
                }}
              >
                <Minus size={15} aria-hidden="true" />
              </button>
              <button
                type="button"
                className="tl-win-btn tl-win-btn-close"
                aria-label="Close window"
                onClick={() => {
                  void ipcDesktopWindowAction('close');
                }}
              >
                <X size={15} aria-hidden="true" />
              </button>
            </div>
          </div>
        </header>

        {/* Compact top section bar at <= 760px */}
        {compact ? (
          <nav
            className="tl-compact-nav"
            aria-label="Compact section navigation"
          >
            {COMPACT_NAV_SECTIONS.map(([id, label]) => {
              const current = activeSection === id;
              return (
                <button
                  key={id}
                  type="button"
                  className="tl-compact-nav-btn"
                  data-active={current ? 'true' : 'false'}
                  aria-current={current ? 'page' : undefined}
                  onClick={() => handleSelectSection(id)}
                >
                  {label}
                </button>
              );
            })}
          </nav>
        ) : null}

        {/* Mobile Radix Dialog Drawer at <= 760px */}
        <Dialog.Root
          open={compact && mobileDrawerOpen}
          onOpenChange={setMobileDrawerOpen}
        >
          <Dialog.Content
            id="tl-navigation-drawer"
            className="tl-drawer-dialog"
            aria-label="Projects and navigation drawer"
            onCloseAutoFocus={(event) => {
              event.preventDefault();
              navigationTriggerRef.current?.focus();
            }}
          >
            <div className="tl-drawer-header">
              <Dialog.Title className="tl-drawer-title">
                Navigation
              </Dialog.Title>
              <Dialog.Description className="tl-sr-only">
                Select Projects Active or Archive, pick a conversation, or
                switch sections.
              </Dialog.Description>
              <IconButton
                size="2"
                variant="ghost"
                color="gray"
                aria-label="Close navigation drawer"
                onClick={() => setMobileDrawerOpen(false)}
              >
                <X size={17} aria-hidden="true" />
              </IconButton>
            </div>
            <SidebarContent
              activeSection={activeSection}
              collapsed={false}
              workspace={workspace}
              visibleProjects={visibleProjects}
              selectedChatKey={activeSelectedChat?.chatKey ?? null}
              collapsedProjects={collapsedProjects}
              onSelectSection={handleSelectSection}
              onSelectLifecycle={handleSelectLifecycle}
              onSelectChat={handleSelectChat}
              onToggleProject={handleToggleProject}
              onSetChatArchived={handleSetChatArchived}
            />
          </Dialog.Content>
        </Dialog.Root>

        {/* Main Workspace Body */}
        <div className="tl-workspace-body">
          {!compact ? (
            <aside
              id="tl-projects-sidebar"
              className="tl-sidebar"
              data-collapsed={sidebarCollapsed ? 'true' : 'false'}
              aria-label="Projects sidebar"
            >
              <SidebarContent
                activeSection={activeSection}
                collapsed={sidebarCollapsed}
                workspace={workspace}
                visibleProjects={visibleProjects}
                selectedChatKey={activeSelectedChat?.chatKey ?? null}
                collapsedProjects={collapsedProjects}
                onSelectSection={handleSelectSection}
                onSelectLifecycle={handleSelectLifecycle}
                onSelectChat={handleSelectChat}
                onToggleProject={handleToggleProject}
                onSetChatArchived={handleSetChatArchived}
              />
            </aside>
          ) : null}

          <main className="tl-main-region">
            {/* Instance-preserving panel strategy: panels remain mounted across section and viewport changes */}
            <section
              className="tl-panel-slot tl-panel-slot--workspace"
              hidden={activeSection !== 'projects'}
              aria-hidden={activeSection !== 'projects'}
              aria-label="Projects workspace"
            >
              {visibleArchiveError ? (
                <div
                  className="tl-workspace-alert-bar"
                  data-tone="danger"
                  role="alert"
                >
                  <div className="tl-workspace-alert-copy">
                    <AlertCircle size={15} aria-hidden="true" />
                    <span>{visibleArchiveError}</span>
                  </div>
                  <Button
                    size="1"
                    variant="soft"
                    color="gray"
                    onClick={() => setArchiveErrorDismissed(true)}
                  >
                    Dismiss
                  </Button>
                </div>
              ) : null}

              {coordState.codexOpenError ? (
                <div
                  className="tl-workspace-alert-bar"
                  data-tone="danger"
                  role="alert"
                >
                  <div className="tl-workspace-alert-copy">
                    <AlertCircle size={15} aria-hidden="true" />
                    <span>{coordState.codexOpenError}</span>
                  </div>
                  <Button
                    size="1"
                    variant="soft"
                    color="gray"
                    onClick={() => {
                      coordinatorRef.current?.clearCodexOpenError();
                    }}
                  >
                    Dismiss
                  </Button>
                </div>
              ) : null}

              {workspace.lifecycleFilter === 'active' &&
              workspace.archivedBlockerNotice ? (
                <div
                  className="tl-workspace-alert-bar"
                  data-tone="attention"
                  role="status"
                >
                  <div className="tl-workspace-alert-copy">
                    <AlertCircle size={15} aria-hidden="true" />
                    <span>{workspace.archivedBlockerNotice}</span>
                  </div>
                  <Button
                    size="1"
                    variant="soft"
                    color="amber"
                    onClick={() => handleSelectLifecycle('archived')}
                  >
                    Open Archive
                  </Button>
                </div>
              ) : null}

              {workspace.status === 'unavailable' &&
              workspace.unavailableReason ? (
                <div
                  className="tl-workspace-alert-bar"
                  data-tone="unavailable"
                  role="status"
                >
                  <div className="tl-workspace-alert-copy">
                    <AlertCircle size={15} aria-hidden="true" />
                    <span>{workspace.unavailableReason}</span>
                  </div>
                  <div className="tl-workspace-alert-actions">
                    <Button
                      size="1"
                      variant="soft"
                      color="gray"
                      onClick={handleWorkspaceRefresh}
                    >
                      Retry
                    </Button>
                    <Button
                      size="1"
                      variant="surface"
                      color="gray"
                      onClick={() => setActiveSection('settings')}
                    >
                      Open Settings
                    </Button>
                  </div>
                </div>
              ) : null}

              {workspace.bounds.native_samples_omitted > 0 ? (
                <div
                  className="tl-workspace-alert-bar"
                  data-tone="neutral"
                  role="status"
                >
                  <div className="tl-workspace-alert-copy">
                    <AlertCircle size={15} aria-hidden="true" />
                    <span>
                      {`Showing a bounded native sample window; ${workspace.bounds.native_samples_omitted} native sample(s) omitted.`}
                    </span>
                  </div>
                </div>
              ) : null}

              <WorkspaceChatPanels
                chat={selectedChatSummary}
                nativeSample={selectedNativeSample}
                snapshot={
                  selectedChatSummary ? coordState.workspaceSnapshot : null
                }
                activeTab={coordState.selectedTab}
                onTabChange={handleTabChange}
                nowMs={workspace.referenceNowMs}
                loading={coordState.observationLoading}
                error={coordState.observationError}
                isStale={workspace.isStale}
                staleReason={workspace.staleReason}
                onRefresh={handleWorkspaceRefresh}
                onOpenCodex={handleOpenCodex}
                codexOpenInFlight={coordState.codexOpenInFlight}
              />
            </section>

            <section
              className="tl-panel-slot"
              hidden={activeSection !== 'tools'}
              aria-hidden={activeSection !== 'tools'}
              aria-label="Tools and models"
            >
              <ToolsPanel
                registry={coordState.registry}
                registryLoading={coordState.registryLoading}
                registryError={coordState.registryError}
                discoveredTools={coordState.discoveredTools}
                discoveryLoading={coordState.discoveryLoading}
                discoveryError={coordState.discoveryError}
                toolVersions={coordState.toolVersions}
                onRefreshRegistry={() => {
                  void coordinatorRef.current?.refreshRegistry({ force: true });
                }}
                onRefreshDiscovery={() => {
                  void coordinatorRef.current?.refreshToolDiscovery();
                }}
                onRecheckVersions={() => {
                  coordinatorRef.current?.recheckToolVersions();
                }}
                onToolInspectionChanged={(rev) => {
                  coordinatorRef.current?.queueToolVersionRecheck(rev);
                }}
                onOpenProfiles={handleOpenProfilesFromTools}
              />
            </section>

            <section
              className="tl-panel-slot"
              hidden={activeSection !== 'profiles'}
              aria-hidden={activeSection !== 'profiles'}
              aria-label="Profiles"
            >
              <div className="tl-panel-scroll">
                <div className="tl-panel-inner">
                  <ProfilesPanel
                    registry={coordState.registry}
                    loading={coordState.registryLoading}
                    error={coordState.registryError}
                    compact={compact}
                    onRegistryChanged={handleProfileRegistryChanged}
                    onOpenTools={handleOpenToolsFromProfiles}
                  />
                </div>
              </div>
            </section>

            <section
              className="tl-panel-slot"
              hidden={activeSection !== 'settings'}
              aria-hidden={activeSection !== 'settings'}
              aria-label="Settings"
            >
              <SettingsPanel
                appearance={appearance}
                onAppearanceChange={handleAppearanceChange}
                diagnostics={workspace.diagnostics}
                warnings={workspace.warnings}
                onObservationLinked={handleWorkspaceRefresh}
              />
            </section>
          </main>
        </div>
      </div>
    </Theme>
  );
}

const mountedRoots = new WeakMap<HTMLElement, Root>();

export function mountDesktopApp(mountNode: HTMLElement): void {
  let root = mountedRoots.get(mountNode);
  if (!root) {
    root = createRoot(mountNode);
    mountedRoots.set(mountNode, root);
  }
  root.render(<ThreadlaneDesktopShell />);
}
