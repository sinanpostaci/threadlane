import '@radix-ui/themes/styles.css';
import './pages/tools.css';
import './app.css';

import {
  Badge,
  Button,
  Callout,
  Dialog,
  Flex,
  Heading,
  IconButton,
  Text,
  Theme,
} from '@radix-ui/themes';
import { invoke } from '@tauri-apps/api/core';
import {
  AlertCircle,
  Archive,
  Blocks,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock,
  ExternalLink,
  FolderGit2,
  FolderOpen,
  History,
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
} from 'react';
import { createRoot } from 'react-dom/client';

import threadlaneIconUrl from '../src-tauri/icons/icon.svg';
import {
  ProviderBrandBadge,
  resolveProviderBrandKey,
} from './app-brand.tsx';
import {
  OPEN_CODEX_CHAT_COMMAND,
  type CodexChatTarget,
} from './app-contract.ts';
import {
  DesktopAppCoordinator,
  OBSERVATION_POLL_INTERVAL_MS,
  type DesktopCoordinatorState,
} from './app-data.ts';
import {
  readProjectsArchiveCutoff,
  writeProjectsArchiveCutoff,
  type DesktopWorkspaceProjection,
  type JournalCallRow,
  type JournalJobSection,
  type JournalWindowHours,
  type ProjectsArchiveStorage,
  type SelectedChatJournal,
  type SidebarChatItem,
  type SidebarProjectGroup,
  type StatusDotTone,
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
import { ipcObservationSnapshot } from './observation/ipc.ts';
import {
  desktop_registry_snapshot,
  desktop_tool_versions,
} from './registry/ipc.ts';
import type { RegistrySnapshot } from './registry/types.ts';
import type {
  AccentName,
  AppearancePreferences,
} from './types.ts';

export type AppSection = 'projects' | 'tools' | 'profiles' | 'settings';

const COMPACT_BREAKPOINT_PX = 760;

function getBrowserStorage(): ProjectsArchiveStorage | null {
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      return window.localStorage;
    }
  } catch {
    return null;
  }
  return null;
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

function mapDotToneToBadgeColor(
  tone: StatusDotTone
): 'blue' | 'amber' | 'red' | 'green' | 'gray' {
  switch (tone) {
    case 'working':
      return 'blue';
    case 'attention':
      return 'amber';
    case 'danger':
      return 'red';
    case 'success':
      return 'green';
    case 'neutral':
      return 'gray';
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
  compact: boolean;
  workspace: DesktopWorkspaceProjection;
  collapsedProjects: ReadonlySet<string>;
  onSelectSection: (section: AppSection) => void;
  onSelectChat: (chatKey: string) => void;
  onToggleProject: (projectKey: string) => void;
  onWindowHoursChange: (hours: JournalWindowHours) => void;
  onArchiveSettled: () => void;
  onRestoreArchived: () => void;
  onToggleCollapse?: () => void;
}

function SidebarContent({
  activeSection,
  collapsed,
  compact,
  workspace,
  collapsedProjects,
  onSelectSection,
  onSelectChat,
  onToggleProject,
  onWindowHoursChange,
  onArchiveSettled,
  onRestoreArchived,
  onToggleCollapse,
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
            <button
              key={item.id}
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
                      {workspace.totalWorkingCount > 0 ? (
                        <span
                          className="tl-mini-pill tl-nav-count"
                          data-tone="working"
                          aria-label={`${workspace.totalWorkingCount} working`}
                        >
                          <span
                            className="tl-status-dot"
                            data-tone="working"
                            aria-hidden="true"
                          />
                          <span aria-hidden="true">
                            {workspace.totalWorkingCount}
                          </span>
                        </span>
                      ) : null}
                      {workspace.totalNeedsHumanCount > 0 ? (
                        <span
                          className="tl-mini-pill tl-nav-count"
                          data-tone="attention"
                          aria-label={
                            workspace.totalNeedsHumanCount === 1
                              ? '1 needs you'
                              : `${workspace.totalNeedsHumanCount} need you`
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
          );
        })}
      </nav>

      {!collapsed ? (
        <div className="tl-sidebar-projects" aria-label="Projects and chats">
          <div className="tl-sidebar-section-bar">
            <span className="tl-sidebar-kicker">Chats</span>
            <div
              className="tl-window-toggle"
              role="group"
              aria-label="Activity time window"
            >
              {([6, 24] as const).map((hours) => (
                <button
                  key={hours}
                  type="button"
                  className="tl-window-chip"
                  data-active={workspace.windowHours === hours ? 'true' : 'false'}
                  aria-pressed={workspace.windowHours === hours}
                  onClick={() => onWindowHoursChange(hours)}
                >
                  {hours}h
                </button>
              ))}
            </div>
          </div>

          {workspace.canArchiveHistory || workspace.canRestoreArchived ? (
            <div className="tl-archive-bar">
              {workspace.canArchiveHistory ? (
                <button
                  type="button"
                  className="tl-quiet-action-btn"
                  onClick={onArchiveSettled}
                >
                  <Archive size={13} aria-hidden="true" />
                  <span>Archive history</span>
                </button>
              ) : null}
              {workspace.canRestoreArchived ? (
                <button
                  type="button"
                  className="tl-quiet-action-btn"
                  onClick={onRestoreArchived}
                >
                  <RotateCcw size={13} aria-hidden="true" />
                  <span>Show archived</span>
                </button>
              ) : null}
            </div>
          ) : null}

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
            ) : workspace.projects.length === 0 ? (
              <div className="tl-sidebar-empty" role="status">
                <span>
                  {workspace.emptyMessage ??
                    `No active chats in ${workspace.windowHours}h window.`}
                </span>
              </div>
            ) : (
              workspace.projects.map((project: SidebarProjectGroup) => {
                const isProjectCollapsed = collapsedProjects.has(project.key);
                return (
                  <section
                    key={project.key}
                    className="tl-project-group"
                    aria-label={project.label}
                  >
                    <button
                      type="button"
                      className="tl-project-header"
                      aria-expanded={!isProjectCollapsed}
                      onClick={() => onToggleProject(project.key)}
                    >
                      {isProjectCollapsed ? (
                        <ChevronRight size={14} aria-hidden="true" />
                      ) : (
                        <ChevronDown size={14} aria-hidden="true" />
                      )}
                      <FolderGit2 size={14} aria-hidden="true" />
                      <span className="tl-project-name">{project.label}</span>
                      <span className="tl-project-counts">
                        {project.hasWorking ? (
                          <span
                            className="tl-mini-pill"
                            data-tone="working"
                            aria-label={project.workingBadgeLabel ?? undefined}
                          >
                            {project.workingCount}
                          </span>
                        ) : null}
                        {project.hasNeedsHuman ? (
                          <span
                            className="tl-mini-pill"
                            data-tone="attention"
                            aria-label={
                              project.needsHumanBadgeLabel ?? undefined
                            }
                          >
                            {project.needsHumanCount}
                          </span>
                        ) : null}
                      </span>
                    </button>

                    {!isProjectCollapsed ? (
                      <div className="tl-chat-list">
                        {project.chats.map((chat: SidebarChatItem) => {
                          const selected =
                            activeSection === 'projects' &&
                            workspace.selectedChatKey === chat.key;
                          return (
                            <button
                              key={chat.key}
                              type="button"
                              className="tl-chat-item"
                              data-selected={selected ? 'true' : 'false'}
                              aria-current={selected ? 'true' : undefined}
                              onClick={() => onSelectChat(chat.key)}
                            >
                              <div className="tl-chat-item-top">
                                <StatusDots
                                  primary={chat.primaryDotTone}
                                  secondary={chat.secondaryDotTone}
                                />
                                <span className="tl-chat-item-label">
                                  {chat.label}
                                </span>
                                <span className="tl-chat-item-age">
                                  {chat.ageLabel}
                                </span>
                              </div>

                              <div className="tl-chat-item-meta">
                                {chat.hasNeedsHuman &&
                                chat.needsHumanBadgeLabel ? (
                                  <span
                                    className="tl-mini-pill"
                                    data-tone="attention"
                                  >
                                    {chat.needsHumanBadgeLabel}
                                  </span>
                                ) : null}
                                {chat.hasWorking && chat.workingBadgeLabel ? (
                                  <span
                                    className="tl-mini-pill"
                                    data-tone="working"
                                  >
                                    {chat.workingBadgeLabel}
                                  </span>
                                ) : null}
                                {!chat.hasNeedsHuman && !chat.hasWorking ? (
                                  <span className="tl-chat-status-text">
                                    {chat.statusSummaryLabel}
                                  </span>
                                ) : null}
                                {chat.outsideWindow ? (
                                  <span className="tl-outside-pill">
                                    Older ({workspace.windowHours}h+)
                                  </span>
                                ) : null}
                              </div>
                            </button>
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

      {!compact && onToggleCollapse ? (
        <div className="tl-sidebar-footer">
          <button
            type="button"
            className="tl-collapse-btn"
            aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            onClick={onToggleCollapse}
          >
            {collapsed ? (
              <PanelLeftOpen size={16} aria-hidden="true" />
            ) : (
              <>
                <PanelLeftClose size={16} aria-hidden="true" />
                <span>Collapse sidebar</span>
              </>
            )}
          </button>
        </div>
      ) : null}
    </div>
  );
}

interface JournalJobCardProps {
  job: JournalJobSection;
}

function JournalJobCard({ job }: JournalJobCardProps) {
  const taskCountLabel =
    job.tasks.length > 0
      ? `${job.tasks.length} ${job.tasks.length === 1 ? 'task' : 'tasks'}`
      : null;

  return (
    <details className="tl-job-card" data-tone={job.tone}>
      <summary
        className="tl-job-summary-row"
        aria-label={`${job.title}, ${job.statusLabel}${
          taskCountLabel ? `, ${taskCountLabel}` : ''
        }`}
      >
        <span className="tl-job-summary-main">
          <StatusDots primary={job.tone} />
          <Text size="2" weight="medium" className="tl-job-title">
            {job.title}
          </Text>
        </span>

        <span className="tl-job-summary-meta">
          <Badge
            size="1"
            variant="soft"
            color={mapDotToneToBadgeColor(job.tone)}
          >
            {job.statusLabel}
          </Badge>
          {taskCountLabel ? (
            <Text size="1" color="gray">
              {taskCountLabel}
            </Text>
          ) : null}
          <ChevronRight
            size={14}
            className="tl-disclosure-chevron tl-job-chevron"
            aria-hidden="true"
          />
        </span>
      </summary>

      <div className="tl-job-body">
        {job.olderWindowLabel ? (
          <Text size="1" color="amber">
            {job.olderWindowLabel}
          </Text>
        ) : null}

        {job.workerFinishedPendingAcceptance && !job.isAuthoritativeJobDone ? (
          <Text size="1" color="gray">
            Worker call finished; job completion requires review and acceptance.
          </Text>
        ) : null}

        {job.summary ? (
          <Text size="2" color="gray" className="tl-job-summary">
            {job.summary}
          </Text>
        ) : null}

        {job.attentionReason ? (
          <div className="tl-job-attention-note">
            <Text size="1" weight="medium">
              {job.attentionReason}
            </Text>
            {job.nextStep ? (
              <Text size="1" color="gray">
                Next: {job.nextStep}
              </Text>
            ) : null}
          </div>
        ) : null}

        {job.tasks.length > 0 ? (
          <div className="tl-task-section">
            {job.taskSummary ? (
              <Text size="1" color="gray" weight="medium">
                Tasks · {job.taskSummary}
              </Text>
            ) : null}
            <ul className="tl-task-list" aria-label={`Tasks for ${job.title}`}>
              {job.tasks.map((task) => (
                <li key={task.key} className="tl-task-row">
                  <div className="tl-task-main">
                    <StatusDots primary={task.tone} />
                    <span className="tl-task-label">{task.label}</span>
                  </div>
                  <Badge
                    size="1"
                    variant="soft"
                    color={mapDotToneToBadgeColor(task.tone)}
                  >
                    {task.statusLabel}
                  </Badge>
                </li>
              ))}
            </ul>
          </div>
        ) : null}

        <details className="tl-inline-disclosure">
          <summary className="tl-inline-summary">
            <ChevronRight
              size={13}
              className="tl-disclosure-chevron"
              aria-hidden="true"
            />
            <span>Technical details</span>
          </summary>
          <dl className="tl-kv-grid">
            {Object.entries(job.technicalDetails).map(([label, val]) => (
              <div key={label} className="tl-kv-pair">
                <dt>{label}</dt>
                <dd>{val}</dd>
              </div>
            ))}
          </dl>
        </details>
      </div>
    </details>
  );
}

interface JournalCallItemProps {
  call: JournalCallRow;
}

function JournalCallItem({ call }: JournalCallItemProps) {
  const brandKey = resolveProviderBrandKey(call.brand, call.model);
  return (
    <article className="tl-call-row" data-tone={call.tone}>
      <div className="tl-call-row-head">
        <div className="tl-call-identity">
          <ProviderBrandBadge brand={brandKey} size={15} />
          <Badge size="1" variant="surface" color="gray">
            {call.roleLabel}
          </Badge>
          <Text size="2" weight="medium" className="tl-call-title">
            {call.title}
          </Text>
        </div>

        <Flex align="center" gap="2" wrap="wrap">
          <span className="tl-call-state">
            {call.running ? (
              <Loader2 size={13} className="tl-spin" aria-hidden="true" />
            ) : (
              <StatusDots primary={call.tone} />
            )}
            <Badge
              size="1"
              variant="soft"
              color={mapDotToneToBadgeColor(call.tone)}
            >
              {call.statusLabel}
            </Badge>
          </span>
          <Text size="1" color="gray">
            {call.ageLabel}
          </Text>
        </Flex>
      </div>

      <div className="tl-call-meta-bar">
        <span className="tl-meta-chip">{call.model}</span>
        {call.effort ? (
          <span className="tl-meta-chip">effort: {call.effort}</span>
        ) : null}
        <span className="tl-meta-chip">{call.durationLabel}</span>
        <span className="tl-meta-chip">{call.usageLabel}</span>
      </div>

      {call.workerPendingNote ? (
        <Text size="1" color="gray">
          {call.workerPendingNote}
        </Text>
      ) : null}

      {call.detail ? (
        <details className="tl-inline-disclosure">
          <summary className="tl-inline-summary">
            <ChevronRight
              size={13}
              className="tl-disclosure-chevron"
              aria-hidden="true"
            />
            <span>Call provenance</span>
          </summary>
          <p className="tl-technical-copy">{call.detail}</p>
        </details>
      ) : null}
    </article>
  );
}

interface ProjectsJournalPanelProps {
  workspace: DesktopWorkspaceProjection;
  codexOpenInFlight: boolean;
  codexOpenError: string | null;
  loadingMore: boolean;
  onRefresh: () => void;
  onLoadMore: () => void;
  onOpenCodex: (target: CodexChatTarget) => void;
  onDismissCodexError: () => void;
  onOpenSettings: () => void;
}

function ProjectsJournalPanel({
  workspace,
  codexOpenInFlight,
  codexOpenError,
  loadingMore,
  onRefresh,
  onLoadMore,
  onOpenCodex,
  onDismissCodexError,
  onOpenSettings,
}: ProjectsJournalPanelProps) {
  if (workspace.status === 'loading') {
    return (
      <div className="tl-panel-scroll">
        <div className="tl-panel-inner">
          <div className="tl-state-card" role="status" aria-live="polite">
            <Loader2 size={22} className="tl-spin" aria-hidden="true" />
            <Text size="2" weight="medium">
              Reading workspace observation journal…
            </Text>
          </div>
        </div>
      </div>
    );
  }

  if (workspace.status === 'unavailable') {
    return (
      <div className="tl-panel-scroll">
        <div className="tl-panel-inner">
          <div className="tl-state-card" role="alert">
            <AlertCircle size={22} aria-hidden="true" />
            <Text size="3" weight="medium">
              Observation runtime unavailable
            </Text>
            <Text size="2" color="gray">
              {workspace.unavailableReason ??
                'Observation data is currently unreachable in this window.'}
            </Text>
            <Flex gap="2" wrap="wrap" justify="center" mt="1">
              <Button size="2" variant="soft" color="gray" onClick={onRefresh}>
                <RefreshCw size={15} aria-hidden="true" />
                Retry observation read
              </Button>
              <Button
                size="2"
                variant="surface"
                color="gray"
                onClick={onOpenSettings}
              >
                Open Settings
              </Button>
            </Flex>
          </div>
        </div>
      </div>
    );
  }

  const journal: SelectedChatJournal | null = workspace.selectedJournal;

  if (workspace.status === 'empty' || !journal) {
    return (
      <div className="tl-panel-scroll">
        <div className="tl-panel-inner">
          <div className="tl-state-card" role="status">
            <CheckCircle2 size={22} aria-hidden="true" />
            <Text size="3" weight="medium">
              No active or recent chats in this {workspace.windowHours}h window
            </Text>
            <Text size="2" color="gray">
              {workspace.emptyMessage ??
                'Observed conversations have no pending questions or active workers right now.'}
            </Text>
            <Flex gap="2" wrap="wrap" justify="center" mt="1">
              <Button size="2" variant="soft" color="gray" onClick={onRefresh}>
                <RefreshCw size={15} aria-hidden="true" />
                Refresh
              </Button>
            </Flex>
          </div>
        </div>
      </div>
    );
  }

  const { attentionBanner, codexTarget, showHeaderCodexAction } = journal;

  return (
    <div className="tl-panel-scroll">
      <div className="tl-panel-inner tl-journal-stack">
        {workspace.isStale && workspace.staleReason ? (
          <Callout.Root color="amber" variant="surface" size="1" role="status">
            <Callout.Icon>
              <AlertCircle size={15} />
            </Callout.Icon>
            <Callout.Text>
              Showing last known observation snapshot ({workspace.staleReason}).
            </Callout.Text>
          </Callout.Root>
        ) : null}

        {/* Selected chat header */}
        <header className="tl-journal-header">
          <div className="tl-journal-header-main">
            <div className="tl-journal-kicker">
              <FolderGit2 size={14} aria-hidden="true" />
              <span className="tl-journal-project">{journal.projectLabel}</span>
            </div>

            <div className="tl-journal-title-row">
              <StatusDots
                primary={journal.primaryTone}
                secondary={
                  journal.workingCount > 0 && journal.needsHumanCount > 0
                    ? 'working'
                    : null
                }
              />
              <Heading size="4" weight="medium" className="tl-journal-title">
                {journal.chatLabel}
              </Heading>
            </div>

            <Flex align="center" gap="2" wrap="wrap">
              {journal.needsHumanBadgeLabel ? (
                <Badge size="1" variant="soft" color="amber">
                  {journal.needsHumanBadgeLabel}
                </Badge>
              ) : null}
              {journal.workingBadgeLabel ? (
                <Badge size="1" variant="soft" color="blue">
                  {journal.workingBadgeLabel}
                </Badge>
              ) : null}
              {!journal.needsHumanBadgeLabel && !journal.workingBadgeLabel ? (
                <Badge
                  size="1"
                  variant="soft"
                  color={mapDotToneToBadgeColor(journal.primaryTone)}
                >
                  {journal.statusLabel}
                </Badge>
              ) : null}
            </Flex>
          </div>

          {showHeaderCodexAction && codexTarget ? (
            <div className="tl-journal-header-actions">
              <Button
                size="2"
                variant="solid"
                color="gray"
                highContrast
                disabled={codexOpenInFlight}
                onClick={() => onOpenCodex(codexTarget)}
              >
                <ExternalLink size={15} aria-hidden="true" />
                {codexOpenInFlight ? 'Opening Codex…' : 'Open in Codex'}
              </Button>
            </div>
          ) : null}
        </header>

        {codexOpenError ? (
          <Callout.Root color="red" variant="surface" size="1" role="alert">
            <Callout.Icon>
              <AlertCircle size={15} />
            </Callout.Icon>
            <Callout.Text>
              <Flex align="center" justify="between" gap="3" wrap="wrap">
                <span>{codexOpenError}</span>
                <Button
                  size="1"
                  variant="soft"
                  color="gray"
                  onClick={onDismissCodexError}
                >
                  Dismiss
                </Button>
              </Flex>
            </Callout.Text>
          </Callout.Root>
        ) : null}

        {journal.outsideWindowNotice ? (
          <div className="tl-outside-banner" role="status">
            <Clock size={14} aria-hidden="true" />
            <span>{journal.outsideWindowNotice}</span>
          </div>
        ) : null}

        {/* Single primary Attention Card when the chat needs human action */}
        {attentionBanner ? (
          <section
            className="tl-attention-banner"
            data-tone={attentionBanner.tone}
            aria-label="Needs your attention"
          >
            <div className="tl-attention-main">
              <div className="tl-attention-top">
                <AlertCircle size={16} aria-hidden="true" />
                <Text size="2" weight="bold">
                  {attentionBanner.title}
                </Text>
                <Badge
                  size="1"
                  variant="soft"
                  color={attentionBanner.tone === 'danger' ? 'red' : 'amber'}
                >
                  {attentionBanner.stateLabel}
                </Badge>
                <Text size="1" color="gray">
                  {attentionBanner.ageLabel}
                </Text>
              </div>
              <Text as="p" size="2" className="tl-attention-reason">
                {attentionBanner.reason}
              </Text>
              <Text as="p" size="1" color="gray">
                Next step: {attentionBanner.nextStep}
              </Text>
            </div>

            {attentionBanner.showOpenInCodex && codexTarget ? (
              <div className="tl-attention-actions">
                <Button
                  size="2"
                  variant="solid"
                  color="gray"
                  highContrast
                  disabled={codexOpenInFlight}
                  onClick={() => onOpenCodex(codexTarget)}
                >
                  <ExternalLink size={15} aria-hidden="true" />
                  {codexOpenInFlight ? 'Opening Codex…' : 'Open in Codex'}
                </Button>
              </div>
            ) : null}
          </section>
        ) : null}

        {/* Jobs & Tasks */}
        {journal.jobs.length > 0 ? (
          <section className="tl-journal-section" aria-label="Jobs and tasks">
            <div className="tl-section-header">
              <Text size="2" weight="medium">
                Jobs ({journal.jobs.length})
              </Text>
            </div>
            <div className="tl-job-list">
              {journal.jobs.map((job) => (
                <JournalJobCard key={job.key} job={job} />
              ))}
            </div>
          </section>
        ) : null}

        {/* Calls / Activity */}
        <section className="tl-journal-section" aria-label="Activity">
          <div className="tl-section-header">
            <Text size="2" weight="medium">
              Activity ({journal.calls.length})
            </Text>
          </div>
          {journal.calls.length === 0 ? (
            <div className="tl-empty-inline">
              <Text size="2" color="gray">
                No model or worker calls recorded in this window.
              </Text>
            </div>
          ) : (
            <div className="tl-call-list">
              {journal.calls.map((call) => (
                <JournalCallItem key={call.key} call={call} />
              ))}
            </div>
          )}
        </section>

        {/* Collapsed Other Activity (grouped low-level MCP reads) */}
        {journal.otherActivity ? (
          <details className="tl-other-activity">
            <summary className="tl-other-activity-summary">
              <Flex align="center" gap="2">
                <ChevronRight
                  size={15}
                  className="tl-disclosure-chevron"
                  aria-hidden="true"
                />
                <Text size="2" weight="medium">
                  {journal.otherActivity.title}
                </Text>
                <Text size="1" color="gray">
                  {journal.otherActivity.summaryLabel}
                </Text>
              </Flex>
              <Flex align="center" gap="2">
                <Badge size="1" variant="soft" color="gray">
                  {journal.otherActivity.count}
                </Badge>
                <Text size="1" color="gray">
                  {journal.otherActivity.ageLabel}
                </Text>
              </Flex>
            </summary>

            <div className="tl-other-activity-body">
              <ul className="tl-noise-list">
                {journal.otherActivity.items.map((item) => (
                  <li key={item.key} className="tl-noise-row">
                    <span className="tl-noise-label">{item.label}</span>
                    <span className="tl-noise-detail">{item.detail}</span>
                    <span className="tl-noise-age">{item.ageLabel}</span>
                  </li>
                ))}
              </ul>
            </div>
          </details>
        ) : null}

        {/* Collapsed Contextual Diagnostics */}
        {journal.diagnostics.length > 0 ? (
          <details className="tl-other-activity">
            <summary className="tl-other-activity-summary">
              <Flex align="center" gap="2">
                <ChevronRight
                  size={15}
                  className="tl-disclosure-chevron"
                  aria-hidden="true"
                />
                <Text size="2" weight="medium">
                  Contextual diagnostics
                </Text>
              </Flex>
              <Badge size="1" variant="soft" color="amber">
                {journal.diagnostics.length}
              </Badge>
            </summary>
            <div className="tl-other-activity-body">
              <div className="tl-source-list">
                {journal.diagnostics.map((diag) => (
                  <div key={diag.key} className="tl-source-row">
                    <Flex align="center" justify="between" gap="2" wrap="wrap">
                      <Text size="2" weight="medium">
                        {diag.title}
                      </Text>
                      <Badge size="1" variant="soft" color="gray">
                        {diag.sourceLabel}
                      </Badge>
                    </Flex>
                    <Text size="1" color="gray">
                      {diag.impact}
                    </Text>
                    <Text size="1" color="gray">
                      Next step: {diag.nextStep}
                    </Text>
                    <details className="tl-inline-disclosure">
                      <summary className="tl-inline-summary">
                        <ChevronRight
                          size={13}
                          className="tl-disclosure-chevron"
                          aria-hidden="true"
                        />
                        <span>Raw diagnostic</span>
                      </summary>
                      <p className="tl-technical-copy">
                        {diag.technicalDetails}
                      </p>
                    </details>
                  </div>
                ))}
              </div>
            </div>
          </details>
        ) : null}

        {workspace.hasMoreHistory ? (
          <Flex justify="center" pt="1">
            <Button
              size="2"
              variant="soft"
              color="gray"
              disabled={loadingMore}
              onClick={onLoadMore}
            >
              <History size={15} aria-hidden="true" />
              {loadingMore
                ? 'Loading older activity…'
                : 'Load older activity'}
            </Button>
          </Flex>
        ) : null}
      </div>
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
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [collapsedProjects, setCollapsedProjects] = useState<Set<string>>(
    () => new Set()
  );

  const coordinatorRef = useRef<DesktopAppCoordinator | null>(null);
  if (coordinatorRef.current === null) {
    const initialArchiveCutoff = readProjectsArchiveCutoff(getBrowserStorage());
    coordinatorRef.current = new DesktopAppCoordinator(
      {
        fetchObservationSnapshot: (req) => ipcObservationSnapshot(req),
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
      },
      (nextState) => {
        setCoordState(nextState);
      },
      {
        archiveBeforeMs: initialArchiveCutoff,
        visible: typeof document === 'undefined' ? true : !document.hidden,
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

  const workspace = useMemo(
    () => coordinatorRef.current!.projectWorkspace(),
    [
      coordState.observationSnapshot,
      coordState.observationLoading,
      coordState.observationError,
      coordState.observationScope,
      coordState.selectedChatKey,
      coordState.windowHours,
      coordState.archiveBeforeMs,
    ]
  );

  // Keep coordinator's selectedChatKey synchronized with resolved default chat
  useEffect(() => {
    if (
      coordState.selectedChatKey === null &&
      workspace.selectedChatKey !== null
    ) {
      coordinatorRef.current?.selectChat(workspace.selectedChatKey);
    }
  }, [coordState.selectedChatKey, workspace.selectedChatKey]);

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

  const handleWindowHoursChange = useCallback((hours: JournalWindowHours) => {
    coordinatorRef.current?.setWindowHours(hours);
  }, []);

  const handleArchiveSettled = useCallback(() => {
    const now = Date.now();
    const res = writeProjectsArchiveCutoff(getBrowserStorage(), now, now);
    if (!res.ok) {
      setHistoryError(res.error);
      return;
    }
    setHistoryError(null);
    coordinatorRef.current?.setArchiveBeforeMs(res.archiveBeforeMs);
  }, []);

  const handleRestoreArchived = useCallback(() => {
    try {
      const storage = getBrowserStorage();
      if (!storage || !('removeItem' in storage) || typeof storage.removeItem !== 'function') {
        setHistoryError('Unable to restore archived activity because local storage is unavailable.');
        return;
      }
      storage.removeItem('threadlane.projects.archive.v1');
    } catch {
      setHistoryError('Unable to restore archived activity in local storage.');
      return;
    }
    setHistoryError(null);
    coordinatorRef.current?.setArchiveBeforeMs(null);
  }, []);

  const handleManualRefresh = useCallback(() => {
    const coordinator = coordinatorRef.current;
    if (!coordinator) {
      return;
    }
    void coordinator.refreshObservation({ mode: 'refresh' });
    if (activeSection === 'tools' || activeSection === 'profiles') {
      void coordinator.refreshRegistry({ force: true });
    }
  }, [activeSection]);

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
        style={
          {
            '--tl-surface-opacity': `${effectiveOpacity}%`,
          } as React.CSSProperties
        }
      >
        {/* Custom Desktop Titlebar */}
        <header className="tl-titlebar" data-tauri-drag-region="true">
          <div className="tl-titlebar-left" data-tauri-drag-region="true">
            {compact ? (
              <IconButton
                size="2"
                variant="ghost"
                color="gray"
                aria-label="Open navigation drawer"
                ref={navigationTriggerRef}
                onClick={() => setMobileDrawerOpen(true)}
              >
                <Menu size={18} aria-hidden="true" />
              </IconButton>
            ) : null}

            <img
              src={threadlaneIconUrl}
              alt=""
              aria-hidden="true"
              className="tl-titlebar-logo"
            />
            <span className="tl-titlebar-brand" data-tauri-drag-region="true">
              Threadlane
            </span>

            {compact ? (
              <span className="tl-titlebar-pills">
                {workspace.totalWorkingCount > 0 ? (
                  <span
                    className="tl-mini-pill tl-nav-count"
                    data-tone="working"
                    aria-label={`${workspace.totalWorkingCount} working`}
                  >
                    <span className="tl-status-dot" data-tone="working" aria-hidden="true" />
                    <span aria-hidden="true">{workspace.totalWorkingCount}</span>
                  </span>
                ) : null}
                {workspace.totalNeedsHumanCount > 0 ? (
                  <span
                    className="tl-mini-pill tl-nav-count"
                    data-tone="attention"
                    aria-label={`${workspace.totalNeedsHumanCount} need you`}
                  >
                    <span className="tl-status-dot" data-tone="attention" aria-hidden="true" />
                    <span aria-hidden="true">{workspace.totalNeedsHumanCount}</span>
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
            {(
              [
                ['projects', 'Projects'],
                ['tools', 'Tools'],
                ['profiles', 'Profiles'],
                ['settings', 'Settings'],
              ] as ReadonlyArray<readonly [AppSection, string]>
            ).map(([id, label]) => {
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
                Select a project conversation or switch sections.
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
              compact={true}
              workspace={workspace}
              collapsedProjects={collapsedProjects}
              onSelectSection={handleSelectSection}
              onSelectChat={handleSelectChat}
              onToggleProject={handleToggleProject}
              onWindowHoursChange={handleWindowHoursChange}
              onArchiveSettled={handleArchiveSettled}
              onRestoreArchived={handleRestoreArchived}
            />
          </Dialog.Content>
        </Dialog.Root>

        {/* Main Workspace Body */}
        <div className="tl-workspace-body">
          {!compact ? (
            <aside
              className="tl-sidebar"
              data-collapsed={sidebarCollapsed ? 'true' : 'false'}
              aria-label="Projects sidebar"
            >
              <SidebarContent
                activeSection={activeSection}
                collapsed={sidebarCollapsed}
                compact={false}
                workspace={workspace}
                collapsedProjects={collapsedProjects}
                onSelectSection={handleSelectSection}
                onSelectChat={handleSelectChat}
                onToggleProject={handleToggleProject}
                onWindowHoursChange={handleWindowHoursChange}
                onArchiveSettled={handleArchiveSettled}
                onRestoreArchived={handleRestoreArchived}
                onToggleCollapse={() =>
                  setSidebarCollapsed((prev) => !prev)
                }
              />
            </aside>
          ) : null}

          <main className="tl-main-region">
            {historyError ? (
              <div className="tl-history-error" role="alert">
                <span>{historyError}</span>
                <button type="button" onClick={() => setHistoryError(null)}>
                  Dismiss
                </button>
              </div>
            ) : null}
            {/* Instance-preserving panel strategy: panels remain mounted across section and viewport changes */}
            <section
              className="tl-panel-slot"
              hidden={activeSection !== 'projects'}
              aria-hidden={activeSection !== 'projects'}
              aria-label="Projects journal"
            >
              <ProjectsJournalPanel
                workspace={workspace}
                codexOpenInFlight={coordState.codexOpenInFlight}
                codexOpenError={coordState.codexOpenError}
                loadingMore={coordState.observationLoadingMore}
                onRefresh={() => {
                  void coordinatorRef.current?.refreshObservation({
                    mode: 'refresh',
                  });
                }}
                onLoadMore={() => {
                  void coordinatorRef.current?.loadMoreHistory();
                }}
                onOpenCodex={(target) => {
                  void coordinatorRef.current?.openCodexChat(target);
                }}
                onDismissCodexError={() => {
                  coordinatorRef.current?.clearCodexOpenError();
                }}
                onOpenSettings={() => setActiveSection('settings')}
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
                onObservationLinked={() => {
                  void coordinatorRef.current?.refreshObservation({
                    mode: 'refresh',
                  });
                }}
              />
            </section>
          </main>
        </div>
      </div>
    </Theme>
  );
}

export function mountDesktopApp(mountNode: HTMLElement): void {
  const root = createRoot(mountNode);
  root.render(<ThreadlaneDesktopShell />);
}
