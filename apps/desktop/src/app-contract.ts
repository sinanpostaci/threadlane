import type { RegistrySnapshot, ToolAdapter } from './registry/types.ts';
import type { WorkspaceChatIdentity } from './workspace-contract.ts';

export {
  DEFAULT_WORKSPACE_CHAT_LIFECYCLE_FILTER,
  DEFAULT_WORKSPACE_CHAT_TAB,
  MAX_WORKSPACE_ARCHIVED_CHAT_PREFERENCES,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_CHAT_LIFECYCLE_FILTERS,
  WORKSPACE_CHAT_SURFACE_TABS,
  WORKSPACE_MAIN_CLOSE_BEHAVIORS,
  WORKSPACE_SURFACE_SEPARATION,
  WORKSPACE_TRAY_ACTIONS,
  WORKSPACE_TRAY_CONTENT_BOUNDS,
  WORKSPACE_TRAY_MAX_DETAIL_CHARS,
  WORKSPACE_TRAY_MAX_ERROR_CHARS,
  WORKSPACE_TRAY_MAX_ROWS,
  WORKSPACE_TRAY_MAX_TERMINAL_RESULT_ROWS,
  WORKSPACE_TRAY_MAX_TITLE_CHARS,
  WORKSPACE_TRAY_REFRESH_INTERVAL_MS,
  WORKSPACE_TRAY_ROW_KINDS,
  WORKSPACE_TRAY_SNAPSHOT_PAGE_LIMIT,
  boundWorkspaceCompactTrayRows,
  normalizeWorkspaceChatTab,
  readWorkspaceChatArchivePreferences,
  resolveWorkspaceChatLifecycle,
  setWorkspaceChatArchivePreference,
  writeWorkspaceChatArchivePreferences,
} from './workspace-contract.ts';

export type {
  WorkspaceChatArchiveEntry,
  WorkspaceChatArchivePreferenceMap,
  WorkspaceChatIdentity,
  WorkspaceChatLifecycle,
  WorkspaceChatPreferenceStorage,
  WorkspaceChatSurfaceTab,
  WorkspaceCompactTrayRow,
  WorkspaceMainCloseBehavior,
  WorkspaceSnapshot,
  WorkspaceSurfaceSeparationContract,
  WorkspaceTrayAction,
  WorkspaceTrayContentBounds,
  WorkspaceTrayRowKind,
} from './workspace-contract.ts';

/** Stable UI boundary shared by independently implemented production panels. */
export interface ProfilesPanelProps {
  registry: RegistrySnapshot | null;
  loading: boolean;
  error: string | null;
  compact: boolean;
  onRegistryChanged: (snapshot: RegistrySnapshot) => void;
  onOpenTools: () => void;
}

export interface ToolBrandProps {
  adapter: ToolAdapter | null;
  size?: number;
}

/** Read-only observed identity; never accepts a caller-provided URI or shell command. */
export interface CodexChatTarget {
  source_id: string;
  thread_uuid: string;
  job_id: string | null;
}

/**
 * Canonical workspace chat target keyed by the verified four-part `WorkspaceChatIdentity`
 * (`source_id`, `binding_id`, `thread_uuid`, `canonical_project_scope`). Never substitutes
 * a chat title, `sessionId`, or `job_id` for chat identity.
 */
export interface WorkspaceScopedChatTarget {
  identity: WorkspaceChatIdentity;
  job_id: string | null;
}

export const OPEN_CODEX_CHAT_COMMAND = 'desktop_open_codex_chat' as const;
