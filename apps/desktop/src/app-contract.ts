import type { RegistrySnapshot, ToolAdapter } from './registry/types.ts';

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

export const OPEN_CODEX_CHAT_COMMAND = 'desktop_open_codex_chat' as const;
