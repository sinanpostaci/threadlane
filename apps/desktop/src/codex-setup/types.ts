export type CodexSetupState =
  | 'checking'
  | 'registered'
  | 'unchanged'
  | 'not_registered'
  | 'disabled'
  | 'conflict'
  | 'error';

export type CodexSetupMode = 'check' | 'apply';

export interface CodexSetupReport {
  state: CodexSetupState;
  mode: CodexSetupMode;
  config_path: string | null;
  registered: boolean;
  config_modified: boolean;
  restart_required: boolean;
  pairing_required: boolean;
  error_code: string | null;
  message: string;
}

export type CodexSetupBadgeTone =
  | 'ready'
  | 'info'
  | 'warning'
  | 'danger'
  | 'neutral';

export interface CodexSetupViewModel {
  state: CodexSetupState;
  registrationBadgeLabel: string;
  registrationBadgeTone: CodexSetupBadgeTone;
  pairingBadgeLabel: string;
  pairingBadgeTone: CodexSetupBadgeTone;
  configPathLabel: string;
  summaryMessage: string;
  restartNotice: string | null;
  actionHint: string | null;
  canRecheck: boolean;
  canRetry: boolean;
  isBusy: boolean;
}

export const PAIRING_REQUIRED_BADGE_LABEL = 'Conversation pairing required';

export const RESTART_REQUIRED_NOTICE =
  'Restart or refresh your Codex MCP connection to load the updated mcp_servers.threadlane registration.';

export const BROWSER_PREVIEW_SETUP_MESSAGE =
  'Browser preview mode: automatic Codex MCP registration only runs in the native Threadlane desktop app.';

export function createInitialCodexSetupReport(): CodexSetupReport {
  return {
    state: 'checking',
    mode: 'apply',
    config_path: null,
    registered: false,
    config_modified: false,
    restart_required: false,
    pairing_required: true,
    error_code: null,
    message: 'Checking Codex MCP server registration in config.toml.',
  };
}

export function createBrowserPreviewCodexSetupReport(): CodexSetupReport {
  return {
    state: 'not_registered',
    mode: 'check',
    config_path: null,
    registered: false,
    config_modified: false,
    restart_required: false,
    pairing_required: true,
    error_code: null,
    message: BROWSER_PREVIEW_SETUP_MESSAGE,
  };
}

export function normalizeCodexSetupReport(raw: unknown): CodexSetupReport {
  if (!raw || typeof raw !== 'object') {
    return {
      state: 'error',
      mode: 'check',
      config_path: null,
      registered: false,
      config_modified: false,
      restart_required: false,
      pairing_required: true,
      error_code: 'invalid_response',
      message: 'Unable to read Codex MCP setup status from the desktop backend.',
    };
  }

  const obj = raw as Record<string, unknown>;
  const validStates: readonly CodexSetupState[] = [
    'checking',
    'registered',
    'unchanged',
    'not_registered',
    'disabled',
    'conflict',
    'error',
  ];
  const state: CodexSetupState =
    typeof obj.state === 'string' &&
    (validStates as readonly string[]).includes(obj.state)
      ? (obj.state as CodexSetupState)
      : 'error';

  const mode: CodexSetupMode = obj.mode === 'check' ? 'check' : 'apply';
  const configPath =
    typeof obj.config_path === 'string' && obj.config_path.trim().length > 0
      ? obj.config_path.trim()
      : null;
  const allowsPostCommitFlags =
    state === 'registered' ||
    (state === 'error' && obj.config_modified === true);
  const registered =
    (state === 'registered' || state === 'unchanged' || allowsPostCommitFlags) &&
    obj.registered === true;
  const configModified = allowsPostCommitFlags && obj.config_modified === true;
  const restartRequired =
    allowsPostCommitFlags && obj.restart_required === true;
  const errorCode =
    typeof obj.error_code === 'string' && obj.error_code.trim().length > 0
      ? obj.error_code.trim()
      : null;
  const message =
    typeof obj.message === 'string' && obj.message.trim().length > 0
      ? obj.message.trim()
      : defaultMessageForState(state);

  return {
    state,
    mode,
    config_path: configPath,
    registered,
    config_modified: configModified,
    restart_required: restartRequired,
    // Registration never implies an active or enrolled conversation.
    pairing_required: true,
    error_code: errorCode,
    message,
  };
}

function defaultMessageForState(state: CodexSetupState): string {
  switch (state) {
    case 'checking':
      return 'Checking Codex MCP server registration in config.toml.';
    case 'registered':
      return 'Registered mcp_servers.threadlane in Codex config.toml. Conversation pairing is still required.';
    case 'unchanged':
      return 'Codex mcp_servers.threadlane registration is already up to date. Conversation pairing is still required.';
    case 'not_registered':
      return 'Codex mcp_servers.threadlane is not yet registered in config.toml.';
    case 'disabled':
      return 'Codex mcp_servers.threadlane is disabled (enabled = false) in config.toml and was left disabled.';
    case 'conflict':
      return 'Codex mcp_servers.threadlane has existing manual or unmanaged settings in config.toml and was not modified.';
    case 'error':
      return 'Codex MCP setup requires operator attention.';
  }
}

export function projectCodexSetupViewModel(
  report: CodexSetupReport,
  options: { busy?: boolean; isBrowserPreview?: boolean } = {}
): CodexSetupViewModel {
  const isBusy = Boolean(options.busy) || report.state === 'checking';
  const isBrowserPreview = Boolean(options.isBrowserPreview);
  const configPathLabel = report.config_path ?? '~/.codex/config.toml';

  let registrationBadgeLabel: string;
  let registrationBadgeTone: CodexSetupBadgeTone;
  let actionHint: string | null = null;

  switch (report.state) {
    case 'checking':
      registrationBadgeLabel = 'Checking registration';
      registrationBadgeTone = 'info';
      break;
    case 'registered':
      if (report.error_code !== null) {
        registrationBadgeLabel = 'Registered (receipt warning)';
        registrationBadgeTone = 'warning';
        actionHint =
          'mcp_servers.threadlane was written to config.toml, but finalizing the ownership receipt or directory sync needs attention. Check management directory permissions before retrying.';
      } else {
        registrationBadgeLabel = 'Registered (updated)';
        registrationBadgeTone = 'ready';
      }
      break;
    case 'unchanged':
      registrationBadgeLabel = 'Registered';
      registrationBadgeTone = 'ready';
      break;
    case 'not_registered':
      registrationBadgeLabel = isBrowserPreview
        ? 'Preview only'
        : 'Not registered';
      registrationBadgeTone = 'neutral';
      if (!isBrowserPreview) {
        actionHint =
          'Click Retry setup to write the unbound mcp_servers.threadlane entry into config.toml.';
      }
      break;
    case 'disabled':
      registrationBadgeLabel = 'Disabled in config.toml';
      registrationBadgeTone = 'warning';
      actionHint =
        'Entry has enabled = false in config.toml and was left disabled without automatic re-enable.';
      break;
    case 'conflict':
      registrationBadgeLabel = 'Manual config conflict';
      registrationBadgeTone = 'warning';
      actionHint =
        'Existing manual or unmanaged mcp_servers.threadlane settings were preserved without overwrite.';
      break;
    case 'error':
      if (report.config_modified) {
        registrationBadgeLabel = 'Registered (receipt warning)';
        registrationBadgeTone = 'warning';
        actionHint =
          'mcp_servers.threadlane was written to config.toml, but finalizing the ownership receipt or directory sync needs attention. Check management directory permissions before retrying.';
      } else {
        registrationBadgeLabel = 'Needs attention';
        registrationBadgeTone = 'danger';
        actionHint =
          report.error_code === 'setup_busy'
            ? 'Another Threadlane process is currently checking or updating config.toml. Wait a moment and click Retry setup.'
            : 'Verify file permissions, management receipt state, and TOML syntax, then retry.';
      }
      break;
  }

  const restartNotice = report.restart_required
    ? RESTART_REQUIRED_NOTICE
    : null;

  return {
    state: report.state,
    registrationBadgeLabel,
    registrationBadgeTone,
    pairingBadgeLabel: PAIRING_REQUIRED_BADGE_LABEL,
    pairingBadgeTone: 'warning',
    configPathLabel,
    summaryMessage: report.message,
    restartNotice,
    actionHint,
    canRecheck: !isBrowserPreview && !isBusy,
    canRetry: !isBrowserPreview && !isBusy,
    isBusy,
  };
}
