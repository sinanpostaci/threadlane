export type CodexSetupState =
  | 'checking'
  | 'registered'
  | 'unchanged'
  | 'not_registered'
  | 'disabled'
  | 'conflict'
  | 'error';

export type CodexSetupMode = 'check' | 'apply';

export type CodexSkillSetupState =
  | 'installed'
  | 'updated'
  | 'unchanged'
  | 'not_installed'
  | 'outdated'
  | 'conflict'
  | 'error';

export type CodexSkillSetupMode = 'check' | 'apply';

export interface CodexSkillSetupReport {
  state: CodexSkillSetupState;
  mode: CodexSkillSetupMode;
  skill_path: string | null;
  version: string;
  installed: boolean;
  files_modified: boolean;
  error_code: string | null;
  message: string;
}

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
  /** Present only when the desktop backend reports bundled skill installation. */
  skill_setup?: CodexSkillSetupReport | null;
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

export interface CodexSkillSetupViewModel {
  state: CodexSkillSetupState | 'unknown';
  badgeLabel: string;
  badgeTone: CodexSetupBadgeTone;
  pathLabel: string;
  message: string;
  invocationHint: string | null;
}

export const PAIRING_REQUIRED_BADGE_LABEL = 'Conversation pairing required';

export const RESTART_REQUIRED_NOTICE =
  'Restart or refresh your Codex MCP connection to load the updated mcp_servers.threadlane registration.';

export const BROWSER_PREVIEW_SETUP_MESSAGE =
  'Browser preview mode: automatic Codex MCP registration only runs in the native Threadlane desktop app.';

export const SKILL_INVOCATION_HINT =
  'Use $threadlane in a new Codex chat.';

export const BROWSER_PREVIEW_SKILL_SETUP_MESSAGE =
  'Browser preview mode: automatic Threadlane skill installation only runs in the native Threadlane desktop app.';

export const SKILL_SETUP_UNAVAILABLE_MESSAGE =
  'Skill installation status was not reported by this desktop backend. Use Recheck to refresh it.';

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
      skill_setup: null,
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
    skill_setup: normalizeCodexSkillSetup(obj.skill_setup),
  };
}

function invalidCodexSkillSetup(): CodexSkillSetupReport {
  return {
    state: 'error',
    mode: 'check',
    skill_path: null,
    version: 'unknown',
    installed: false,
    files_modified: false,
    error_code: 'invalid_response',
    message:
      'Threadlane skill installation status from the desktop backend was malformed.',
  };
}

function normalizeCodexSkillSetup(raw: unknown): CodexSkillSetupReport | null {
  if (raw === undefined || raw === null) {
    return null;
  }

  if (typeof raw !== 'object' || Array.isArray(raw)) {
    return {
      state: 'error',
      mode: 'check',
      skill_path: null,
      version: 'unknown',
      installed: false,
      files_modified: false,
      error_code: 'invalid_response',
      message:
        'Unable to read Threadlane skill installation status from the desktop backend.',
    };
  }

  const obj = raw as Record<string, unknown>;
  const validStates: readonly CodexSkillSetupState[] = [
    'installed',
    'updated',
    'unchanged',
    'not_installed',
    'outdated',
    'conflict',
    'error',
  ];
  const hasValidState =
    typeof obj.state === 'string' &&
    (validStates as readonly string[]).includes(obj.state);
  const hasValidMode = obj.mode === 'check' || obj.mode === 'apply';
  const hasValidVersion =
    typeof obj.version === 'string' && obj.version.trim().length > 0;
  const hasValidInstalled = typeof obj.installed === 'boolean';
  const hasValidFilesModified = typeof obj.files_modified === 'boolean';
  const hasValidSkillPath =
    obj.skill_path === null || typeof obj.skill_path === 'string';
  const hasValidErrorCode =
    obj.error_code === null || typeof obj.error_code === 'string';

  if (
    !hasValidState ||
    !hasValidMode ||
    !hasValidVersion ||
    !hasValidInstalled ||
    !hasValidFilesModified ||
    !hasValidSkillPath ||
    !hasValidErrorCode
  ) {
    return invalidCodexSkillSetup();
  }

  const state = obj.state as CodexSkillSetupState;
  const mode: CodexSkillSetupMode = obj.mode === 'check' ? 'check' : 'apply';
  const skillPath =
    typeof obj.skill_path === 'string' && obj.skill_path.trim().length > 0
      ? obj.skill_path.trim()
      : null;
  const version = (obj.version as string).trim();
  const reportedErrorCode =
    typeof obj.error_code === 'string' && obj.error_code.trim().length > 0
      ? obj.error_code.trim()
      : null;
  const reportedMessage =
    typeof obj.message === 'string' && obj.message.trim().length > 0
      ? obj.message.trim()
      : null;
  const isSuccessState =
    state === 'installed' ||
    state === 'updated' ||
    state === 'unchanged';

  if (
    isSuccessState &&
    (obj.installed !== true ||
      skillPath === null ||
      reportedErrorCode !== null)
  ) {
    return invalidCodexSkillSetup();
  }

  const postWriteError = state === 'error' && obj.files_modified === true;
  const allowsInstalled =
    isSuccessState ||
    state === 'outdated' ||
    state === 'not_installed' ||
    postWriteError;
  const installed = allowsInstalled && obj.installed === true;
  // files_modified records an observed side effect, not install authority.
  const filesModified = obj.files_modified === true;
  const errorCode = reportedErrorCode;
  const message = reportedMessage ?? defaultSkillMessageForState(state);

  return {
    state,
    mode,
    skill_path: skillPath,
    version,
    installed,
    files_modified: filesModified,
    error_code: errorCode,
    message,
  };
}

function defaultSkillMessageForState(state: CodexSkillSetupState): string {
  switch (state) {
    case 'installed':
      return 'Threadlane skill files are installed. Use $threadlane in a new Codex chat.';
    case 'updated':
      return 'Threadlane skill files were updated. Use $threadlane in a new Codex chat.';
    case 'unchanged':
      return 'Threadlane skill files are already up to date. Use $threadlane in a new Codex chat.';
    case 'not_installed':
      return 'Threadlane skill files are not installed in the Codex skills directory.';
    case 'outdated':
      return 'Threadlane skill files are outdated and need an update.';
    case 'conflict':
      return 'Threadlane skill files conflict with local edits and were preserved without overwrite.';
    case 'error':
      return 'Threadlane skill setup requires operator attention.';
  }
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

export function projectCodexSkillSetupView(
  skillSetup: CodexSkillSetupReport | null | undefined,
  options: { isBrowserPreview?: boolean; isChecking?: boolean } = {}
): CodexSkillSetupViewModel {
  if (options.isBrowserPreview) {
    return {
      state: 'unknown',
      badgeLabel: 'Preview only',
      badgeTone: 'neutral',
      pathLabel: 'Unavailable in browser preview',
      message: BROWSER_PREVIEW_SKILL_SETUP_MESSAGE,
      invocationHint: null,
    };
  }

  if (!skillSetup) {
    if (options.isChecking) {
      return {
        state: 'unknown',
        badgeLabel: 'Checking skill status',
        badgeTone: 'info',
        pathLabel: 'Skill path pending',
        message: 'Checking Threadlane skill installation status.',
        invocationHint: null,
      };
    }

    return {
      state: 'unknown',
      badgeLabel: 'Skill status unavailable',
      badgeTone: 'neutral',
      pathLabel: 'Skill path unavailable',
      message: SKILL_SETUP_UNAVAILABLE_MESSAGE,
      invocationHint: null,
    };
  }

  const pathLabel = skillSetup.skill_path ?? 'Skill path unavailable';
  const provenInstalled =
    skillSetup.installed === true &&
    (skillSetup.state === 'installed' ||
      skillSetup.state === 'updated' ||
      skillSetup.state === 'unchanged');
  const invocationHint = provenInstalled ? SKILL_INVOCATION_HINT : null;

  let badgeLabel: string;
  let badgeTone: CodexSetupBadgeTone;

  switch (skillSetup.state) {
    case 'installed':
      badgeLabel = 'Skill installed';
      badgeTone = 'ready';
      break;
    case 'updated':
      badgeLabel = 'Skill updated';
      badgeTone = 'ready';
      break;
    case 'unchanged':
      badgeLabel = 'Skill up to date';
      badgeTone = 'ready';
      break;
    case 'not_installed':
      badgeLabel = 'Skill not installed';
      badgeTone = 'warning';
      break;
    case 'outdated':
      badgeLabel = 'Skill update available';
      badgeTone = 'warning';
      break;
    case 'conflict':
      badgeLabel = 'Skill conflict (local edits preserved)';
      badgeTone = 'warning';
      break;
    case 'error':
      badgeLabel = skillSetup.files_modified
        ? 'Skill setup error (files written)'
        : 'Skill setup error';
      badgeTone = 'danger';
      break;
  }

  return {
    state: skillSetup.state,
    badgeLabel,
    badgeTone,
    pathLabel,
    message: skillSetup.message,
    invocationHint,
  };
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
