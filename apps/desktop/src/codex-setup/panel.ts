import { createButton, h } from '../ui/dom.ts';
import {
  projectCodexSetupViewModel,
  type CodexSetupBadgeTone,
  type CodexSetupReport,
} from './types.ts';

export interface CodexSetupPanelProps {
  report: CodexSetupReport;
  busy: boolean;
  isBrowserPreview: boolean;
  onRecheck: () => void;
  onRetry: () => void;
}

function badgeDataStatus(tone: CodexSetupBadgeTone): string | undefined {
  switch (tone) {
    case 'ready':
      return 'active';
    case 'info':
      return 'info';
    case 'warning':
      return 'warn';
    case 'danger':
      return 'error';
    case 'neutral':
      return undefined;
  }
}

/**
 * Renders the compact, quiet Codex MCP Startup Registration panel inside `Settings > Connections`.
 * Displays `mcp_servers.threadlane` registration state separately from `Conversation pairing required`
 * and never claims an active MCP connection, enrolled thread, or verified runtime session.
 */
export function buildCodexSetupPanel(props: CodexSetupPanelProps): HTMLElement {
  const vm = projectCodexSetupViewModel(props.report, {
    busy: props.busy,
    isBrowserPreview: props.isBrowserPreview,
  });

  const badgeWrapStyle =
    'max-width: 100%; white-space: normal; overflow-wrap: anywhere;';

  const regStatus = badgeDataStatus(vm.registrationBadgeTone);
  const regBadge = h(
    'span',
    {
      className: 'aq-badge',
      style: badgeWrapStyle,
      ...(regStatus ? { 'data-status': regStatus } : {}),
      'data-codex-setup-badge': 'registration',
    },
    vm.registrationBadgeLabel
  );

  const pairingBadge = h(
    'span',
    {
      className: 'aq-badge',
      style: badgeWrapStyle,
      'data-status': 'warn',
      'data-codex-setup-badge': 'pairing',
    },
    vm.pairingBadgeLabel
  );

  const sectionHeader = h(
    'div',
    {
      className: 'aq-section',
      style:
        'margin-top: 14px; margin-bottom: 6px; flex-wrap: wrap; min-width: 0; max-width: 100%;',
    },
    h(
      'span',
      { style: 'min-width: 0; max-width: 100%; overflow-wrap: anywhere;' },
      'Codex MCP startup registration'
    )
  );

  const statusBadgeRow = h(
    'div',
    {
      'data-codex-setup-status-row': 'true',
      style:
        'display: flex; flex-wrap: wrap; align-items: center; gap: 6px; min-width: 0; max-width: 100%; margin-bottom: 8px;',
    },
    regBadge,
    pairingBadge
  );

  const codeWrapStyle =
    'display: inline-block; max-width: 100%; white-space: normal; overflow-wrap: anywhere; word-break: break-word;';

  const card = h(
    'section',
    {
      className: 'aq-card codex-setup-panel',
      style: 'min-width: 0; max-width: 100%; box-sizing: border-box;',
      'aria-label': 'Codex MCP startup registration',
      'data-codex-setup-state': vm.state,
    },
    sectionHeader,
    statusBadgeRow,
    h('p', { className: 'aq-note' }, vm.summaryMessage),
    h(
      'dl',
      {
        className: 'aq-facts',
        style: 'margin: 6px 0 8px; min-width: 0; max-width: 100%;',
      },
      h(
        'div',
        { style: 'flex-wrap: wrap; min-width: 0; max-width: 100%;' },
        h('dt', undefined, 'MCP entry'),
        h(
          'dd',
          { style: 'min-width: 0; max-width: 100%;' },
          h('code', { style: codeWrapStyle }, 'mcp_servers.threadlane')
        )
      ),
      h(
        'div',
        { style: 'flex-wrap: wrap; min-width: 0; max-width: 100%;' },
        h('dt', undefined, 'Config path'),
        h(
          'dd',
          { style: 'min-width: 0; max-width: 100%;' },
          h('code', { style: codeWrapStyle }, vm.configPathLabel)
        )
      )
    )
  );

  if (vm.restartNotice) {
    card.appendChild(
      h(
        'p',
        {
          className: 'aq-note',
          'data-codex-setup-restart-notice': 'true',
        },
        vm.restartNotice
      )
    );
  }

  if (vm.actionHint) {
    card.appendChild(
      h(
        'p',
        {
          className: 'aq-note',
          'data-codex-setup-hint': 'true',
        },
        vm.actionHint
      )
    );
  }

  const recheckBtn = createButton(
    vm.isBusy ? 'Checking…' : 'Recheck',
    () => {
      if (vm.canRecheck) {
        props.onRecheck();
      }
    },
    !vm.canRecheck
  );
  recheckBtn.dataset.focus = 'codex-setup-recheck';

  const retryBtn = createButton(
    'Retry setup',
    () => {
      if (vm.canRetry) {
        props.onRetry();
      }
    },
    !vm.canRetry
  );
  retryBtn.dataset.focus = 'codex-setup-retry';

  card.appendChild(
    h(
      'div',
      {
        className: 'aq-actions',
        style: 'flex-wrap: wrap; min-width: 0; max-width: 100%;',
      },
      recheckBtn,
      retryBtn
    )
  );

  return card;
}
