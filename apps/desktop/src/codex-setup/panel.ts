import { createUiIcon } from '../icons.ts';
import { createButton, h } from '../ui/dom.ts';
import {
  projectCodexSetupViewModel,
  projectCodexSkillSetupView,
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

  const skillVm = projectCodexSkillSetupView(props.report.skill_setup, {
    isBrowserPreview: props.isBrowserPreview,
    isChecking: vm.isBusy,
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
        'margin-top: 10px; margin-bottom: 6px; flex-wrap: wrap; min-width: 0; max-width: 100%;',
    },
    h(
      'span',
      { style: 'min-width: 0; max-width: 100%; overflow-wrap: anywhere;' },
      'Codex setup'
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

  const skillStatus = badgeDataStatus(skillVm.badgeTone);
  const skillBadge = h(
    'span',
    {
      className: 'aq-badge',
      style: badgeWrapStyle,
      ...(skillStatus ? { 'data-status': skillStatus } : {}),
      'data-codex-skill-badge': skillVm.state,
    },
    skillVm.badgeLabel
  );

  const skillSection = h(
    'div',
    {
      'data-codex-skill-section': 'true',
      'data-codex-skill-state': skillVm.state,
      style: 'margin: 8px 0 6px; min-width: 0; max-width: 100%;',
    },
    h(
      'div',
      {
        className: 'aq-section',
        style:
          'margin-bottom: 6px; flex-wrap: wrap; min-width: 0; max-width: 100%;',
      },
      h(
        'span',
        {
          style: 'min-width: 0; max-width: 100%; overflow-wrap: anywhere;',
        },
        'Threadlane skill'
      ),
      skillBadge
    ),
    h('p', { className: 'aq-note' }, skillVm.message)
  );

  if (skillVm.invocationHint) {
    skillSection.appendChild(
      h(
        'p',
        {
          className: 'aq-note',
          'data-codex-skill-hint': 'true',
        },
        skillVm.invocationHint
      )
    );
  }

  const setupDetails = h(
    'details',
    {
      className: 'aq-technical',
      'data-codex-setup-details': 'true',
      style: 'margin-top: 8px;',
    },
    h(
      'summary',
      { 'data-focus': 'codex-setup-details' },
      h('span', undefined, 'Setup details'),
      createUiIcon('chevron-down')
    ),
    h(
      'dl',
      {
        className: 'aq-facts',
        style: 'margin: 4px 0 6px; min-width: 0; max-width: 100%;',
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
      ),
      h(
        'div',
        { style: 'flex-wrap: wrap; min-width: 0; max-width: 100%;' },
        h('dt', undefined, 'Skill path'),
        h(
          'dd',
          { style: 'min-width: 0; max-width: 100%;' },
          h('code', { style: codeWrapStyle }, skillVm.pathLabel)
        )
      )
    )
  );

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
    h('p', { className: 'aq-note' }, vm.summaryMessage)
  );

  card.appendChild(skillSection);

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
  card.appendChild(setupDetails);

  return card;
}
