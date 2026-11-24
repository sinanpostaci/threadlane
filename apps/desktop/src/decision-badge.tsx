import openaiSvgUrl from '@lobehub/icons-static-svg/icons/openai.svg';
import { Popover } from '@radix-ui/themes';
import { GitBranch } from 'lucide-react';

import type { WorkspaceDecisionBadge } from './observation/types.ts';
import { formatDurationMs } from './pages/activity-feed-model.ts';

function label(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function sourceLabel(badge: WorkspaceDecisionBadge): string | null {
  if (badge.advice_source === 'fresh') return 'Fresh provider response';
  if (badge.advice_source !== 'cached') return null;
  return badge.cache_age_ms ? `Cached advice (${formatDurationMs(badge.cache_age_ms)} ago)` : 'Cached advice';
}

interface DecisionBadgeProps {
  badge?: WorkspaceDecisionBadge | null;
  placement: 'stream' | 'drawer';
}

export function DecisionBadge({ badge, placement }: DecisionBadgeProps) {
  if (!badge) return null;
  const provider = badge.provider === 'jev' ? 'Jev' : 'OpenAI';
  let status = 'Pending';
  if (badge.status === 'result') {
    if (!badge.recommended_difficulty) return null;
    status = label(badge.recommended_difficulty);
  } else if (badge.status === 'failed') {
    status = 'Consultation failed';
  }

  const rows = [
    [badge.status === 'result' ? 'Recommended difficulty' : 'Consultation', status],
    ['Coworker estimate', badge.coworker_difficulty && label(badge.coworker_difficulty)],
    ['Decision model', badge.advisor_model],
    ['Source', sourceLabel(badge)],
    ['Reason', badge.failure_reason_code && label(badge.failure_reason_code.replace(/_/g, ' '))],
  ];
  return (
    <Popover.Root>
      <Popover.Trigger>
        <button
          type="button"
          className="tl-decision-badge"
          data-decision-badge="true"
          data-decision-placement={placement}
          data-decision-item-id={badge.item_id}
          data-decision-execution-ref={badge.execution_ref}
          data-decision-provider={badge.provider}
          data-decision-status={badge.status}
          aria-label={`Decision advice from ${provider}: ${status}`}
        >
          {badge.provider === 'openai'
            ? <img src={openaiSvgUrl} className="tl-brand-invert" alt="" width={12} height={12} />
            : <GitBranch size={12} aria-hidden="true" />}
          {provider} · {status}
        </button>
      </Popover.Trigger>
      <Popover.Content
        aria-label={`Decision advice from ${provider}`}
        data-decision-popover="true"
        side="bottom"
        align="start"
        sideOffset={5}
        onEscapeKeyDown={(event) => event.stopPropagation()}
      >
        <strong>Decision advice · {provider}</strong>
        <dl className="tl-decision-popover-list">
          {rows.map(([name, value]) => value && (
            <div className="tl-decision-popover-row" key={name}>
              <dt>{name}</dt><dd>{value}</dd>
            </div>
          ))}
        </dl>
      </Popover.Content>
    </Popover.Root>
  );
}
