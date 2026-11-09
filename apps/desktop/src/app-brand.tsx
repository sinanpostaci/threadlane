import antigravitySvgUrl from '@lobehub/icons-static-svg/icons/antigravity-color.svg';
import deepseekSvgUrl from '@lobehub/icons-static-svg/icons/deepseek-color.svg';
import geminiSvgUrl from '@lobehub/icons-static-svg/icons/gemini-color.svg';
import openaiSvgUrl from '@lobehub/icons-static-svg/icons/openai.svg';
import {
  Bot,
  GitBranch,
  Network,
  SquareTerminal,
  Terminal,
} from 'lucide-react';

import type { ToolBrandProps } from './app-contract.ts';

export type ProviderBrandKey =
  | 'openai'
  | 'antigravity'
  | 'opencode'
  | 'gemini'
  | 'deepseek'
  | 'git'
  | 'tmux'
  | 'codegraph'
  | 'generic';

export function resolveProviderBrandKey(
  providerOrAdapter?: string | null,
  model?: string | null
): ProviderBrandKey {
  const combined = `${providerOrAdapter ?? ''} ${model ?? ''}`.toLowerCase();
  if (combined.includes('gemini')) {
    return 'gemini';
  }
  if (combined.includes('deepseek')) {
    return 'deepseek';
  }
  if (combined.includes('antigravity') || /\bagy\b/.test(combined)) {
    return 'antigravity';
  }
  if (
    combined.includes('codex') ||
    combined.includes('openai') ||
    combined.includes('gpt-')
  ) {
    return 'openai';
  }
  if (combined.includes('opencode')) {
    return 'opencode';
  }
  if (combined.includes('codegraph')) {
    return 'codegraph';
  }
  if (combined.includes('tmux')) {
    return 'tmux';
  }
  if (/\bgit\b/.test(combined)) {
    return 'git';
  }
  return 'generic';
}

export function ToolBrand({ adapter, size = 18 }: ToolBrandProps) {
  const boxStyle = { width: size + 10, height: size + 10 };
  const imgStyle = { width: size, height: size };

  if (adapter === 'codex_cli' || adapter === 'openai_decisions_api') {
    return (
      <span className="tl-brand-badge" data-brand="openai" style={boxStyle} aria-hidden="true">
        <img
          src={openaiSvgUrl}
          alt=""
          className="tl-brand-svg tl-brand-invert"
          style={imgStyle}
        />
      </span>
    );
  }

  if (adapter === 'antigravity') {
    return (
      <span
        className="tl-brand-badge"
        data-brand="antigravity"
        style={boxStyle}
        aria-hidden="true"
      >
        <img src={antigravitySvgUrl} alt="" className="tl-brand-svg" style={imgStyle} />
      </span>
    );
  }

  if (adapter === 'opencode_v2') {
    return (
      <span className="tl-brand-badge" data-brand="opencode" style={boxStyle} aria-hidden="true">
        <Terminal size={size - 2} />
      </span>
    );
  }

  if (adapter === 'git') {
    return (
      <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
        <GitBranch size={size - 2} />
      </span>
    );
  }

  if (adapter === 'tmux') {
    return (
      <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
        <SquareTerminal size={size - 2} />
      </span>
    );
  }

  if (adapter === 'codegraph') {
    return (
      <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
        <Network size={size - 2} />
      </span>
    );
  }

  return (
    <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
      <Bot size={size - 2} />
    </span>
  );
}

export interface ProviderBrandBadgeProps {
  brand: ProviderBrandKey;
  size?: number;
}

export function ProviderBrandBadge({ brand, size = 16 }: ProviderBrandBadgeProps) {
  const boxStyle = { width: size + 10, height: size + 10 };
  const imgStyle = { width: size, height: size };

  switch (brand) {
    case 'openai':
      return (
        <span className="tl-brand-badge" data-brand="openai" style={boxStyle} aria-hidden="true">
          <img
            src={openaiSvgUrl}
            alt=""
            className="tl-brand-svg tl-brand-invert"
            style={imgStyle}
          />
        </span>
      );
    case 'antigravity':
      return (
        <span
          className="tl-brand-badge"
          data-brand="antigravity"
          style={boxStyle}
          aria-hidden="true"
        >
          <img src={antigravitySvgUrl} alt="" className="tl-brand-svg" style={imgStyle} />
        </span>
      );
    case 'gemini':
      return (
        <span className="tl-brand-badge" data-brand="gemini" style={boxStyle} aria-hidden="true">
          <img src={geminiSvgUrl} alt="" className="tl-brand-svg" style={imgStyle} />
        </span>
      );
    case 'deepseek':
      return (
        <span className="tl-brand-badge" data-brand="deepseek" style={boxStyle} aria-hidden="true">
          <img src={deepseekSvgUrl} alt="" className="tl-brand-svg" style={imgStyle} />
        </span>
      );
    case 'opencode':
      return (
        <span
          className="tl-brand-badge"
          data-brand="opencode"
          style={boxStyle}
          aria-hidden="true"
        >
          <Terminal size={size - 2} />
        </span>
      );
    case 'git':
      return (
        <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
          <GitBranch size={size - 2} />
        </span>
      );
    case 'tmux':
      return (
        <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
          <SquareTerminal size={size - 2} />
        </span>
      );
    case 'codegraph':
      return (
        <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
          <Network size={size - 2} />
        </span>
      );
    default:
      return (
        <span className="tl-brand-badge" data-brand="generic" style={boxStyle} aria-hidden="true">
          <Bot size={size - 2} />
        </span>
      );
  }
}
