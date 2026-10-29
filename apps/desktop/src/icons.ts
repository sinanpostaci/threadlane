import openaiSvgUrl from '@lobehub/icons-static-svg/icons/openai.svg';
import opencodeSvgUrl from '@lobehub/icons-static-svg/icons/opencode.svg';
import antigravityColorSvgUrl from '@lobehub/icons-static-svg/icons/antigravity-color.svg';
import geminiColorSvgUrl from '@lobehub/icons-static-svg/icons/gemini-color.svg';
import deepseekColorSvgUrl from '@lobehub/icons-static-svg/icons/deepseek-color.svg';

import type { BrandKey } from './types.ts';

const SVG_NS = 'http://www.w3.org/2000/svg';

export type UiIconName =
  | 'activity'
  | 'messages-square'
  | 'blocks'
  | 'sliders-horizontal'
  | 'settings-2'
  | 'arrow-left'
  | 'chevron-right'
  | 'chevron-down'
  | 'list-filter'
  | 'corner-up-right'
  | 'refresh-cw'
  | 'alert-circle'
  | 'minus'
  | 'x'
  | 'play'
  | 'check'
  | 'ban'
  | 'terminal';

const COLOR_BRAND_URLS: Record<'antigravity' | 'gemini' | 'deepseek', string> = {
  antigravity: antigravityColorSvgUrl,
  gemini: geminiColorSvgUrl,
  deepseek: deepseekColorSvgUrl,
};

const MASK_BRAND_URLS: Record<'openai' | 'opencode', string> = {
  openai: openaiSvgUrl,
  opencode: opencodeSvgUrl,
};

export function createBrandIconElement(brand: BrandKey): HTMLElement {
  if (brand === 'openai' || brand === 'opencode') {
    const span = document.createElement('span');
    span.className = 'aq-brand aq-mask';
    span.setAttribute('aria-hidden', 'true');
    span.style.setProperty('--aq-logo', `url("${MASK_BRAND_URLS[brand]}")`);
    return span;
  }

  if (brand === 'antigravity' || brand === 'gemini' || brand === 'deepseek') {
    const img = document.createElement('img');
    img.className = 'aq-brand';
    img.src = COLOR_BRAND_URLS[brand];
    img.alt = '';
    img.setAttribute('aria-hidden', 'true');
    img.decoding = 'async';
    return img;
  }

  const wrapper = document.createElement('span');
  wrapper.className = 'aq-brand aq-brand-fallback';
  wrapper.setAttribute('aria-hidden', 'true');
  wrapper.appendChild(createUiIcon(brand === 'demo' ? 'play' : 'terminal'));
  return wrapper;
}

function appendPath(svg: SVGSVGElement, d: string): void {
  const path = document.createElementNS(SVG_NS, 'path');
  path.setAttribute('d', d);
  svg.appendChild(path);
}

function appendCircle(svg: SVGSVGElement, cx: string, cy: string, r: string): void {
  const circle = document.createElementNS(SVG_NS, 'circle');
  circle.setAttribute('cx', cx);
  circle.setAttribute('cy', cy);
  circle.setAttribute('r', r);
  svg.appendChild(circle);
}

function appendRect(
  svg: SVGSVGElement,
  x: string,
  y: string,
  width: string,
  height: string,
  rx?: string
): void {
  const rect = document.createElementNS(SVG_NS, 'rect');
  rect.setAttribute('x', x);
  rect.setAttribute('y', y);
  rect.setAttribute('width', width);
  rect.setAttribute('height', height);
  if (rx) {
    rect.setAttribute('rx', rx);
  }
  svg.appendChild(rect);
}

export function createUiIcon(name: UiIconName): SVGSVGElement {
  const svg = document.createElementNS(SVG_NS, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('fill', 'none');
  svg.setAttribute('stroke', 'currentColor');
  svg.setAttribute('stroke-width', '1.7');
  svg.setAttribute('stroke-linecap', 'round');
  svg.setAttribute('stroke-linejoin', 'round');
  svg.setAttribute('aria-hidden', 'true');

  switch (name) {
    case 'activity':
      appendPath(svg, 'M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2');
      break;
    case 'messages-square':
      appendPath(svg, 'M14 9a2 2 0 0 1-2 2H6l-4 4V4a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2z');
      appendPath(svg, 'M18 9h2a2 2 0 0 1 2 2v11l-4-4h-6a2 2 0 0 1-2-2v-1');
      break;
    case 'blocks':
      appendRect(svg, '14', '2', '8', '8', '1');
      appendPath(svg, 'M10 2H4a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6a2 2 0 0 0-2-2h-8V4a2 2 0 0 0-2-2Z');
      appendPath(svg, 'M2 12h10');
      appendPath(svg, 'M12 12v10');
      break;
    case 'sliders-horizontal':
      appendPath(svg, 'M21 4h-7');
      appendPath(svg, 'M10 4H3');
      appendPath(svg, 'M21 12h-9');
      appendPath(svg, 'M8 12H3');
      appendPath(svg, 'M21 20h-5');
      appendPath(svg, 'M12 20H3');
      appendPath(svg, 'M14 2v4');
      appendPath(svg, 'M8 10v4');
      appendPath(svg, 'M16 18v4');
      break;
    case 'settings-2':
      appendPath(svg, 'M20 7h-9');
      appendPath(svg, 'M14 17H5');
      appendCircle(svg, '17', '17', '3');
      appendCircle(svg, '7', '7', '3');
      break;
    case 'arrow-left':
      appendPath(svg, 'm12 19-7-7 7-7');
      appendPath(svg, 'M19 12H5');
      break;
    case 'chevron-right':
      appendPath(svg, 'm9 18 6-6-6-6');
      break;
    case 'chevron-down':
      appendPath(svg, 'm6 9 6 6 6-6');
      break;
    case 'list-filter':
      appendPath(svg, 'M3 6h18');
      appendPath(svg, 'M7 12h10');
      appendPath(svg, 'M10 18h4');
      break;
    case 'corner-up-right':
      appendPath(svg, 'm15 14 5-5-5-5');
      appendPath(svg, 'M4 20v-7a4 4 0 0 1 4-4h12');
      break;
    case 'refresh-cw':
      appendPath(svg, 'M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8');
      appendPath(svg, 'M21 3v5h-5');
      appendPath(svg, 'M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16');
      appendPath(svg, 'M8 16H3v5');
      break;
    case 'alert-circle':
      appendCircle(svg, '12', '12', '10');
      appendPath(svg, 'M12 8v4');
      appendPath(svg, 'M12 16h.01');
      break;
    case 'minus':
      appendPath(svg, 'M5 12h14');
      break;
    case 'x':
      appendPath(svg, 'M18 6 6 18');
      appendPath(svg, 'm6 6 12 12');
      break;
    case 'play':
      appendPath(svg, 'm6 4 14 8-14 8V4z');
      break;
    case 'check':
      appendPath(svg, 'M20 6 9 17l-5-5');
      break;
    case 'ban':
      appendCircle(svg, '12', '12', '10');
      appendPath(svg, 'm4.9 4.9 14.2 14.2');
      break;
    case 'terminal':
      appendPath(svg, 'm4 17 6-6-6-6');
      appendPath(svg, 'M12 19h8');
      break;
  }

  return svg;
}
