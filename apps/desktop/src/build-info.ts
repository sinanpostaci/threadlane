/**
 * Pure build identity parsing and display projection for the desktop Settings
 * surface. This module has no DOM, Tauri, or other runtime imports, so it can
 * be validated directly under `node --experimental-strip-types --test`.
 *
 * Packaged builds receive exactly one Vite-visible value derived from the
 * unfiltered flake source. The value is a full clean commit, or the explicit
 * `dirty` marker; every missing, unknown, or malformed value stays `unknown`.
 * No paths, credentials, or other environment entries are read.
 */

// Nix sets this exact name. The loader below spells the Vite property access
// out literally so the production build can replace just this variable.
export const DESKTOP_BUILD_IDENTITY_ENV =
  'VITE_THREADLANE_DESKTOP_BUILD_IDENTITY' as const;

export const DESKTOP_BUILD_SHORT_REVISION_LENGTH = 12;

export type DesktopBuildIdentityKind = 'revision' | 'dirty' | 'unknown';

export interface DesktopBuildIdentity {
  kind: DesktopBuildIdentityKind;
  /**
   * Full validated commit revision, never truncated. Null for dirty or
   * unknown sources so a stale or fabricated HEAD cannot be presented.
   */
  revision: string | null;
  /** Short, safe value for the compact Settings row. */
  displayValue: string;
  /** English status label for the Settings row. */
  statusLabel: string;
}

const FULL_REVISION_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const DIRTY_SOURCE_MARKER = 'dirty';

function unknownBuildIdentity(): DesktopBuildIdentity {
  return {
    kind: 'unknown',
    revision: null,
    displayValue: 'unknown',
    statusLabel: 'Unknown source',
  };
}

export function parseDesktopBuildIdentity(raw: unknown): DesktopBuildIdentity {
  if (typeof raw !== 'string') {
    return unknownBuildIdentity();
  }

  const value = raw.trim();
  if (value === DIRTY_SOURCE_MARKER) {
    return {
      kind: 'dirty',
      revision: null,
      displayValue: 'dirty',
      statusLabel: 'Dirty source',
    };
  }

  if (FULL_REVISION_PATTERN.test(value)) {
    return {
      kind: 'revision',
      revision: value,
      displayValue: value.slice(0, DESKTOP_BUILD_SHORT_REVISION_LENGTH),
      statusLabel: 'Clean source',
    };
  }

  return unknownBuildIdentity();
}

export function readDesktopBuildIdentity(): DesktopBuildIdentity {
  try {
    // Keep the property access static so Vite replaces exactly this variable
    // instead of inlining a broader environment object.
    return parseDesktopBuildIdentity(
      import.meta.env.VITE_THREADLANE_DESKTOP_BUILD_IDENTITY
    );
  } catch {
    return unknownBuildIdentity();
  }
}
