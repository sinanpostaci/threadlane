import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DESKTOP_BUILD_IDENTITY_ENV,
  DESKTOP_BUILD_SHORT_REVISION_LENGTH,
  parseDesktopBuildIdentity,
  readDesktopBuildIdentity,
} from './build-info.ts';

const CLEAN_REVISION_40 = '0123456789abcdef0123456789abcdef01234567';
const CLEAN_REVISION_64 =
  '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';

test('keeps a full clean revision and shortens only the display value', () => {
  for (const revision of [CLEAN_REVISION_40, CLEAN_REVISION_64]) {
    const identity = parseDesktopBuildIdentity(revision);
    assert.equal(identity.kind, 'revision');
    assert.equal(identity.revision, revision);
    assert.equal(identity.statusLabel, 'Clean source');
    assert.equal(
      identity.displayValue,
      revision.slice(0, DESKTOP_BUILD_SHORT_REVISION_LENGTH)
    );
    assert.equal(
      identity.displayValue.length,
      DESKTOP_BUILD_SHORT_REVISION_LENGTH
    );
    assert.ok(identity.displayValue.length < revision.length);
  }
});

test('marks a dirty source without inventing a revision', () => {
  const identity = parseDesktopBuildIdentity('dirty');
  assert.equal(identity.kind, 'dirty');
  assert.equal(identity.revision, null);
  assert.equal(identity.displayValue, 'dirty');
  assert.equal(identity.statusLabel, 'Dirty source');
});

test('treats missing, unknown, and malformed metadata as unknown', () => {
  const malformedInputs: unknown[] = [
    undefined,
    null,
    42,
    '',
    '   ',
    'main',
    'HEAD',
    'unknown',
    'dirty-tree',
    'dirty 0123456789abcdef0123456789abcdef01234567',
    '0123456789abcdef0123456789abcdef0123456',
    '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0',
    '0123456789ABCDEF0123456789ABCDEF01234567',
    '0123456789abcdef0123456789abcdef0123456g',
  ];

  for (const raw of malformedInputs) {
    const identity = parseDesktopBuildIdentity(raw);
    assert.equal(identity.kind, 'unknown', String(raw));
    assert.equal(identity.revision, null);
    assert.equal(identity.displayValue, 'unknown');
    assert.equal(identity.statusLabel, 'Unknown source');
  }
});

test('never renders arbitrary input as build identity text', () => {
  const identity = parseDesktopBuildIdentity(
    '<img src=x onerror="alert(1)">'
  );
  assert.equal(identity.kind, 'unknown');
  assert.equal(identity.revision, null);
  assert.equal(identity.displayValue, 'unknown');
  assert.ok(!identity.displayValue.includes('<'));
});

test('reads the packaged Vite variable defensively', () => {
  assert.equal(
    DESKTOP_BUILD_IDENTITY_ENV,
    'VITE_THREADLANE_DESKTOP_BUILD_IDENTITY'
  );

  // Node tests do not define import.meta.env; the loader must not throw and
  // must fall back to an explicit unknown identity.
  const identity = readDesktopBuildIdentity();
  assert.equal(identity.kind, 'unknown');
  assert.equal(identity.revision, null);
  assert.equal(identity.displayValue, 'unknown');
});
