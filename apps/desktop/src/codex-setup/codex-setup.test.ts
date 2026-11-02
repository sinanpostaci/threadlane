import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyCodexSetup,
  fetchCodexSetupStatus,
  recheckCodexSetup,
} from './ipc.ts';
import {
  BROWSER_PREVIEW_SETUP_MESSAGE,
  createBrowserPreviewCodexSetupReport,
  createInitialCodexSetupReport,
  normalizeCodexSetupReport,
  PAIRING_REQUIRED_BADGE_LABEL,
  projectCodexSetupViewModel,
  RESTART_REQUIRED_NOTICE,
  type CodexSetupReport,
} from './types.ts';

function makeReport(
  overrides: Partial<CodexSetupReport> = {}
): CodexSetupReport {
  return {
    state: 'unchanged',
    mode: 'apply',
    config_path: '/home/operator/.codex/config.toml',
    registered: true,
    config_modified: false,
    restart_required: false,
    pairing_required: true,
    error_code: null,
    message:
      'Codex mcp_servers.threadlane registration is already up to date. Conversation pairing is still required before scoped tools can run.',
    ...overrides,
  };
}

describe('Codex MCP startup setup frontend model and IPC', () => {
  it('distinguishes changed registration (restart required) from unchanged registration while always requiring pairing', () => {
    const changed = makeReport({
      state: 'registered',
      registered: true,
      config_modified: true,
      restart_required: true,
    });
    const changedVm = projectCodexSetupViewModel(changed);
    assert.equal(changedVm.registrationBadgeLabel, 'Registered (updated)');
    assert.equal(changedVm.registrationBadgeTone, 'ready');
    assert.equal(changedVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(changedVm.restartNotice, RESTART_REQUIRED_NOTICE);
    assert.equal(changedVm.canRecheck, true);
    assert.equal(changedVm.canRetry, true);

    const unchanged = makeReport({
      state: 'unchanged',
      registered: true,
      config_modified: false,
      restart_required: false,
    });
    const unchangedVm = projectCodexSetupViewModel(unchanged);
    assert.equal(unchangedVm.registrationBadgeLabel, 'Registered');
    assert.equal(unchangedVm.registrationBadgeTone, 'ready');
    assert.equal(unchangedVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(unchangedVm.restartNotice, null);
  });

  it('keeps disabled, manual conflict, error, and checking states explicit without claiming registration', () => {
    const initial = createInitialCodexSetupReport();
    assert.equal(initial.state, 'checking');
    assert.equal(initial.registered, false);
    assert.equal(initial.pairing_required, true);
    const checkingVm = projectCodexSetupViewModel(initial);
    assert.equal(checkingVm.registrationBadgeLabel, 'Checking registration');
    assert.equal(checkingVm.isBusy, true);
    assert.equal(checkingVm.canRecheck, false);
    assert.equal(checkingVm.canRetry, false);

    const disabled = makeReport({
      state: 'disabled',
      registered: false,
      config_modified: false,
      restart_required: false,
    });
    const disabledVm = projectCodexSetupViewModel(disabled);
    assert.equal(disabledVm.registrationBadgeLabel, 'Disabled in config.toml');
    assert.equal(disabledVm.registrationBadgeTone, 'warning');
    assert.equal(disabledVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(disabledVm.restartNotice, null);
    assert.ok(disabledVm.actionHint?.includes('enabled = false'));

    const conflict = makeReport({
      state: 'conflict',
      registered: false,
      config_modified: false,
      restart_required: false,
      error_code: 'manual_conflict',
    });
    const conflictVm = projectCodexSetupViewModel(conflict);
    assert.equal(conflictVm.registrationBadgeLabel, 'Manual config conflict');
    assert.equal(conflictVm.registrationBadgeTone, 'warning');
    assert.equal(conflictVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.ok(conflictVm.actionHint?.includes('preserved without overwrite'));

    const error = makeReport({
      state: 'error',
      registered: false,
      config_modified: false,
      restart_required: false,
      error_code: 'malformed_config',
    });
    const errorVm = projectCodexSetupViewModel(error);
    assert.equal(errorVm.registrationBadgeLabel, 'Needs attention');
    assert.equal(errorVm.registrationBadgeTone, 'danger');
    assert.equal(errorVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(errorVm.restartNotice, null);
    assert.ok(!errorVm.actionHint?.includes('did not modify config.toml'));

    const busyError = makeReport({
      state: 'error',
      registered: false,
      config_modified: false,
      restart_required: false,
      error_code: 'setup_busy',
    });
    const busyVm = projectCodexSetupViewModel(busyError);
    assert.equal(busyVm.registrationBadgeLabel, 'Needs attention');
    assert.equal(busyVm.registrationBadgeTone, 'danger');
    assert.equal(busyVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.ok(busyVm.actionHint?.includes('Another Threadlane process'));
  });

  it('projects post-commit receipt warning truthfully while preserving restart notice and pairing requirement', () => {
    const postCommitRaw = normalizeCodexSetupReport({
      state: 'registered',
      mode: 'apply',
      config_path: '/home/operator/.codex/config.toml',
      registered: true,
      config_modified: true,
      restart_required: true,
      pairing_required: false,
      error_code: 'receipt_persistence_failed',
      message:
        'Updated mcp_servers.threadlane in Codex config.toml, but failed to persist the private ownership receipt.',
    });

    assert.equal(postCommitRaw.state, 'registered');
    assert.equal(postCommitRaw.registered, true);
    assert.equal(postCommitRaw.config_modified, true);
    assert.equal(postCommitRaw.restart_required, true);
    assert.equal(postCommitRaw.pairing_required, true);
    assert.equal(postCommitRaw.error_code, 'receipt_persistence_failed');

    const postCommitVm = projectCodexSetupViewModel(postCommitRaw);
    assert.equal(
      postCommitVm.registrationBadgeLabel,
      'Registered (receipt warning)'
    );
    assert.equal(postCommitVm.registrationBadgeTone, 'warning');
    assert.equal(postCommitVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(postCommitVm.pairingBadgeTone, 'warning');
    assert.equal(postCommitVm.restartNotice, RESTART_REQUIRED_NOTICE);
    assert.ok(
      postCommitVm.actionHint?.includes(
        'written to config.toml, but finalizing the ownership receipt'
      )
    );
  });

  it('normalizes malformed backend responses safely and enforces pairing_required: true', () => {
    const normalizedMalformed = normalizeCodexSetupReport(null);
    assert.equal(normalizedMalformed.state, 'error');
    assert.equal(normalizedMalformed.registered, false);
    assert.equal(normalizedMalformed.pairing_required, true);

    const normalizedSpoofed = normalizeCodexSetupReport({
      state: 'conflict',
      mode: 'apply',
      config_path: '/tmp/.codex/config.toml',
      registered: true,
      config_modified: true,
      restart_required: true,
      pairing_required: false,
      error_code: 'manual_conflict',
      message: 'Conflict detected.',
    });
    assert.equal(normalizedSpoofed.state, 'conflict');
    assert.equal(normalizedSpoofed.registered, false);
    assert.equal(normalizedSpoofed.config_modified, false);
    assert.equal(normalizedSpoofed.restart_required, false);
    assert.equal(normalizedSpoofed.pairing_required, true);
  });

  it('never simulates success or executes setup in browser preview mode', async () => {
    const previewReport = createBrowserPreviewCodexSetupReport();
    assert.equal(previewReport.state, 'not_registered');
    assert.equal(previewReport.registered, false);
    assert.equal(previewReport.config_modified, false);
    assert.equal(previewReport.restart_required, false);
    assert.equal(previewReport.pairing_required, true);
    assert.equal(previewReport.message, BROWSER_PREVIEW_SETUP_MESSAGE);

    const previewVm = projectCodexSetupViewModel(previewReport, {
      isBrowserPreview: true,
    });
    assert.equal(previewVm.registrationBadgeLabel, 'Preview only');
    assert.equal(previewVm.canRecheck, false);
    assert.equal(previewVm.canRetry, false);

    // Without window.__TAURI_INTERNALS__, IPC helpers return the preview report without throwing or simulating registration.
    const status = await fetchCodexSetupStatus();
    assert.equal(status.registered, false);
    assert.equal(status.state, 'not_registered');
    assert.equal(status.pairing_required, true);

    const rechecked = await recheckCodexSetup();
    assert.equal(rechecked.registered, false);
    assert.equal(rechecked.state, 'not_registered');

    const retried = await applyCodexSetup();
    assert.equal(retried.registered, false);
    assert.equal(retried.state, 'not_registered');
  });
});
