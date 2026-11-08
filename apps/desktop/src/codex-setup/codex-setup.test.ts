import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyCodexSetup,
  fetchCodexSetupStatus,
  recheckCodexSetup,
} from './ipc.ts';
import {
  BROWSER_PREVIEW_SETUP_MESSAGE,
  BROWSER_PREVIEW_SKILL_SETUP_MESSAGE,
  createBrowserPreviewCodexSetupReport,
  createInitialCodexSetupReport,
  normalizeCodexSetupReport,
  PAIRING_REQUIRED_BADGE_LABEL,
  projectCodexSetupViewModel,
  projectCodexSkillSetupView,
  RESTART_REQUIRED_NOTICE,
  SKILL_INVOCATION_HINT,
  SKILL_SETUP_UNAVAILABLE_MESSAGE,
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

function makeRawSetupReport(
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    state: 'unchanged',
    mode: 'apply',
    config_path: '/home/operator/.codex/config.toml',
    registered: true,
    config_modified: false,
    restart_required: false,
    pairing_required: false,
    error_code: null,
    message: 'Codex setup status.',
    ...overrides,
  };
}

function makeSkillSetupReport(
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    state: 'installed',
    mode: 'apply',
    skill_path: '/home/operator/.agents/skills/threadlane',
    version: '1.0.0',
    installed: true,
    files_modified: true,
    error_code: null,
    message: 'Threadlane skill files are installed.',
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

  it('normalizes nested skill setup states and projects concise skill status', () => {
    const installed = normalizeCodexSetupReport(
      makeRawSetupReport({ skill_setup: makeSkillSetupReport() })
    );
    assert.equal(installed.skill_setup?.state, 'installed');
    assert.equal(installed.skill_setup?.mode, 'apply');
    assert.equal(
      installed.skill_setup?.skill_path,
      '/home/operator/.agents/skills/threadlane'
    );
    assert.equal(installed.skill_setup?.version, '1.0.0');
    assert.equal(installed.skill_setup?.installed, true);
    assert.equal(installed.skill_setup?.files_modified, true);
    assert.equal(installed.skill_setup?.error_code, null);
    assert.equal(
      installed.skill_setup?.message,
      'Threadlane skill files are installed.'
    );

    const installedVm = projectCodexSkillSetupView(installed.skill_setup);
    assert.equal(installedVm.badgeLabel, 'Skill installed');
    assert.equal(installedVm.badgeTone, 'ready');
    assert.equal(installedVm.invocationHint, SKILL_INVOCATION_HINT);

    const updated = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'updated',
          message: 'Threadlane skill files were updated.',
        }),
      })
    );
    assert.equal(updated.skill_setup?.state, 'updated');
    assert.equal(updated.skill_setup?.installed, true);
    assert.equal(updated.skill_setup?.files_modified, true);
    const updatedVm = projectCodexSkillSetupView(updated.skill_setup);
    assert.equal(updatedVm.badgeLabel, 'Skill updated');
    assert.equal(updatedVm.badgeTone, 'ready');
    assert.equal(updatedVm.invocationHint, SKILL_INVOCATION_HINT);

    const unchanged = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'unchanged',
          files_modified: false,
        }),
      })
    );
    const unchangedVm = projectCodexSkillSetupView(unchanged.skill_setup);
    assert.equal(unchangedVm.badgeLabel, 'Skill up to date');
    assert.equal(unchangedVm.badgeTone, 'ready');
    assert.equal(unchangedVm.invocationHint, SKILL_INVOCATION_HINT);

    const notInstalled = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'not_installed',
          installed: false,
          files_modified: false,
          message: 'Threadlane skill files are not installed.',
        }),
      })
    );
    const notInstalledVm = projectCodexSkillSetupView(
      notInstalled.skill_setup
    );
    assert.equal(notInstalledVm.badgeLabel, 'Skill not installed');
    assert.equal(notInstalledVm.badgeTone, 'warning');
    assert.equal(notInstalledVm.invocationHint, null);

    const outdated = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'outdated',
          files_modified: false,
          message: 'Threadlane skill files are outdated.',
        }),
      })
    );
    assert.equal(outdated.skill_setup?.installed, true);
    const outdatedVm = projectCodexSkillSetupView(outdated.skill_setup);
    assert.equal(outdatedVm.badgeLabel, 'Skill update available');
    assert.equal(outdatedVm.badgeTone, 'warning');
    assert.equal(outdatedVm.invocationHint, null);

    const conflict = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'conflict',
          installed: false,
          files_modified: false,
          message: 'Local skill edits were preserved.',
        }),
      })
    );
    const conflictVm = projectCodexSkillSetupView(conflict.skill_setup);
    assert.equal(
      conflictVm.badgeLabel,
      'Skill conflict (local edits preserved)'
    );
    assert.equal(conflictVm.badgeTone, 'warning');
    assert.equal(conflictVm.message, 'Local skill edits were preserved.');
    assert.equal(conflictVm.invocationHint, null);

    const error = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'error',
          installed: false,
          files_modified: false,
          error_code: 'skill_install_failed',
          message: 'Threadlane skill installation failed.',
        }),
      })
    );
    const errorVm = projectCodexSkillSetupView(error.skill_setup);
    assert.equal(errorVm.badgeLabel, 'Skill setup error');
    assert.equal(errorVm.badgeTone, 'danger');
    assert.equal(errorVm.invocationHint, null);
  });

  it('rejects incomplete, wrongly typed, or contradictory skill metadata', () => {
    const malformedMessage =
      'Threadlane skill installation status from the desktop backend was malformed.';
    const cases: Array<[string, Record<string, unknown>]> = [
      ['missing mode', { mode: undefined }],
      ['unknown mode', { mode: 'install' }],
      ['missing skill path', { skill_path: undefined }],
      ['null success path', { skill_path: null }],
      ['missing version', { version: undefined }],
      ['blank version', { version: '   ' }],
      ['missing installed flag', { installed: undefined }],
      ['uninstalled success', { installed: false }],
      ['missing files_modified flag', { files_modified: undefined }],
      ['missing error code', { error_code: undefined }],
      ['success with error code', { error_code: 'skill_warning' }],
      ['string installed flag', { installed: 'true' }],
      ['string files_modified flag', { files_modified: 'true' }],
      ['numeric skill path', { skill_path: 42 }],
      ['numeric version', { version: 1 }],
      ['numeric error code', { error_code: 7 }],
    ];

    for (const [name, overrides] of cases) {
      const report = normalizeCodexSetupReport(
        makeRawSetupReport({
          skill_setup: makeSkillSetupReport(overrides),
        })
      );
      assert.equal(report.skill_setup?.state, 'error', name);
      assert.equal(report.skill_setup?.mode, 'check', name);
      assert.equal(report.skill_setup?.installed, false, name);
      assert.equal(report.skill_setup?.files_modified, false, name);
      assert.equal(report.skill_setup?.skill_path, null, name);
      assert.equal(report.skill_setup?.version, 'unknown', name);
      assert.equal(report.skill_setup?.error_code, 'invalid_response', name);
      assert.equal(report.skill_setup?.message, malformedMessage, name);
      const vm = projectCodexSkillSetupView(report.skill_setup);
      assert.equal(vm.badgeLabel, 'Skill setup error', name);
      assert.equal(vm.badgeTone, 'danger', name);
      assert.equal(vm.invocationHint, null, name);
    }
  });

  it('reports a missing legacy skill report truthfully without claiming installation', () => {
    const legacy = normalizeCodexSetupReport(makeRawSetupReport());
    assert.equal(legacy.mode, 'apply');
    assert.equal(legacy.skill_setup, null);

    const unavailableVm = projectCodexSkillSetupView(legacy.skill_setup);
    assert.equal(unavailableVm.state, 'unknown');
    assert.equal(unavailableVm.badgeLabel, 'Skill status unavailable');
    assert.equal(unavailableVm.badgeTone, 'neutral');
    assert.equal(unavailableVm.pathLabel, 'Skill path unavailable');
    assert.equal(unavailableVm.message, SKILL_SETUP_UNAVAILABLE_MESSAGE);
    assert.equal(unavailableVm.invocationHint, null);
    assert.ok(!unavailableVm.badgeLabel.includes('Installed'));

    const initial = createInitialCodexSetupReport();
    const checkingVm = projectCodexSkillSetupView(initial.skill_setup, {
      isChecking: initial.state === 'checking',
    });
    assert.equal(checkingVm.badgeLabel, 'Checking skill status');
    assert.equal(checkingVm.badgeTone, 'info');
    assert.equal(checkingVm.invocationHint, null);
  });

  it('rejects malformed nested skill metadata while preserving observed non-success flags', () => {
    const malformed = normalizeCodexSetupReport(
      makeRawSetupReport({ skill_setup: 'installed' })
    );
    assert.equal(malformed.skill_setup?.state, 'error');
    assert.equal(malformed.skill_setup?.installed, false);
    assert.equal(malformed.skill_setup?.files_modified, false);
    assert.equal(malformed.skill_setup?.error_code, 'invalid_response');
    assert.equal(
      projectCodexSkillSetupView(malformed.skill_setup).badgeLabel,
      'Skill setup error'
    );

    const conflictPostWrite = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'conflict',
          installed: false,
          files_modified: true,
          message: 'Local edits preserved.',
        }),
      })
    );
    assert.equal(conflictPostWrite.skill_setup?.state, 'conflict');
    assert.equal(conflictPostWrite.skill_setup?.installed, false);
    assert.equal(conflictPostWrite.skill_setup?.files_modified, true);
    const conflictPostWriteVm = projectCodexSkillSetupView(
      conflictPostWrite.skill_setup
    );
    assert.equal(conflictPostWriteVm.badgeTone, 'warning');
    assert.equal(conflictPostWriteVm.invocationHint, null);

    const bundledButUnowned = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'not_installed',
          installed: true,
          files_modified: false,
        }),
      })
    );
    assert.equal(bundledButUnowned.skill_setup?.state, 'not_installed');
    assert.equal(bundledButUnowned.skill_setup?.installed, true);
    assert.equal(bundledButUnowned.skill_setup?.files_modified, false);
    const bundledButUnownedVm = projectCodexSkillSetupView(
      bundledButUnowned.skill_setup
    );
    assert.equal(bundledButUnownedVm.badgeLabel, 'Skill not installed');
    assert.equal(bundledButUnownedVm.badgeTone, 'warning');
    assert.equal(bundledButUnownedVm.invocationHint, null);

    const unknownState = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'installed_spoof',
          installed: true,
          files_modified: true,
          skill_path: '/tmp/spoofed-skill',
        }),
      })
    );
    assert.equal(unknownState.skill_setup?.state, 'error');
    assert.equal(unknownState.skill_setup?.installed, false);
    assert.equal(unknownState.skill_setup?.files_modified, false);
    assert.equal(unknownState.skill_setup?.skill_path, null);
    assert.equal(unknownState.skill_setup?.error_code, 'invalid_response');
  });

  it('keeps skill installation independent from MCP registration and conversation pairing', () => {
    const report = normalizeCodexSetupReport(
      makeRawSetupReport({
        state: 'not_registered',
        registered: true,
        config_modified: true,
        restart_required: true,
        pairing_required: false,
        error_code: 'not_registered',
        message: 'MCP entry is not registered.',
        skill_setup: makeSkillSetupReport(),
      })
    );

    assert.equal(report.state, 'not_registered');
    assert.equal(report.registered, false);
    assert.equal(report.config_modified, false);
    assert.equal(report.restart_required, false);
    assert.equal(report.pairing_required, true);
    assert.equal(report.skill_setup?.installed, true);
    assert.equal(report.skill_setup?.files_modified, true);

    const mcpVm = projectCodexSetupViewModel(report);
    assert.equal(mcpVm.registrationBadgeLabel, 'Not registered');
    assert.equal(mcpVm.pairingBadgeLabel, PAIRING_REQUIRED_BADGE_LABEL);
    assert.equal(mcpVm.pairingBadgeTone, 'warning');

    const skillVm = projectCodexSkillSetupView(report.skill_setup);
    assert.equal(skillVm.badgeLabel, 'Skill installed');
    assert.equal(skillVm.invocationHint, SKILL_INVOCATION_HINT);
  });

  it('preserves a legitimate post-write skill error for attention without suggesting use', () => {
    const postWrite = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'error',
          installed: true,
          files_modified: true,
          error_code: 'skill_receipt_failed',
          message:
            'Wrote Threadlane skill files but failed to persist the ownership receipt.',
        }),
      })
    );
    assert.equal(postWrite.skill_setup?.state, 'error');
    assert.equal(postWrite.skill_setup?.installed, true);
    assert.equal(postWrite.skill_setup?.files_modified, true);
    assert.equal(postWrite.skill_setup?.error_code, 'skill_receipt_failed');

    const postWriteVm = projectCodexSkillSetupView(postWrite.skill_setup);
    assert.equal(postWriteVm.badgeLabel, 'Skill setup error (files written)');
    assert.equal(postWriteVm.badgeTone, 'danger');
    assert.equal(postWriteVm.invocationHint, null);
    assert.equal(
      postWriteVm.message,
      'Wrote Threadlane skill files but failed to persist the ownership receipt.'
    );

    const unwritten = normalizeCodexSetupReport(
      makeRawSetupReport({
        skill_setup: makeSkillSetupReport({
          state: 'error',
          installed: true,
          files_modified: false,
          error_code: 'skill_install_failed',
        }),
      })
    );
    assert.equal(unwritten.skill_setup?.installed, false);
    assert.equal(unwritten.skill_setup?.files_modified, false);
    assert.equal(
      projectCodexSkillSetupView(unwritten.skill_setup).badgeLabel,
      'Skill setup error'
    );
  });

  it('reports browser preview skill status as preview only without simulating installation', () => {
    const preview = createBrowserPreviewCodexSetupReport();
    const previewVm = projectCodexSkillSetupView(preview.skill_setup, {
      isBrowserPreview: true,
    });
    assert.equal(previewVm.state, 'unknown');
    assert.equal(previewVm.badgeLabel, 'Preview only');
    assert.equal(previewVm.badgeTone, 'neutral');
    assert.equal(previewVm.pathLabel, 'Unavailable in browser preview');
    assert.equal(previewVm.message, BROWSER_PREVIEW_SKILL_SETUP_MESSAGE);
    assert.equal(previewVm.invocationHint, null);
    assert.ok(!previewVm.badgeLabel.includes('Installed'));

    const overriddenVm = projectCodexSkillSetupView(
      {
        state: 'installed',
        mode: 'apply',
        skill_path: '/tmp/fake-skill',
        version: '1.0.0',
        installed: true,
        files_modified: true,
        error_code: null,
        message: 'Installed.',
      },
      { isBrowserPreview: true }
    );
    assert.equal(overriddenVm.badgeLabel, 'Preview only');
    assert.equal(overriddenVm.invocationHint, null);
  });
});
