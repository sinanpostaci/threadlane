import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  applyToolVersionScanError,
  applyToolVersionScanSuccess,
  beginStartupToolVersionScan,
  beginToolVersionScan,
  createToolVersionScanState,
  findToolVersionInspection,
  markToolVersionInspectionsStale,
  toolInspectionInputsChanged,
} from './tool-versions.ts';
import type {
  DesktopToolVersionsSnapshot,
  ToolDefinition,
  ToolVersionInspection,
} from './types.ts';

function makeTool(overrides: Partial<ToolDefinition> = {}): ToolDefinition {
  return {
    id: 'codex',
    label: 'Codex CLI',
    adapter: 'codex_cli',
    executable_path: null,
    enabled: true,
    ...overrides,
  };
}

function makeInspection(
  toolId: string,
  overrides: Partial<ToolVersionInspection> = {}
): ToolVersionInspection {
  return {
    tool_id: toolId,
    availability: 'found',
    selection_mode: 'system',
    configured_path: null,
    resolved_path: `/nix/var/nix/profiles/default/bin/${toolId}`,
    system_path: `/nix/var/nix/profiles/default/bin/${toolId}`,
    version: `${toolId} 1.2.3`,
    system_version: `${toolId} 1.2.3`,
    system_path_differs: false,
    message: 'Version command completed.',
    error_code: null,
    ...overrides,
  };
}

function makeResponse(
  registryRevision: number,
  tools: ToolVersionInspection[]
): DesktopToolVersionsSnapshot {
  return { registry_revision: registryRevision, tools };
}

describe('tool version scan orchestration', () => {
  it('admits the startup scan only once and allows explicit later rechecks', () => {
    const initial = createToolVersionScanState();
    const startup = beginStartupToolVersionScan(initial, 4);

    assert.ok(startup.ticket);
    assert.equal(startup.state.startupStarted, true);
    assert.equal(startup.ticket?.reason, 'startup');
    assert.equal(startup.ticket?.registryRevision, 4);

    const repeatedStartup = beginStartupToolVersionScan(startup.state, 4);
    assert.equal(repeatedStartup.ticket, null);
    assert.equal(repeatedStartup.state.startupStarted, true);

    const manual = beginToolVersionScan(repeatedStartup.state, 4, 'manual');
    assert.ok(manual.ticket);
    assert.equal(manual.ticket?.reason, 'manual');
    assert.equal(manual.state.status, 'loading');
    assert.notEqual(manual.state.activeTicketId, startup.state.activeTicketId);
  });

  it('does not re-admit the startup scan after it fails', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 4);
    const failed = applyToolVersionScanError(
      startup.state,
      startup.ticket!,
      'Version checks could not start.',
      4
    ).state;

    const retryStartup = beginStartupToolVersionScan(failed, 4);
    assert.equal(retryStartup.ticket, null);
    assert.equal(retryStartup.state, failed);
  });

  it('accepts the current response in backend order and records its revision', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);
    const tools = [
      makeInspection('codex'),
      makeInspection('opencode', {
        version: 'opencode 2.0.0',
        resolved_path: '/usr/bin/opencode',
        system_path: '/usr/bin/opencode',
        system_version: 'opencode 2.0.0',
      }),
    ];

    const settled = applyToolVersionScanSuccess(
      startup.state,
      startup.ticket!,
      makeResponse(7, tools),
      7
    );

    assert.equal(settled.accepted, true);
    assert.equal(settled.state.status, 'ready');
    assert.equal(settled.state.inspectedRegistryRevision, 7);
    assert.deepEqual(settled.state.inspections, tools);
    assert.equal(settled.state.activeTicketId, null);
    assert.equal(findToolVersionInspection(settled.state, 'opencode')?.version, 'opencode 2.0.0');
  });

  it('discards a late response when the current registry revision advanced', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);

    const settled = applyToolVersionScanSuccess(
      startup.state,
      startup.ticket!,
      makeResponse(7, [makeInspection('codex')]),
      8
    );

    assert.equal(settled.accepted, false);
    assert.equal(settled.state.status, 'stale');
    assert.deepEqual(settled.state.inspections, []);
    assert.equal(settled.state.inspectedRegistryRevision, null);
  });

  it('discards a response for a different registry revision than the ticket', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);

    const settled = applyToolVersionScanSuccess(
      startup.state,
      startup.ticket!,
      makeResponse(8, [makeInspection('codex')]),
      7
    );

    assert.equal(settled.accepted, false);
    assert.equal(settled.state.status, 'stale');
    assert.deepEqual(settled.state.inspections, []);
  });

  it('ignores a superseded request without disturbing the newer scan', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);
    const manual = beginToolVersionScan(startup.state, 7, 'after_save');

    const stale = applyToolVersionScanSuccess(
      manual.state,
      startup.ticket!,
      makeResponse(7, [makeInspection('old')]),
      7
    );

    assert.equal(stale.accepted, false);
    assert.equal(stale.state, manual.state);
    assert.equal(stale.state.status, 'loading');
    assert.equal(stale.state.activeTicketId, manual.ticket?.id ?? null);
  });

  it('settles an error only for the active ticket and current revision', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);

    const current = applyToolVersionScanError(
      startup.state,
      startup.ticket!,
      'Version checks timed out.',
      7
    );
    assert.equal(current.accepted, true);
    assert.equal(current.state.status, 'error');
    assert.equal(current.state.error, 'Version checks timed out.');

    const second = beginToolVersionScan(current.state, 7, 'manual');
    const supersededError = applyToolVersionScanError(
      second.state,
      startup.ticket!,
      'Old request failed.',
      7
    );
    assert.equal(supersededError.accepted, false);
    assert.equal(supersededError.state, second.state);

    const staleError = applyToolVersionScanError(
      second.state,
      second.ticket!,
      'Registry moved.',
      8
    );
    assert.equal(staleError.accepted, false);
    assert.equal(staleError.state.status, 'stale');
  });

  it('invalidates accepted results when the registry revision changes', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), 7);
    const ready = applyToolVersionScanSuccess(
      startup.state,
      startup.ticket!,
      makeResponse(7, [makeInspection('codex')]),
      7
    ).state;

    assert.equal(markToolVersionInspectionsStale(ready, 7), ready);

    const stale = markToolVersionInspectionsStale(ready, 8);
    assert.equal(stale.status, 'stale');
    assert.deepEqual(stale.inspections, []);
    assert.equal(stale.inspectedRegistryRevision, null);
  });

  it('does not start a request for an invalid registry revision', () => {
    const startup = beginStartupToolVersionScan(createToolVersionScanState(), Number.NaN);
    assert.equal(startup.ticket, null);
    assert.equal(startup.state.startupStarted, false);
    assert.equal(startup.state.status, 'idle');
  });
});

describe('tool inspection input invalidation', () => {
  it('keeps version metadata across label-only tool edits', () => {
    const previous = [makeTool({ label: 'Codex CLI' })];
    const next = [makeTool({ label: 'Codex (work)' })];

    assert.equal(toolInspectionInputsChanged(previous, next), false);
  });

  it('ignores API-only tool changes because they never launch version processes', () => {
    const api = makeTool({ id: 'jev-api', adapter: 'jev_api', enabled: true });
    const removed = makeTool({ id: 'api-old', adapter: 'openai_decisions_api' });

    assert.equal(
      toolInspectionInputsChanged(
        [api],
        [makeTool({ id: 'jev-api', adapter: 'jev_api', enabled: false })]
      ),
      false
    );
    assert.equal(toolInspectionInputsChanged([], [api]), false);
    assert.equal(toolInspectionInputsChanged([removed], []), false);
  });

  it('detects executable, enablement, adapter, and membership changes', () => {
    const base = [makeTool()];

    assert.equal(
      toolInspectionInputsChanged(base, [
        makeTool({ executable_path: '/nix/store/example/bin/codex' }),
      ]),
      true
    );
    assert.equal(toolInspectionInputsChanged(base, [makeTool({ enabled: false })]), true);
    assert.equal(
      toolInspectionInputsChanged(base, [makeTool({ adapter: 'opencode_v2' })]),
      true
    );
    assert.equal(
      toolInspectionInputsChanged(base, [makeTool(), makeTool({ id: 'git', adapter: 'git' })]),
      true
    );
    assert.equal(toolInspectionInputsChanged(base, []), true);
  });
});
