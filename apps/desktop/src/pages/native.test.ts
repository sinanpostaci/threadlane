import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  NATIVE_THREAD_RETAINED_LIMIT,
  NativeThreadsController,
  nativeToolBlockReason,
  type NativeThreadsQuery,
} from './native-logic.ts';
import type {
  DesktopNativeThreadsArgs,
  NativeThread,
  NativeThreadPage,
  RegistrySnapshot,
  ToolDefinition,
} from '../registry/types.ts';
import type { BindingSummary } from '../types.ts';

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function makeTool(
  id: string,
  overrides: Partial<ToolDefinition> = {}
): ToolDefinition {
  return {
    id,
    label: `Codex ${id}`,
    adapter: 'codex_cli',
    executable_path: '/usr/bin/codex',
    enabled: true,
    ...overrides,
  };
}

function makeRegistry(
  revision: number = 1,
  tools: ToolDefinition[] = [
    makeTool('codex-a'),
    makeTool('codex-b'),
    makeTool('opencode-main', {
      adapter: 'opencode_v2',
      executable_path: '/usr/bin/opencode',
    }),
    makeTool('agy-main', {
      adapter: 'antigravity',
      executable_path: '/usr/bin/agy',
    }),
  ]
): RegistrySnapshot {
  return {
    schema_version: 1,
    revision,
    tools,
    models: [],
    profiles: [],
    default_profile_id: null,
  };
}

function makeThread(
  id: string,
  overrides: Partial<NativeThread> = {}
): NativeThread {
  return {
    id,
    project_id: 'threadlane',
    title: `Thread ${id}`,
    cwd: `/tmp/${id}`,
    status: 'idle',
    model: 'gpt-6-astra',
    effort: 'max',
    ...overrides,
  };
}

function makePage(
  threads: NativeThread[],
  nextCursor: string | null = null
): NativeThreadPage {
  return { threads, next_cursor: nextCursor };
}

function makeBinding(
  threadUuid: string,
  bindingId: string = 'bind-1'
): BindingSummary {
  return {
    binding_id: bindingId,
    thread_uuid: threadUuid,
    label: 'Local binding',
    is_local_demo: false,
  };
}

interface FakeApi {
  calls: DesktopNativeThreadsArgs[];
  pending: Deferred<NativeThreadPage>[];
  query: NativeThreadsQuery;
}

function makeFakeApi(): FakeApi {
  const calls: DesktopNativeThreadsArgs[] = [];
  const pending: Deferred<NativeThreadPage>[] = [];
  return {
    calls,
    pending,
    query: (args) => {
      calls.push({ ...args });
      const reply = deferred<NativeThreadPage>();
      pending.push(reply);
      return reply.promise;
    },
  };
}

function makeController(api: FakeApi): NativeThreadsController {
  const controller = new NativeThreadsController(api.query, {
    active: true,
    formatError: (err) =>
      err instanceof Error ? err.message : 'Native metadata query failed.',
  });
  controller.sync(makeRegistry(), []);
  return controller;
}

describe('Native metadata request/context controller', () => {
  it('never reads native metadata without an explicit query', async () => {
    const api = makeFakeApi();
    const controller = new NativeThreadsController(api.query, { active: true });

    controller.sync(makeRegistry(), [makeBinding('thread-1')]);
    controller.setActive(true);
    controller.sync(makeRegistry(), [makeBinding('thread-2')]);
    controller.updateProjectDraft('threadlane');
    controller.sync(makeRegistry(), [makeBinding('thread-3')]);

    assert.equal(api.calls.length, 0);
    assert.equal(controller.getState().hasQueried, false);
    assert.equal(controller.getState().loading, false);

    const loadedMore = await controller.load('more');
    assert.equal(loadedMore, false);
    assert.equal(api.calls.length, 0);

    controller.setActive(false);
    const hiddenLoad = await controller.load('fresh');
    assert.equal(hiddenLoad, false);
    assert.equal(api.calls.length, 0);
  });

  it('queries an enabled Codex tool with executable_path null and paginates its cursor', async () => {
    const api = makeFakeApi();
    const controller = new NativeThreadsController(api.query, {
      active: true,
      formatError: (err) =>
        err instanceof Error ? err.message : 'Native metadata query failed.',
    });
    controller.sync(
      makeRegistry(1, [makeTool('codex-a', { executable_path: null })]),
      []
    );

    assert.equal(nativeToolBlockReason(controller.getState()), null);

    const first = controller.load('fresh');
    assert.deepEqual(api.calls[0], {
      toolId: 'codex-a',
      projectId: null,
      cursor: null,
    });
    api.pending[0]!.resolve(makePage([makeThread('thread-1')], 'cursor-1'));
    await first;

    let state = controller.getState();
    assert.equal(state.loading, false);
    assert.equal(state.appliedContext?.toolId, 'codex-a');
    assert.equal(state.threads.length, 1);
    assert.equal(state.nextCursor, 'cursor-1');

    const second = controller.load('more');
    assert.deepEqual(api.calls[1], {
      toolId: 'codex-a',
      projectId: null,
      cursor: 'cursor-1',
    });
    api.pending[1]!.resolve(makePage([makeThread('thread-2')], null));
    await second;

    state = controller.getState();
    assert.equal(state.threads.length, 2);
    assert.equal(state.nextCursor, null);
    assert.equal(state.errorMessage, null);
  });

  it('invalidates an in-flight PATH-discovered query when the configured path changes at the same ID and revision', async () => {
    const api = makeFakeApi();
    const controller = new NativeThreadsController(api.query, {
      active: true,
      formatError: (err) =>
        err instanceof Error ? err.message : 'Native metadata query failed.',
    });
    const registry = makeRegistry(1, [
      makeTool('codex-a', { executable_path: null }),
    ]);
    controller.sync(registry, []);

    const pending = controller.load('fresh');
    assert.equal(api.calls.length, 1);

    registry.tools[0]!.executable_path = '/usr/bin/codex';
    controller.sync(registry, []);

    assert.equal(api.calls.length, 1);
    let state = controller.getState();
    assert.equal(state.loading, false);
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
    assert.equal(state.requestContext, null);
    assert.equal(state.appliedContext, null);

    api.pending[0]!.resolve(makePage([makeThread('stale')], 'stale-cursor'));
    await pending;

    state = controller.getState();
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
    assert.equal(state.errorMessage, null);
    assert.equal(state.hasQueried, false);
    assert.equal(api.calls.length, 1);
  });

  it('does not issue overlapping requests while one query is in flight', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);

    const first = controller.load('fresh');
    const second = await controller.load('fresh');
    const third = await controller.load('more');
    assert.equal(second, false);
    assert.equal(third, false);
    assert.equal(api.calls.length, 1);

    api.pending[0]!.resolve(makePage([makeThread('thread-1')], null));
    await first;
    assert.equal(controller.getState().threads.length, 1);
  });

  it('discards stale success and error replies after tool, filter, registry, or visibility changes', async () => {
    const scenarios: Array<{
      name: string;
      invalidate: (controller: NativeThreadsController) => void;
    }> = [
      {
        name: 'tool',
        invalidate: (controller) => controller.selectTool('codex-b'),
      },
      {
        name: 'filter',
        invalidate: (controller) => controller.updateProjectDraft('other-project'),
      },
      {
        name: 'registry',
        invalidate: (controller) => controller.sync(makeRegistry(2), []),
      },
      {
        name: 'hidden',
        invalidate: (controller) => controller.setActive(false),
      },
    ];

    for (const scenario of scenarios) {
      for (const outcome of ['success', 'error'] as const) {
        const api = makeFakeApi();
        const controller = makeController(api);
        const pending = controller.load('fresh');
        assert.equal(api.calls.length, 1);

        scenario.invalidate(controller);
        if (outcome === 'success') {
          api.pending[0]!.resolve(
            makePage([makeThread('stale-thread')], 'stale-cursor')
          );
        } else {
          api.pending[0]!.reject(new Error('stale failure'));
        }
        await pending;

        const state = controller.getState();
        const label = `${scenario.name} ${outcome}`;
        assert.equal(state.threads.length, 0, `${label}: no stale threads`);
        assert.equal(state.nextCursor, null, `${label}: no stale cursor`);
        assert.equal(state.errorMessage, null, `${label}: no stale error`);
        assert.equal(state.requestContext, null, `${label}: no stale request`);
        assert.equal(state.appliedContext, null, `${label}: no stale view`);
        assert.equal(state.hasQueried, false, `${label}: no stale completion`);
        assert.equal(state.loading, false, `${label}: not loading`);
      }
    }
  });

  it('does not let an older tool reply overwrite a newer query result', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    const older = controller.load('fresh');
    controller.selectTool('codex-b');
    const newer = controller.load('fresh');

    assert.deepEqual(api.calls[1], {
      toolId: 'codex-b',
      projectId: null,
      cursor: null,
    });
    api.pending[1]!.resolve(makePage([makeThread('new-thread')], null));
    await newer;
    api.pending[0]!.resolve(makePage([makeThread('old-thread')], null));
    await older;

    const state = controller.getState();
    assert.deepEqual(
      state.threads.map((thread) => thread.id),
      ['new-thread']
    );
    assert.equal(state.selectedToolId, 'codex-b');
  });

  it('invalidates in-flight work when the selected tool becomes disabled', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    const pending = controller.load('fresh');

    controller.sync(
      makeRegistry(2, [
        makeTool('codex-a', { enabled: false }),
        makeTool('codex-b'),
        makeTool('opencode-main', {
          adapter: 'opencode_v2',
          executable_path: '/usr/bin/opencode',
        }),
      ]),
      []
    );
    api.pending[0]!.resolve(makePage([makeThread('thread-a')]));
    await pending;

    const state = controller.getState();
    assert.equal(state.selectedToolId, 'codex-a');
    assert.equal(state.threads.length, 0);
    assert.equal(state.appliedContext, null);
    assert.match(nativeToolBlockReason(state) ?? '', /disabled/);

    const reloaded = await controller.load('fresh');
    assert.equal(reloaded, false);
    assert.equal(api.calls.length, 1);
    assert.deepEqual(
      state.codexTools.map((tool) => tool.id),
      ['codex-a', 'codex-b']
    );

    controller.selectTool('opencode-main');
    assert.equal(controller.getState().selectedToolId, 'codex-a');
  });

  it('invalidates in-flight work when the selected tool adapter or executable changes', async () => {
    const executableApi = makeFakeApi();
    const executableController = makeController(executableApi);
    const executablePending = executableController.load('fresh');
    executableController.sync(
      makeRegistry(1, [
        makeTool('codex-a', { executable_path: '/usr/bin/codex-next' }),
        makeTool('codex-b'),
      ]),
      []
    );
    executableApi.pending[0]!.resolve(makePage([makeThread('stale')]));
    await executablePending;

    let state = executableController.getState();
    assert.equal(state.registryRevision, 1);
    assert.equal(state.selectedToolId, 'codex-a');
    assert.equal(state.threads.length, 0);
    assert.equal(state.appliedContext, null);

    const adapterApi = makeFakeApi();
    const adapterController = makeController(adapterApi);
    const adapterPending = adapterController.load('fresh');
    adapterController.sync(
      makeRegistry(1, [
        makeTool('codex-a', { adapter: 'opencode_v2' }),
        makeTool('codex-b'),
      ]),
      []
    );
    adapterApi.pending[0]!.resolve(makePage([makeThread('stale')]));
    await adapterPending;

    state = adapterController.getState();
    assert.equal(state.selectedToolId, 'codex-b');
    assert.equal(state.threads.length, 0);
    assert.equal(state.appliedContext, null);
  });

  it('loads the next page only with the applied cursor and matching context', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    controller.updateProjectDraft('project-1');

    const first = controller.load('fresh');
    assert.deepEqual(api.calls[0], {
      toolId: 'codex-a',
      projectId: 'project-1',
      cursor: null,
    });
    api.pending[0]!.resolve(makePage([makeThread('thread-1')], 'cursor-1'));
    await first;
    assert.equal(controller.getState().nextCursor, 'cursor-1');

    const second = controller.load('more');
    assert.deepEqual(api.calls[1], {
      toolId: 'codex-a',
      projectId: 'project-1',
      cursor: 'cursor-1',
    });

    controller.updateProjectDraft('project-2');
    assert.equal(controller.getState().nextCursor, null);
    assert.equal(await controller.load('more'), false);
    assert.equal(api.calls.length, 2);

    api.pending[1]!.resolve(makePage([makeThread('thread-2')], 'cursor-2'));
    await second;

    const state = controller.getState();
    assert.equal(state.projectDraft, 'project-2');
    assert.equal(state.appliedProjectFilter, null);
    assert.equal(state.appliedContext, null);
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
  });

  it('deduplicates thread IDs within one page and across appended pages', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);

    const first = controller.load('fresh');
    api.pending[0]!.resolve(
      makePage(
        [makeThread('thread-1'), makeThread('thread-1'), makeThread('thread-2')],
        'cursor-1'
      )
    );
    await first;
    assert.deepEqual(
      controller.getState().threads.map((thread) => thread.id),
      ['thread-1', 'thread-2']
    );

    const second = controller.load('more');
    api.pending[1]!.resolve(
      makePage(
        [makeThread('thread-2'), makeThread('thread-3'), makeThread('thread-3')],
        null
      )
    );
    await second;
    assert.deepEqual(
      controller.getState().threads.map((thread) => thread.id),
      ['thread-1', 'thread-2', 'thread-3']
    );
  });

  it('caps retained threads at 100, stops pagination honestly, and resets with the filter context', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);

    const firstPage = Array.from({ length: 60 }, (_, index) =>
      makeThread(`first-${index}`)
    );
    const first = controller.load('fresh');
    api.pending[0]!.resolve(makePage(firstPage, 'cursor-60'));
    await first;
    assert.equal(controller.getState().threads.length, 60);
    assert.equal(controller.getState().nextCursor, 'cursor-60');

    const secondPage = Array.from({ length: 60 }, (_, index) =>
      makeThread(`second-${index}`)
    );
    const second = controller.load('more');
    api.pending[1]!.resolve(makePage(secondPage, 'cursor-120'));
    await second;

    let state = controller.getState();
    assert.equal(state.threads.length, NATIVE_THREAD_RETAINED_LIMIT);
    assert.equal(state.threads[0]?.id, 'first-0');
    assert.equal(
      state.threads[NATIVE_THREAD_RETAINED_LIMIT - 1]?.id,
      'second-39'
    );
    assert.equal(state.nextCursor, null);
    assert.equal(state.limitReached, true);

    controller.updateProjectDraft('other-project');
    state = controller.getState();
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
    assert.equal(state.limitReached, false);
    assert.equal(state.appliedProjectFilter, null);
    assert.equal(state.projectDraft, 'other-project');

    const third = controller.load('fresh');
    assert.deepEqual(api.calls[2], {
      toolId: 'codex-a',
      projectId: 'other-project',
      cursor: null,
    });
    api.pending[2]!.resolve(makePage([makeThread('other-1')], null));
    await third;
    state = controller.getState();
    assert.equal(state.threads.length, 1);
    assert.equal(state.limitReached, false);
  });

  it('caps a single oversized page at the retained limit', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    const oversizedPage = Array.from(
      { length: NATIVE_THREAD_RETAINED_LIMIT + 25 },
      (_, index) => makeThread(`oversized-${index}`)
    );

    const pending = controller.load('fresh');
    api.pending[0]!.resolve(makePage(oversizedPage, 'cursor-next'));
    await pending;

    const state = controller.getState();
    assert.equal(state.threads.length, NATIVE_THREAD_RETAINED_LIMIT);
    assert.equal(state.nextCursor, null);
    assert.equal(state.limitReached, true);
  });

  it('clears the applied context on every filter keystroke while a query is in flight', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    const pending = controller.load('fresh');

    controller.updateProjectDraft('p');
    controller.updateProjectDraft('pr');
    controller.updateProjectDraft('pro');
    assert.equal(controller.getState().projectDraft, 'pro');
    assert.equal(controller.getState().requestContext, null);

    api.pending[0]!.resolve(makePage([makeThread('thread-a')], 'cursor-a'));
    await pending;

    const state = controller.getState();
    assert.equal(state.projectDraft, 'pro');
    assert.equal(state.appliedProjectFilter, null);
    assert.equal(state.appliedContext, null);
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
    assert.equal(state.hasQueried, false);
  });

  it('retains the filter draft and cached view across unrelated binding polls', async () => {
    const api = makeFakeApi();
    const controller = makeController(api);
    controller.updateProjectDraft('project-1');
    controller.sync(makeRegistry(1), [makeBinding('thread-1', 'bind-1')]);
    assert.equal(controller.getState().projectDraft, 'project-1');

    const pending = controller.load('fresh');
    api.pending[0]!.resolve(makePage([makeThread('thread-1')], 'cursor-1'));
    await pending;

    let state = controller.getState();
    assert.equal(state.appliedProjectFilter, 'project-1');
    assert.equal(state.threads.length, 1);
    assert.equal(state.nextCursor, 'cursor-1');
    assert.equal(api.calls.length, 1);

    controller.setActive(false);
    controller.setActive(true);
    controller.sync(makeRegistry(1), [makeBinding('thread-1', 'bind-1')]);
    state = controller.getState();
    assert.equal(state.appliedProjectFilter, 'project-1');
    assert.equal(state.threads.length, 1);
    assert.equal(state.nextCursor, 'cursor-1');

    controller.sync(makeRegistry(1), [makeBinding('thread-1', 'bind-2')]);
    state = controller.getState();
    assert.equal(state.projectDraft, 'project-1');
    assert.equal(state.appliedProjectFilter, 'project-1');
    assert.equal(state.threads.length, 1);
    assert.equal(state.nextCursor, 'cursor-1');
    assert.equal(state.bindings[0]?.binding_id, 'bind-2');
    assert.equal(api.calls.length, 1);

    controller.sync(makeRegistry(2), [makeBinding('thread-1', 'bind-2')]);
    state = controller.getState();
    assert.equal(state.projectDraft, 'project-1');
    assert.equal(state.appliedProjectFilter, null);
    assert.equal(state.appliedContext, null);
    assert.equal(state.threads.length, 0);
    assert.equal(state.nextCursor, null);
    assert.equal(state.hasQueried, false);
    assert.equal(await controller.load('more'), false);
    assert.equal(api.calls.length, 1);

    controller.selectTool('codex-b');
    state = controller.getState();
    assert.equal(state.selectedToolId, 'codex-b');
    assert.equal(state.projectDraft, 'project-1');
    assert.equal(state.threads.length, 0);
  });
});
