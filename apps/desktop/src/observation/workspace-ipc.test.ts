import assert from 'node:assert/strict';
import test from 'node:test';

import { AppIpcError } from '../registry/ipc.ts';
import {
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  createVerifiedConfiguredSelection,
  evaluateWorkspaceNativeSampleAt,
  formatWorkspaceChatIdentityKey,
  parseWorkspaceSnapshot,
  upgradeLegacyObservationSnapshot,
  validateWorkspaceChatIdentity,
} from '../workspace-contract.ts';
import {
  ipcLinkObservationContext,
  ipcObservationSnapshot,
  ipcObservationSources,
  ipcWorkspaceSnapshot,
} from './ipc.ts';
import type {
  ObservationSnapshot,
  ObservationSnapshotRequest,
  ObservationSource,
  WorkspaceChatIdentity,
  WorkspaceNativeSample,
  WorkspaceSnapshot,
} from './types.ts';

const VALID_UUID_ALPHA = '0196ed00-0000-7000-8000-000000000042';
const VALID_UUID_BETA = '0196ed00-0000-7000-8000-000000000099';

interface RecordedInvokeCall {
  cmd: string;
  args: unknown;
  options: unknown;
}

type InvokeStubHandler = (
  cmd: string,
  args: unknown,
  options: unknown
) => unknown | Promise<unknown>;

async function withWindowOverride<T>(
  nextWindow: unknown,
  hasWindow: boolean,
  run: () => Promise<T>
): Promise<T> {
  const globalObj = globalThis as Record<string, unknown>;
  const hadWindow = Object.prototype.hasOwnProperty.call(globalObj, 'window');
  const prevWindowDescriptor = Object.getOwnPropertyDescriptor(globalObj, 'window');
  const prevWindowObj =
    hadWindow && typeof globalObj.window === 'object' && globalObj.window !== null
      ? (globalObj.window as Record<string, unknown>)
      : null;
  const hadInternals =
    prevWindowObj !== null &&
    Object.prototype.hasOwnProperty.call(prevWindowObj, '__TAURI_INTERNALS__');
  const prevInternalsDescriptor =
    prevWindowObj !== null
      ? Object.getOwnPropertyDescriptor(prevWindowObj, '__TAURI_INTERNALS__')
      : undefined;
  const hadTauri =
    prevWindowObj !== null && Object.prototype.hasOwnProperty.call(prevWindowObj, '__TAURI__');
  const prevTauriDescriptor =
    prevWindowObj !== null
      ? Object.getOwnPropertyDescriptor(prevWindowObj, '__TAURI__')
      : undefined;

  const hadFetch = Object.prototype.hasOwnProperty.call(globalObj, 'fetch');
  const prevFetchDescriptor = Object.getOwnPropertyDescriptor(globalObj, 'fetch');

  try {
    Object.defineProperty(globalObj, 'fetch', {
      configurable: true,
      writable: true,
      value: async () => {
        throw new Error('Unexpected HTTP or local-server fallback fetch during workspace IPC.');
      },
    });

    if (hasWindow) {
      Object.defineProperty(globalObj, 'window', {
        configurable: true,
        writable: true,
        value: nextWindow,
      });
    } else {
      delete globalObj.window;
    }

    return await run();
  } finally {
    if (prevWindowObj !== null) {
      if (hadInternals && prevInternalsDescriptor) {
        Object.defineProperty(prevWindowObj, '__TAURI_INTERNALS__', prevInternalsDescriptor);
      } else {
        delete prevWindowObj.__TAURI_INTERNALS__;
      }
      if (hadTauri && prevTauriDescriptor) {
        Object.defineProperty(prevWindowObj, '__TAURI__', prevTauriDescriptor);
      } else {
        delete prevWindowObj.__TAURI__;
      }
    }

    if (hadWindow && prevWindowDescriptor) {
      Object.defineProperty(globalObj, 'window', prevWindowDescriptor);
    } else {
      delete globalObj.window;
    }

    if (hadFetch && prevFetchDescriptor) {
      Object.defineProperty(globalObj, 'fetch', prevFetchDescriptor);
    } else {
      delete globalObj.fetch;
    }
  }
}

async function withTauriInvokeStub<T>(
  handler: InvokeStubHandler,
  run: (calls: RecordedInvokeCall[]) => Promise<T>
): Promise<T> {
  const calls: RecordedInvokeCall[] = [];
  const stubWindow = {
    __TAURI_INTERNALS__: {
      invoke: async (cmd: string, args?: unknown, options?: unknown): Promise<unknown> => {
        calls.push({ cmd, args, options });
        return await handler(cmd, args, options);
      },
    },
  };
  return await withWindowOverride(stubWindow, true, () => run(calls));
}

function alphaIdentity(): WorkspaceChatIdentity {
  return validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_alpha',
    thread_uuid: VALID_UUID_ALPHA,
    canonical_project_scope: '/home/dev/repos/alpha-app',
  });
}

function betaIdentity(): WorkspaceChatIdentity {
  return validateWorkspaceChatIdentity({
    source_id: 'default',
    binding_id: 'bnd_chat_beta',
    thread_uuid: VALID_UUID_BETA,
    canonical_project_scope: '/home/dev/repos/beta-app',
  });
}

function createBaseObservation(generatedAtMs: number): ObservationSnapshot {
  return {
    schema_version: 1,
    generated_at_ms: generatedAtMs,
    sources: [
      {
        id: 'default',
        label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        availability: 'available',
        message: null,
      },
    ],
    jobs: [],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
  };
}

function createLiveSample(
  identity: WorkspaceChatIdentity,
  observedAtMs: number,
  activity: WorkspaceNativeSample['activity'] = 'waiting_for_input'
): WorkspaceNativeSample {
  return {
    chat_identity: identity,
    producer_epoch: 'epoch_ipc_01',
    backend_process_instance: {
      pid: 4242,
      boot_id: 'boot-linux-01',
      start_ticks: 900_001,
    },
    sequence: 7,
    observed_at_ms: observedAtMs,
    last_success_at_ms: observedAtMs,
    availability: 'available',
    activity,
    connection: {
      state: 'connected',
      freshness: 'fresh',
      route: 'managed_linux_desktop',
      diagnostic: null,
    },
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'supported',
    },
    configured_selection: createVerifiedConfiguredSelection('gpt-5.4', 'high'),
  };
}

function createRawWorkspacePayload(generatedAtMs: number): Record<string, unknown> {
  const identity = alphaIdentity();
  return {
    schema_version: 1,
    generated_at_ms: generatedAtMs,
    sampling_policy: {
      interval_ms: 2_000,
      metadata_timeout_ms: 750,
      stale_threshold_ms: 10_000,
      snapshot_resets_sample_age: false,
    },
    observation: createBaseObservation(generatedAtMs),
    chats: [
      {
        identity,
        thread_label: 'Alpha Chat',
        project_label: 'alpha-app',
        availability: 'available',
        activity: 'waiting_for_input',
        configured_selection: {
          selected_model: 'gpt-5.4',
          selected_effort: 'high',
          evidence: 'verified_configuration',
          counts_as_runtime_telemetry: false,
        },
        active_job_count: 1,
        attention_count: 1,
        updated_at_ms: generatedAtMs - 2_000,
      },
    ],
    native_samples: [createLiveSample(identity, generatedAtMs - 2_000, 'waiting_for_input')],
    job_histories: [
      {
        job: {
          source_id: 'default',
          job_id: 'job_ipc_01',
          binding_id: 'bnd_chat_alpha',
          chat_identity: identity,
          title: 'Workspace IPC verification',
          job_revision: 4,
          state: 'completed',
          phase: 'completed',
          approval_status: 'approved',
          execution_status: 'accepted',
          review_state: 'review_passed',
          created_at_ms: generatedAtMs - 9_000,
          updated_at_ms: generatedAtMs - 2_500,
          summary: 'Verified workspace IPC snapshot.',
        },
        tasks: [
          {
            task_id: 'task_backed',
            title: 'Backed task',
            role: 'coder',
            difficulty: 'hard',
            stage: 'accepted',
            status: 'accepted',
            depends_on: [],
            current_attempt_no: 1,
            attempts: [],
            candidates: [],
            active_candidate_id: null,
            accepted_candidate_id: 'cand_01',
            historical_failure: null,
            current_attention: null,
            acceptance_proof: {
              status: 'verified_accepted',
              accepted_candidate_id: 'cand_01',
              combined_candidate_id: null,
              review_verdict_id: 'rverd_01',
              review_receipt_id: 'rrcpt_01',
              integration_receipt_id: null,
              closure_reservation_id: null,
              closure_verdict_id: null,
              verified_at_ms: generatedAtMs - 3_000,
            },
          },
          {
            task_id: 'task_unbacked',
            title: 'Unbacked claim task',
            role: 'coder',
            difficulty: 'medium',
            stage: 'review_pending',
            status: 'completed',
            depends_on: ['task_backed'],
            current_attempt_no: 1,
            attempts: [],
            candidates: [],
            active_candidate_id: 'cand_02',
            accepted_candidate_id: 'cand_02',
            historical_failure: null,
            current_attention: null,
            acceptance_proof: {
              status: 'verified_accepted',
              accepted_candidate_id: 'cand_02',
              review_verdict_id: null,
              review_receipt_id: null,
            },
          },
        ],
        runs: [],
        outbox: [],
        combined_candidates: [
          {
            combined_candidate_id: 'ccand_01',
            job_revision: 4,
            status: 'accepted',
            final_commit_sha: '777788889999',
            final_tree_sha: 'aaaabbbbcccc',
            candidate_digest: 'digest_01',
            closure_reservation_id: 'clos_res_01',
            check_count: 3,
            produced_at_ms: generatedAtMs - 2_800,
          },
        ],
        result: {
          outcome: 'completed',
          summary: 'Workspace IPC contract verified.',
          produced_at_ms: generatedAtMs - 2_500,
          delivery_stage: 'result_ready',
          closure_verdict_id: 'cverd_01',
        },
        links: {
          source_plan_id: 'plan_01',
          source_plan_revision: 1,
        },
        historical_failure: null,
        current_attention: null,
        acceptance_proof: {
          status: 'verified_accepted',
          combined_candidate_id: 'ccand_01',
          closure_verdict_id: 'cverd_01',
          verified_at_ms: generatedAtMs - 2_500,
        },
        truncation: {
          tasks_omitted: 0,
          attempts_omitted: 0,
          candidates_omitted: 0,
          runs_omitted: 0,
          outbox_omitted: 0,
          combined_candidates_omitted: 0,
        },
      },
    ],
    messages: {
      availability: 'available',
      items: [
        {
          item_id: 'wmsg:v1:7:default:15:closure_summary:8:cverd_01:0',
          source_kind: 'closure_summary',
          source_record_id: 'cverd_01',
          source_order: {
            recorded_at_ms: generatedAtMs - 2_500,
            source_seq: 4,
            sub_index: 0,
          },
          scope: {
            source_id: 'default',
            binding_id: 'bnd_chat_alpha',
            thread_uuid: VALID_UUID_ALPHA,
            canonical_project_scope: '/home/dev/repos/alpha-app',
            chat_identity: identity,
            job_id: 'job_ipc_01',
            task_id: null,
            run_id: null,
            attempt_no: null,
          },
          actor_role: 'closure',
          summary: 'Workspace IPC contract verified.',
          completeness: {
            summary_truncated: false,
            original_char_count: 32,
            max_summary_chars: 240,
            redacted: false,
            missing_fields: [],
            incomplete_reason: null,
          },
        },
        {
          item_id: 'forged_transcript_item',
          source_kind: 'raw_transcript',
          source_record_id: 'forged_01',
          source_order: {
            recorded_at_ms: generatedAtMs - 1_000,
            source_seq: 5,
            sub_index: 0,
          },
          scope: {
            source_id: 'default',
          },
          actor_role: 'user',
          summary: 'Forbidden transcript turn must be dropped.',
        },
      ],
      max_items: 1_000,
      omitted_count: 0,
      has_more: false,
      truncated: false,
      requires_transcript: false,
      allows_synthetic_user_messages: false,
    },
    bounds: {
      native_samples_omitted: 0,
      job_histories_omitted: 0,
      messages_omitted: 0,
      requests_capped: false,
      jobs_capped: false,
    },
  };
}

test('ipcWorkspaceSnapshot invokes exact desktop_workspace_snapshot command, matches legacy request scope, and keeps legacy IPC exports intact', async () => {
  const globalObj = globalThis as Record<string, unknown>;
  const hadWindowBefore = Object.prototype.hasOwnProperty.call(globalObj, 'window');
  const hadFetchBefore = Object.prototype.hasOwnProperty.call(globalObj, 'fetch');

  const baseObs = createBaseObservation(1_700_000_010_000);
  const baseWs = createRawWorkspacePayload(1_700_000_010_000);
  const sampleSource: ObservationSource = {
    id: 'ctx_alpha',
    label: 'alpha-app',
    project_path: '/home/dev/repos/alpha-app',
    availability: 'available',
    message: null,
  };

  await withTauriInvokeStub(
    (cmd) => {
      if (cmd === 'desktop_workspace_snapshot') {
        return baseWs;
      }
      if (cmd === 'desktop_observation_snapshot') {
        return baseObs;
      }
      if (cmd === 'desktop_observation_sources') {
        return [sampleSource];
      }
      if (cmd === 'desktop_link_observation_context') {
        return sampleSource;
      }
      throw new Error(`Unexpected command: ${cmd}`);
    },
    async (calls) => {
      await ipcWorkspaceSnapshot();
      await ipcObservationSnapshot();

      assert.equal(calls.length, 2);
      assert.equal(calls[0]!.cmd, 'desktop_workspace_snapshot');
      assert.notEqual(calls[0]!.cmd, 'desktop_workspace_snapshot_v1');
      assert.equal(calls[1]!.cmd, 'desktop_observation_snapshot');
      assert.deepEqual(calls[0]!.args, { request: undefined });
      assert.deepEqual(calls[0]!.args, calls[1]!.args);
      assert.equal(calls[0]!.options, undefined);

      const scopedRequest: ObservationSnapshotRequest = {
        source_id: 'ctx_alpha',
        job_id: 'job_ipc_01',
        cursor: 'obs:cursor:17',
        limit: 40,
      };

      await ipcWorkspaceSnapshot(scopedRequest);
      await ipcObservationSnapshot(scopedRequest);

      assert.equal(calls.length, 4);
      assert.equal(calls[2]!.cmd, 'desktop_workspace_snapshot');
      assert.equal(calls[3]!.cmd, 'desktop_observation_snapshot');
      assert.deepEqual(calls[2]!.args, { request: scopedRequest });
      assert.deepEqual(calls[2]!.args, calls[3]!.args);
      assert.equal(calls[2]!.options, undefined);

      const nullScopedRequest: ObservationSnapshotRequest = {
        source_id: null,
        job_id: null,
        cursor: null,
      };
      await ipcWorkspaceSnapshot(nullScopedRequest);
      assert.equal(calls[4]!.cmd, 'desktop_workspace_snapshot');
      assert.deepEqual(calls[4]!.args, { request: nullScopedRequest });

      const sources = await ipcObservationSources();
      assert.deepEqual(sources, [sampleSource]);
      assert.equal(calls[5]!.cmd, 'desktop_observation_sources');

      const linked = await ipcLinkObservationContext('/home/dev/repos/alpha-app');
      assert.deepEqual(linked, sampleSource);
      assert.equal(calls[6]!.cmd, 'desktop_link_observation_context');
      assert.deepEqual(calls[6]!.args, { contextPath: '/home/dev/repos/alpha-app' });
    }
  );

  assert.equal(Object.prototype.hasOwnProperty.call(globalObj, 'window'), hadWindowBefore);
  assert.equal(Object.prototype.hasOwnProperty.call(globalObj, 'fetch'), hadFetchBefore);
});

test('ipcWorkspaceSnapshot normalizes structured object and JSON string payloads into typed WorkspaceSnapshot via parseWorkspaceSnapshot', async () => {
  const generatedAtMs = 1_700_000_010_000;
  const rawPayload = createRawWorkspacePayload(generatedAtMs);
  const expected = parseWorkspaceSnapshot(rawPayload);

  const fromObject: WorkspaceSnapshot = await withTauriInvokeStub(
    () => rawPayload,
    async () => await ipcWorkspaceSnapshot({ source_id: 'default', job_id: 'job_ipc_01' })
  );

  assert.deepEqual(fromObject, expected);
  assert.equal(fromObject.schema_version, WORKSPACE_SNAPSHOT_SCHEMA_VERSION);
  assert.equal(fromObject.sampling_policy.snapshot_resets_sample_age, false);
  assert.equal(
    formatWorkspaceChatIdentityKey(fromObject.chats[0]!.identity),
    'wchat:v1:7:default:14:bnd_chat_alpha:36:0196ed00-0000-7000-8000-000000000042:25:/home/dev/repos/alpha-app'
  );
  assert.equal(fromObject.chats[0]!.availability, 'available');
  assert.equal(fromObject.chats[0]!.activity, 'waiting_for_input');
  assert.equal(fromObject.chats[0]!.configured_selection.selected_model, 'gpt-5.4');
  assert.equal(fromObject.chats[0]!.configured_selection.evidence, 'verified_configuration');
  assert.equal(fromObject.chats[0]!.configured_selection.counts_as_runtime_telemetry, false);

  // Backed review/closure proof stays verified_accepted; unbacked task claim degrades to unknown.
  assert.equal(fromObject.job_histories[0]!.acceptance_proof.status, 'verified_accepted');
  assert.equal(
    fromObject.job_histories[0]!.tasks[0]!.acceptance_proof.status,
    'verified_accepted'
  );
  assert.equal(fromObject.job_histories[0]!.tasks[1]!.acceptance_proof.status, 'unknown');

  // Forbidden raw_transcript message item is stripped and counted in omissions.
  assert.equal(fromObject.messages.items.length, 1);
  assert.equal(fromObject.messages.items[0]!.source_kind, 'closure_summary');
  assert.equal(fromObject.messages.requires_transcript, false);
  assert.equal(fromObject.messages.allows_synthetic_user_messages, false);
  assert.equal(fromObject.messages.omitted_count, 1);
  assert.equal(fromObject.messages.truncated, true);
  assert.equal(fromObject.bounds.messages_omitted, 1);

  const fromJsonString = await withTauriInvokeStub(
    () => JSON.stringify(rawPayload),
    async () => await ipcWorkspaceSnapshot()
  );
  assert.deepEqual(fromJsonString, fromObject);
});

test('ipcWorkspaceSnapshot preserves stale sample evaluation, degrades unknown variants, and upgrades legacy ObservationSnapshot without fabricating samples or messages', async () => {
  const staleGeneratedAtMs = 60_001;
  const staleSample = createLiveSample(alphaIdentity(), 50_000, 'working');

  const staleAndUnknownPayload = {
    schema_version: 1,
    generated_at_ms: staleGeneratedAtMs,
    observation: createBaseObservation(staleGeneratedAtMs),
    chats: [
      {
        identity: betaIdentity(),
        thread_label: 'Beta Chat',
        project_label: 'beta-app',
        availability: 'future_availability_variant',
        activity: 'future_activity_variant',
        configured_selection: {
          selected_model: 'gpt-5.4',
          selected_effort: 'high',
          evidence: 'unverified_runtime_claim',
          counts_as_runtime_telemetry: true,
        },
        active_job_count: 0,
        attention_count: 0,
        updated_at_ms: 59_000,
      },
    ],
    native_samples: [staleSample],
    job_histories: [],
    messages: {
      availability: 'future_window_state',
      items: [],
      requires_transcript: true,
      allows_synthetic_user_messages: true,
    },
  };

  const parsedStale = await withTauriInvokeStub(
    () => staleAndUnknownPayload,
    async () => await ipcWorkspaceSnapshot()
  );

  assert.equal(parsedStale.native_samples.length, 1);
  assert.equal(parsedStale.native_samples[0]!.observed_at_ms, 50_000);
  assert.equal(parsedStale.native_samples[0]!.last_success_at_ms, 50_000);

  const evaluatedSample = evaluateWorkspaceNativeSampleAt(
    parsedStale.native_samples[0]!,
    parsedStale.generated_at_ms,
    parsedStale.sampling_policy.stale_threshold_ms
  );
  assert.equal(evaluatedSample.freshness, 'stale');
  assert.equal(evaluatedSample.effective_availability, 'stale');
  assert.equal(evaluatedSample.effective_activity, 'unknown');

  const alphaChat = parsedStale.chats.find(
    (chat) => chat.identity.binding_id === 'bnd_chat_alpha'
  );
  assert.ok(alphaChat);
  assert.equal(alphaChat.availability, 'stale');
  assert.equal(alphaChat.activity, 'unknown');
  assert.equal(alphaChat.configured_selection.evidence, 'unavailable');
  assert.equal(alphaChat.configured_selection.selected_model, null);
  assert.equal(alphaChat.configured_selection.counts_as_runtime_telemetry, false);

  const betaChat = parsedStale.chats.find(
    (chat) => chat.identity.binding_id === 'bnd_chat_beta'
  );
  assert.ok(betaChat);
  assert.equal(betaChat.availability, 'unavailable');
  assert.equal(betaChat.activity, 'unknown');
  assert.equal(betaChat.configured_selection.evidence, 'unavailable');
  assert.equal(betaChat.configured_selection.selected_model, null);
  assert.equal(betaChat.configured_selection.counts_as_runtime_telemetry, false);

  assert.equal(parsedStale.messages.availability, 'unavailable');
  assert.equal(parsedStale.messages.requires_transcript, false);
  assert.equal(parsedStale.messages.allows_synthetic_user_messages, false);

  // Legacy ObservationSnapshot payload upgrades cleanly without fabricating samples, messages, or acceptance proof.
  const legacyPayload: ObservationSnapshot = {
    schema_version: 1,
    generated_at_ms: 1_700_000_020_000,
    sources: [
      {
        id: 'default',
        label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        availability: 'available',
        message: null,
      },
    ],
    jobs: [
      {
        source_id: 'default',
        id: 'job_legacy_needs_user',
        project_label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        title: 'Legacy Approval Gate',
        thread_uuid: VALID_UUID_ALPHA,
        thread_label: 'Alpha Chat',
        binding_id: 'bnd_chat_alpha',
        plan_id: 'plan_legacy_01',
        plan_revision: 2,
        state: 'needs_user',
        approval_status: 'pending',
        execution_status: 'blocked',
        review_state: null,
        updated_at_ms: 1_700_000_019_500,
        active_call_count: 0,
        tasks: [
          {
            id: 'task-legacy-1',
            title: 'Await approval',
            status: 'blocked',
            depends_on: [],
          },
        ],
        summary: 'Waiting on operator approval.',
      },
      {
        source_id: 'default',
        id: 'job_legacy_unknown',
        project_label: 'alpha-app',
        project_path: '/home/dev/repos/alpha-app',
        title: 'Legacy Unknown Job',
        thread_uuid: VALID_UUID_ALPHA,
        thread_label: 'Alpha Chat',
        binding_id: 'bnd_chat_alpha',
        plan_id: null,
        plan_revision: null,
        state: 'unknown',
        approval_status: 'unknown',
        execution_status: 'unknown',
        review_state: null,
        updated_at_ms: 1_700_000_018_000,
        active_call_count: 0,
        tasks: [],
        summary: null,
      },
    ],
    requests: [],
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: ['legacy read model fallback'],
  };

  const upgradedViaIpc = await withTauriInvokeStub(
    () => legacyPayload,
    async () => await ipcWorkspaceSnapshot({ source_id: 'default' })
  );

  assert.deepEqual(upgradedViaIpc, upgradeLegacyObservationSnapshot(legacyPayload));
  assert.deepEqual(upgradedViaIpc.native_samples, []);
  assert.deepEqual(upgradedViaIpc.messages.items, []);
  assert.equal(upgradedViaIpc.messages.availability, 'unavailable');
  assert.equal(upgradedViaIpc.messages.requires_transcript, false);
  assert.equal(upgradedViaIpc.messages.allows_synthetic_user_messages, false);
  assert.equal(upgradedViaIpc.chats.length, 1);
  assert.equal(upgradedViaIpc.chats[0]!.availability, 'unavailable');
  assert.equal(upgradedViaIpc.chats[0]!.activity, 'unknown');
  assert.equal(upgradedViaIpc.chats[0]!.configured_selection.evidence, 'unavailable');
  assert.equal(upgradedViaIpc.chats[0]!.active_job_count, 1);
  assert.equal(upgradedViaIpc.chats[0]!.attention_count, 1);
  assert.equal(upgradedViaIpc.job_histories.length, 2);
  assert.equal(upgradedViaIpc.job_histories[0]!.acceptance_proof.status, 'unknown');
  assert.equal(upgradedViaIpc.job_histories[1]!.acceptance_proof.status, 'unknown');
});

test('ipcWorkspaceSnapshot rejects with bridge_disconnected before invoke when native Tauri runtime is unavailable and restores globals on failure', async () => {
  const sentinelWindow = { customMarker: 'restore-check' };
  const globalObj = globalThis as Record<string, unknown>;

  await withWindowOverride(sentinelWindow, true, async () => {
    // 1. Completely missing `window` global.
    await withWindowOverride(undefined, false, async () => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot({ source_id: 'default' }),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'bridge_disconnected' &&
          err.message === 'Tauri desktop bridge is not connected in this browser window.'
      );
    });
    assert.equal(globalObj.window, sentinelWindow);

    // 2. Plain browser `window` without Tauri globals.
    await withWindowOverride({}, true, async () => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot(),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'bridge_disconnected' &&
          err.message === 'Tauri desktop bridge is not connected in this browser window.'
      );
    });
    assert.equal(globalObj.window, sentinelWindow);

    // 3. Incomplete Tauri globals without a callable `__TAURI_INTERNALS__.invoke`.
    for (const partialWindow of [{ __TAURI_INTERNALS__: {} }, { __TAURI__: {} }]) {
      await withWindowOverride(partialWindow, true, async () => {
        await assert.rejects(
          () => ipcWorkspaceSnapshot(),
          (err: unknown) =>
            err instanceof AppIpcError &&
            err.code === 'bridge_disconnected' &&
            err.message === 'Tauri desktop bridge is not connected in this browser window.'
        );
      });
      assert.equal(globalObj.window, sentinelWindow);
    }

    // 4. Throwing inside stubbed execution still restores the previous `window`.
    await assert.rejects(
      () =>
        withTauriInvokeStub(
          () => createRawWorkspacePayload(1_000),
          async () => {
            throw new Error('intentional callback failure');
          }
        ),
      /intentional callback failure/
    );
    assert.equal(globalObj.window, sentinelWindow);
  });
});

test('ipcWorkspaceSnapshot normalizes and redacts backend invoke errors without fallback', async () => {
  await withTauriInvokeStub(
    () => {
      return Promise.reject({
        code: 'workspace_lock_busy',
        message: 'Supervisor snapshot lock timed out.',
      });
    },
    async (calls) => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot({ source_id: 'default' }),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'workspace_lock_busy' &&
          err.message === 'Supervisor snapshot lock timed out.'
      );
      assert.equal(calls.length, 1);
    }
  );

  await withTauriInvokeStub(
    () =>
      Promise.reject(
        JSON.stringify({
          code: 'source_not_found',
          message: 'Requested observation source was not found.',
        })
      ),
    async () => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot({ source_id: 'missing_src' }),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'source_not_found' &&
          err.message === 'Requested observation source was not found.'
      );
    }
  );

  await withTauriInvokeStub(
    () =>
      Promise.reject(
        new Error(
          'Backend failure with Bearer super-secret-jwt and api_key=sk-secret987654321'
        )
      ),
    async () => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot(),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'workspace_snapshot_failed' &&
          !err.message.includes('super-secret-jwt') &&
          !err.message.includes('sk-secret987654321') &&
          err.message.includes('[redacted]')
      );
    }
  );

  await withTauriInvokeStub(
    () => Promise.reject('   '),
    async () => {
      await assert.rejects(
        () => ipcWorkspaceSnapshot(),
        (err: unknown) =>
          err instanceof AppIpcError &&
          err.code === 'workspace_snapshot_failed' &&
          err.message === 'Failed to load workspace snapshot.'
      );
    }
  );
});

test('ipcWorkspaceSnapshot rejects malformed snapshot responses instead of returning silent empty snapshots', async () => {
  const validObs = createBaseObservation(1_700_000_010_000);
  const malformedPayloads: unknown[] = [
    null,
    undefined,
    42,
    true,
    [],
    '{malformed json',
    '[]',
    'null',
    {},
    { ok: true },
    {
      schema_version: 2,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
    },
    {
      schema_version: '1',
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
    },
    {
      schema_version: 1,
      generated_at_ms: '1700000010000',
      observation: validObs,
    },
    {
      schema_version: 1,
      generated_at_ms: -10,
      observation: validObs,
    },
    {
      schema_version: 1,
      generated_at_ms: Number.NaN,
      observation: validObs,
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: null,
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: {
        schema_version: 1,
        generated_at_ms: 1_700_000_010_000,
        sources: 'not-an-array',
        jobs: [],
        requests: [],
      },
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
      chats: 'not-an-array',
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
      native_samples: {},
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
      job_histories: 'not-an-array',
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
      messages: [],
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      observation: validObs,
      messages: {
        availability: 'available',
        items: 'not-an-array',
      },
    },
    {
      schema_version: 1,
      generated_at_ms: 1_700_000_010_000,
      sources: 'not-an-array',
      jobs: [],
      requests: [],
    },
    JSON.stringify({}),
  ];

  for (const badPayload of malformedPayloads) {
    await withTauriInvokeStub(
      () => badPayload,
      async () => {
        await assert.rejects(
          () => ipcWorkspaceSnapshot(),
          (err: unknown) => err instanceof AppIpcError && err.code === 'invalid_json'
        );
      }
    );
  }
});
