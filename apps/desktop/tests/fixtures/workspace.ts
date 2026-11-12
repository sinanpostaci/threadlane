import type { Page } from '@playwright/test';
import type { CodexSetupReport } from '../../src/codex-setup/types.ts';
import type {
  ObservationActor,
  ObservationCall,
  ObservationExecutor,
  ObservationJob,
  ObservationRequest,
  ObservationSnapshot,
  ObservationSource,
  ObservationUsage,
} from '../../src/observation/types.ts';
import type {
  DesktopToolVersionsSnapshot,
  RegistrySnapshot,
} from '../../src/registry/types.ts';
import type { TrayCloseBehavior, TrayPreferencesResponse } from '../../src/tray-model.ts';
import type { DesktopSnapshot, ToolStatus } from '../../src/types.ts';
import {
  createSafeWorkspaceMessageItem,
  formatWorkspaceChatIdentityKey,
  MAX_WORKSPACE_MESSAGES_WINDOW,
  OBSERVATION_SCHEMA_VERSION,
  parseWorkspaceSnapshot,
  regenerateWorkspaceSnapshotAt,
  validateWorkspaceChatIdentity,
  WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
  WORKSPACE_METADATA_TIMEOUT_MS,
  WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
  WORKSPACE_SAMPLING_INTERVAL_MS,
  WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
  type WorkspaceAcceptanceProof,
  type WorkspaceChatIdentity,
  type WorkspaceChatSummary,
  type WorkspaceExternalResolution,
  type WorkspaceJobHistory,
  type WorkspaceMessageItem,
  type WorkspaceNativeSample,
  type WorkspaceSafeMessageSourceKind,
  type WorkspaceSnapshot,
  type WorkspaceTaskHistory,
} from '../../src/workspace-contract.ts';

export const FIXTURE_BASE_NOW_MS = 1_780_000_000_000;
export const FIXTURE_SOURCE_ID = 'src_desktop_primary';
export const FIXTURE_CANONICAL_PROJECT_ALPHA = '/workspace/threadlane-alpha';
export const FIXTURE_CANONICAL_PROJECT_LEGACY = '/workspace/threadlane-legacy';

export const CHAT_ALPHA_IDENTITY: WorkspaceChatIdentity = validateWorkspaceChatIdentity({
  source_id: FIXTURE_SOURCE_ID,
  binding_id: 'bnd_alpha_01',
  thread_uuid: '550e8400-e29b-41d4-a716-446655440001',
  canonical_project_scope: FIXTURE_CANONICAL_PROJECT_ALPHA,
});

export const CHAT_BETA_SAME_PROJECT_IDENTITY: WorkspaceChatIdentity =
  validateWorkspaceChatIdentity({
    source_id: FIXTURE_SOURCE_ID,
    binding_id: 'bnd_beta_02',
    thread_uuid: '550e8400-e29b-41d4-a716-446655440002',
    canonical_project_scope: FIXTURE_CANONICAL_PROJECT_ALPHA,
  });

export const CHAT_GAMMA_IDENTITY: WorkspaceChatIdentity = validateWorkspaceChatIdentity({
  source_id: FIXTURE_SOURCE_ID,
  binding_id: 'bnd_gamma_03',
  thread_uuid: '550e8400-e29b-41d4-a716-446655440003',
  canonical_project_scope: FIXTURE_CANONICAL_PROJECT_LEGACY,
});

export const CHAT_ALPHA_KEY = formatWorkspaceChatIdentityKey(CHAT_ALPHA_IDENTITY)!;
export const CHAT_BETA_KEY = formatWorkspaceChatIdentityKey(CHAT_BETA_SAME_PROJECT_IDENTITY)!;
export const CHAT_GAMMA_KEY = formatWorkspaceChatIdentityKey(CHAT_GAMMA_IDENTITY)!;

export const WRONG_SCOPE_UNALIGNED_THREAD_UUID = '550e8400-e29b-41d4-a716-446655449999';
export const BETA_CHAT_ONLY_SENTINEL =
  'BETA_SCOPE_SENTINEL: Cross-chat release verification summary belongs strictly to bnd_beta_02.';
export const WRONG_SCOPE_QUARANTINE_SENTINEL =
  'WRONG_SCOPE_SENTINEL: Unaligned record missing verified four-part chat identity.';
export const HISTORICAL_FAILURE_REASON =
  'Historical attempt 1 sandbox timeout before clean retry succeeded.';
export const PENDING_ACCEPTANCE_SUMMARY =
  'Worker candidate cand_pending_01 finished compiling; awaiting review and Closure acceptance.';
export const VERIFIED_ACCEPTED_SUMMARY =
  'Closure verified combined candidate comb_accepted_02 and recorded final acceptance.';

export const EXTERNAL_RESOLUTION_FULL_RECEIPT_ID = 'rcpt_ext_fixture_full_01';
export const EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID = 'rcpt_ext_fixture_partial_02';
export const EXTERNAL_RESOLUTION_STALE_RECEIPT_ID = 'rcpt_ext_fixture_stale_03';
export const EXTERNAL_RESOLUTION_UNVERIFIED_RECEIPT_ID = 'rcpt_ext_fixture_unverified_04';
export const EXTERNAL_RESOLUTION_UNKNOWN_RECEIPT_ID = 'rcpt_ext_fixture_unknown_05';
export const EXTERNAL_RESOLUTION_FULL_SUMMARY =
  'Full job was resolved outside Threadlane by explicit operator action.';
export const EXTERNAL_RESOLUTION_PARTIAL_SUMMARY =
  'One task was resolved outside Threadlane while unrelated task output remains visible.';
export const EXTERNAL_RESOLUTION_STALE_SUMMARY =
  'Historical stale external receipt remains visible without closing active blockers.';
export const EXTERNAL_RESOLUTION_UNVERIFIED_SUMMARY =
  'Historical unverified external receipt remains visible without closing active blockers.';
export const EXTERNAL_RESOLUTION_UNKNOWN_SUMMARY =
  'A full receipt exists, but the recorded task effect is still unknown.';
export const EXTERNAL_RESOLUTION_COVERED_JOB_REASON =
  'Covered job-level approval blocker remains in receipt history.';
export const EXTERNAL_RESOLUTION_COVERED_TASK_REASON =
  'Covered task-level hold blocker remains in receipt history.';
export const EXTERNAL_RESOLUTION_UNRELATED_QUESTION =
  'UNRELATED_QUESTION_SENTINEL: Keep this native question visible and unlinked from any receipt.';

const CODEX_COWORKER_ACTOR: ObservationActor = {
  label: 'Codex Coworker',
  role: 'coworker',
  source: 'native_codex',
  selected_model: 'gpt-5.4',
  selected_effort: 'xhigh',
  model: null,
  effort: null,
  evidence: 'configured',
};

const AGY_WORKER_EXECUTOR: ObservationExecutor = {
  tool: 'antigravity',
  provider: 'google',
  role: 'coder',
  requested_model: 'gemini-4-argon-high',
  observed_model: 'gemini-4-argon-high',
  requested_effort: 'high',
  observed_effort: 'high',
};

const LOCAL_MCP_EXECUTOR: ObservationExecutor = {
  tool: 'threadlane_mcp',
  provider: 'local',
  role: null,
  requested_model: null,
  observed_model: null,
  requested_effort: null,
  observed_effort: null,
};

const EMPTY_USAGE: ObservationUsage = {
  input_tokens: null,
  output_tokens: null,
  cache_read_tokens: null,
  total_tokens: null,
  provenance: 'not_reported',
};

const REPORTED_WORKER_USAGE: ObservationUsage = {
  input_tokens: 8_420,
  output_tokens: 1_940,
  cache_read_tokens: 4_100,
  total_tokens: 10_360,
  provenance: 'provider_report',
};

function createEmptyAcceptanceProof(
  status: WorkspaceAcceptanceProof['status'] = 'not_accepted',
): WorkspaceAcceptanceProof {
  return {
    status,
    accepted_candidate_id: null,
    combined_candidate_id: null,
    review_verdict_id: null,
    review_receipt_id: null,
    integration_receipt_id: null,
    closure_reservation_id: null,
    closure_verdict_id: null,
    verified_at_ms: null,
  };
}

function createBaseSource(): ObservationSource {
  return {
    id: FIXTURE_SOURCE_ID,
    label: 'Primary Desktop Store',
    project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
    availability: 'available',
    message: null,
  };
}

export function createChatMessageItem(params: {
  identity: WorkspaceChatIdentity;
  seq: number;
  recordId: string;
  summary: string;
  sourceKind?: WorkspaceSafeMessageSourceKind;
  actorRole?: string;
  jobId?: string | null;
  recordedAtMs?: number;
}): WorkspaceMessageItem {
  const {
    identity,
    seq,
    recordId,
    summary,
    sourceKind = 'task_result_summary',
    actorRole = 'coder',
    jobId = 'job_alpha_live_01',
    recordedAtMs = FIXTURE_BASE_NOW_MS - Math.max(0, 60 - seq) * 1_000,
  } = params;

  return createSafeWorkspaceMessageItem({
    source_kind: sourceKind,
    source_record_id: recordId,
    source_order: {
      recorded_at_ms: recordedAtMs,
      source_seq: seq,
      sub_index: 0,
    },
    scope: {
      source_id: identity.source_id,
      binding_id: identity.binding_id,
      thread_uuid: identity.thread_uuid,
      canonical_project_scope: identity.canonical_project_scope,
      chat_identity: identity,
      job_id: jobId,
      task_id: 'task_alpha_01',
      run_id: 'run_alpha_01',
      attempt_no: 1,
    },
    actor_role: actorRole,
    raw_summary: summary,
  });
}

function buildAlphaScrollableMessages(count: number = 36): WorkspaceMessageItem[] {
  const kinds: readonly WorkspaceSafeMessageSourceKind[] = [
    'work_brief_summary',
    'plan_summary',
    'question_prompt',
    'question_answer',
    'task_result_summary',
    'review_verdict_summary',
  ];
  const items: WorkspaceMessageItem[] = [];
  for (let idx = 1; idx <= count; idx += 1) {
    const padded = String(idx).padStart(2, '0');
    const sourceKind = kinds[(idx - 1) % kinds.length]!;
    items.push(
      createChatMessageItem({
        identity: CHAT_ALPHA_IDENTITY,
        seq: idx,
        recordId: `rec_alpha_msg_${padded}`,
        sourceKind,
        actorRole: sourceKind.startsWith('question') ? 'coworker' : 'coder',
        summary: `Alpha message #${padded}: Scoped workspace summary checkpoint ${padded} for ${CHAT_ALPHA_IDENTITY.binding_id}.`,
        recordedAtMs: FIXTURE_BASE_NOW_MS - (count - idx + 5) * 2_000,
      }),
    );
  }
  return items;
}

function createNativeSample(params: {
  identity: WorkspaceChatIdentity;
  sequence?: number;
  observedAtMs?: number | null;
  lastSuccessAtMs?: number | null;
  availability?: WorkspaceNativeSample['availability'];
  activity?: WorkspaceNativeSample['activity'];
  connectionState?: WorkspaceNativeSample['connection']['state'];
  freshness?: WorkspaceNativeSample['connection']['freshness'];
  diagnostic?: string | null;
  selectedModel?: string | null;
  selectedEffort?: string | null;
  selectionEvidence?: WorkspaceNativeSample['configured_selection']['evidence'];
}): WorkspaceNativeSample {
  const observedAtMs =
    params.observedAtMs === undefined ? FIXTURE_BASE_NOW_MS - 1_500 : params.observedAtMs;
  const lastSuccessAtMs =
    params.lastSuccessAtMs === undefined ? observedAtMs : params.lastSuccessAtMs;
  return {
    chat_identity: params.identity,
    producer_epoch: 'epoch_native_01',
    backend_process_instance: {
      pid: 4242,
      boot_id: 'boot_nixos_accept_01',
      start_ticks: 987_654,
    },
    sequence: params.sequence ?? 12,
    observed_at_ms: observedAtMs,
    last_success_at_ms: lastSuccessAtMs,
    availability: params.availability ?? 'available',
    activity: params.activity ?? 'waiting_for_input',
    connection: {
      state: params.connectionState ?? 'connected',
      freshness: params.freshness ?? 'fresh',
      route: 'linux_native_bridge',
      diagnostic: params.diagnostic ?? null,
    },
    capabilities: {
      exact_thread_read: 'supported',
      activity_detail: 'supported',
      configured_selection: 'supported',
      wakeup_delivery: 'supported',
      per_call_identity: 'supported',
    },
    configured_selection: {
      selected_model: params.selectedModel === undefined ? 'gpt-5.4' : params.selectedModel,
      selected_effort: params.selectedEffort === undefined ? 'xhigh' : params.selectedEffort,
      evidence: params.selectionEvidence ?? 'verified_configuration',
      counts_as_runtime_telemetry: false,
    },
  };
}

/**
 * Primary multi-chat workspace fixture covering:
 * - Active vs Archive chats with canonical 4-part `WorkspaceChatIdentity`
 * - Separate `Messages`, `Jobs`, and `Activity` data per chat
 * - Live Coworker (`waiting_for_input`, configured `gpt-5.4` `xhigh`) vs running Worker (`coder` `gemini-4-argon-high` `high`)
 * - Scrollable 36-item `Messages` window + dynamic bottom append
 * - Running-only spinner Worker call alongside completed Worker and local MCP calls
 * - Same-project sibling chat (`CHAT_BETA_SAME_PROJECT_IDENTITY`) + unaligned wrong-scope quarantine item
 */
export function buildInteractiveWorkspaceSnapshot(): WorkspaceSnapshot {
  const alphaMessages = buildAlphaScrollableMessages(36);
  const betaMessage = createChatMessageItem({
    identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
    seq: 101,
    recordId: 'rec_beta_msg_01',
    sourceKind: 'work_brief_summary',
    actorRole: 'coworker',
    jobId: 'job_beta_01',
    summary: BETA_CHAT_ONLY_SENTINEL,
    recordedAtMs: FIXTURE_BASE_NOW_MS - 3_000,
  });
  const gammaMessage = createChatMessageItem({
    identity: CHAT_GAMMA_IDENTITY,
    seq: 201,
    recordId: 'rec_gamma_msg_01',
    sourceKind: 'closure_summary',
    actorRole: 'closure',
    jobId: 'job_gamma_01',
    summary: 'Legacy migration audit finalized and ready for per-chat archive verification.',
    recordedAtMs: FIXTURE_BASE_NOW_MS - 120_000,
  });
  const wrongScopeItem = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: 'rec_wrong_scope_99',
    source_order: {
      recorded_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      source_seq: 999,
      sub_index: 0,
    },
    scope: {
      source_id: FIXTURE_SOURCE_ID,
      binding_id: null,
      thread_uuid: WRONG_SCOPE_UNALIGNED_THREAD_UUID,
      canonical_project_scope: FIXTURE_CANONICAL_PROJECT_ALPHA,
      chat_identity: null,
      job_id: null,
    },
    actor_role: 'coder',
    raw_summary: WRONG_SCOPE_QUARANTINE_SENTINEL,
    incomplete_reason: 'Missing binding_id prevented canonical chat scope alignment.',
  });

  const runningWorkerCall: ObservationCall = {
    id: 'call_alpha_worker_running',
    title: 'Execute isolated Worker implementation slice',
    executor: AGY_WORKER_EXECUTOR,
    state: 'running',
    started_at_ms: FIXTURE_BASE_NOW_MS - 14_000,
    finished_at_ms: null,
    duration_ms: null,
    usage: EMPTY_USAGE,
    summary: 'Executing isolated Playwright and Wayland acceptance checks in task worktree.',
  };

  const completedWorkerCall: ObservationCall = {
    id: 'call_alpha_worker_done',
    title: 'Run contract schema validation pass',
    executor: AGY_WORKER_EXECUTOR,
    state: 'completed',
    started_at_ms: FIXTURE_BASE_NOW_MS - 70_000,
    finished_at_ms: FIXTURE_BASE_NOW_MS - 25_000,
    duration_ms: 45_000,
    usage: REPORTED_WORKER_USAGE,
    summary: 'Completed contract schema validation pass with zero errors.',
  };

  const localMcpCall: ObservationCall = {
    id: 'call_alpha_local_mcp',
    title: 'Validate shared contract types via local MCP',
    executor: LOCAL_MCP_EXECUTOR,
    state: 'completed',
    started_at_ms: FIXTURE_BASE_NOW_MS - 72_000,
    finished_at_ms: FIXTURE_BASE_NOW_MS - 71_800,
    duration_ms: 200,
    usage: EMPTY_USAGE,
    summary: 'Validated shared contract types via local Threadlane MCP.',
  };

  const observationJobs: ObservationJob[] = [
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'job_alpha_live_01',
      title: 'Workspace Contract Acceptance Slice',
      project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      thread_label: 'Workspace UI Contract',
      plan_id: 'plan_alpha_01',
      plan_revision: 2,
      state: 'running',
      approval_status: 'approved',
      execution_status: 'running',
      review_state: 'not_started',
      updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
      active_call_count: 1,
      tasks: [
        {
          id: 'task_alpha_01',
          title: 'Build acceptance suite against shared workspace contract',
          status: 'running',
          depends_on: [],
        },
      ],
      summary: 'Worker coder slice running in isolated checkout while Coworker awaits operator input.',
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'job_beta_01',
      title: 'Sibling Chat Release Gate',
      project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
      thread_uuid: CHAT_BETA_SAME_PROJECT_IDENTITY.thread_uuid,
      thread_label: 'Release Verification Gate',
      plan_id: 'plan_beta_01',
      plan_revision: 1,
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
      review_state: 'accepted',
      updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      active_call_count: 0,
      tasks: [],
      summary: BETA_CHAT_ONLY_SENTINEL,
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'job_gamma_01',
      title: 'Legacy Migration Audit',
      project_path: FIXTURE_CANONICAL_PROJECT_LEGACY,
      project_label: 'threadlane-legacy',
      binding_id: CHAT_GAMMA_IDENTITY.binding_id,
      thread_uuid: CHAT_GAMMA_IDENTITY.thread_uuid,
      thread_label: 'Archived Migration Audit',
      plan_id: 'plan_gamma_01',
      plan_revision: 1,
      state: 'completed',
      approval_status: 'approved',
      execution_status: 'completed',
      review_state: 'accepted',
      updated_at_ms: FIXTURE_BASE_NOW_MS - 120_000,
      active_call_count: 0,
      tasks: [],
      summary: 'Completed legacy audit.',
    },
  ];

  const observationRequests: ObservationRequest[] = [
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'req_alpha_running',
      job_id: 'job_alpha_live_01',
      parent_request_id: null,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      thread_label: 'Workspace UI Contract',
      title: 'Run isolated Worker implementation pass',
      operation: 'worker_dispatch',
      actor: CODEX_COWORKER_ACTOR,
      executor: AGY_WORKER_EXECUTOR,
      state: 'running',
      started_at_ms: FIXTURE_BASE_NOW_MS - 14_000,
      finished_at_ms: null,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
      duration_ms: null,
      usage: EMPTY_USAGE,
      delivery: [],
      children: [runningWorkerCall],
      children_count: 1,
      summary: 'Active Worker coder call running while native Coworker chat is waiting for user input.',
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'req_alpha_worker_completed',
      job_id: 'job_alpha_live_01',
      parent_request_id: null,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      thread_label: 'Workspace UI Contract',
      title: 'Complete contract schema validation pass',
      operation: 'worker_dispatch',
      actor: CODEX_COWORKER_ACTOR,
      executor: AGY_WORKER_EXECUTOR,
      state: 'completed',
      started_at_ms: FIXTURE_BASE_NOW_MS - 70_000,
      finished_at_ms: FIXTURE_BASE_NOW_MS - 25_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 25_000,
      duration_ms: 45_000,
      usage: REPORTED_WORKER_USAGE,
      delivery: [
        {
          stage: 'response_returned',
          at_ms: FIXTURE_BASE_NOW_MS - 24_500,
          recipient: 'coworker',
          summary: 'Returned validated contract diff.',
        },
      ],
      children: [completedWorkerCall],
      children_count: 1,
      summary: 'Completed Worker contract schema validation pass.',
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'req_alpha_completed',
      job_id: 'job_alpha_live_01',
      parent_request_id: null,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      thread_label: 'Workspace UI Contract',
      title: 'Validate shared contract types',
      operation: 'mcp.validate_contract',
      actor: CODEX_COWORKER_ACTOR,
      executor: LOCAL_MCP_EXECUTOR,
      state: 'completed',
      started_at_ms: FIXTURE_BASE_NOW_MS - 72_000,
      finished_at_ms: FIXTURE_BASE_NOW_MS - 71_800,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 71_800,
      duration_ms: 200,
      usage: EMPTY_USAGE,
      delivery: [
        {
          stage: 'response_returned',
          at_ms: FIXTURE_BASE_NOW_MS - 71_800,
          recipient: 'coworker',
          summary: 'Validated shared contract types via local Threadlane MCP.',
        },
      ],
      children: [localMcpCall],
      children_count: 1,
      summary: 'Validated shared contract types via local Threadlane MCP.',
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'req_beta_only',
      job_id: 'job_beta_01',
      parent_request_id: null,
      project_label: 'threadlane-alpha',
      binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
      thread_uuid: CHAT_BETA_SAME_PROJECT_IDENTITY.thread_uuid,
      thread_label: 'Release Verification Gate',
      title: 'Beta-only verification check',
      operation: 'worker_dispatch',
      actor: CODEX_COWORKER_ACTOR,
      executor: AGY_WORKER_EXECUTOR,
      state: 'completed',
      started_at_ms: FIXTURE_BASE_NOW_MS - 49_000,
      finished_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      duration_ms: 46_000,
      usage: REPORTED_WORKER_USAGE,
      delivery: [],
      children: [],
      children_count: 0,
      summary: BETA_CHAT_ONLY_SENTINEL,
    },
    {
      source_id: FIXTURE_SOURCE_ID,
      id: 'req_wrong_scope_99',
      job_id: null,
      parent_request_id: null,
      project_label: 'threadlane-alpha',
      binding_id: null,
      thread_uuid: WRONG_SCOPE_UNALIGNED_THREAD_UUID,
      thread_label: 'Workspace UI Contract',
      title: 'Unaligned wrong-scope request with colliding title',
      operation: 'worker_dispatch',
      actor: CODEX_COWORKER_ACTOR,
      executor: AGY_WORKER_EXECUTOR,
      state: 'completed',
      started_at_ms: FIXTURE_BASE_NOW_MS - 3_900,
      finished_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      duration_ms: 1_900,
      usage: EMPTY_USAGE,
      delivery: [],
      children: [],
      children_count: 0,
      summary: WRONG_SCOPE_QUARANTINE_SENTINEL,
    },
  ];

  const chats: WorkspaceChatSummary[] = [
    {
      identity: CHAT_ALPHA_IDENTITY,
      thread_label: 'Workspace UI Contract',
      project_label: 'threadlane-alpha',
      availability: 'available',
      activity: 'waiting_for_input',
      configured_selection: {
        selected_model: 'gpt-5.4',
        selected_effort: 'xhigh',
        evidence: 'verified_configuration',
        counts_as_runtime_telemetry: false,
      },
      active_job_count: 1,
      attention_count: 0,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
    },
    {
      identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
      thread_label: 'Release Verification Gate',
      project_label: 'threadlane-alpha',
      availability: 'available',
      activity: 'idle',
      configured_selection: {
        selected_model: 'gpt-5.4',
        selected_effort: 'high',
        evidence: 'verified_configuration',
        counts_as_runtime_telemetry: false,
      },
      active_job_count: 0,
      attention_count: 0,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
    },
    {
      identity: CHAT_GAMMA_IDENTITY,
      thread_label: 'Archived Migration Audit',
      project_label: 'threadlane-legacy',
      availability: 'available',
      activity: 'idle',
      configured_selection: {
        selected_model: 'gpt-5.4',
        selected_effort: 'medium',
        evidence: 'verified_configuration',
        counts_as_runtime_telemetry: false,
      },
      active_job_count: 0,
      attention_count: 0,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 120_000,
    },
  ];

  const nativeSamples: WorkspaceNativeSample[] = [
    createNativeSample({
      identity: CHAT_ALPHA_IDENTITY,
      sequence: 15,
      observedAtMs: FIXTURE_BASE_NOW_MS - 1_200,
      activity: 'waiting_for_input',
      selectedModel: 'gpt-5.4',
      selectedEffort: 'xhigh',
    }),
    createNativeSample({
      identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
      sequence: 9,
      observedAtMs: FIXTURE_BASE_NOW_MS - 2_500,
      activity: 'idle',
      selectedModel: 'gpt-5.4',
      selectedEffort: 'high',
    }),
    createNativeSample({
      identity: CHAT_GAMMA_IDENTITY,
      sequence: 4,
      observedAtMs: FIXTURE_BASE_NOW_MS - 4_000,
      activity: 'idle',
      selectedModel: 'gpt-5.4',
      selectedEffort: 'medium',
    }),
  ];

  const jobHistories: WorkspaceJobHistory[] = [
    {
      job: {
        source_id: FIXTURE_SOURCE_ID,
        job_id: 'job_alpha_live_01',
        binding_id: CHAT_ALPHA_IDENTITY.binding_id,
        chat_identity: CHAT_ALPHA_IDENTITY,
        title: 'Workspace Contract Acceptance Slice',
        job_revision: 3,
        state: 'running',
        phase: 'implementation',
        approval_status: 'approved',
        execution_status: 'running',
        review_state: 'not_started',
        created_at_ms: FIXTURE_BASE_NOW_MS - 90_000,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
        summary: 'Worker coder slice running in isolated checkout while Coworker awaits operator input.',
      },
      tasks: [
        {
          task_id: 'task_alpha_01',
          title: 'Build acceptance suite against shared workspace contract',
          role: 'coder',
          difficulty: 'hard',
          stage: 'implementation',
          status: 'running',
          depends_on: [],
          current_attempt_no: 1,
          attempts: [
            {
              attempt_no: 1,
              status: 'running',
              run_id: 'run_alpha_01',
              worker_id: 'wrk_alpha_01',
              outbox_id: 'out_alpha_01',
              executor: AGY_WORKER_EXECUTOR,
              started_at_ms: FIXTURE_BASE_NOW_MS - 14_000,
              finished_at_ms: null,
              summary: 'Executing isolated Playwright and Wayland acceptance checks.',
            },
          ],
          candidates: [],
          active_candidate_id: null,
          accepted_candidate_id: null,
          historical_failure: null,
          current_attention: null,
          acceptance_proof: createEmptyAcceptanceProof('not_accepted'),
        },
      ],
      runs: [
        {
          run_id: 'run_alpha_01',
          worker_id: 'wrk_alpha_01',
          binding_id: CHAT_ALPHA_IDENTITY.binding_id,
          task_id: 'task_alpha_01',
          attempt_no: 1,
          role: 'coder',
          status: 'running',
          executor: AGY_WORKER_EXECUTOR,
          started_at_ms: FIXTURE_BASE_NOW_MS - 14_000,
          finished_at_ms: null,
          updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
        },
      ],
      outbox: [],
      combined_candidates: [],
      result: null,
      links: {
        source_plan_id: 'plan_alpha_01',
        source_plan_revision: 2,
        source_approval_id: 'apr_alpha_01',
        source_work_brief_id: 'wb_alpha_01',
        planning_grant_id: null,
        planning_reservation_id: null,
        supersedes_job_id: null,
        superseded_by_job_id: null,
        parent_job_id: null,
        follow_up_job_ids: [],
        architect_advice_ids: [],
        source_request_ids: [
          'req_alpha_running',
          'req_alpha_worker_completed',
          'req_alpha_completed',
        ],
      },
      historical_failure: null,
      current_attention: null,
      acceptance_proof: createEmptyAcceptanceProof('not_accepted'),
      truncation: {
        tasks_omitted: 0,
        attempts_omitted: 0,
        candidates_omitted: 0,
        runs_omitted: 0,
        outbox_omitted: 0,
        combined_candidates_omitted: 0,
      },
    },
    {
      job: {
        source_id: FIXTURE_SOURCE_ID,
        job_id: 'job_beta_01',
        binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
        chat_identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
        title: 'Sibling Chat Release Gate',
        job_revision: 2,
        state: 'completed',
        phase: 'closed',
        approval_status: 'approved',
        execution_status: 'completed',
        review_state: 'accepted',
        created_at_ms: FIXTURE_BASE_NOW_MS - 180_000,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
        summary: BETA_CHAT_ONLY_SENTINEL,
      },
      tasks: [],
      runs: [],
      outbox: [],
      combined_candidates: [
        {
          combined_candidate_id: 'comb_beta_01',
          job_revision: 2,
          status: 'accepted',
          final_commit_sha: '1111111111111111111111111111111111111111',
          final_tree_sha: '2222222222222222222222222222222222222222',
          candidate_digest: 'sha256:beta01',
          closure_reservation_id: 'cres_beta_01',
          check_count: 3,
          produced_at_ms: FIXTURE_BASE_NOW_MS - 4_000,
        },
      ],
      result: {
        outcome: 'accepted',
        summary: BETA_CHAT_ONLY_SENTINEL,
        produced_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
        delivery_stage: 'response_returned',
        closure_verdict_id: 'cverd_beta_01',
      },
      links: {
        source_plan_id: 'plan_beta_01',
        source_plan_revision: 1,
        source_approval_id: 'apr_beta_01',
        source_work_brief_id: null,
        planning_grant_id: null,
        planning_reservation_id: null,
        supersedes_job_id: null,
        superseded_by_job_id: null,
        parent_job_id: null,
        follow_up_job_ids: [],
        architect_advice_ids: [],
        source_request_ids: ['req_beta_only'],
      },
      historical_failure: null,
      current_attention: null,
      acceptance_proof: {
        status: 'verified_accepted',
        accepted_candidate_id: 'cand_beta_01',
        combined_candidate_id: 'comb_beta_01',
        review_verdict_id: 'rverd_beta_01',
        review_receipt_id: 'rrcpt_beta_01',
        integration_receipt_id: 'ircpt_beta_01',
        closure_reservation_id: 'cres_beta_01',
        closure_verdict_id: 'cverd_beta_01',
        verified_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
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
    {
      job: {
        source_id: FIXTURE_SOURCE_ID,
        job_id: 'job_gamma_01',
        binding_id: CHAT_GAMMA_IDENTITY.binding_id,
        chat_identity: CHAT_GAMMA_IDENTITY,
        title: 'Legacy Migration Audit',
        job_revision: 1,
        state: 'completed',
        phase: 'closed',
        approval_status: 'approved',
        execution_status: 'completed',
        review_state: 'accepted',
        created_at_ms: FIXTURE_BASE_NOW_MS - 300_000,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 120_000,
        summary: 'Completed legacy audit.',
      },
      tasks: [],
      runs: [],
      outbox: [],
      combined_candidates: [],
      result: null,
      links: {
        source_plan_id: 'plan_gamma_01',
        source_plan_revision: 1,
        source_approval_id: 'apr_gamma_01',
        source_work_brief_id: null,
        planning_grant_id: null,
        planning_reservation_id: null,
        supersedes_job_id: null,
        superseded_by_job_id: null,
        parent_job_id: null,
        follow_up_job_ids: [],
        architect_advice_ids: [],
        source_request_ids: [],
      },
      historical_failure: null,
      current_attention: null,
      acceptance_proof: createEmptyAcceptanceProof('not_accepted'),
      truncation: {
        tasks_omitted: 0,
        attempts_omitted: 0,
        candidates_omitted: 0,
        runs_omitted: 0,
        outbox_omitted: 0,
        combined_candidates_omitted: 0,
      },
    },
  ];

  const observation: ObservationSnapshot = {
    schema_version: OBSERVATION_SCHEMA_VERSION,
    generated_at_ms: FIXTURE_BASE_NOW_MS,
    sources: [createBaseSource()],
    jobs: observationJobs,
    requests: observationRequests,
    next_cursor: null,
    has_more: false,
    jobs_has_more: false,
    warnings: [],
    warning_contexts: [],
  };

  return parseWorkspaceSnapshot({
    schema_version: WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
    generated_at_ms: FIXTURE_BASE_NOW_MS,
    sampling_policy: {
      interval_ms: WORKSPACE_SAMPLING_INTERVAL_MS,
      metadata_timeout_ms: WORKSPACE_METADATA_TIMEOUT_MS,
      stale_threshold_ms: WORKSPACE_SAMPLE_STALE_THRESHOLD_MS,
      snapshot_resets_sample_age: false,
    },
    observation,
    chats,
    native_samples: nativeSamples,
    job_histories: jobHistories,
    messages: {
      availability: 'available',
      items: [...alphaMessages, betaMessage, gammaMessage, wrongScopeItem],
      max_items: MAX_WORKSPACE_MESSAGES_WINDOW,
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
  });
}

/**
 * Produces a stale native sample snapshot by advancing `generated_at_ms` past
 * `WORKSPACE_SAMPLE_STALE_THRESHOLD_MS` without resetting `observed_at_ms` or `last_success_at_ms`.
 */
export function buildStaleWorkspaceSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  return regenerateWorkspaceSnapshotAt(
    base,
    FIXTURE_BASE_NOW_MS + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 15_000,
  );
}

/**
 * Produces a disconnected native sample snapshot where the native bridge is offline
 * while durable job and message records remain inspectable. Advances envelope generation
 * and sample sequence monotonically after `buildStaleWorkspaceSnapshot()` without faking
 * `last_success_at_ms`.
 */
export function buildDisconnectedWorkspaceSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  const disconnectedAtMs = FIXTURE_BASE_NOW_MS + WORKSPACE_SAMPLE_STALE_THRESHOLD_MS + 20_000;
  const disconnectedSamples = base.native_samples.map((sample) =>
    sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id
      ? createNativeSample({
          identity: CHAT_ALPHA_IDENTITY,
          sequence: sample.sequence + 1,
          observedAtMs: disconnectedAtMs - 500,
          lastSuccessAtMs: sample.last_success_at_ms,
          availability: 'disconnected',
          activity: 'unknown',
          connectionState: 'disconnected',
          freshness: 'stale',
          diagnostic: 'Native Codex bridge socket closed; showing durable local history only.',
          selectedModel: 'gpt-5.4',
          selectedEffort: 'xhigh',
          selectionEvidence: 'unavailable',
        })
      : sample,
  );
  return parseWorkspaceSnapshot({
    ...base,
    generated_at_ms: disconnectedAtMs,
    observation: {
      ...base.observation,
      generated_at_ms: disconnectedAtMs,
    },
    native_samples: disconnectedSamples,
  });
}

/**
 * Produces a fixture distinguishing a resolved/superseded historical attempt failure from
 * active human attention (`needs_approval`).
 */
export function buildHistoricalFailureWorkspaceSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  const recoveredJob: ObservationJob = {
    source_id: FIXTURE_SOURCE_ID,
    id: 'job_hist_recovered_01',
    title: 'Recovered Retry Pipeline',
    project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
    project_label: 'threadlane-alpha',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: 'Workspace UI Contract',
    plan_id: 'plan_hist_01',
    plan_revision: 1,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: 'accepted',
    updated_at_ms: FIXTURE_BASE_NOW_MS - 8_000,
    active_call_count: 0,
    tasks: [
      {
        id: 'task_retry_01',
        title: 'Run deterministic worktree check',
        status: 'completed',
        depends_on: [],
      },
    ],
    summary: 'Attempt 2 completed cleanly after attempt 1 timeout was superseded.',
  };

  const activeAttentionJob: ObservationJob = {
    source_id: FIXTURE_SOURCE_ID,
    id: 'job_active_attention_02',
    title: 'Plan Approval Gate',
    project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
    project_label: 'threadlane-alpha',
    binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
    thread_uuid: CHAT_BETA_SAME_PROJECT_IDENTITY.thread_uuid,
    thread_label: 'Release Verification Gate',
    plan_id: 'plan_attn_02',
    plan_revision: 2,
    state: 'needs_user',
    approval_status: 'pending',
    execution_status: 'not_started',
    review_state: null,
    updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
    active_call_count: 0,
    tasks: [],
    summary: 'Awaiting explicit human approval for plan revision 2.',
  };

  const recoveredHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_hist_recovered_01',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Recovered Retry Pipeline',
      job_revision: 4,
      state: 'completed',
      phase: 'closed',
      approval_status: 'approved',
      execution_status: 'completed',
      review_state: 'accepted',
      created_at_ms: FIXTURE_BASE_NOW_MS - 240_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 8_000,
      summary: 'Attempt 2 completed cleanly after attempt 1 timeout was superseded.',
    },
    tasks: [
      {
        task_id: 'task_retry_01',
        title: 'Run deterministic worktree check',
        role: 'coder',
        difficulty: 'medium',
        stage: 'closed',
        status: 'completed',
        depends_on: [],
        current_attempt_no: 2,
        attempts: [
          {
            attempt_no: 1,
            status: 'failed',
            run_id: 'run_retry_att1',
            worker_id: 'wrk_retry_1',
            outbox_id: 'out_retry_1',
            executor: AGY_WORKER_EXECUTOR,
            started_at_ms: FIXTURE_BASE_NOW_MS - 220_000,
            finished_at_ms: FIXTURE_BASE_NOW_MS - 190_000,
            summary: HISTORICAL_FAILURE_REASON,
          },
          {
            attempt_no: 2,
            status: 'completed',
            run_id: 'run_retry_att2',
            worker_id: 'wrk_retry_2',
            outbox_id: 'out_retry_2',
            executor: AGY_WORKER_EXECUTOR,
            started_at_ms: FIXTURE_BASE_NOW_MS - 180_000,
            finished_at_ms: FIXTURE_BASE_NOW_MS - 10_000,
            summary: 'Attempt 2 passed all checks.',
          },
        ],
        candidates: [
          {
            candidate_id: 'cand_retry_02',
            attempt_no: 2,
            worker_id: 'wrk_retry_2',
            head_commit_sha: '3333333333333333333333333333333333333333',
            tree_sha: '4444444444444444444444444444444444444444',
            verification_passed: true,
            review_status: 'accepted',
            review_verdict_id: 'rverd_retry_02',
            review_receipt_id: 'rrcpt_retry_02',
            produced_at_ms: FIXTURE_BASE_NOW_MS - 10_000,
            summary: 'Clean retry candidate.',
          },
        ],
        active_candidate_id: 'cand_retry_02',
        accepted_candidate_id: 'cand_retry_02',
        historical_failure: {
          failure_id: 'fail_hist_att1',
          stage: 'coder_execution',
          failed_at_ms: FIXTURE_BASE_NOW_MS - 190_000,
          task_id: 'task_retry_01',
          attempt_no: 1,
          run_id: 'run_retry_att1',
          outbox_id: 'out_retry_1',
          reason: HISTORICAL_FAILURE_REASON,
          superseded_or_resolved: true,
        },
        current_attention: null,
        acceptance_proof: {
          status: 'verified_accepted',
          accepted_candidate_id: 'cand_retry_02',
          combined_candidate_id: 'comb_retry_02',
          review_verdict_id: 'rverd_retry_02',
          review_receipt_id: 'rrcpt_retry_02',
          integration_receipt_id: 'ircpt_retry_02',
          closure_reservation_id: 'cres_retry_02',
          closure_verdict_id: 'cverd_retry_02',
          verified_at_ms: FIXTURE_BASE_NOW_MS - 8_000,
        },
      },
    ],
    runs: [],
    outbox: [],
    combined_candidates: [
      {
        combined_candidate_id: 'comb_retry_02',
        job_revision: 4,
        status: 'accepted',
        final_commit_sha: '3333333333333333333333333333333333333333',
        final_tree_sha: '4444444444444444444444444444444444444444',
        candidate_digest: 'sha256:retry02',
        closure_reservation_id: 'cres_retry_02',
        check_count: 4,
        produced_at_ms: FIXTURE_BASE_NOW_MS - 8_500,
      },
    ],
    result: {
      outcome: 'accepted',
      summary: 'Accepted after attempt 2 recovery.',
      produced_at_ms: FIXTURE_BASE_NOW_MS - 8_000,
      delivery_stage: 'response_returned',
      closure_verdict_id: 'cverd_retry_02',
    },
    links: {
      source_plan_id: 'plan_hist_01',
      source_plan_revision: 1,
      source_approval_id: 'apr_hist_01',
      source_work_brief_id: null,
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: [],
    },
    historical_failure: {
      failure_id: 'fail_hist_att1',
      stage: 'coder_execution',
      failed_at_ms: FIXTURE_BASE_NOW_MS - 190_000,
      task_id: 'task_retry_01',
      attempt_no: 1,
      run_id: 'run_retry_att1',
      outbox_id: 'out_retry_1',
      reason: HISTORICAL_FAILURE_REASON,
      superseded_or_resolved: true,
    },
    current_attention: null,
    acceptance_proof: {
      status: 'verified_accepted',
      accepted_candidate_id: 'cand_retry_02',
      combined_candidate_id: 'comb_retry_02',
      review_verdict_id: 'rverd_retry_02',
      review_receipt_id: 'rrcpt_retry_02',
      integration_receipt_id: 'ircpt_retry_02',
      closure_reservation_id: 'cres_retry_02',
      closure_verdict_id: 'cverd_retry_02',
      verified_at_ms: FIXTURE_BASE_NOW_MS - 8_000,
    },
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
  };

  const activeAttentionHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_active_attention_02',
      binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
      chat_identity: CHAT_BETA_SAME_PROJECT_IDENTITY,
      title: 'Plan Approval Gate',
      job_revision: 1,
      state: 'needs_user',
      phase: 'approval',
      approval_status: 'pending',
      execution_status: 'not_started',
      review_state: null,
      created_at_ms: FIXTURE_BASE_NOW_MS - 40_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      summary: 'Awaiting explicit human approval for plan revision 2.',
    },
    tasks: [],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: {
      source_plan_id: 'plan_attn_02',
      source_plan_revision: 2,
      source_approval_id: null,
      source_work_brief_id: null,
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: [],
    },
    historical_failure: null,
    current_attention: {
      kind: 'needs_approval',
      reason: 'Awaiting explicit human approval for plan revision 2.',
      next_step: 'Review and approve published plan revision 2 in Codex.',
      blocking_hold_ids: ['hold_plan_02'],
      blocking_outbox_ids: [],
      question_id: null,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
    },
    acceptance_proof: createEmptyAcceptanceProof('not_accepted'),
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
  };

  return parseWorkspaceSnapshot({
    ...base,
    chats: base.chats.map((chat) => {
      if (chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id) {
        return { ...chat, active_job_count: 0, attention_count: 0 };
      }
      if (chat.identity.binding_id === CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id) {
        return { ...chat, active_job_count: 1, attention_count: 1 };
      }
      return chat;
    }),
    observation: {
      ...base.observation,
      jobs: [recoveredJob, activeAttentionJob],
      requests: [],
    },
    job_histories: [recoveredHistory, activeAttentionHistory],
  });
}

/**
 * Produces a fixture where a Worker call has finished (`completed`) and produced a task
 * candidate, while authoritative job acceptance (`review_verdict_id` / `closure_verdict_id`)
 * is still pending (`pending_review`), contrasted with a `verified_accepted` job.
 */
export function buildPendingAcceptanceWorkspaceSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();

  const pendingJob: ObservationJob = {
    source_id: FIXTURE_SOURCE_ID,
    id: 'job_pending_accept_01',
    title: 'Worker Finished Pending Acceptance Slice',
    project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
    project_label: 'threadlane-alpha',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: 'Workspace UI Contract',
    plan_id: 'plan_pend_01',
    plan_revision: 1,
    state: 'reviewing',
    approval_status: 'approved',
    execution_status: 'worker_completed_pending_review',
    review_state: 'pending',
    updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
    active_call_count: 0,
    tasks: [
      {
        id: 'task_pend_01',
        title: 'Produce candidate cand_pending_01',
        status: 'ready_for_review',
        depends_on: [],
      },
    ],
    summary: PENDING_ACCEPTANCE_SUMMARY,
  };

  const acceptedJob: ObservationJob = {
    source_id: FIXTURE_SOURCE_ID,
    id: 'job_verified_accepted_02',
    title: 'Closure Verified Package Slice',
    project_path: FIXTURE_CANONICAL_PROJECT_ALPHA,
    project_label: 'threadlane-alpha',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: 'Workspace UI Contract',
    plan_id: 'plan_acc_02',
    plan_revision: 1,
    state: 'completed',
    approval_status: 'approved',
    execution_status: 'completed',
    review_state: 'accepted',
    updated_at_ms: FIXTURE_BASE_NOW_MS - 20_000,
    active_call_count: 0,
    tasks: [],
    summary: VERIFIED_ACCEPTED_SUMMARY,
  };

  const finishedWorkerRequest: ObservationRequest = {
    source_id: FIXTURE_SOURCE_ID,
    id: 'req_coder_finished_pending_accept',
    job_id: 'job_pending_accept_01',
    parent_request_id: null,
    project_label: 'threadlane-alpha',
    binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
    thread_label: 'Workspace UI Contract',
    title: 'Execute coder candidate cand_pending_01',
    operation: 'worker_dispatch',
    actor: CODEX_COWORKER_ACTOR,
    executor: AGY_WORKER_EXECUTOR,
    state: 'completed',
    started_at_ms: FIXTURE_BASE_NOW_MS - 58_000,
    finished_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
    updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
    duration_ms: 55_000,
    usage: REPORTED_WORKER_USAGE,
    delivery: [
      {
        stage: 'result_ready',
        at_ms: FIXTURE_BASE_NOW_MS - 3_000,
        recipient: 'reviewer',
        summary: 'Candidate cand_pending_01 produced; awaiting review and Closure acceptance.',
      },
    ],
    children: [],
    children_count: 0,
    summary: PENDING_ACCEPTANCE_SUMMARY,
  };

  const pendingHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_pending_accept_01',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Worker Finished Pending Acceptance Slice',
      job_revision: 3,
      state: 'reviewing',
      phase: 'review',
      approval_status: 'approved',
      execution_status: 'worker_completed_pending_review',
      review_state: 'pending',
      created_at_ms: FIXTURE_BASE_NOW_MS - 100_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      summary: PENDING_ACCEPTANCE_SUMMARY,
    },
    tasks: [
      {
        task_id: 'task_pend_01',
        title: 'Produce candidate cand_pending_01',
        role: 'coder',
        difficulty: 'hard',
        stage: 'review',
        status: 'ready_for_review',
        depends_on: [],
        current_attempt_no: 1,
        attempts: [
          {
            attempt_no: 1,
            status: 'completed',
            run_id: 'run_pend_01',
            worker_id: 'wrk_pend_01',
            outbox_id: 'out_pend_01',
            executor: AGY_WORKER_EXECUTOR,
            started_at_ms: FIXTURE_BASE_NOW_MS - 58_000,
            finished_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
            summary: 'Worker execution finished; candidate awaits review.',
          },
        ],
        candidates: [
          {
            candidate_id: 'cand_pending_01',
            attempt_no: 1,
            worker_id: 'wrk_pend_01',
            head_commit_sha: '5555555555555555555555555555555555555555',
            tree_sha: '6666666666666666666666666666666666666666',
            verification_passed: true,
            review_status: 'pending',
            review_verdict_id: null,
            review_receipt_id: null,
            produced_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
            summary: PENDING_ACCEPTANCE_SUMMARY,
          },
        ],
        active_candidate_id: 'cand_pending_01',
        accepted_candidate_id: null,
        historical_failure: null,
        current_attention: {
          kind: 'review_required',
          reason: PENDING_ACCEPTANCE_SUMMARY,
          next_step: 'Complete implementation review and Closure verification before acceptance.',
          blocking_hold_ids: [],
          blocking_outbox_ids: [],
          question_id: null,
          updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
        },
        acceptance_proof: createEmptyAcceptanceProof('pending_review'),
      },
    ],
    runs: [
      {
        run_id: 'run_pend_01',
        worker_id: 'wrk_pend_01',
        binding_id: CHAT_ALPHA_IDENTITY.binding_id,
        task_id: 'task_pend_01',
        attempt_no: 1,
        role: 'coder',
        status: 'completed',
        executor: AGY_WORKER_EXECUTOR,
        started_at_ms: FIXTURE_BASE_NOW_MS - 58_000,
        finished_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      },
    ],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: {
      source_plan_id: 'plan_pend_01',
      source_plan_revision: 1,
      source_approval_id: 'apr_pend_01',
      source_work_brief_id: null,
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: ['req_coder_finished_pending_accept'],
    },
    historical_failure: null,
    current_attention: {
      kind: 'review_required',
      reason: PENDING_ACCEPTANCE_SUMMARY,
      next_step: 'Complete implementation review and Closure verification before acceptance.',
      blocking_hold_ids: [],
      blocking_outbox_ids: [],
      question_id: null,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
    },
    acceptance_proof: createEmptyAcceptanceProof('pending_review'),
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
  };

  const acceptedHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_verified_accepted_02',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Closure Verified Package Slice',
      job_revision: 5,
      state: 'completed',
      phase: 'closed',
      approval_status: 'approved',
      execution_status: 'completed',
      review_state: 'accepted',
      created_at_ms: FIXTURE_BASE_NOW_MS - 200_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 20_000,
      summary: VERIFIED_ACCEPTED_SUMMARY,
    },
    tasks: [],
    runs: [],
    outbox: [],
    combined_candidates: [
      {
        combined_candidate_id: 'comb_accepted_02',
        job_revision: 5,
        status: 'accepted',
        final_commit_sha: '7777777777777777777777777777777777777777',
        final_tree_sha: '8888888888888888888888888888888888888888',
        candidate_digest: 'sha256:accepted02',
        closure_reservation_id: 'cres_accepted_02',
        check_count: 5,
        produced_at_ms: FIXTURE_BASE_NOW_MS - 21_000,
      },
    ],
    result: {
      outcome: 'accepted',
      summary: VERIFIED_ACCEPTED_SUMMARY,
      produced_at_ms: FIXTURE_BASE_NOW_MS - 20_000,
      delivery_stage: 'response_returned',
      closure_verdict_id: 'cverd_accepted_02',
    },
    links: {
      source_plan_id: 'plan_acc_02',
      source_plan_revision: 1,
      source_approval_id: 'apr_acc_02',
      source_work_brief_id: null,
      planning_grant_id: null,
      planning_reservation_id: null,
      supersedes_job_id: null,
      superseded_by_job_id: null,
      parent_job_id: null,
      follow_up_job_ids: [],
      architect_advice_ids: [],
      source_request_ids: [],
    },
    historical_failure: null,
    current_attention: null,
    acceptance_proof: {
      status: 'verified_accepted',
      accepted_candidate_id: 'cand_accepted_02',
      combined_candidate_id: 'comb_accepted_02',
      review_verdict_id: 'rverd_accepted_02',
      review_receipt_id: 'rrcpt_accepted_02',
      integration_receipt_id: 'ircpt_accepted_02',
      closure_reservation_id: 'cres_accepted_02',
      closure_verdict_id: 'cverd_accepted_02',
      verified_at_ms: FIXTURE_BASE_NOW_MS - 20_000,
    },
    truncation: {
      tasks_omitted: 0,
      attempts_omitted: 0,
      candidates_omitted: 0,
      runs_omitted: 0,
      outbox_omitted: 0,
      combined_candidates_omitted: 0,
    },
  };

  return parseWorkspaceSnapshot({
    ...base,
    chats: base.chats.map((chat) =>
      chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id
        ? { ...chat, active_job_count: 1, attention_count: 1 }
        : chat,
    ),
    observation: {
      ...base.observation,
      jobs: [pendingJob, acceptedJob],
      requests: [finishedWorkerRequest],
    },
    job_histories: [pendingHistory, acceptedHistory],
    messages: {
      ...base.messages,
      items: [
        createChatMessageItem({
          identity: CHAT_ALPHA_IDENTITY,
          seq: 1,
          recordId: 'rec_pend_msg_01',
          sourceKind: 'task_result_summary',
          actorRole: 'coder',
          jobId: 'job_pending_accept_01',
          summary: PENDING_ACCEPTANCE_SUMMARY,
          recordedAtMs: FIXTURE_BASE_NOW_MS - 2_000,
        }),
        createChatMessageItem({
          identity: CHAT_ALPHA_IDENTITY,
          seq: 2,
          recordId: 'rec_acc_msg_02',
          sourceKind: 'closure_summary',
          actorRole: 'closure',
          jobId: 'job_verified_accepted_02',
          summary: VERIFIED_ACCEPTED_SUMMARY,
          recordedAtMs: FIXTURE_BASE_NOW_MS - 20_000,
        }),
      ],
    },
  });
}

export function buildExternalResolutionWorkspaceSnapshot(): WorkspaceSnapshot {
  const base = buildInteractiveWorkspaceSnapshot();
  const alphaChat = base.chats.find(
    (chat) => chat.identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
  )!;
  const alphaSample = base.native_samples.find(
    (sample) => sample.chat_identity.binding_id === CHAT_ALPHA_IDENTITY.binding_id,
  )!;

  const createAttention = (
    kind: NonNullable<WorkspaceTaskHistory['current_attention']>['kind'],
    reason: string,
    overrides: Partial<
      NonNullable<WorkspaceTaskHistory['current_attention']>
    > = {},
  ): NonNullable<WorkspaceTaskHistory['current_attention']> => ({
    kind,
    reason,
    next_step: 'Resolve this item in Codex.',
    blocking_hold_ids: [],
    blocking_outbox_ids: [],
    question_id: null,
    updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
    ...overrides,
  });

  const emptyTruncation = (): WorkspaceJobHistory['truncation'] => ({
    tasks_omitted: 0,
    attempts_omitted: 0,
    candidates_omitted: 0,
    runs_omitted: 0,
    outbox_omitted: 0,
    combined_candidates_omitted: 0,
  });

  const createLinks = (
    planId: string,
    sourceRequestIds: string[] = [],
  ): WorkspaceJobHistory['links'] => ({
    source_plan_id: planId,
    source_plan_revision: 1,
    source_approval_id: `apr_${planId}`,
    source_work_brief_id: `wb_${planId}`,
    planning_grant_id: null,
    planning_reservation_id: null,
    supersedes_job_id: null,
    superseded_by_job_id: null,
    parent_job_id: null,
    follow_up_job_ids: [],
    architect_advice_ids: [],
    source_request_ids: sourceRequestIds,
  });

  const createTask = (
    overrides: Partial<WorkspaceTaskHistory> &
      Pick<WorkspaceTaskHistory, 'task_id' | 'title'>,
  ): WorkspaceTaskHistory => ({
    task_id: overrides.task_id,
    title: overrides.title,
    role: 'coder',
    difficulty: 'hard',
    stage: 'closed',
    status: 'completed',
    depends_on: [],
    current_attempt_no: 1,
    attempts: [
      {
        attempt_no: 1,
        status: 'completed',
        run_id: `run_${overrides.task_id}`,
        worker_id: `wrk_${overrides.task_id}`,
        outbox_id: null,
        executor: AGY_WORKER_EXECUTOR,
        started_at_ms: FIXTURE_BASE_NOW_MS - 20_000,
        finished_at_ms: FIXTURE_BASE_NOW_MS - 10_000,
        summary: 'Worker completed the task slice.',
      },
    ],
    candidates: [],
    active_candidate_id: null,
    accepted_candidate_id: null,
    historical_failure: null,
    current_attention: null,
    acceptance_proof: createEmptyAcceptanceProof('not_accepted'),
    ...overrides,
  });

  const fullReceipt: WorkspaceExternalResolution = {
    receipt_id: EXTERNAL_RESOLUTION_FULL_RECEIPT_ID,
    request_key: 'req_ext_fixture_full_01',
    preview_digest: 'digest_ext_fixture_full_01',
    outcome: 'resolved_externally',
    scope: 'job',
    task_ids: ['task_ext_full_01'],
    applicability: 'applicable',
    summary: EXTERNAL_RESOLUTION_FULL_SUMMARY,
    evidence_refs: ['ev_ext_fixture_full_01', 'ev_ext_fixture_full_02'],
    recorded_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
    covered_attention: [
      {
        task_id: null,
        attention: createAttention(
          'needs_approval',
          EXTERNAL_RESOLUTION_COVERED_JOB_REASON,
          { blocking_hold_ids: ['hold_ext_fixture_job'] },
        ),
      },
      {
        task_id: 'task_ext_full_01',
        attention: createAttention(
          'on_hold',
          EXTERNAL_RESOLUTION_COVERED_TASK_REASON,
          { blocking_hold_ids: ['hold_ext_fixture_task'] },
        ),
      },
    ],
  };

  const partialReceipt: WorkspaceExternalResolution = {
    receipt_id: EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID,
    request_key: 'req_ext_fixture_partial_02',
    preview_digest: 'digest_ext_fixture_partial_02',
    outcome: 'resolved_externally',
    scope: 'tasks',
    task_ids: ['task_ext_partial_01'],
    applicability: 'applicable',
    summary: EXTERNAL_RESOLUTION_PARTIAL_SUMMARY,
    evidence_refs: ['ev_ext_fixture_partial_02'],
    recorded_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
    covered_attention: [
      {
        task_id: 'task_ext_partial_01',
        attention: createAttention(
          'needs_user_input',
          EXTERNAL_RESOLUTION_COVERED_TASK_REASON,
          { question_id: 'q_ext_fixture_partial_task' },
        ),
      },
    ],
  };

  const staleReceipt: WorkspaceExternalResolution = {
    receipt_id: EXTERNAL_RESOLUTION_STALE_RECEIPT_ID,
    request_key: 'req_ext_fixture_stale_03',
    preview_digest: 'digest_ext_fixture_stale_03',
    outcome: 'resolved_externally',
    scope: 'tasks',
    task_ids: ['task_ext_blocked_03'],
    applicability: 'stale',
    summary: EXTERNAL_RESOLUTION_STALE_SUMMARY,
    evidence_refs: ['ev_ext_fixture_stale_03'],
    recorded_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
    covered_attention: [
      {
        task_id: 'task_ext_blocked_03',
        attention: createAttention(
          'on_hold',
          EXTERNAL_RESOLUTION_COVERED_TASK_REASON,
          { blocking_hold_ids: ['hold_ext_fixture_stale'] },
        ),
      },
    ],
  };

  const unverifiedReceipt: WorkspaceExternalResolution = {
    receipt_id: EXTERNAL_RESOLUTION_UNVERIFIED_RECEIPT_ID,
    request_key: 'req_ext_fixture_unverified_04',
    preview_digest: 'digest_ext_fixture_unverified_04',
    outcome: 'resolved_externally',
    scope: 'job',
    task_ids: [],
    applicability: 'unverified',
    summary: EXTERNAL_RESOLUTION_UNVERIFIED_SUMMARY,
    evidence_refs: ['ev_ext_fixture_unverified_04'],
    recorded_at_ms: FIXTURE_BASE_NOW_MS - 4_000,
    covered_attention: [
      {
        task_id: null,
        attention: createAttention(
          'needs_approval',
          EXTERNAL_RESOLUTION_COVERED_JOB_REASON,
          { blocking_hold_ids: ['hold_ext_fixture_unverified'] },
        ),
      },
    ],
  };

  const unknownReceipt: WorkspaceExternalResolution = {
    receipt_id: EXTERNAL_RESOLUTION_UNKNOWN_RECEIPT_ID,
    request_key: 'req_ext_fixture_unknown_05',
    preview_digest: 'digest_ext_fixture_unknown_05',
    outcome: 'resolved_externally',
    scope: 'job',
    task_ids: ['task_ext_unknown_04'],
    applicability: 'applicable',
    summary: EXTERNAL_RESOLUTION_UNKNOWN_SUMMARY,
    evidence_refs: ['ev_ext_fixture_unknown_05'],
    recorded_at_ms: FIXTURE_BASE_NOW_MS - 5_000,
    covered_attention: [],
  };

  const fullHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_ext_full_01',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Externally Resolved Full Job',
      job_revision: 4,
      state: 'unknown',
      phase: 'external_resolution',
      approval_status: 'approved',
      execution_status: 'resolved_externally',
      review_state: null,
      created_at_ms: FIXTURE_BASE_NOW_MS - 90_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
      summary: EXTERNAL_RESOLUTION_FULL_SUMMARY,
    },
    tasks: [
      createTask({
        task_id: 'task_ext_full_01',
        title: 'Full job task',
      }),
    ],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: {
      outcome: 'completed',
      summary: 'Real recorded job result remains visible alongside the external receipt.',
      produced_at_ms: FIXTURE_BASE_NOW_MS - 6_000,
      delivery_stage: 'result_ready',
      closure_verdict_id: null,
    },
    links: createLinks('plan_ext_full_01', ['req_ext_fixture_full_01']),
    historical_failure: {
      failure_id: 'fail_ext_fixture_full_01',
      stage: 'worker_execution',
      failed_at_ms: FIXTURE_BASE_NOW_MS - 30_000,
      task_id: 'task_ext_full_01',
      attempt_no: 1,
      run_id: 'run_task_ext_full_01',
      outbox_id: null,
      reason: 'Historical failure remains visible even after external resolution.',
      superseded_or_resolved: true,
    },
    current_attention: null,
    acceptance_proof: createEmptyAcceptanceProof('unknown'),
    external_resolutions: [fullReceipt],
    truncation: emptyTruncation(),
  };

  const partialHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_ext_partial_02',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Externally Resolved Partial Job',
      job_revision: 3,
      state: 'failed',
      phase: 'external_resolution',
      approval_status: 'approved',
      execution_status: 'failed',
      review_state: null,
      created_at_ms: FIXTURE_BASE_NOW_MS - 85_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      summary: EXTERNAL_RESOLUTION_PARTIAL_SUMMARY,
    },
    tasks: [
      createTask({
        task_id: 'task_ext_partial_01',
        title: 'Partially resolved task',
      }),
      createTask({
        task_id: 'task_ext_unrelated_02',
        title: 'Unrelated task remains visible',
      }),
    ],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: createLinks('plan_ext_partial_02', ['req_ext_fixture_partial_02']),
    historical_failure: null,
    current_attention: null,
    acceptance_proof: createEmptyAcceptanceProof('unknown'),
    external_resolutions: [partialReceipt],
    truncation: emptyTruncation(),
  };

  const blockedHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_ext_blocked_03',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Blocked With Historical Receipts',
      job_revision: 2,
      state: 'needs_user',
      phase: 'approval',
      approval_status: 'pending',
      execution_status: 'held',
      review_state: null,
      created_at_ms: FIXTURE_BASE_NOW_MS - 80_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      summary: 'Uncovered approval remains active despite historical receipts.',
    },
    tasks: [
      createTask({
        task_id: 'task_ext_blocked_03',
        title: 'Held task with historical receipts',
        status: 'held',
        current_attention: createAttention(
          'on_hold',
          'Task hold remains active while stale and unverified receipts exist.',
          {
            blocking_hold_ids: ['hold_ext_fixture_blocked_task'],
            question_id: 'q_ext_fixture_blocked_task',
          },
        ),
      }),
    ],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: createLinks('plan_ext_blocked_03', []),
    historical_failure: null,
    current_attention: createAttention(
      'needs_approval',
      'Uncovered job approval remains active while stale and unverified receipts exist.',
      {
        blocking_hold_ids: ['hold_ext_fixture_blocked_job'],
        question_id: 'q_ext_fixture_blocked_job',
        updated_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      },
    ),
    acceptance_proof: createEmptyAcceptanceProof('unknown'),
    external_resolutions: [staleReceipt, unverifiedReceipt],
    truncation: emptyTruncation(),
  };

  const unknownHistory: WorkspaceJobHistory = {
    job: {
      source_id: FIXTURE_SOURCE_ID,
      job_id: 'job_ext_unknown_04',
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      chat_identity: CHAT_ALPHA_IDENTITY,
      title: 'Unknown Effect With Full Receipt',
      job_revision: 1,
      state: 'unknown',
      phase: 'execution',
      approval_status: 'approved',
      execution_status: 'unknown',
      review_state: null,
      created_at_ms: FIXTURE_BASE_NOW_MS - 75_000,
      updated_at_ms: FIXTURE_BASE_NOW_MS - 5_000,
      summary: 'Unknown task effect remains visible even with a full receipt.',
    },
    tasks: [
      createTask({
        task_id: 'task_ext_unknown_04',
        title: 'Unknown task effect',
        status: 'unknown',
        current_attention: null,
      }),
    ],
    runs: [],
    outbox: [],
    combined_candidates: [],
    result: null,
    links: createLinks('plan_ext_unknown_04', []),
    historical_failure: null,
    current_attention: null,
    acceptance_proof: createEmptyAcceptanceProof('unknown'),
    external_resolutions: [unknownReceipt],
    truncation: emptyTruncation(),
  };

  const receiptMessage = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: EXTERNAL_RESOLUTION_FULL_RECEIPT_ID,
    source_order: {
      recorded_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
      source_seq: 501,
      sub_index: 0,
    },
    scope: {
      source_id: CHAT_ALPHA_IDENTITY.source_id,
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      canonical_project_scope: CHAT_ALPHA_IDENTITY.canonical_project_scope,
      chat_identity: CHAT_ALPHA_IDENTITY,
      job_id: 'job_ext_full_01',
    },
    actor_role: 'operator',
    raw_summary: 'Backend-recorded external resolution receipt summary.',
  });
  const partialReceiptMessage = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: EXTERNAL_RESOLUTION_PARTIAL_RECEIPT_ID,
    source_order: {
      recorded_at_ms: FIXTURE_BASE_NOW_MS - 2_000,
      source_seq: 502,
      sub_index: 0,
    },
    scope: {
      source_id: CHAT_ALPHA_IDENTITY.source_id,
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      canonical_project_scope: CHAT_ALPHA_IDENTITY.canonical_project_scope,
      chat_identity: CHAT_ALPHA_IDENTITY,
      job_id: 'job_ext_partial_02',
    },
    actor_role: 'operator',
    raw_summary: 'Backend-recorded partial external resolution receipt summary.',
  });
  const staleReceiptMessage = createSafeWorkspaceMessageItem({
    source_kind: 'journal_summary',
    source_record_id: EXTERNAL_RESOLUTION_STALE_RECEIPT_ID,
    source_order: {
      recorded_at_ms: FIXTURE_BASE_NOW_MS - 3_000,
      source_seq: 503,
      sub_index: 0,
    },
    scope: {
      source_id: CHAT_ALPHA_IDENTITY.source_id,
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      canonical_project_scope: CHAT_ALPHA_IDENTITY.canonical_project_scope,
      chat_identity: CHAT_ALPHA_IDENTITY,
      job_id: 'job_ext_blocked_03',
    },
    actor_role: 'operator',
    raw_summary: 'Backend-recorded stale external resolution receipt summary.',
  });
  const unrelatedQuestion = createSafeWorkspaceMessageItem({
    source_kind: 'question_prompt',
    source_record_id: 'q_ext_fixture_unrelated_01',
    source_order: {
      recorded_at_ms: FIXTURE_BASE_NOW_MS - 1_500,
      source_seq: 504,
      sub_index: 0,
    },
    scope: {
      source_id: CHAT_ALPHA_IDENTITY.source_id,
      binding_id: CHAT_ALPHA_IDENTITY.binding_id,
      thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
      canonical_project_scope: CHAT_ALPHA_IDENTITY.canonical_project_scope,
      chat_identity: CHAT_ALPHA_IDENTITY,
      job_id: 'job_ext_full_01',
      task_id: 'task_ext_full_01',
    },
    actor_role: 'coworker',
    raw_summary: EXTERNAL_RESOLUTION_UNRELATED_QUESTION,
  });

  return parseWorkspaceSnapshot({
    ...base,
    observation: {
      ...base.observation,
      jobs: [],
      requests: [],
      warnings: [],
      warning_contexts: [],
    },
    chats: [
      {
        ...alphaChat,
        thread_label: 'External Resolution Chat',
        active_job_count: 1,
        attention_count: 1,
        updated_at_ms: FIXTURE_BASE_NOW_MS - 1_000,
      },
    ],
    native_samples: [{ ...alphaSample, activity: 'idle' }],
    job_histories: [fullHistory, partialHistory, blockedHistory, unknownHistory],
    messages: {
      ...base.messages,
      items: [
        receiptMessage,
        partialReceiptMessage,
        staleReceiptMessage,
        unrelatedQuestion,
      ],
    },
  });
}

export function buildDefaultRegistryFixture(): RegistrySnapshot {
  return {
    schema_version: 1,
    revision: 3,
    tools: [
      {
        id: 'tool-codex',
        label: 'Codex CLI',
        adapter: 'codex_cli',
        executable_path: null,
        enabled: true,
      },
      {
        id: 'tool-agy',
        label: 'Antigravity CLI',
        adapter: 'antigravity',
        executable_path: null,
        enabled: true,
      },
      {
        id: 'tool-opencode',
        label: 'OpenCode CLI',
        adapter: 'opencode_v2',
        executable_path: null,
        enabled: true,
      },
    ],
    models: [
      {
        id: 'model-gpt-5-4',
        tool_id: 'tool-codex',
        label: 'GPT-5.4',
        model_arg: 'gpt-5.4',
        efforts: ['medium', 'high', 'xhigh'],
        allow_no_effort: false,
      },
      {
        id: 'model-argon-high',
        tool_id: 'tool-agy',
        label: 'Gemini 4 Argon High',
        model_arg: 'gemini-4-argon-high',
        efforts: ['high'],
        allow_no_effort: false,
      },
      {
        id: 'model-deepseek-flash',
        tool_id: 'tool-opencode',
        label: 'DeepSeek v4.1 Flash',
        model_arg: 'opencode-go/deepseek-v4.1-flash',
        efforts: ['max'],
        allow_no_effort: false,
      },
    ],
    profiles: [
      {
        id: 'profile-default',
        name: 'Default Workspace Profile',
        assignments: [
          {
            role: 'orchestrator',
            difficulty: null,
            choice: { model_id: 'model-gpt-5-4', effort: 'xhigh' },
          },
          {
            role: 'coder',
            difficulty: 'hard',
            choice: { model_id: 'model-argon-high', effort: 'high' },
          },
          {
            role: 'coder',
            difficulty: 'easy',
            choice: { model_id: 'model-deepseek-flash', effort: 'max' },
          },
        ],
        decision_advisor: null,
      },
    ],
    default_profile_id: 'profile-default',
  };
}

export function buildDefaultToolVersionsFixture(): DesktopToolVersionsSnapshot {
  return {
    registry_revision: 3,
    tools: [
      {
        tool_id: 'tool-codex',
        availability: 'found',
        selection_mode: 'system',
        configured_path: null,
        resolved_path: '/run/current-system/sw/bin/codex',
        system_path: '/run/current-system/sw/bin/codex',
        version: '0.45.0',
        system_version: '0.45.0',
        system_path_differs: false,
        message: 'Resolved from isolated test PATH.',
        error_code: null,
      },
      {
        tool_id: 'tool-agy',
        availability: 'found',
        selection_mode: 'system',
        configured_path: null,
        resolved_path: '/run/current-system/sw/bin/agy',
        system_path: '/run/current-system/sw/bin/agy',
        version: '2.4.0',
        system_version: '2.4.0',
        system_path_differs: false,
        message: 'Resolved from isolated test PATH.',
        error_code: null,
      },
    ],
  };
}

export function buildDefaultToolStatusFixture(): ToolStatus[] {
  return [
    {
      id: 'codex',
      label: 'Codex',
      executable_path: '/run/current-system/sw/bin/codex',
      availability: 'found',
      note: 'Test-injected read-only status.',
    },
    {
      id: 'antigravity',
      label: 'Antigravity',
      executable_path: '/run/current-system/sw/bin/agy',
      availability: 'found',
      note: 'Test-injected read-only status.',
    },
  ];
}

export function buildDefaultCodexSetupFixture(): CodexSetupReport {
  return {
    state: 'unchanged',
    mode: 'check',
    config_path: null,
    registered: true,
    config_modified: false,
    restart_required: false,
    pairing_required: false,
    error_code: null,
    message: 'Startup Codex setup skipped in acceptance harness (--no-codex-startup-setup).',
    skill_setup: {
      state: 'unchanged',
      mode: 'check',
      skill_path: null,
      version: '0.1.0',
      installed: true,
      files_modified: false,
      error_code: null,
      message: 'Bundled skill check skipped in acceptance harness.',
    },
  };
}

export function buildDefaultDesktopBridgeFixture(): DesktopSnapshot {
  return {
    attachment: {
      mode: 'runtime',
      connection: 'connected',
      state_dir: '/tmp/threadlane-isolated-acceptance-state',
      message: null,
      updated_at_ms: FIXTURE_BASE_NOW_MS,
    },
    bindings: [
      {
        binding_id: CHAT_ALPHA_IDENTITY.binding_id,
        thread_uuid: CHAT_ALPHA_IDENTITY.thread_uuid,
        label: 'Workspace UI Contract',
        is_local_demo: false,
      },
      {
        binding_id: CHAT_BETA_SAME_PROJECT_IDENTITY.binding_id,
        thread_uuid: CHAT_BETA_SAME_PROJECT_IDENTITY.thread_uuid,
        label: 'Release Verification Gate',
        is_local_demo: false,
      },
    ],
    selected_binding_id: CHAT_ALPHA_IDENTITY.binding_id,
    tasks: [],
    total_runs: 0,
    active_runs: 0,
    has_more: false,
    demo_controls: {
      available: false,
      run_id: null,
      binding_id: null,
      can_answer: false,
      can_cancel: false,
      revision: null,
    },
  };
}

export interface RecordedTauriInvocation {
  cmd: string;
  args: Record<string, unknown>;
  atMs: number;
}

export interface InstallWorkspaceTauriOptions {
  workspaceSnapshot?: WorkspaceSnapshot;
  fixedNowMs?: number;
  windowLabel?: 'main' | 'tray';
  trayPreferences?: TrayPreferencesResponse;
  initialLocalStorage?: Record<string, string>;
}

interface BrowserHarnessWindow extends Window {
  __THREADLANE_TEST_HARNESS__?: {
    getInvocations: () => RecordedTauriInvocation[];
    clearInvocations: () => void;
    getWorkspaceSnapshot: () => WorkspaceSnapshot;
    setWorkspaceSnapshot: (nextSnapshot: WorkspaceSnapshot) => void;
    stallNextWorkspaceSnapshotRead: () => void;
    getStalledWorkspaceSnapshotCount: () => number;
    releaseStalledWorkspaceSnapshotReads: () => void;
    appendMessageItem: (item: WorkspaceMessageItem) => number;
    emitTauriEvent: (event: string, payload: unknown) => void;
  };
}

export interface WorkspaceTauriController {
  getInvocations: () => Promise<RecordedTauriInvocation[]>;
  clearInvocations: () => Promise<void>;
  getWorkspaceSnapshot: () => Promise<WorkspaceSnapshot | null>;
  setWorkspaceSnapshot: (nextSnapshot: WorkspaceSnapshot) => Promise<void>;
  stallNextWorkspaceSnapshotRead: () => Promise<void>;
  getStalledWorkspaceSnapshotCount: () => Promise<number>;
  releaseStalledWorkspaceSnapshotReads: () => Promise<void>;
  appendMessageItem: (item: WorkspaceMessageItem) => Promise<void>;
  emitTauriEvent: (event: string, payload: unknown) => Promise<void>;
  readArchivePreferencesRaw: () => Promise<string | null>;
}

/**
 * Injects a strictly test-scoped Tauri 2 IPC mock and deterministic browser `Date`
 * (`page.clock.setFixedTime`, keeping interval and animation-frame timers live) before
 * `index.html -> main.ts -> app.tsx` executes. Production source files contain zero
 * test fallbacks; without this init script, `hasTauriRuntime()` remains `false`.
 */
export async function installWorkspaceTauriFixture(
  page: Page,
  options: InstallWorkspaceTauriOptions = {},
): Promise<WorkspaceTauriController> {
  const initialWorkspace = options.workspaceSnapshot ?? buildInteractiveWorkspaceSnapshot();
  const initialNowMs = options.fixedNowMs ?? initialWorkspace.generated_at_ms;
  const windowLabel = options.windowLabel ?? 'main';
  const initialTrayPrefs: TrayPreferencesResponse = options.trayPreferences ?? {
    close_behavior: 'hide_to_tray',
    tray_available: true,
    message: null,
  };
  const initialStorage = options.initialLocalStorage ?? {};
  const registrySnapshot = buildDefaultRegistryFixture();
  const toolVersions = buildDefaultToolVersionsFixture();
  const toolStatus = buildDefaultToolStatusFixture();
  const codexSetup = buildDefaultCodexSetupFixture();
  const desktopBridge = buildDefaultDesktopBridgeFixture();

  // Fix browser wall-clock `Date` at the fixture timestamp before first navigation
  // while leaving `setInterval`, `setTimeout`, and `requestAnimationFrame` running normally.
  await page.clock.setFixedTime(initialNowMs);

  await page.addInitScript(
    (seed: {
      workspace: WorkspaceSnapshot;
      windowLabel: 'main' | 'tray';
      trayPrefs: TrayPreferencesResponse;
      initialStorage: Record<string, string>;
      registrySnapshot: RegistrySnapshot;
      toolVersions: DesktopToolVersionsSnapshot;
      toolStatus: ToolStatus[];
      codexSetup: CodexSetupReport;
      desktopBridge: DesktopSnapshot;
    }) => {
      const storageInitMarker = '__threadlane_test_storage_seeded__';
      try {
        if (!window.sessionStorage.getItem(storageInitMarker)) {
          for (const [key, val] of Object.entries(seed.initialStorage)) {
            window.localStorage.setItem(key, val);
          }
          window.sessionStorage.setItem(storageInitMarker, '1');
        }
      } catch {
        // Ignore storage errors in restricted frames.
      }

      let currentWorkspace: WorkspaceSnapshot = JSON.parse(JSON.stringify(seed.workspace));
      let currentTrayPrefs: TrayPreferencesResponse = { ...seed.trayPrefs };
      const invocations: RecordedTauriInvocation[] = [];
      let stallNextWorkspaceRead = false;
      const stalledWorkspaceReadResolvers: Array<() => void> = [];
      let nextCallbackId = 1;
      let nextEventListenerId = 1;
      const callbacks = new Map<number, (payload: unknown) => void>();
      const eventListeners = new Map<string, Map<number, number>>();

      const cloneJson = <T,>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

      const projectFilteredWorkspace = (
        rawRequest: unknown,
        includeLegacyTopLevel: boolean,
      ): unknown => {
        const snap = cloneJson(currentWorkspace);
        const req =
          rawRequest && typeof rawRequest === 'object' && !Array.isArray(rawRequest)
            ? (rawRequest as Record<string, unknown>)
            : null;
        const filterSourceId =
          typeof req?.source_id === 'string' && req.source_id.trim().length > 0
            ? req.source_id.trim()
            : null;
        const filterJobId =
          typeof req?.job_id === 'string' && req.job_id.trim().length > 0
            ? req.job_id.trim()
            : null;
        const limit =
          typeof req?.limit === 'number' && Number.isFinite(req.limit) && req.limit > 0
            ? Math.min(200, Math.trunc(req.limit))
            : 50;

        if (filterSourceId) {
          snap.observation.jobs = snap.observation.jobs.filter(
            (job) => job.source_id === filterSourceId,
          );
          snap.observation.requests = snap.observation.requests.filter(
            (item) => item.source_id === filterSourceId,
          );
        }
        if (filterJobId) {
          snap.observation.jobs = snap.observation.jobs.filter(
            (job) => job.id === filterJobId,
          );
          snap.observation.requests = snap.observation.requests.filter(
            (item) => item.job_id === filterJobId,
          );
        }
        if (snap.observation.requests.length > limit) {
          snap.observation.requests = snap.observation.requests.slice(0, limit);
          snap.observation.has_more = true;
          snap.observation.next_cursor = 'obscur1_test_page_2';
        }

        if (includeLegacyTopLevel) {
          return {
            ...snap.observation,
            ...snap,
          };
        }
        return snap;
      };

      const emitEvent = (eventName: string, payload: unknown): void => {
        const listenersForEvent = eventListeners.get(eventName);
        if (!listenersForEvent) {
          return;
        }
        for (const [listenerId, callbackId] of listenersForEvent.entries()) {
          const cb = callbacks.get(callbackId);
          if (cb) {
            cb({
              event: eventName,
              id: listenerId,
              payload: cloneJson(payload),
            });
          }
        }
      };

      const win = window as unknown as Record<string, unknown>;

      win.__TAURI_EVENT_PLUGIN_INTERNALS__ = {
        unregisterListener(event: string, eventId: number): void {
          const listenersForEvent = eventListeners.get(event);
          if (listenersForEvent) {
            listenersForEvent.delete(eventId);
          }
        },
      };

      win.__TAURI_INTERNALS__ = {
        metadata: {
          currentWindow: { label: seed.windowLabel },
          currentWebview: { windowLabel: seed.windowLabel, label: seed.windowLabel },
          windows: [{ label: 'main' }, { label: 'tray' }],
          webviews: [
            { windowLabel: 'main', label: 'main' },
            { windowLabel: 'tray', label: 'tray' },
          ],
        },
        transformCallback(callback?: (payload: unknown) => void, once = false): number {
          const id = nextCallbackId++;
          const wrapped = (payload: unknown) => {
            if (once) {
              callbacks.delete(id);
              delete win[`_${id}`];
            }
            callback?.(payload);
          };
          callbacks.set(id, wrapped);
          win[`_${id}`] = wrapped;
          return id;
        },
        unregisterCallback(id: number): void {
          callbacks.delete(id);
          delete win[`_${id}`];
        },
        convertFileSrc(filePath: string, protocol = 'asset'): string {
          return `http://${protocol}.localhost/${encodeURIComponent(filePath)}`;
        },
        async invoke(cmd: string, rawArgs: Record<string, unknown> = {}): Promise<unknown> {
          const safeArgs = cloneJson(rawArgs ?? {});
          invocations.push({
            cmd,
            args: safeArgs,
            atMs: Date.now(),
          });

          if (cmd === 'plugin:event|listen') {
            const eventName = String(safeArgs.event ?? '');
            const handlerId = Number(safeArgs.handler ?? 0);
            const listenerId = nextEventListenerId++;
            let mapForEvent = eventListeners.get(eventName);
            if (!mapForEvent) {
              mapForEvent = new Map<number, number>();
              eventListeners.set(eventName, mapForEvent);
            }
            mapForEvent.set(listenerId, handlerId);
            return listenerId;
          }

          if (cmd === 'plugin:event|unlisten') {
            const eventName = String(safeArgs.event ?? '');
            const eventId = Number(safeArgs.eventId ?? 0);
            eventListeners.get(eventName)?.delete(eventId);
            return null;
          }

          if (cmd === 'plugin:event|emit' || cmd === 'plugin:event|emit_to') {
            emitEvent(String(safeArgs.event ?? ''), safeArgs.payload);
            return null;
          }

          switch (cmd) {
            case 'desktop_workspace_snapshot': {
              if (stallNextWorkspaceRead) {
                stallNextWorkspaceRead = false;
                return new Promise<unknown>((resolve) => {
                  stalledWorkspaceReadResolvers.push(() => {
                    resolve(projectFilteredWorkspace(safeArgs.request, false));
                  });
                });
              }
              return projectFilteredWorkspace(safeArgs.request, false);
            }
            case 'desktop_observation_snapshot':
              return projectFilteredWorkspace(safeArgs.request, true);
            case 'desktop_observation_sources':
              return cloneJson(currentWorkspace.observation.sources);
            case 'desktop_registry_snapshot':
              return cloneJson(seed.registrySnapshot);
            case 'desktop_tool_versions':
              return cloneJson(seed.toolVersions);
            case 'desktop_tool_status':
              return cloneJson(seed.toolStatus);
            case 'desktop_codex_setup_status':
            case 'desktop_codex_setup_recheck':
              return cloneJson(seed.codexSetup);
            case 'desktop_snapshot':
              return cloneJson(seed.desktopBridge);
            case 'desktop_plan_approvals':
              return [];
            case 'desktop_plans_snapshot':
              return [];
            case 'desktop_tray_preferences':
              return cloneJson(currentTrayPrefs);
            case 'desktop_set_tray_preferences': {
              const req = safeArgs.request as { close_behavior?: TrayCloseBehavior } | undefined;
              if (
                !req ||
                (req.close_behavior !== 'hide_to_tray' && req.close_behavior !== 'quit')
              ) {
                throw { code: 'invalid_argument', message: 'Invalid close_behavior value.' };
              }
              currentTrayPrefs = {
                ...currentTrayPrefs,
                close_behavior: req.close_behavior,
              };
              emitEvent('threadlane:tray-preferences-changed', currentTrayPrefs);
              return cloneJson(currentTrayPrefs);
            }
            case 'desktop_tray_action':
            case 'desktop_window_action':
              return null;
            case 'desktop_open_codex_chat': {
              const topLevelKeys = Object.keys(safeArgs);
              if (topLevelKeys.length !== 1 || topLevelKeys[0] !== 'request') {
                throw {
                  code: 'invalid_chat_target',
                  message:
                    'desktop_open_codex_chat requires exact { request: { source_id, thread_uuid, job_id? } } payload.',
                };
              }
              const rawReq = safeArgs.request;
              if (!rawReq || typeof rawReq !== 'object' || Array.isArray(rawReq)) {
                throw {
                  code: 'invalid_chat_target',
                  message: 'desktop_open_codex_chat request must be a plain object.',
                };
              }
              const request = rawReq as Record<string, unknown>;
              const allowedKeys = new Set(['source_id', 'thread_uuid', 'job_id']);
              for (const key of Object.keys(request)) {
                if (!allowedKeys.has(key)) {
                  throw {
                    code: 'invalid_chat_target',
                    message: `Unsupported field in desktop_open_codex_chat request: ${key}`,
                  };
                }
              }
              const sourceId = request.source_id;
              const threadUuid = request.thread_uuid;
              const jobId = request.job_id;
              if (
                typeof sourceId !== 'string' ||
                sourceId.trim().length === 0 ||
                typeof threadUuid !== 'string' ||
                threadUuid.trim().length === 0 ||
                (jobId !== undefined && jobId !== null && typeof jobId !== 'string')
              ) {
                throw {
                  code: 'invalid_chat_target',
                  message: 'Missing verified source_id or thread_uuid for Codex chat navigation.',
                };
              }
              return null;
            }
            default:
              throw {
                code: 'unsupported_test_command',
                message: `Unhandled Tauri command in test fixture: ${cmd}`,
              };
          }
        },
      };

      (window as unknown as BrowserHarnessWindow).__THREADLANE_TEST_HARNESS__ = {
        getInvocations: () => cloneJson(invocations),
        clearInvocations: () => {
          invocations.length = 0;
        },
        getWorkspaceSnapshot: () => cloneJson(currentWorkspace),
        setWorkspaceSnapshot: (nextSnapshot: WorkspaceSnapshot) => {
          currentWorkspace = cloneJson(nextSnapshot);
        },
        stallNextWorkspaceSnapshotRead: () => {
          stallNextWorkspaceRead = true;
        },
        getStalledWorkspaceSnapshotCount: () => stalledWorkspaceReadResolvers.length,
        releaseStalledWorkspaceSnapshotReads: () => {
          stallNextWorkspaceRead = false;
          const pending = stalledWorkspaceReadResolvers.splice(
            0,
            stalledWorkspaceReadResolvers.length,
          );
          for (const release of pending) {
            release();
          }
        },
        appendMessageItem: (item: WorkspaceMessageItem) => {
          const next = cloneJson(currentWorkspace);
          next.generated_at_ms += 1_000;
          next.observation.generated_at_ms = next.generated_at_ms;
          next.messages.items = [...next.messages.items, item];
          currentWorkspace = next;
          return next.generated_at_ms;
        },
        emitTauriEvent: (event: string, payload: unknown) => {
          emitEvent(event, payload);
        },
      };
    },
    {
      workspace: initialWorkspace,
      windowLabel,
      trayPrefs: initialTrayPrefs,
      initialStorage,
      registrySnapshot,
      toolVersions,
      toolStatus,
      codexSetup,
      desktopBridge,
    },
  );

  return {
    getInvocations: () =>
      page.evaluate(
        () =>
          (window as unknown as BrowserHarnessWindow).__THREADLANE_TEST_HARNESS__?.getInvocations() ??
          [],
      ),
    clearInvocations: () =>
      page.evaluate(() => {
        (window as unknown as BrowserHarnessWindow).__THREADLANE_TEST_HARNESS__?.clearInvocations();
      }),
    getWorkspaceSnapshot: () =>
      page.evaluate(
        () =>
          (
            window as unknown as BrowserHarnessWindow
          ).__THREADLANE_TEST_HARNESS__?.getWorkspaceSnapshot() ?? null,
      ),
    setWorkspaceSnapshot: async (nextSnapshot: WorkspaceSnapshot) => {
      await page.clock.setFixedTime(nextSnapshot.generated_at_ms);
      await page.evaluate((snap) => {
        (
          window as unknown as BrowserHarnessWindow
        ).__THREADLANE_TEST_HARNESS__?.setWorkspaceSnapshot(snap);
      }, nextSnapshot);
    },
    stallNextWorkspaceSnapshotRead: () =>
      page.evaluate(() => {
        (
          window as unknown as BrowserHarnessWindow
        ).__THREADLANE_TEST_HARNESS__?.stallNextWorkspaceSnapshotRead();
      }),
    getStalledWorkspaceSnapshotCount: () =>
      page.evaluate(
        () =>
          (
            window as unknown as BrowserHarnessWindow
          ).__THREADLANE_TEST_HARNESS__?.getStalledWorkspaceSnapshotCount() ?? 0,
      ),
    releaseStalledWorkspaceSnapshotReads: () =>
      page.evaluate(() => {
        (
          window as unknown as BrowserHarnessWindow
        ).__THREADLANE_TEST_HARNESS__?.releaseStalledWorkspaceSnapshotReads();
      }),
    appendMessageItem: async (item: WorkspaceMessageItem) => {
      const nextGeneratedAtMs = await page.evaluate((msg) => {
        return (
          (
            window as unknown as BrowserHarnessWindow
          ).__THREADLANE_TEST_HARNESS__?.appendMessageItem(msg) ?? null
        );
      }, item);
      if (typeof nextGeneratedAtMs === 'number' && Number.isFinite(nextGeneratedAtMs)) {
        await page.clock.setFixedTime(nextGeneratedAtMs);
      }
    },
    emitTauriEvent: (event: string, payload: unknown) =>
      page.evaluate(
        ({ eventName, eventPayload }) => {
          (
            window as unknown as BrowserHarnessWindow
          ).__THREADLANE_TEST_HARNESS__?.emitTauriEvent(eventName, eventPayload);
        },
        { eventName: event, eventPayload: payload },
      ),
    readArchivePreferencesRaw: () =>
      page.evaluate(
        (storageKey) => window.localStorage.getItem(storageKey),
        WORKSPACE_CHAT_ARCHIVE_STORAGE_KEY,
      ),
  };
}
