import test from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, "ordinary-closure.js"), "utf8");
const SOURCE_SHA256 = crypto.createHash("sha256").update(source, "utf8").digest("hex");
const legacyReadBlock = fs.readFileSync(path.join(here, "legacy-read-wrapper.fixture.txt"), "utf8");
const createOrdinaryClosure = vm.runInNewContext(
  `${source}\n;createOrdinaryClosure`,
  Object.create(null),
  { filename: "ordinary-closure.js" }
);

const CONTROL = "bnd_0196ed00-0000-7000-8000-000000000001";
const SESSION = "sbnd_0196ed00-0000-7000-8000-000000000002";
const APPROVAL = "apr_0196ed00000070008000000000000003";
const TASK = "task-ordinary";
const ITEM = "blg_0196ed00-0000-7000-8000-000000000004";
const EXEC = "wbe_0196ed00-0000-7000-8000-000000000005";
const RUN = "run_0196ed00-0000-7000-8000-000000000006";
const WORKER = "wrk_0196ed00-0000-7000-8000-000000000007";
const PROFILE = "worker-profile";
const BASE = "a".repeat(40);
const SUBJECT = "b".repeat(64);
const TASK_DIGEST = "c".repeat(64);
const RESULT_DIGEST = "d".repeat(64);
const DELIVERY_DIGEST = "e".repeat(64);
const MODEL = "example/model";
const EFFORT = "high";
const WORKTREE = "/private/tasks/actual-worktree";
const WRITE_PATHS = ["src/lib.rs", "src/new.rs"];
const RAW_LOG = "RAW_PROVIDER_LOG_SHOULD_STAY_IN_EVIDENCE_ONLY";

const START_EXECUTION = {
  execution_ref: EXEC,
  binding_id: CONTROL,
  item_id: ITEM,
  authority: {
    approval_id: APPROVAL,
    subject_digest: SUBJECT,
    task_id: TASK,
    task_digest: TASK_DIGEST
  },
  acceptance_condition_digest: "1".repeat(64),
  delivery_description_digest: "2".repeat(64),
  reservation_revision: 5,
  reserved_at_ms: 1000,
  dispatch_receipt: {
    session_binding_id: SESSION,
    run_id: RUN,
    worker_id: WORKER,
    profile_id: PROFILE,
    execution_binding_digest: "3".repeat(64)
  },
  attached_at_ms: 1100,
  outcome: null,
  completion: null
};

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

// Values returned from the vm realm have foreign prototypes; normalize before
// strict deep comparison so assertions compare data, not realm identity.
function plain(value) {
  return JSON.parse(JSON.stringify(value));
}

function startEnvelope(execution = START_EXECUTION) {
  return {
    untrusted_data: true,
    binding_id: CONTROL,
    tool: "backlog_worker_start",
    data: { execution: clone(execution) }
  };
}

function makeEvent(seq, overrides = {}) {
  return {
    event_id: `evt_${String(seq).padStart(4, "0")}`,
    binding_id: SESSION,
    seq,
    run_id: RUN,
    revision: 1,
    step_seq: seq,
    kind: "worker_progress",
    important: false,
    payload: {
      untrusted_data: true,
      summary: "bounded progress",
      provider_log: RAW_LOG
    },
    created_at_ms: 2000 + seq,
    ...overrides
  };
}

function makePage({
  pageNumber,
  events,
  hasMore = false,
  ackedSeq = 0,
  batchId = `bat_page_${pageNumber}`,
  controlBinding = CONTROL,
  sessionBinding = SESSION,
  batchBinding = SESSION,
  fromSeq,
  toSeq,
  deliveredSeq
}) {
  const from = fromSeq ?? (events.length > 0 ? events[0].seq : ackedSeq);
  const to = toSeq ?? (events.length > 0 ? events[events.length - 1].seq : from);
  return {
    approval_id: APPROVAL,
    control_binding_id: controlBinding,
    session_binding_id: sessionBinding,
    batch: {
      batch_id: batchId,
      binding_id: batchBinding,
      events,
      from_seq: from,
      to_seq: to,
      delivered_seq: deliveredSeq ?? to,
      acked_seq: ackedSeq,
      has_more: hasMore
    }
  };
}

const PAGE_ONE = makePage({
  pageNumber: 1,
  events: [makeEvent(1), makeEvent(2)],
  hasMore: true,
  ackedSeq: 0
});
const PAGE_TWO = makePage({
  pageNumber: 2,
  events: [makeEvent(3)],
  hasMore: false,
  ackedSeq: 2
});
const EMPTY_PAGE = makePage({
  pageNumber: 0,
  events: [],
  hasMore: false,
  ackedSeq: 0,
  batchId: "bat_empty"
});

function makeWorkerEvidence(overrides = {}) {
  return {
    approval_id: APPROVAL,
    task_id: TASK,
    session_binding_id: SESSION,
    run_id: RUN,
    worker_id: WORKER,
    profile_id: PROFILE,
    provider: "example",
    requested_model: MODEL,
    requested_effort: EFFORT,
    observed_model: MODEL,
    observed_effort: EFFORT,
    worker_status: "completed",
    worker_revision: 4,
    attempt_count: 1,
    started_at_ms: 1200,
    completed_at_ms: 2500,
    duration_ms: 1300,
    exit_code: 0,
    timed_out: false,
    cancelled: false,
    cancel_requested_before_completion: false,
    missing_model_evidence: false,
    missing_effort_evidence: false,
    provider_error: false,
    incomplete_cleanup: false,
    rejected_by_diff_gate: false,
    violation_count: 0,
    stdout_truncated: false,
    stderr_truncated: false,
    task_worktree_path: WORKTREE,
    ...overrides
  };
}

function makeDelivery(overrides = {}) {
  return {
    approval_id: APPROVAL,
    subject_digest: SUBJECT,
    binding_id: CONTROL,
    thread_uuid: "0196ed00-0000-7000-8000-000000000008",
    session_binding_id: SESSION,
    task_id: TASK,
    task_digest: TASK_DIGEST,
    profile_id: PROFILE,
    profile_digest: "4".repeat(64),
    command_key: "command-key",
    run_id: RUN,
    worker_id: WORKER,
    worktree_id: "wt_0196ed00-0000-7000-8000-000000000009",
    base_commit: BASE,
    head_commit: BASE,
    delivery_kind: "patch",
    delivery_digest: DELIVERY_DIGEST,
    result_digest: RESULT_DIGEST,
    changed_paths_count: WRITE_PATHS.length,
    changed_path_set_digest: "5".repeat(64),
    content_bytes: 128,
    ...overrides
  };
}

function makeResultData(overrides = {}) {
  const execution = clone(START_EXECUTION);
  execution.outcome = {
    outcome: "success",
    note: "verified terminal worker success",
    result_digest: RESULT_DIGEST,
    recorded_at_ms: 3000
  };
  return {
    execution,
    host_status: "ready_for_review",
    delivery: makeDelivery(overrides.delivery),
    item_snapshot: {
      item_id: ITEM,
      revision: 7,
      status: "doing",
      current_execution_ref: EXEC,
      ...(overrides.itemSnapshot || {})
    },
    worker_evidence: makeWorkerEvidence(overrides.workerEvidence)
  };
}

function makeSecondExecution() {
  const execution = clone(START_EXECUTION);
  execution.execution_ref = "wbe_fresh_second";
  execution.item_id = "blg_fresh_second";
  execution.authority = {
    approval_id: "apr_0196ed00000070008000000000000004",
    subject_digest: "6".repeat(64),
    task_id: "task-fresh-second",
    task_digest: "7".repeat(64)
  };
  execution.acceptance_condition_digest = "8".repeat(64);
  execution.delivery_description_digest = "9".repeat(64);
  execution.reservation_revision = 6;
  execution.reserved_at_ms = 1200;
  execution.dispatch_receipt.execution_binding_digest = "a".repeat(64);
  return execution;
}

function makeResultForExecution(execution, resultDigest, deliveryDigest) {
  const data = makeResultData();
  data.execution = clone(execution);
  data.execution.outcome = {
    outcome: "success",
    note: "verified terminal worker success",
    result_digest: resultDigest,
    recorded_at_ms: 3000
  };
  data.delivery = {
    ...data.delivery,
    approval_id: execution.authority.approval_id,
    subject_digest: execution.authority.subject_digest,
    task_id: execution.authority.task_id,
    task_digest: execution.authority.task_digest,
    binding_id: execution.binding_id,
    session_binding_id: execution.dispatch_receipt.session_binding_id,
    run_id: execution.dispatch_receipt.run_id,
    worker_id: execution.dispatch_receipt.worker_id,
    profile_id: execution.dispatch_receipt.profile_id,
    result_digest: resultDigest,
    delivery_digest: deliveryDigest
  };
  data.item_snapshot = {
    ...data.item_snapshot,
    item_id: execution.item_id,
    current_execution_ref: execution.execution_ref
  };
  data.worker_evidence = {
    ...data.worker_evidence,
    approval_id: execution.authority.approval_id,
    task_id: execution.authority.task_id,
    session_binding_id: execution.dispatch_receipt.session_binding_id,
    run_id: execution.dispatch_receipt.run_id,
    worker_id: execution.dispatch_receipt.worker_id,
    profile_id: execution.dispatch_receipt.profile_id
  };
  return data;
}

function makeContextForExecution(env, execution, commandKey) {
  const context = clone(env.approvedContext);
  context.start_args = {
    command_key: commandKey,
    item_id: execution.item_id,
    expected_revision: execution.reservation_revision - 1,
    approval_id: execution.authority.approval_id,
    task_id: execution.authority.task_id
  };
  return context;
}

function configureGuideAttempt(env, execution, resultData) {
  env.setNoticeApproval(execution.authority.approval_id);
  env.setStart(() => sdkStart(startEnvelope(execution)));
  env.tools.mcp__threadlane__approved_pull_events = async () => {
    env.calls.pull += 1;
    const page = clone(PAGE_TWO);
    page.approval_id = execution.authority.approval_id;
    return env.sdk("approved_pull_events", page);
  };
  env.tools.mcp__threadlane__approved_ack_events = async (args) => {
    env.calls.ack += 1;
    const ack = makeAckData(args.batch_id, 3, true);
    ack.approval_id = execution.authority.approval_id;
    return env.sdk("approved_ack_events", ack);
  };
  env.tools.mcp__threadlane__backlog_worker_result = async () => {
    env.calls.result += 1;
    return env.sdk("backlog_worker_result", resultData);
  };
}

function makeInspection(paths = WRITE_PATHS, overrides = {}) {
  return {
    schema_version: 1,
    tool: "threadlane inspect-delivery",
    inspection_only: true,
    authority_granted: false,
    ok: true,
    message: "physical delivery inspection completed",
    root: WORKTREE,
    expected_base: BASE,
    head_commit: BASE,
    base_is_ancestor: true,
    changed_paths: paths.map((filePath) => ({
      path: filePath,
      change_kinds: ["modified"],
      staged: false,
      unstaged: true,
      untracked: false,
      ignored: false,
      allowed: true
    })),
    files: paths.map((filePath, index) => ({
      path: filePath,
      changed: true,
      change_kinds: ["modified"],
      physical: "regular_file",
      regular_file: true,
      utf8: true,
      final_lf: true,
      size_bytes: 10 + index,
      sha256: String(index + 6).repeat(64),
      text_status: "included",
      text: `untrusted text for ${filePath}\n`,
      text_untrusted: true
    })),
    text_included: true,
    untrusted_data: true,
    output_omitted: false,
    ...overrides
  };
}

function envelope(tool, data) {
  return {
    untrusted_data: true,
    binding_id: CONTROL,
    tool,
    data
  };
}

function makeAckData(batchId, ackedSeq, advanced) {
  return {
    approval_id: APPROVAL,
    control_binding_id: CONTROL,
    session_binding_id: SESSION,
    ack: {
      binding_id: SESSION,
      batch_id: batchId,
      acked_seq: ackedSeq,
      advanced
    }
  };
}

function makeHarness(options = {}) {
  const pages = options.pages ?? [PAGE_ONE, PAGE_TWO];
  const calls = { pull: [], ack: [], result: [], inspect: [] };
  let pullIndex = 0;
  let lastAckedSeq = 0;
  const advance = options.advance ?? (() => {});

  const callbacks = {
    pull: async (args) => {
      calls.pull.push(args);
      if (options.pullImpl) {
        return options.pullImpl(args, pullIndex, calls);
      }
      if (pullIndex >= pages.length) {
        throw new Error("unexpected extra pull");
      }
      const page = pages[pullIndex];
      pullIndex += 1;
      advance("pull");
      return envelope("approved_pull_events", page);
    },
    ack: async (args) => {
      calls.ack.push(args);
      if (options.ackImpl) {
        return options.ackImpl(args, lastAckedSeq, calls);
      }
      const page = pages.find((candidate) => candidate.batch.batch_id === args.batch_id);
      const toSeq = page === undefined ? 0 : page.batch.to_seq;
      const advanced = toSeq > lastAckedSeq;
      lastAckedSeq = toSeq;
      advance("ack");
      return envelope("approved_ack_events", makeAckData(args.batch_id, toSeq, advanced));
    },
    result: async (args) => {
      calls.result.push(args);
      if (options.resultImpl) {
        return options.resultImpl(args, calls);
      }
      advance("result");
      return envelope("backlog_worker_result", options.resultData ?? makeResultData());
    },
    inspect: async (args) => {
      calls.inspect.push(args);
      if (options.inspectImpl) {
        return options.inspectImpl(args, calls);
      }
      advance("inspect");
      return options.inspection ?? makeInspection(args.allowed_paths);
    },
    now: options.now ?? (() => 1000)
  };

  return {
    calls,
    callbacks,
    closure: createOrdinaryClosure(callbacks)
  };
}

const SCOPE = {
  base_commit: BASE,
  requested_model: MODEL,
  requested_effort: EFFORT,
  write_paths: WRITE_PATHS,
  inspector: {
    executable: "/trusted/bin/threadlane",
    argv: ["inspect-delivery"]
  }
};

const BUDGETS = {
  max_pages: 4,
  max_calls: 12,
  deadline_ms: 60000
};

function paddedBatchPage(padText) {
  return makePage({
    pageNumber: 1,
    events: [
      makeEvent(1, {
        payload: { untrusted_data: true, pad: padText }
      })
    ],
    hasMore: false,
    ackedSeq: 0
  });
}

function exactBatchPage(targetBytes) {
  const page = paddedBatchPage("");
  const base = Buffer.byteLength(JSON.stringify(page.batch), "utf8");
  assert.ok(targetBytes >= base, `target ${targetBytes} is below base ${base}`);
  page.batch.events[0].payload.pad = "x".repeat(targetBytes - base);
  const bytes = Buffer.byteLength(JSON.stringify(page.batch), "utf8");
  assert.equal(bytes, targetBytes, "ASCII padding reaches the exact requested byte count");
  return { page, bytes };
}

function readInput(overrides = {}) {
  return {
    start: startEnvelope(),
    scope: SCOPE,
    budgets: BUDGETS,
    ...overrides
  };
}

async function readResult(harness, overrides = {}) {
  return harness.closure.read(readInput(overrides));
}

async function expectIncomplete(harness, reason, overrides = {}) {
  const { report, evidence } = await readResult(harness, overrides);
  assert.equal(report.status, "incomplete");
  assert.equal(report.reason, reason, JSON.stringify(report));
  assert.equal(report.authority_granted, false);
  assert.ok(evidence);
  return { report, evidence };
}

test("successful two-page flow enforces control/dispatch split and keeps evidence separate", async () => {
  const resultEnvelope = envelope("backlog_worker_result", makeResultData());
  const harness = makeHarness({ resultImpl: async () => resultEnvelope });
  const { report, evidence } = await readResult(harness);

  assert.equal(report.status, "ready_for_semantic_review");
  assert.equal(report.authority_granted, false);
  assert.equal(report.mapping.control_binding_id, CONTROL);
  assert.equal(report.mapping.session_binding_id, SESSION);
  assert.notEqual(report.mapping.control_binding_id, report.mapping.session_binding_id);
  assert.equal(report.result.current_revision, 7);
  assert.equal(report.result.result_digest, RESULT_DIGEST);
  assert.equal(report.result.delivery_digest, DELIVERY_DIGEST);
  assert.equal(report.batches.length, 2);
  assert.equal(report.events.length, 3);
  assert.deepEqual(
    plain(report.events.map((event) => event.seq)),
    [1, 2, 3]
  );
  assert.equal(report.events[0].batch_id, "bat_page_1");
  assert.equal(report.events[2].batch_id, "bat_page_2");
  assert.equal(report.events[2].acked_seq, 3);
  assert.equal(report.physical.head_commit, BASE);
  assert.equal(report.physical.files.length, WRITE_PATHS.length);
  assert.equal(report.physical.files[0].text_untrusted, true);

  assert.equal(harness.calls.pull.length, 2);
  assert.deepEqual(plain(harness.calls.pull[1]), {
    approval_id: APPROVAL,
    limit: 20,
    after_seq: 2
  });
  assert.equal(harness.calls.ack.length, 2);
  assert.deepEqual(plain(harness.calls.ack[0]), {
    approval_id: APPROVAL,
    batch_id: "bat_page_1"
  });
  assert.equal(harness.calls.result.length, 1);
  assert.deepEqual(plain(harness.calls.result[0]), {
    item_id: ITEM,
    execution_ref: EXEC
  });
  assert.equal(harness.calls.inspect.length, 1);
  assert.equal(harness.calls.inspect[0].root, WORKTREE);
  assert.deepEqual(plain(harness.calls.inspect[0].allowed_paths), WRITE_PATHS);
  assert.deepEqual(plain(harness.calls.inspect[0].text_files), WRITE_PATHS);

  assert.equal(evidence.pages.length, 2);
  assert.equal(evidence.acks.length, 2);
  assert.equal(evidence.result, resultEnvelope, "the exact original result ToolEnvelope is retained");
  assert.equal(evidence.result.tool, "backlog_worker_result");
  assert.equal(evidence.result.binding_id, CONTROL);
  assert.equal(evidence.result.data.execution.outcome.outcome, "success");
  assert.ok(JSON.stringify(evidence).includes(RAW_LOG));
  assert.ok(!JSON.stringify(report).includes(RAW_LOG));
  assert.equal("completion" in report, false);
  assert.ok(!JSON.stringify(report).includes("full_receipt"));
});

test("start validates and retains immutable reservation fields before any callback", async () => {
  const invalidStarts = [
    ["missing acceptance digest", (execution) => delete execution.acceptance_condition_digest],
    ["uppercase acceptance digest", (execution) => {
      execution.acceptance_condition_digest = "A".repeat(64);
    }],
    ["missing delivery description digest", (execution) => {
      delete execution.delivery_description_digest;
    }],
    ["missing reserved_at_ms", (execution) => delete execution.reserved_at_ms],
    ["negative reserved_at_ms", (execution) => {
      execution.reserved_at_ms = -1;
    }],
    ["zero reservation_revision", (execution) => {
      execution.reservation_revision = 0;
    }]
  ];
  for (const [label, mutate] of invalidStarts) {
    const start = startEnvelope();
    mutate(start.data.execution);
    const harness = makeHarness();
    const { report, evidence } = await expectIncomplete(harness, "missing_field", { start });
    assert.equal(report.authority_granted, false, label);
    assert.equal(evidence.start, start, `${label}: the exact start ToolEnvelope is retained`);
    assert.equal(harness.calls.pull.length, 0, label);
    assert.equal(harness.calls.ack.length, 0, label);
  }
});

test("result execution must preserve all four immutable reservation fields before inspect", async () => {
  const mismatches = [
    ["acceptance_condition_digest", (execution) => {
      execution.acceptance_condition_digest = "f".repeat(64);
    }],
    ["delivery_description_digest", (execution) => {
      execution.delivery_description_digest = "f".repeat(64);
    }],
    ["reservation_revision", (execution) => {
      execution.reservation_revision += 1;
    }],
    ["reserved_at_ms", (execution) => {
      execution.reserved_at_ms += 1;
    }]
  ];
  for (const [field, mutate] of mismatches) {
    const wrong = makeResultData();
    mutate(wrong.execution);
    const resultEnvelope = envelope("backlog_worker_result", wrong);
    const harness = makeHarness({ resultImpl: async () => resultEnvelope });
    const { report, evidence } = await expectIncomplete(harness, "immutable_execution_mismatch");
    assert.equal(report.authority_granted, false, field);
    assert.equal(evidence.result, resultEnvelope, `${field}: the exact result ToolEnvelope is retained`);
    assert.equal(harness.calls.inspect.length, 0, `${field}: no inspection follows a mismatch`);
    assert.equal(harness.calls.result.length, 1, field);
  }
});

test("optional dispatch execution_binding_digest is retained and compared exactly", async () => {
  {
    const wrong = makeResultData();
    delete wrong.execution.dispatch_receipt.execution_binding_digest;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "dispatch_mismatch");
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const wrong = makeResultData();
    wrong.execution.dispatch_receipt.execution_binding_digest = "f".repeat(64);
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "dispatch_mismatch");
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const start = startEnvelope();
    delete start.data.execution.dispatch_receipt.execution_binding_digest;
    const result = makeResultData();
    delete result.execution.dispatch_receipt.execution_binding_digest;
    const harness = makeHarness({ resultData: result });
    const { report } = await readResult(harness, { start });
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(report.result.execution_binding_digest, null);
  }
});

test("event step_seq must be a non-negative safe integer, with zero valid, before ACK", async () => {
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(1, { step_seq: 0 })],
      hasMore: false,
      ackedSeq: 0
    });
    const harness = makeHarness({ pages: [page] });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(report.events[0].step_seq, 0);
    assert.equal(harness.calls.ack.length, 1);
  }
  const invalidStepSeqs = [
    ["missing", undefined],
    ["string", "1"],
    ["object", { value: 1 }],
    ["negative", -1],
    ["unsafe", Number.MAX_SAFE_INTEGER + 1]
  ];
  for (const [label, stepSeq] of invalidStepSeqs) {
    const event = makeEvent(1);
    if (label === "missing") {
      delete event.step_seq;
    } else {
      event.step_seq = stepSeq;
    }
    const page = makePage({ pageNumber: 1, events: [event], hasMore: false, ackedSeq: 0 });
    const harness = makeHarness({ pages: [page] });
    const { report, evidence } = await expectIncomplete(harness, "missing_field");
    assert.equal(harness.calls.ack.length, 0, `${label}: no ACK before step_seq validation`);
    assert.equal(harness.calls.result.length, 0, label);
    assert.equal(harness.calls.inspect.length, 0, label);
    assert.equal(evidence.pages.length, 1, `${label}: the actual raw page is retained`);
    assert.equal(report.pending_batch, null, `${label}: no pending ACK state was manufactured`);
  }
});

test("inner EventBatch UTF-8 bytes are checked at exactly 49152 before ACK", async () => {
  {
    const { page, bytes } = exactBatchPage(49152);
    assert.equal(bytes, 49152);
    const harness = makeHarness({ pages: [page] });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(harness.calls.ack.length, 1, "the exact boundary is allowed");
  }
  {
    const { page, bytes } = exactBatchPage(49153);
    assert.equal(bytes, 49153);
    const harness = makeHarness({ pages: [page] });
    const { report, evidence } = await expectIncomplete(harness, "batch_too_large");
    assert.equal(harness.calls.ack.length, 0, "oversize inner batch is rejected before ACK");
    assert.equal(harness.calls.result.length, 0);
    assert.equal(harness.calls.inspect.length, 0);
    assert.equal(evidence.pages[0].data.batch.batch_id, page.batch.batch_id);
    assert.equal(report.pending_batch.batch_id, page.batch.batch_id);
    assert.equal(report.cursor, page.batch.to_seq);
  }
});

test("UTF-8 byte counting counts escaped and multibyte text, not UTF-16 length", async () => {
  {
    const page = paddedBatchPage("é".repeat(30000));
    const serialized = JSON.stringify(page.batch);
    assert.ok(serialized.length < 49152, "UTF-16 length alone looks legal");
    assert.ok(Buffer.byteLength(serialized, "utf8") > 49152, "real UTF-8 bytes exceed the limit");
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "batch_too_large");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const page = paddedBatchPage("\n".repeat(30000));
    const serialized = JSON.stringify(page.batch);
    assert.ok(Buffer.byteLength(serialized, "utf8") > 49152);
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "batch_too_large");
    assert.equal(harness.calls.ack.length, 0);
  }
});

test("a legal inner batch remains legal when the duplicate wrapper is larger than 49152 bytes", async () => {
  const page = paddedBatchPage("x".repeat(30000));
  const innerBytes = Buffer.byteLength(JSON.stringify(page.batch), "utf8");
  assert.ok(innerBytes < 49152);
  const structured = envelope("approved_pull_events", page);
  const wrapper = {
    structured_content: structured,
    content: [{ type: "text", text: JSON.stringify(structured) }]
  };
  assert.ok(Buffer.byteLength(JSON.stringify(wrapper), "utf8") > 49152);
  const harness = makeHarness({
    pages: [page],
    pullImpl: async () => wrapper
  });
  const { report } = await readResult(harness);
  assert.equal(report.status, "ready_for_semantic_review");
  assert.equal(harness.calls.ack.length, 1);
});

test("empty page is acknowledged once and stops pagination", async () => {
  const harness = makeHarness({ pages: [EMPTY_PAGE] });
  const { report } = await readResult(harness);

  assert.equal(report.status, "ready_for_semantic_review");
  assert.equal(report.events.length, 0);
  assert.equal(report.batches.length, 1);
  assert.equal(report.batches[0].batch_id, "bat_empty");
  assert.equal(report.batches[0].advanced, false);
  assert.equal(report.read.pages, 1);
  assert.equal(harness.calls.pull.length, 1);
  assert.equal(harness.calls.ack.length, 1);
  assert.equal(harness.calls.ack[0].batch_id, "bat_empty");
  assert.equal(harness.calls.result.length, 1);
  assert.equal(harness.calls.inspect.length, 1);
});

test("wrong control, session, batch, run, execution, task, and item identities fail closed", async () => {
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(1)],
      controlBinding: "bnd_wrong"
    });
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "control_binding_mismatch");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(1)],
      batchBinding: "sbnd_wrong"
    });
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "session_binding_mismatch");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(1, { run_id: "run_wrong" })]
    });
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "event_run_mismatch");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const wrong = makeResultData();
    wrong.execution.execution_ref = "wbe_wrong";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "execution_ref_mismatch");
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const wrong = makeResultData();
    wrong.worker_evidence.task_id = "task_wrong";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "authority_mismatch");
  }
  {
    const wrong = makeResultData();
    wrong.item_snapshot.current_execution_ref = "wbe_wrong";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "stale_item_execution_ref");
  }
  {
    const wrong = makeResultData();
    wrong.delivery.base_commit = "f".repeat(40);
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "base_mismatch");
  }
});

test("missing, stale, failed, Unknown, and unsafe worker evidence fails closed", async () => {
  {
    const wrong = makeResultData();
    delete wrong.item_snapshot;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "missing_item_snapshot");
  }
  {
    const wrong = makeResultData();
    delete wrong.worker_evidence;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "missing_worker_evidence");
  }
  {
    const wrong = makeResultData();
    delete wrong.delivery;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "missing_delivery");
  }
  {
    const wrong = makeResultData();
    delete wrong.execution.outcome;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "missing_outcome");
  }
  {
    const wrong = makeResultData();
    wrong.execution.outcome.outcome = "failure";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "outcome_failure");
  }
  {
    const wrong = makeResultData();
    wrong.execution.outcome.outcome = "unknown";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "outcome_unknown");
  }
  {
    const wrong = makeResultData();
    wrong.worker_evidence.worker_status = "running";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "worker_not_completed");
  }
  {
    const wrong = makeResultData();
    wrong.item_snapshot.status = "done";
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "item_not_current");
  }
  {
    const wrong = makeResultData();
    wrong.item_snapshot.revision = 4;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "stale_item_revision");
  }
  {
    const wrong = makeResultData();
    wrong.worker_evidence.exit_code = 1;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "exit_nonzero");
  }
  {
    const wrong = makeResultData();
    delete wrong.worker_evidence.stdout_truncated;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "unsafe_flag");
  }
  {
    const wrong = makeResultData();
    wrong.worker_evidence.provider_error = true;
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "unsafe_flag");
  }
  {
    const wrong = makeResultData();
    wrong.execution.completion = { completed_at_ms: 3000 };
    const harness = makeHarness({ resultData: wrong });
    await expectIncomplete(harness, "already_completed");
  }
});

test("bad acknowledgements and event order stop before later effects", async () => {
  {
    const harness = makeHarness({
      ackImpl: async (args) =>
        envelope("approved_ack_events", makeAckData("bat_wrong", 2, true))
    });
    await expectIncomplete(harness, "ack_batch_mismatch");
    assert.equal(harness.calls.result.length, 0);
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const harness = makeHarness({
      ackImpl: async (args) =>
        envelope("approved_ack_events", makeAckData(args.batch_id, 1, true))
    });
    await expectIncomplete(harness, "ack_cursor_mismatch");
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const harness = makeHarness({
      ackImpl: async (args) =>
        envelope("approved_ack_events", makeAckData(args.batch_id, 2, false))
    });
    await expectIncomplete(harness, "ack_not_advanced");
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(2), makeEvent(1)],
      fromSeq: 1,
      toSeq: 2
    });
    const harness = makeHarness({ pages: [page] });
    await expectIncomplete(harness, "event_order_invalid");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const page = makePage({
      pageNumber: 1,
      events: [makeEvent(1)],
      hasMore: true,
      batchId: "bat_page_1"
    });
    const duplicatePage = makePage({
      pageNumber: 2,
      events: [makeEvent(2)],
      hasMore: false,
      ackedSeq: 1,
      batchId: "bat_page_1"
    });
    const harness = makeHarness({ pages: [page, duplicatePage] });
    await expectIncomplete(harness, "duplicate_batch");
    assert.equal(harness.calls.result.length, 0);
  }
});

test("cross-page continuity rejects skipped, repeated, and duplicate event ranges before ACK", async () => {
  {
    const skipped = makePage({
      pageNumber: 2,
      events: [makeEvent(4)],
      hasMore: false,
      ackedSeq: 2,
      fromSeq: 4,
      toSeq: 4
    });
    const harness = makeHarness({ pages: [PAGE_ONE, skipped] });
    const { report } = await expectIncomplete(harness, "sequence_continuity");
    assert.equal(harness.calls.pull.length, 2);
    assert.equal(harness.calls.ack.length, 1, "only the contiguous first page may be ACKed");
    assert.equal(harness.calls.result.length, 0);
    assert.equal(report.last_ack.batch_id, "bat_page_1");
    assert.equal(report.cursor, 2);
    assert.equal(report.pending_batch, null);
  }
  {
    const repeated = makePage({
      pageNumber: 2,
      events: [makeEvent(2)],
      hasMore: false,
      ackedSeq: 2,
      fromSeq: 2,
      toSeq: 2
    });
    const harness = makeHarness({ pages: [PAGE_ONE, repeated] });
    await expectIncomplete(harness, "sequence_continuity");
    assert.equal(harness.calls.ack.length, 1);
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const wrongAckCursor = makePage({
      pageNumber: 2,
      events: [makeEvent(3)],
      hasMore: false,
      ackedSeq: 1
    });
    const harness = makeHarness({ pages: [PAGE_ONE, wrongAckCursor] });
    await expectIncomplete(harness, "cursor_mismatch");
    assert.equal(harness.calls.ack.length, 1);
  }
  {
    const duplicateInPage = makePage({
      pageNumber: 1,
      events: [
        makeEvent(1, { event_id: "evt_repeated" }),
        makeEvent(2, { event_id: "evt_repeated" })
      ]
    });
    const harness = makeHarness({ pages: [duplicateInPage] });
    await expectIncomplete(harness, "duplicate_event_id");
    assert.equal(harness.calls.ack.length, 0);
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const first = makePage({
      pageNumber: 1,
      events: [makeEvent(1, { event_id: "evt_repeated" })],
      hasMore: true,
      ackedSeq: 0
    });
    const second = makePage({
      pageNumber: 2,
      events: [makeEvent(2, { event_id: "evt_repeated" })],
      hasMore: false,
      ackedSeq: 1
    });
    const harness = makeHarness({ pages: [first, second] });
    await expectIncomplete(harness, "duplicate_event_id");
    assert.equal(harness.calls.ack.length, 1);
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const emptyAtCursor = makePage({
      pageNumber: 2,
      events: [],
      hasMore: false,
      ackedSeq: 2,
      batchId: "bat_empty_cursor"
    });
    const harness = makeHarness({ pages: [PAGE_ONE, emptyAtCursor] });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(report.events.length, 2);
    assert.equal(report.batches.length, 2);
    assert.equal(report.batches[1].batch_id, "bat_empty_cursor");
    assert.equal(report.batches[1].advanced, false);
    assert.equal(harness.calls.ack.length, 2);
  }
});

test("malformed ACK or callback failure retains exact receipts, mapping, pending batch, and last ACK", async () => {
  {
    const malformed = envelope("approved_ack_events", makeAckData("bat_wrong", 2, true));
    const harness = makeHarness({
      ackImpl: async () => malformed
    });
    const { report, evidence } = await expectIncomplete(harness, "ack_batch_mismatch");
    assert.equal(evidence.pages.length, 1);
    assert.equal(evidence.acks.length, 1);
    assert.equal(evidence.acks[0], malformed, "the exact malformed ACK DTO is retained");
    assert.equal(report.mapping.approval_id, APPROVAL);
    assert.equal(report.mapping.execution_ref, EXEC);
    assert.equal(report.pending_batch.batch_id, "bat_page_1");
    assert.equal(report.cursor, 2);
    assert.equal(report.last_ack, null);
    assert.equal(harness.calls.result.length, 0);
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const harness = makeHarness({
      ackImpl: async () => {
        throw new Error("transport lost after possible effect");
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "callback_failed");
    assert.equal(evidence.pages.length, 1);
    assert.equal(evidence.acks.length, 0);
    assert.equal(report.pending_batch.batch_id, "bat_page_1");
    assert.equal(report.cursor, 2);
    assert.equal(report.last_ack, null);
    assert.equal(harness.calls.pull.length, 1, "a failed callback is not retried");
    assert.equal(harness.calls.result.length, 0);
  }
  {
    const harness = makeHarness({
      resultImpl: async () => {
        throw new Error("result transport failed");
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "callback_failed");
    assert.equal(evidence.pages.length, 2);
    assert.equal(evidence.acks.length, 2);
    assert.equal(evidence.result, null);
    assert.equal(report.pending_batch, null);
    assert.equal(report.last_ack.batch_id, "bat_page_2");
    assert.equal(report.cursor, 3);
    assert.equal(harness.calls.inspect.length, 0);
  }
});

test("finite page/call/deadline budgets stop without starting later effects", async () => {
  {
    const harness = makeHarness();
    await expectIncomplete(harness, "invalid_budget", {
      budgets: { max_pages: 0, max_calls: 10, deadline_ms: 1000 }
    });
    assert.equal(harness.calls.pull.length, 0);
  }
  {
    const harness = makeHarness();
    await expectIncomplete(harness, "invalid_budget", {
      budgets: { max_pages: 1, max_calls: Infinity, deadline_ms: 1000 }
    });
    assert.equal(harness.calls.pull.length, 0);
  }
  {
    const harness = makeHarness();
    await expectIncomplete(harness, "page_budget_exhausted", {
      budgets: { max_pages: 1, max_calls: 10, deadline_ms: 1000 }
    });
    assert.equal(harness.calls.pull.length, 1);
    assert.equal(harness.calls.ack.length, 1);
    assert.equal(harness.calls.result.length, 0);
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const harness = makeHarness();
    const { report } = await expectIncomplete(harness, "call_budget_exhausted", {
      budgets: { max_pages: 4, max_calls: 4, deadline_ms: 1000 }
    });
    assert.equal(harness.calls.pull.length, 2);
    assert.equal(harness.calls.ack.length, 2);
    assert.equal(harness.calls.result.length, 0);
    assert.equal(report.last_ack.batch_id, "bat_page_2");
  }
  {
    let elapsed = 0;
    const harness = makeHarness({
      now: () => 1000 + elapsed,
      advance: (stage) => {
        if (stage === "pull") {
          elapsed += 6000;
        }
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "deadline_exceeded", {
      budgets: { max_pages: 4, max_calls: 10, deadline_ms: 5000 }
    });
    assert.equal(harness.calls.pull.length, 1);
    assert.equal(harness.calls.ack.length, 0);
    assert.equal(harness.calls.result.length, 0);
    assert.equal(evidence.pages.length, 1, "late pull receipt stays in private evidence");
    assert.equal(report.pending_batch, null);
    assert.equal(report.cursor, null);
    assert.equal(report.last_ack, null);
  }
  {
    let elapsed = 0;
    const harness = makeHarness({
      now: () => 1000 + elapsed,
      advance: (stage) => {
        if (stage === "ack") {
          elapsed += 6000;
        }
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "deadline_exceeded", {
      budgets: { max_pages: 4, max_calls: 10, deadline_ms: 5000 }
    });
    assert.equal(harness.calls.pull.length, 1);
    assert.equal(harness.calls.ack.length, 1);
    assert.equal(harness.calls.result.length, 0, "no new effect after an expired ACK receipt");
    assert.equal(evidence.acks.length, 1, "late ACK receipt stays in private evidence");
    assert.equal(report.pending_batch.batch_id, "bat_page_1");
    assert.equal(report.cursor, 2);
    assert.equal(report.last_ack, null);
  }
  {
    let elapsed = 0;
    const harness = makeHarness({
      now: () => 1000 + elapsed,
      advance: (stage) => {
        if (stage === "result") {
          elapsed += 6000;
        }
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "deadline_exceeded", {
      budgets: { max_pages: 4, max_calls: 10, deadline_ms: 5000 }
    });
    assert.equal(harness.calls.pull.length, 2);
    assert.equal(harness.calls.ack.length, 2);
    assert.equal(harness.calls.result.length, 1);
    assert.equal(harness.calls.inspect.length, 0, "no inspection after an expired result receipt");
    assert.ok(evidence.result, "late result receipt stays in private evidence");
    assert.equal(report.last_ack.batch_id, "bat_page_2");
  }
  {
    let elapsed = 0;
    const harness = makeHarness({
      now: () => 1000 + elapsed,
      advance: (stage) => {
        if (stage === "inspect") {
          elapsed += 6000;
        }
      }
    });
    const { report, evidence } = await expectIncomplete(harness, "deadline_exceeded", {
      budgets: { max_pages: 4, max_calls: 10, deadline_ms: 5000 }
    });
    assert.equal(harness.calls.pull.length, 2);
    assert.equal(harness.calls.ack.length, 2);
    assert.equal(harness.calls.result.length, 1);
    assert.equal(harness.calls.inspect.length, 1);
    assert.ok(evidence.inspect, "late inspection receipt stays in private evidence");
    assert.equal(report.status, "incomplete");
    assert.equal(report.last_ack.batch_id, "bat_page_2");
  }
  {
    const harness = makeHarness({ now: () => Number.NaN });
    await expectIncomplete(harness, "invalid_clock", {
      budgets: { max_pages: 4, max_calls: 10, deadline_ms: 1000 }
    });
    assert.equal(harness.calls.pull.length, 0);
  }
});

test("false has_more ends pagination without polling", async () => {
  const harness = makeHarness({ pages: [PAGE_TWO] });
  const { report } = await readResult(harness);
  assert.equal(report.status, "ready_for_semantic_review");
  assert.equal(harness.calls.pull.length, 1);
  assert.equal(harness.calls.ack.length, 1);
});

test("running or refused physical inspection stops once without retry", async () => {
  {
    const harness = makeHarness({
      inspectImpl: async () => ({ status: "running" })
    });
    const { report } = await expectIncomplete(harness, "physical_inspection_running");
    assert.equal(harness.calls.inspect.length, 1);
    assert.equal(harness.calls.result.length, 1);
    assert.equal(report.last_ack.batch_id, "bat_page_2");
  }
  {
    const harness = makeHarness({
      inspection: { ok: false, refusal_code: "wrong_head" }
    });
    const { report } = await expectIncomplete(harness, "physical_inspection_refused");
    assert.equal(harness.calls.inspect.length, 1);
    assert.equal(report.cursor, 3);
  }
});

test("read exposes no start, acceptance, or completion capability", () => {
  const harness = makeHarness();
  assert.deepEqual(Object.keys(harness.closure).sort(), ["doneView", "read"]);
  assert.equal(harness.closure.start, undefined);
  assert.equal(harness.closure.complete, undefined);
  assert.equal(harness.closure.reject, undefined);
  assert.equal(harness.closure.accept, undefined);
  // `doneView` may name the completion tool to validate a receipt, but the
  // template must not call any start, completion, or rejection operation.
  for (const forbidden of [
    "backlog_worker_complete(",
    "backlog_worker_reject(",
    "start_approved_task",
    "backlog_worker_start("
  ]) {
    assert.ok(!source.includes(forbidden), `template must not contain ${forbidden}`);
  }
  for (const forbidden of [
    "require(",
    "fetch(",
    "process.",
    "new Function(",
    "eval(",
    "import ",
    "TextEncoder",
    "XMLHttpRequest",
    "WebSocket",
    "node:"
  ]) {
    assert.ok(!source.includes(forbidden), `template must not contain ${forbidden}`);
  }
});

test("hostile CLI and data arguments are carried as exact argv values and shell-quoted only for display", async () => {
  const hostilePaths = [
    "src/a b'$(touch owned);\"quoted\"&|.rs",
    "src/back`tick`;*.rs"
  ];
  const scope = {
    ...SCOPE,
    write_paths: hostilePaths
  };
  const harness = makeHarness({
    inspection: makeInspection(hostilePaths)
  });
  const { report } = await readResult(harness, { scope });

  assert.equal(report.status, "ready_for_semantic_review");
  const request = harness.calls.inspect[0];
  const expectedArgv = [
    "/trusted/bin/threadlane",
    "inspect-delivery",
    "--root",
    WORKTREE,
    "--expected-base",
    BASE,
    ...hostilePaths.flatMap((filePath) => ["--allowed-path", filePath]),
    ...hostilePaths.flatMap((filePath) => ["--text-file", filePath])
  ];
  assert.deepEqual(plain(request.argv), expectedArgv);
  assert.deepEqual(plain(request.allowed_paths), hostilePaths);
  assert.deepEqual(plain(request.text_files), hostilePaths);
  assert.deepEqual(parseDisplayCommand(request.command), expectedArgv);
  assert.equal(request.command, expectedArgv.map(shellQuote).join(" "));
  assert.ok(!request.command.includes("eval("));
  assert.ok(!source.includes("child_process"));
  assert.ok(!source.includes("exec("));
});

function shellQuote(value) {
  return `'${String(value).replace(/'/g, "'\\''")}'`;
}

// Parse the template's display-only POSIX single-quoted command back to argv.
function parseDisplayCommand(command) {
  const args = [];
  let index = 0;
  while (index < command.length) {
    while (command[index] === " ") {
      index += 1;
    }
    if (index >= command.length) {
      break;
    }
    let argument = "";
    while (index < command.length && command[index] !== " ") {
      if (command[index] === "'") {
        index += 1;
        while (index < command.length && command[index] !== "'") {
          argument += command[index];
          index += 1;
        }
        if (command[index] !== "'") {
          throw new Error("unterminated display command quote");
        }
        index += 1;
      } else if (command[index] === "\\" && command[index + 1] === "'") {
        argument += "'";
        index += 2;
      } else {
        argument += command[index];
        index += 1;
      }
    }
    args.push(argument);
  }
  return args;
}

test("structured and text envelopes must not be missing or conflicting", async () => {
  {
    const page = clone(PAGE_TWO);
    const harness = makeHarness({
      pullImpl: async () => ({
        content: [{ type: "text", text: JSON.stringify(envelope("approved_pull_events", page)) }]
      })
    });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
  }
  {
    const page = clone(PAGE_TWO);
    const harness = makeHarness({
      pullImpl: async () => ({
        structured_content: envelope("approved_pull_events", page)
      })
    });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
  }
  {
    const page = clone(PAGE_TWO);
    const structured = envelope("approved_pull_events", page);
    const harness = makeHarness({
      pullImpl: async () => ({
        structured_content: structured,
        content: [{ type: "text", text: JSON.stringify(structured) }]
      })
    });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
  }
  {
    const page = clone(PAGE_TWO);
    const structured = envelope("approved_pull_events", page);
    const conflicting = clone(structured);
    conflicting.data.batch.has_more = true;
    const harness = makeHarness({
      pullImpl: async () => ({
        structured_content: structured,
        content: [{ type: "text", text: JSON.stringify(conflicting) }]
      })
    });
    await expectIncomplete(harness, "conflicting_envelope");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const harness = makeHarness({
      pullImpl: async () => ({})
    });
    await expectIncomplete(harness, "missing_envelope");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const harness = makeHarness({
      pullImpl: async () => ({ unexpected: true })
    });
    await expectIncomplete(harness, "missing_envelope");
    assert.equal(harness.calls.ack.length, 0);
  }
});

test("transport errors, envelope conflicts, and outer binding/tool mismatches fail closed", async () => {
  {
    const start = startEnvelope();
    start.isError = true;
    const harness = makeHarness();
    const { evidence } = await expectIncomplete(harness, "transport_error", { start });
    assert.equal(evidence.start, start, "the exact transport result is retained");
    assert.equal(harness.calls.pull.length, 0);
  }
  {
    const harness = makeHarness({
      pullImpl: async () => ({
        isError: true,
        structuredContent: envelope("approved_pull_events", clone(PAGE_TWO)),
        content: [{ type: "text", text: JSON.stringify(envelope("approved_pull_events", clone(PAGE_TWO))) }]
      })
    });
    await expectIncomplete(harness, "transport_error");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const harness = makeHarness({
      ackImpl: async (args) => ({
        isError: true,
        structuredContent: envelope("approved_ack_events", makeAckData(args.batch_id, 2, true))
      })
    });
    const { report, evidence } = await expectIncomplete(harness, "transport_error");
    assert.equal(evidence.acks.length, 1, "the exact error ACK result is retained");
    assert.equal(report.pending_batch.batch_id, "bat_page_1");
    assert.equal(report.last_ack, null);
  }
  {
    const page = clone(PAGE_TWO);
    const left = envelope("approved_pull_events", page);
    const right = envelope("approved_pull_events", clone(page));
    right.data.batch.batch_id = "bat_conflict";
    const harness = makeHarness({
      pullImpl: async () => ({
        structured_content: left,
        structuredContent: right,
        content: [{ type: "text", text: JSON.stringify(left) }]
      })
    });
    await expectIncomplete(harness, "conflicting_envelope");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const page = clone(PAGE_TWO);
    const harness = makeHarness({
      pullImpl: async () => ({
        content: [
          { type: "text", text: JSON.stringify(envelope("approved_pull_events", page)) },
          { type: "text", text: JSON.stringify(envelope("approved_pull_events", page)) }
        ]
      })
    });
    const { report } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
  }
  {
    const page = clone(PAGE_TWO);
    const conflicting = envelope("approved_pull_events", clone(page));
    conflicting.data.batch.batch_id = "bat_conflict";
    const harness = makeHarness({
      pullImpl: async () => ({
        content: [
          { type: "text", text: JSON.stringify(envelope("approved_pull_events", page)) },
          { type: "text", text: JSON.stringify(conflicting) }
        ]
      })
    });
    await expectIncomplete(harness, "conflicting_envelope");
    assert.equal(harness.calls.ack.length, 0);
  }
  {
    const wrongResult = envelope("backlog_worker_result", makeResultData());
    wrongResult.binding_id = "bnd_wrong";
    const harness = makeHarness({ resultImpl: async () => wrongResult });
    await expectIncomplete(harness, "control_binding_mismatch");
    assert.equal(harness.calls.inspect.length, 0);
  }
  {
    const harness = makeHarness({
      pullImpl: async () => envelope("approved_activity", clone(PAGE_TWO))
    });
    await expectIncomplete(harness, "wrong_tool");
    assert.equal(harness.calls.ack.length, 0);
  }
});

test("physical exec_command wrappers require exit zero, no live session, and complete JSON output", async () => {
  {
    const harness = makeHarness({
      inspectImpl: async (args) => ({
        exit_code: 0,
        output: JSON.stringify(makeInspection(args.allowed_paths))
      })
    });
    const { report, evidence } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(report.physical.head_commit, BASE);
    assert.equal(evidence.inspect.exit_code, 0);
  }
  {
    const harness = makeHarness({
      inspectImpl: async (args) => ({
        exit_code: 0,
        output: JSON.stringify(makeInspection(args.allowed_paths)),
        session_id: "sess_still_running"
      })
    });
    const { report, evidence } = await expectIncomplete(harness, "physical_inspection_running");
    assert.equal(harness.calls.inspect.length, 1, "a live session is not retried");
    assert.equal(evidence.inspect.session_id, "sess_still_running");
    assert.equal(report.status, "incomplete");
    assert.equal("physical" in report, false, "printed JSON is not reused while a session is live");
  }
  {
    const harness = makeHarness({
      inspectImpl: async (args) => ({
        exit_code: 1,
        output: JSON.stringify(makeInspection(args.allowed_paths))
      })
    });
    await expectIncomplete(harness, "physical_inspection_exit_nonzero");
    assert.equal(harness.calls.inspect.length, 1);
  }
  {
    const harness = makeHarness({
      inspectImpl: async (args) => ({
        exit_code: 0,
        output: JSON.stringify(makeInspection(args.allowed_paths)),
        output_truncated: true
      })
    });
    await expectIncomplete(harness, "physical_inspection_truncated");
    assert.equal(harness.calls.inspect.length, 1);
  }
  {
    const harness = makeHarness({
      inspectImpl: async () => ({
        isError: true,
        exit_code: 0,
        output: "{}"
      })
    });
    await expectIncomplete(harness, "physical_inspection_error");
    assert.equal(harness.calls.inspect.length, 1);
  }
  {
    const harness = makeHarness({
      inspectImpl: async () => ({
        exit_code: 0,
        output: "{not json"
      })
    });
    await expectIncomplete(harness, "physical_inspection_invalid_json");
  }
});

test("physical inspection rejects a changed path outside the frozen set even when allowed is true", async () => {
  const inspection = makeInspection(WRITE_PATHS);
  inspection.changed_paths.push({
    path: "src/extra.rs",
    change_kinds: ["modified"],
    allowed: true
  });
  const harness = makeHarness({ inspection });
  await expectIncomplete(harness, "physical_path_out_of_scope");
  assert.equal(harness.calls.inspect.length, 1);
});

test("direct physical reports are validated while wrapper copies cannot be bypassed", async () => {
  {
    const direct = makeInspection(WRITE_PATHS);
    const harness = makeHarness({ inspectImpl: async () => direct });
    const { report, evidence } = await readResult(harness);
    assert.equal(report.status, "ready_for_semantic_review");
    assert.equal(report.physical.head_commit, BASE);
    assert.equal(report.physical.files.length, WRITE_PATHS.length);
    assert.equal(evidence.inspect, direct, "the exact direct report is retained");
  }
  {
    const refusal = makeInspection(WRITE_PATHS, {
      ok: false,
      refusal_code: "wrong_head",
      changed_paths: [],
      files: [],
      text_included: false,
      untrusted_data: false
    });
    const harness = makeHarness({ inspectImpl: async () => refusal });
    const { evidence } = await expectIncomplete(harness, "physical_inspection_refused");
    assert.equal(evidence.inspect, refusal);
  }
  {
    const invalid = makeInspection(WRITE_PATHS, { authority_granted: true });
    const harness = makeHarness({ inspectImpl: async () => invalid });
    await expectIncomplete(harness, "physical_inspection_invalid");
  }
  {
    const direct = makeInspection(WRITE_PATHS);
    const structured = envelope("threadlane inspect-delivery", makeInspection(WRITE_PATHS));
    const conflicting = clone(structured);
    conflicting.data.head_commit = "f".repeat(40);
    const raw = {
      ...direct,
      structured_content: structured,
      content: [{ type: "text", text: JSON.stringify(conflicting) }]
    };
    const harness = makeHarness({ inspectImpl: async () => raw });
    await expectIncomplete(harness, "conflicting_envelope");
  }
  {
    const direct = makeInspection(WRITE_PATHS);
    const wrappedReport = makeInspection(WRITE_PATHS, { head_commit: "f".repeat(40) });
    const structured = envelope("threadlane inspect-delivery", wrappedReport);
    const raw = {
      ...direct,
      structured_content: structured,
      content: [{ type: "text", text: JSON.stringify(structured) }]
    };
    const harness = makeHarness({ inspectImpl: async () => raw });
    await expectIncomplete(harness, "physical_head_mismatch");
  }
});

test("report is compact and does not duplicate full event DTOs", async () => {
  const harness = makeHarness();
  const { report, evidence } = await readResult(harness);
  assert.equal(report.status, "ready_for_semantic_review");
  assert.ok(JSON.stringify(report).length < JSON.stringify(evidence).length);
  for (const event of report.events) {
    assert.deepEqual(Object.keys(event).sort(), [
      "ack_advanced",
      "acked_seq",
      "batch_id",
      "created_at_ms",
      "event_id",
      "important",
      "kind",
      "revision",
      "run_id",
      "seq",
      "step_seq"
    ]);
    assert.equal(event.payload, undefined);
  }
  assert.equal(report.physical.files[0].text_untrusted, true);
  assert.equal(typeof report.physical.files[0].text, "string");
});

test("doneView verifies a successful Done receipt and returns only bounded fields", async () => {
  const harness = makeHarness();
  const { report } = await readResult(harness);
  const receipt = makeDoneReceipt(report);

  const view = harness.closure.doneView({ receipt, report });
  assert.deepEqual(plain(view), {
    status: "done",
    item_id: ITEM,
    revision: 8,
    completed_at_ms: 5000
  });
  assert.deepEqual(plain(harness.closure.doneView(receipt, report)), plain(view));
  assert.deepEqual(Object.keys(view).sort(), [
    "completed_at_ms",
    "item_id",
    "revision",
    "status"
  ]);
  assert.ok(!JSON.stringify(view).includes(RESULT_DIGEST));
  assert.ok(!JSON.stringify(view).includes("execution"));
});

test("doneView fails closed for conflicting or missing Done facts", async () => {
  const harness = makeHarness();
  const { report } = await readResult(harness);

  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.execution.completion.result_digest = "f".repeat(64);
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.item.revision = 7;
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.item.completed_at_ms = 2000;
    receipt.data.completion.execution.completion.completed_at_ms = 2000;
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.item.status = "doing";
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.tool = "backlog_worker_reject";
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.binding_id = "bnd_wrong";
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.execution.authority.subject_digest = "f".repeat(64);
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.execution.authority.task_digest = "f".repeat(64);
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.delivery.subject_digest = "f".repeat(64);
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.delivery.task_digest = "f".repeat(64);
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.isError = true;
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    delete receipt.data.completion;
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    assert.equal(
      harness.closure.doneView({ receipt: {}, report }).status,
      "incompatible"
    );
  }
  {
    const receipt = makeDoneReceipt(report);
    assert.equal(
      harness.closure.doneView({ receipt, report: { status: "incomplete" } }).status,
      "incompatible"
    );
  }
  {
    const receipt = makeDoneReceipt(report);
    const conflicting = clone(receipt.data);
    conflicting.completion.item.revision = 99;
    receipt.structured_content = receipt.data;
    receipt.content = [{ type: "text", text: JSON.stringify(conflicting) }];
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
});

test("legacy callback and guide prep/complete work with typed data in fresh isolates", async () => {
  const guide = fs.readFileSync(
    path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md"),
    "utf8"
  );
  assert.ok(!guide.includes(["text(", "await tools.exec_command"].join("")));
  assert.ok(!guide.includes(["con", "sole", "lo", "g"].join("")));
  const rawBlocks = [...guide.matchAll(/```js\n([\s\S]*?)```/g)].map((match) => match[1]);
  assert.equal(rawBlocks.length, 3, "guide must contain prep, callback, and complete recipes");

  const [prepBlock, , completeBlock] = rawBlocks;
  const readBlock = legacyReadBlock;
  rawBlocks[1] = readBlock;
  assert.ok(prepBlock.includes("const input = APPROVED_TASK_CONTEXT;"));
  assert.equal((prepBlock.match(/APPROVED_TASK_CONTEXT/g) || []).length, 1);
  assert.ok(completeBlock.includes("const acceptance = ACCEPTANCE_CONTEXT;"));
  assert.equal((completeBlock.match(/ACCEPTANCE_CONTEXT/g) || []).length, 1);
  assert.ok(!readBlock.includes("APPROVED_TASK_CONTEXT"));
  assert.ok(!readBlock.includes("ACCEPTANCE_CONTEXT"));
  assert.ok(!/<[A-Z][A-Z0-9_]*>/.test(readBlock), "callback has no unresolved value markers");
  assert.ok(!/Number\(\s*["']/.test(rawBlocks.join("\n")), "never coerce a quoted marker");
  for (const block of rawBlocks) {
    assert.ok(!block.includes("<ACTUAL_"));
    assert.ok(!block.includes("<FRESH_"));
    assert.ok(!block.includes("<BOUNDED_"));
  }

  const APPROVED_TASK_CONTEXT = {
    binary: "/trusted/bin/threadlane",
    expected_source_sha256: SOURCE_SHA256,
    start_args: {
      command_key: "actual-command-key",
      item_id: ITEM,
      expected_revision: START_EXECUTION.reservation_revision - 1,
      approval_id: APPROVAL,
      task_id: TASK
    },
    scope: {
      base_commit: BASE,
      requested_model: MODEL,
      requested_effort: EFFORT,
      write_paths: [WRITE_PATHS[0]],
      inspector: {
        executable: "/trusted/bin/threadlane",
        argv: ["inspect-delivery"]
      }
    }
  };
  const ACCEPTANCE_CONTEXT = {
    command_key: "fresh-command-key",
    acceptance_note: "bounded acceptance note",
    acceptance_evidence: "bounded acceptance evidence"
  };
  const template = {
    schema_version: 1,
    tool: "threadlane closure-template",
    version: "2026-11-19.v1",
    source_path: "skills/threadlane/references/ordinary-closure.js",
    source_sha256: SOURCE_SHA256,
    factory_name: "createOrdinaryClosure",
    javascript: source
  };
  const sdkFromEnvelope = (value) => ({
    isError: false,
    structuredContent: value,
    content: [{ type: "text", text: JSON.stringify(value) }]
  });
  const sdk = (tool, data) => sdkFromEnvelope(envelope(tool, data));

  const preexistingItem = { item_id: ITEM, revision: START_EXECUTION.reservation_revision - 1, kind: "worker", status: "todo" };
  const backlog = new Map([[preexistingItem.item_id, preexistingItem]]);
  const inspectedPaths = [WRITE_PATHS[0]];
  const stored = new Map();
  const texts = [];
  const calls = { exec: 0, start: 0, complete: 0, pull: 0, ack: 0, result: 0, inspect: 0 };
  const startArgsSeen = [];
  const completeArgsSeen = [];
  const startWrapper = sdk("backlog_worker_start", { execution: clone(START_EXECUTION) });

  const fakeTools = {
    exec_command: async ({ cmd }) => {
      calls.exec += 1;
      if (cmd.includes("closure-template")) {
        return { exit_code: 0, output: JSON.stringify(template) };
      }
      calls.inspect += 1;
      return { exit_code: 0, output: JSON.stringify(makeInspection(inspectedPaths)) };
    },
    mcp__threadlane__backlog_worker_start: async (args) => {
      calls.start += 1;
      startArgsSeen.push(clone(args));
      assert.ok(backlog.has(args.item_id), "start must target a preexisting backlog item");
      assert.equal(args.item_id, ITEM);
      assert.equal(args.expected_revision, preexistingItem.revision);
      assert.equal(args.approval_id, APPROVAL);
      assert.equal(args.task_id, TASK);
      return startWrapper;
    },
    mcp__threadlane__approved_pull_events: async () => {
      calls.pull += 1;
      return sdk("approved_pull_events", clone(PAGE_TWO));
    },
    mcp__threadlane__approved_ack_events: async (args) => {
      calls.ack += 1;
      return sdk("approved_ack_events", makeAckData(args.batch_id, 3, true));
    },
    mcp__threadlane__backlog_worker_result: async () => {
      calls.result += 1;
      return sdk("backlog_worker_result", makeResultData());
    },
    mcp__threadlane__backlog_worker_complete: async (args) => {
      calls.complete += 1;
      completeArgsSeen.push(clone(args));
      const report = JSON.parse(stored.get("threadlane/closure-report.json"));
      assert.equal(report.status, "ready_for_semantic_review");
      assert.equal(args.expected_revision, report.result.current_revision);
      assert.equal(args.execution_ref, report.mapping.execution_ref);
      assert.equal(args.expected_result_digest, report.result.result_digest);
      assert.equal(args.expected_delivery_digest, report.result.delivery_digest);
      return sdkFromEnvelope(makeDoneReceipt(report));
    }
  };
  const store = async (key, value) => {
    stored.set(key, JSON.stringify(value));
    return undefined;
  };
  const load = (key) => {
    const serialized = stored.get(key);
    return serialized === undefined ? undefined : JSON.parse(serialized);
  };
  const text = (value) => {
    texts.push(value);
    return undefined;
  };
  const runRecipe = async (index, globals = {}, tools = fakeTools) => {
    const sandbox = { tools, store, load, text, NOTICE_APPROVAL_ID: APPROVAL, ...globals };
    return vm.runInNewContext(
      `(async () => {\n${rawBlocks[index]}\n})()`,
      sandbox,
      { filename: `guide-recipe-${index}.js` }
    );
  };

  await assert.rejects(
    runRecipe(0, { APPROVED_TASK_CONTEXT: null }),
    /invalid approved task context/,
    "a null typed input stops before effects"
  );
  assert.equal(calls.exec, 0, "invalid input performs no exec_command");
  assert.equal(calls.start, 0, "invalid input performs no start");
  const invalidContext = clone(APPROVED_TASK_CONTEXT);
  invalidContext.start_args.expected_revision = 0;
  await assert.rejects(
    runRecipe(0, { APPROVED_TASK_CONTEXT: invalidContext }),
    /invalid approved task context/,
    "a non-positive revision stops before effects"
  );
  assert.equal(calls.exec, 0);
  assert.equal(calls.start, 0);

  let badExecCalls = 0;
  const badCapsuleTools = {
    ...fakeTools,
    exec_command: async () => {
      badExecCalls += 1;
      return {
        exit_code: 0,
        output: JSON.stringify({ ...template, source_sha256: "b".repeat(64) })
      };
    }
  };
  await assert.rejects(
    runRecipe(0, { APPROVED_TASK_CONTEXT: clone(APPROVED_TASK_CONTEXT) }, badCapsuleTools),
    /untrusted closure template/,
    "a failed capsule check stops before start"
  );
  assert.equal(badExecCalls, 1);
  assert.equal(calls.start, 0, "failed capsule performs no start");
  assert.equal(stored.has("threadlane/start-receipt.json"), false);

  await runRecipe(0, { APPROVED_TASK_CONTEXT: clone(APPROVED_TASK_CONTEXT) });
  assert.equal(calls.exec, 1);
  assert.equal(calls.start, 1);
  assert.deepEqual(plain(startArgsSeen[0]), APPROVED_TASK_CONTEXT.start_args);
  assert.deepEqual(plain(load("threadlane/approved-task-context.json")), plain(APPROVED_TASK_CONTEXT));
  assert.equal(load("threadlane/closure-template.json").source_sha256, template.source_sha256);
  assert.deepEqual(plain(load("threadlane/start-receipt.json")), plain(startWrapper));
  assert.equal(typeof stored.get("threadlane/start-receipt.json"), "string");
  assert.equal(texts.length, 1);
  assert.ok(!texts[0].includes(template.javascript));
  assert.ok(!texts[0].includes("function createOrdinaryClosure"));

  await runRecipe(1);
  assert.equal(calls.pull, 1);
  assert.equal(calls.ack, 1);
  assert.equal(calls.result, 1);
  assert.equal(calls.inspect, 1);
  const report = load("threadlane/closure-report.json");
  const evidence = load("threadlane/closure-evidence.json");
  assert.equal(report.status, "ready_for_semantic_review");
  assert.equal(report.authority_granted, false);
  assert.equal("completion" in report, false);
  assert.equal(evidence.pages.length, 1);
  assert.equal(evidence.pages[0].structuredContent.tool, "approved_pull_events");
  assert.ok(JSON.stringify(evidence).includes(RAW_LOG));
  assert.ok(!JSON.stringify(report).includes(RAW_LOG));
  assert.equal(texts.length, 2);
  const compact = JSON.parse(texts[1]);
  assert.equal(compact.status, "ready_for_semantic_review");
  assert.equal(compact.authority_granted, false);
  assert.equal(compact.source_sha256, template.source_sha256);
  assert.deepEqual(plain(compact.mapping), plain(report.mapping));
  assert.deepEqual(plain(compact.scope), plain(report.scope));
  assert.deepEqual(plain(compact.result), plain(report.result));
  assert.deepEqual(plain(compact.batches), plain(report.batches));
  assert.deepEqual(plain(compact.checks), plain(report.checks));
  assert.deepEqual(plain(compact.physical), plain(report.physical));
  assert.equal(compact.events.count, report.events.length);
  assert.equal(compact.events.acked_seq, report.batches[report.batches.length - 1].acked_seq);
  assert.equal(Array.isArray(compact.events), false);
  assert.ok(!texts[1].includes(RAW_LOG));
  assert.ok(!texts[1].includes("closure-evidence"));

  assert.equal(calls.complete, 0, "read performs no automatic completion");
  const semanticAccepted =
    report.status === "ready_for_semantic_review" &&
    report.physical.head_commit === BASE &&
    report.physical.files[0].text_untrusted === true;
  assert.equal(semanticAccepted, true, "the Coworker makes a separate semantic decision");
  await assert.rejects(
    runRecipe(2, { ACCEPTANCE_CONTEXT: null }),
    /missing explicit acceptance input/,
    "no completion without the separate typed semantic input"
  );
  assert.equal(calls.complete, 0);
  const invalidAcceptance = clone(ACCEPTANCE_CONTEXT);
  invalidAcceptance.acceptance_evidence = "";
  await assert.rejects(
    runRecipe(2, { ACCEPTANCE_CONTEXT: invalidAcceptance }),
    /missing explicit acceptance input/
  );
  assert.equal(calls.complete, 0);

  await runRecipe(2, { ACCEPTANCE_CONTEXT: clone(ACCEPTANCE_CONTEXT) });
  assert.equal(calls.complete, 1);
  assert.deepEqual(plain(completeArgsSeen[0]), {
    command_key: ACCEPTANCE_CONTEXT.command_key,
    item_id: ITEM,
    expected_revision: report.result.current_revision,
    execution_ref: EXEC,
    expected_result_digest: RESULT_DIGEST,
    expected_delivery_digest: DELIVERY_DIGEST,
    acceptance_note: ACCEPTANCE_CONTEXT.acceptance_note,
    acceptance_evidence: ACCEPTANCE_CONTEXT.acceptance_evidence
  });
  assert.deepEqual(
    plain(storedCompletionRecord(stored, "threadlane/closure-done-receipt.json", EXEC).record),
    plain(sdkFromEnvelope(makeDoneReceipt(report)))
  );
  assert.equal(texts.length, 3, "prep metadata, compact report, and doneView are texted once each");
  assert.ok(!texts.join("\n").includes(template.javascript));
  assert.ok(!texts.join("\n").includes(RAW_LOG));
  assert.deepEqual(plain(JSON.parse(texts[2])), {
    final_summary: ACCEPTANCE_CONTEXT.acceptance_note,
    status: "done",
    item_id: ITEM,
    revision: 8,
    completed_at_ms: 5000
  });
});

function documentRecipeBlocks(documentPath) {
  const document = fs.readFileSync(documentPath, "utf8");
  const blocks = [...document.matchAll(/```js\n([\s\S]*?)```/g)].map((match) => match[1]);
  blocks[1] = legacyReadBlock;
  return blocks;
}

function makeGuideEnvironment(
  documentPath = path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md")
) {
  const blocks = documentRecipeBlocks(documentPath);
  assert.equal(blocks.length, 3);
  const template = {
    schema_version: 1,
    tool: "threadlane closure-template",
    version: "2026-11-19.v1",
    source_path: "skills/threadlane/references/ordinary-closure.js",
    source_sha256: SOURCE_SHA256,
    factory_name: "createOrdinaryClosure",
    javascript: source
  };
  const approvedContext = {
    binary: "/trusted/bin/threadlane",
    expected_source_sha256: template.source_sha256,
    start_args: {
      command_key: "actual-command-key",
      item_id: ITEM,
      expected_revision: START_EXECUTION.reservation_revision - 1,
      approval_id: APPROVAL,
      task_id: TASK
    },
    scope: {
      base_commit: BASE,
      requested_model: MODEL,
      requested_effort: EFFORT,
      write_paths: [WRITE_PATHS[0]],
      inspector: {
        executable: "/trusted/bin/threadlane",
        argv: ["inspect-delivery"]
      }
    }
  };
  const acceptanceContext = {
    command_key: "fresh-command-key",
    acceptance_note: "bounded acceptance note",
    acceptance_evidence: "bounded acceptance evidence"
  };
  const stored = new Map();
  const texts = [];
  const calls = { exec: 0, start: 0, complete: 0, pull: 0, ack: 0, result: 0, inspect: 0 };
  const startArgsSeen = [];
  const completeArgsSeen = [];
  const sdkFromEnvelope = (value) => ({
    isError: false,
    structuredContent: value,
    content: [{ type: "text", text: JSON.stringify(value) }]
  });
  const sdk = (tool, data) => sdkFromEnvelope(envelope(tool, data));
  let startBuilder = () => sdk("backlog_worker_start", { execution: clone(START_EXECUTION) });
  const tools = {
    exec_command: async ({ cmd }) => {
      calls.exec += 1;
      if (cmd.includes("closure-template")) {
        return { exit_code: 0, output: JSON.stringify(template) };
      }
      calls.inspect += 1;
      return { exit_code: 0, output: JSON.stringify(makeInspection([WRITE_PATHS[0]])) };
    },
    mcp__threadlane__backlog_worker_start: async (args) => {
      calls.start += 1;
      startArgsSeen.push(clone(args));
      return startBuilder(args);
    },
    mcp__threadlane__approved_pull_events: async () => {
      calls.pull += 1;
      return sdk("approved_pull_events", clone(PAGE_TWO));
    },
    mcp__threadlane__approved_ack_events: async (args) => {
      calls.ack += 1;
      return sdk("approved_ack_events", makeAckData(args.batch_id, 3, true));
    },
    mcp__threadlane__backlog_worker_result: async () => {
      calls.result += 1;
      return sdk("backlog_worker_result", makeResultData());
    },
    mcp__threadlane__backlog_worker_complete: async (args) => {
      calls.complete += 1;
      completeArgsSeen.push(clone(args));
      const report = JSON.parse(stored.get("threadlane/closure-report.json"));
      return sdkFromEnvelope(makeDoneReceipt(report));
    }
  };
  const store = async (key, value) => {
    stored.set(key, JSON.stringify(value));
    return undefined;
  };
  const load = (key) => {
    const serialized = stored.get(key);
    return serialized === undefined ? undefined : JSON.parse(serialized);
  };
  const text = (value) => {
    texts.push(value);
    return undefined;
  };
  let noticeApprovalId = APPROVAL;
  const runRecipe = async (index, globals = {}, activeTools = tools) => {
    const sandbox = { tools: activeTools, store, load, text, NOTICE_APPROVAL_ID: noticeApprovalId, ...globals };
    return vm.runInNewContext(
      `(async () => {\n${blocks[index]}\n})()`,
      sandbox,
      { filename: `guide-correlation-${index}.js` }
    );
  };
  return {
    documentPath,
    blocks,
    template,
    approvedContext,
    acceptanceContext,
    stored,
    texts,
    calls,
    startArgsSeen,
    completeArgsSeen,
    tools,
    runRecipe,
    sdkFromEnvelope,
    sdk,
    setNoticeApproval: (id) => { noticeApprovalId = id; },
    setStart: (builder) => {
      startBuilder = builder;
    }
  };
}

function storedCompletionState(stored, key) {
  const serialized = stored.get(key);
  assert.notEqual(serialized, undefined, `${key}: completion state exists`);
  const state = JSON.parse(serialized);
  assert.equal(state.schema_version, 2, `${key}: completion state schema`);
  assert.equal(state.tool, "threadlane closure-client completion", `${key}: completion state tool`);
  assert.ok(state.records && typeof state.records === "object", `${key}: records`);
  return state;
}

function storedCompletionRecord(stored, key, executionRef) {
  const state = storedCompletionState(stored, key);
  const matches = Object.keys(state.records).filter((recordKey) =>
    JSON.stringify(state.records[recordKey]).includes(executionRef)
  );
  assert.equal(matches.length, 1, `${key}: one record for ${executionRef}`);
  return {
    state,
    recordKey: matches[0],
    record: state.records[matches[0]]
  };
}

function updateStoredCompletionRecord(stored, key, executionRef, mutate) {
  const found = storedCompletionRecord(stored, key, executionRef);
  mutate(found.record);
  stored.set(key, JSON.stringify(found.state));
  return found;
}

function deleteStoredCompletionRecord(stored, key, executionRef) {
  const found = storedCompletionRecord(stored, key, executionRef);
  delete found.state.records[found.recordKey];
  stored.set(key, JSON.stringify(found.state));
}

function completionRecordCount(stored, key) {
  return Object.keys(storedCompletionState(stored, key).records).length;
}

function installGuideCompletionClock(env, times) {
  let completedIndex = 0;
  env.tools.mcp__threadlane__backlog_worker_complete = async (args) => {
    env.calls.complete += 1;
    env.completeArgsSeen.push(clone(args));
    const report = JSON.parse(env.stored.get("threadlane/closure-report.json"));
    const completedAtMs = times[completedIndex] ?? 5000 + completedIndex * 1000;
    completedIndex += 1;
    return env.sdkFromEnvelope(makeDoneReceipt(report, completedAtMs));
  };
}

test("MCP guide prep rejects empty model/effort and unsafe write paths before any tool call", async () => {
  const env = makeGuideEnvironment();
  const invalidContexts = [
    ["empty requested_model", (context) => {
      context.scope.requested_model = "";
    }],
    ["empty requested_effort", (context) => {
      context.scope.requested_effort = "";
    }],
    ["absolute write path", (context) => {
      context.scope.write_paths = ["/tmp/escape.rs"];
    }],
    ["parent traversal", (context) => {
      context.scope.write_paths = ["src/../escape.rs"];
    }],
    ["dot segment", (context) => {
      context.scope.write_paths = ["src/./escape.rs"];
    }],
    ["empty segment", (context) => {
      context.scope.write_paths = ["src//escape.rs"];
    }],
    ["backslash", (context) => {
      context.scope.write_paths = ["src\\escape.rs"];
    }],
    ["NUL", (context) => {
      context.scope.write_paths = ["src/escape\u0000.rs"];
    }]
  ];
  for (const [label, mutate] of invalidContexts) {
    const context = clone(env.approvedContext);
    mutate(context);
    await assert.rejects(
      env.runRecipe(0, { APPROVED_TASK_CONTEXT: context }),
      /invalid approved task context/,
      label
    );
    assert.equal(env.calls.exec, 0, `${label}: no exec_command before validation`);
    assert.equal(env.calls.start, 0, `${label}: no start before validation`);
    assert.equal(env.stored.size, 0, `${label}: no retained data before validation`);
  }
});

test("MCP guide completion refuses stale, missing, or changed report correlation before guarded completion", async () => {
  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    assert.equal(env.calls.complete, 0, "read performs no automatic completion");
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: null }),
      /missing explicit acceptance input/,
      "no semantic object means no completion"
    );
    assert.equal(env.calls.complete, 0);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    assert.equal(env.calls.complete, 1, "the current correlated report completes exactly once");
    const report = JSON.parse(env.stored.get("threadlane/closure-report.json"));
    assert.deepEqual(plain(env.completeArgsSeen[0]), {
      command_key: env.acceptanceContext.command_key,
      item_id: ITEM,
      expected_revision: report.result.current_revision,
      execution_ref: EXEC,
      expected_result_digest: RESULT_DIGEST,
      expected_delivery_digest: DELIVERY_DIGEST,
      acceptance_note: env.acceptanceContext.acceptance_note,
      acceptance_evidence: env.acceptanceContext.acceptance_evidence
    });
    assert.deepEqual(plain(JSON.parse(env.texts[2])), {
      final_summary: env.acceptanceContext.acceptance_note,
      status: "done",
      item_id: ITEM,
      revision: 8,
      completed_at_ms: 5000
    });
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    const oldReport = env.stored.get("threadlane/closure-report.json");
    const oldEvidence = env.stored.get("threadlane/closure-evidence.json");
    const oldCorrelation = env.stored.get("threadlane/closure-read-correlation.json");
    assert.ok(oldReport && oldEvidence && oldCorrelation);
    const nextContext = clone(env.approvedContext);
    nextContext.start_args.command_key = "next-command-key";
    nextContext.start_args.expected_revision = 8;
    nextContext.start_args.approval_id = "apr_next_attempt";
    nextContext.start_args.task_id = "task-next-attempt";
    nextContext.scope.requested_effort = "medium";
    const nextExecution = clone(START_EXECUTION);
    nextExecution.execution_ref = "wbe_next_attempt";
    nextExecution.authority.approval_id = "apr_next_attempt";
    nextExecution.authority.task_id = "task-next-attempt";
    nextExecution.acceptance_condition_digest = "9".repeat(64);
    nextExecution.delivery_description_digest = "8".repeat(64);
    nextExecution.reservation_revision = 6;
    nextExecution.reserved_at_ms = 1200;
    nextExecution.dispatch_receipt.execution_binding_digest = "7".repeat(64);
    env.setStart(() => sdkStart(startEnvelope(nextExecution)));
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: nextContext });
    assert.equal(env.calls.start, 2, "a new prep records a new start without a callback");
    assert.equal(
      env.stored.get("threadlane/closure-report.json"),
      oldReport,
      "the old ready report is preserved"
    );
    assert.equal(
      env.stored.get("threadlane/closure-evidence.json"),
      oldEvidence,
      "the old evidence is preserved"
    );
    assert.equal(
      env.stored.get("threadlane/closure-read-correlation.json"),
      oldCorrelation,
      "the old correlation is preserved"
    );
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "the stale report never reaches guarded completion");
    assert.equal(env.stored.has("threadlane/closure-done-receipt.json"), false);
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    const nextExecution = clone(START_EXECUTION);
    nextExecution.execution_ref = "wbe_same_item_new_attempt";
    nextExecution.authority.approval_id = "apr_same_item_new_attempt";
    nextExecution.authority.task_id = "task-same-item-new-attempt";
    nextExecution.acceptance_condition_digest = "9".repeat(64);
    nextExecution.dispatch_receipt.execution_binding_digest = "8".repeat(64);
    env.setStart(() => sdkStart(startEnvelope(nextExecution)));
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    assert.equal(env.calls.start, 2);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "changed authority/ref/start on the same item refuses");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    const changedReport = JSON.parse(env.stored.get("threadlane/closure-report.json"));
    changedReport.result.current_revision = 6;
    env.stored.set("threadlane/closure-report.json", JSON.stringify(changedReport));
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "a changed report no longer matches the read correlation");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    env.stored.delete("threadlane/closure-read-correlation.json");
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "missing correlation refuses");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    const correlation = JSON.parse(
      env.stored.get("threadlane/closure-read-correlation.json")
    );
    correlation.input.scope.requested_effort = "medium";
    env.stored.set("threadlane/closure-read-correlation.json", JSON.stringify(correlation));
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "changed context correlation refuses");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    const correlation = JSON.parse(
      env.stored.get("threadlane/closure-read-correlation.json")
    );
    correlation.start.structuredContent.data.execution.execution_ref = "wbe_changed_correlation";
    env.stored.set("threadlane/closure-read-correlation.json", JSON.stringify(correlation));
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /invalid closure capsule, context, or report.*original-receipt fallback/
    );
    assert.equal(env.calls.complete, 0, "changed start correlation refuses");
  }
});

test("legacy callback refuses mixed prep input/start bindings before effects", async () => {
  const documents = [
    ["MCP guide", path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md")],
    ["closure doc", path.join(here, "../../../docs/ORDINARY-WORKER-CLOSURE.md")]
  ];
  const bindingKey = "threadlane/start-binding.json";
  const nextContext = (env, suffix) => {
    const next = clone(env.approvedContext);
    next.start_args = {
      command_key: `b-command-key-${suffix}`,
      item_id: `blg_${suffix}`,
      expected_revision: 8,
      approval_id: `apr_${suffix}`,
      task_id: `task-${suffix}`
    };
    return next;
  };
  const readCounts = (env) => clone({
    exec: env.calls.exec,
    start: env.calls.start,
    complete: env.calls.complete,
    pull: env.calls.pull,
    ack: env.calls.ack,
    result: env.calls.result,
    inspect: env.calls.inspect
  });
  const storedSnapshot = (env) => ({
    start: env.stored.get("threadlane/start-receipt.json"),
    binding: env.stored.get(bindingKey),
    evidence: env.stored.get("threadlane/closure-evidence.json"),
    report: env.stored.get("threadlane/closure-report.json"),
    correlation: env.stored.get("threadlane/closure-read-correlation.json")
  });
  const assertSnapshot = (env, snapshot, label) => {
    assert.equal(env.stored.get("threadlane/start-receipt.json"), snapshot.start, `${label}: old start`);
    assert.equal(env.stored.get(bindingKey), snapshot.binding, `${label}: old binding`);
    assert.equal(env.stored.get("threadlane/closure-evidence.json"), snapshot.evidence, `${label}: old evidence`);
    assert.equal(env.stored.get("threadlane/closure-report.json"), snapshot.report, `${label}: old report`);
    assert.equal(
      env.stored.get("threadlane/closure-read-correlation.json"),
      snapshot.correlation,
      `${label}: old correlation`
    );
  };
  const prepareAndRead = async (env) => {
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    assert.equal(env.calls.start, 1, "A prep starts exactly once");
    assert.equal(env.calls.pull, 1);
    assert.equal(env.calls.ack, 1);
    assert.equal(env.calls.result, 1);
    assert.equal(env.calls.inspect, 1);
  };
  const expectRefusal = async (env, activeTools, label) => {
    const counts = readCounts(env);
    await assert.rejects(
      env.runRecipe(1, {}, activeTools),
      /missing or changed retained context\/start\/binding.*fallback/,
      `${label}: callback refuses before effects`
    );
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }, activeTools),
      /invalid closure capsule, context, or report.*start\/binding\/correlation.*original-receipt fallback/,
      `${label}: complete refuses before completion`
    );
    assert.deepEqual(readCounts(env), counts, `${label}: refusal performs no pull/ACK/result/inspect/complete`);
    assert.equal(env.calls.complete, counts.complete, `${label}: no guarded completion`);
    assert.equal(env.stored.has("threadlane/closure-done-receipt.json"), false, label);
  };

  for (const [label, documentPath] of documents) {
    {
      const structure = makeGuideEnvironment(documentPath);
      assert.equal(structure.blocks.length, 3, `${label}: three literal fresh-isolate examples`);
      assert.equal(
        (structure.blocks[0].match(/APPROVED_TASK_CONTEXT/g) || []).length,
        1,
        `${label}: one typed prep object`
      );
      assert.ok(!/<[A-Z][A-Z0-9_]*>/.test(structure.blocks[1]), `${label}: callback has no value markers`);
      assert.ok(!structure.blocks[1].includes("APPROVED_TASK_CONTEXT"), `${label}: callback binds no prep input`);
      assert.ok(!structure.blocks[1].includes("ACCEPTANCE_CONTEXT"), `${label}: callback binds no acceptance input`);
      assert.ok(
        !structure.blocks[1].includes('store("threadlane/start-binding.json"'),
        `${label}: callback never mints or repairs the binding`
      );
      assert.ok(
        !structure.blocks[2].includes('store("threadlane/start-binding.json"'),
        `${label}: complete never mints or repairs the binding`
      );
      assert.equal(
        (structure.blocks[2].match(/ACCEPTANCE_CONTEXT/g) || []).length,
        1,
        `${label}: one separate semantic DATA object`
      );
      assert.ok(
        structure.blocks[0].includes('store("threadlane/start-binding.json", {input, start: started})'),
        `${label}: prep owns one start binding`
      );
      assert.equal(
        (structure.blocks[0].match(/backlog_worker_start\(/g) || []).length,
        1,
        `${label}: prep owns one start`
      );
      const startIndex = structure.blocks[0].indexOf("backlog_worker_start(a)");
      const receiptIndex = structure.blocks[0].indexOf('store("threadlane/start-receipt.json"');
      const bindingIndex = structure.blocks[0].indexOf('store("threadlane/start-binding.json"');
      assert.ok(
        startIndex >= 0 && startIndex < receiptIndex && receiptIndex < bindingIndex,
        `${label}: prep retains the returned DTO before its additive binding`
      );
      assert.ok(
        structure.blocks[1].includes("createOrdinaryClosureClient"),
        `${label}: callback uses the small client helper`
      );
      assert.ok(
        structure.blocks[2].includes("createOrdinaryClosureClient"),
        `${label}: complete uses the small client helper`
      );
      assert.ok(
        !structure.blocks[1].includes("backlog_worker_start"),
        `${label}: callback never starts`
      );
      assert.ok(
        !structure.blocks[1].includes("backlog_worker_complete"),
        `${label}: callback never completes`
      );
      assert.ok(
        !structure.blocks[2].includes("backlog_worker_start"),
        `${label}: complete never starts`
      );
    }

    {
      const env = makeGuideEnvironment(documentPath);
      await prepareAndRead(env);
      const report = JSON.parse(env.stored.get("threadlane/closure-report.json"));
      assert.equal(report.status, "ready_for_semantic_review", label);
      const binding = JSON.parse(env.stored.get(bindingKey));
      assert.deepEqual(plain(binding), plain({
        input: env.approvedContext,
        start: JSON.parse(env.stored.get("threadlane/start-receipt.json"))
      }), `${label}: prep binds the exact retained input and raw start`);
      await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
      assert.equal(env.calls.complete, 1, `${label}: positive flow completes once`);
      assert.deepEqual(plain(JSON.parse(env.texts[2])), {
        final_summary: env.acceptanceContext.acceptance_note,
        status: "done",
        item_id: ITEM,
        revision: 8,
        completed_at_ms: 5000
      }, label);
      assert.ok(!env.texts.join("\n").includes(env.template.javascript), `${label}: no JS echo`);
      assert.ok(!env.texts.join("\n").includes(RAW_LOG), `${label}: no raw provider log`);
    }

    {
      const env = makeGuideEnvironment(documentPath);
      await prepareAndRead(env);
      const snapshot = storedSnapshot(env);
      const counts = readCounts(env);
      const bContext = nextContext(env, "exec_fault");
      assert.deepEqual(plain(bContext.scope), plain(env.approvedContext.scope), `${label}: B keeps scope`);
      assert.notEqual(bContext.start_args.item_id, env.approvedContext.start_args.item_id);
      assert.notEqual(bContext.start_args.approval_id, env.approvedContext.start_args.approval_id);
      assert.notEqual(bContext.start_args.task_id, env.approvedContext.start_args.task_id);
      let faults = 1;
      const faultTools = {
        ...env.tools,
        exec_command: async (args) => {
          if (faults > 0 && args.cmd.includes("closure-template")) {
            faults -= 1;
            env.calls.exec += 1;
            return { exit_code: 1, output: "one-shot closure-template fault" };
          }
          return env.tools.exec_command(args);
        }
      };
      await assert.rejects(
        env.runRecipe(0, { APPROVED_TASK_CONTEXT: bContext }, faultTools),
        /canonical closure-template failed/,
        `${label}: B exec failure stops prep`
      );
      assert.equal(faults, 0, `${label}: one-shot exec fault is restored before callback`);
      assert.deepEqual(
        plain(JSON.parse(env.stored.get("threadlane/approved-task-context.json"))),
        plain(bContext),
        `${label}: B input persists before exec`
      );
      assertSnapshot(env, snapshot, `${label}: B exec failure keeps A evidence/start/binding`);
      assert.deepEqual(
        readCounts(env),
        { ...counts, exec: counts.exec + 1 },
        `${label}: B exec failure starts nothing else`
      );
      await expectRefusal(env, faultTools, `${label}: B exec failure`);
      assertSnapshot(env, snapshot, `${label}: B exec failure refusal keeps old evidence`);
      assert.equal(env.calls.start, counts.start, `${label}: no replay after B exec failure`);
    }

    for (const [mode, startFault] of [
      ["throw", () => { throw new Error("lost start reply"); }],
      ["lost", () => undefined]
    ]) {
      const env = makeGuideEnvironment(documentPath);
      await prepareAndRead(env);
      const snapshot = storedSnapshot(env);
      const counts = readCounts(env);
      const bContext = nextContext(env, `start_${mode}`);
      let armed = true;
      env.setStart(() => {
        if (armed) {
          armed = false;
          return startFault();
        }
        return sdkStart(startEnvelope(clone(START_EXECUTION)));
      });
      await assert.rejects(
        env.runRecipe(0, { APPROVED_TASK_CONTEXT: bContext }),
        mode === "throw" ? /lost start reply/ : /missing guarded start reply/,
        `${label}: B start ${mode} stops prep`
      );
      assert.equal(armed, false, `${label}: one-shot start fault is restored before callback`);
      env.setStart(() => sdkStart(startEnvelope(clone(START_EXECUTION))));
      assert.deepEqual(
        plain(JSON.parse(env.stored.get("threadlane/approved-task-context.json"))),
        plain(bContext),
        `${label}: B input persists before start ${mode}`
      );
      assert.deepEqual(
        plain(env.startArgsSeen[env.startArgsSeen.length - 1]),
        plain(bContext.start_args),
        `${label}: B start uses the different item/authority`
      );
      assertSnapshot(env, snapshot, `${label}: B start ${mode} keeps A evidence/start/binding`);
      assert.deepEqual(
        readCounts(env),
        { ...counts, exec: counts.exec + 1, start: counts.start + 1 },
        `${label}: B start ${mode} performs no callback effect`
      );
      await expectRefusal(env, env.tools, `${label}: B start ${mode}`);
      assertSnapshot(env, snapshot, `${label}: B start ${mode} refusal keeps old evidence`);
      assert.equal(env.calls.start, counts.start + 1, `${label}: no replay after B start ${mode}`);
    }

    for (const [caseLabel, mutate] of [
      ["missing binding", (env) => {
        env.stored.delete(bindingKey);
      }],
      ["changed binding input", (env) => {
        const binding = JSON.parse(env.stored.get(bindingKey));
        binding.input.scope.requested_effort = "medium";
        env.stored.set(bindingKey, JSON.stringify(binding));
      }],
      ["changed binding start", (env) => {
        const binding = JSON.parse(env.stored.get(bindingKey));
        binding.start.structuredContent.data.execution.execution_ref = "wbe_changed_binding";
        env.stored.set(bindingKey, JSON.stringify(binding));
      }],
      ["partial binding input", (env) => {
        const binding = JSON.parse(env.stored.get(bindingKey));
        env.stored.set(bindingKey, JSON.stringify({ input: binding.input }));
      }],
      ["partial binding start", (env) => {
        const binding = JSON.parse(env.stored.get(bindingKey));
        env.stored.set(bindingKey, JSON.stringify({ start: binding.start }));
      }]
    ]) {
      const env = makeGuideEnvironment(documentPath);
      await prepareAndRead(env);
      const snapshot = storedSnapshot(env);
      mutate(env);
      const mutatedBinding = env.stored.get(bindingKey);
      await expectRefusal(env, env.tools, `${label}: ${caseLabel}`);
      assert.equal(env.stored.get(bindingKey), mutatedBinding, `${label}: ${caseLabel} is never repaired`);
      assert.equal(env.stored.get("threadlane/start-receipt.json"), snapshot.start, `${label}: ${caseLabel} keeps old start`);
      assert.equal(
        env.stored.get("threadlane/closure-evidence.json"),
        snapshot.evidence,
        `${label}: ${caseLabel} keeps old evidence`
      );
      assert.equal(
        env.stored.get("threadlane/closure-report.json"),
        snapshot.report,
        `${label}: ${caseLabel} keeps old report`
      );
      assert.equal(
        env.stored.get("threadlane/closure-read-correlation.json"),
        snapshot.correlation,
        `${label}: ${caseLabel} keeps old correlation`
      );
    }
  }
});

test("ordinary closure client returns a compact verified view while raw evidence and physical text stay unchanged", async () => {
  const documents = [
    path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md"),
    path.join(here, "../../../docs/ORDINARY-WORKER-CLOSURE.md")
  ];
  for (const documentPath of documents) {
    const label = path.basename(documentPath);
    const env = makeGuideEnvironment(documentPath);
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);

    const view = JSON.parse(env.texts[1]);
    const report = JSON.parse(env.stored.get("threadlane/closure-report.json"));
    const evidence = JSON.parse(env.stored.get("threadlane/closure-evidence.json"));
    const correlation = JSON.parse(env.stored.get("threadlane/closure-read-correlation.json"));
    const start = JSON.parse(env.stored.get("threadlane/start-receipt.json"));
    assert.equal(view.status, "ready_for_semantic_review", label);
    assert.equal(view.authority_granted, false, label);
    assert.equal(view.source_sha256, env.template.source_sha256, label);
    assert.deepEqual(plain(view.mapping), plain(report.mapping), label);
    assert.deepEqual(plain(view.scope), plain(report.scope), label);
    assert.deepEqual(plain(view.result), plain(report.result), label);
    assert.deepEqual(plain(view.batches), plain(report.batches), label);
    assert.deepEqual(plain(view.checks), plain(report.checks), label);
    assert.deepEqual(plain(view.physical), plain(report.physical), label);
    assert.equal(Array.isArray(view.events), false, `${label}: events stay compact`);
    assert.equal(view.events.count, report.events.length, label);
    assert.equal(view.events.first_seq, report.events[0].seq, label);
    assert.equal(
      view.events.last_seq,
      report.events[report.events.length - 1].seq,
      label
    );
    assert.equal(
      view.events.acked_seq,
      report.batches[report.batches.length - 1].acked_seq,
      label
    );
    assert.equal(
      view.physical.files[0].text,
      makeInspection([WRITE_PATHS[0]]).files[0].text,
      `${label}: exact physical text is retained`
    );
    assert.deepEqual(plain(correlation), plain({
      input: env.approvedContext,
      start,
      report
    }), `${label}: the stored correlation is exact`);
    assert.equal(evidence.pages[0].structuredContent.tool, "approved_pull_events", label);
    assert.ok(JSON.stringify(evidence).includes(RAW_LOG), `${label}: raw evidence stays full`);
    assert.ok(!JSON.stringify(view).includes(RAW_LOG), `${label}: raw provider log is not texted`);
    assert.equal(env.calls.complete, 0, `${label}: read never completes`);
    assert.equal(Array.isArray(report.events), true, `${label}: the private report stays full`);
  }
});

test("ordinary closure client requires semantic data and reuses only matching validated Done", async () => {
  const env = makeGuideEnvironment();
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
  await env.runRecipe(1);

  const invalidAcceptances = [
    null,
    {},
    {
      command_key: "",
      acceptance_note: "note",
      acceptance_evidence: "evidence"
    },
    {
      command_key: "key",
      acceptance_note: "",
      acceptance_evidence: "evidence"
    },
    {
      command_key: "key",
      acceptance_note: "note",
      acceptance_evidence: ""
    }
  ];
  for (const acceptance of invalidAcceptances) {
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: acceptance }),
      /missing explicit acceptance input/,
      "empty semantic input must not complete"
    );
  }
  assert.equal(env.calls.complete, 0, "empty semantic input performs zero complete calls");
  assert.equal(env.stored.has("threadlane/closure-completion-intent.json"), false);
  assert.equal(env.stored.has("threadlane/closure-done-receipt.json"), false);

  await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
  assert.equal(env.calls.complete, 1, "the positive flow completes exactly once");
  const doneView = storedCompletionRecord(
    env.stored,
    "threadlane/closure-done-view.json",
    EXEC
  ).record;
  assert.equal(doneView.status, "done");
  assert.equal(doneView.item_id, ITEM);
  assert.equal(doneView.revision, 8);
  assert.equal(doneView.completed_at_ms, 5000);
  assert.deepEqual(plain(JSON.parse(env.texts[env.texts.length - 1])), {
    final_summary: env.acceptanceContext.acceptance_note,
    status: "done",
    item_id: ITEM,
    revision: 8,
    completed_at_ms: 5000
  });

  const countsAfterDone = clone(env.calls);
  const receiptAfterDone = env.stored.get("threadlane/closure-done-receipt.json");
  const textCount = env.texts.length;
  await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
  assert.deepEqual(plain(env.calls), plain(countsAfterDone), "validated Done makes no later network call");
  assert.equal(env.stored.get("threadlane/closure-done-receipt.json"), receiptAfterDone);
  assert.deepEqual(plain(JSON.parse(env.texts[env.texts.length - 1])), {
    final_summary: env.acceptanceContext.acceptance_note,
    status: "done",
    item_id: ITEM,
    revision: 8,
    completed_at_ms: 5000
  });
  assert.equal(env.texts.length, textCount + 1, "the matching retained Done is formatted purely");
});

test("ordinary closure client refuses stale, mixed, and failed-prep state before any effect", async () => {
  const env = makeGuideEnvironment();
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
  await env.runRecipe(1);
  const counts = clone(env.calls);
  const report = env.stored.get("threadlane/closure-report.json");
  const evidence = env.stored.get("threadlane/closure-evidence.json");
  const correlation = env.stored.get("threadlane/closure-read-correlation.json");
  env.stored.delete("threadlane/start-binding.json");
  await assert.rejects(
    env.runRecipe(1),
    /missing or changed retained context\/start\/binding.*fallback/,
    "missing binding refuses before a read effect"
  );
  await assert.rejects(
    env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
    /invalid closure capsule, context, or report.*start\/binding\/correlation.*original-receipt fallback/,
    "missing binding refuses before completion"
  );
  assert.deepEqual(plain(env.calls), plain(counts), "binding refusal performs no callback");
  assert.equal(env.stored.get("threadlane/closure-report.json"), report);
  assert.equal(env.stored.get("threadlane/closure-evidence.json"), evidence);
  assert.equal(env.stored.get("threadlane/closure-read-correlation.json"), correlation);
  assert.equal(env.stored.has("threadlane/closure-completion-intent.json"), false);
  assert.equal(env.calls.complete, 0);

  const failedPrep = makeGuideEnvironment();
  await failedPrep.runRecipe(0, {
    APPROVED_TASK_CONTEXT: clone(failedPrep.approvedContext)
  });
  await failedPrep.runRecipe(1);
  const failedCounts = clone(failedPrep.calls);
  const nextContext = clone(failedPrep.approvedContext);
  nextContext.start_args = {
    command_key: "failed-prep-key",
    item_id: "blg_failed_prep",
    expected_revision: 8,
    approval_id: "apr_failed_prep",
    task_id: "task-failed-prep"
  };
  let faults = 1;
  const faultTools = {
    ...failedPrep.tools,
    exec_command: async (args) => {
      if (faults > 0 && args.cmd.includes("closure-template")) {
        faults -= 1;
        failedPrep.calls.exec += 1;
        return { exit_code: 1, output: "one-shot closure-template fault" };
      }
      return failedPrep.tools.exec_command(args);
    }
  };
  await assert.rejects(
    failedPrep.runRecipe(0, { APPROVED_TASK_CONTEXT: nextContext }, faultTools),
    /canonical closure-template failed/,
    "failed prep must not mint a new binding"
  );
  assert.deepEqual(
    plain(JSON.parse(failedPrep.stored.get("threadlane/approved-task-context.json"))),
    plain(nextContext),
    "the new prep input is retained before the failed exec"
  );
  await assert.rejects(
    failedPrep.runRecipe(1),
    /missing or changed retained context\/start\/binding.*fallback/,
    "failed prep refuses stale mixed state"
  );
  assert.deepEqual(
    plain(failedPrep.calls),
    { ...failedCounts, exec: failedCounts.exec + 1 },
    "failed prep and the refusal perform no pull/ACK/result/inspect/complete"
  );
  assert.equal(failedPrep.stored.has("threadlane/closure-completion-intent.json"), false);
});

test("ordinary closure client never retries missing, error, or unknown completion replies", async () => {
  for (const [label, reply] of [
    ["missing", async () => undefined],
    ["thrown", async () => { throw new Error("lost completion reply"); }],
    ["error", async () => ({
      isError: true,
      content: [{ type: "text", text: "completion refused" }]
    })]
  ]) {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    let completeCalls = 0;
    env.tools.mcp__threadlane__backlog_worker_complete = async () => {
      completeCalls += 1;
      env.calls.complete += 1;
      assert.equal(
        env.stored.has("threadlane/closure-completion-intent.json"),
        true,
        "completion intent must be retained before the effect"
      );
      return reply();
    };
    if (label === "error") {
      await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
      const view = JSON.parse(env.texts[env.texts.length - 1]);
      assert.equal(view.status, "incompatible", "an error reply is never Done");
      assert.equal(Object.hasOwn(view, "final_summary"), false);
      assert.equal(env.stored.has("threadlane/closure-done-receipt.json"), true);
      assert.equal(env.stored.has("threadlane/closure-done-view.json"), false);
    } else {
      await assert.rejects(
        env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
        label === "missing"
          ? /missing completion reply.*no retry/
          : /completion callback failed.*no retry/,
        `${label}: no completion claim`
      );
      assert.equal(env.stored.has("threadlane/closure-done-receipt.json"), false, label);
    }
    assert.equal(completeCalls, 1, `${label}: exactly one completion attempt`);
    assert.equal(
      env.stored.has("threadlane/closure-completion-intent.json"),
      true,
      `${label}: intent is retained before the effect`
    );
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /no retry|not Done/,
      `${label}: a later call never retries`
    );
    assert.deepEqual(plain(env.calls), plain(counts), `${label}: no later callback`);
    assert.equal(completeCalls, 1, `${label}: still exactly one completion attempt`);
    assert.equal(env.stored.has("threadlane/closure-done-view.json"), false, label);
  }
});

test("ordinary closure client refuses corrupt or changed retained Done evidence", async () => {
  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    assert.equal(env.calls.complete, 1);
    updateStoredCompletionRecord(
      env.stored,
      "threadlane/closure-done-receipt.json",
      EXEC,
      (receipt) => {
        delete receipt.structuredContent.data.completion;
        receipt.content[0].text = JSON.stringify(receipt.structuredContent);
      }
    );
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /retained completion receipt is not Done.*no retry/,
      "a corrupt retained receipt is never reported as Done"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "corrupt receipt performs no later callback");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    updateStoredCompletionRecord(
      env.stored,
      "threadlane/closure-done-view.json",
      EXEC,
      (retained) => {
        retained.revision += 1;
      }
    );
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /retained Done view is corrupt or belongs to another attempt/,
      "a corrupt retained view is refused"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "corrupt view performs no later callback");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    assert.equal(env.calls.complete, 1);
    const receiptAfterDone = env.stored.get("threadlane/closure-done-receipt.json");
    const counts = clone(env.calls);
    const changedAcceptance = clone(env.acceptanceContext);
    changedAcceptance.command_key = "changed-semantic-key";
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: changedAcceptance }),
      /retained completion intent belongs to a different attempt/,
      "a changed semantic key cannot alter the original completion"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "changed semantic key makes no callback");
    assert.equal(
      env.stored.get("threadlane/closure-done-receipt.json"),
      receiptAfterDone,
      "changed semantic key preserves the original receipt"
    );
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    deleteStoredCompletionRecord(
      env.stored,
      "threadlane/closure-completion-intent.json",
      EXEC
    );
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /retained completion receipt lacks matching retained intent/,
      "a current receipt without its intent is refused"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "orphan receipt makes no callback");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    deleteStoredCompletionRecord(
      env.stored,
      "threadlane/closure-done-receipt.json",
      EXEC
    );
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /retained Done view has no matching receipt/,
      "an orphan current Done view is refused"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "orphan view makes no callback");
  }

  {
    const env = makeGuideEnvironment();
    await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
    await env.runRecipe(1);
    await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
    const intents = "threadlane/closure-completion-intent.json";
    const receipts = "threadlane/closure-done-receipt.json";
    const views = "threadlane/closure-done-view.json";
    const aIntent = plain(storedCompletionRecord(env.stored, intents, EXEC).record);
    const aView = plain(storedCompletionRecord(env.stored, views, EXEC).record);
    const brokenReceipt = plain(storedCompletionRecord(env.stored, receipts, EXEC).record);
    delete brokenReceipt.structuredContent.data.completion.execution;
    brokenReceipt.content[0].text = JSON.stringify(brokenReceipt.structuredContent);
    env.stored.set(intents, JSON.stringify(aIntent));
    env.stored.set(receipts, JSON.stringify(brokenReceipt));
    env.stored.set(views, JSON.stringify(aView));
    const counts = clone(env.calls);
    await assert.rejects(
      env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
      /ambiguous retained completion state.*original-receipt fallback/,
      "unresolvable legacy identity uses the original-receipt fallback"
    );
    assert.deepEqual(plain(env.calls), plain(counts), "ambiguous legacy state makes no callback");
  }
});

test("ordinary closure client isolates completion records by original execution", async () => {
  const documents = [
    path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md"),
    path.join(here, "../../../docs/ORDINARY-WORKER-CLOSURE.md")
  ];
  const secondExecution = makeSecondExecution();
  const secondResult = makeResultForExecution(
    secondExecution,
    "b".repeat(64),
    "c".repeat(64)
  );
  const intents = "threadlane/closure-completion-intent.json";
  const receipts = "threadlane/closure-done-receipt.json";
  const views = "threadlane/closure-done-view.json";
  const done = (completedAtMs) => ({
    status: "done",
    item_id: ITEM,
    revision: 8,
    completed_at_ms: completedAtMs
  });

  for (const documentPath of documents) {
    const label = path.basename(documentPath);
    for (const scenario of ["client", "legacy", "legacy-no-intent"]) {
      const env = makeGuideEnvironment(documentPath);
      installGuideCompletionClock(env, [5000, 6000]);
      await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
      await env.runRecipe(1);
      await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
      assert.equal(env.calls.complete, 1, `${label}/${scenario}: A completes once`);

      const aIntent = plain(storedCompletionRecord(env.stored, intents, EXEC).record);
      const aReceipt = plain(storedCompletionRecord(env.stored, receipts, EXEC).record);
      const aView = plain(storedCompletionRecord(env.stored, views, EXEC).record);
      assert.equal(aView.completed_at_ms, 5000, `${label}/${scenario}: A time`);

      if (scenario !== "client") {
        env.stored.set(intents, JSON.stringify(aIntent));
        env.stored.set(receipts, JSON.stringify(aReceipt));
        env.stored.set(views, JSON.stringify(aView));
        if (scenario === "legacy-no-intent") {
          env.stored.delete(intents);
          const counts = clone(env.calls);
          await assert.rejects(
            env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
            /retained completion receipt lacks matching retained intent/,
            `${label}/${scenario}: current legacy receipt without intent refuses`
          );
          assert.deepEqual(plain(env.calls), plain(counts), `${label}/${scenario}: no callback`);
        } else {
          const legacyReceipt = env.stored.get(receipts);
          const counts = clone(env.calls);
          const textCount = env.texts.length;
          await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
          assert.deepEqual(plain(env.calls), plain(counts), `${label}/${scenario}: pure Done`);
          assert.equal(env.stored.get(receipts), legacyReceipt, `${label}/${scenario}: legacy receipt kept`);
          assert.deepEqual(
            plain(JSON.parse(env.texts[env.texts.length - 1])),
            {...done(5000), final_summary: env.acceptanceContext.acceptance_note},
            `${label}/${scenario}: matching legacy current Done is formatted purely`
          );
          assert.equal(env.texts.length, textCount + 1, `${label}/${scenario}: one text`);
        }
      }

      const secondContext = makeContextForExecution(
        env,
        secondExecution,
        "fresh-second-command-key"
      );
      configureGuideAttempt(env, secondExecution, secondResult);
      await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(secondContext) });
      const staleCounts = clone(env.calls);
      await assert.rejects(
        env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
        /invalid closure capsule, context, or report.*original-receipt fallback/,
        `${label}/${scenario}: stale B without a read refuses`
      );
      assert.deepEqual(plain(env.calls), plain(staleCounts), `${label}/${scenario}: stale B no effect`);
      assert.equal(env.calls.complete, 1, `${label}/${scenario}: stale B never completes`);

      await env.runRecipe(1);
      assert.equal(
        JSON.parse(env.stored.get("threadlane/closure-report.json")).status,
        "ready_for_semantic_review",
        `${label}/${scenario}: fresh B read is ready`
      );
      await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
      assert.equal(env.calls.complete, 2, `${label}/${scenario}: fresh B completes exactly once`);
      const bReceipt = plain(storedCompletionRecord(
        env.stored,
        receipts,
        secondExecution.execution_ref
      ).record);
      const bView = plain(storedCompletionRecord(
        env.stored,
        views,
        secondExecution.execution_ref
      ).record);
      assert.equal(
        bReceipt.structuredContent.data.completion.item.completed_at_ms,
        6000,
        `${label}/${scenario}: B time`
      );
      assert.notDeepEqual(bReceipt, aReceipt, `${label}/${scenario}: B receipt is distinct`);
      const bDone = {
        status: "done",
        item_id: secondExecution.item_id,
        revision: 8,
        completed_at_ms: 6000
      };
      assert.deepEqual(bView, {
        ...bDone,
        schema_version: 1,
        template_version: env.template.version,
        source_sha256: env.template.source_sha256,
        acceptance: plain(env.acceptanceContext),
        args: plain(env.completeArgsSeen[1]),
        start: plain(JSON.parse(env.stored.get("threadlane/start-receipt.json")))
      }, `${label}/${scenario}: B has its own validated view`);
      if (scenario === "client") {
        assert.deepEqual(
          plain(storedCompletionRecord(env.stored, receipts, EXEC).record),
          aReceipt,
          `${label}/${scenario}: A raw receipt preserved`
        );
        assert.deepEqual(
          plain(storedCompletionRecord(env.stored, views, EXEC).record),
          aView,
          `${label}/${scenario}: A view preserved`
        );
        assert.equal(completionRecordCount(env.stored, receipts), 2, `${label}/${scenario}: A and B`);
      } else {
        const receiptState = storedCompletionState(env.stored, receipts);
        assert.deepEqual(plain(receiptState.legacy), aReceipt, `${label}/${scenario}: legacy A receipt preserved`);
        assert.deepEqual(
          plain(storedCompletionState(env.stored, views).legacy),
          aView,
          `${label}/${scenario}: legacy A view preserved`
        );
        assert.equal(
          receiptState.legacy.structuredContent.data.completion.item.completed_at_ms,
          5000,
          `${label}/${scenario}: legacy A time preserved`
        );
        assert.equal(completionRecordCount(env.stored, receipts), 1, `${label}/${scenario}: only B current`);
        if (scenario === "legacy") {
          assert.deepEqual(
            plain(storedCompletionState(env.stored, intents).legacy),
            aIntent,
            `${label}/${scenario}: legacy A intent preserved`
          );
        }
      }

      const countsAfterB = clone(env.calls);
      const receiptAfterB = env.stored.get(receipts);
      const textCountAfterB = env.texts.length;
      await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
      assert.deepEqual(plain(env.calls), plain(countsAfterB), `${label}/${scenario}: repeated B no callback`);
      assert.equal(env.stored.get(receipts), receiptAfterB, `${label}/${scenario}: repeated B zero effect`);
      assert.deepEqual(
        plain(JSON.parse(env.texts[env.texts.length - 1])),
        {...bDone, final_summary: env.acceptanceContext.acceptance_note},
        `${label}/${scenario}: repeated B formats its own Done`
      );
      assert.equal(env.texts.length, textCountAfterB + 1, `${label}/${scenario}: one extra text`);
    }
  }
});

test("ordinary closure client preserves legacy current unknown intent without retry", async () => {
  const intents = "threadlane/closure-completion-intent.json";
  const receipts = "threadlane/closure-done-receipt.json";
  const views = "threadlane/closure-done-view.json";
  const env = makeGuideEnvironment();
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
  await env.runRecipe(1);
  env.tools.mcp__threadlane__backlog_worker_complete = async () => {
    env.calls.complete += 1;
    return undefined;
  };
  await assert.rejects(
    env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
    /missing completion reply.*no retry/
  );
  const aIntent = plain(storedCompletionRecord(env.stored, intents, EXEC).record);
  env.stored.set(intents, JSON.stringify(aIntent));
  env.stored.delete(receipts);
  env.stored.delete(views);
  const legacyIntent = env.stored.get(intents);
  const counts = clone(env.calls);
  await assert.rejects(
    env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) }),
    /completion outcome is unknown.*no retry/,
    "matching legacy current unknown intent is never replayed"
  );
  assert.deepEqual(plain(env.calls), plain(counts), "legacy current unknown performs no callback");
  assert.equal(env.stored.get(intents), legacyIntent, "legacy current intent is preserved");

  const secondExecution = makeSecondExecution();
  const secondResult = makeResultForExecution(
    secondExecution,
    "b".repeat(64),
    "c".repeat(64)
  );
  installGuideCompletionClock(env, [6000]);
  const secondContext = makeContextForExecution(
    env,
    secondExecution,
    "fresh-second-command-key"
  );
  configureGuideAttempt(env, secondExecution, secondResult);
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(secondContext) });
  await env.runRecipe(1);
  assert.equal(
    JSON.parse(env.stored.get("threadlane/closure-report.json")).status,
    "ready_for_semantic_review",
    "fresh B read is ready"
  );
  await env.runRecipe(2, { ACCEPTANCE_CONTEXT: clone(env.acceptanceContext) });
  assert.equal(env.calls.complete, 2, "fresh B completes despite legacy foreign unknown A");
  assert.deepEqual(
    plain(JSON.parse(env.stored.get(intents)).legacy),
    aIntent,
    "legacy foreign unknown A stays preserved"
  );
  assert.equal(
    storedCompletionRecord(env.stored, receipts, secondExecution.execution_ref)
      .record.structuredContent.data.completion.item.completed_at_ms,
    6000,
    "B keeps its own receipt"
  );
});

test("the actual notice and original start must match before any event is pulled", async () => {
  const env = makeGuideEnvironment();
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
  const afterPrep = clone(env.calls);
  for (const id of [undefined, "", "apr_bad", "apr_" + "f".repeat(32)]) {
    await assert.rejects(env.runRecipe(1, { NOTICE_APPROVAL_ID: id }), /invalid approval_id|identity mismatch/);
    assert.deepEqual(env.calls, afterPrep);
  }
  for (const [field, value] of [["item_id", "foreign-item"], ["task_id", "foreign-task"],
                               ["expected_revision", env.approvedContext.start_args.expected_revision + 1]]) {
    const input = clone(env.approvedContext);
    input.start_args[field] = value;
    const binding = JSON.parse(env.stored.get("threadlane/start-binding.json"));
    env.stored.set("threadlane/approved-task-context.json", JSON.stringify(input));
    env.stored.set("threadlane/start-binding.json", JSON.stringify({input, start: binding.start}));
    await assert.rejects(env.runRecipe(1), /identity mismatch/);
    assert.deepEqual(env.calls, afterPrep);
  }
});

test("a later notice acknowledges its own batch and retains the accepted delivery", async () => {
  const env = makeGuideEnvironment();
  await env.runRecipe(0, { APPROVED_TASK_CONTEXT: clone(env.approvedContext) });
  await env.runRecipe(1);
  await env.runRecipe(2, { ACCEPTANCE_CONTEXT: env.acceptanceContext });
  const before = new Map(env.stored);
  const result = makeResultData();
  result.item_snapshot.status = "done";
  result.item_snapshot.revision = 8;
  await env.runRecipe(1, {}, {
    ...env.tools,
    mcp__threadlane__backlog_worker_result: async () => env.sdk("backlog_worker_result", result)
  });
  assert.equal(env.calls.ack, 2);
  const prefix = "threadlane/closure-after-done/" + START_EXECUTION.execution_ref;
  const later = JSON.parse(env.stored.get(prefix + "/1"));
  assert.equal(later.evidence.acks.length, 1);
  assert.equal(later.evidence.acks[0].structuredContent.data.ack.batch_id, PAGE_TWO.batch.batch_id);
  const malformed = {isError: true, content: [{type: "text", text: "ACK outcome unknown"}]};
  await env.runRecipe(1, {}, {...env.tools, mcp__threadlane__approved_ack_events: async () => malformed});
  assert.deepEqual(JSON.parse(env.stored.get(prefix + "/2")).evidence.acks, [malformed]);
  assert.equal(JSON.parse(env.stored.get(prefix + "/count")), 2);
  for (const key of ["closure-report", "closure-evidence", "closure-read-correlation", "closure-done-receipt"]) {
    const path = "threadlane/" + key + ".json";
    assert.equal(env.stored.get(path), before.get(path));
  }
  await env.runRecipe(2, { ACCEPTANCE_CONTEXT: env.acceptanceContext });
  assert.equal(env.calls.complete, 1);
  assert.equal(JSON.parse(env.texts.at(-1)).status, "done");
});

test("closure client wrappers stay compact, identical, and within document byte budgets", () => {
  const guidePath = path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md");
  const closurePath = path.join(here, "../../../docs/ORDINARY-WORKER-CLOSURE.md");
  const quickstartPath = path.join(here, "../../../docs/ORDINARY-WORKER-QUICKSTART.md");
  const guide = fs.readFileSync(guidePath, "utf8");
  const quickstart = fs.readFileSync(quickstartPath, "utf8");
  assert.ok(Buffer.byteLength(guide, "utf8") <= 16384, "guide stays within 16384 UTF-8 bytes");
  assert.ok(Buffer.byteLength(quickstart, "utf8") <= 4096, "quickstart stays within 4096 UTF-8 bytes");

  const readBlocks = (p) => [...fs.readFileSync(p, "utf8").matchAll(/```js\n([\s\S]*?)```/g)].map((m) => m[1]);
  const guideBlocks = readBlocks(guidePath);
  const closureBlocks = readBlocks(closurePath);
  assert.equal(guideBlocks.length, 3);
  assert.equal(closureBlocks.length, 3);
  assert.equal(guideBlocks[1], closureBlocks[1], "read wrappers are identical");
  assert.equal(guideBlocks[2], closureBlocks[2], "complete wrappers are identical");
  assert.ok(!guideBlocks[1].includes("APPROVED_TASK_CONTEXT"));
  assert.ok(!guideBlocks[1].includes("ACCEPTANCE_CONTEXT"));
  assert.equal((guideBlocks[2].match(/ACCEPTANCE_CONTEXT/g) || []).length, 1);
  assert.ok(guideBlocks[1].includes("createOrdinaryClosureClient"));
  assert.ok(guideBlocks[2].includes("createOrdinaryClosureClient"));
  assert.ok(!guideBlocks[1].includes("backlog_worker_complete"));
  assert.ok(!guideBlocks[2].includes("backlog_worker_start"));
  for (const [label, block] of [
    ["read", guideBlocks[1]],
    ["complete", guideBlocks[2]]
  ]) {
    assert.ok(
      Buffer.byteLength(block, "utf8") <= 2200,
      `${label} program body stays within 2200 UTF-8 bytes, got ${Buffer.byteLength(block, "utf8")}`
    );
  }
});

function sdkStart(value) {
  return {
    isError: false,
    structuredContent: value,
    content: [{ type: "text", text: JSON.stringify(value) }]
  };
}

test("doneView rejects changed immutable execution fields and dispatch digest", async () => {
  const harness = makeHarness();
  const { report } = await readResult(harness);
  assert.equal(report.result.acceptance_condition_digest, "1".repeat(64));
  assert.equal(report.result.delivery_description_digest, "2".repeat(64));
  assert.equal(report.result.reservation_revision, 5);
  assert.equal(report.result.reserved_at_ms, 1000);
  assert.equal(report.result.execution_binding_digest, "3".repeat(64));

  const immutableMismatches = [
    ["acceptance_condition_digest", (execution) => {
      execution.acceptance_condition_digest = "f".repeat(64);
    }],
    ["delivery_description_digest", (execution) => {
      execution.delivery_description_digest = "f".repeat(64);
    }],
    ["reservation_revision", (execution) => {
      execution.reservation_revision = 6;
    }],
    ["reserved_at_ms", (execution) => {
      execution.reserved_at_ms = 1001;
    }],
    ["execution_binding_digest", (execution) => {
      execution.dispatch_receipt.execution_binding_digest = "f".repeat(64);
    }]
  ];
  for (const [field, mutate] of immutableMismatches) {
    const receipt = makeDoneReceipt(report);
    mutate(receipt.data.completion.execution);
    assert.equal(
      harness.closure.doneView({ receipt, report }).reason,
      field === "execution_binding_digest" ? "dispatch_mismatch" : "immutable_execution_mismatch",
      field
    );
  }
  {
    const receipt = makeDoneReceipt(report);
    delete receipt.data.completion.execution.dispatch_receipt.execution_binding_digest;
    assert.equal(harness.closure.doneView({ receipt, report }).status, "incompatible");
  }
  {
    const receipt = makeDoneReceipt(report);
    receipt.data.completion.execution.extra_immutable_field = "not compared";
    assert.equal(harness.closure.doneView({ receipt, report }).status, "done");
  }
});

function makeDoneReceipt(
  report,
  completedAtMs = Math.max(
    5000,
    report.result.completed_at_ms,
    report.result.started_at_ms
  )
) {
  const execution = {
    execution_ref: report.mapping.execution_ref,
    binding_id: report.mapping.control_binding_id,
    item_id: report.mapping.item_id,
    authority: {
      approval_id: report.mapping.approval_id,
      subject_digest: report.mapping.subject_digest,
      task_id: report.mapping.task_id,
      task_digest: report.mapping.task_digest
    },
    acceptance_condition_digest: report.result.acceptance_condition_digest,
    delivery_description_digest: report.result.delivery_description_digest,
    reservation_revision: report.result.reservation_revision,
    reserved_at_ms: report.result.reserved_at_ms,
    dispatch_receipt: {
      session_binding_id: report.mapping.session_binding_id,
      run_id: report.mapping.run_id,
      worker_id: report.mapping.worker_id,
      profile_id: report.mapping.profile_id,
      ...(report.result.execution_binding_digest === undefined
        ? {}
        : { execution_binding_digest: report.result.execution_binding_digest })
    },
    attached_at_ms: 1100,
    outcome: {
      outcome: "success",
      note: "verified terminal worker success",
      result_digest: report.result.result_digest,
      recorded_at_ms: 3000
    },
    completion: {
      result_digest: report.result.result_digest,
      delivery_digest: report.result.delivery_digest,
      acceptance_note: "semantic acceptance",
      acceptance_evidence: "bounded evidence",
      completed_at_ms: completedAtMs
    }
  };
  const item = {
    item_id: report.mapping.item_id,
    binding_id: report.mapping.control_binding_id,
    revision: report.result.current_revision + 1,
    title: "ordinary task",
    acceptance_condition: "bounded condition",
    delivery_description: "bounded delivery",
    kind: "worker",
    status: "done",
    blocked_reason: null,
    current_execution_ref: report.mapping.execution_ref,
    completion_note: "semantic acceptance",
    completion_evidence: "bounded evidence",
    completed_at_ms: completedAtMs,
    created_at_ms: 900,
    updated_at_ms: completedAtMs
  };
  return {
    untrusted_data: true,
    binding_id: report.mapping.control_binding_id,
    tool: "backlog_worker_complete",
    data: {
      completion: { item, execution },
      delivery: makeDelivery({
        approval_id: report.mapping.approval_id,
        subject_digest: report.mapping.subject_digest,
        task_id: report.mapping.task_id,
        task_digest: report.mapping.task_digest,
        binding_id: report.mapping.control_binding_id,
        session_binding_id: report.mapping.session_binding_id,
        run_id: report.mapping.run_id,
        worker_id: report.mapping.worker_id,
        profile_id: report.mapping.profile_id,
        base_commit: report.scope.base_commit,
        head_commit: report.scope.base_commit,
        result_digest: report.result.result_digest,
        delivery_digest: report.result.delivery_digest
      })
    }
  };
}

// The public notice recipe is tested independently of the legacy wrapper above.
function makeRecoveredGuide() {
  const env = makeGuideEnvironment();
  const document = fs.readFileSync(path.join(here, "../../../docs/MCP-OPERATING-GUIDE.md"), "utf8");
  const block = [...document.matchAll(/```js\n([\s\S]*?)```/g)][1][1];
  let context = {
    execution: clone(START_EXECUTION),
    scope: {base_commit: BASE, requested_model: MODEL, requested_effort: EFFORT, write_paths: [WRITE_PATHS[0]]},
    item_snapshot: {item_id: ITEM, revision: START_EXECUTION.reservation_revision, status: "doing", current_execution_ref: EXEC}
  };
  let override = null;
  let lookups = 0;
  const tools = {...env.tools,
    mcp__threadlane__backlog_worker_notice_context: async ({approval_id}) => {
      lookups += 1;
      assert.equal(approval_id, APPROVAL);
      return override || env.sdk("backlog_worker_notice_context", clone(context));
    }
  };
  const run = async (activeTools = tools, vars = {}) => vm.runInNewContext(
    `(async () => {\n${block}\n})()`, {
      tools: activeTools,
      load: (key) => env.stored.has(key) ? JSON.parse(env.stored.get(key)) : undefined,
      store: async (key, value) => { env.stored.set(key, JSON.stringify(value)); },
      text: (value) => env.texts.push(value),
      NOTICE_APPROVAL_ID: APPROVAL,
      NOTICE_CLI_PACKAGE: "/trusted",
      NOTICE_SOURCE_SHA256: SOURCE_SHA256,
      ...vars
    }
  );
  return {env, tools, run, setContext: (value) => { context = value; }, getContext: () => clone(context),
    setOverride: (value) => {override = value;}, getLookups: () => lookups};
}

const TEMPORARY_KEYS = ["approved-task-context", "closure-template", "start-receipt", "start-binding"].map(
  (name) => `threadlane/${name}.json`
);

test("public notice recipe recovers all missing state from the original scoped record", async () => {
  const fixture = makeRecoveredGuide();
  await fixture.env.runRecipe(0, {APPROVED_TASK_CONTEXT: clone(fixture.env.approvedContext)});
  for (const key of TEMPORARY_KEYS) fixture.env.stored.set(key, "null");
  const before = clone(fixture.env.calls);
  await fixture.run();
  const view = JSON.parse(fixture.env.texts.at(-1));
  assert.equal(view.status, "ready_for_semantic_review");
  assert.equal(view.authority_granted, false);
  assert.equal(fixture.getLookups(), 1);
  assert.equal(fixture.env.calls.start, before.start);
  assert.equal(fixture.env.calls.pull, before.pull + 1);
  assert.equal(fixture.env.calls.ack, before.ack + 1);
  assert.equal(fixture.env.calls.result, before.result + 1);
  assert.equal(fixture.env.calls.inspect, before.inspect + 1);
  for (const key of TEMPORARY_KEYS) assert.notEqual(JSON.parse(fixture.env.stored.get(key)), null, key);
  await fixture.env.runRecipe(2, {ACCEPTANCE_CONTEXT: clone(fixture.env.acceptanceContext)});
  assert.equal(fixture.env.calls.complete, 1, "semantic acceptance remains separate");
  assert.equal(JSON.parse(fixture.env.texts.at(-1)).status, "done");
});

test("recovered notice rejects conflicting frozen state and never clobbers it", async () => {
  for (const [label, change] of [
    ["foreign approval", (ctx) => {ctx.execution.authority.approval_id = "apr_" + "f".repeat(32);}],
    ["foreign control", (ctx) => {ctx.execution.binding_id = "bnd_foreign";}],
    ["stale ref", (ctx) => {ctx.item_snapshot.current_execution_ref = "wbe_stale";}],
    ["model", (ctx) => {ctx.scope.requested_model = "other/model";}],
    ["base", (ctx) => {ctx.scope.base_commit = "f".repeat(40);}],
    ["path", (ctx) => {ctx.scope.write_paths = ["foreign.txt"]; }],
    ["effort", (ctx) => {ctx.scope.requested_effort = "low";}]
  ]) {
    const fixture = makeRecoveredGuide();
    await fixture.env.runRecipe(0, {APPROVED_TASK_CONTEXT: clone(fixture.env.approvedContext)});
    const context = fixture.getContext(); change(context); fixture.setContext(context);
    const before = new Map(fixture.env.stored); const calls = clone(fixture.env.calls);
    await assert.rejects(fixture.run(), /mismatch|conflicts/, label);
    assert.deepEqual(fixture.env.stored, before, `${label}: nothing overwritten`);
    assert.equal(fixture.env.calls.pull, calls.pull, `${label}: no pull`);
    assert.equal(fixture.env.calls.ack, calls.ack, `${label}: no ACK`);
    assert.equal(fixture.env.calls.result, calls.result, `${label}: no result`);
  }
  const fixture = makeRecoveredGuide();
  await fixture.env.runRecipe(0, {APPROVED_TASK_CONTEXT: clone(fixture.env.approvedContext)});
  const badTemplate = JSON.parse(fixture.env.stored.get(TEMPORARY_KEYS[1]));
  badTemplate.javascript += "\n// changed cached source";
  fixture.env.stored.set(TEMPORARY_KEYS[1], JSON.stringify(badTemplate));
  const before = new Map(fixture.env.stored);
  await assert.rejects(fixture.run(), /retained template conflicts/);
  assert.deepEqual(fixture.env.stored, before);
  assert.equal(fixture.env.calls.pull, 0);
});

test("completed notice verifies durable completion and ACKs only real late events", async () => {
  const fixture = makeRecoveredGuide();
  await fixture.env.runRecipe(0, {APPROVED_TASK_CONTEXT: clone(fixture.env.approvedContext)});
  await fixture.run();
  await fixture.env.runRecipe(2, {ACCEPTANCE_CONTEXT: clone(fixture.env.acceptanceContext)});
  const report = JSON.parse(fixture.env.stored.get("threadlane/closure-report.json"));
  const completion = makeDoneReceipt(report).data.completion;
  const context = fixture.getContext();
  context.execution = clone(completion.execution);
  context.item_snapshot = {item_id: ITEM, revision: completion.item.revision, status: "done", current_execution_ref: EXEC};
  context.completion = clone(completion);
  fixture.setContext(context);
  for (const key of TEMPORARY_KEYS) fixture.env.stored.set(key, "null");
  const counts = clone(fixture.env.calls);
  const empty = makePage({pageNumber: 5, events: [], ackedSeq: 3});
  await fixture.run({...fixture.tools, mcp__threadlane__approved_pull_events: async () => fixture.env.sdk("approved_pull_events", empty)});
  let view = JSON.parse(fixture.env.texts.at(-1));
  assert.equal(view.status, "done");
  assert.equal(view.events.status, "verified");
  assert.equal(view.events.acked_seq, 3);
  assert.deepEqual(plain(view.events.batches), []);
  assert.equal(fixture.env.calls.ack, counts.ack, "empty batch is not ACKed");
  assert.equal(fixture.env.calls.result, counts.result);
  assert.equal(fixture.env.calls.inspect, counts.inspect);
  assert.equal(fixture.env.calls.complete, counts.complete);
  assert.equal(fixture.env.calls.start, counts.start);
  const late = makePage({pageNumber: 6, events: [makeEvent(4)], ackedSeq: 3});
  let actualAck;
  await fixture.run({...fixture.tools,
    mcp__threadlane__approved_pull_events: async () => fixture.env.sdk("approved_pull_events", late),
    mcp__threadlane__approved_ack_events: async (args) => {actualAck = args; return fixture.env.sdk("approved_ack_events", makeAckData(args.batch_id, 4, true));}
  });
  view = JSON.parse(fixture.env.texts.at(-1));
  assert.equal(view.status, "done");
  assert.equal(view.events.acked_seq, 4);
  assert.equal(actualAck.batch_id, late.batch.batch_id);
  assert.equal(fixture.env.calls.complete, counts.complete);
  assert.equal(fixture.env.calls.result, counts.result);
  assert.ok(fixture.env.stored.has(`threadlane/closure-after-done/${EXEC}/${late.batch.batch_id}/ack`));
  await fixture.run({...fixture.tools, mcp__threadlane__approved_pull_events: async () => {throw new Error("host stopped");}});
  view = JSON.parse(fixture.env.texts.at(-1));
  assert.equal(view.status, "done");
  assert.equal(view.events.status, "unverified");
  assert.equal(view.events.acked_seq, null);
  const tampered = clone(context); tampered.completion.execution.completion.delivery_digest = "f".repeat(64); fixture.setContext(tampered);
  await assert.rejects(fixture.run(), /durable completion/);
});

test("only authenticated no_guarded_execution permits legacy handling; unknown refuses", async () => {
  const fixture = makeRecoveredGuide();
  const error = (code) => ({isError: true, structuredContent: {tool: "backlog_worker_notice_context", binding_id: CONTROL, error: {code}}});
  fixture.setOverride(error("no_guarded_execution"));
  await fixture.run();
  assert.equal(JSON.parse(fixture.env.texts.at(-1)).status, "legacy_events");
  for (const code of ["worker_effect_unknown", "foreign_binding", "ambiguous_execution", "stale_execution"]) {
    fixture.setOverride(error(code));
    await assert.rejects(fixture.run(), /refused; no legacy fallback/);
  }
  assert.equal(fixture.env.calls.pull, 0);
  assert.equal(fixture.env.calls.complete, 0);
});

test("blocked original drains validated events without completion", async () => {
  const fixture = makeRecoveredGuide();
  const ctx = fixture.getContext();
  ctx.item_snapshot.status = "blocked";
  ctx.execution.outcome = {outcome: "failure", result_digest: null};
  fixture.setContext(ctx);
  await fixture.run();
  const view = JSON.parse(fixture.env.texts.at(-1));
  assert.equal(view.status, "blocked");
  assert.equal(view.events.status, "verified");
  assert.equal(fixture.env.calls.ack, 1);
  assert.equal(fixture.env.calls.result, 0);
  assert.equal(fixture.env.calls.inspect, 0);
  assert.equal(fixture.env.calls.complete, 0);
});

test("live concurrent duplicate notices accept an already-ACKed real batch without a second completion", async () => {
  const createClient = vm.runInNewContext(`${source}\n;createOrdinaryClosureClient`, Object.create(null));
  const memory = new Map();
  const template = {schema_version: 1, tool: "threadlane closure-template", version: "2026-11-19.v1",
    source_path: "skills/threadlane/references/ordinary-closure.js", source_sha256: SOURCE_SHA256,
    factory_name: "createOrdinaryClosure", javascript: source};
  const snapshot = {execution: clone(START_EXECUTION),
    scope: {base_commit: BASE, requested_model: MODEL, requested_effort: EFFORT, write_paths: [WRITE_PATHS[0]]},
    item_snapshot: {item_id: ITEM, revision: 5, status: "doing", current_execution_ref: EXEC}};
  let count = 0, release;
  const gate = new Promise((resolve) => {release = resolve;});
  let acks = 0, results = 0, inspections = 0, completions = 0;
  const client = () => createClient({
    load: (key) => memory.get(key), store: async (key, value) => memory.set(key, clone(value)),
    bootstrap: {binary: "/trusted/bin/threadlane", expected_source_sha256: SOURCE_SHA256, template},
    recover: async () => envelope("backlog_worker_notice_context", clone(snapshot)),
    pull: async () => {
      const n = ++count;
      if (n === 2) release();
      await gate;
      return envelope("approved_pull_events", makePage({pageNumber: n, events: [makeEvent(1)], ackedSeq: 0}));
    },
    ack: async (args) => envelope("approved_ack_events", makeAckData(args.batch_id, 1, ++acks === 1)),
    result: async () => {results += 1; return envelope("backlog_worker_result", makeResultData());},
    inspect: async () => {inspections += 1; return makeInspection([WRITE_PATHS[0]]);},
    complete: async () => {completions += 1; throw new Error("unexpected");}
  });
  const [first, second] = await Promise.all([client().readNotice(APPROVAL), client().readNotice(APPROVAL)]);
  assert.equal(first.status, "ready_for_semantic_review");
  assert.equal(second.status, "ready_for_semantic_review");
  assert.equal(acks, 2);
  assert.equal(results, 2);
  assert.equal(inspections, 2);
  assert.equal(completions, 0, "callbacks do not accept or complete");
  assert.deepEqual([first.batches[0].advanced, second.batches[0].advanced].sort(), [false, true]);
});

test("duplicate notices on one client share the same in-flight read", async () => {
  const createClient = vm.runInNewContext(`${source}\n;createOrdinaryClosureClient`, Object.create(null));
  const memory = new Map(); const template = {schema_version: 1, tool: "threadlane closure-template", version: "2026-11-19.v1",
    source_path: "skills/threadlane/references/ordinary-closure.js", source_sha256: SOURCE_SHA256,
    factory_name: "createOrdinaryClosure", javascript: source};
  let lookups = 0, pulls = 0;
  const client = createClient({load: (k) => memory.get(k), store: async (k, v) => memory.set(k, clone(v)),
    bootstrap: {binary: "/trusted/bin/threadlane", expected_source_sha256: SOURCE_SHA256, template},
    recover: async () => {lookups += 1; return envelope("backlog_worker_notice_context", {execution: clone(START_EXECUTION),
      scope: {base_commit: BASE, requested_model: MODEL, requested_effort: EFFORT, write_paths: [WRITE_PATHS[0]]},
      item_snapshot: {item_id: ITEM, revision: 5, status: "doing", current_execution_ref: EXEC}});},
    pull: async () => {pulls += 1; return envelope("approved_pull_events", PAGE_TWO);},
    ack: async (a) => envelope("approved_ack_events", makeAckData(a.batch_id, 3, true)),
    result: async () => envelope("backlog_worker_result", makeResultData()),
    inspect: async () => makeInspection([WRITE_PATHS[0]])});
  const [a,b] = await Promise.all([client.readNotice(APPROVAL), client.readNotice(APPROVAL)]);
  assert.equal(a.status, "ready_for_semantic_review");
  assert.equal(b.status, "ready_for_semantic_review");
  assert.equal(lookups, 1); assert.equal(pulls, 1);
});
