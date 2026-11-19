// Threadlane ordinary Worker callback closure template (2026-11-19.v1).
//
// This is trusted packaged source, not event/provider text. It uses standard
// language builtins only, with no dynamic loading, network, external commands,
// filesystem access, or ambient tools. The real client must evaluate only this
// immutable source and inject the four callbacks below.
//
// `read` performs one finite closure composition and returns:
//   { report, evidence }
// where `report` is the bounded printable report and `evidence` retains every
// full actual start/page/ACK/result/inspector DTO immediately on receipt, before
// validation, for private storage. It enforces cross-page continuity against the
// retained verified ACK cursor, keeps late receipts without starting new effects,
// and accepts only a clean physical `exec_command` wrapper. A successful report
// status is only `ready_for_semantic_review`; it never grants completion,
// rejection, acceptance, or authority.
//
// `doneView` is a pure formatter for a separately obtained successful guarded
// Done receipt. It returns only bounded status/item/revision/time fields.

"use strict";

var CREATE_ORDINARY_CLOSURE_VERSION = "2026-11-19.v1";
var ORDINARY_CLOSURE_PAGE_LIMIT = 20;
var ORDINARY_CLOSURE_MAX_WRITE_PATHS = 128;
var ORDINARY_CLOSURE_MAX_BATCH_BYTES = 49152;
var ORDINARY_CLOSURE_CLIENT_CONTEXT_KEY = "threadlane/approved-task-context.json";
var ORDINARY_CLOSURE_CLIENT_TEMPLATE_KEY = "threadlane/closure-template.json";
var ORDINARY_CLOSURE_CLIENT_START_KEY = "threadlane/start-receipt.json";
var ORDINARY_CLOSURE_CLIENT_BINDING_KEY = "threadlane/start-binding.json";
var ORDINARY_CLOSURE_CLIENT_EVIDENCE_KEY = "threadlane/closure-evidence.json";
var ORDINARY_CLOSURE_CLIENT_REPORT_KEY = "threadlane/closure-report.json";
var ORDINARY_CLOSURE_CLIENT_CORRELATION_KEY = "threadlane/closure-read-correlation.json";
var ORDINARY_CLOSURE_CLIENT_INTENT_KEY = "threadlane/closure-completion-intent.json";
var ORDINARY_CLOSURE_CLIENT_DONE_RECEIPT_KEY = "threadlane/closure-done-receipt.json";
var ORDINARY_CLOSURE_CLIENT_DONE_VIEW_KEY = "threadlane/closure-done-view.json";
var ORDINARY_CLOSURE_CLIENT_COMPLETION_STATE_TOOL = "threadlane closure-client completion";

function createOrdinaryClosure(callbacks) {
  if (!isPlainObject(callbacks)) {
    throw new TypeError("createOrdinaryClosure requires a callbacks object");
  }
  var pull = callbacks.pull;
  var ack = callbacks.ack;
  var result = callbacks.result;
  var inspect = callbacks.inspect;
  var now = callbacks.now;
  var allowDuplicateAck = callbacks.allow_duplicate_ack === true;
  for (var index = 0; index < 4; index += 1) {
    var named = [
      ["pull", pull],
      ["ack", ack],
      ["result", result],
      ["inspect", inspect]
    ][index];
    if (typeof named[1] !== "function") {
      throw new TypeError("callback " + named[0] + " must be a function");
    }
  }
  if (now !== undefined && typeof now !== "function") {
    throw new TypeError("callback now must be a function when supplied");
  }
  var clock = now === undefined ? function () { return Date.now(); } : now;

  async function read(input) {
    var evidence = {
      schema_version: 1,
      template_version: CREATE_ORDINARY_CLOSURE_VERSION,
      start: null,
      pages: [],
      acks: [],
      result: null,
      inspect: null
    };
    var state = {
      stage: "validate",
      mapping: null,
      last_ack: null,
      cursor: null,
      pending_batch: null
    };
    var seenEventIds = Object.create(null);
    var seenBatches = Object.create(null);

    try {
      if (!isPlainObject(input)) {
        incomplete("invalid_input", "read requires an input object", state);
      }
      if (typeof clock !== "function") {
        incomplete("invalid_clock", "injected clock is not callable", state);
      }

      var startInput = input.start !== undefined ? input.start : input.start_dto;
      var scopeInput = input.scope !== undefined
        ? input.scope
        : (input.expected !== undefined ? input.expected : input.frozen);
      var budgetInput = input.budgets !== undefined ? input.budgets : input.limits;
      evidence.start = startInput;
      var start = parseStart(startInput, state);
      var scope = parseScope(scopeInput, state);
      var budgets = parseBudgets(budgetInput, state);
      var startedAt = clockOnce(clock, state);
      var deadlineAt = startedAt + budgets.deadline_ms;
      if (!Number.isSafeInteger(deadlineAt)) {
        incomplete("invalid_clock", "deadline computation is not a finite safe integer", state);
      }
      state.mapping = start.mapping;

      var calls = 0;
      var pages = 0;
      var eventRecords = [];
      var batchRecords = [];
      var cursor = null;
      var verifiedCursor = null;

      function checkDeadline(stage, phase) {
        state.stage = stage;
        var observed = clockOnce(clock, state);
        if (observed >= deadlineAt) {
          incomplete(
            "deadline_exceeded",
            "closure deadline was exhausted " + (phase || "before " + stage),
            state
          );
        }
        return observed;
      }

      function retain(name, raw) {
        if (name === "pull") {
          evidence.pages.push(raw);
        } else if (name === "ack") {
          evidence.acks.push(raw);
        } else if (name === "result") {
          evidence.result = raw;
        } else if (name === "inspect") {
          evidence.inspect = raw;
        }
      }

      async function invoke(name, fn, args) {
        checkDeadline(name);
        if (calls + 1 > budgets.max_calls) {
          incomplete("call_budget_exhausted", "closure call budget was exhausted before " + name, state);
        }
        calls += 1;
        var raw;
        try {
          raw = await fn(args);
        } catch (_error) {
          incomplete("callback_failed", name + " callback failed", state);
        }
        retain(name, raw);
        checkDeadline(name, "after the " + name + " callback");
        return raw;
      }

      for (;;) {
        checkDeadline("pull");
        if (pages >= budgets.max_pages) {
          incomplete("page_budget_exhausted", "closure page budget was exhausted with more pages advertised", state);
        }
        var pullArgs = {
          approval_id: start.approvalId,
          limit: ORDINARY_CLOSURE_PAGE_LIMIT
        };
        if (cursor !== null) {
          pullArgs.after_seq = cursor;
        }
        var pullRaw = await invoke("pull", pull, pullArgs);
        var page = validatePage(
          dataOf(
            unwrapEnvelope(pullRaw, "pull response"),
            "approved_pull_events",
            "pull",
            state,
            start.controlBinding
          ),
          start,
          state,
          verifiedCursor,
          seenEventIds
        );
        var priorBatch = seenBatches[page.batch.batch_id];
        if (priorBatch !== undefined &&
            !(priorBatch.empty === true && page.batch.events.length === 0)) {
          incomplete("duplicate_batch", "pull returned an already handled non-empty batch_id", state);
        }
        seenBatches[page.batch.batch_id] = {
          empty: page.batch.events.length === 0
        };

        state.pending_batch = {
          batch_id: page.batch.batch_id,
          from_seq: page.batch.from_seq,
          to_seq: page.batch.to_seq
        };
        state.cursor = page.batch.to_seq;

        var batchJson = JSON.stringify(page.batch);
        if (typeof batchJson !== "string") {
          incomplete("invalid_page", "inner EventBatch is not serializable", state);
        }
        var batchBytes = utf8ByteLength(batchJson);
        if (batchBytes > ORDINARY_CLOSURE_MAX_BATCH_BYTES) {
          incomplete(
            "batch_too_large",
            "inner EventBatch JSON is " + batchBytes + " UTF-8 bytes (max 49152)",
            state
          );
        }

        var ackRaw = await invoke("ack", ack, {
          approval_id: start.approvalId,
          batch_id: page.batch.batch_id
        });
        var acked = validateAck(
          dataOf(
            unwrapEnvelope(ackRaw, "ack response"),
            "approved_ack_events",
            "ack",
            state,
            start.controlBinding
          ),
          start,
          page,
          state,
          allowDuplicateAck
        );
        verifiedCursor = acked.acked_seq;
        state.last_ack = {
          batch_id: acked.batch_id,
          acked_seq: acked.acked_seq,
          advanced: acked.advanced
        };
        state.pending_batch = null;
        state.cursor = page.batch.to_seq;
        batchRecords.push({
          batch_id: acked.batch_id,
          from_seq: page.batch.from_seq,
          to_seq: page.batch.to_seq,
          has_more: page.batch.has_more,
          acked_seq: acked.acked_seq,
          advanced: acked.advanced
        });

        for (var eventIndex = 0; eventIndex < page.batch.events.length; eventIndex += 1) {
          var event = page.batch.events[eventIndex];
          eventRecords.push({
            seq: event.seq,
            event_id: event.event_id,
            run_id: event.run_id,
            kind: event.kind,
            revision: event.revision,
            step_seq: event.step_seq,
            important: event.important,
            created_at_ms: event.created_at_ms,
            batch_id: acked.batch_id,
            acked_seq: acked.acked_seq,
            ack_advanced: acked.advanced
          });
        }

        pages += 1;
        cursor = page.batch.to_seq;
        if (page.batch.has_more === false) {
          break;
        }
      }

      checkDeadline("result");
      var resultRaw = await invoke("result", result, {
        item_id: start.itemId,
        execution_ref: start.executionRef
      });
      var resultData = dataOf(
        unwrapEnvelope(resultRaw, "result response"),
        "backlog_worker_result",
        "result",
        state,
        start.controlBinding
      );
      var checked = validateResult(resultData, start, scope, state);

      checkDeadline("inspect");
      var inspectionRequest = buildInspectionRequest(checked.workerEvidence, scope);
      var inspectRaw = await invoke("inspect", inspect, inspectionRequest);
      var inspection = parseInspection(inspectRaw, state);
      var physical = validateInspection(inspection, inspectionRequest, scope, state);

      var finishedAt = checkDeadline("inspect", "after the final inspection");
      return {
        report: {
          schema_version: 1,
          tool: "threadlane closure-template",
          template_version: CREATE_ORDINARY_CLOSURE_VERSION,
          status: "ready_for_semantic_review",
          authority_granted: false,
          mapping: copyMapping(start.mapping),
          scope: {
            base_commit: scope.base_commit,
            requested_model: scope.requested_model,
            requested_effort: scope.requested_effort,
            write_paths: scope.write_paths.slice()
          },
          read: {
            pages: pages,
            calls: calls,
            started_at_ms: startedAt,
            finished_at_ms: finishedAt,
            deadline_ms: budgets.deadline_ms
          },
          result: checked.result,
          batches: batchRecords,
          events: eventRecords,
          physical: physical,
          checks: {
            authority: true,
            dispatch: true,
            control_dispatch_split: true,
            event_ids_order_run: true,
            batch_acks: true,
            item_snapshot_current: true,
            model_pins: true,
            effort_pins: true,
            exit_zero: true,
            terminal_flags_clear: true,
            result_delivery_digest_match: true,
            physical_inspection: true
          }
        },
        evidence: evidence
      };
    } catch (error) {
      if (error && error.closureIncomplete === true) {
        return {
          report: buildIncompleteReport(error.reason, error.detail, state),
          evidence: evidence
        };
      }
      return {
        report: buildIncompleteReport("internal_error", "unexpected closure template failure", state),
        evidence: evidence
      };
    }
  }

  function doneView(input, positionalReport) {
    try {
      if (!isPlainObject(input)) {
        return incompatible("missing_read_report");
      }
      var report;
      var receiptInput;
      if (input.status === "ready_for_semantic_review") {
        report = input;
        receiptInput = input.receipt !== undefined ? input.receipt : positionalReport;
      } else {
        report = isPlainObject(input.report) ? input.report : positionalReport;
        receiptInput = input.receipt !== undefined ? input.receipt : input;
      }
      if (!isPlainObject(report) ||
          report.status !== "ready_for_semantic_review" || !isPlainObject(report.mapping) ||
          typeof report.mapping.subject_digest !== "string" ||
          typeof report.mapping.task_digest !== "string" ||
          !isPlainObject(report.result) || report.result.outcome !== "success" ||
          report.result.current_status !== "doing" ||
          typeof report.result.acceptance_condition_digest !== "string" ||
          typeof report.result.delivery_description_digest !== "string" ||
          !isPositiveInteger(report.result.reservation_revision) ||
          !isNonNegativeInteger(report.result.reserved_at_ms)) {
        return incompatible("missing_read_report");
      }
      var receiptEnvelope = unwrapEnvelope(receiptInput, "done receipt");
      if (isPlainObject(receiptEnvelope) && receiptEnvelope.error !== undefined) {
        return incompatible("receipt_error");
      }
      if (isPlainObject(receiptEnvelope) &&
          receiptEnvelope.tool !== undefined &&
          receiptEnvelope.tool !== "backlog_worker_complete") {
        return incompatible("receipt_tool_mismatch");
      }
      if (isPlainObject(receiptEnvelope) &&
          receiptEnvelope.binding_id !== undefined &&
          receiptEnvelope.binding_id !== report.mapping.control_binding_id) {
        return incompatible("control_binding_mismatch");
      }
      if (isPlainObject(receiptEnvelope) &&
          receiptEnvelope.control_binding_id !== undefined &&
          receiptEnvelope.control_binding_id !== report.mapping.control_binding_id) {
        return incompatible("control_binding_mismatch");
      }
      var receiptData = isPlainObject(receiptEnvelope) && receiptEnvelope.data !== undefined
        ? receiptEnvelope.data
        : receiptEnvelope;
      if (!isPlainObject(receiptData) || !isPlainObject(receiptData.completion)) {
        return incompatible("missing_completion");
      }
      if (receiptData.control_binding_id !== undefined &&
          receiptData.control_binding_id !== report.mapping.control_binding_id) {
        return incompatible("control_binding_mismatch");
      }
      if (receiptData.approval_id !== undefined &&
          receiptData.approval_id !== report.mapping.approval_id) {
        return incompatible("authority_mismatch");
      }
      if (receiptData.task_id !== undefined &&
          receiptData.task_id !== report.mapping.task_id) {
        return incompatible("authority_mismatch");
      }
      var item = receiptData.completion.item;
      var execution = receiptData.completion.execution;
      if (!isPlainObject(item) || !isPlainObject(execution)) {
        return incompatible("missing_completion");
      }
      if (item.status !== "done") {
        return incompatible("item_not_done");
      }
      if (item.item_id !== report.mapping.item_id ||
          item.binding_id !== report.mapping.control_binding_id) {
        return incompatible("item_mismatch");
      }
      if (item.current_execution_ref !== report.mapping.execution_ref ||
          execution.execution_ref !== report.mapping.execution_ref ||
          execution.item_id !== report.mapping.item_id ||
          execution.binding_id !== report.mapping.control_binding_id) {
        return incompatible("execution_ref_mismatch");
      }
      if (execution.acceptance_condition_digest !== report.result.acceptance_condition_digest ||
          execution.delivery_description_digest !== report.result.delivery_description_digest ||
          execution.reservation_revision !== report.result.reservation_revision ||
          execution.reserved_at_ms !== report.result.reserved_at_ms) {
        return incompatible("immutable_execution_mismatch");
      }
      if (!isPlainObject(execution.authority) ||
          execution.authority.approval_id !== report.mapping.approval_id ||
          execution.authority.task_id !== report.mapping.task_id ||
          execution.authority.subject_digest !== report.mapping.subject_digest ||
          execution.authority.task_digest !== report.mapping.task_digest) {
        return incompatible("authority_mismatch");
      }
      if (!isPlainObject(execution.dispatch_receipt) ||
          execution.dispatch_receipt.session_binding_id !== report.mapping.session_binding_id ||
          execution.dispatch_receipt.run_id !== report.mapping.run_id ||
          execution.dispatch_receipt.worker_id !== report.mapping.worker_id ||
          execution.dispatch_receipt.profile_id !== report.mapping.profile_id ||
          normalizeOptional(execution.dispatch_receipt.execution_binding_digest) !==
            normalizeOptional(report.result.execution_binding_digest)) {
        return incompatible("dispatch_mismatch");
      }
      if (!isPlainObject(execution.outcome) ||
          execution.outcome.outcome !== "success" ||
          execution.outcome.result_digest !== report.result.result_digest) {
        return incompatible("result_digest_mismatch");
      }
      if (execution.acceptance_rejection !== undefined &&
          execution.acceptance_rejection !== null) {
        return incompatible("rejected_execution");
      }
      if (!isPlainObject(execution.completion)) {
        return incompatible("missing_completion_record");
      }
      if (execution.completion.result_digest !== report.result.result_digest ||
          execution.completion.delivery_digest !== report.result.delivery_digest) {
        return incompatible("digest_mismatch");
      }
      if (!isPositiveInteger(item.revision) ||
          item.revision !== report.result.current_revision + 1) {
        return incompatible("revision_mismatch");
      }
      if (!isNonNegativeInteger(item.completed_at_ms) ||
          item.completed_at_ms !== execution.completion.completed_at_ms ||
          item.completed_at_ms < report.result.completed_at_ms ||
          item.completed_at_ms < report.result.started_at_ms) {
        return incompatible("timestamp_mismatch");
      }
      if (receiptData.delivery !== undefined && receiptData.delivery !== null) {
        if (!isPlainObject(receiptData.delivery) ||
            !isPlainObject(report.scope) ||
            receiptData.delivery.approval_id !== report.mapping.approval_id ||
            receiptData.delivery.task_id !== report.mapping.task_id ||
            receiptData.delivery.subject_digest !== report.mapping.subject_digest ||
            receiptData.delivery.task_digest !== report.mapping.task_digest ||
            receiptData.delivery.binding_id !== report.mapping.control_binding_id ||
            receiptData.delivery.session_binding_id !== report.mapping.session_binding_id ||
            receiptData.delivery.run_id !== report.mapping.run_id ||
            receiptData.delivery.worker_id !== report.mapping.worker_id ||
            receiptData.delivery.profile_id !== report.mapping.profile_id ||
            receiptData.delivery.base_commit !== report.scope.base_commit ||
            receiptData.delivery.head_commit !== report.scope.base_commit ||
            receiptData.delivery.result_digest !== report.result.result_digest ||
            receiptData.delivery.delivery_digest !== report.result.delivery_digest) {
          return incompatible("delivery_digest_mismatch");
        }
      }
      return {
        status: "done",
        item_id: item.item_id,
        revision: item.revision,
        completed_at_ms: item.completed_at_ms
      };
    } catch (error) {
      return incompatible("invalid_receipt");
    }
  }

  return Object.freeze({
    read: read,
    doneView: doneView
  });
}

// Small serialized-state client around the legacy read/doneView contract. It
// loads and validates the prep-owned context/start/binding before any effect,
// delegates the finite read to the unchanged legacy composition, stores the
// full private evidence/report/correlation, and returns only a compact view.
// Completion repeats every current-state check before retaining intent and
// making one guarded complete call. Intent, exact receipt, and validated Done
// view are stored per original execution identity; legacy global records stay
// preserved and are used only when they belong to the current execution.
function createOrdinaryClosureClient(callbacks) {
  if (!isPlainObject(callbacks)) {
    throw new TypeError("createOrdinaryClosureClient requires a callbacks object");
  }
  var load = callbacks.load;
  var store = callbacks.store;
  var pull = callbacks.pull;
  var ack = callbacks.ack;
  var result = callbacks.result;
  var inspect = callbacks.inspect;
  var complete = callbacks.complete;
  var recover = callbacks.recover;
  var bootstrap = callbacks.bootstrap;
  var required = [
    ["load", load],
    ["store", store],
    ["pull", pull],
    ["ack", ack],
    ["result", result],
    ["inspect", inspect]
  ];
  for (var index = 0; index < required.length; index += 1) {
    if (typeof required[index][1] !== "function") {
      throw new TypeError("callback " + required[index][0] + " must be a function");
    }
  }
  if (complete !== undefined && typeof complete !== "function") {
    throw new TypeError("callback complete must be a function when supplied");
  }
  if (recover !== undefined && typeof recover !== "function") {
    throw new TypeError("callback recover must be a function when supplied");
  }
  var closure = createOrdinaryClosure({
    pull: pull,
    ack: ack,
    result: result,
    inspect: inspect,
    allow_duplicate_ack: recover !== undefined
  });
  var noticesInFlight = Object.create(null);

  function validCapsule(input, template) {
    return isPlainObject(input) &&
      typeof input.expected_source_sha256 === "string" &&
      /^[0-9a-f]{64}$/.test(input.expected_source_sha256) &&
      isPlainObject(template) &&
      template.schema_version === 1 &&
      template.tool === "threadlane closure-template" &&
      template.version === CREATE_ORDINARY_CLOSURE_VERSION &&
      template.source_path === "skills/threadlane/references/ordinary-closure.js" &&
      template.factory_name === "createOrdinaryClosure" &&
      typeof template.source_sha256 === "string" &&
      /^[0-9a-f]{64}$/.test(template.source_sha256) &&
      template.source_sha256 === input.expected_source_sha256 &&
      typeof template.javascript === "string" &&
      template.javascript.indexOf(
        'var CREATE_ORDINARY_CLOSURE_VERSION = "2026-11-19.v1";'
      ) !== -1;
  }

  function compactReport(report, input) {
    var view = {};
    var keys = Object.keys(report);
    for (var keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
      var key = keys[keyIndex];
      if (key !== "events") {
        view[key] = report[key];
      }
    }
    if (Array.isArray(report.events)) {
      var events = report.events;
      var batches = Array.isArray(report.batches) ? report.batches : [];
      var important = 0;
      for (var eventIndex = 0; eventIndex < events.length; eventIndex += 1) {
        if (events[eventIndex].important === true) {
          important += 1;
        }
      }
      view.events = {
        count: events.length,
        first_seq: events.length > 0 ? events[0].seq : null,
        last_seq: events.length > 0 ? events[events.length - 1].seq : null,
        acked_seq: batches.length > 0 ? batches[batches.length - 1].acked_seq : null,
        important_count: important
      };
    }
    view.source_sha256 = input.expected_source_sha256;
    return view;
  }

  async function readRetained(approvalId) {
    var input = await load(ORDINARY_CLOSURE_CLIENT_CONTEXT_KEY);
    var template = await load(ORDINARY_CLOSURE_CLIENT_TEMPLATE_KEY);
    var start = await load(ORDINARY_CLOSURE_CLIENT_START_KEY);
    var binding = await load(ORDINARY_CLOSURE_CLIENT_BINDING_KEY);
    if (!validCapsule(input, template) || !isPlainObject(start) || !isPlainObject(binding) ||
        JSON.stringify(binding) !== JSON.stringify({input: input, start: start})) {
      throw new Error(
        "missing or changed retained context/start/binding; use original-receipt fallback"
      );
    }
    var identity = storedStartIdentity(start);
    var args = input.start_args;
    if (!identity || !isPlainObject(args) ||
        identity.approval_id !== args.approval_id || identity.task_id !== args.task_id ||
        identity.item_id !== args.item_id || !isPositiveInteger(args.expected_revision) ||
        identity.reservation_revision !== args.expected_revision + 1 ||
        (approvalId !== undefined && identity.approval_id !== approvalId)) {
      throw new Error("notice or retained request identity mismatch; use original-receipt fallback");
    }
    var outcome = await closure.read({
      start: start,
      scope: input.scope,
      budgets: {
        max_pages: 8,
        max_calls: 16,
        deadline_ms: 60000
      }
    });
    var report = outcome.report;
    var evidence = outcome.evidence;
    if (report.status !== "ready_for_semantic_review") {
      var receipts = completionState(await load(ORDINARY_CLOSURE_CLIENT_DONE_RECEIPT_KEY));
      var key = stableStringify(identity);
      var priorReceipt = completionRecord(receipts, key);
      if (priorReceipt === undefined) priorReceipt = receipts.legacy;
      var priorIdentity = storedReceiptIdentity(priorReceipt);
      if (priorIdentity && stableStringify(priorIdentity) === key) {
        var priorReport = await load(ORDINARY_CLOSURE_CLIENT_REPORT_KEY);
        var correlation = await load(ORDINARY_CLOSURE_CLIENT_CORRELATION_KEY);
        if (isPlainObject(priorReport) && isPlainObject(correlation) &&
            JSON.stringify(correlation) === JSON.stringify({input: input, start: start, report: priorReport}) &&
            closure.doneView({receipt: priorReceipt, report: priorReport}).status === "done") {
          // Keep each subsequent observation without replacing the accepted verification.
          var prefix = "threadlane/closure-after-done/" + identity.execution_ref;
          var previous = await load(prefix + "/count");
          var sequence = previous === undefined ? 1 : previous + 1;
          if (!isPositiveInteger(sequence)) throw new Error("invalid post-completion evidence index");
          await store(prefix + "/" + sequence, {identity: identity, report: report, evidence: evidence});
          await store(prefix + "/count", sequence);
          return compactReport(report, input);
        }
      }
    }
    await store(ORDINARY_CLOSURE_CLIENT_EVIDENCE_KEY, evidence);
    await store(ORDINARY_CLOSURE_CLIENT_REPORT_KEY, report);
    await store(ORDINARY_CLOSURE_CLIENT_CORRELATION_KEY, {
      input: input,
      start: start,
      report: report
    });
    return compactReport(report, input);
  }

  async function readStored() {
    return readRetained();
  }

  async function readNotice(approvalId) {
    if (typeof approvalId !== "string" || !/^apr_[0-9a-f]{32}$/.test(approvalId)) {
      throw new Error("invalid approval_id format");
    }
    if (recover === undefined) return readRetained(approvalId);
    if (noticesInFlight[approvalId]) return noticesInFlight[approvalId];
    var pending = readRecovered(approvalId);
    noticesInFlight[approvalId] = pending;
    try {
      return await pending;
    } finally {
      delete noticesInFlight[approvalId];
    }
  }

  // The caller obtains this capsule from the already verified canonical package
  // and guide, never from the notification or a Worker. Recovery is read-only.
  async function readRecovered(approvalId) {
    if (!isPlainObject(bootstrap) || typeof bootstrap.binary !== "string" ||
        bootstrap.binary.charAt(0) !== "/" || bootstrap.binary.indexOf("\u0000") !== -1 ||
        !validCapsule({expected_source_sha256: bootstrap.expected_source_sha256}, bootstrap.template)) {
      throw new Error("invalid verified package bootstrap");
    }
    var raw = await recover({approval_id: approvalId});
    var env;
    if (isPlainObject(raw) && (raw.isError === true || raw.is_error === true)) {
      var errorCopy = {};
      Object.keys(raw).forEach(function (key) {
        if (key !== "isError" && key !== "is_error") errorCopy[key] = raw[key];
      });
      var failure = unwrapEnvelope(errorCopy, "notice context error");
      if (isPlainObject(failure) && failure.tool === "backlog_worker_notice_context" &&
          typeof failure.binding_id === "string" && isPlainObject(failure.error) &&
          failure.error.code === "no_guarded_execution") {
        return {status: "legacy_events", approval_id: approvalId};
      }
      throw new Error("scoped notice context refused; no legacy fallback");
    }
    env = unwrapEnvelope(raw, "notice context");
    if (!isPlainObject(env) || env.tool !== "backlog_worker_notice_context" ||
        typeof env.binding_id !== "string") {
      throw new Error("invalid scoped notice context");
    }
    var data = dataOf(env, "backlog_worker_notice_context", "notice context", {});
    if (!isPlainObject(data) || !isPlainObject(data.execution) ||
        !isPlainObject(data.scope) || !isPlainObject(data.item_snapshot) ||
        typeof data.acceptance_condition !== "string" || !data.acceptance_condition ||
        utf8ByteLength(data.acceptance_condition) > 2048 ||
        typeof data.delivery_description !== "string" || !data.delivery_description ||
        utf8ByteLength(data.delivery_description) > 2048) {
      throw new Error("invalid scoped notice context");
    }
    var semanticReview = {untrusted_data: true, acceptance_condition: data.acceptance_condition,
      delivery_description: data.delivery_description};
    var parsed = parseStart({execution: data.execution}, {}, true);
    var identity = executionIdentityFromParsed(parsed);
    var snapshot = data.item_snapshot;
    if (env.binding_id !== identity.control_binding_id || identity.approval_id !== approvalId ||
        snapshot.item_id !== identity.item_id || snapshot.current_execution_ref !== identity.execution_ref ||
        !isPositiveInteger(snapshot.revision) || snapshot.revision < identity.reservation_revision ||
        (snapshot.status !== "doing" && snapshot.status !== "done" && snapshot.status !== "blocked")) {
      throw new Error("notice context identity or current revision mismatch");
    }
    var scope = parseScope({
      base_commit: data.scope.base_commit,
      requested_model: data.scope.requested_model,
      requested_effort: data.scope.requested_effort,
      write_paths: data.scope.write_paths,
      inspector: {executable: bootstrap.binary, argv: ["inspect-delivery"]}
    }, {});
    var input = await load(ORDINARY_CLOSURE_CLIENT_CONTEXT_KEY);
    var template = await load(ORDINARY_CLOSURE_CLIENT_TEMPLATE_KEY);
    var start = await load(ORDINARY_CLOSURE_CLIENT_START_KEY);
    var binding = await load(ORDINARY_CLOSURE_CLIENT_BINDING_KEY);
    var priorReport = await load(ORDINARY_CLOSURE_CLIENT_REPORT_KEY);
    var priorCorrelation = await load(ORDINARY_CLOSURE_CLIENT_CORRELATION_KEY);
    var priorEvidence = await load(ORDINARY_CLOSURE_CLIENT_EVIDENCE_KEY);
    var missingInput = input === undefined || input === null;
    var missingStart = start === undefined || start === null;
    var missingBinding = binding === undefined || binding === null;
    var missingTemplate = template === undefined || template === null;
    if (!missingTemplate && stableStringify(template) !== stableStringify(bootstrap.template)) {
      throw new Error("retained template conflicts with verified package");
    }
    if (!missingBinding) {
      if (!isPlainObject(binding) || !isPlainObject(binding.input) || !isPlainObject(binding.start) ||
          (!missingInput && stableStringify(input) !== stableStringify(binding.input)) ||
          (!missingStart && stableStringify(start) !== stableStringify(binding.start))) {
        throw new Error("retained context/start/binding conflict");
      }
      if (missingInput) input = binding.input;
      if (missingStart) start = binding.start;
    }
    if (missingBinding && isPlainObject(priorCorrelation)) {
      if (!isPlainObject(priorCorrelation.input) || !isPlainObject(priorCorrelation.start) ||
          !isPlainObject(priorCorrelation.report)) {
        throw new Error("retained correlation is incomplete");
      }
      if (missingInput) input = priorCorrelation.input;
      if (missingStart) start = priorCorrelation.start;
    }
    if (input !== undefined && input !== null) {
      var args = input.start_args;
      if (!validCapsule(input, bootstrap.template) || input.binary !== bootstrap.binary ||
          !isPlainObject(args) || args.approval_id !== identity.approval_id ||
          args.task_id !== identity.task_id || args.item_id !== identity.item_id ||
          !isPositiveInteger(args.expected_revision) ||
          args.expected_revision + 1 !== identity.reservation_revision ||
          stableStringify(parseScope(input.scope, {})) !== stableStringify(scope)) {
        throw new Error("retained context conflicts with frozen notice scope");
      }
    } else {
      if (identity.reservation_revision <= 1) throw new Error("invalid original reservation revision");
      input = {
        binary: bootstrap.binary,
        expected_source_sha256: bootstrap.expected_source_sha256,
        start_args: {
          item_id: identity.item_id, approval_id: identity.approval_id, task_id: identity.task_id,
          expected_revision: identity.reservation_revision - 1
        },
        scope: scope
      };
    }
    if (start !== undefined && start !== null &&
        stableStringify(storedStartIdentity(start)) !== stableStringify(identity)) {
      throw new Error("retained start conflicts with original execution");
    }
    if (priorReport !== undefined && priorReport !== null) {
      var reportIdentity = executionIdentityFromReport(priorReport);
      if (!reportIdentity || stableStringify(reportIdentity) !== stableStringify(identity) ||
          !isPlainObject(priorCorrelation) ||
          stableStringify(priorCorrelation.input) !== stableStringify(input) ||
          stableStringify(priorCorrelation.start) !== stableStringify(start) ||
          stableStringify(priorCorrelation.report) !== stableStringify(priorReport) ||
          !isPlainObject(priorEvidence) ||
          stableStringify(storedStartIdentity(priorEvidence.start)) !== stableStringify(identity) ||
          stableStringify(priorReport.scope) !== stableStringify({
            base_commit: scope.base_commit, requested_model: scope.requested_model,
            requested_effort: scope.requested_effort, write_paths: scope.write_paths
          })) {
        throw new Error("retained report conflicts with original execution");
      }
    } else if (priorCorrelation !== undefined && priorCorrelation !== null) {
      throw new Error("retained correlation has no matching report");
    } else if (priorEvidence !== undefined && priorEvidence !== null) {
      throw new Error("retained evidence has no matching report");
    }
    if (snapshot.status === "done") {
      var done = validateDurableDone(data, parsed, identity, priorReport);
      await validateDoneCache(data.completion, identity);
      done.events = await drainDoneEvents(parsed);
      return done;
    }
    if (snapshot.status === "blocked") {
      if (data.completion != null || data.execution.completion != null ||
          !isPlainObject(data.execution.outcome) ||
          !(data.execution.outcome.outcome === "failure" ||
            (data.execution.outcome.outcome === "success" && isPlainObject(data.execution.acceptance_rejection)))) {
        throw new Error("blocked notice lacks an original failure or rejection");
      }
      return {status: "blocked", item_id: identity.item_id, revision: snapshot.revision,
        events: await drainDoneEvents(parsed)};
    }
    if (data.completion !== undefined && data.completion !== null ||
        data.execution.completion !== undefined && data.execution.completion !== null) {
      throw new Error("doing notice has a completion record");
    }
    if (priorReport && (priorReport.status !== "ready_for_semantic_review" ||
        priorReport.result.current_revision !== snapshot.revision)) {
      throw new Error("retained result is incomplete or stale; reconcile without retry");
    }
    if (start === undefined || start === null) start = {execution: data.execution};
    // Never replace a present key. A missing binding contains only facts just
    // checked against the authoritative, binding-scoped original record.
    if (missingTemplate) await store(ORDINARY_CLOSURE_CLIENT_TEMPLATE_KEY, bootstrap.template);
    if (missingInput) await store(ORDINARY_CLOSURE_CLIENT_CONTEXT_KEY, input);
    if (missingStart) await store(ORDINARY_CLOSURE_CLIENT_START_KEY, start);
    if (missingBinding) await store(ORDINARY_CLOSURE_CLIENT_BINDING_KEY, {input: input, start: start});
    if (priorReport) {
      var retained = compactReport(priorReport, input);
      retained.later_events = await drainDoneEvents(parsed);
      retained.semantic_review = semanticReview;
      return retained;
    }
    var view = await readRetained(approvalId);
    if (view.status === "ready_for_semantic_review") view.semantic_review = semanticReview;
    return view;
  }

  function validateDurableDone(data, parsed, identity, priorReport) {
    var receipt = data.completion;
    var item = receipt && receipt.item;
    var execution = receipt && receipt.execution;
    var snapshot = data.item_snapshot;
    if (!isPlainObject(item) || !isPlainObject(execution) ||
        stableStringify(execution) !== stableStringify(data.execution) ||
        item.status !== "done" || item.item_id !== identity.item_id ||
        item.binding_id !== identity.control_binding_id ||
        item.acceptance_condition !== data.acceptance_condition ||
        item.delivery_description !== data.delivery_description ||
        item.current_execution_ref !== identity.execution_ref ||
        item.revision !== snapshot.revision || !isPositiveInteger(item.revision) ||
        item.revision <= identity.reservation_revision ||
        !isPlainObject(execution.outcome) || execution.outcome.outcome !== "success" ||
        !isPlainObject(execution.completion) ||
        !/^[0-9a-f]{64}$/.test(execution.outcome.result_digest || "") ||
        execution.completion.result_digest !== execution.outcome.result_digest ||
        !/^[0-9a-f]{64}$/.test(execution.completion.delivery_digest || "") ||
        !isNonNegativeInteger(item.completed_at_ms) ||
        item.completed_at_ms !== execution.completion.completed_at_ms ||
        item.completion_note !== execution.completion.acceptance_note ||
        item.completion_evidence !== execution.completion.acceptance_evidence) {
      throw new Error("durable completion does not match original execution");
    }
    if (priorReport && closure.doneView({receipt: {completion: receipt}, report: priorReport}).status !== "done") {
      throw new Error("durable completion conflicts with retained report");
    }
    return {status: "done", approval_id: identity.approval_id,
      execution_ref: identity.execution_ref, item_id: item.item_id, revision: item.revision,
      completed_at_ms: item.completed_at_ms};
  }

  async function validateDoneCache(completion, identity) {
    var key = stableStringify(identity);
    var receiptState = completionState(await load(ORDINARY_CLOSURE_CLIENT_DONE_RECEIPT_KEY));
    var receipt = completionRecord(receiptState, key);
    if (receipt === undefined && receiptState.legacy !== null &&
        stableStringify(storedReceiptIdentity(receiptState.legacy)) === key) {
      receipt = receiptState.legacy;
    }
    if (receipt !== undefined) {
      var envelope = unwrapEnvelope(receipt, "retained done receipt");
      var body = envelope && envelope.data !== undefined ? envelope.data : envelope;
      if (!isPlainObject(body) || stableStringify(body.completion) !== stableStringify(completion)) {
        throw new Error("retained completion conflicts with durable completion");
      }
    }
    var intentState = completionState(await load(ORDINARY_CLOSURE_CLIENT_INTENT_KEY));
    var intent = completionRecord(intentState, key);
    if (intent !== undefined && (!isPlainObject(intent) || !isPlainObject(intent.args) ||
        stableStringify(storedStartIdentity(intent.start)) !== key ||
        intent.args.execution_ref !== identity.execution_ref ||
        intent.args.expected_result_digest !== completion.execution.completion.result_digest ||
        intent.args.expected_delivery_digest !== completion.execution.completion.delivery_digest)) {
      throw new Error("retained completion intent conflicts with durable completion");
    }
    var views = completionState(await load(ORDINARY_CLOSURE_CLIENT_DONE_VIEW_KEY));
    var view = completionRecord(views, key);
    if (view !== undefined && (!isPlainObject(view) || view.status !== "done" ||
        view.item_id !== completion.item.item_id || view.revision !== completion.item.revision ||
        view.completed_at_ms !== completion.item.completed_at_ms)) {
      throw new Error("retained Done view conflicts with durable completion");
    }
  }

  async function drainDoneEvents(start) {
    var deadline = Date.now() + 60000;
    var cursor = null, pages = 0, calls = 0, seen = Object.create(null);
    var state = {}, batches = [];
    async function retainOnce(key, value) {
      var previous = await load(key);
      if (previous !== undefined && previous !== null) {
        if (stableStringify(previous) !== stableStringify(value)) {
          throw new Error("conflicting event evidence");
        }
        return;
      }
      await store(key, value);
    }
    try {
      for (;;) {
        if (Date.now() >= deadline || pages >= 8 || calls >= 16) {
          throw new Error("event drain budget exhausted");
        }
        var args = {approval_id: start.approvalId, limit: ORDINARY_CLOSURE_PAGE_LIMIT};
        if (cursor !== null) args.after_seq = cursor;
        calls += 1;
        var raw = await pull(args);
        if (Date.now() >= deadline) throw new Error("event drain deadline exceeded");
        var page = validatePage(dataOf(unwrapEnvelope(raw, "pull response"),
          "approved_pull_events", "pull", state, start.controlBinding), start, state, cursor, seen);
        if (!/^bat_[a-zA-Z0-9_-]{1,128}$/.test(page.batch.batch_id)) {
          throw new Error("unsafe event batch identifier");
        }
        var evidenceKey = "threadlane/closure-after-done/" + start.executionRef + "/" + page.batch.batch_id;
        await retainOnce(evidenceKey + "/page", raw);
        if (utf8ByteLength(JSON.stringify(page.batch)) > ORDINARY_CLOSURE_MAX_BATCH_BYTES) {
          throw new Error("event batch too large");
        }
        pages += 1;
        if (page.batch.events.length === 0) {
          return {status: "verified", pages: pages, batches: batches,
            acked_seq: page.batch.acked_seq};
        }
        if (calls >= 16 || Date.now() >= deadline) throw new Error("event drain budget exhausted");
        calls += 1;
        var ackRaw = await ack({approval_id: start.approvalId, batch_id: page.batch.batch_id});
        await retainOnce(evidenceKey + "/ack", ackRaw);
        if (Date.now() >= deadline) throw new Error("event drain deadline exceeded");
        var checked = validateAck(dataOf(unwrapEnvelope(ackRaw, "ack response"),
          "approved_ack_events", "ack", state, start.controlBinding), start, page, state, true);
        cursor = checked.acked_seq;
        batches.push({batch_id: checked.batch_id, acked_seq: checked.acked_seq, advanced: checked.advanced});
        if (!page.batch.has_more) return {status: "verified", pages: pages, batches: batches, acked_seq: cursor};
      }
    } catch (error) {
      // Done is durable even if a stopped host or a bad/unknown ACK prevents
      // verification of later event delivery. Do not retry or claim an ACK.
      return {status: "unverified", pages: pages, batches: batches,
        acked_seq: cursor, reason: error && error.closureIncomplete ? error.reason : "unavailable_or_unknown"};
    }
  }

  // Each completion key holds a versioned map of original-execution records
  // plus the untouched legacy value written by older versions.
  function completionState(prior) {
    if (prior === undefined || prior === null) {
      return { records: {}, legacy: null };
    }
    if (!isPlainObject(prior) ||
        prior.tool !== ORDINARY_CLOSURE_CLIENT_COMPLETION_STATE_TOOL) {
      return { records: {}, legacy: prior };
    }
    if (prior.schema_version !== 2 || !isPlainObject(prior.records)) {
      throw new Error(
        "corrupt retained completion state; use original-receipt fallback"
      );
    }
    var records = {};
    var keys = Object.keys(prior.records);
    for (var index = 0; index < keys.length; index += 1) {
      records[keys[index]] = prior.records[keys[index]];
    }
    return {
      records: records,
      legacy: prior.legacy === undefined ? null : prior.legacy
    };
  }

  function completionStateValue(state, template) {
    var records = {};
    var keys = Object.keys(state.records);
    for (var index = 0; index < keys.length; index += 1) {
      records[keys[index]] = state.records[keys[index]];
    }
    return {
      schema_version: 2,
      tool: ORDINARY_CLOSURE_CLIENT_COMPLETION_STATE_TOOL,
      template_version: CREATE_ORDINARY_CLOSURE_VERSION,
      source_sha256: template.source_sha256,
      records: records,
      legacy: state.legacy === undefined ? null : state.legacy
    };
  }

  function completionRecord(state, recordKey) {
    if (Object.prototype.hasOwnProperty.call(state.records, recordKey)) {
      return state.records[recordKey];
    }
    return undefined;
  }

  async function storeCompletionRecord(key, state, recordKey, value, template) {
    state.records[recordKey] = value;
    await store(key, completionStateValue(state, template));
  }

  function executionIdentityFromParsed(parsed) {
    return {
      approval_id: parsed.approvalId,
      task_id: parsed.taskId,
      subject_digest: parsed.subjectDigest,
      task_digest: parsed.taskDigest,
      item_id: parsed.itemId,
      execution_ref: parsed.executionRef,
      control_binding_id: parsed.controlBinding,
      session_binding_id: parsed.dispatchSession,
      run_id: parsed.dispatch.run_id,
      worker_id: parsed.dispatch.worker_id,
      profile_id: parsed.dispatch.profile_id,
      acceptance_condition_digest: parsed.acceptanceConditionDigest,
      delivery_description_digest: parsed.deliveryDescriptionDigest,
      reservation_revision: parsed.reservationRevision,
      reserved_at_ms: parsed.reservedAtMs,
      execution_binding_digest: normalizeOptional(
        parsed.dispatch.execution_binding_digest
      )
    };
  }

  function executionIdentityFromReport(report) {
    if (!isPlainObject(report) || !isPlainObject(report.mapping) ||
        !isPlainObject(report.result)) {
      return null;
    }
    var mapping = report.mapping;
    var result = report.result;
    var identity = {
      approval_id: mapping.approval_id,
      task_id: mapping.task_id,
      subject_digest: mapping.subject_digest,
      task_digest: mapping.task_digest,
      item_id: mapping.item_id,
      execution_ref: mapping.execution_ref,
      control_binding_id: mapping.control_binding_id,
      session_binding_id: mapping.session_binding_id,
      run_id: mapping.run_id,
      worker_id: mapping.worker_id,
      profile_id: mapping.profile_id,
      acceptance_condition_digest: result.acceptance_condition_digest,
      delivery_description_digest: result.delivery_description_digest,
      reservation_revision: result.reservation_revision,
      reserved_at_ms: result.reserved_at_ms,
      execution_binding_digest: normalizeOptional(result.execution_binding_digest)
    };
    var required = [
      "approval_id",
      "task_id",
      "subject_digest",
      "task_digest",
      "item_id",
      "execution_ref",
      "control_binding_id",
      "session_binding_id",
      "run_id",
      "worker_id",
      "profile_id",
      "acceptance_condition_digest",
      "delivery_description_digest"
    ];
    for (var index = 0; index < required.length; index += 1) {
      if (typeof identity[required[index]] !== "string" ||
          identity[required[index]].length === 0) {
        return null;
      }
    }
    if (!isPositiveInteger(identity.reservation_revision) ||
        !isNonNegativeInteger(identity.reserved_at_ms)) {
      return null;
    }
    return identity;
  }

  function storedStartIdentity(raw) {
    try {
      return executionIdentityFromParsed(parseStart(raw, {}));
    } catch (_error) {
      return null;
    }
  }

  function storedReceiptIdentity(raw) {
    try {
      var envelope = unwrapEnvelope(raw, "retained done receipt");
      if (!isPlainObject(envelope) || envelope.error !== undefined) {
        return null;
      }
      var data = envelope.data !== undefined ? envelope.data : envelope;
      if (!isPlainObject(data) || !isPlainObject(data.completion)) {
        return null;
      }
      var item = data.completion.item;
      var execution = data.completion.execution;
      if (!isPlainObject(item) || !isPlainObject(execution) ||
          !isPlainObject(execution.authority) ||
          !isPlainObject(execution.dispatch_receipt) ||
          item.item_id !== execution.item_id ||
          item.binding_id !== execution.binding_id) {
        return null;
      }
      var identity = {
        approval_id: execution.authority.approval_id,
        task_id: execution.authority.task_id,
        subject_digest: execution.authority.subject_digest,
        task_digest: execution.authority.task_digest,
        item_id: execution.item_id,
        execution_ref: execution.execution_ref,
        control_binding_id: execution.binding_id,
        session_binding_id: execution.dispatch_receipt.session_binding_id,
        run_id: execution.dispatch_receipt.run_id,
        worker_id: execution.dispatch_receipt.worker_id,
        profile_id: execution.dispatch_receipt.profile_id,
        acceptance_condition_digest: execution.acceptance_condition_digest,
        delivery_description_digest: execution.delivery_description_digest,
        reservation_revision: execution.reservation_revision,
        reserved_at_ms: execution.reserved_at_ms,
        execution_binding_digest: normalizeOptional(
          execution.dispatch_receipt.execution_binding_digest
        )
      };
      var required = [
        "approval_id",
        "task_id",
        "subject_digest",
        "task_digest",
        "item_id",
        "execution_ref",
        "control_binding_id",
        "session_binding_id",
        "run_id",
        "worker_id",
        "profile_id",
        "acceptance_condition_digest",
        "delivery_description_digest"
      ];
      for (var index = 0; index < required.length; index += 1) {
        if (typeof identity[required[index]] !== "string" ||
            identity[required[index]].length === 0) {
          return null;
        }
      }
      if (!isPositiveInteger(identity.reservation_revision) ||
          !isNonNegativeInteger(identity.reserved_at_ms)) {
        return null;
      }
      return identity;
    } catch (_error) {
      return null;
    }
  }

  function legacyIdentityKey(identity) {
    var copy = {};
    var keys = Object.keys(identity);
    for (var index = 0; index < keys.length; index += 1) {
      if (keys[index] !== "execution_binding_digest") {
        copy[keys[index]] = identity[keys[index]];
      }
    }
    return stableStringify(copy);
  }

  function legacyPartKind(value, identity, expectedIdentity) {
    if (value === undefined || value === null) {
      return "absent";
    }
    if (identity === null || identity === undefined) {
      return "ambiguous";
    }
    return legacyIdentityKey(identity) === legacyIdentityKey(expectedIdentity)
      ? "current"
      : "foreign";
  }

  async function completeStored(acceptance) {
    var note = acceptance && acceptance.acceptance_note;
    var evidence = acceptance && acceptance.acceptance_evidence;
    if (!isPlainObject(acceptance) ||
        typeof acceptance.command_key !== "string" || acceptance.command_key.length === 0 ||
        typeof note !== "string" || note.length === 0 ||
        typeof evidence !== "string" || evidence.length === 0) {
      throw new Error("missing explicit acceptance input");
    }
    if (typeof complete !== "function") {
      throw new Error("completion callback is not available");
    }
    var input = await load(ORDINARY_CLOSURE_CLIENT_CONTEXT_KEY);
    var template = await load(ORDINARY_CLOSURE_CLIENT_TEMPLATE_KEY);
    var start = await load(ORDINARY_CLOSURE_CLIENT_START_KEY);
    var binding = await load(ORDINARY_CLOSURE_CLIENT_BINDING_KEY);
    var corr = await load(ORDINARY_CLOSURE_CLIENT_CORRELATION_KEY);
    var report = await load(ORDINARY_CLOSURE_CLIENT_REPORT_KEY);
    var reportIdentity = executionIdentityFromReport(report);
    var startIdentity = storedStartIdentity(start);
    if (!validCapsule(input, template) ||
        !isPlainObject(start) || !isPlainObject(binding) ||
        JSON.stringify(binding) !== JSON.stringify({input: input, start: start}) ||
        !isPlainObject(corr) || !isPlainObject(report) ||
        JSON.stringify(corr) !== JSON.stringify({input: input, start: start, report: report}) ||
        report.status !== "ready_for_semantic_review" ||
        report.tool !== "threadlane closure-template" ||
        report.template_version !== CREATE_ORDINARY_CLOSURE_VERSION ||
        report.authority_granted !== false ||
        !isPlainObject(report.mapping) || !isPlainObject(report.result) ||
        reportIdentity === null || startIdentity === null ||
        stableStringify(startIdentity) !== stableStringify(reportIdentity) ||
        typeof report.mapping.item_id !== "string" || report.mapping.item_id.length === 0 ||
        typeof report.mapping.execution_ref !== "string" ||
        report.mapping.execution_ref.length === 0 ||
        !isPositiveInteger(report.result.current_revision) ||
        report.result.outcome !== "success" ||
        report.result.current_status !== "doing" ||
        typeof report.result.result_digest !== "string" ||
        report.result.result_digest.length === 0 ||
        typeof report.result.delivery_digest !== "string" ||
        report.result.delivery_digest.length === 0) {
      throw new Error(
        "invalid closure capsule, context, or report; stale or missing " +
        "start/binding/correlation; use original-receipt fallback"
      );
    }

    var acceptanceData = {
      command_key: acceptance.command_key,
      acceptance_note: note,
      acceptance_evidence: evidence
    };
    var args = {
      command_key: acceptance.command_key,
      item_id: report.mapping.item_id,
      expected_revision: report.result.current_revision,
      execution_ref: report.mapping.execution_ref,
      expected_result_digest: report.result.result_digest,
      expected_delivery_digest: report.result.delivery_digest,
      acceptance_note: note,
      acceptance_evidence: evidence
    };
    var intent = {
      schema_version: 1,
      template_version: CREATE_ORDINARY_CLOSURE_VERSION,
      source_sha256: template.source_sha256,
      acceptance: acceptanceData,
      args: args,
      start: start
    };
    var identityKey = stableStringify(reportIdentity);
    var priorIntentState = completionState(
      await load(ORDINARY_CLOSURE_CLIENT_INTENT_KEY)
    );
    var priorReceiptState = completionState(
      await load(ORDINARY_CLOSURE_CLIENT_DONE_RECEIPT_KEY)
    );
    var priorViewState = completionState(
      await load(ORDINARY_CLOSURE_CLIENT_DONE_VIEW_KEY)
    );
    var recordIntent = completionRecord(priorIntentState, identityKey);
    var recordReceipt = completionRecord(priorReceiptState, identityKey);
    var recordView = completionRecord(priorViewState, identityKey);

    function intentMatches(priorIntent) {
      return isPlainObject(priorIntent) &&
        priorIntent.schema_version === 1 &&
        priorIntent.template_version === CREATE_ORDINARY_CLOSURE_VERSION &&
        priorIntent.source_sha256 === template.source_sha256 &&
        stableStringify(priorIntent.acceptance) === stableStringify(acceptanceData) &&
        stableStringify(priorIntent.args) === stableStringify(args) &&
        stableStringify(priorIntent.start) === stableStringify(start);
    }

    function expectedViewFor(retained) {
      return {
        schema_version: 1,
        template_version: CREATE_ORDINARY_CLOSURE_VERSION,
        source_sha256: template.source_sha256,
        acceptance: acceptanceData,
        args: args,
        start: start,
        status: "done",
        item_id: retained.item_id,
        revision: retained.revision,
        completed_at_ms: retained.completed_at_ms
      };
    }

    function retainedDone(receipt, view) {
      var retained = closure.doneView({receipt: receipt, report: report});
      if (!isPlainObject(retained) || retained.status !== "done") {
        throw new Error(
          "retained completion receipt is not Done; reconcile it; no retry"
        );
      }
      if (view === undefined || view === null) {
        throw new Error(
          "retained completion receipt has no validated Done view; " +
          "use original-receipt fallback"
        );
      }
      if (!isPlainObject(view) ||
          stableStringify(view) !== stableStringify(expectedViewFor(retained))) {
        throw new Error(
          "retained Done view is corrupt or belongs to another attempt; " +
          "use original-receipt fallback"
        );
      }
      return {
        status: "done",
        item_id: retained.item_id,
        revision: retained.revision,
        completed_at_ms: retained.completed_at_ms
      };
    }

    if (recordIntent !== undefined ||
        recordReceipt !== undefined ||
        recordView !== undefined) {
      if (recordIntent !== undefined && !intentMatches(recordIntent)) {
        throw new Error(
          "retained completion intent belongs to a different attempt; " +
          "use original-receipt fallback"
        );
      }
      if (recordReceipt !== undefined) {
        if (recordIntent === undefined) {
          throw new Error(
            "retained completion receipt lacks matching retained intent; " +
            "use original-receipt fallback"
          );
        }
        return retainedDone(recordReceipt, recordView);
      }
      if (recordView !== undefined) {
        throw new Error(
          "retained Done view has no matching receipt; use original-receipt fallback"
        );
      }
      throw new Error(
        "completion outcome is unknown; reconcile the retained intent and " +
        "original receipt; no retry"
      );
    }

    var legacyIntentKind = legacyPartKind(
      priorIntentState.legacy,
      isPlainObject(priorIntentState.legacy)
        ? storedStartIdentity(priorIntentState.legacy.start)
        : null,
      reportIdentity
    );
    var legacyReceiptKind = legacyPartKind(
      priorReceiptState.legacy,
      storedReceiptIdentity(priorReceiptState.legacy),
      reportIdentity
    );
    var legacyViewKind = legacyPartKind(
      priorViewState.legacy,
      isPlainObject(priorViewState.legacy)
        ? storedStartIdentity(priorViewState.legacy.start)
        : null,
      reportIdentity
    );
    if (legacyIntentKind === "ambiguous" ||
        legacyReceiptKind === "ambiguous" ||
        legacyViewKind === "ambiguous") {
      throw new Error(
        "ambiguous retained completion state; use original-receipt fallback"
      );
    }
    var legacyIntent = legacyIntentKind === "current"
      ? priorIntentState.legacy
      : null;
    var legacyReceipt = legacyReceiptKind === "current"
      ? priorReceiptState.legacy
      : null;
    var legacyView = legacyViewKind === "current"
      ? priorViewState.legacy
      : null;
    if (legacyIntent !== null && !intentMatches(legacyIntent)) {
      throw new Error(
        "retained completion intent belongs to a different attempt; " +
        "use original-receipt fallback"
      );
    }
    if (legacyReceipt !== null) {
      if (legacyIntent === null) {
        throw new Error(
          "retained completion receipt lacks matching retained intent; " +
          "use original-receipt fallback"
        );
      }
      return retainedDone(legacyReceipt, legacyView);
    }
    if (legacyView !== null) {
      throw new Error(
        "retained Done view has no matching receipt; use original-receipt fallback"
      );
    }
    if (legacyIntent !== null) {
      throw new Error(
        "completion outcome is unknown; reconcile the retained intent and " +
        "original receipt; no retry"
      );
    }

    await storeCompletionRecord(
      ORDINARY_CLOSURE_CLIENT_INTENT_KEY,
      priorIntentState,
      identityKey,
      intent,
      template
    );
    var receipt;
    try {
      receipt = await complete(args);
    } catch (_error) {
      throw new Error(
        "completion callback failed; outcome unknown; reconcile; no retry"
      );
    }
    if (receipt === undefined || receipt === null) {
      throw new Error(
        "missing completion reply; outcome unknown; reconcile; no retry"
      );
    }
    await storeCompletionRecord(
      ORDINARY_CLOSURE_CLIENT_DONE_RECEIPT_KEY,
      priorReceiptState,
      identityKey,
      receipt,
      template
    );
    var view = closure.doneView({receipt: receipt, report: report});
    if (isPlainObject(view) && view.status === "done") {
      await storeCompletionRecord(
        ORDINARY_CLOSURE_CLIENT_DONE_VIEW_KEY,
        priorViewState,
        identityKey,
        expectedViewFor(view),
        template
      );
    }
    return view;
  }

  return Object.freeze({
    readStored: readStored,
    readNotice: readNotice,
    completeStored: completeStored
  });
}

function parseStart(raw, state, allowCompleted) {
  state.stage = "start";
  var envelope = unwrapEnvelope(raw, "start response");
  if (!isPlainObject(envelope)) {
    incomplete("invalid_start", "start response is not an object", state);
  }
  if (envelope.error !== undefined) {
    var errorCode = isPlainObject(envelope.error) && typeof envelope.error.code === "string"
      ? envelope.error.code
      : "unknown";
    incomplete("start_error", "start response contains an error (" + safeToken(errorCode) + ")", state);
  }
  if (envelope.tool !== undefined && envelope.tool !== "backlog_worker_start") {
    incomplete("wrong_start_tool", "start response is not backlog_worker_start", state);
  }
  var data = envelope.data !== undefined ? envelope.data : envelope;
  if (!isPlainObject(data) || !isPlainObject(data.execution)) {
    incomplete("missing_start_execution", "start response has no execution DTO", state);
  }
  var execution = data.execution;
  requireString(execution.execution_ref, "start execution_ref", state);
  requireString(execution.binding_id, "start binding_id", state);
  requireString(execution.item_id, "start item_id", state);
  requireSha256(
    execution.acceptance_condition_digest,
    "start acceptance_condition_digest",
    state
  );
  requireSha256(
    execution.delivery_description_digest,
    "start delivery_description_digest",
    state
  );
  requirePositiveInteger(execution.reservation_revision, "start reservation_revision", state);
  requireNonNegativeInteger(execution.reserved_at_ms, "start reserved_at_ms", state);
  if (!isPlainObject(execution.authority)) {
    incomplete("missing_authority", "start execution has no authority", state);
  }
  var authority = execution.authority;
  requireString(authority.approval_id, "start approval_id", state);
  requireString(authority.task_id, "start task_id", state);
  requireSha256(authority.subject_digest, "start subject_digest", state);
  requireSha256(authority.task_digest, "start task_digest", state);
  if (!isPlainObject(execution.dispatch_receipt)) {
    incomplete("missing_dispatch_receipt", "start execution has no dispatch receipt", state);
  }
  var dispatch = execution.dispatch_receipt;
  requireString(dispatch.session_binding_id, "dispatch session_binding_id", state);
  requireString(dispatch.run_id, "dispatch run_id", state);
  requireString(dispatch.worker_id, "dispatch worker_id", state);
  requireString(dispatch.profile_id, "dispatch profile_id", state);
  if (dispatch.execution_binding_digest !== undefined &&
      dispatch.execution_binding_digest !== null) {
    requireSha256(dispatch.execution_binding_digest, "dispatch execution_binding_digest", state);
  }
  if (!allowCompleted && execution.completion !== undefined && execution.completion !== null) {
    incomplete("already_completed", "start execution already has a completion record", state);
  }
  if (!allowCompleted && execution.acceptance_rejection !== undefined &&
      execution.acceptance_rejection !== null) {
    incomplete("already_rejected", "start execution already has a rejection record", state);
  }
  var outerBindings = [];
  if (envelope.binding_id !== undefined) {
    outerBindings.push(envelope.binding_id);
  }
  if (data.control_binding_id !== undefined) {
    outerBindings.push(data.control_binding_id);
  }
  for (var outerIndex = 0; outerIndex < outerBindings.length; outerIndex += 1) {
    if (typeof outerBindings[outerIndex] !== "string" ||
        outerBindings[outerIndex] !== execution.binding_id) {
      incomplete("control_binding_mismatch", "outer/control binding does not match the start execution", state);
    }
  }
  var outerBinding = outerBindings.length > 0 ? outerBindings[0] : execution.binding_id;
  var mapping = {
    approval_id: authority.approval_id,
    task_id: authority.task_id,
    subject_digest: authority.subject_digest,
    task_digest: authority.task_digest,
    item_id: execution.item_id,
    execution_ref: execution.execution_ref,
    control_binding_id: outerBinding,
    session_binding_id: dispatch.session_binding_id,
    run_id: dispatch.run_id,
    worker_id: dispatch.worker_id,
    profile_id: dispatch.profile_id
  };
  state.mapping = copyMapping(mapping);
  return {
    mapping: mapping,
    approvalId: authority.approval_id,
    taskId: authority.task_id,
    subjectDigest: authority.subject_digest,
    taskDigest: authority.task_digest,
    itemId: execution.item_id,
    executionRef: execution.execution_ref,
    controlBinding: outerBinding,
    dispatchSession: dispatch.session_binding_id,
    dispatch: dispatch,
    acceptanceConditionDigest: execution.acceptance_condition_digest,
    deliveryDescriptionDigest: execution.delivery_description_digest,
    reservationRevision: execution.reservation_revision,
    reservedAtMs: execution.reserved_at_ms
  };
}

function parseScope(raw, state) {
  state.stage = "scope";
  if (!isPlainObject(raw)) {
    incomplete("invalid_scope", "scope object is required", state);
  }
  var baseCommit = raw.base_commit !== undefined ? raw.base_commit : raw.base;
  var requestedModel = raw.requested_model !== undefined ? raw.requested_model : raw.model;
  var requestedEffort = raw.requested_effort !== undefined ? raw.requested_effort : raw.effort;
  var writePathInput = raw.write_paths !== undefined ? raw.write_paths : raw.allowed_paths;
  requireSha40(baseCommit, "scope base_commit", state);
  requireString(requestedModel, "scope requested_model", state);
  requireString(requestedEffort, "scope requested_effort", state);
  if (!Array.isArray(writePathInput) || writePathInput.length === 0) {
    incomplete("invalid_scope", "scope write_paths must be a non-empty array", state);
  }
  if (writePathInput.length > ORDINARY_CLOSURE_MAX_WRITE_PATHS) {
    incomplete("invalid_scope", "scope write_paths exceeds the bounded limit", state);
  }
  var seen = Object.create(null);
  var paths = [];
  for (var index = 0; index < writePathInput.length; index += 1) {
    var path = validateRelativePath(writePathInput[index], "scope write_paths", state);
    if (seen[path] === true) {
      incomplete("duplicate_write_path", "scope write_paths contains a duplicate", state);
    }
    seen[path] = true;
    paths.push(path);
  }
  if (!isPlainObject(raw.inspector)) {
    incomplete("invalid_inspector", "scope inspector object is required", state);
  }
  var executable = raw.inspector.executable !== undefined
    ? raw.inspector.executable
    : raw.inspector.path;
  var argv = raw.inspector.argv !== undefined ? raw.inspector.argv : raw.inspector.args;
  requireString(executable, "scope inspector executable", state);
  if (executable.charAt(0) !== "/") {
    incomplete("invalid_inspector", "scope inspector executable must be absolute", state);
  }
  if (!Array.isArray(argv) || argv.length !== 1 || argv[0] !== "inspect-delivery") {
    incomplete("invalid_inspector", "scope inspector argv must be exactly [\"inspect-delivery\"]", state);
  }
  return {
    base_commit: baseCommit,
    requested_model: requestedModel,
    requested_effort: requestedEffort,
    write_paths: paths,
    inspector: {
      executable: executable,
      argv: argv.slice()
    }
  };
}

function parseBudgets(raw, state) {
  state.stage = "budgets";
  if (!isPlainObject(raw)) {
    incomplete("invalid_budget", "budgets object is required", state);
  }
  var maxPages = requireBudget(raw.max_pages, "max_pages", state);
  var maxCalls = requireBudget(raw.max_calls, "max_calls", state);
  var deadlineMs = requireBudget(raw.deadline_ms, "deadline_ms", state);
  return {
    max_pages: maxPages,
    max_calls: maxCalls,
    deadline_ms: deadlineMs
  };
}

function validatePage(data, start, state, expectedCursor, seenEventIds) {
  state.stage = "page";
  if (!isPlainObject(data)) {
    incomplete("invalid_page", "pull data is not an object", state);
  }
  if (data.approval_id !== start.approvalId) {
    incomplete("approval_mismatch", "page approval_id does not match the retained start receipt", state);
  }
  if (data.control_binding_id !== start.controlBinding) {
    incomplete("control_binding_mismatch", "page control_binding_id does not match the chat/control binding", state);
  }
  if (data.session_binding_id !== start.dispatchSession) {
    incomplete("session_binding_mismatch", "page session_binding_id does not match dispatch", state);
  }
  if (!isPlainObject(data.batch)) {
    incomplete("missing_batch", "pull data has no batch", state);
  }
  var batch = data.batch;
  requireString(batch.batch_id, "batch_id", state);
  if (batch.binding_id !== start.dispatchSession) {
    incomplete("session_binding_mismatch", "batch binding_id does not match dispatch session", state);
  }
  if (typeof batch.has_more !== "boolean") {
    incomplete("invalid_page", "batch has_more must be boolean", state);
  }
  requireNonNegativeInteger(batch.from_seq, "batch from_seq", state);
  requireNonNegativeInteger(batch.to_seq, "batch to_seq", state);
  requireNonNegativeInteger(batch.delivered_seq, "batch delivered_seq", state);
  requireNonNegativeInteger(batch.acked_seq, "batch acked_seq", state);
  if (batch.from_seq > batch.to_seq || batch.to_seq > batch.delivered_seq) {
    incomplete("invalid_page", "batch sequence limits are inconsistent", state);
  }
  if (!Array.isArray(batch.events)) {
    incomplete("invalid_page", "batch events must be an array", state);
  }
  if (batch.events.length > ORDINARY_CLOSURE_PAGE_LIMIT) {
    incomplete("page_limit_exceeded", "batch contains more events than the requested limit", state);
  }
  if (batch.events.length === 0) {
    if (batch.from_seq !== batch.to_seq || batch.to_seq !== batch.acked_seq) {
      incomplete("invalid_page", "empty batch sequence limits are inconsistent", state);
    }
    if (expectedCursor !== null && batch.acked_seq !== expectedCursor) {
      incomplete("cursor_mismatch", "empty batch does not repeat the retained acknowledged cursor", state);
    }
    if (batch.has_more !== false) {
      incomplete("empty_page_has_more", "an empty page must not advertise more pages", state);
    }
  } else {
    if (expectedCursor !== null && batch.acked_seq !== expectedCursor) {
      incomplete("cursor_mismatch", "batch acknowledged cursor does not match the retained verified ACK", state);
    }
    var expectedFrom = expectedCursor === null
      ? batch.acked_seq + 1
      : expectedCursor + 1;
    if (batch.from_seq !== expectedFrom) {
      var relation = batch.from_seq < expectedFrom ? "repeated" : "skipped";
      incomplete(
        "sequence_continuity",
        "batch from_seq " + relation + " the retained acknowledged cursor",
        state
      );
    }
    if (batch.to_seq !== batch.from_seq + batch.events.length - 1) {
      incomplete("invalid_page", "batch sequence range does not match its event count", state);
    }
    var localEventIds = Object.create(null);
    var expectedSeq = batch.from_seq;
    for (var index = 0; index < batch.events.length; index += 1) {
      var event = batch.events[index];
      if (!isPlainObject(event)) {
        incomplete("invalid_event", "batch contains a non-object event", state);
      }
      if (event.seq !== expectedSeq) {
        incomplete("event_order_invalid", "batch events are not in contiguous order", state);
      }
      requireString(event.event_id, "event_id", state);
      if (localEventIds[event.event_id] === true ||
          seenEventIds[event.event_id] === true) {
        incomplete("duplicate_event_id", "batch repeats an event_id", state);
      }
      localEventIds[event.event_id] = true;
      if (event.binding_id !== start.dispatchSession) {
        incomplete("event_binding_mismatch", "event binding_id does not match dispatch session", state);
      }
      if (event.run_id !== start.dispatch.run_id) {
        incomplete("event_run_mismatch", "event run_id does not match the start dispatch", state);
      }
      requireNonNegativeInteger(event.revision, "event revision", state);
      requireNonNegativeInteger(event.step_seq, "event step_seq", state);
      requireString(event.kind, "event kind", state);
      if (typeof event.important !== "boolean") {
        incomplete("invalid_event", "event important must be boolean", state);
      }
      requireNonNegativeInteger(event.created_at_ms, "event created_at_ms", state);
      expectedSeq += 1;
    }
    for (var seenIndex = 0; seenIndex < batch.events.length; seenIndex += 1) {
      seenEventIds[batch.events[seenIndex].event_id] = true;
    }
  }
  return { batch: batch };
}

function validateAck(data, start, page, state, allowDuplicateAck) {
  state.stage = "ack";
  if (!isPlainObject(data)) {
    incomplete("invalid_ack", "ack data is not an object", state);
  }
  if (data.approval_id !== start.approvalId) {
    incomplete("approval_mismatch", "ack approval_id does not match the retained start receipt", state);
  }
  if (data.control_binding_id !== start.controlBinding) {
    incomplete("control_binding_mismatch", "ack control_binding_id does not match the chat/control binding", state);
  }
  if (data.session_binding_id !== start.dispatchSession) {
    incomplete("session_binding_mismatch", "ack session_binding_id does not match dispatch", state);
  }
  if (!isPlainObject(data.ack)) {
    incomplete("invalid_ack", "ack data has no ack object", state);
  }
  var ack = data.ack;
  if (ack.binding_id !== start.dispatchSession) {
    incomplete("session_binding_mismatch", "ack binding_id does not match dispatch session", state);
  }
  if (ack.batch_id !== page.batch.batch_id) {
    incomplete("ack_batch_mismatch", "ack batch_id does not match the actual batch", state);
  }
  requireNonNegativeInteger(ack.acked_seq, "ack acked_seq", state);
  if (ack.acked_seq !== page.batch.to_seq) {
    incomplete("ack_cursor_mismatch", "ack acked_seq does not match the batch cursor", state);
  }
  if (typeof ack.advanced !== "boolean") {
    incomplete("invalid_ack", "ack advanced must be boolean", state);
  }
  if (page.batch.events.length > 0 && ack.advanced !== true && !allowDuplicateAck) {
    incomplete("ack_not_advanced", "a non-empty batch acknowledgement did not advance", state);
  }
  return {
    batch_id: ack.batch_id,
    acked_seq: ack.acked_seq,
    advanced: ack.advanced
  };
}

function validateResult(data, start, scope, state) {
  state.stage = "result";
  if (!isPlainObject(data) || !isPlainObject(data.execution)) {
    incomplete("missing_result_execution", "result has no execution DTO", state);
  }
  if (data.control_binding_id !== undefined &&
      data.control_binding_id !== start.controlBinding) {
    incomplete("control_binding_mismatch", "result control_binding_id does not match the chat/control binding", state);
  }
  if (data.approval_id !== undefined && data.approval_id !== start.approvalId) {
    incomplete("approval_mismatch", "result approval_id does not match the retained start receipt", state);
  }
  if (data.task_id !== undefined && data.task_id !== start.taskId) {
    incomplete("authority_mismatch", "result task_id does not match the retained start receipt", state);
  }
  var execution = data.execution;
  if (execution.execution_ref !== start.executionRef) {
    incomplete("execution_ref_mismatch", "result execution_ref does not match the retained start receipt", state);
  }
  if (execution.binding_id !== start.controlBinding || execution.item_id !== start.itemId) {
    incomplete("item_mismatch", "result binding/item does not match the retained start receipt", state);
  }
  if (execution.acceptance_condition_digest !== start.acceptanceConditionDigest ||
      execution.delivery_description_digest !== start.deliveryDescriptionDigest ||
      execution.reservation_revision !== start.reservationRevision ||
      execution.reserved_at_ms !== start.reservedAtMs) {
    incomplete(
      "immutable_execution_mismatch",
      "result execution does not preserve the original immutable reservation fields",
      state
    );
  }
  if (!isPlainObject(execution.authority) ||
      execution.authority.approval_id !== start.approvalId ||
      execution.authority.task_id !== start.taskId ||
      execution.authority.subject_digest !== start.subjectDigest ||
      execution.authority.task_digest !== start.taskDigest) {
    incomplete("authority_mismatch", "result authority does not match the retained start receipt", state);
  }
  if (!isPlainObject(execution.dispatch_receipt) ||
      execution.dispatch_receipt.session_binding_id !== start.dispatchSession ||
      execution.dispatch_receipt.run_id !== start.dispatch.run_id ||
      execution.dispatch_receipt.worker_id !== start.dispatch.worker_id ||
      execution.dispatch_receipt.profile_id !== start.dispatch.profile_id ||
      normalizeOptional(execution.dispatch_receipt.execution_binding_digest) !==
        normalizeOptional(start.dispatch.execution_binding_digest)) {
    incomplete("dispatch_mismatch", "result dispatch receipt does not match the retained start receipt", state);
  }
  if (execution.completion !== undefined && execution.completion !== null) {
    incomplete("already_completed", "result execution already has a completion record", state);
  }
  if (execution.acceptance_rejection !== undefined &&
      execution.acceptance_rejection !== null) {
    incomplete("already_rejected", "result execution already has a rejection record", state);
  }
  if (!isPlainObject(execution.outcome)) {
    incomplete("missing_outcome", "result execution has no outcome", state);
  }
  var outcome = execution.outcome;
  if (outcome.outcome === "unknown") {
    incomplete("outcome_unknown", "worker outcome remains unknown", state);
  }
  if (outcome.outcome === "failure") {
    incomplete("outcome_failure", "worker outcome is failure", state);
  }
  if (outcome.outcome !== "success") {
    incomplete("invalid_outcome", "worker outcome is not exact success", state);
  }
  requireSha256(outcome.result_digest, "result_digest", state);
  var resultDigest = outcome.result_digest;

  if (!isPlainObject(data.item_snapshot)) {
    incomplete("missing_item_snapshot", "result has no current item_snapshot", state);
  }
  var item = data.item_snapshot;
  if (item.item_id !== start.itemId) {
    incomplete("item_mismatch", "item_snapshot item_id does not match the retained start receipt", state);
  }
  if (item.binding_id !== undefined && item.binding_id !== start.controlBinding) {
    incomplete("item_mismatch", "item_snapshot binding_id does not match the chat/control binding", state);
  }
  if (item.current_execution_ref !== start.executionRef) {
    incomplete("stale_item_execution_ref", "item_snapshot current_execution_ref is not the start execution", state);
  }
  if (!isPositiveInteger(item.revision) || item.revision < start.reservationRevision) {
    incomplete("stale_item_revision", "item_snapshot revision is missing, stale, or invalid", state);
  }
  if (item.status !== "doing") {
    incomplete("item_not_current", "item_snapshot is not the pre-acceptance Doing state", state);
  }

  if (!isPlainObject(data.worker_evidence)) {
    incomplete("missing_worker_evidence", "result has no worker_evidence", state);
  }
  var worker = data.worker_evidence;
  if (worker.approval_id !== start.approvalId || worker.task_id !== start.taskId) {
    incomplete("authority_mismatch", "worker_evidence authority does not match the start receipt", state);
  }
  if (worker.session_binding_id !== start.dispatchSession ||
      worker.run_id !== start.dispatch.run_id ||
      worker.worker_id !== start.dispatch.worker_id ||
      worker.profile_id !== start.dispatch.profile_id) {
    incomplete("dispatch_mismatch", "worker_evidence dispatch identity does not match the start receipt", state);
  }
  if (worker.worker_status !== "completed") {
    incomplete("worker_not_completed", "worker_evidence status is not completed", state);
  }
  if (!isPositiveInteger(worker.worker_revision) ||
      !isPositiveInteger(worker.attempt_count)) {
    incomplete("missing_worker_evidence", "worker_evidence revision/attempt facts are absent", state);
  }
  if (worker.requested_model !== scope.requested_model ||
      worker.requested_effort !== scope.requested_effort) {
    incomplete("requested_pin_mismatch", "worker_evidence requested pins do not match the frozen scope", state);
  }
  if (worker.observed_model !== scope.requested_model ||
      worker.observed_effort !== scope.requested_effort) {
    incomplete("observed_pin_mismatch", "worker_evidence observed pins do not match the frozen scope", state);
  }
  if (worker.exit_code !== 0) {
    incomplete("exit_nonzero", "worker_evidence exit_code is not exactly zero", state);
  }
  if (worker.signal !== undefined && worker.signal !== null) {
    incomplete("failure_signal", "worker_evidence reports a terminating signal", state);
  }
  var requiredFalseFlags = [
    "timed_out",
    "cancelled",
    "cancel_requested_before_completion",
    "missing_model_evidence",
    "missing_effort_evidence",
    "provider_error",
    "incomplete_cleanup",
    "rejected_by_diff_gate",
    "stdout_truncated",
    "stderr_truncated"
  ];
  for (var flagIndex = 0; flagIndex < requiredFalseFlags.length; flagIndex += 1) {
    if (worker[requiredFalseFlags[flagIndex]] !== false) {
      incomplete("unsafe_flag", "worker_evidence " + requiredFalseFlags[flagIndex] + " is not false", state);
    }
  }
  if (worker.violation_count !== 0) {
    incomplete("unsafe_flag", "worker_evidence violation_count is not zero", state);
  }
  if (!isNonNegativeInteger(worker.started_at_ms) ||
      !isNonNegativeInteger(worker.completed_at_ms) ||
      worker.completed_at_ms < worker.started_at_ms ||
      !isNonNegativeInteger(worker.duration_ms)) {
    incomplete("missing_timing", "worker_evidence timing facts are absent or inconsistent", state);
  }
  requireString(worker.task_worktree_path, "worker task_worktree_path", state);
  if (worker.task_worktree_path.charAt(0) !== "/") {
    incomplete("invalid_task_worktree", "worker task_worktree_path must be absolute", state);
  }

  if (!isPlainObject(data.delivery)) {
    incomplete("missing_delivery", "result has no path-free delivery evidence", state);
  }
  var delivery = data.delivery;
  if (delivery.approval_id !== start.approvalId ||
      delivery.subject_digest !== start.subjectDigest ||
      delivery.task_id !== start.taskId ||
      delivery.task_digest !== start.taskDigest ||
      delivery.binding_id !== start.controlBinding ||
      delivery.session_binding_id !== start.dispatchSession ||
      delivery.run_id !== start.dispatch.run_id ||
      delivery.worker_id !== start.dispatch.worker_id ||
      delivery.profile_id !== start.dispatch.profile_id) {
    incomplete("delivery_identity_mismatch", "delivery identity does not match the retained start receipt", state);
  }
  if (delivery.base_commit !== scope.base_commit ||
      delivery.head_commit !== scope.base_commit) {
    incomplete("base_mismatch", "delivery base/head does not match the frozen base", state);
  }
  if (delivery.delivery_kind !== "patch" || !isPositiveInteger(delivery.changed_paths_count)) {
    incomplete("missing_delivery", "delivery is not a bounded patch with changed paths", state);
  }
  requireSha256(delivery.result_digest, "delivery result_digest", state);
  requireSha256(delivery.delivery_digest, "delivery_digest", state);
  if (delivery.result_digest !== resultDigest) {
    incomplete("result_digest_mismatch", "result and delivery result digests disagree", state);
  }
  requireString(delivery.worktree_id, "delivery worktree_id", state);
  requireSha256(delivery.changed_path_set_digest, "delivery changed_path_set_digest", state);
  requireNonNegativeInteger(delivery.content_bytes, "delivery content_bytes", state);

  return {
    workerEvidence: worker,
    result: {
      outcome: "success",
      result_digest: resultDigest,
      delivery_digest: delivery.delivery_digest,
      acceptance_condition_digest: start.acceptanceConditionDigest,
      delivery_description_digest: start.deliveryDescriptionDigest,
      reservation_revision: start.reservationRevision,
      reserved_at_ms: start.reservedAtMs,
      execution_binding_digest: normalizeOptional(start.dispatch.execution_binding_digest),
      current_revision: item.revision,
      current_status: item.status,
      worker_status: worker.worker_status,
      worker_revision: worker.worker_revision,
      attempt_count: worker.attempt_count,
      exit_code: worker.exit_code,
      requested_model: worker.requested_model,
      requested_effort: worker.requested_effort,
      observed_model: worker.observed_model,
      observed_effort: worker.observed_effort,
      started_at_ms: worker.started_at_ms,
      completed_at_ms: worker.completed_at_ms,
      duration_ms: worker.duration_ms
    }
  };
}

function buildInspectionRequest(workerEvidence, scope) {
  var argv = [scope.inspector.executable].concat(scope.inspector.argv);
  argv.push("--root", workerEvidence.task_worktree_path);
  argv.push("--expected-base", scope.base_commit);
  for (var index = 0; index < scope.write_paths.length; index += 1) {
    argv.push("--allowed-path", scope.write_paths[index]);
  }
  for (var textIndex = 0; textIndex < scope.write_paths.length; textIndex += 1) {
    argv.push("--text-file", scope.write_paths[textIndex]);
  }
  return {
    root: workerEvidence.task_worktree_path,
    expected_base: scope.base_commit,
    allowed_paths: scope.write_paths.slice(),
    text_files: scope.write_paths.slice(),
    // Execute only `argv` directly. `command` is display-only shell quoting,
    // never a string to pass through a shell.
    argv: argv,
    command: argv.map(shellQuote).join(" ")
  };
}

function parseInspection(raw, state) {
  state.stage = "inspect";
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch (_error) {
      incomplete("physical_inspection_invalid_json", "physical inspector output is not JSON", state);
    }
  }
  if (!isPlainObject(raw)) {
    incomplete("physical_inspection_invalid", "physical inspector returned no object", state);
  }
  if (raw.isError === true || raw.is_error === true) {
    incomplete("physical_inspection_error", "physical inspection transport reported an error", state);
  }
  if (raw.running === true || raw.status === "running") {
    incomplete("physical_inspection_running", "physical inspector is still running", state);
  }
  if (raw.session_id !== undefined && raw.session_id !== null && raw.session_id !== "") {
    incomplete("physical_inspection_running", "physical inspector left a live command session", state);
  }
  var hasCommandWrapper = Object.prototype.hasOwnProperty.call(raw, "exit_code") ||
    Object.prototype.hasOwnProperty.call(raw, "output") ||
    Object.prototype.hasOwnProperty.call(raw, "session_id");
  if (hasCommandWrapper) {
    if (raw.truncated === true ||
        raw.output_truncated === true ||
        raw.stdout_truncated === true ||
        raw.stderr_truncated === true ||
        raw.output_omitted === true ||
        raw.omitted_output === true) {
      incomplete("physical_inspection_truncated", "physical inspector command output was truncated or omitted", state);
    }
    if (raw.exit_code !== 0) {
      incomplete("physical_inspection_exit_nonzero", "physical inspector command exited non-zero", state);
    }
    if (typeof raw.output !== "string" || raw.output.length === 0) {
      incomplete("physical_inspection_invalid", "physical inspector command returned no output text", state);
    }
    try {
      return JSON.parse(raw.output);
    } catch (_error) {
      incomplete("physical_inspection_invalid_json", "physical inspector command output is not JSON", state);
    }
  }
  if (raw.output_omitted === true || raw.omitted_output === true) {
    incomplete("physical_inspection_truncated", "physical inspector output was omitted", state);
  }
  if (typeof raw.stdout === "string") {
    try {
      return JSON.parse(raw.stdout);
    } catch (_error) {
      incomplete("physical_inspection_invalid_json", "physical inspector stdout is not JSON", state);
    }
  }
  if (isDirectInspectionReport(raw)) {
    return raw;
  }
  var envelope = unwrapEnvelope(raw, "inspect response");
  if (isPlainObject(envelope) && envelope.data !== undefined) {
    return envelope.data;
  }
  return envelope;
}

function isDirectInspectionReport(raw) {
  if (!isPlainObject(raw) || hasInspectionEnvelopeWrapper(raw)) {
    return false;
  }
  if (raw.ok === false) {
    return typeof raw.refusal_code === "string";
  }
  return raw.ok === true &&
    raw.schema_version === 1 &&
    raw.tool === "threadlane inspect-delivery";
}

function hasInspectionEnvelopeWrapper(raw) {
  if (Object.prototype.hasOwnProperty.call(raw, "structured_content") &&
      raw.structured_content !== undefined && raw.structured_content !== null) {
    return true;
  }
  if (Object.prototype.hasOwnProperty.call(raw, "structuredContent") &&
      raw.structuredContent !== undefined && raw.structuredContent !== null) {
    return true;
  }
  if (Object.prototype.hasOwnProperty.call(raw, "content") &&
      raw.content !== undefined && raw.content !== null) {
    return true;
  }
  return Object.prototype.hasOwnProperty.call(raw, "data") ||
    raw.execution !== undefined ||
    raw.batch !== undefined ||
    raw.ack !== undefined ||
    raw.completion !== undefined;
}

function validateInspection(report, request, scope, state) {
  state.stage = "inspect";
  if (!isPlainObject(report)) {
    incomplete("physical_inspection_invalid", "physical inspector report is not an object", state);
  }
  if (report.ok !== true) {
    var refusal = typeof report.refusal_code === "string" ? safeToken(report.refusal_code) : "unknown";
    incomplete("physical_inspection_refused", "physical inspector refused (" + refusal + ")", state);
  }
  if (report.schema_version !== 1 || report.tool !== "threadlane inspect-delivery" ||
      report.inspection_only !== true || report.authority_granted !== false) {
    incomplete("physical_inspection_invalid", "physical inspector report has invalid identity or authority flags", state);
  }
  if (report.root !== request.root || report.expected_base !== request.expected_base) {
    incomplete("physical_inspection_scope_mismatch", "physical inspector root/base does not match the request", state);
  }
  if (report.head_commit !== scope.base_commit) {
    incomplete("physical_head_mismatch", "physical inspector HEAD does not equal the expected base", state);
  }
  if (report.output_omitted !== false || report.text_included !== true ||
      report.untrusted_data !== true) {
    incomplete("physical_inspection_incomplete", "physical inspector omitted required text/evidence", state);
  }
  if (!Array.isArray(report.changed_paths) || report.changed_paths.length === 0) {
    incomplete("physical_inspection_incomplete", "physical inspector reported no changed paths", state);
  }
  var allowedPaths = Object.create(null);
  for (var allowedIndex = 0; allowedIndex < scope.write_paths.length; allowedIndex += 1) {
    allowedPaths[scope.write_paths[allowedIndex]] = true;
  }
  var changed = Object.create(null);
  for (var changedIndex = 0; changedIndex < report.changed_paths.length; changedIndex += 1) {
    var entry = report.changed_paths[changedIndex];
    if (!isPlainObject(entry) || typeof entry.path !== "string" ||
        entry.allowed !== true || allowedPaths[entry.path] !== true) {
      incomplete("physical_path_out_of_scope", "physical inspector reported an out-of-scope changed path", state);
    }
    if (changed[entry.path] === true) {
      incomplete("physical_inspection_invalid", "physical inspector repeated a changed path", state);
    }
    changed[entry.path] = true;
  }
  for (var scopeIndex = 0; scopeIndex < scope.write_paths.length; scopeIndex += 1) {
    if (changed[scope.write_paths[scopeIndex]] !== true) {
      incomplete("physical_path_missing", "physical inspector did not report every frozen write path as changed", state);
    }
  }
  if (!Array.isArray(report.files) || report.files.length !== scope.write_paths.length) {
    incomplete("physical_inspection_incomplete", "physical inspector file evidence does not match the frozen paths", state);
  }
  var filesByPath = Object.create(null);
  for (var fileIndex = 0; fileIndex < report.files.length; fileIndex += 1) {
    var file = report.files[fileIndex];
    if (!isPlainObject(file) || typeof file.path !== "string" || filesByPath[file.path] === true) {
      incomplete("physical_inspection_invalid", "physical inspector file evidence is malformed", state);
    }
    filesByPath[file.path] = file;
  }
  var physicalFiles = [];
  for (var pathIndex = 0; pathIndex < scope.write_paths.length; pathIndex += 1) {
    var expectedPath = scope.write_paths[pathIndex];
    var inspected = filesByPath[expectedPath];
    if (!isPlainObject(inspected) ||
        inspected.changed !== true ||
        inspected.regular_file !== true ||
        inspected.physical !== "regular_file" ||
        inspected.utf8 !== true ||
        typeof inspected.final_lf !== "boolean" ||
        inspected.text_status !== "included" ||
        inspected.text_untrusted !== true ||
        typeof inspected.text !== "string") {
      incomplete("physical_text_missing", "physical inspector did not return bounded untrusted text for " + expectedPath, state);
    }
    requireNonNegativeInteger(inspected.size_bytes, "physical size_bytes", state);
    requireSha256(inspected.sha256, "physical sha256", state);
    physicalFiles.push({
      path: expectedPath,
      size_bytes: inspected.size_bytes,
      sha256: inspected.sha256,
      utf8: true,
      final_lf: inspected.final_lf,
      text_untrusted: true,
      text: inspected.text
    });
  }
  var changedPaths = [];
  for (var actualIndex = 0; actualIndex < report.changed_paths.length; actualIndex += 1) {
    var actual = report.changed_paths[actualIndex];
    changedPaths.push({
      path: actual.path,
      change_kinds: Array.isArray(actual.change_kinds) ? actual.change_kinds.slice() : [],
      allowed: true
    });
  }
  return {
    root: request.root,
    expected_base: request.expected_base,
    head_commit: report.head_commit,
    changed_paths: changedPaths,
    files: physicalFiles
  };
}

function unwrapEnvelope(raw, label) {
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch (_error) {
      incomplete("invalid_json", label + " is not JSON", {});
    }
  }
  if (!isPlainObject(raw)) {
    incomplete("invalid_envelope", label + " must be an object", {});
  }
  if (raw.isError === true || raw.is_error === true) {
    incomplete("transport_error", label + " reports a transport error", {});
  }
  var structuredCopies = [];
  if (Object.prototype.hasOwnProperty.call(raw, "structured_content") &&
      raw.structured_content !== undefined && raw.structured_content !== null) {
    structuredCopies.push(raw.structured_content);
  }
  if (Object.prototype.hasOwnProperty.call(raw, "structuredContent") &&
      raw.structuredContent !== undefined && raw.structuredContent !== null) {
    structuredCopies.push(raw.structuredContent);
  }
  for (var copyIndex = 1; copyIndex < structuredCopies.length; copyIndex += 1) {
    if (stableStringify(structuredCopies[copyIndex]) !==
        stableStringify(structuredCopies[0])) {
      incomplete("conflicting_envelope", label + " structured copies disagree", {});
    }
  }
  var hasStructured = structuredCopies.length > 0;
  var structured = hasStructured ? structuredCopies[0] : undefined;
  var hasText = false;
  var textValue;
  if (Object.prototype.hasOwnProperty.call(raw, "content")) {
    if (raw.content !== undefined && raw.content !== null && !Array.isArray(raw.content)) {
      incomplete("invalid_envelope", label + " content must be an array", {});
    }
    if (Array.isArray(raw.content)) {
      var texts = [];
      for (var index = 0; index < raw.content.length; index += 1) {
        var part = raw.content[index];
        if (typeof part === "string") {
          if (part.length > 0) {
            texts.push(part);
          }
        } else if (isPlainObject(part) && part.type === "text" && typeof part.text === "string") {
          if (part.text.length > 0) {
            texts.push(part.text);
          }
        } else if (part !== undefined && part !== null) {
          incomplete("invalid_envelope", label + " content contains non-text data", {});
        }
      }
      if (texts.length > 0) {
        var parsedCopies = [];
        var parsedAll = true;
        for (var textIndex = 0; textIndex < texts.length; textIndex += 1) {
          try {
            parsedCopies.push(JSON.parse(texts[textIndex]));
          } catch (_error) {
            parsedAll = false;
            break;
          }
        }
        if (parsedAll) {
          for (var parsedIndex = 1; parsedIndex < parsedCopies.length; parsedIndex += 1) {
            if (stableStringify(parsedCopies[parsedIndex]) !==
                stableStringify(parsedCopies[0])) {
              incomplete("conflicting_envelope", label + " text copies disagree", {});
            }
          }
          hasText = true;
          textValue = parsedCopies[0];
        } else {
          try {
            textValue = JSON.parse(texts.join(""));
            hasText = true;
          } catch (_error) {
            if (!hasStructured) {
              incomplete("missing_structured_envelope", label + " has no parseable structured data", {});
            }
            incomplete("conflicting_envelope", label + " has non-JSON text alongside structured data", {});
          }
        }
      }
    }
  }
  if (hasStructured && hasText &&
      stableStringify(structured) !== stableStringify(textValue)) {
    incomplete("conflicting_envelope", label + " structured and text data disagree", {});
  }
  if (!hasStructured && !hasText) {
    var hasDirectData = isPlainObject(raw) &&
      (Object.prototype.hasOwnProperty.call(raw, "data") ||
        raw.execution !== undefined ||
        raw.batch !== undefined ||
        raw.ack !== undefined ||
        raw.completion !== undefined);
    if (!hasDirectData) {
      incomplete("missing_envelope", label + " has no structured or text envelope", {});
    }
  }
  if (hasStructured) {
    return structured;
  }
  if (hasText) {
    return textValue;
  }
  return raw;
}

function dataOf(envelope, expectedTool, label, state, expectedBindingId) {
  if (!isPlainObject(envelope)) {
    incomplete("invalid_envelope", label + " data is not an object", state);
  }
  if (envelope.error !== undefined) {
    var code = isPlainObject(envelope.error) && typeof envelope.error.code === "string"
      ? safeToken(envelope.error.code)
      : "unknown";
    incomplete("tool_error", label + " returned an error (" + code + ")", state);
  }
  if (envelope.tool !== undefined && envelope.tool !== expectedTool) {
    incomplete("wrong_tool", label + " response is not " + expectedTool, state);
  }
  if (expectedBindingId !== undefined &&
      envelope.binding_id !== undefined &&
      envelope.binding_id !== expectedBindingId) {
    incomplete("control_binding_mismatch", label + " outer binding does not match the chat/control binding", state);
  }
  if (expectedBindingId !== undefined &&
      envelope.control_binding_id !== undefined &&
      envelope.control_binding_id !== expectedBindingId) {
    incomplete("control_binding_mismatch", label + " outer control binding does not match the chat/control binding", state);
  }
  if (envelope.data !== undefined) {
    return envelope.data;
  }
  if (expectedTool === "approved_pull_events" &&
      envelope.approval_id !== undefined && envelope.batch !== undefined) {
    return envelope;
  }
  if (expectedTool === "approved_ack_events" &&
      envelope.approval_id !== undefined && envelope.ack !== undefined) {
    return envelope;
  }
  if (expectedTool === "backlog_worker_result" && envelope.execution !== undefined) {
    return envelope;
  }
  incomplete("missing_envelope", label + " response has no structured data envelope", state);
}

function buildIncompleteReport(reason, detail, state) {
  var report = {
    schema_version: 1,
    tool: "threadlane closure-template",
    template_version: CREATE_ORDINARY_CLOSURE_VERSION,
    status: "incomplete",
    authority_granted: false,
    stage: safeToken(state.stage || "unknown"),
    reason: safeToken(reason || "unknown"),
    detail: typeof detail === "string" ? detail.slice(0, 512) : "closure stopped",
    mapping: state.mapping === null ? null : copyMapping(state.mapping),
    last_ack: state.last_ack === null ? null : {
      batch_id: state.last_ack.batch_id,
      acked_seq: state.last_ack.acked_seq,
      advanced: state.last_ack.advanced
    },
    cursor: state.cursor,
    pending_batch: state.pending_batch === null ? null : {
      batch_id: state.pending_batch.batch_id,
      from_seq: state.pending_batch.from_seq,
      to_seq: state.pending_batch.to_seq
    }
  };
  return report;
}

function incomplete(reason, detail, state) {
  var error = new Error(reason);
  error.closureIncomplete = true;
  error.reason = reason;
  error.detail = detail;
  error.state = state;
  throw error;
}

function incompatible(reason) {
  return {
    status: "incompatible",
    reason: safeToken(reason)
  };
}

function copyMapping(mapping) {
  return {
    approval_id: mapping.approval_id,
    task_id: mapping.task_id,
    subject_digest: mapping.subject_digest,
    task_digest: mapping.task_digest,
    item_id: mapping.item_id,
    execution_ref: mapping.execution_ref,
    control_binding_id: mapping.control_binding_id,
    session_binding_id: mapping.session_binding_id,
    run_id: mapping.run_id,
    worker_id: mapping.worker_id,
    profile_id: mapping.profile_id
  };
}

function clockOnce(clock, state) {
  var value;
  try {
    value = clock();
  } catch (_error) {
    incomplete("invalid_clock", "injected clock failed", state);
  }
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    incomplete("invalid_clock", "injected clock must return a non-negative safe integer", state);
  }
  return value;
}

function shellQuote(value) {
  var text = String(value);
  return "'" + text.replace(/'/g, "'\\''") + "'";
}

function utf8ByteLength(value) {
  var text = String(value);
  var bytes = 0;
  for (var index = 0; index < text.length; index += 1) {
    var code = text.charCodeAt(index);
    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      var next = index + 1 < text.length ? text.charCodeAt(index + 1) : 0;
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    var parts = [];
    for (var index = 0; index < value.length; index += 1) {
      parts.push(stableStringify(value[index]));
    }
    return "[" + parts.join(",") + "]";
  }
  var keys = Object.keys(value).sort();
  var fields = [];
  for (var keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
    var key = keys[keyIndex];
    if (value[key] !== undefined) {
      fields.push(JSON.stringify(key) + ":" + stableStringify(value[key]));
    }
  }
  return "{" + fields.join(",") + "}";
}

function safeToken(value) {
  if (typeof value !== "string") {
    return "unknown";
  }
  return value.replace(/[^a-zA-Z0-9_.:-]/g, "_").slice(0, 96);
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isPositiveInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeInteger(value) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function requireString(value, field, state) {
  if (typeof value !== "string" || value.length === 0) {
    incomplete("missing_field", field + " must be a non-empty string", state);
  }
  return value;
}

function requireSha256(value, field, state) {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    incomplete("missing_field", field + " must be a lowercase SHA-256 hex string", state);
  }
  return value;
}

function requireSha40(value, field, state) {
  if (typeof value !== "string" || !/^[0-9a-f]{40}$/.test(value)) {
    incomplete("missing_field", field + " must be a lowercase 40-hex commit", state);
  }
  return value;
}

function requirePositiveInteger(value, field, state) {
  if (!isPositiveInteger(value)) {
    incomplete("missing_field", field + " must be a positive safe integer", state);
  }
  return value;
}

function requireNonNegativeInteger(value, field, state) {
  if (!isNonNegativeInteger(value)) {
    incomplete("missing_field", field + " must be a non-negative safe integer", state);
  }
  return value;
}

function requireBudget(value, field, state) {
  if (!isPositiveInteger(value)) {
    incomplete("invalid_budget", field + " must be a positive finite safe integer", state);
  }
  return value;
}

function validateRelativePath(value, field, state) {
  requireString(value, field, state);
  if (value.indexOf("\u0000") !== -1 || value.indexOf("\\") !== -1 || value.charAt(0) === "/") {
    incomplete("invalid_write_path", field + " must be a clean repository-relative path", state);
  }
  var segments = value.split("/");
  for (var index = 0; index < segments.length; index += 1) {
    if (segments[index] === "" || segments[index] === "." || segments[index] === "..") {
      incomplete("invalid_write_path", field + " must be a clean repository-relative path", state);
    }
  }
  return value;
}

function normalizeOptional(value) {
  return value === undefined || value === null ? null : value;
}
