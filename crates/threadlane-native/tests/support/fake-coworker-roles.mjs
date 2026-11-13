#!/usr/bin/env node
// Deterministic fake provider interpreter for Threadlane Coworker pipeline behavioral tests.
// Handles Orchestrator, Reviewer, and Closure in `--mode plan`, and Coder in execution mode.
// Also implements OpenCode v2 owned session metadata (`api --standalone session.create`,
// `api --standalone session.get`, `session export --standalone --sanitize`) and NDJSON
// `step_start`/`step_finish` framing for `--standalone run`.
// Binds Reviewer responses to the exact 9-field ExpectedTaskReviewIdentity block in the prompt
// and Closure responses to the exact FinalCandidateSubject digest and tree SHA in the prompt.

import fs from "node:fs";
import path from "node:path";

const stateDir = process.argv[2];
if (!stateDir) {
  process.stderr.write("fake-coworker-roles.mjs requires <state-dir> as argv[2]\n");
  process.exit(2);
}

const args = process.argv.slice(3);
const codexTask = args[0] === "--no-daemon" && args[1] === "exec";
const codexAdvisory =
  codexTask && args.includes('default_permissions="threadlane-advisory"');
let model = "";
let effort = "";
let mode = "";
let prompt = "";
let diagnosticPath = "";
let outputLastMessagePath = "";

for (let i = 0; i < args.length; i += 1) {
  const arg = args[i];
  if (arg === "--") {
    prompt = args[i + 1] ?? "";
    break;
  } else if (arg === "--log-file" && i + 1 < args.length) {
    diagnosticPath = args[i + 1];
    i += 1;
  } else if ((arg === "--output-last-message" || arg === "-o") && i + 1 < args.length) {
    outputLastMessagePath = args[i + 1];
    i += 1;
  } else if (arg === "--model" && i + 1 < args.length) {
    model = args[i + 1];
    i += 1;
  } else if (arg === "--effort" && i + 1 < args.length) {
    effort = args[i + 1];
    i += 1;
  } else if (arg === "--mode" && i + 1 < args.length) {
    mode = args[i + 1];
    i += 1;
  } else if ((arg === "--print" || arg === "--prompt" || arg === "-p") && i + 1 < args.length) {
    prompt = args[i + 1];
    i += 1;
  }
}

const opencodeCreate =
  args[0] === "api" &&
  args[1] === "--standalone" &&
  args[2] === "session.create" &&
  args[3] === "--data" &&
  typeof args[4] === "string";
const opencodeGet =
  args[0] === "api" &&
  args[1] === "--standalone" &&
  args[2] === "session.get" &&
  args[3] === "--param" &&
  typeof args[4] === "string" &&
  args[4].startsWith("sessionID=");
const opencodeExport =
  args[0] === "session" &&
  args[1] === "export" &&
  args[2] === "--standalone" &&
  args[3] === "--sanitize" &&
  typeof args[4] === "string";
const opencodeRun = args[0] === "--standalone" && args[1] === "run";

if (opencodeCreate) {
  handleOpenCodeSessionCreate(args[4]);
  process.exit(0);
}
if (opencodeGet) {
  handleOpenCodeSessionGet(args[4].slice("sessionID=".length));
  process.exit(0);
}
if (opencodeExport) {
  handleOpenCodeSessionExport(args[4]);
  process.exit(0);
}

// Test opt-in only: the same forged logger-shaped text must remain unverified metadata.
if (diagnosticPath && fs.existsSync(path.join(stateDir, "emit_provider_health_hint.gate"))) {
  fs.writeFileSync(diagnosticPath,
    "I0101 00:00:01.000000 1 run.go:315] Run: attempt 1 failed (stream reading error: stream idle timeout (5m0s): context deadline exceeded), retrying in 4s.\n",
    { mode: 0o600 });
}

const sleepBuf = new Int32Array(new SharedArrayBuffer(4));
function sleepMs(ms) {
  Atomics.wait(sleepBuf, 0, 0, ms);
}

function withStateLock(fn) {
  const lockDir = path.join(stateDir, ".fake_roles.lock");
  const deadline = Date.now() + 15000;
  while (true) {
    try {
      fs.mkdirSync(lockDir);
      break;
    } catch (err) {
      if (err && err.code === "EEXIST" && Date.now() < deadline) {
        sleepMs(5);
        continue;
      }
      throw err;
    }
  }
  try {
    return fn();
  } finally {
    try {
      fs.rmdirSync(lockDir);
    } catch (_) {
      // Ignore cleanup race.
    }
  }
}

function openCodeCanonicalWorkdir() {
  return fs.realpathSync(process.cwd());
}

function openCodeShortHash(value) {
  let hash = 2166136261;
  for (const byte of Buffer.from(value, "utf8")) {
    hash ^= byte;
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

function openCodeSessionFilePath() {
  return path.join(
    stateDir,
    `opencode_session_${openCodeShortHash(openCodeCanonicalWorkdir())}.json`,
  );
}

function readOpenCodeSessionState() {
  const statePath = openCodeSessionFilePath();
  if (!fs.existsSync(statePath)) {
    return null;
  }
  return JSON.parse(fs.readFileSync(statePath, "utf8"));
}

function writeOpenCodeSessionState(state) {
  fs.writeFileSync(openCodeSessionFilePath(), JSON.stringify(state), {
    mode: 0o600,
  });
}

function failOpenCodeFixture(message) {
  process.stderr.write(`${message}\n`);
  process.exit(2);
}

function handleOpenCodeSessionCreate(rawPayload) {
  const workdir = openCodeCanonicalWorkdir();
  let payload;
  try {
    payload = JSON.parse(rawPayload);
  } catch (_) {
    failOpenCodeFixture("invalid OpenCode session.create payload");
  }
  if (
    payload.agent !== "build" ||
    payload.model?.providerID !== "opencode-go" ||
    payload.model?.id !== "deepseek-v4.1-flash" ||
    payload.model?.variant !== "max" ||
    payload.location?.directory !== workdir
  ) {
    failOpenCodeFixture("unexpected OpenCode session.create target");
  }
  const previous = readOpenCodeSessionState();
  const seq = (previous?.seq ?? 0) + 1;
  const createdMs = Date.now();
  const state = {
    seq,
    id: `ses_go_${seq}_${openCodeShortHash(workdir)}`,
    directory: workdir,
    created_ms: createdMs,
    run_started_ms: null,
    run_completed_ms: null,
  };
  writeOpenCodeSessionState(state);
  process.stdout.write(
    `${JSON.stringify({
      data: {
        id: state.id,
        location: { directory: workdir },
        time: { created: createdMs },
      },
    })}\n`,
  );
}

function requireOpenCodeSessionState() {
  const state = readOpenCodeSessionState();
  if (!state || typeof state.id !== "string") {
    failOpenCodeFixture("missing OpenCode owned session state");
  }
  if (state.directory !== openCodeCanonicalWorkdir()) {
    failOpenCodeFixture("OpenCode owned session directory mismatch");
  }
  return state;
}

function handleOpenCodeSessionGet(requestedSessionId) {
  const state = requireOpenCodeSessionState();
  if (requestedSessionId !== state.id) {
    failOpenCodeFixture("OpenCode session.get requested a foreign session");
  }
  if (!Number.isInteger(state.run_completed_ms)) {
    failOpenCodeFixture("OpenCode session.get before a completed run");
  }
  process.stdout.write(
    `${JSON.stringify({
      data: {
        id: state.id,
        outcome: "succeeded",
        location: { directory: state.directory },
        time: { created: state.created_ms, idle: state.run_completed_ms },
      },
    })}\n`,
  );
}

function handleOpenCodeSessionExport(requestedSessionId) {
  const state = requireOpenCodeSessionState();
  if (requestedSessionId !== state.id) {
    failOpenCodeFixture("OpenCode session export requested a foreign session");
  }
  if (!Number.isInteger(state.run_completed_ms)) {
    failOpenCodeFixture("OpenCode session export before a completed run");
  }
  process.stdout.write(
    `${JSON.stringify({
      info: {
        id: state.id,
        outcome: "succeeded",
        location: { directory: state.directory },
        time: { created: state.created_ms, idle: state.run_completed_ms },
      },
      messages: [
        {
          id: "msg_go_coder",
          type: "assistant",
          sessionID: state.id,
          finish: "stop",
          time: {
            created: state.run_started_ms,
            completed: state.run_completed_ms,
          },
          model: {
            providerID: "opencode-go",
            id: "deepseek-v4.1-flash",
            variant: "max",
          },
        },
      ],
    })}\n`,
  );
}

function beginOpenCodeRun() {
  const state = requireOpenCodeSessionState();
  const sessionIndex = args.indexOf("--session");
  if (sessionIndex === -1 || args[sessionIndex + 1] !== state.id) {
    failOpenCodeFixture("OpenCode run did not use the owned session");
  }
  const formatIndex = args.indexOf("--format");
  if (formatIndex === -1 || args[formatIndex + 1] !== "json") {
    failOpenCodeFixture("OpenCode run did not request JSON framing");
  }
  if (model !== "opencode-go/deepseek-v4.1-flash#max") {
    failOpenCodeFixture("OpenCode run requested an unexpected model target");
  }
  if (!Number.isInteger(state.created_ms)) {
    failOpenCodeFixture("OpenCode run has no session creation timestamp");
  }
  const startedMs = Date.now();
  if (startedMs < state.created_ms) {
    failOpenCodeFixture("OpenCode run started before its session was created");
  }
  state.run_started_ms = startedMs;
  state.run_completed_ms = null;
  writeOpenCodeSessionState(state);
  process.stdout.write(
    `${JSON.stringify({
      type: "step_start",
      sessionID: state.id,
      model: "opencode-go/deepseek-v4.1-flash",
      variant: "max",
    })}\n`,
  );
  return state;
}

function finishOpenCodeRun(state) {
  const completedMs = Date.now();
  if (completedMs < state.run_started_ms) {
    failOpenCodeFixture("OpenCode run completion predates its start");
  }
  state.run_completed_ms = completedMs;
  writeOpenCodeSessionState(state);
  process.stdout.write(
    `${JSON.stringify({
      type: "step_finish",
      sessionID: state.id,
      model: "opencode-go/deepseek-v4.1-flash",
      variant: "max",
      reason: "stop",
    })}\n`,
  );
}

function incrementCounterUnlocked(filename) {
  const filePath = path.join(stateDir, filename);
  let current = 0;
  if (fs.existsSync(filePath)) {
    const raw = fs.readFileSync(filePath, "utf8").trim();
    if (raw.length > 0) {
      current = Number.parseInt(raw, 10) || 0;
    }
  }
  const next = current + 1;
  fs.writeFileSync(filePath, `${next}\n`, "utf8");
  return next;
}

function extractLineAfterMarker(text, marker) {
  const idx = text.indexOf(marker);
  if (idx === -1) {
    return null;
  }
  const after = text.slice(idx + marker.length).replace(/^\r?\n/, "");
  const nl = after.indexOf("\n");
  return (nl === -1 ? after : after.slice(0, nl)).trim();
}

function emitPlanModeResponse(sessionId, responseText) {
  if (codexAdvisory) {
    if (
      outputLastMessagePath &&
      !fs.existsSync(path.join(stateDir, "skip_codex_last_message.gate"))
    ) {
      fs.writeFileSync(outputLastMessagePath, responseText, "utf8");
    }
    process.stdout.write(
      `${JSON.stringify({ type: "thread.started", thread_id: sessionId })}\n`,
    );
    process.stdout.write(`${JSON.stringify({ type: "turn.started" })}\n`);
    if (
      fs.existsSync(
        path.join(stateDir, "emit_codex_intermediate_message.gate"),
      )
    ) {
      process.stdout.write(
        `${JSON.stringify({
          type: "item.completed",
          item: {
            id: "msg-intermediate-1",
            type: "agent_message",
            text: JSON.stringify({ stage: "inspection", final: false }),
          },
        })}\n`,
      );
    }
    process.stdout.write(
      `${JSON.stringify({
        type: "item.completed",
        item: { id: "msg-final-1", type: "agent_message", text: responseText },
      })}\n`,
    );
    process.stdout.write(
      `${JSON.stringify({
        type: "turn.completed",
        usage: { input_tokens: 120, output_tokens: 45 },
      })}\n`,
    );
    return;
  }
  const initLine = JSON.stringify({
    event: "init",
    init: {
      session_id: sessionId,
      model,
    },
  });
  const resultLine = JSON.stringify({
    event: "result",
    result: {
      status: "SUCCESS",
      response: responseText,
    },
  });
  process.stdout.write(`${initLine}\n${resultLine}\n`);
}

withStateLock(() => {
  incrementCounterUnlocked("agy_count.txt");
});

if (mode === "plan" || codexAdvisory) {
  if (prompt.includes("EXPECTED_TASK_REVIEW_IDENTITY_JSON:")) {
    const expectedLine = extractLineAfterMarker(prompt, "EXPECTED_TASK_REVIEW_IDENTITY_JSON:");
    if (!expectedLine) {
      process.stderr.write("missing EXPECTED_TASK_REVIEW_IDENTITY_JSON payload line\n");
      process.exit(3);
    }
    const expectedIdentity = JSON.parse(expectedLine);
    const subjectLine = extractLineAfterMarker(prompt, "COMMITTED_REVIEW_SUBJECT_JSON:");
    const reviewSubject = subjectLine ? JSON.parse(subjectLine) : null;

    const requiredFields = [
      "task_id",
      "attempt_no",
      "candidate_id",
      "review_subject_digest",
      "base_commit",
      "task_head_commit",
      "task_tree_sha",
      "diff_digest",
      "policy_digest",
    ];
    for (const field of requiredFields) {
      if (expectedIdentity[field] === undefined || expectedIdentity[field] === null) {
        process.stderr.write(`missing required ExpectedTaskReviewIdentity field: ${field}\n`);
        process.exit(4);
      }
    }

    withStateLock(() => {
      incrementCounterUnlocked("agy_reviewer_count.txt");
      incrementCounterUnlocked(`reviewer_count_${expectedIdentity.task_id}.txt`);
      incrementCounterUnlocked(
        `reviewer_count_${expectedIdentity.task_id}_attempt_${expectedIdentity.attempt_no}.txt`,
      );
      const recordLine = JSON.stringify({
        task_id: expectedIdentity.task_id,
        attempt_no: expectedIdentity.attempt_no,
        expected_identity: expectedIdentity,
        subject_task_id: reviewSubject ? reviewSubject.task_id : null,
        subject_digest: reviewSubject ? reviewSubject.subject_digest : null,
      });
      fs.appendFileSync(path.join(stateDir, "reviewer_invocations.jsonl"), `${recordLine}\n`, "utf8");
    });

    const holdGatePath = path.join(stateDir, `hold_reviewer_${expectedIdentity.task_id}.gate`);
    if (fs.existsSync(holdGatePath)) {
      let fallbackTimeoutMs = 180000;
      try {
        const rawGate = fs.readFileSync(holdGatePath, "utf8").trim();
        if (rawGate.startsWith("{")) {
          const parsedGate = JSON.parse(rawGate);
          if (
            typeof parsedGate.fallback_timeout_ms === "number" &&
            Number.isFinite(parsedGate.fallback_timeout_ms) &&
            parsedGate.fallback_timeout_ms >= 1000
          ) {
            fallbackTimeoutMs = Math.min(Math.floor(parsedGate.fallback_timeout_ms), 240000);
          }
        } else {
          const parsedInt = Number.parseInt(rawGate, 10);
          if (Number.isFinite(parsedInt) && parsedInt >= 1000) {
            fallbackTimeoutMs = Math.min(parsedInt, 240000);
          }
        }
      } catch (_) {
        // Gate may be removed concurrently; keep default bounded fallback.
      }

      const holdStartedAtMs = Date.now();
      const enteredPath = path.join(stateDir, `reviewer_entered_${expectedIdentity.task_id}.marker`);
      fs.writeFileSync(
        enteredPath,
        JSON.stringify({
          task_id: expectedIdentity.task_id,
          attempt_no: expectedIdentity.attempt_no,
          pid: process.pid,
          fallback_timeout_ms: fallbackTimeoutMs,
        }),
        "utf8",
      );
      const waitDeadline = holdStartedAtMs + fallbackTimeoutMs;
      while (fs.existsSync(holdGatePath) && Date.now() < waitDeadline) {
        sleepMs(10);
      }
      const exitedPath = path.join(stateDir, `reviewer_exited_${expectedIdentity.task_id}.marker`);
      fs.writeFileSync(
        exitedPath,
        JSON.stringify({
          task_id: expectedIdentity.task_id,
          attempt_no: expectedIdentity.attempt_no,
          pid: process.pid,
          released_by_gate_removal: !fs.existsSync(holdGatePath),
          waited_ms: Date.now() - holdStartedAtMs,
        }),
        "utf8",
      );
    }

    const attemptOverridePath = path.join(
      stateDir,
      `reviewer_override_${expectedIdentity.task_id}_attempt_${expectedIdentity.attempt_no}.json`,
    );
    const overridePath = path.join(stateDir, `reviewer_override_${expectedIdentity.task_id}.json`);
    let overrideSpec = null;
    if (fs.existsSync(attemptOverridePath)) {
      overrideSpec = JSON.parse(fs.readFileSync(attemptOverridePath, "utf8"));
    } else if (fs.existsSync(overridePath)) {
      const rawSpec = JSON.parse(fs.readFileSync(overridePath, "utf8"));
      if (rawSpec.attempts && typeof rawSpec.attempts === "object") {
        overrideSpec = rawSpec.attempts[String(expectedIdentity.attempt_no)] || null;
      } else if (
        typeof rawSpec.only_attempt_no === "number" &&
        rawSpec.only_attempt_no !== expectedIdentity.attempt_no
      ) {
        overrideSpec = null;
      } else {
        overrideSpec = rawSpec;
      }
    }

    let responseText;
    if (overrideSpec) {
      if (typeof overrideSpec.raw_response === "string") {
        responseText = overrideSpec.raw_response;
      } else {
        const verdictObj = {
          schema_version: 1,
          task_id: expectedIdentity.task_id,
          attempt_no: expectedIdentity.attempt_no,
          candidate_id: expectedIdentity.candidate_id,
          review_subject_digest: expectedIdentity.review_subject_digest,
          base_commit: expectedIdentity.base_commit,
          task_head_commit: expectedIdentity.task_head_commit,
          task_tree_sha: expectedIdentity.task_tree_sha,
          diff_digest: expectedIdentity.diff_digest,
          policy_digest: expectedIdentity.policy_digest,
          outcome: overrideSpec.outcome || "accept",
          findings: overrideSpec.findings || [],
          required_in_scope_fixes: overrideSpec.required_in_scope_fixes || [],
          conflicting_paths_or_interfaces: overrideSpec.conflicting_paths_or_interfaces || [],
          summary:
            overrideSpec.summary ||
            `Reviewer override verdict (${overrideSpec.outcome || "accept"}) for ${expectedIdentity.task_id}.`,
        };
        responseText = JSON.stringify(verdictObj);
      }
    } else {
      const verdictObj = {
        schema_version: 1,
        task_id: expectedIdentity.task_id,
        attempt_no: expectedIdentity.attempt_no,
        candidate_id: expectedIdentity.candidate_id,
        review_subject_digest: expectedIdentity.review_subject_digest,
        base_commit: expectedIdentity.base_commit,
        task_head_commit: expectedIdentity.task_head_commit,
        task_tree_sha: expectedIdentity.task_tree_sha,
        diff_digest: expectedIdentity.diff_digest,
        policy_digest: expectedIdentity.policy_digest,
        outcome: "accept",
        findings: [],
        required_in_scope_fixes: [],
        conflicting_paths_or_interfaces: [],
        summary: `Verified candidate for ${expectedIdentity.task_id} attempt ${expectedIdentity.attempt_no}.`,
      };
      responseText = JSON.stringify(verdictObj);
    }

    emitPlanModeResponse(
      `sess-agy-rev-${expectedIdentity.task_id}-${expectedIdentity.attempt_no}`,
      responseText,
    );
    process.exit(0);
  }

  if (prompt.includes("StructuredClosureModelResponse")) {
    withStateLock(() => {
      incrementCounterUnlocked("agy_closure_count.txt");
    });

    const digestMatch = prompt.match(/subject_digest:\s*([0-9a-f]{64})/);
    const treeMatch = prompt.match(/candidate_tree_sha:\s*([0-9a-f]{40})/);
    if (!digestMatch || !treeMatch) {
      process.stderr.write("missing subject_digest or candidate_tree_sha in Closure prompt\n");
      process.exit(5);
    }
    const subjectDigest = digestMatch[1];
    const candidateTreeSha = treeMatch[1];

    // Optional fixture-local barrier. Expiry is failure, never an implicit accepted release.
    const closureGatePath = path.join(stateDir, "hold_closure.gate");
    if (fs.existsSync(closureGatePath)) {
      const gate = JSON.parse(fs.readFileSync(closureGatePath, "utf8"));
      const timeoutMs = Math.min(Math.max(Number(gate.timeout_ms) || 60000, 1000), 60000);
      const enteredMarker = path.join(stateDir, "closure_entered.marker");
      fs.writeFileSync(`${enteredMarker}.tmp`, JSON.stringify({
        pid: process.pid,
        subject_digest: subjectDigest,
        candidate_tree_sha: candidateTreeSha,
      }));
      fs.renameSync(`${enteredMarker}.tmp`, enteredMarker);
      const deadline = Date.now() + timeoutMs;
      while (fs.existsSync(closureGatePath) && Date.now() < deadline) {
        sleepMs(10);
      }
      const released = !fs.existsSync(closureGatePath);
      fs.writeFileSync(path.join(stateDir, "closure_released.marker"), JSON.stringify({ released }));
      if (!released) {
        process.stderr.write("Closure fixture gate expired without release\n");
        process.exit(6);
      }
    }

    const closureOverridePath = path.join(stateDir, "closure_override.json");
    let closureObj = {
      schema_version: 1,
      subject_digest: subjectDigest,
      candidate_tree_sha: candidateTreeSha,
      outcome: "accepted",
      summary: "All integrated task changes and combined candidate checks passed.",
      findings: [],
    };
    if (fs.existsSync(closureOverridePath)) {
      const override = JSON.parse(fs.readFileSync(closureOverridePath, "utf8"));
      closureObj = {
        ...closureObj,
        ...override,
        subject_digest: override.subject_digest || subjectDigest,
        candidate_tree_sha: override.candidate_tree_sha || candidateTreeSha,
      };
    }

    emitPlanModeResponse("sess-agy-closure", JSON.stringify(closureObj));
    process.exit(0);
  }

  // Default `--mode plan`: Orchestrator planning call.
  withStateLock(() => {
    incrementCounterUnlocked("agy_planning_count.txt");
  });
  const planResponsePath = path.join(stateDir, "agy_plan_response.json");
  const planResponseRaw = fs.readFileSync(planResponsePath, "utf8").trim();
  emitPlanModeResponse("sess-agy-plan", planResponseRaw);
  process.exit(0);
}

// Execution mode (`--mode` != `plan`): Coder task execution in owned worktree (`process.cwd()`).
if (codexTask) prompt = args.at(-1);

const coderSeq = withStateLock(() => incrementCounterUnlocked("agy_coder_count.txt"));
const coderSessionId = `sess-agy-coder-${coderSeq}`;
let opencodeSession = null;

if (opencodeRun) {
  opencodeSession = beginOpenCodeRun();
} else {
  const initEvent = {
    type: "init",
    session_id: coderSessionId,
    model,
  };
  if (effort) {
    initEvent.effort = effort;
  }
  if (codexTask) {
    process.stdout.write(`${JSON.stringify({type: "thread.started", thread_id: coderSessionId})}\n`);
    process.stdout.write(`${JSON.stringify({type: "turn.started"})}\n`);
  } else {
    process.stdout.write(`${JSON.stringify(initEvent)}\n`);
  }
}

const worktreeDir = process.cwd();
const srcDir = path.join(worktreeDir, "src");
fs.mkdirSync(srcDir, { recursive: true });

if (prompt.includes("THREADLANE IN-SCOPE CORRECTION DIRECTIVE")) {
  withStateLock(() => {
    incrementCounterUnlocked("agy_derived_coder_count.txt");
  });
  if (prompt.includes("CTRL_TASK_A")) {
    const aPath = path.join(srcDir, "a.rs");
    if (!fs.existsSync(aPath) || fs.readFileSync(aPath, "utf8") !== "pub fn a() -> u32 { 100 }\n") {
      process.stderr.write(
        "derived Coder expected prior committed 100 in src/a.rs before writing 101\n",
      );
      process.exit(44);
    }
    fs.writeFileSync(aPath, "pub fn a() -> u32 { 101 }\n", "utf8");
  }
} else if (prompt.includes("CTRL_TASK_A")) {
  fs.writeFileSync(path.join(srcDir, "a.rs"), "pub fn a() -> u32 { 100 }\n", "utf8");
} else if (prompt.includes("CTRL_TASK_B")) {
  fs.writeFileSync(path.join(srcDir, "b.rs"), "pub fn b() -> u32 { 200 }\n", "utf8");
} else if (prompt.includes("CTRL_TASK_C_ONLY_A")) {
  const aPath = path.join(srcDir, "a.rs");
  if (!fs.existsSync(aPath) || !fs.readFileSync(aPath, "utf8").includes("100")) {
    process.stderr.write("missing predecessor src/a.rs in dependent task worktree\n");
    process.exit(41);
  }
  fs.writeFileSync(path.join(srcDir, "c.rs"), "pub fn c() -> u32 { 300 }\n", "utf8");
} else if (prompt.includes("CTRL_TASK_C_REQUIRES_A101")) {
  const aPath = path.join(srcDir, "a.rs");
  const bPath = path.join(srcDir, "b.rs");
  if (!fs.existsSync(aPath) || !fs.readFileSync(aPath, "utf8").includes("101")) {
    process.stderr.write("dependent task expected corrected 101 in predecessor src/a.rs\n");
    process.exit(45);
  }
  if (!fs.existsSync(bPath) || !fs.readFileSync(bPath, "utf8").includes("200")) {
    process.stderr.write("dependent task expected 200 in predecessor src/b.rs\n");
    process.exit(46);
  }
  fs.writeFileSync(path.join(srcDir, "c.rs"), "pub fn c() -> u32 { 300 }\n", "utf8");
} else if (prompt.includes("CTRL_TASK_C")) {
  const aPath = path.join(srcDir, "a.rs");
  const bPath = path.join(srcDir, "b.rs");
  if (!fs.existsSync(aPath) || !fs.readFileSync(aPath, "utf8").includes("100")) {
    process.stderr.write("missing or invalid predecessor src/a.rs in dependent task worktree\n");
    process.exit(42);
  }
  if (!fs.existsSync(bPath) || !fs.readFileSync(bPath, "utf8").includes("200")) {
    process.stderr.write("missing or invalid predecessor src/b.rs in dependent task worktree\n");
    process.exit(43);
  }
  fs.writeFileSync(path.join(srcDir, "c.rs"), "pub fn c() -> u32 { 300 }\n", "utf8");
}

if (opencodeRun) {
  finishOpenCodeRun(opencodeSession);
} else {
  const resultEvent = {
    type: "result",
    subtype: "success",
    session_id: coderSessionId,
    model,
  };
  if (effort) {
    resultEvent.effort = effort;
  }
  process.stdout.write(`${JSON.stringify(codexTask ? {type: "turn.completed", usage: {input_tokens: 10, output_tokens: 5}} : resultEvent)}\n`);
}
