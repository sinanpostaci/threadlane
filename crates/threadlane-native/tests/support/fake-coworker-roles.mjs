#!/usr/bin/env node
// Deterministic fake provider interpreter for Threadlane Coworker pipeline behavioral tests.
// Handles Orchestrator, Reviewer, and Closure in `--mode plan`, and Coder in execution mode.
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
let model = "";
let effort = "";
let mode = "";
let prompt = "";

for (let i = 0; i < args.length; i += 1) {
  const arg = args[i];
  if (arg === "--model" && i + 1 < args.length) {
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

if (mode === "plan") {
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
const coderSeq = withStateLock(() => incrementCounterUnlocked("agy_coder_count.txt"));
const coderSessionId = `sess-agy-coder-${coderSeq}`;

const initEvent = {
  type: "init",
  session_id: coderSessionId,
  model,
};
if (effort) {
  initEvent.effort = effort;
}
process.stdout.write(`${JSON.stringify(initEvent)}\n`);

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

const resultEvent = {
  type: "result",
  subtype: "success",
  session_id: coderSessionId,
  model,
};
if (effort) {
  resultEvent.effort = effort;
}
process.stdout.write(`${JSON.stringify(resultEvent)}\n`);
