import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  SELF_TEST_FILE,
  discoverTestFiles,
  main,
} from "./run-tests.mjs";

function makeSourceFixture() {
  const sourceDir = mkdtempSync(path.join(os.tmpdir(), "threadlane-test-runner-"));
  mkdirSync(path.join(sourceDir, "nested", "deeper"), { recursive: true });
  writeFileSync(path.join(sourceDir, "root.test.ts"), "");
  writeFileSync(path.join(sourceDir, "nested", "middle.test.ts"), "");
  writeFileSync(path.join(sourceDir, "nested", "deeper", "leaf.test.ts"), "");
  writeFileSync(path.join(sourceDir, "root.test.tsx"), "");
  writeFileSync(path.join(sourceDir, "nested", "middle.spec.ts"), "");
  writeFileSync(path.join(sourceDir, "nested", "helper.ts"), "");
  return sourceDir;
}

function makeEmptyFixture() {
  return mkdtempSync(path.join(os.tmpdir(), "threadlane-test-runner-empty-"));
}

function captureStderr() {
  return {
    output: "",
    write(chunk) {
      this.output += chunk;
    },
  };
}

describe("discoverTestFiles", () => {
  it("finds root and nested .test.ts files and ignores other files", (t) => {
    const sourceDir = makeSourceFixture();
    t.after(() => rmSync(sourceDir, { recursive: true, force: true }));

    assert.deepEqual(discoverTestFiles(sourceDir), [
      path.join(sourceDir, "nested", "deeper", "leaf.test.ts"),
      path.join(sourceDir, "nested", "middle.test.ts"),
      path.join(sourceDir, "root.test.ts"),
    ].sort());
  });

  it("returns an empty list for a source tree without tests", (t) => {
    const sourceDir = makeEmptyFixture();
    t.after(() => rmSync(sourceDir, { recursive: true, force: true }));

    assert.deepEqual(discoverTestFiles(sourceDir), []);
  });
});

describe("main", () => {
  it("fails without spawning when discovery finds no test files", (t) => {
    const sourceDir = makeEmptyFixture();
    t.after(() => rmSync(sourceDir, { recursive: true, force: true }));
    const stderr = captureStderr();
    let spawned = false;

    const exitCode = main([], {
      sourceDir,
      stderr,
      spawn: () => {
        spawned = true;
        return { status: 0, signal: null };
      },
    });

    assert.equal(exitCode, 1);
    assert.equal(spawned, false);
    assert.match(stderr.output, /No test files matching src\/\*\*\/\*\.test\.ts/);
  });

  it("forwards the bounded isolation option and preserves a failing status", (t) => {
    const sourceDir = makeSourceFixture();
    t.after(() => rmSync(sourceDir, { recursive: true, force: true }));
    const stderr = captureStderr();
    const calls = [];

    const exitCode = main(["--test-isolation=none"], {
      sourceDir,
      stderr,
      spawn: (command, args, options) => {
        calls.push({ command, args, options });
        return { status: 7, signal: null };
      },
    });

    assert.equal(exitCode, 7);
    assert.equal(stderr.output, "");
    assert.equal(calls.length, 1);
    const { command, args, options } = calls[0];
    assert.equal(command, process.execPath);
    assert.equal(args[0], "--experimental-strip-types");
    assert.equal(args[1], "--test");
    assert.equal(args[2], "--test-isolation=none");
    assert.deepEqual(args.slice(3, -1), discoverTestFiles(sourceDir));
    assert.equal(args.at(-1), SELF_TEST_FILE);
    assert.equal(options.stdio, "inherit");
  });

  it("rejects arguments outside the bounded forwarding surface", () => {
    const stderr = captureStderr();

    const exitCode = main(["--test-reporter=tap"], {
      stderr,
      spawn: () => {
        assert.fail("spawn must not be called for an unsupported argument");
      },
    });

    assert.equal(exitCode, 2);
    assert.match(stderr.output, /Unsupported argument "--test-reporter=tap"/);
  });
});
