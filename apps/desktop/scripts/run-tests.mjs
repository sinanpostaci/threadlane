#!/usr/bin/env node
// Discover frontend tests with Node's filesystem APIs so validation does not
// depend on shell globstar support. The runner keeps type stripping enabled and
// leaves Node's normal per-file test isolation in place.

import { spawnSync } from "node:child_process";
import { readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const TEST_SUFFIX = ".test.ts";
const ALLOWED_NODE_TEST_ARGS = new Set([
  "--test-isolation=none",
  "--test-isolation=process",
]);

export const DEFAULT_SOURCE_DIR = path.resolve(SCRIPT_DIR, "..", "src");
export const SELF_TEST_FILE = path.join(SCRIPT_DIR, "run-tests.test.mjs");

/**
 * Recursively discover every `*.test.ts` file below `sourceDir`, including files
 * directly in the source root. Paths are absolute and sorted for stable output.
 */
export function discoverTestFiles(sourceDir = DEFAULT_SOURCE_DIR) {
  const files = [];
  const pendingDirectories = [sourceDir];

  while (pendingDirectories.length > 0) {
    const directory = pendingDirectories.pop();
    const entries = readdirSync(directory, { withFileTypes: true });
    for (const entry of entries) {
      const entryPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        pendingDirectories.push(entryPath);
      } else if (entry.isFile() && entry.name.endsWith(TEST_SUFFIX)) {
        files.push(entryPath);
      }
    }
  }

  files.sort();
  return files;
}

export function parseTestArguments(args) {
  const forwardedArgs = [];
  for (const arg of args) {
    if (!ALLOWED_NODE_TEST_ARGS.has(arg)) {
      throw new Error(
        `Unsupported argument "${arg}". Only --test-isolation=none and ` +
          "--test-isolation=process may be forwarded to the Node.js test runner.",
      );
    }
    forwardedArgs.push(arg);
  }
  return forwardedArgs;
}

/**
 * Run the discovered frontend tests and return the child process exit status.
 * A missing test set is an error, never a successful empty run.
 */
export function runTests({
  sourceDir = DEFAULT_SOURCE_DIR,
  nodeTestArgs = [],
  includeSelfTest = true,
  spawn = spawnSync,
  nodePath = process.execPath,
} = {}) {
  const discoveredFiles = discoverTestFiles(sourceDir);
  if (discoveredFiles.length === 0) {
    throw new Error(
      `No test files matching src/**/*${TEST_SUFFIX} were found under ${sourceDir}.`,
    );
  }

  // The runner regression test lives in scripts/, so include it explicitly
  // while keeping the zero-file guard scoped to application tests.
  const testFiles = includeSelfTest
    ? [...discoveredFiles, SELF_TEST_FILE]
    : discoveredFiles;

  const child = spawn(
    nodePath,
    ["--experimental-strip-types", "--test", ...nodeTestArgs, ...testFiles],
    { stdio: "inherit" },
  );

  if (child.error) {
    throw child.error;
  }
  if (child.signal) {
    throw new Error(`Frontend test process terminated by signal ${child.signal}.`);
  }
  if (typeof child.status !== "number") {
    throw new Error("Frontend test process did not report an exit status.");
  }
  return child.status;
}

export function main(args = process.argv.slice(2), options = {}) {
  const { stderr = process.stderr, ...runOptions } = options;

  let nodeTestArgs;
  try {
    nodeTestArgs = parseTestArguments(args);
  } catch (error) {
    stderr.write(`${formatError(error)}\n`);
    return 2;
  }

  try {
    return runTests({ ...runOptions, nodeTestArgs });
  } catch (error) {
    stderr.write(`${formatError(error)}\n`);
    return 1;
  }
}

function formatError(error) {
  return error instanceof Error ? error.message : String(error);
}

const invokedAsScript =
  process.argv[1] !== undefined &&
  pathToFileURL(process.argv[1]).href === import.meta.url;

if (invokedAsScript) {
  process.exitCode = main();
}
