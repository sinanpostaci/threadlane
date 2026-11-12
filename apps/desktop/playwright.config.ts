import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from '@playwright/test';

const configDir = path.dirname(fileURLToPath(import.meta.url));

const FORBIDDEN_SHARED_ARTIFACT_ROOTS = new Set([
  '/',
  '/bin',
  '/boot',
  '/dev',
  '/etc',
  '/home',
  '/lib',
  '/lib64',
  '/nix',
  '/nix/store',
  '/opt',
  '/proc',
  '/root',
  '/run',
  '/sbin',
  '/srv',
  '/sys',
  '/tmp',
  '/usr',
  '/var',
  '/var/tmp',
]);

function validateSafeAbsolutePath(raw: string, label: string): string {
  if (raw.length === 0 || raw.includes('\0')) {
    throw new Error(`${label} must be a non-empty path without NUL bytes.`);
  }
  if (raw !== raw.trim()) {
    throw new Error(`${label} must not have leading or trailing whitespace.`);
  }
  if (raw.startsWith('-')) {
    throw new Error(`${label} must not start with '-'.`);
  }
  if (!path.isAbsolute(raw)) {
    throw new Error(`${label} must be an absolute path: ${raw}`);
  }
  if (raw.split(path.sep).includes('..')) {
    throw new Error(`${label} must not contain '..' traversal segments: ${raw}`);
  }
  return path.resolve(raw);
}

function ensureNoSymlinkChain(targetPath: string): void {
  const resolved = path.resolve(targetPath);
  const parsed = path.parse(resolved);
  let current = parsed.root;
  const relativeParts = resolved
    .slice(parsed.root.length)
    .split(path.sep)
    .filter((segment) => segment.length > 0);

  for (const segment of relativeParts) {
    current = path.join(current, segment);
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(current);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
        break;
      }
      throw err;
    }
    if (stat.isSymbolicLink()) {
      throw new Error(`Refusing symlinked acceptance artifact path component: ${current}`);
    }
    if (!stat.isDirectory()) {
      throw new Error(`Refusing non-directory acceptance artifact path component: ${current}`);
    }
  }
}

function findEnclosingGitToplevel(targetPath: string): string | null {
  let probe = path.resolve(targetPath);
  while (!fs.existsSync(probe)) {
    const parent = path.dirname(probe);
    if (parent === probe) {
      return null;
    }
    probe = parent;
  }
  const stat = fs.lstatSync(probe);
  const probeDir = stat.isDirectory() ? probe : path.dirname(probe);
  try {
    const out = execFileSync('git', ['-C', probeDir, 'rev-parse', '--show-toplevel'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      env: {
        PATH: process.env.PATH ?? '',
        GIT_TERMINAL_PROMPT: '0',
      },
    }).trim();
    return out.length > 0 ? path.resolve(out) : null;
  } catch {
    return null;
  }
}

function checkGitIgnored(repoRoot: string, targetPath: string): boolean {
  try {
    execFileSync('git', ['-C', repoRoot, 'check-ignore', '-q', '--', targetPath], {
      stdio: ['ignore', 'ignore', 'ignore'],
      env: {
        PATH: process.env.PATH ?? '',
        GIT_TERMINAL_PROMPT: '0',
      },
    });
    return true;
  } catch {
    return false;
  }
}

function isInsideOrEqual(parentDir: string, candidate: string): boolean {
  const rel = path.relative(parentDir, candidate);
  return rel === '' || (!rel.startsWith('..') && !path.isAbsolute(rel));
}

function validateInRepoDotLocal(repoRoot: string, targetPath: string): void {
  if (targetPath === repoRoot) {
    throw new Error('THREADLANE_ACCEPTANCE_ARTIFACT_DIR must not point to the repository root.');
  }
  const dotLocal = path.join(repoRoot, '.local');
  if (!isInsideOrEqual(dotLocal, targetPath)) {
    throw new Error(
      `In-repo Playwright artifacts are only permitted under ${dotLocal}; got ${targetPath}`,
    );
  }
  ensureNoSymlinkChain(dotLocal);
  let dotLocalStat: fs.Stats;
  try {
    dotLocalStat = fs.lstatSync(dotLocal);
  } catch {
    throw new Error(
      `Refusing in-repo artifact directory because ${dotLocal} does not pre-exist. ` +
        'Use scripts/desktop-acceptance.py or pass an external THREADLANE_ACCEPTANCE_ARTIFACT_DIR.',
    );
  }
  if (!dotLocalStat.isDirectory() || dotLocalStat.isSymbolicLink()) {
    throw new Error(`${dotLocal} must be an existing non-symlink directory.`);
  }
  if (!checkGitIgnored(repoRoot, dotLocal) || !checkGitIgnored(repoRoot, targetPath)) {
    throw new Error(
      `Refusing in-repo artifact directory because ${targetPath} is not git-ignored.`,
    );
  }
}

function resolveValidatedArtifactDir(): string {
  const rawExplicit = process.env.THREADLANE_ACCEPTANCE_ARTIFACT_DIR;
  const rawExplicitRepo = process.env.THREADLANE_ACCEPTANCE_REPO_ROOT;
  const explicitRepo =
    rawExplicitRepo !== undefined && rawExplicitRepo.length > 0
      ? validateSafeAbsolutePath(rawExplicitRepo, 'THREADLANE_ACCEPTANCE_REPO_ROOT')
      : null;

  if (rawExplicit !== undefined && rawExplicit.length > 0) {
    const normalized = validateSafeAbsolutePath(
      rawExplicit,
      'THREADLANE_ACCEPTANCE_ARTIFACT_DIR',
    );
    if (FORBIDDEN_SHARED_ARTIFACT_ROOTS.has(normalized)) {
      throw new Error(
        `THREADLANE_ACCEPTANCE_ARTIFACT_DIR must be a dedicated directory, not shared root: ${normalized}`,
      );
    }
    ensureNoSymlinkChain(normalized);

    if (explicitRepo !== null && isInsideOrEqual(normalized, explicitRepo)) {
      throw new Error(
        `THREADLANE_ACCEPTANCE_ARTIFACT_DIR must not be the repository root or an ancestor of it: ${normalized}`,
      );
    }

    const enclosingGitRoot = findEnclosingGitToplevel(normalized);
    if (enclosingGitRoot !== null) {
      if (isInsideOrEqual(normalized, enclosingGitRoot)) {
        throw new Error(
          `THREADLANE_ACCEPTANCE_ARTIFACT_DIR must not be a Git repository root or an ancestor of it: ${normalized}`,
        );
      }
      if (explicitRepo !== null && enclosingGitRoot !== explicitRepo) {
        throw new Error(
          `Refusing external artifact directory inside another Git worktree (${enclosingGitRoot}): ${normalized}`,
        );
      }
      validateInRepoDotLocal(enclosingGitRoot, normalized);
    } else if (explicitRepo !== null && isInsideOrEqual(explicitRepo, normalized)) {
      validateInRepoDotLocal(explicitRepo, normalized);
    }

    fs.mkdirSync(normalized, { recursive: true, mode: 0o700 });
    return normalized;
  }

  const repoRoot =
    explicitRepo ?? findEnclosingGitToplevel(configDir) ?? path.resolve(configDir, '..', '..');
  const dotLocal = path.join(repoRoot, '.local');
  const fallback = path.join(dotLocal, 'desktop-acceptance', 'playwright');
  validateInRepoDotLocal(repoRoot, fallback);
  fs.mkdirSync(fallback, { recursive: true, mode: 0o700 });
  return fallback;
}

function resolveChromiumLaunchOptions(): { chromiumSandbox: true; executablePath?: string } {
  const rawExecutable = process.env.THREADLANE_CHROMIUM_EXECUTABLE;
  if (rawExecutable === undefined || rawExecutable.trim().length === 0) {
    return {
      chromiumSandbox: true,
    };
  }
  const executablePath = validateSafeAbsolutePath(
    rawExecutable.trim(),
    'THREADLANE_CHROMIUM_EXECUTABLE',
  );
  return {
    chromiumSandbox: true,
    executablePath,
  };
}

function resolvePort(): number {
  const raw = process.env.THREADLANE_PLAYWRIGHT_PORT?.trim() ?? '4173';
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed < 1024 || parsed > 65535) {
    throw new Error(`Invalid THREADLANE_PLAYWRIGHT_PORT: ${raw}`);
  }
  return parsed;
}

const artifactDir = resolveValidatedArtifactDir();
const port = resolvePort();
const explicitBaseUrl = process.env.THREADLANE_PLAYWRIGHT_BASE_URL?.trim();
const baseURL =
  explicitBaseUrl && explicitBaseUrl.length > 0 ? explicitBaseUrl : `http://127.0.0.1:${port}`;
const distOutDir = path.join(artifactDir, 'dist');
const viteCacheDir = path.join(artifactDir, 'vite-cache');
const viteRunnerScript = path.join(artifactDir, 'run-vite-preview.mjs');
const chromiumLaunchOptions = resolveChromiumLaunchOptions();

if (!explicitBaseUrl) {
  const runnerSource = [
    "import fs from 'node:fs';",
    "import path from 'node:path';",
    "import { pathToFileURL } from 'node:url';",
    '',
    `const configDir = ${JSON.stringify(configDir)};`,
    `const distOutDir = ${JSON.stringify(distOutDir)};`,
    `const viteCacheDir = ${JSON.stringify(viteCacheDir)};`,
    `const port = ${String(port)};`,
    '',
    "const vitePkgPath = path.join(configDir, 'node_modules', 'vite', 'package.json');",
    "const vitePkg = JSON.parse(fs.readFileSync(vitePkgPath, 'utf8'));",
    "const dotExport = vitePkg.exports?.['.'];",
    'const relEntry =',
    "  typeof dotExport === 'string'",
    '    ? dotExport',
    '    : dotExport?.import?.default ??',
    '      dotExport?.import ??',
    '      dotExport?.default ??',
    '      vitePkg.module ??',
    '      vitePkg.main ??',
    "      './dist/node/index.js';",
    'const viteEntryUrl = pathToFileURL(path.resolve(path.dirname(vitePkgPath), relEntry)).href;',
    'const { build, preview } = await import(viteEntryUrl);',
    '',
    "const configFile = path.join(configDir, 'vite.config.ts');",
    'await build({',
    '  root: configDir,',
    '  configFile,',
    '  cacheDir: viteCacheDir,',
    '  build: {',
    '    outDir: distOutDir,',
    '    emptyOutDir: true,',
    '  },',
    '});',
    '',
    'const previewServer = await preview({',
    '  root: configDir,',
    '  configFile,',
    '  cacheDir: viteCacheDir,',
    '  build: {',
    '    outDir: distOutDir,',
    '  },',
    '  preview: {',
    "    host: '127.0.0.1',",
    '    port,',
    '    strictPort: true,',
    '  },',
    '});',
    '',
    'previewServer.printUrls();',
    '',
    'const shutdown = () => {',
    '  previewServer.httpServer.close(() => {',
    '    process.exit(0);',
    '  });',
    '};',
    "process.on('SIGINT', shutdown);",
    "process.on('SIGTERM', shutdown);",
    '',
  ].join('\n');
  fs.writeFileSync(viteRunnerScript, runnerSource, { encoding: 'utf8', mode: 0o600 });
}

const webServerCommand = `${JSON.stringify(process.execPath)} ${JSON.stringify(viteRunnerScript)}`;

export default defineConfig({
  testDir: path.join(configDir, 'tests'),
  testMatch: ['**/*.spec.ts'],
  outputDir: path.join(artifactDir, 'test-results'),
  fullyParallel: false,
  workers: 1,
  retries: 0,
  forbidOnly: true,
  timeout: 30_000,
  expect: {
    timeout: 7_500,
  },
  reporter: [
    ['list'],
    ['json', { outputFile: path.join(artifactDir, 'playwright-results.json') }],
    ['html', { outputFolder: path.join(artifactDir, 'html-report'), open: 'never' }],
  ],
  use: {
    baseURL,
    browserName: 'chromium',
    headless: true,
    viewport: { width: 1040, height: 760 },
    actionTimeout: 7_500,
    navigationTimeout: 15_000,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    launchOptions: chromiumLaunchOptions,
  },
  webServer: explicitBaseUrl
    ? undefined
    : {
        command: webServerCommand,
        cwd: configDir,
        url: baseURL,
        timeout: 60_000,
        reuseExistingServer: false,
      },
});
