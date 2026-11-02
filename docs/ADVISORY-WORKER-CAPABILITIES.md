# Advisory Worker Read-Only Capabilities (`threadlane-workers`)

## 1. Purpose and Security Boundary

Planning and advisory roles (such as `Orchestrator` and `Architect`) inspect repository context to produce structured plans or architecture reviews without mutating repository files, running shell commands, delegating to subagents or MCP servers, or reading protected credentials/control state.

Previously, `WorkerFilePermissions` provided only:

- `WorkerFilePermissions::Inherit` (`"inherit"`): preserves inherited provider file permissions and does **not** establish a global deny policy.
- `WorkerFilePermissions::TaskFiles` (`"task_files"`): requires a non-empty `allowed_write_paths` set and grants file-edit/write tools on those scopes.

Neither `Inherit` nor `TaskFiles` establishes preventive read-only capability enforcement, and neither a prompt instruction ("do not write") nor a post-run Git diff check prevents a model from invoking effectful tools during execution.

`WorkerFilePermissions::ReadOnly` (`"read_only"`) and `AdvisoryWorkerSpec` provide **provider-enforced read-only advisory capabilities** with:

1. **Zero allowed write scopes**: `allowed_write_paths` must be empty (`0` entries) when `file_permissions` is `ReadOnly`, while `Inherit` and `TaskFiles` continue to require at least one repository-relative write path.
2. **Scoped read-only tool access**: Model-visible file tools are restricted to read-only inspection inside the validated canonical non-root `working_directory` (`cwd`), with explicit denials for `.env*`, `.git/**`, `.codex/**`, `.gemini/**`, `.opencode/**`, `.agents/**`, and `.claude/**`.
3. **Explicit denial of writes, shell, delegation, MCP, and web tools**: All file-edit/write tools, shell/terminal execution tools, subagent/task delegation tools, MCP tools, browser/web tools, and interactive tools are explicitly denied in the per-session provider configuration.
4. **Exact model and effort/variant pins**: OpenCode V2 per-child configuration pins the top-level `model` and all four agents (`build`, `title`, `summary`, `compaction`) to the exact requested `{ providerID, model, variant }` target, preventing silent fallback to small or auxiliary models.
5. **Fail-closed capability admission**: Any provider or permission mode for which preventive read-only capability enforcement cannot be established is rejected **before launch** with `WorkerError::UnsupportedCapability` (`WorkerErrorCode::UnsupportedCapability`).

> **Non-Claim (Provider Capability Enforcement vs. OS Sandbox Isolation):**
> `WorkerFilePermissions::ReadOnly` enforces session-scoped tool and permission rules through each supported provider CLI's documented configuration surface (`--settings <inline-json>` for Antigravity; `OPENCODE_CONFIG_CONTENT` for OpenCode V2). It is **not** a claim of kernel, container, or OS-level filesystem sandbox isolation. Source credentials remain strictly provider-managed and are never embedded in `AdvisoryWorkerSpec`, `WorkerSpec`, snapshots, or worker output.

---

## 2. Supported Providers and Rule Schemas

### 2.1 Google Antigravity (`WorkerProvider::Antigravity` / `"antigravity"`)

- **CLI Launch Arguments**:
  ```text
  --model <model> --effort <effort> --mode accept-edits --output-format stream-json --print-timeout 0 --settings <inline-json> --print <prompt>
  ```
  No undocumented flags are invented, and `--dangerously-skip-permissions` or `--auto` are never used.
- **`--settings <inline-json>` Schema (`AntigravitySessionSettings`)**:
  - `permissions.allow`:
    - Exactly one entry: `["read_file(<canonical_workdir>/**)"]`.
  - `permissions.deny`:
    - Every write/edit tool in `AGY_WRITE_PERMISSION_TOOLS` (`edit_file`, `write_file`, `write_to_file`, `replace_file_content`, `multi_replace_file_content`) denied both globally (`<tool>(*)`) and under `<canonical_workdir>/**` (`<tool>(<canonical_workdir>/**)`).
    - Every execution, shell, subagent, MCP, web/browser, and interactive tool in `AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS` (`run_command`, `bash`, `shell`, `execute`, `run_in_terminal`, `manage_task`, `invoke_subagent`, `define_subagent`, `manage_subagents`, `send_message`, `subagent`, `task`, `agent`, `call_mcp_tool`, `list_resources`, `read_resource`, `mcp`, `search_web`, `web`, `webfetch`, `websearch`, `browser`, `ask_question`, `schedule`) denied via `<tool>(*)`.
    - `read_file(<canonical_workdir>/<pattern>)` denied for all 28 protected secret and control patterns in `AGY_PROTECTED_READ_DENY_PATTERNS` (`.env`, `.env.*`, `**/.env`, `**/.env.*`, `.git`, `.git/**`, `**/.git`, `**/.git/**`, `.opencode`, `.opencode/**`, `**/.opencode`, `**/.opencode/**`, `.codex`, `.codex/**`, `**/.codex`, `**/.codex/**`, `.gemini`, `.gemini/**`, `**/.gemini`, `**/.gemini/**`, `.agents`, `.agents/**`, `**/.agents`, `**/.agents/**`, `.claude`, `.claude/**`, `**/.claude`, `**/.claude/**`).
- **Legacy `TaskFiles` Compatibility**:
  - `AntigravityPermissionsBlock::deny` uses `#[serde(default, skip_serializing_if = "Vec::is_empty")]`, so `WorkerFilePermissions::TaskFiles` payloads serialize as `{"permissions":{"allow":[...]}}` byte-for-byte identically to existing profiles.

### 2.2 OpenCode 2.2.0 (`WorkerProvider::OpenCodeV2` / `"opencode_v2"`)

- **CLI Launch Arguments**:
  ```text
  --standalone run --model <provider/model#variant> --format json [--session <id>] -- <prompt>
  ```
- **Per-Child Environment (`OPENCODE_CONFIG_CONTENT` / `OpenCodeConfigContent`)**:
  - `update`: `"disable"`
  - `default_agent`: `"build"`
  - `model`: `{ "providerID": "<provider>", "model": "<model>", "variant": "<variant>" }`
  - `agents`: `build`, `title`, `summary`, and `compaction` (`OPENCODE_PINNED_AGENTS`) are all pinned to the exact same `model` object and `permissions` list so auxiliary title/summary/compaction steps cannot fall back to unapproved small models.
  - `policies`:
    - `provider.use` deny `*`, then allow `<providerID>`.
    - `mcp.use` deny `*`.
    - `tool.use` deny for all 16 resources in `OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES` (`subagent`, `task`, `agent`, `edit`, `write`, `patch`, `apply_patch`, `multiedit`, `shell`, `bash`, `execute`, `web`, `webfetch`, `websearch`, `todowrite`, `question`).
  - `permissions` (ordered V2 `{ action, resource, effect }` rules, last-matching-rule wins):
    1. Baseline `Deny` on `"*"` for all 19 actions in `OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS` (`*`, `external_directory`, `shell`, `bash`, `subagent`, `task`, `agent`, `execute`, `web`, `webfetch`, `websearch`, `mcp`, `edit`, `write`, `patch`, `apply_patch`, `multiedit`, `todowrite`, `question`).
    2. `Allow` on `"*"` for location-relative internal `read`, `glob`, and `grep` inspection (zero `edit` allow rules).
    3. Trailing `Deny` rules for all 28 secret/control patterns on `read`, `glob`, `grep`, and `edit`, plus 22 repository/agent control patterns on `edit`.

### 2.3 Canonical Advisory `cwd` Validation

Both Antigravity and OpenCode V2 `ReadOnly` builders validate `working_directory` via `validate_advisory_read_only_workdir` before constructing argv or environment payloads, refusing:

- Non-existent paths, relative paths, non-directories, or any path with symlink components (`WorkerError::SecurityViolation` / `WorkerError::InvalidSpec`).
- The filesystem root directory `/`.
- Any path containing glob or permission-rule metacharacters (`*`, `?`, `[`, `]`, `{`, `}`, `(`, `)`, `\`) or control characters.
- Any path that targets or resides inside a protected control component (`.git`, `.gitmodules`, `.gitattributes`, `.gitignore`, `.codex`, `.gemini`, `.opencode`, `.agents`, `.claude`, `.github`, `.cargo`, `.env`, `.env.*`, `AGENTS.md`, `CLAUDE.md`, `opencode.json`, `opencode.jsonc`).

---

## 3. Provider-Private State vs. Repository Writes and Bounded Supervision

- **Provider-Private Logging/State**: External provider CLIs maintain private operational state outside the repository `cwd` (for example under provider home/data directories) and emit operational diagnostics on `stderr`. `run_advisory_worker*` does not suppress `stderr`; it captures bounded, secret-redacted diagnostics in `WorkerExecutionReport::redacted_stderr_summary` and collects owned OpenCode session metadata (`opencode_metadata`) when applicable.
- **Process-Group Supervision and Pipe Ownership**: Advisory workers run in a dedicated Unix process group (`process_group(0)`). If a worker spawns background descendants that inherit `stdout` or `stderr` pipes, the supervisor terminates and reaps surviving process-group members after main child exit within `POST_EXIT_PIPE_DRAIN_TIMEOUT_MS` (`250ms`), preventing pipe-ownership hangs.
- **Bounded Evidence and Cancellation**: Explicit cancellation (`WorkerRunStatus::Cancelled`), timeouts (`WorkerRunStatus::TimedOut`), stream overflow (`WorkerRunStatus::OutputOverflow`), and fail-closed stream/metadata evidence checks (`MissingModelEvidence`, `MissingEffortEvidence`, `MissingResultEvidence`, `ModelMismatch`, `EffortMismatch`, `SessionMismatch`) are identical to `run_worker*`.

---

## 4. Verification Scope and Remaining Provider Limitations

- **Deterministic Fixture Coverage**: `crates/threadlane-workers/tests/advisory_permissions.rs` verifies:
  - Antigravity and OpenCode V2 permission payload construction, rule precedence (`deny` overriding `allow` in Antigravity; trailing `deny` overriding `allow *` in OpenCode V2), and default denial of unlisted tools.
  - Fail-closed `WorkerError::UnsupportedCapability` rejection for unsupported providers (`codex_cli`, `jev_api`, `openai_decisions_api`, `git`, `tmux`, `codegraph`, etc.) and non-`ReadOnly` modes (`Inherit`, `TaskFiles`).
  - Zero write-scope enforcement for `ReadOnly` and preservation of non-empty write-scope validation for `Inherit` and `TaskFiles`.
  - Refusal of `/`, relative paths, symlinks, glob metacharacters, and protected control directories as advisory `cwd`.
  - Primary and auxiliary (`build`, `title`, `summary`, `compaction`) model and effort/variant pins in OpenCode V2.
  - Legacy `Inherit` and `TaskFiles` serde, `--settings` JSON, and `argv` compatibility.
  - Fake-provider supervised execution covering provider-private logging outside `cwd`, `stderr` diagnostics, pipe-holding descendant cleanup, cancellation, and missing-effort evidence rejection.
- **Remaining Live-Provider Verification Limitation**:
  - No live model or provider CLI calls were executed in this implementation package. While the generated `--settings` JSON (`agy`) and `OPENCODE_CONFIG_CONTENT` (`opencode`) payloads strictly follow each provider's documented schema, runtime enforcement of those rules against live model tool calls depends on the installed provider CLI binary. A live read-only permission probe can be separately scoped and authorized by the operator.

---

## 5. Exported Rust API Signatures for Native Planning Service Integration

All types, constants, and functions below are exported from the root of `threadlane_workers` (as well as `threadlane_workers::permissions`, `threadlane_workers::spec`, and `threadlane_workers::supervisor`):

```rust
// Permission mode and capability validation
pub enum WorkerFilePermissions {
    Inherit,
    TaskFiles,
    ReadOnly,
}

impl WorkerFilePermissions {
    pub fn as_str(self) -> &'static str;
    pub fn is_read_only(self) -> bool;
    pub fn enforces_preventive_read_only(self) -> bool;
    pub fn requires_workdir_argv(self) -> bool;
    pub fn ensure_advisory_read_only_capability(
        self,
        provider: WorkerProvider,
    ) -> Result<(), WorkerError>;
    pub fn validate_for_provider_and_paths(
        self,
        provider: WorkerProvider,
        allowed_write_paths: &BTreeSet<String>,
    ) -> Result<(), WorkerError>;
}

pub fn validate_advisory_provider_and_mode(
    provider_id: &str,
    file_permissions: WorkerFilePermissions,
) -> Result<WorkerProvider, WorkerError>;

pub fn validate_advisory_read_only_workdir(
    working_directory: &Path,
) -> Result<PathBuf, WorkerError>;

// Antigravity read-only settings builders and evaluator
pub const AGY_READ_PERMISSION_TOOL: &str = "read_file";
pub const AGY_WRITE_PERMISSION_TOOLS: [&str; 5];
pub const AGY_DENIED_EXECUTION_AND_DELEGATION_TOOLS: [&str; 24];
pub const AGY_PROTECTED_READ_DENY_PATTERNS: [&str; 28];

pub struct AntigravitySessionSettings {
    pub permissions: AntigravityPermissionsBlock,
}

pub struct AntigravityPermissionsBlock {
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

pub fn build_antigravity_read_only_settings(
    working_directory: &Path,
) -> Result<AntigravitySessionSettings, WorkerError>;

pub fn build_antigravity_read_only_settings_json(
    working_directory: &Path,
) -> Result<String, WorkerError>;

pub fn evaluate_antigravity_settings(
    settings: &AntigravitySessionSettings,
    tool: &str,
    resource: Option<&str>,
) -> OpenCodeRuleEffect;

// OpenCode V2 read-only constants and builders
pub const OPENCODE_READ_ONLY_DENIED_TOOL_POLICIES: [&str; 16];
pub const OPENCODE_READ_ONLY_DENIED_PERMISSION_ACTIONS: [&str; 19];

pub fn build_opencode_config_content(
    resolved_model: &ResolvedModelTarget,
    file_permissions: WorkerFilePermissions,
    working_directory: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<OpenCodeConfigContent, WorkerError>;

pub fn build_opencode_config_content_json(
    resolved_model: &ResolvedModelTarget,
    file_permissions: WorkerFilePermissions,
    working_directory: &Path,
    allowed_write_paths: &BTreeSet<String>,
) -> Result<String, WorkerError>;

// Dedicated advisory worker specification and validation report
pub struct AdvisoryWorkerSpec {
    pub provider: WorkerProvider,
    pub executable: PathBuf,
    pub model: String,
    pub effort: Option<String>,
    pub allowed_models: BTreeSet<String>,
    pub session_id: Option<String>,
    pub standalone: bool,
    pub working_directory: PathBuf,
    pub allowed_write_paths: BTreeSet<String>,
    pub prompt: String,
    pub file_permissions: WorkerFilePermissions,
    pub budgets: ExecutionBudgets,
}

pub struct AdvisorySpecValidationReport {
    pub provider: WorkerProvider,
    pub file_permissions: WorkerFilePermissions,
    pub canonical_executable: PathBuf,
    pub canonical_working_directory: PathBuf,
    pub resolved_model: ResolvedModelTarget,
    pub constructed_argv: Vec<String>,
    pub child_env: Vec<(String, String)>,
    pub allowed_write_paths: BTreeSet<String>,
    pub budgets: ExecutionBudgets,
}

impl AdvisoryWorkerSpec {
    pub fn validate(&self) -> Result<AdvisorySpecValidationReport, WorkerError>;
    pub fn validate_against_policy(
        &self,
        policy: &WorkerPolicy,
    ) -> Result<AdvisorySpecValidationReport, WorkerError>;
    pub fn resolve_model_target(&self) -> Result<ResolvedModelTarget, WorkerError>;
    pub fn build_argv(&self) -> Result<Vec<String>, WorkerError>;
    pub fn build_argv_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<String>, WorkerError>;
    pub fn build_child_env(&self) -> Result<Vec<(String, String)>, WorkerError>;
    pub fn build_child_env_for_workdir(
        &self,
        working_directory: &Path,
    ) -> Result<Vec<(String, String)>, WorkerError>;
    pub fn build_antigravity_settings(&self) -> Result<AntigravitySessionSettings, WorkerError>;
    pub fn build_antigravity_settings_json(&self) -> Result<String, WorkerError>;
    pub fn build_opencode_config_content(&self) -> Result<OpenCodeConfigContent, WorkerError>;
    pub fn build_opencode_config_json(&self) -> Result<String, WorkerError>;
}

// Bounded Unix process-group advisory worker execution
pub async fn run_advisory_worker(
    spec: &AdvisoryWorkerSpec,
) -> Result<WorkerExecutionReport, WorkerError>;

pub async fn run_advisory_worker_with_cancel(
    spec: &AdvisoryWorkerSpec,
    cancel_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<WorkerExecutionReport, WorkerError>;

pub async fn run_advisory_worker_with_observer(
    spec: &AdvisoryWorkerSpec,
    cancel_rx: &mut tokio::sync::watch::Receiver<bool>,
    progress_tx: Option<tokio::sync::mpsc::Sender<NormalizedWorkerEvent>>,
) -> Result<WorkerExecutionReport, WorkerError>;
```
