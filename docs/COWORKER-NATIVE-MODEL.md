# Coworker Native Model and Five Execution Roles

The user's native Codex conversation is called **Coworker**. The human chooses and switches the active Coworker model and reasoning effort directly in Codex. Threadlane does not re-select, lock, or override the native Coworker model in workflow profiles or native wake-up transport.

## Role model

Threadlane separates the conversational Coworker session from background and role-handoff execution assignments:

- **Coworker (native chat)**: Controlled by the human in Codex. Its active model and reasoning effort may change at any time during an idle or busy conversation.
- **Configurable execution roles (`5`)**:
  1. `orchestrator` (`WorkflowRole::Orchestrator`)
  2. `architect` (`WorkflowRole::Architect`)
  3. `coder` (`WorkflowRole::Coder`)
  4. `reviewer` (`WorkflowRole::Reviewer`)
  5. `closure` (`WorkflowRole::Closure`)

### Backward compatibility for historical `follower` data

- `WorkflowRole::Follower` (`"follower"`) remains in `WorkflowRole::ALL` (`crates/threadlane-registry/src/types.rs`) and `WORKFLOW_ROLES` (`apps/desktop/src/registry/types.ts`) so existing `registry.json` snapshots, saved plans, and historical records continue to deserialize without rewriting user data.
- `WorkflowRole::Follower` is not globally renamed to `"coworker"` in serialized registry schemas; Coworker is outside profile role assignments.
- `WorkflowRole::EXECUTION_ROLES` (`[WorkflowRole; 5]`) and TypeScript `EXECUTION_ROLES` / `EXECUTION_WORKFLOW_ROLES` (`['orchestrator', 'architect', 'coder', 'reviewer', 'closure']`) define the active configurable execution roles.
- In the desktop **Profiles** page (`apps/desktop/src/pages/profiles.ts`), the legacy `Follower` role editor is hidden, profile completeness is measured across the five execution role defaults (`5/5 role defaults`), legacy profiles containing `follower` assignments decode cleanly, and creating, editing, cloning, or saving a profile never requires or emits a `follower` assignment.

## Native notification transport invariants

Native Unix WebSocket wake-up delivery (`crates/threadlane/src/native_tool_output.rs`) enforces target chat and workspace identity without locking the human's active Codex model:

1. **Preserved fail-closed identity and safety checks**:
   - Same-UID `0600` private context file and `0700` parent directory validation.
   - Same-UID Unix domain socket validation, or Linux desktop endpoint discovery, process-ancestry proof, and socket peer validation when `reconnect` is configured.
   - Exact `thread_id` (`thread.id`) and canonical `project_root` (`thread.cwd`) verification on both `thread/read` and `thread/resume`.
   - Thread readiness verification (`idle` or `active`; `notLoaded` remains retryable before any effect write).
   - In `user_message_steer` mode when the thread is `active`, bounded read-only `thread/turns/list` (`limit: 1, sortDirection: "desc", itemsView: "summary"`) verification of an `inProgress` turn and `turn/steer` submission with exact `expectedTurnId`.
   - Strict `may_have_written` uncertainty tracking (`WakeupOutcome::Unknown` after effect write) and no fallback to the `codex queue` executable when native transport is configured.

2. **Model and effort independence**:
   - `thread/read` and `thread/resume` do **not** require the live thread's `model` or `reasoningEffort` to match `NativeToolOutputConfig`.
   - Switching the active model or reasoning effort in Codex during either `idle` or `active` (`busy`) turns succeeds for the same authorized chat and project.
   - `thread/resume`, `turn/start`, and `turn/steer` never send `model`, `effort`, or `reasoningEffort` overrides.
   - Malformed non-string `model` / `reasoningEffort` fields and outer/inner `thread` field disagreements in `thread/resume` are still rejected.

3. **Context serialization, digest compatibility, and live observation**:
   - Legacy `NativeToolOutputConfig` JSON files with `"model"` and `"effort"` continue to load without modification and produce the exact same canonical SHA-256 `digest()` (`5869029fab09b993d9ccb2f1422ebbc5bf4a1c95406647dd8d87ce8bd961d518` for the canonical test fixture).
   - New Coworker notification contexts constructed via `NativeToolOutputConfig::new(socket_path, thread_id, project_root)` leave `model` and `effort` empty and omit both keys when serialized to JSON.
   - `observe_thread_read_model` and `observe_thread_resume_model` return `NativeObservedThreadModel { model, effort }` populated only when the native server actually reports non-empty strings, never fabricating model identity from defaults.
