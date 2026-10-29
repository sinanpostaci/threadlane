# Milestone 2C: scoped provider permissions and native worker pilot

This checkpoint makes an AGY file-edit task runnable in a fresh owned worktree,
without a permission bypass or a global provider setting. It is not a promise
that print mode supports interactive Answer/Steer or native multi-chat identity.

- Opt-in trusted `file_permissions: "task_files"`; default `"inherit"` preserves
  the previous argv and behavior. Supported only for Antigravity.
- Export `WorkerFilePermissions { Inherit, TaskFiles }` with Default=Inherit,
  snake_case serde and strict variant validation. Add serde-default field
  `file_permissions` to WorkerSpec and runtime WorkerSpecTemplate.
- TaskFiles generates a session-only `--settings <inline-json>` from the actual
  canonical task working directory. Grant read_file(workdir/**), and exact plus
  descendant edit_file/write_file/write_to_file/replace_file_content/
  multi_replace_file_content paths only for the existing allowed_write_paths.
  Include read aliases only where required by AGY's documented action mapping.
  No supplied arbitrary settings, shell/network/MCP grants, global file edits,
  --auto or --dangerously-skip-permissions. Permissions are provider hints,
  not an OS sandbox and not a replacement for the post-run diff gate. User
  inherited provider policy may still apply; do not claim global deny isolation.
- Reject unsafe permission-pattern interpolation. For TaskFiles reject ambiguous
  glob/rule metacharacters (* ? [ ] { } ( ) backslash and control characters)
  in the canonical workdir and write-path entries before spawning. Clean Unicode
  and spaces should work with JSON serialization. Avoid broad workspace grants.
- `WorkerSpec::build_argv_for_workdir(&Path) -> Result<Vec<String>, WorkerError>`
  returns the real argv after validating the actual directory. Base validate/
  build_argv remain side-effect-free and backwards compatible for Inherit.
  Static SpecValidationReport must expose the chosen file_permissions and clearly
  document any workdir-dependent argv resolution, not pretend it is a full launch
  for TaskFiles. The supervisor executes and records the actual resolved argv.
- Runtime template copies the typed policy to WorkerSpec. The opt-in TaskFiles
  policy is added to the profile digest; omitted or explicit Inherit retains the
  exact legacy digest recipe. Caller HTTP/MCP schemas continue to reject settings/policy
  overrides; startup validation rejects TaskFiles with OpenCode and unsafe allowed
  paths without creating anything. No new MCP tool or persisted schema required.
- TaskFiles is limited to 128 write entries and 65,536 actual serialized settings
  bytes. Oversized grants fail before launch; grants are never silently dropped.
- Preserve all existing tests; add actual fake-process argv/scope preservation and strict
  validation coverage. Parent runs fmt, serial workspace tests, Clippy, build,
  real-provider pilot and independently inspects outcomes. Missing observed effort
  remains missing; never promote it based on a requested flag.
- Native pilot is restricted to the existing explicitly authorized disposable
  test project/chat; restore temporary configuration, stop only owned processes,
  preserve user model/effort/security settings. Same-project automatic chat
  identity, interactive provider continuation, reviewed merge/cleanup and UI are
  still separate gates unless directly demonstrated.
- Primary performs normal review. One package-closing review uses Astra Max as
  newly requested. Any fix after review receives targeted follow-up on the changed
  findings, not a needless full duplicate review.


## Verified native pilot boundary

The primary observed a genuine native MCP `start_worker` invocation in an
explicitly authorized disposable chat, one real Argon file edit, a fixed wake-up
back to that same chat, and native event pull/ACK plus terminal inspection. The
provider did not report effort; the outcome correctly stayed Failed/ExecutionFailed
with `missing_effort_evidence`. No patch acceptance or merge was granted.

A separate deterministic local sleeping process verified native
start/get/cancel and the later proven Cancelled result. This was not a second
model invocation. General native multi-chat identity, busy-turn steering,
interactive print-mode continuation and reviewed result integration remain
separate gates. See [VALIDATION.md](VALIDATION.md).
