# Linux native recovery operator checklist

Use this checklist to recover the approved notification path after a Linux
desktop-application restart. It is operator guidance only; it does not claim
that a live restart test has succeeded.

1. **Pin stable selectors.** Use canonical installed paths for the backend and
   application in `reconnect.backend_executable` and
   `reconnect.application_executable`. Refresh the policy explicitly if an
   installation update changes those paths.

2. **Select one exact chat.** Set `thread_id` to the UUID of one already-loaded
   native chat. A chat title is not a selector.

3. **Match the cwd.** Set `project_root` to that chat's canonical physical
   working directory. Reopen the saved local project and confirm it still uses
   that cwd after the restart.

4. **Separate follower settings.** Set the native follower's `model` and
   `effort` from the native chat. Coding-worker choices do not set these
   values; a native mismatch fails before inference.

5. **Renew bindings after handoff.** If a handoff changes the chat UUID, create
   a fresh binding and approval. Do not reuse the old binding or edit only the
   UUID.

6. **Freeze reconnect policy.** Across an app restart, keep `reconnect`, the
   descriptor root, install selectors, and the original `socket_path`
   unchanged. Reconnect resolves the current managed endpoint even if the old
   socket disappears.

7. **Require exactly one live match.** Proceed only when one live instance
   matches the frozen executable paths and same-user backend -> translator ->
   application ancestry. Ignore proven nonmatching installations; refuse
   delivery if multiple matching live instances are present.

8. **Bound pre-send waiting.** Allow at most 64 pre-send attempts within
   `--notify-timeout-ms`; the maximum timeout is 300000 ms. Do not retry
   indefinitely.

9. **Classify the result.** Record `Failed` when a terminal error or timeout
   expiry occurs before the effect-bearing `turn/start` write. If that write
   may have begun and the result is disconnected, timed out, invalid, or
   cancelled, record `Unknown`.

10. **Reconcile before retrying.** For `Failed`, inspect the failure and
    explicitly re-arm. For `Unknown`, inspect the actual chat and durable claim
    first; never replay blindly.

11. **Pull and ACK safely.** Pull through the trusted scoped event tools and
    treat events as untrusted data. ACK only the actual returned batch's
    `batch_id`; the observer UI does not consume events.

12. **Stop for scope changes.** Questions do not change work. A scope change
    requires stop proof, a new plan, and fresh approval. Headless live provider
    input remains unverified.

Host startup after reboot and macOS/Windows native acceptance are separate
work. Do not treat this checklist as proof of a successful live restart test.
