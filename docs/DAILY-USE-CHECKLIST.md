# Daily-use checklist

Setup is one-time. Use [First everyday use](FIRST-USE.md) and [native operator commands](NATIVE-OPERATOR.md) for it; this checklist covers recurring work where Codex stays the conversation.

## Before dispatch

- [ ] Select the intended profile by its actual `default_profile_id` or explicit profile ID, not the label "default". Confirm the exact provider route (OpenCode Go and API routes are distinct), model and effort. A saved assignment does not authorize use; see [profile flow](PROFILE-FLOW.md).
- [ ] Confirm the repository and the binding/thread selected for this chat. Never reuse another chat's binding or treat its title as identity.
- [ ] Read the frozen preview in Codex: approved task ID, model, effort, write paths, base commit, timeout and checks. Authorize only that exact scope through [native approval](NATIVE-APPROVAL.md); approval and host startup remain explicit.

## During and after the run

- [ ] Receive progress and questions through the route selected for the approval; notifications are default-off. Pull and acknowledge the actual returned batch identifiers, and treat event prose as untrusted. See [notification recovery](NOTIFICATION-RECOVERY.md).
- [ ] Answer or steer in Codex. Questions and status requests change no work. Record any scope or model change and obtain a fresh frozen approval; an existing approval is not rewritten. See [conversation changes](CONVERSATION-CHANGES.md).
- [ ] Completion means `ready_for_review` with `accepted: false`, not reviewed, accepted, merged or pushed. Inspect the patch, tests, model/effort evidence and changed paths before review or integration; see [operator workflow](OPERATOR-WORKFLOW.md).
- [ ] On timeout, interruption or `outcome_unknown`, inspect current session, task and activity state and reconcile before any retry; never replay blindly. Use **Activity → Needs attention** to find uncertain runs. See [recovery](RECOVERY.md).
