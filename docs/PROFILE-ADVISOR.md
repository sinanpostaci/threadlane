# Profile decision advisor

A profile may select an optional **Decision advisor**, independently of its five
execution roles. Connections, credentials, and model catalogs stay in **Tools**;
profiles reference a registered API model without copying its credential.

## Configure a profile

1. Save and enable a Jev API (`jev_api`) or OpenAI Decisions API
   (`openai_decisions_api`) tool in Tools, enter its credential, and register an
   eligible no-effort model (for Jev, load its model catalog and choose an
   actual server-returned model; for OpenAI Decisions, register an explicit
   supported Decisions model such as `gpt-6-luna` or a dated `gpt-6-luna-*`
   identifier).
2. Open Profiles and create or edit a profile.
3. Under Decision advisor, choose an API model or keep **Off — no advisor**.
4. Save the profile. Cancel leaves the saved selection unchanged.

The only supported advisor mode is **Shadow**. The advisor suggests task
difficulty; it does not change a plan, select a worker model, grant approval,
or start work. Editing, loading, cloning, or saving a profile makes no provider
request. Existing running tasks retain their frozen settings.

Advisors have no effort control. Only models belonging to enabled Jev or OpenAI
Decisions API tools and permitting no effort are eligible. API advisors are not
offered as worker-role models. A missing, disabled, or incompatible selection
requires an explicit repair or clear; another model is never selected silently.

Both enabled Jev (`jev_api`) and enabled OpenAI Decisions
(`openai_decisions_api`) shadow profile advisors are supported by
`threadlane-advice --profile-id` for explicit `preview` and `classify` calls.
Routing follows the saved advisor model's enabled owning adapter with no
automatic provider selection or cross-provider fallback. `threadlane-advice
models` remains Jev-only and rejects `--profile-id`. Jev and OpenAI Decisions
credentials remain separate per tool ID, and a saved selection does not prove
account entitlement or successful inference.

## Storage and compatibility

Configured profile metadata has this shape:

```json
{
  "decision_advisor": {
    "model_id": "jev-api:jev-latest",
    "mode": "shadow"
  }
}
```

The model ID is an example; use the exact entry in your registry. There is no
credential, effort, endpoint override, or executable in this object.

Missing or null metadata means off. Off profiles serialize without the field,
preserving the original profile shape and existing approval material. Registry
schema version 1 is unchanged. Profiles expose five execution roles; the legacy
`follower` identifier remains readable for compatibility. Unsupported modes,
duplicate fields, invalid references, and stale compare-and-swap revisions are
refused. A referenced advisor model cannot be deleted until the profile clears
or changes its selection.

Compatibility means new binaries can read older profiles. Older binaries reject
the configured advisor field, so update the CLI, MCP process, and desktop panel
together before enabling it. No downgrade migration or replay of frozen jobs is
performed.

Advisor changes participate in the editor's existing external-change detection.
Changing a profile in another window cannot silently overwrite an unsaved draft.
Role-assignment and difficulty-override counts do not include the advisor.

## Explicit native advice

`threadlane-workflow catalog` includes the optional advisor's model and tool
identifiers, labels, exact model argument, adapter, and mode from the same
registry revision as the role assignments. It contains no credential or
executable path, performs no provider probe, and does not change role counts or
the profile's worker availability. Off profiles omit the new catalog field.

Profile metadata is configuration, not permission to send a request. The native
operator supplies a bounded task summary and explicitly invokes shadow advice.
Use `--profile-id` to bind a preview or classification to the saved selection:

```sh
threadlane-context --context /absolute/private/context.json \
  advice --profile-id example-profile preview --request /absolute/private/request.json

threadlane-context --context /absolute/private/context.json \
  advice --profile-id example-profile classify --request /absolute/private/request.json
```

The private version-1 request still contains the exact `tool_id` and `model_id`.
They must match the profile's configured advisor exactly, and `threadlane-advice`
routes by the saved advisor model's enabled owning adapter (`jev_api` -> `"jev"`
or `openai_decisions_api` -> `"openai"`); the CLI never substitutes defaults or
falls back across providers. A missing or off advisor, a mismatched tool, model,
or provider, a disabled tool, an effort-requiring model, an unsupported adapter,
or an unsupported model argument is refused before cache creation, a credential
read, or a provider request. The same registry revision guard applies to the
profile and its model selection, and any baseline difficulty comparison stays
strictly advisory and side-effect-free to model routing.

With a profile selected, both its identifier and the resolved provider
participate in cache isolation, and `profile_id` is reported in the result.
Without `--profile-id`, existing Jev version-1 cache keys remain unchanged,
including the protection for prior pending or uncertain attempts, and
`default_profile_id` is never auto-selected. Selecting a different profile or
provider never reuses another profile's or provider's result.

See [shadow difficulty advice](JEV-SHADOW.md) for the request, cache, and
uncertain-outcome boundaries. Only the supplied summary is sent; profile editing
does not listen to or forward a conversation.

Local correlation labels do not authenticate native per-call identity or human
approval. No advisor output dispatches work or changes Git state.
