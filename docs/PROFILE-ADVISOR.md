# Profile decision advisor

A profile may select an optional **Decision advisor**, independently of its six
worker roles. Connections, credentials, and model catalogs stay in **Tools**;
profiles reference a registered API model without copying its credential.

## Configure a profile

1. Save and enable a Jev API tool in Tools, enter its credential, and load its
   model catalog. Choose an actual server-returned model.
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

OpenAI Decisions can be reserved in the profile as a future provider extension.
The current shadow advice CLI executes Jev only and refuses an OpenAI selection
before credential or network access. A saved selection does not prove account
entitlement or successful inference.

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
schema version 1 and the six workflow roles are unchanged. Unsupported modes,
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
They must match the profile's configured advisor; the CLI does not substitute
defaults. A missing or off advisor, a mismatched model, or an unsupported provider
is refused before a credential read or provider request. The same registry
revision guard applies to the profile and its model selection.

With a profile selected, its identifier participates in cache scope and is
reported in the result. Without `--profile-id`, existing version-1 cache keys
remain unchanged, including the protection for prior pending or uncertain
attempts. Selecting a different profile never reuses another profile's result.

See [Jev shadow advice](JEV-SHADOW.md) for the request, cache, and uncertain-outcome
boundaries. Only the supplied summary is sent; profile editing does not listen
to or forward a conversation.

Local correlation labels do not authenticate native per-call identity or human
approval. No advisor output dispatches work or changes Git state.
