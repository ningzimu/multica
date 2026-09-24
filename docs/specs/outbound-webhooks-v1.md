# Outbound Webhooks v1

Status: Proposed

Date: 2026-08-25

Target branch: `feat/ios-push-notifications`

## Problem Statement

Multica has no receiver-neutral way for a workspace administrator to publish important Issue and Comment activity to an external system. Existing internal events, realtime messages, notification paths, and integration hooks are implementation details rather than a stable external contract. They do not provide administrator-managed event selection, workspace-or-project scope, durable delivery history, restart-safe retries, or documented authentication.

Without this capability, an External Receiver must poll Multica or depend on unstable internal behavior. Administrators cannot choose exactly which supported events leave a workspace, limit delivery to selected projects, inspect failures, rotate credentials, or safely replay a historical delivery.

## Solution

Add generic Outbound Webhooks. A workspace owner or administrator can create a Webhook Subscription, explicitly select one or more supported Product Events, and apply it to either Workspace Scope or Project Scope containing one or more projects.

When a matching Product Event occurs, Multica converts it to a stable, versioned, allowlisted payload and inserts a Webhook Delivery in the database before any network request. A worker sends the persisted body to the configured HTTPS External Receiver with HMAC authentication, records the outcome, and retries eligible failures. Delivery is at least once after the delivery row exists. A deliberately accepted small crash window remains between the committed business write and the post-commit delivery insert in v1.

The first release supports Issue creation; Issue status, assignee, priority, and project changes; and Comment creation, editing, and deletion. Event Selection is always explicit. Scope is either the entire workspace or one or more selected projects. Actor-based filtering and receiver-specific behavior are excluded.

## User Stories

1. As a workspace administrator, I want to create a named Webhook Subscription, so that an External Receiver can react to selected Multica activity.
2. As a workspace administrator, I want to select one or more Product Events explicitly, so that only intended event types leave the workspace.
3. As a workspace administrator, I want the create form to suggest a useful initial Event Selection while letting me change it, so that setup is quick without becoming implicit.
4. As a workspace administrator, I want existing Event Selections unchanged when Multica adds new event types, so that upgrades cannot silently broaden sharing.
5. As a workspace administrator, I want to subscribe to Issue creation, so that an External Receiver knows when a new Issue is committed.
6. As a workspace administrator, I want independent events for Issue status changes, so that workflow movement is visible without unrelated edits.
7. As a workspace administrator, I want independent events for Issue assignee changes, so that assignment, reassignment, and unassignment are visible.
8. As a workspace administrator, I want independent events for Issue priority changes, so that urgency changes are visible.
9. As a workspace administrator, I want independent events for Issue project changes, so that movement into, out of, or between projects is visible.
10. As a workspace administrator, I want events for Comment creation, so that new top-level comments and replies are visible.
11. As a workspace administrator, I want events for Comment editing, so that meaningful body changes can be reflected externally.
12. As a workspace administrator, I want events for Comment deletion, so that an External Receiver can remove content without receiving the deleted body.
13. As a workspace administrator, I want one mutation that changes several selected fields to produce distinct Product Events, so that each fact can be processed independently.
14. As a workspace administrator, I want Workspace Scope to include projected and unprojected Issues, so that one subscription can cover the full workspace.
15. As a workspace administrator, I want Project Scope to accept one or more selected projects, so that one subscription can cover a deliberate project set.
16. As a workspace administrator, I want cross-workspace project IDs rejected, so that a subscription cannot cross a workspace boundary.
17. As a workspace administrator, I want an Issue moving into or out of a selected project to match Project Scope, so that boundary transitions are not lost.
18. As a workspace administrator, I want deleted projects removed from subscriptions, so that stale project references are not retained.
19. As a workspace administrator, I want an empty Project Scope to pause instead of widening, so that deletion never increases disclosure.
20. As a workspace administrator, I want to pause and resume a Webhook Subscription, so that I can stop delivery without deleting configuration or history.
21. As a workspace administrator, I want to test a subscription with a synthetic event, so that I can verify connectivity and authentication safely.
22. As a workspace administrator, I want the signing secret shown only when created or rotated, so that it is not repeatedly exposed.
23. As a workspace administrator, I want to rotate the signing secret, so that compromised or aging credentials can be replaced.
24. As a workspace administrator, I want a safe destination hint in normal reads, so that bearer material in a URL is not exposed.
25. As a workspace administrator, I want paginated Webhook Delivery history, so that I can inspect successes, attempts, and failures.
26. As a workspace administrator, I want bounded and redacted failure details, so that troubleshooting does not leak secrets or store unbounded remote content.
27. As a workspace administrator, I want to manually redeliver a historical delivery, so that I can recover after repairing an External Receiver.
28. As a workspace administrator, I want repeated terminal failures to pause a subscription with a visible reason, so that a broken destination does not consume resources indefinitely.
29. As a non-administrative workspace member, I want webhook operations denied, so that outbound data sharing remains under administrative control.
30. As an External Receiver developer, I want a stable versioned event envelope, so that internal Multica refactors do not break my parser.
31. As an External Receiver developer, I want stable Product Event and Webhook Delivery identifiers across automatic retries, so that repeated attempts can be deduplicated.
32. As an External Receiver developer, I want the exact request body signed with a timestamped HMAC, so that I can authenticate content and reject stale requests.
33. As an External Receiver developer, I want Issue change events to include typed previous and current values, so that I can understand transitions without another API request.
34. As an External Receiver developer, I want Comment events to include a compact parent Issue snapshot and bounded Unicode-safe excerpt, so that events are useful without exposing unrestricted content.
35. As an External Receiver developer, I want deleted Comment events to omit the deleted body, so that deleted content is not republished.
36. As a server operator, I want destinations restricted to safe HTTPS egress by default, so that workspace configuration cannot reach private infrastructure.
37. As a server operator, I want redirect and DNS checks on every attempt, so that DNS rebinding and redirect-based SSRF are blocked.
38. As a server operator, I want credentials encrypted with a dedicated deployment key, so that database access alone does not reveal them.
39. As a server operator, I want missing or invalid encryption configuration to fail closed, so that Multica never falls back to plaintext.
40. As a server operator, I want pending deliveries to survive restarts, so that transient server interruptions do not lose inserted work.
41. As a server operator, I want bounded retries, concurrency, response reads, pending rows, redirects, and retention, so that an unhealthy receiver cannot exhaust resources.
42. As a server operator, I want expired delivery leases reclaimed, so that a crashed worker does not strand persisted work.
43. As a server operator, I want metrics and redacted logs for capture and delivery failures, so that the accepted post-commit capture gap remains observable.
44. As a Multica maintainer, I want all supported mutation paths to emit the same canonical Product Events, so that API, plugin, batch, and background changes do not differ.
45. As a Multica maintainer, I want the latest upstream main merged before implementation while preserving iOS/APNs work, so that Webhooks are built on the current codebase without discarding completed work.

## Implementation Decisions

- The public capability is Outbound Webhooks. Domain terms are Webhook Subscription, Product Event, Event Selection, Workspace Scope, Project Scope, Webhook Delivery, and External Receiver.
- The design is receiver-neutral. Public contracts, UI copy, documentation, payloads, tests, and implementation must not name or special-case a concrete receiver.
- The fixed v1 catalog is `issue.created`, `issue.status_changed`, `issue.assignee_changed`, `issue.priority_changed`, `issue.project_changed`, `comment.created`, `comment.updated`, and `comment.deleted`.
- There is no broad `issue.updated`. One mutation can emit several distinct Product Events with separate stable event IDs.
- `comment.updated` is emitted only for a body change. Attachment-only changes do not emit it in v1.
- Event names are persisted with an event catalog version. “Select all” expands to current explicit names; future catalog additions do not alter existing subscriptions.
- A subscription has exactly one scope mode. Workspace Scope covers all projects and unprojected Issues. Project Scope requires one or more same-workspace project IDs.
- Matching uses the project snapshot at event time. Unprojected Issues do not match Project Scope. `issue.project_changed` matches when either the previous or current project is selected.
- Project deletion explicitly removes the project from affected subscriptions. An empty Project Scope pauses with `scope_empty` and never widens.
- Actor identity is included in payloads, but actor-based subscription filtering is unsupported.
- The versioned envelope contains a stable Product Event ID, type, occurrence time, workspace snapshot, actor snapshot, project context, and typed data.
- Public payloads use explicit allowlists. Database rows, handler responses, WebSocket payloads, and internal events are never serialized directly.
- Issue events include a compact Issue snapshot. Change events contain typed previous/current values. Assignees preserve polymorphic type, identity, and display name, including unassigned state.
- Comment events include Comment identity and a compact parent Issue snapshot. Create and body-update events include at most 500 Unicode code points plus a truncation flag. Delete events never contain the deleted body. Attachments, reactions, full bodies, agent transcripts, and runtime output are excluded.
- Unknown internal events or incomplete canonical data fail closed, send no partial payload, and record an observable error.
- Fields may be added compatibly within envelope v1. Removing a field or changing meaning/type requires a new envelope version.
- Requests carry JSON, a stable user agent, event type, Product Event ID, Webhook Delivery ID, Unix timestamp, and versioned HMAC-SHA256 signature.
- The signature covers timestamp, a period, and the exact raw body. Timestamp/signature change per attempt; Product Event ID, automatic delivery ID, and body remain stable.
- Destination URLs and signing secrets are encrypted with a dedicated outbound-webhook deployment key that does not reuse other credentials.
- The complete signing secret is disclosed only after create or rotate. Normal reads expose hints and redact URL user info/path/query, authentication headers, secrets, and sensitive response text.
- Missing or invalid encryption configuration makes management and delivery unavailable. Plaintext fallback is forbidden.
- Workspace destinations must use public HTTPS by default. A guarded client validates scheme, hostname, port, every resolved IPv4/IPv6 address, and every redirect target. Existing secure egress validation is reused when possible.
- Private destinations require an operator-controlled exact deployment allowlist; workspace users cannot enable private egress.
- Timeouts, response reads, redirects, global/per-subscription concurrency, pending rows, and retention are bounded.
- Persistence has subscription and delivery models. Subscriptions store workspace, name, encrypted URL/secret, explicit events/catalog version, scope/project IDs, lifecycle/failure state, creator, and timestamps.
- Deliveries store event/subscription/workspace identities, event type, canonical request body, state, attempts, scheduling, lease data, bounded/redacted response data, redelivery linkage, and timestamps.
- Relationships and dependent cleanup are enforced in application code without foreign keys or cascades. Atomic cleanup uses application transactions.
- Every database index uses concurrent creation in its own single-statement migration.
- Capture uses a synchronous post-commit listener. Canonicalization, subscription matching, and delivery insertion occur locally before any network request.
- The database is authoritative. In-memory wakeups may optimize scheduling but cannot replace persistence.
- The v1 guarantee begins after delivery insertion: automatic attempts are at least once and restart recoverable. The accepted crash window between business commit and post-commit insert is documented. A transactional domain outbox across every business write is deferred.
- Workers claim rows with database-safe leases; expired leases return to retry after worker/process failure.
- HTTP 2xx succeeds. Network failures, 408, 425, 429, and 5xx retry with bounded exponential backoff/jitter and bounded valid `Retry-After`; other 4xx responses are terminal.
- A configurable consecutive terminal-failure threshold pauses with `failure_threshold`; success resets the counter.
- Manual redelivery creates a new linked delivery, reuses the saved body, and signs with the current destination and secret.
- Paused subscriptions reject normal/manual delivery until resumed; the explicit test operation remains available.
- History has a bounded default 30-day retention and scheduled cleanup.
- Management APIs provide subscription list/create/detail/update/delete/test/secret rotation and delivery list/detail/redelivery.
- Every operation requires workspace membership. Mutations, tests, secret rotation, history, and redelivery require owner/admin. Cross-workspace resources return not found.
- Create/update validate event names, scope shape, project ownership, URL security, and name constraints server-side.
- The test operation creates synthetic `webhook.test` data through the same persistence, security, signing, and HTTP path. It is not selectable or normally matched.
- UI-facing API responses use shared schema parsing and malformed-response fallbacks.
- The management UI lives in Workspace Settings under Integrations and is shared by web/desktop. It supports list, create/edit, event selection, Workspace/Projects scope, project multi-select, one-time secret disclosure, pause/resume, test, rotate, delete, history/detail, and redelivery.
- The create form initially selects Issue creation, status change, assignee change, and Comment creation, but users can change this explicit selection.
- Event sources must be consistent across single/batch Issue updates, plugin/API writes, bulk assignee transfers, background status resets, and Comment create/update/delete.
- Canonicalizers preserve explicit null transitions, event-time project context, and typed heterogeneous inputs. Status-catalog edits never map to Issue status changes.
- Observability covers canonicalization, matching, insert failures, delivery states, retries, lease recovery, pauses, safe egress rejection, cleanup, and oldest pending age.
- Before implementation, fetch latest `upstream/main` and merge it into `feat/ios-push-notifications`. Preserve iOS/APNs and unrelated user work, resolve deliberately, verify affected areas, and allocate migrations after the merge. This merge is not part of this documentation-only phase.

## Testing Decisions

- Tests assert externally visible behavior, not private helpers, exact SQL, loop structure, or library details.
- The primary seam is one backend integration flow: Product Event -> persisted Webhook Delivery -> controlled HTTPS receiver. It starts through real product mutation behavior, observes the database-backed delivery, and verifies the exact received request.
- This seam covers all eight Product Events, envelope/IDs, HMAC, Event Selection, Workspace Scope, multi-project Project Scope, event-time matching, restart recovery, retry/terminal outcomes, automatic pause, and manual redelivery.
- A focused backend contract suite verifies event-source coverage across single/batch Issue updates, plugin/API writes, bulk transfers, background resets, Comment create/update/delete, explicit nulls, multi-field mutations, and attachment-only edits.
- Golden public-contract cases cover all events, actor and assignee variants, replies, deletion privacy, and Unicode-safe truncation.
- Security tests cover owner/admin authorization, workspace isolation, encryption/one-time disclosure, rotation/redaction, body/timestamp tampering, replay inputs, private targets, DNS rebinding, redirects, IPv4/IPv6, and operator allowlists.
- Reliability tests use persisted rows and controlled time/transport behavior for restart, lease expiry, network failure, 429, retryable 5xx, terminal 4xx, `Retry-After`, jitter, caps, success reset, retention, and receiver deduplication.
- Shared Workspace Settings UI keeps one narrow contract suite for event/project multi-select validation, one-time secret handling, pause/capability states, history/detail, redelivery, and shared web/desktop wiring.
- Shared API parsing includes malformed-response fallback coverage.
- Existing database fixtures and handler helpers are reused. Existing outbound subscribers/notification listeners are event-integration prior art; shared settings views are UI prior art.
- A generic controlled HTTPS receiver is the only external test destination. No concrete receiver product or receiver-specific behavior appears in tests.
- Completion requires UI creation of a scoped multi-event subscription with a persisted successful delivery, plus proof that a restarted server resumes pending work.
- Existing iOS notification, Inbox, realtime, web, desktop, backend, and API-compatibility tests remain regression gates.

## Out of Scope

- Concrete receiver names, receiver-specific adapters, rendering, payload templates, or message formats.
- Inbound webhooks.
- Actor-based filters.
- Wildcard events, automatic future-event opt-in, or broad `issue.updated`.
- Mention, task, reaction, member, workspace, chat, or inbox Product Events.
- Arbitrary custom headers or user-authored payload templates.
- Full Comment/attachment bodies, reactions, agent transcripts, raw runtime errors, or unrestricted content.
- Workspace-user-controlled private-network destinations.
- Exactly-once delivery or claims of transactionally lossless capture in v1.
- A transactional domain outbox added to every business transaction.
- Mobile management UI.
- Product implementation, upstream merge, commit, or push during this documentation-only phase.

## Further Notes

- This Spec is the v1 source of truth. Research notes, internal bus payloads, WebSocket messages, and plugin hooks are not public contracts.
- The post-commit crash window must remain visible in documentation and operations. Metrics and administrator-visible errors expose canonicalization/insertion failures; the feature must not claim lossless capture.
- Implementation should remain compact: establish contract/persistence/security/delivery and backend tests; close event-source gaps and add shared UI; then run integration/regression verification.
- Completion requires all supported mutation paths, restart-safe persisted delivery, correct scope and Event Selection, secret hygiene, and coexistence of the latest upstream base with preserved iOS/APNs work.
