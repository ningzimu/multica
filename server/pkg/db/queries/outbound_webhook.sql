-- name: CreateOutboundWebhookSubscription :one
INSERT INTO outbound_webhook_subscription (
    workspace_id, name, destination_ciphertext, secret_ciphertext,
    destination_hint, events, scope_mode, project_ids, created_by,
    signing_secret_hint
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING *;

-- name: LockOutboundWebhookScopeProjects :many
SELECT id FROM project
WHERE workspace_id = sqlc.arg('workspace_id')
  AND id = ANY(sqlc.arg('project_ids')::uuid[])
FOR KEY SHARE;

-- name: ListOutboundWebhookSubscriptions :many
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = $1
ORDER BY created_at ASC;

-- name: ListActiveOutboundWebhookSubscriptionsForEvent :many
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = sqlc.arg('workspace_id')
  AND status = 'active'
  AND events ? sqlc.arg('event_type')::text
ORDER BY created_at ASC
FOR UPDATE;

-- name: LockWorkspaceForOutboundWebhookCapture :one
-- Capture takes the workspace row before subscription rows. Workspace teardown
-- takes FOR UPDATE on the same row, so no delivery can commit behind its sweep.
SELECT id FROM workspace WHERE id = $1 FOR KEY SHARE;

-- name: GetOutboundWebhookSubscription :one
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2;

-- name: GetOutboundWebhookSubscriptionForUpdate :one
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2
FOR UPDATE;

-- name: UpdateOutboundWebhookSubscription :one
UPDATE outbound_webhook_subscription
SET name = $3,
    destination_ciphertext = $4,
    destination_hint = $5,
    events = $6,
    scope_mode = $7,
    project_ids = $8,
    updated_at = now()
WHERE workspace_id = $1 AND id = $2
RETURNING *;

-- name: PauseOutboundWebhookSubscription :one
WITH locked AS MATERIALIZED (
    SELECT subscription.id, subscription.status
    FROM outbound_webhook_subscription AS subscription
    WHERE subscription.workspace_id = $1 AND subscription.id = $2
    FOR UPDATE OF subscription
)
UPDATE outbound_webhook_subscription AS subscription
SET status = 'paused', pause_reason = 'manual', updated_at = now()
FROM locked
WHERE subscription.id = locked.id
RETURNING sqlc.embed(subscription), locked.status <> 'paused' AS newly_paused;

-- name: ResumeOutboundWebhookSubscription :one
UPDATE outbound_webhook_subscription
SET status = 'active', pause_reason = NULL,
    consecutive_terminal_failures = 0, updated_at = now()
WHERE workspace_id = $1 AND id = $2
RETURNING *;

-- name: RotateOutboundWebhookSigningSecret :one
UPDATE outbound_webhook_subscription
SET secret_ciphertext = $3,
    signing_secret_hint = $4,
    secret_version = secret_version + 1,
    updated_at = now()
WHERE workspace_id = $1 AND id = $2
RETURNING *;

-- name: UpdateOutboundWebhookSubscriptionScope :one
UPDATE outbound_webhook_subscription
SET scope_mode = sqlc.arg('scope_mode'),
    project_ids = sqlc.arg('project_ids')::uuid[],
    updated_at = now()
WHERE workspace_id = sqlc.arg('workspace_id') AND id = sqlc.arg('id')
RETURNING *;

-- name: RemoveProjectFromOutboundWebhookScopes :many
WITH locked AS MATERIALIZED (
    SELECT subscription.id, subscription.status,
           cardinality(array_remove(subscription.project_ids, sqlc.arg('project_id')::uuid)) = 0 AS scope_empty
    FROM outbound_webhook_subscription AS subscription
    WHERE subscription.workspace_id = sqlc.arg('workspace_id')
      AND subscription.scope_mode = 'project'
      AND subscription.project_ids @> ARRAY[sqlc.arg('project_id')::uuid]
    FOR UPDATE OF subscription
)
UPDATE outbound_webhook_subscription AS subscription
SET project_ids = array_remove(subscription.project_ids, sqlc.arg('project_id')::uuid),
    status = CASE
        WHEN locked.scope_empty THEN 'paused'
        ELSE subscription.status
    END,
    pause_reason = CASE
        WHEN locked.scope_empty THEN 'scope_empty'
        ELSE subscription.pause_reason
    END,
    updated_at = now()
FROM locked
WHERE subscription.id = locked.id
RETURNING subscription.id, locked.scope_empty AND locked.status <> 'paused' AS newly_paused;

-- name: DeleteOutboundWebhookDeliveriesBySubscription :exec
DELETE FROM outbound_webhook_delivery
WHERE workspace_id = $1 AND subscription_id = $2;

-- name: DeleteOutboundWebhookSubscription :execrows
DELETE FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2;

-- name: CreateOutboundWebhookDelivery :one
INSERT INTO outbound_webhook_delivery (
    event_id, subscription_id, workspace_id, event_type, request_body,
    signing_secret_ciphertext, destination_ciphertext, secret_version
) SELECT $1, $2, $3, $4, $5, $6, $7, $8
WHERE (
    SELECT count(*)
    FROM outbound_webhook_delivery AS existing
    WHERE existing.subscription_id = $2 AND existing.state = 'pending'
) < sqlc.arg('max_pending')::bigint
RETURNING *;

-- name: LockOutboundWebhookDeliveryClaim :exec
-- Call in a short transaction before ClaimDueOutboundWebhookDelivery. Keeping
-- lock acquisition in its own statement gives the claim a fresh READ COMMITTED
-- snapshot after any preceding claimant commits.
SELECT pg_advisory_xact_lock(hashtext('outbound_webhook'), hashtext('delivery_claim'));

-- name: ClaimDueOutboundWebhookDelivery :one
WITH candidate AS (
    SELECT delivery.id, delivery.lease_token IS NOT NULL AS lease_recovered
    FROM outbound_webhook_delivery AS delivery
    JOIN outbound_webhook_subscription AS subscription
      ON subscription.id = delivery.subscription_id
    WHERE delivery.state = 'pending'
      AND delivery.next_attempt_at <= now()
      AND (delivery.lease_expires_at IS NULL OR delivery.lease_expires_at <= now())
      AND delivery.attempt_count < sqlc.arg('max_attempts')::integer
      AND (subscription.status = 'active' OR delivery.event_type = 'webhook.test')
      AND (
          SELECT count(*) FROM outbound_webhook_delivery AS globally_leased
          WHERE globally_leased.state = 'pending'
            AND globally_leased.lease_token IS NOT NULL
            AND globally_leased.lease_expires_at > now()
      ) < sqlc.arg('max_global_concurrency')::bigint
      AND (
          SELECT count(*) FROM outbound_webhook_delivery AS subscription_leased
          WHERE subscription_leased.state = 'pending'
            AND subscription_leased.subscription_id = delivery.subscription_id
            AND subscription_leased.lease_token IS NOT NULL
            AND subscription_leased.lease_expires_at > now()
      ) < sqlc.arg('max_subscription_concurrency')::bigint
    ORDER BY delivery.next_attempt_at, delivery.created_at
    FOR UPDATE OF delivery SKIP LOCKED
    LIMIT 1
)
UPDATE outbound_webhook_delivery AS delivery
SET lease_token = gen_random_uuid(),
    lease_expires_at = now() + make_interval(secs => sqlc.arg('lease_seconds')::double precision),
    attempt_count = delivery.attempt_count + 1,
    last_attempt_at = now()
FROM candidate
WHERE delivery.id = candidate.id
RETURNING sqlc.embed(delivery), candidate.lease_recovered;

-- name: FailExhaustedOutboundWebhookDelivery :one
WITH target AS MATERIALIZED (
    SELECT delivery.id, delivery.subscription_id
    FROM outbound_webhook_delivery AS delivery
    JOIN outbound_webhook_subscription AS subscription
      ON subscription.id = delivery.subscription_id
    WHERE delivery.state = 'pending'
      AND delivery.next_attempt_at <= now()
      AND (delivery.lease_expires_at IS NULL OR delivery.lease_expires_at <= now())
      AND delivery.attempt_count >= sqlc.arg('max_attempts')::integer
      AND subscription.status = 'active'
    ORDER BY delivery.next_attempt_at, delivery.created_at
    LIMIT 1
), locked_subscription AS MATERIALIZED (
    SELECT subscription.id, subscription.status
    FROM outbound_webhook_subscription AS subscription
    JOIN target ON target.subscription_id = subscription.id
    WHERE subscription.status = 'active'
    FOR UPDATE OF subscription
), completed AS (
    UPDATE outbound_webhook_delivery AS delivery
    SET state = 'failed',
        failure_reason = 'maximum delivery attempts exhausted',
        completed_at = now(),
        lease_token = NULL,
        lease_expires_at = NULL
    FROM target, locked_subscription
    WHERE delivery.id = target.id
      AND delivery.subscription_id = locked_subscription.id
      AND delivery.state = 'pending'
      AND delivery.next_attempt_at <= now()
      AND (delivery.lease_expires_at IS NULL OR delivery.lease_expires_at <= now())
      AND delivery.attempt_count >= sqlc.arg('max_attempts')::integer
    RETURNING delivery.subscription_id
)
UPDATE outbound_webhook_subscription AS subscription
SET consecutive_terminal_failures = subscription.consecutive_terminal_failures + 1,
    status = CASE
        WHEN subscription.consecutive_terminal_failures + 1 >= sqlc.arg('failure_threshold') THEN 'paused'
        ELSE subscription.status
    END,
    pause_reason = CASE
        WHEN subscription.consecutive_terminal_failures + 1 >= sqlc.arg('failure_threshold') THEN 'failure_threshold'
        ELSE subscription.pause_reason
    END,
    updated_at = now()
FROM locked_subscription
WHERE subscription.id = (SELECT subscription_id FROM completed)
  AND subscription.id = locked_subscription.id
RETURNING subscription.id,
          locked_subscription.status <> 'paused' AND subscription.status = 'paused' AS newly_paused;

-- name: ReleaseClaimedOutboundWebhookDelivery :execrows
UPDATE outbound_webhook_delivery
SET lease_token = NULL, lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'pending';

-- name: RetryClaimedOutboundWebhookDelivery :execrows
UPDATE outbound_webhook_delivery
SET response_status = $3,
    failure_reason = $4,
    response_excerpt = sqlc.arg('response_excerpt'),
    next_attempt_at = sqlc.arg('next_attempt_at'),
    lease_token = NULL,
    lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'pending';

-- name: SucceedClaimedOutboundWebhookDelivery :execrows
WITH locked_subscription AS MATERIALIZED (
    SELECT subscription.id
    FROM outbound_webhook_subscription AS subscription
    JOIN outbound_webhook_delivery AS delivery
      ON delivery.subscription_id = subscription.id
    WHERE delivery.id = $1 AND delivery.lease_token = $2 AND delivery.state = 'pending'
    FOR UPDATE OF subscription
), completed AS (
    UPDATE outbound_webhook_delivery AS delivery
    SET state = 'succeeded',
        response_status = $3,
        response_excerpt = sqlc.arg('response_excerpt'),
        failure_reason = NULL,
        completed_at = now(),
        lease_token = NULL,
        lease_expires_at = NULL
    FROM locked_subscription
    WHERE delivery.id = $1
      AND delivery.subscription_id = locked_subscription.id
      AND delivery.lease_token = $2
      AND delivery.state = 'pending'
    RETURNING delivery.subscription_id
)
UPDATE outbound_webhook_subscription
SET consecutive_terminal_failures = 0,
    updated_at = now()
WHERE id = (SELECT subscription_id FROM completed);

-- name: FailClaimedOutboundWebhookDelivery :one
WITH locked_subscription AS MATERIALIZED (
    SELECT subscription.id, subscription.status
    FROM outbound_webhook_subscription AS subscription
    JOIN outbound_webhook_delivery AS delivery
      ON delivery.subscription_id = subscription.id
    WHERE delivery.id = $1 AND delivery.lease_token = $2 AND delivery.state = 'pending'
    FOR UPDATE OF subscription
), completed AS (
    UPDATE outbound_webhook_delivery AS delivery
    SET state = 'failed',
        response_status = $3,
        failure_reason = $4,
        response_excerpt = sqlc.arg('response_excerpt'),
        completed_at = now(),
        lease_token = NULL,
        lease_expires_at = NULL
    FROM locked_subscription
    WHERE delivery.id = $1
      AND delivery.subscription_id = locked_subscription.id
      AND delivery.lease_token = $2
      AND delivery.state = 'pending'
    RETURNING delivery.subscription_id
)
UPDATE outbound_webhook_subscription AS subscription
SET consecutive_terminal_failures = subscription.consecutive_terminal_failures + 1,
    status = CASE
        WHEN subscription.consecutive_terminal_failures + 1 >= sqlc.arg('failure_threshold') THEN 'paused'
        ELSE subscription.status
    END,
    pause_reason = CASE
        WHEN subscription.consecutive_terminal_failures + 1 >= sqlc.arg('failure_threshold') THEN 'failure_threshold'
        ELSE subscription.pause_reason
    END,
    updated_at = now()
FROM locked_subscription
WHERE subscription.id = (SELECT subscription_id FROM completed)
  AND subscription.id = locked_subscription.id
RETURNING subscription.id,
          locked_subscription.status <> 'paused' AND subscription.status = 'paused' AS newly_paused;

-- name: GetOutboundWebhookDelivery :one
SELECT * FROM outbound_webhook_delivery WHERE id = $1;

-- name: ListOutboundWebhookDeliveries :many
SELECT * FROM outbound_webhook_delivery
WHERE workspace_id = sqlc.arg('workspace_id')
  AND subscription_id = sqlc.arg('subscription_id')
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg('page_limit') OFFSET sqlc.arg('page_offset');

-- name: CountOutboundWebhookDeliveries :one
SELECT count(*) FROM outbound_webhook_delivery
WHERE workspace_id = sqlc.arg('workspace_id')
  AND subscription_id = sqlc.arg('subscription_id');

-- name: GetOutboundWebhookDeliveryForHistory :one
SELECT * FROM outbound_webhook_delivery
WHERE workspace_id = sqlc.arg('workspace_id')
  AND subscription_id = sqlc.arg('subscription_id')
  AND id = sqlc.arg('id');

-- name: CreateOutboundWebhookRedelivery :one
INSERT INTO outbound_webhook_delivery (
    event_id, subscription_id, workspace_id, event_type, request_body,
    signing_secret_ciphertext, destination_ciphertext, secret_version,
    redelivery_of_id
)
SELECT original.event_id, original.subscription_id, original.workspace_id,
       original.event_type, original.request_body, subscription.secret_ciphertext,
       subscription.destination_ciphertext, subscription.secret_version, original.id
FROM outbound_webhook_delivery AS original
JOIN outbound_webhook_subscription AS subscription
  ON subscription.id = original.subscription_id
 AND subscription.workspace_id = original.workspace_id
WHERE original.workspace_id = sqlc.arg('workspace_id')
  AND original.subscription_id = sqlc.arg('subscription_id')
  AND original.id = sqlc.arg('delivery_id')
  AND subscription.status = 'active'
RETURNING outbound_webhook_delivery.*;

-- name: CleanupExpiredOutboundWebhookDeliveries :execrows
DELETE FROM outbound_webhook_delivery
WHERE (state IN ('succeeded', 'failed') AND completed_at < sqlc.arg('cutoff'))
   OR (state = 'pending' AND created_at < sqlc.arg('cutoff'));

-- name: OldestPendingOutboundWebhookDelivery :one
SELECT COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0)::double precision AS age_seconds
FROM outbound_webhook_delivery
WHERE state = 'pending';
