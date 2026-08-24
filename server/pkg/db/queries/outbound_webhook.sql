-- name: CreateOutboundWebhookSubscription :one
INSERT INTO outbound_webhook_subscription (
    workspace_id, name, destination_ciphertext, secret_ciphertext,
    destination_hint, events, created_by
) VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

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

-- name: GetActiveOutboundWebhookSubscription :one
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2 AND status = 'active';

-- name: GetOutboundWebhookSubscriptionForUpdate :one
SELECT * FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2
FOR UPDATE;

-- name: UpdateOutboundWebhookSubscriptionEvents :one
UPDATE outbound_webhook_subscription
SET events = $3, updated_at = now()
WHERE workspace_id = $1 AND id = $2
RETURNING *;

-- name: DeleteOutboundWebhookDeliveriesBySubscription :exec
DELETE FROM outbound_webhook_delivery
WHERE workspace_id = $1 AND subscription_id = $2;

-- name: DeleteOutboundWebhookSubscription :execrows
DELETE FROM outbound_webhook_subscription
WHERE workspace_id = $1 AND id = $2;

-- name: CreateOutboundWebhookDelivery :one
INSERT INTO outbound_webhook_delivery (
    event_id, subscription_id, workspace_id, event_type, request_body
) SELECT $1, $2, $3, $4, $5
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
    SELECT delivery.id
    FROM outbound_webhook_delivery AS delivery
    JOIN outbound_webhook_subscription AS subscription
      ON subscription.id = delivery.subscription_id
    WHERE delivery.state = 'pending'
      AND delivery.next_attempt_at <= now()
      AND (delivery.lease_expires_at IS NULL OR delivery.lease_expires_at <= now())
      AND delivery.attempt_count < sqlc.arg('max_attempts')::integer
      AND subscription.status = 'active'
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
RETURNING delivery.*;

-- name: FailExhaustedOutboundWebhookDelivery :execrows
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
    SELECT subscription.id
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
WHERE id = (SELECT subscription_id FROM completed);

-- name: ReleaseClaimedOutboundWebhookDelivery :execrows
UPDATE outbound_webhook_delivery
SET lease_token = NULL, lease_expires_at = NULL
WHERE id = $1 AND lease_token = $2 AND state = 'pending';

-- name: RetryClaimedOutboundWebhookDelivery :execrows
UPDATE outbound_webhook_delivery
SET response_status = $3,
    failure_reason = $4,
    next_attempt_at = $5,
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

-- name: FailClaimedOutboundWebhookDelivery :execrows
WITH locked_subscription AS MATERIALIZED (
    SELECT subscription.id
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
WHERE id = (SELECT subscription_id FROM completed);

-- name: GetOutboundWebhookDelivery :one
SELECT * FROM outbound_webhook_delivery WHERE id = $1;
