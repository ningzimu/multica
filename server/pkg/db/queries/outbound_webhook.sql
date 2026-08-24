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
WHERE workspace_id = $1 AND events ? sqlc.arg('event_type')::text
ORDER BY created_at ASC
FOR SHARE;

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
) VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: MarkOutboundWebhookDeliverySucceeded :exec
UPDATE outbound_webhook_delivery
SET state = 'succeeded', attempt_count = attempt_count + 1,
    response_status = $2, failure_reason = NULL, completed_at = now()
WHERE id = $1;

-- name: MarkOutboundWebhookDeliveryFailed :exec
UPDATE outbound_webhook_delivery
SET state = 'failed', attempt_count = attempt_count + 1,
    response_status = $2, failure_reason = $3, completed_at = now()
WHERE id = $1;

-- name: GetOutboundWebhookDelivery :one
SELECT * FROM outbound_webhook_delivery WHERE id = $1;
