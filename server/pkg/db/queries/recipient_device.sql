-- name: LockRecipientDeviceToken :exec
SELECT pg_advisory_xact_lock(
    hashtextextended(
        CAST(sqlc.arg(bundle_id) AS text) || E'\x1f' ||
        CAST(sqlc.arg(push_environment) AS text) || E'\x1f' ||
        CAST(sqlc.arg(device_token) AS text),
        0
    )
);

-- name: DisplaceRecipientDeviceToken :exec
UPDATE recipient_device
SET
    enabled = FALSE,
    revoked_at = now(),
    updated_at = now()
WHERE device_token = sqlc.arg(device_token)
  AND bundle_id = sqlc.arg(bundle_id)
  AND push_environment = sqlc.arg(push_environment)
  AND installation_id <> sqlc.arg(installation_id)
  AND enabled;

-- name: UpsertRecipientDevice :one
INSERT INTO recipient_device (
    installation_id,
    user_id,
    platform,
    bundle_id,
    push_environment,
    device_token
)
VALUES (
    sqlc.arg(installation_id),
    sqlc.arg(user_id),
    sqlc.arg(platform),
    sqlc.arg(bundle_id),
    sqlc.arg(push_environment),
    sqlc.arg(device_token)
)
ON CONFLICT (installation_id, bundle_id, push_environment)
DO UPDATE SET
    user_id = EXCLUDED.user_id,
    platform = EXCLUDED.platform,
    device_token = EXCLUDED.device_token,
    enabled = TRUE,
    bound_at = now(),
    revoked_at = NULL,
    invalidated_at = NULL,
    last_seen_at = now(),
    updated_at = now()
RETURNING *;

-- name: RevokeRecipientDevice :one
UPDATE recipient_device
SET
    enabled = FALSE,
    revoked_at = now(),
    updated_at = now()
WHERE installation_id = $1
  AND user_id = $2
RETURNING *;

-- name: ListActiveRecipientDevicesForPush :many
SELECT *
FROM recipient_device
WHERE user_id = $1
  AND bundle_id = $2
  AND push_environment = $3
  AND enabled
ORDER BY last_seen_at DESC;

-- name: InvalidateRecipientDevice :exec
UPDATE recipient_device
SET
    enabled = FALSE,
    invalidated_at = now(),
    updated_at = now()
WHERE id = $1;
