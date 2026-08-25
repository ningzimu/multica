ALTER TABLE outbound_webhook_delivery
    DROP COLUMN IF EXISTS last_attempt_at,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS lease_token,
    DROP COLUMN IF EXISTS next_attempt_at;

ALTER TABLE outbound_webhook_subscription
    DROP COLUMN IF EXISTS consecutive_terminal_failures,
    DROP COLUMN IF EXISTS pause_reason,
    DROP COLUMN IF EXISTS status;
