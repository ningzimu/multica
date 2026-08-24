ALTER TABLE outbound_webhook_subscription
    ADD COLUMN status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused')),
    ADD COLUMN pause_reason TEXT,
    ADD COLUMN consecutive_terminal_failures INTEGER NOT NULL DEFAULT 0
        CHECK (consecutive_terminal_failures >= 0);

ALTER TABLE outbound_webhook_delivery
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN last_attempt_at TIMESTAMPTZ;
