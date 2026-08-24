CREATE INDEX CONCURRENTLY idx_outbound_webhook_delivery_due
    ON outbound_webhook_delivery (next_attempt_at, created_at)
    WHERE state = 'pending';
