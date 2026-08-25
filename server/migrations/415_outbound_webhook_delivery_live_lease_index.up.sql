CREATE INDEX CONCURRENTLY idx_outbound_webhook_delivery_live_lease
    ON outbound_webhook_delivery (subscription_id, lease_expires_at)
    WHERE state = 'pending' AND lease_token IS NOT NULL;
