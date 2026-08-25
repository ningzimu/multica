CREATE INDEX CONCURRENTLY IF NOT EXISTS outbound_webhook_subscription_workspace_idx ON outbound_webhook_subscription (workspace_id, created_at);
