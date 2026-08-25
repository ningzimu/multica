CREATE INDEX CONCURRENTLY outbound_webhook_delivery_retention_idx ON outbound_webhook_delivery (completed_at) WHERE state IN ('succeeded', 'failed');
