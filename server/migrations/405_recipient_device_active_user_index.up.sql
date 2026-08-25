CREATE INDEX CONCURRENTLY idx_recipient_device_active_user ON recipient_device (user_id, last_seen_at DESC) WHERE enabled;
