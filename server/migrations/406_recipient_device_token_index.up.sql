CREATE UNIQUE INDEX CONCURRENTLY idx_recipient_device_token_active ON recipient_device (device_token, bundle_id, push_environment) WHERE enabled;
