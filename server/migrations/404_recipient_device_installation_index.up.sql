CREATE UNIQUE INDEX CONCURRENTLY idx_recipient_device_installation ON recipient_device (installation_id, bundle_id, push_environment);
