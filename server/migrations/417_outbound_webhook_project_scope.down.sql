UPDATE outbound_webhook_subscription
SET scope_mode = 'workspace',
    project_ids = '{}',
    status = 'paused',
    pause_reason = 'scope_removed',
    updated_at = now()
WHERE scope_mode = 'project';

ALTER TABLE outbound_webhook_subscription
    DROP CONSTRAINT outbound_webhook_subscription_scope_shape_check,
    DROP COLUMN project_ids,
    ADD CONSTRAINT outbound_webhook_subscription_scope_mode_check CHECK (scope_mode = 'workspace');
