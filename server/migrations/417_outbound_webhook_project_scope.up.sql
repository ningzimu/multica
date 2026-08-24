ALTER TABLE outbound_webhook_subscription
    DROP CONSTRAINT outbound_webhook_subscription_scope_mode_check,
    ADD COLUMN project_ids UUID[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT outbound_webhook_subscription_scope_shape_check CHECK (
        (scope_mode = 'workspace' AND cardinality(project_ids) = 0)
        OR
        (scope_mode = 'project' AND (
            cardinality(project_ids) > 0
            OR (status = 'paused' AND pause_reason = 'scope_empty')
        ))
    );
