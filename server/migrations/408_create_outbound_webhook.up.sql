CREATE TABLE outbound_webhook_subscription (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    name TEXT NOT NULL,
    destination_ciphertext BYTEA NOT NULL,
    secret_ciphertext BYTEA NOT NULL,
    destination_hint TEXT NOT NULL,
    events JSONB NOT NULL,
    event_catalog_version INTEGER NOT NULL DEFAULT 1,
    scope_mode TEXT NOT NULL DEFAULT 'workspace' CHECK (scope_mode = 'workspace'),
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (char_length(name) BETWEEN 1 AND 100),
    CHECK (jsonb_typeof(events) = 'array' AND jsonb_array_length(events) > 0)
);

CREATE TABLE outbound_webhook_delivery (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    subscription_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    request_body BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'succeeded', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    response_status INTEGER,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
