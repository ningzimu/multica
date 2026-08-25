CREATE TABLE recipient_device (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    installation_id UUID NOT NULL,
    user_id UUID NOT NULL,
    platform TEXT NOT NULL CHECK (platform IN ('ios')),
    bundle_id TEXT NOT NULL,
    push_environment TEXT NOT NULL CHECK (push_environment IN ('sandbox', 'production')),
    device_token TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ,
    invalidated_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
