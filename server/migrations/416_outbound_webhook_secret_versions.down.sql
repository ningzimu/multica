ALTER TABLE outbound_webhook_delivery
    DROP COLUMN IF EXISTS secret_version,
    DROP COLUMN IF EXISTS signing_secret_ciphertext,
    DROP COLUMN IF EXISTS destination_ciphertext;

ALTER TABLE outbound_webhook_subscription
    DROP COLUMN IF EXISTS secret_version,
    DROP COLUMN IF EXISTS signing_secret_hint;
