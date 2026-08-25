ALTER TABLE outbound_webhook_subscription
    ADD COLUMN signing_secret_hint TEXT NOT NULL DEFAULT '',
    ADD COLUMN secret_version INTEGER NOT NULL DEFAULT 1
        CHECK (secret_version > 0);

ALTER TABLE outbound_webhook_delivery
    ADD COLUMN signing_secret_ciphertext BYTEA,
    ADD COLUMN destination_ciphertext BYTEA,
    ADD COLUMN secret_version INTEGER NOT NULL DEFAULT 1
        CHECK (secret_version > 0);

UPDATE outbound_webhook_delivery AS delivery
SET signing_secret_ciphertext = subscription.secret_ciphertext,
    destination_ciphertext = subscription.destination_ciphertext,
    secret_version = subscription.secret_version
FROM outbound_webhook_subscription AS subscription
WHERE subscription.id = delivery.subscription_id
  AND subscription.workspace_id = delivery.workspace_id
  AND delivery.signing_secret_ciphertext IS NULL;
