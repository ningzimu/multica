ALTER TABLE outbound_webhook_delivery
    ADD COLUMN redelivery_of_id UUID,
    ADD COLUMN response_excerpt TEXT;
