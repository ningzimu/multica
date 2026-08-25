ALTER TABLE outbound_webhook_delivery
    DROP COLUMN IF EXISTS response_excerpt,
    DROP COLUMN IF EXISTS redelivery_of_id;
