package migrations

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOutboundWebhookSecretVersionMigrationBackfillsExistingDeliveries(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("integration test requires Postgres at DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `
		CREATE TEMP TABLE outbound_webhook_subscription (
			id UUID NOT NULL,
			workspace_id UUID NOT NULL,
			destination_ciphertext BYTEA NOT NULL,
			secret_ciphertext BYTEA NOT NULL,
			signing_secret_hint TEXT NOT NULL DEFAULT '',
			secret_version INTEGER NOT NULL DEFAULT 1 CHECK (secret_version > 0)
		);
		CREATE TEMP TABLE outbound_webhook_delivery (
			id UUID NOT NULL,
			subscription_id UUID NOT NULL,
			workspace_id UUID NOT NULL,
			signing_secret_ciphertext BYTEA,
			secret_version INTEGER NOT NULL DEFAULT 1 CHECK (secret_version > 0)
		)
	`); err != nil {
		t.Fatal(err)
	}
	applyMigrationFile(t, ctx, conn.Conn(), "416_outbound_webhook_secret_versions.down.sql")

	const subscriptionID = "00000000-0000-0000-0000-000000000016"
	const workspaceID = "00000000-0000-0000-0000-000000000001"
	const deliveryID = "00000000-0000-0000-0000-000000000017"
	if _, err := conn.Exec(ctx, `
		INSERT INTO outbound_webhook_subscription (id, workspace_id, destination_ciphertext, secret_ciphertext)
		VALUES ($1, $2, $3, $4)
	`, subscriptionID, workspaceID, []byte("encrypted-destination"), []byte("encrypted-secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO outbound_webhook_delivery (id, subscription_id, workspace_id)
		VALUES ($1, $2, $3)
	`, deliveryID, subscriptionID, workspaceID); err != nil {
		t.Fatal(err)
	}
	applyMigrationFile(t, ctx, conn.Conn(), "416_outbound_webhook_secret_versions.up.sql")

	var ciphertext []byte
	var destinationCiphertext []byte
	var version int32
	if err := conn.QueryRow(ctx, `
		SELECT signing_secret_ciphertext, destination_ciphertext, secret_version
		FROM outbound_webhook_delivery WHERE id = $1
	`, deliveryID).Scan(&ciphertext, &destinationCiphertext, &version); err != nil {
		t.Fatal(err)
	}
	if string(ciphertext) != "encrypted-secret" || string(destinationCiphertext) != "encrypted-destination" || version != 1 {
		t.Fatalf("existing delivery was not backfilled: secret=%q destination=%q version=%d", ciphertext, destinationCiphertext, version)
	}
}
