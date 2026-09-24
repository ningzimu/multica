package migrations

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// These exact pairs were already shipped independently by this fork and
// upstream before integration. Versions are full filename stems, so changing
// them would replay applied DDL. This is a closed historical exception list:
// another file sharing any of these numbers still fails the uniqueness check.
// See docs/agents/migration-history.md.
var integratedMigrationPrefixPairs = map[[2]string]bool{
	{"404_agent_starter_prompts", "404_recipient_device_installation_index"}:                                 true,
	{"407_issue_source_context", "407_runtime_profile_add_zeroclaw"}:                                         true,
	{"408_create_outbound_webhook", "408_issue_source_context_id_index"}:                                     true,
	{"409_issue_source_context_issue_index", "409_outbound_webhook_subscription_id_index"}:                   true,
	{"410_issue_source_context_origin_task_index", "410_outbound_webhook_subscription_workspace_index"}:      true,
	{"411_attachment_source_context_index", "411_outbound_webhook_delivery_id_index"}:                        true,
	{"412_issue_source_context_object_intent_key_index", "412_outbound_webhook_delivery_subscription_index"}: true,
	{"413_issue_source_context_object_intent_due_index", "413_outbound_webhook_delivery_reliability"}:        true,
	{"414_issue_source_context_object_intent_context_index", "414_outbound_webhook_delivery_due_index"}:      true,
	{"415_outbound_webhook_delivery_live_lease_index", "415_seat_capacity_outbox"}:                           true,
	{"416_outbound_webhook_secret_versions", "416_seat_capacity_operation_token_index"}:                      true,
	{"417_outbound_webhook_project_scope", "417_seat_capacity_primary_key"}:                                  true,
	{"418_outbound_webhook_delivery_history", "418_seat_capacity_due_index"}:                                 true,
	{"419_outbound_webhook_delivery_retention_index", "419_seat_capacity_share_join_index"}:                  true,
}

func TestMigrationNumericPrefixesAreUnique(t *testing.T) {
	files := migrationFilesForLint(t, "*.up.sql")

	// Migrations through 128 contain historical duplicate numeric prefixes.
	// From 129 onward, apart from the exact integrated pairs above, keep the
	// numeric sequence unique so release tooling and
	// operators can identify one schema change unambiguously by its number.
	const firstUniqueMigrationNumber = 129
	stemByNumber := make(map[int]string)
	for _, file := range files {
		stem, _, ok := splitMigrationFilename(filepath.Base(file))
		if !ok {
			continue
		}
		prefix, _, ok := strings.Cut(stem, "_")
		if !ok {
			continue
		}
		number, err := strconv.Atoi(prefix)
		if err != nil || number < firstUniqueMigrationNumber {
			continue
		}
		if previous, exists := stemByNumber[number]; exists {
			if integratedMigrationPrefixPairs[[2]string{previous, stem}] {
				continue
			}
			t.Errorf("migrations %s and %s share numeric prefix %s", previous, stem, prefix)
			continue
		}
		stemByNumber[number] = stem
	}
}

func TestMigrationFilesHaveMatchingDirections(t *testing.T) {
	files := migrationFilesForLint(t, "*.sql")

	directionsByStem := make(map[string]map[string]bool)
	for _, file := range files {
		stem, direction, ok := splitMigrationFilename(filepath.Base(file))
		if !ok {
			continue
		}
		if directionsByStem[stem] == nil {
			directionsByStem[stem] = make(map[string]bool)
		}
		directionsByStem[stem][direction] = true
	}

	for stem, directions := range directionsByStem {
		if !directions["up"] || !directions["down"] {
			t.Errorf("migration %s must have both .up.sql and .down.sql files", stem)
		}
	}
}

func migrationFilesForLint(t *testing.T, pattern string) []string {
	t.Helper()

	dir := realMigrationsDir(t)
	files, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no migration files matched %s in %s", pattern, dir)
	}
	sort.Strings(files)
	return files
}

func realMigrationsDir(t *testing.T) string {
	t.Helper()

	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve migration lint test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(self), "..", "..", "migrations"))
}

func splitMigrationFilename(name string) (stem, direction string, ok bool) {
	for _, candidateDirection := range []string{"up", "down"} {
		suffix := fmt.Sprintf(".%s.sql", candidateDirection)
		if strings.HasSuffix(name, suffix) {
			return strings.TrimSuffix(name, suffix), candidateDirection, true
		}
	}
	return "", "", false
}
