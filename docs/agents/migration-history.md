# Fork migration history

The September 2026 upstream integration retains the already-committed APNs and
Outbound Webhook migration filenames (403–419), including the earlier relocation
of the ZeroClaw migration to 407. Upstream independently used several of these
numeric prefixes. The migration ledger and readiness checks identify versions by
the **full filename stem**, not the number alone.

Do not rename these applied migrations to make the numbers look unique: an
existing installation would see a different version and could replay table or
index creation. The migration lint has a closed list of the 14 exact historical
pairs introduced by this integration. It still rejects any new duplicate,
including a third migration using an already-excepted prefix. New migrations must
use an unused number after the current sequence.

Both a fresh database and an upgrade from the prior develop migration set were
verified during integration. The concurrent-index retry cleanup registry retains
both branches' full version names. No production migration ledger or database was
rewritten.
