# Domain Docs

This repository uses a multi-context layout.

Before changing domain behavior:

1. Read `CONTEXT-MAP.md`.
2. Read each relevant `CONTEXT.md`.
3. Read relevant ADRs.

System-wide decisions live in `docs/adr/`.
Mobile decisions may live in `apps/mobile/docs/adr/`.
Server decisions may live in `server/docs/adr/`.

Use the terms defined by the relevant `CONTEXT.md`.
If a proposed change conflicts with an ADR, report the conflict before changing code.

Missing context files are created only when domain terms or decisions are ready to record.
