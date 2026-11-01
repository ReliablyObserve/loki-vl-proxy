---
name: reuse-existing-test-stack
description: Use for local Docker Compose, Grafana, Loki, VictoriaLogs, proxy, browser, or end-to-end verification. Always reuse the user's existing stack and test assets; do not create duplicate stacks or services.
---

# Reuse the existing test stack

Use this skill whenever a repository has an existing local Grafana or
end-to-end stack, or a task asks to rebuild, verify, or exercise one.

1. Inspect running containers, ports, Compose project labels, health, and
   existing service URLs before starting or changing anything.
2. Reuse the existing Grafana, proxy, Loki, VictoriaLogs, datasets, fixtures,
   test helpers, and CI-generated evidence where possible. Search for an
   existing test before adding a new one; extend it only for uncovered behavior.
3. Do not launch another Compose project or duplicate Grafana/backend/proxy
   services. Run test groups sequentially against the single existing stack.
4. Check test statefulness first. Re-ingestion may duplicate data or invalidate
   parity comparisons. Reuse existing fixtures if safe; if clean state is
   required, reset the existing stack in place only when its data can safely
   be discarded and the user's stack identity is clear.
5. Never run broad cleanup or `down -v` unless every affected container and
   volume was created by this task and is confirmed disposable. Preserve
   unrelated containers, named volumes, and user data.
6. If the existing stack is missing a required service or has incompatible
   state, complete checks that do not require it and report the exact blocker.
   Ask before creating a replacement stack.
7. Report which existing services and tests were reused, what was executed,
   and any state or compatibility limits. Do not claim end-to-end proof from
   unit tests or from a different stack.

For this repository, these instructions supplement the E2E requirements in
`AGENTS.md` and the `loki-vl-proxy-dev` skill. When a test contract calls for a
fresh dataset, use a controlled reset of the existing stack rather than a
parallel stack, subject to the data-safety rules above.
