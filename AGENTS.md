# Repository working rules

## No AGPL code in the repository (hard rule)

Grafana Loki is AGPL-3.0. No AGPL code is added to, copied into or adapted in
this repository: not in product code, and not in tests, scripts, benchmarks or
generators either. Loki is used only as a test comparison.

- Loki's packages are imported only by the test-only oracle modules under
  `test/oracle/loki`, which have their own `go.mod` and are never built into the
  proxy or anything the project ships. They run Loki on test inputs and record
  its answers.
- Recorded answers are data and may be committed (for example
  `internal/proxy/testdata/step_align_loki_answers.jsonl.gz`). Loki's source is
  not: never paste, transcribe or port a Loki function, test helper or
  reference model. Write the proxy's code from the behaviour (the HTTP API,
  recorded answers, observed responses) and compare it with Loki through an
  oracle.
- The gates:
  - `TestProductModuleHasNoOracleDependency`: the proxy's `go.mod` and `go.sum`
    never name `github.com/grafana/loki`.
  - The release workflow's SBOM check, which fails if a published SBOM names
    `grafana/loki`.
  - The similarity scan in the PR quality gate (`test/oracle/loki similar`:
    50-token runs against Loki's `pkg/`, tests included). A file that shares a
    run fails the gate. `test/oracle/loki/similarity-allowlist.json` holds
    reviewed exceptions for API names only (such as the template function
    names a query may call), never code. Its limits are never raised to make a
    pull request pass.

## Loki difference registry and query matrix

Every difference discovered between the proxy and Loki must enter the existing
conformance registry and the typed query matrix before it is considered covered.
Use `conformance/registry/cases/` for the gap and Loki evidence,
`test/e2e-compat/query-semantics-matrix.json` for the query contract, and
`test/e2e-compat/query-semantics-operations.json` for its operation and depth
path. Link the registry ID, matrix case and focused differential test so CI
reports the same behavior from one catalog. Extend existing fixtures and
harnesses; do not create a parallel registry or test suite.

A case that needs several queries in the plan corpus lists them under
`corpus.dimensions.shapes` (`- query: '...'` per row) in its case file: the
corpus walker reads every `query` key of a case, so each shape becomes a corpus
row the shadows, the oracles and the route-parity digests see. Use it for the
spellings or nestings of one difference (for example
`semantics/stddev-stdvar-nested-or-binary`); a different difference gets its
own case.

- Record the actual ingest path, query stages, response encoding, tenant and
  time window that expose the difference. A passing variant with a different
  ingest path does not close the original gap.
- Compare complete Loki and proxy answers for correctness, including labels,
  line content, timestamps, metadata categories and expected errors as relevant.
  Keep availability-only checks and `known_deviation` cases visibly open.
- Fix each difference at the layer that can preserve its required information,
  then retain a regression test and pinned-Loki proof in CI. Close the registry
  gap only after those checks pass; document any still-open axes explicitly.
- Reuse matrix cases for later performance scenarios by case ID. Keep visual
  proof in its existing separate harness and add a PR-specific capture when the
  behavior changes what Grafana or Drilldown shows.

## Errors are covered like answers

Every Loki error message is a registry item, every route x error kind is a cell of the generated matrix
(`docs/error-compatibility-coverage.md`), and both are checked against recorded Loki answers: the unit replay
(`TestLokiErrorAnswersReplay`) and the end-to-end comparison (`TestCompat_ErrorAnswersLikeLoki`).

- A pull request that flips rows of `conformance/registry/error-layer-known-differences.json`, adds a `lokierr`
  message, a gate route or failure mode, or records a new row reruns `python3 conformance/scripts/error_registry.py`
  and commits what it writes. The scores (errors that match Loki, matrix coverage) may not drop; the quality report
  shows them and the conformance gate fails on a stale file.
- A new `lokierr` message, error-layer gate route or failure mode needs an entry in
  `conformance/registry/error-coverage-spec.json` (its stage in Loki's order of checks, the route or kind it belongs
  to, or the reason and test when Loki cannot be asked), or the conformance gate fails.
- Record new answers with `go run ./test/errorrecord` against a running Loki; add rows by hand, never edit a recorded answer.

## Fail fast like Loki (hard rule)

A query Loki rejects quickly (a parse error, a validation error, a limit) the proxy rejects just as early: at the first
parse or validation step of the route, before the response cache lookup, the query plan, the translation, response key
fences, fan-out or any request to VictoriaLogs. The status, `errorType` and error text are Loki's, recorded with the
Loki oracle (`test/oracle/loki`, API only).

- Parsers and validators terminate quickly with bounded memory on any input. A termination fuzz target (a time and an
  allocation bound per input) runs in fuzz-smoke; every parser or validator change adds its seeds.
- Route order is validate, then cache, then plan. When a route validates after its cache, fix the order instead of
  adding another response key fence.
- Record Loki's answers for the inputs a change touches (acceptance and error text), and test the rejection both at
  the parser and through the HTTP route (no upstream call is made for a rejected query).
- A pull request that touches parsing or validation says in its body that rejected inputs still fail before any other
  work.

## Cache semantic layers

Keep the proxy's Loki-facing cache and VictoriaLogs-facing backend cache
logically separate. Loki API responses and LogQL-derived fragments have a
different contract from VL API responses and backend-result fragments.

- Use distinct versioned key namespaces, typed payloads and explicit conversion
  between layers. Never decode a backend payload as a Loki response or reuse a
  Loki response as a backend result.
- Keep each layer's admission, completeness, freshness, expiry, coalescing and
  invalidation rules explicit. Preserve tenant/auth/backend scope and the
  relevant parser/profile, time bounds, step phase, ordering and limits.
- Both layers may share bounded RAM pools, an SSD engine or files, codecs,
  allocation pools and peer transport. Sharing physical resources does not
  merge semantic identities or validity rules.
- Account for semantic layers separately and enforce aggregate physical
  budgets. One layer's hit is not another layer's hit. Purging one layer does
  not prove another was invalidated; deletion/security revocation must cover
  all affected layers, replicas and in-flight writes explicitly.
- Resource statistics must distinguish each layer's occupancy, read/write
  bytes and rates, hits/misses, admissions/rejections and evictions. Keep
  logical RAM/encoded SSD bytes separate from process RSS, file high-water
  and physical device throughput or IOPS.
- Shared allocation policies require hard total bounds, explicit per-layer
  minimum/maximum allocations and bounded borrowing. Adaptive policies must
  preserve those bounds, avoid starvation and use bounded adjustment with
  hysteresis rather than reacting to every request.
- Support separate layer directories/mounts. When paths share a filesystem,
  enforce both layer limits and a shared physical budget with free-space
  headroom, including file/index overhead and temporary compaction space.
  Logical record eviction alone does not bound a bbolt file's physical size.
- Byte or percentage settings must resolve against finite operator budgets
  and effective container/host limits. Auto mode may adjust target shares,
  never bypass hard ceilings. Include auxiliary caches and reserved in-flight
  cache work in RAM accounting; a payload limit is not a process RSS limit.
- Runtime configuration, Helm values and generated references must agree on
  defaults, precedence and validation. Do not expose a setting until its
  enforcement works; test shared/separate mounts, invalid budgets and pressure.
- Tests must prove key/payload isolation across layers. Benchmarks distinguish
  proxy Loki-facing cache, proxy VL-facing cache and the reference Loki
  server's own caches. A disabled cache is not a measured lookup miss.

See [Architecture](docs/architecture.md#cache-semantic-layers-and-storage-tiers)
for the distinction between semantic layers and physical storage tiers.

## Cache change evidence in pull requests

Every PR that changes cache behavior must put a readable **main versus PR**
matrix in its PR summary, with links to retained, reproducible evidence.

- Identify measured base/candidate commits, dataset, versions, budgets, node
  count, replication factor, Tier0 state and priming. Preserve the actual
  measured source identities after a rebase; label older results historical.
- Compare relevant cold, exact-repeat, shifted-window, refresh, disk-restart,
  peer-loss and pressure cases. Mark every untested relevant state explicitly;
  do not present exact-repeat results as proof of moving-window or fleet reuse.
- Report answer parity/errors, p50/p95, VL request counts/ranges/body bytes and
  peer requests. Give hit/lookup counts and exclusions by Loki/VL semantic
  layer and physical tier; a bypass is not a cache miss or a zero-percent hit
  rate. Keep reference Loki's own caches separate.
- Include proxy CPU with its measurement scope, per-operation allocation,
  retained logical RAM/SSD occupancy, process RSS, encoded disk traffic and
  evictions/rejections. Report VL-server CPU/RSS separately when attributable.
  Distinguish process-counter deltas from per-query CPU, lifetime RSS peaks
  from simultaneous fleet peaks, and file length from physical IOPS/throughput.
- Show the costs as well as gains: cold-admission overhead, memory retained,
  regressions, unchanged cases and measurement uncertainty. Use "not measured"
  or "not applicable" with a reason instead of an invented zero. Report
  delayed priming/background work separately from measured-request work.
- Explain which work is avoided and where reuse still stops. State effects on
  freshness, completeness, tenant/auth isolation, placement, resource bounds
  and invalidation; do not infer new guarantees from a higher hit ratio.

Infrastructure, release metadata and documentation changes may mark this matrix
not applicable with their scope reason; they must not claim runtime savings.

## Fleet scale

Design every change for tens of proxy replicas, a much larger VictoriaLogs
cluster and far more concurrent clients than the test stacks, with limits that
operators may raise or remove. No limit is the safety mechanism.

- Minimise VictoriaLogs calls per client request: reuse cached answers,
  extents and settled steps, coalesce identical in-flight work across clients
  and replicas, push work down so VictoriaLogs returns aggregates.
- The proxy shields VictoriaLogs: bounded fan-out per request and replica,
  bounded heavy-call concurrency, Loki's back-pressure statuses, a separate
  lane for label and label-value calls.
- Parallel VictoriaLogs calls are sized by the request and bounded; memory is
  bounded by construction (chunking, streaming, aggregated input), not by a
  limit that rejects.
- Shareable state lives in the fleet cache so replicas reuse each other's work.
- Pull requests that change VictoriaLogs calls state the calls per request
  (first, repeat, refresh), their growth with replicas and clients, and the
  memory and concurrency bounds. See CONTRIBUTING.md, Performance Rules.

## Security review in every change

Every feature, fix or refactor gets a small security review of what it adds or
modifies, and every pull request records the result in its **Security review**
section (see `.github/pull_request_template.md`). The proxy parses untrusted
query text and headers for many tenants in front of a shared VictoriaLogs
cluster, so check at least:

- Untrusted input: which query text, parameters and headers reach the change,
  and what a crafted value can make it do.
- Resource bounds on crafted input: memory, CPU, regexp compilation, nesting
  and recursion depth, fan-out, cache growth. Fuzz new parsers and decoders.
- Tenant isolation: cache and plan keys carry the tenant scope; no answer is
  reused across tenants or served to a peer for another tenant.
- Query injection: LogsQL is built from typed nodes or properly quoted values,
  never by splicing client text; client text never becomes proxy-internal
  syntax.
- Error bodies: clients see only the Loki error layer, never VictoriaLogs
  messages, URLs or internal details.
- Logs and telemetry: no credentials or tokens, bounded label cardinality.
- Admin and peer routes keep their authentication.
- Concurrency: no data races or goroutine leaks; work stops when its last
  waiter cancels.
- Dependencies: no new module without review; versions and checksums pinned.

Write "no security-relevant surface" with the reason when nothing applies.
Report a vulnerability privately as described in `SECURITY.md`, never in a
public pull request or issue.

## Performance stats in every PR

Every pull request records what it costs in its **Performance** section (see
`.github/pull_request_template.md`): a base-vs-PR table for the code it
touches, measured on the same machine with `benchstat` (at least 6 runs; name
the machine and `GOMAXPROCS`):

- CPU: `ns/op`, or the CPU time of a request path;
- memory: `B/op` and `allocs/op`, and peak RSS when the change can grow it;
- latency: the per-request time, or p50/p95 when a request path changes;
- the change against base, faster or slower, and why.

A refactor, or code no handler calls yet, still measures its own new code and
says why the request path does not change. A change to answers or routes keeps
its timing tables against Loki and its load test on top of this. Commit the
benchmark rather than a throwaway one, and name it in the registry decision.


## Docs, rules and skill in the same pull request

A pull request updates every README and `docs/` page its change affects, in the same pull request: compatibility
coverage (Loki differences fixed or newly known), performance (what got faster and where it is measured), the query
plan's progress, and configuration and observability for new flags and metrics. Generated pages are regenerated;
hand-written pages are edited; public pages state the current behaviour only. A pull request that sets or changes a
working convention adds it here (and to the pull request template when it is a checklist item). List the docs a pull
request touches in its description.

## No performance regression; bring touched components to Loki's speed

A pull request never makes anything slower than its base: proxy CPU, memory,
allocations and latency, and the work it asks of VictoriaLogs (more requests,
or a heavier filter such as a regexp in place of an indexed one). Fixing an
answer is no exception: find the exact form that is also fast.

When a pull request touches a component that is already as slow as Loki or
slower, or its change makes something slower, bringing that component to
Loki's speed or better is part of the same work, not a follow-up. Measure it
(`benchstat`, the cost on VictoriaLogs, timings against Loki for the affected
query shapes) and keep improving it. Only when that is not possible within
the change, show why with numbers and ask the maintainers before it ships.
## Serving a construct from the query plan

A construct moves from the string translator to the typed plan (`internal/queryplan/lower`, written by
`physical.Encode`) by being added to `proxy.QueryPlanFlippedConstructs`, behind `-query-plan-lowering`.

- The plan must write the translator's text byte for byte, not an equal parsed query: the text is the cache key and the
  request VictoriaLogs sees. `TestPlanShadow` and the generated grids assert it for every native row under the 12
  profiles; fix a difference of spelling in the AST builders, never in `internal/translator`.
- Decide eligibility before planning (`logql.MayHaveNonFilterStage` and a first-byte test for log queries): a query the
  plan does not cover pays no plan. `TestFlipScreenAdmitsEveryNativeRow` holds the screen to the native rows.
- No shape may allocate more than its base (benchstat of `BenchmarkTranslateQueryMissShapes`, base against branch); add
  the new construct's shapes to it. Add the construct to the route-parity test
  (`TestQueryPlanLowering_RouteParity`), which runs the same queries with the flag on and off against a fake
  VictoriaLogs.

## Published numbers

Every number the README, `docs/` and the website state is generated and kept current: a generator writes it, a
scheduled run (daily or weekly) re-measures it, and a gate fails when the committed copy is stale. The comparison
with Loki is the model: `conformance/scripts/perf_evidence.py --quality-summary` and `--site-data`, run by
`bench/ab/daily.py`, checked by `scripts/ci/check_conformance.py`.

- A figure that is not generated yet is labelled with its date, commit and method, and never presented as current.
- Show the whole distribution, not only medians: the p90 and the slowest queries, by time range, and the heaviest
  queries users wait for, next to Loki's.
- State the current measured position only. README, docs and marketing pages do not compare with earlier
  versions of the proxy ("regression", "no longer", "improved from") or explain why a past build was slower; that
  history belongs to pull requests and the changelog.

## Production investigations

Cache investigations and operational checks must be reproducible in a deployed
proxy. Reuse existing runtime metrics first; when a required signal is missing,
add bounded metrics or structured diagnostics and document the production check.
Laboratory-only instrumentation must not be presented as production visibility.

- Keep Prometheus and OTLP definitions consistent. Use fixed labels; never put
  queries, selectors, keys, tenants or credentials into metric labels or logs.
- Distinguish client requests, internal tier lookups, exclusions, background
  admission, backend requests and body completion. Separate decoded consumed
  bytes from encoded network traffic and requested spans from scanned rows.
- Explain counter resets, process churn, measurement boundaries and partial
  observations. Pair resource deltas with successful answers and errors; reduced
  transfer due to failures is not an optimization.
- Normal hits/misses and exclusions belong in metrics or debug diagnostics.
  Keep actionable error/timeout events bounded and free of raw sensitive text.
  Test exporter consistency, outcomes, error propagation and cancellation.
- Put concrete metric queries, log fields and interpretation limits in a runbook
  alongside the delivered behavior, including what still cannot be measured.

## Reuse the existing local test stack

Follow the repository skill at
`.agents/skills/reuse-existing-test-stack/SKILL.md` for the operational
checklist.

- Before running browser or end-to-end checks, inspect the already-running
  Grafana, Loki, proxy and VictoriaLogs containers and their URLs. Reuse that
  stack and its existing datasets whenever the test contract allows it.
- Never start a second Compose project or duplicate Grafana/backend/proxy
  containers to make a test convenient. Run test groups sequentially against
  the one existing stack.
- Prefer existing tests, fixtures, helpers and CI evidence. Extend them only
  when a required behavior or edge case is not covered; do not create parallel
  test setups or duplicate coverage.
- Check whether a test mutates state or re-ingests data before reusing the
  stack. If it requires a clean state, reset/recreate only the existing stack
  in place when that is safe and preserves required data. Otherwise use a
  non-mutating test path or report the specific E2E limitation; do not silently
  launch another stack.
- Stop or remove only containers and resources created by the current task.
  Do not use broad Compose cleanup or `down -v` against a stack whose ownership
  or data-retention requirements are uncertain.
- If no compatible stack is already available, finish the local checks that do
  not need it and state what must be run in the existing environment. Ask
  before creating a new stack.
