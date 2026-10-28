# Conformance registry

The registry is the inventory of everything the proxy must be compatible with, one small
file per item, so it is easy to read, extend and review:

```
conformance/registry/
  loki/endpoints/<id>.yaml       every Loki HTTP endpoint: what it returns, who calls it,
                                 real examples, how the proxy implements it, its edge cases
  loki/errors/<id>.yaml          every Loki failure class: trigger, status, errorType, message,
                                 precedence (added in the next phase)
  loki/payloads/<id>.yaml        response shapes, stats counters, headers, empty forms
  vl/capabilities/<id>.yaml      VictoriaLogs endpoints, pipes, stats functions and version gates,
                                 with how the proxy uses each one
  cases/<track>/<endpoint>/<id>.yaml   edge cases and consumer-specific combinations, each with a
                                 real request and the comparison contract
  state/<track>/<id>.yaml        the state of each item: proven, partial, gap, waived,
                                 not-applicable, with the reason and any owner decision
  generated/                     raw extraction output, refreshed by the sync scripts, never edited
```

Each endpoint file carries a `generated:` block (refreshed automatically) and curated prose
(kept across regeneration): what the endpoint does, which consumers call it and how their
parameter combinations differ (Grafana Explore and Logs Drilldown send different shapes),
real example requests, what VictoriaLogs answers natively, what the proxy must compute
itself and why VictoriaLogs cannot, and the edge cases that prove it.

## Write methods are out of scope

The proxy serves Loki's read API. A state file lists the write methods of an endpoint that
are left out of scope, with the owner decision quoted; every generator judges the endpoint on
its remaining (read) methods and the gate rejects a write method that is neither listed nor
allowed:

```yaml
owner_decision: |
  "do not list write APIs as gap; we will only support delete maybe, not any other." (owner 2026-10-27)
out_of_scope_methods: [DELETE, POST]
```

An endpoint whose every method is listed is `not-applicable` (the push routes) and is left out
of every total, ratio, gap list and ranking. `POST` on a query, label or series endpoint is a
form-encoded read and is never listed. The delete endpoint stays in scope.

## Refreshing the generated data

```bash
python3 conformance/scripts/sync_loki.py --version v3.7.7   # Loki API surface, from the Loki source
python3 conformance/scripts/sync_proxy.py --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json
python3 conformance/scripts/sync_impl.py --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/sync_vl.py                      # VictoriaLogs surface the proxy uses
python3 conformance/scripts/seed_registry.py \
  --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json \
  --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/report.py                       # state and coverage summary
```

`execution` in the generated block is a static approximation: `native_vl` (VictoriaLogs answers
it), `hybrid` (the proxy reshapes or merges a VictoriaLogs answer) or `proxy_side` (the proxy
computes the result). The differential runner confirms it per query shape at runtime in a later
phase; a curated value in the file wins over the generated one.

## Recording a difference from Loki

A case that records a known difference carries a `gap:` block, which
`conformance/scripts/parity_gaps.py` ranks into `conformance/reports/parity-gaps.md`:

```yaml
gap:
  status: open          # open | owner-kept | documented | upstream | fixed
  impact: explore-visible   # explore-visible | drilldown-visible | grafana-datasource | api-only | edge
  effort: S             # S | M | L
  area: 'internal/... (where the fix belongs)'
  loki: 'pkg/logql/...:line (Loki v3.7.7 reference)'
  proxy: 'what the proxy does instead'
  planned_test: 'the test that will prove the fix and stay as its regression guard'
  fixed_by: '#700'      # required with status fixed: the PR or commit that removed the difference
  note: 'optional: when and on which code the difference was measured'
  clusters:             # differential-run clusters (bench/parity) this difference accounts for
    - 0a9f54e0
```

Every cluster in `generated/parity-discovery.json` is claimed by exactly one case: a
cluster the diff marked as a recorded deviation by the case id it names (which must be a
case with status documented, owner-kept, upstream or fixed), every other one by a case's
`gap.clusters`. A fixed case keeps its clusters until the next recorded run no longer
shows them. The conformance gate fails otherwise.

## Performance acceptance

The owner's rule (2026-10-28): at most 2× Loki in the worst case, close to Loki in general, below
Loki for most queries; up to 2× only where going lower is high effort now. Every item an A/B
shape covers (`bench/ab/shapes.json` `covers`) carries the bound in its own file (not its state
file), in this fixed form:

```yaml
perf:
  max_ratio: 2.0          # the hard bound (at most 2.0): a locked row above it is a regression
  target_ratio: 1.0       # below Loki; a row between 1× and max_ratio needs an effort_exception
  modes: [cold, warm, refresh]
  locked: true            # every row of its shapes measured within max_ratio on the daily run
  locked_since: '2026-10-27'
  effort_exception: null
```

An exception is two indented one-line keys, `reason` (why going below Loki is high effort now,
quoting the evidence) and `follow_up` (what would bring it below Loki). Locks were seeded from the
daily results of 2026-10-27 (`python3 bench/ab/over2x.py locks seed`) and are added by people
(`over2x.py locks lock <id>`, which refuses an item the latest daily results show above its bound);
the daily run lists the items ready to lock and never edits the registry. A locked item's rows
above their bound fail the daily run and are listed in `perf-smoke` (bench/ab/README.md,
"Acceptance tiers and the registry lock"). `scripts/ci/check_conformance.py` fails a malformed
block, a `max_ratio` above 2.0, a covered item without a block, a block on an item nothing covers,
and a locked item that covers a row on `bench/ab/history/over-2x-loki.json`.
`conformance/reports/performance.md` shows each item's lock state and the rows between 1× and 2×.

## Errors: one Loki-shaped layer

A client never sees VictoriaLogs: no VictoriaLogs status, error body, LogsQL fragment,
backend host:port or Go transport and decoder error. Every route answers a failure through
the proxy's single error layer (`writeError` and the status mapping beside it in
`internal/proxy/http_utils.go`) with Loki's status, `errorType` and text: a failed backend
call is 500, a client cancellation 499 and a deadline 504 with Loki's fixed texts
(`pkg/util/server/error.go`), a full queue 429 "too many outstanding requests", a bad query
400 with Loki's wording. A proxy-only status (502, 503, 422, 408) is a leak unless it is a
documented proxy limit listed here whose message names its flag. `loki/errors/status-<code>.yaml`
records when Loki answers each status and the cases that assert it.

`TestErrorLayer_NoVictoriaLogsLeak` (internal/proxy) is the gate: it drives every
client route that reaches VictoriaLogs or the ruler through every backend failure mode
(VictoriaLogs 4xx/5xx and 429, a front proxy's 502/503, refused and reset connections,
timeouts, cancellations, truncated, malformed and oversized bodies) with a fake backend
and asserts Loki's answer. `TestCompat_ErrorLayerLikeLoki` (test/e2e-compat, group core)
repeats the main modes against the stack's VictoriaLogs and compares with Loki.

- A new route, or a new way a route calls the backend, is added to `errLayerRoutes` in the
  gate and answers its failures through the layer; a new failure mode is added to
  `errLayerModes`.
- The cells that still leak are listed in `error-layer-known-leaks.json`, each with the
  `cases/semantics/error-layer-*` cases it fails. The gate skips exactly those (naming the
  case ids) and fails on any other leak, and on a listed leak that is fixed, so the list
  can only shrink. After a fix, regenerate it with
  `ERROR_LAYER_RECORD=conformance/registry/error-layer-known-leaks.json go test -run TestErrorLayer_NoVictoriaLogsLeak ./internal/proxy/`,
  check that only removals show in the diff, and mark a case `fixed` once no cell lists it.
- `scripts/ci/check_conformance.py` holds the list to its `ceiling` (the number of listed
  cell-case pairs), fails when it grows against `origin/main` where the checkout has it,
  and, given the unit results, fails when the gate did not run, failed, or skipped more
  cells than are listed.
