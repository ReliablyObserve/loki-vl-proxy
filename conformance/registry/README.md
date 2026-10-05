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
