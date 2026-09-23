# Conformance

Everything that defines and proves Loki compatibility lives here.

```
conformance/
  registry/   the inventory, one file per item:
              loki/endpoints/   Loki HTTP endpoints
              loki/errors/      failure classes: status, errorType, message, precedence
              loki/logql/       the LogQL surface: every operator, range function,
                                parser, filter and conversion Loki defines
              behaviours/       semantics, severity, identity and data-quality behaviour
                                (detected_level derivation, service_name discovery,
                                window bounds, parse errors, chart density, probes)
              vl/capabilities/  version-gated VictoriaLogs capabilities
              vl/logsql/        every VictoriaLogs pipe and stats function, the release it
                                appeared in, and whether the proxy emits it
              translations/     per feature, end to end: the Loki layer, the VictoriaLogs
                                layer, every difference between them, and the layer the
                                proxy inserts to close each one, with cost and proof
              cases/            edge cases per consumer, with real requests
              state/            the state of each item, owner decisions, waivers
  scripts/    generators and checks that keep the registry true to the code
  reports/    generated output: the coverage map, the translation map and the open gaps
```

- **Registry** — see [registry/README.md](registry/README.md) for the file layout and the fields
  each item carries. Curated prose and owner state survive regeneration; `registry/generated/`
  is extraction output and is never edited by hand.
- **Reports** — [reports/coverage-map.md](reports/coverage-map.md) shows every Loki endpoint with
  its state, whether VictoriaLogs answers it natively or the proxy computes it, which clients
  depend on it, and what evidence exists. [reports/translation-map.md](reports/translation-map.md) shows, per feature, what Loki gives,
  what VictoriaLogs gives, and what the proxy adds between them.
  [reports/gaps.md](reports/gaps.md) ranks what is missing.
  [reports/performance.md](reports/performance.md) joins the saved A/B runs in `bench/ab/results/`
  with the registry items each measured shape covers, and lists every item the proxy answers
  slower than Loki on first load.
- **Gate** — `python3 scripts/ci/check_conformance.py` fails when a test claims an unknown registry
  id, a proxy route has no registry item, an item points at code that no longer exists, or the
  coverage map is stale.

## What the gate enforces

`scripts/ci/check_conformance.py` runs in CI on every pull request and fails when:

- a test claims a registry id that does not exist, a proxy route has no registry
  item, or an item points at code that no longer exists;
- a generated report is stale (performance, coverage map, gaps, translation map,
  roadmap, compatibility matrix);
- a `bench/ab/shapes.json` shape covers a registry id that does not exist, covers
  nothing, or a saved A/B result does not match the shape sets;
- a flag is missing from the places that list every flag;
- a score or an item's state regressed against `registry/baseline.json`, or a
  waiver expired;
- an item's state claims more than the evidence supports: `proven` without a
  passing test that declares it.

Scores may rise, never fall. `python3 conformance/scripts/ratchet.py --accept`
records an improvement as the new baseline.

## Proving a case against the real stack

`live_proof.py` sends each case to Loki and to the proxy on the same data and
compares them under the case's contract:

```bash
python3 conformance/scripts/live_proof.py \
  --loki http://127.0.0.1:13101 --proxy http://127.0.0.1:13100
```

Verdicts are `holds`, `gap` (the answers differ) or `blocked` (one side errored,
or Loki returned nothing, so nothing is proven). Results land in
`registry/generated/live-evidence.json`, and the matrix report shows them beside
the versions under test.

## Reporting what a change touches

Before opening a pull request that changes client-visible behaviour, run:

```bash
python3 conformance/scripts/pr_report.py           # against origin/main
```

It prints a table of the registry items behind the files you changed: their
state, whether VictoriaLogs serves them natively, and how many tests declare
them. Paste it into the PR description so a reviewer sees which compatibility
contracts the change stands on, and what is still unproven.

## Wiring tests

Every test declares what it proves, so coverage is measured rather than assumed:

```go
// conformance: loki_api_v1_index_volume, drilldown/index_volume/single-sample-returns-vector
func TestVolume_RangeWithOneSamplePerSeriesIsVector(t *testing.T) {
```

```ts
test('landing volume renders services @cov:loki_api_v1_index_volume', async ({ page }) => {
```

## Refresh

```bash
python3 conformance/scripts/sync_loki.py --version v3.7.7
python3 conformance/scripts/sync_proxy.py --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json
python3 conformance/scripts/sync_impl.py --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/sync_errors.py
python3 conformance/scripts/sync_vl.py
python3 conformance/scripts/seed_registry.py \
  --endpoints conformance/registry/generated/loki/v3.7.7/endpoints.json \
  --coverage conformance/registry/generated/proxy/coverage.json
python3 conformance/scripts/coverage_map.py
python3 conformance/scripts/wire.py
python3 conformance/scripts/translation_map.py
python3 conformance/scripts/gaps.py
```
