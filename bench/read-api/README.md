# Read API alias and POST cache proof

This proof compares a base proxy, the candidate proxy and Loki against one
dual-written fixture. It checks a real filtered labels response, GET/POST form
equivalence, direct singular-alias responses, isolation from a second empty
selector and proxy upstream select counters.
It does not measure general query performance or fleet behavior.

`compose.json` is an isolated four-service subset of the existing compatibility
stack. Loki 3.7.9 and VictoriaLogs 1.52.0 have separate named volumes from the
playground. Each proxy has its own disk cache; peers are disabled. Primary L1 is
256 MiB with 10,000 entries, the additional GET compatibility cache is 10%, and
the logical disk cache budget is 512 MiB per proxy. `GOMEMLIMIT=2GiB` is a Go
runtime target, not a cgroup limit or measured RSS. Loki has a 12 GiB container
limit and VictoriaLogs an 8 GiB limit with a 3 GiB internal memory allowance.

## Reproduce

Run from the repository root with Docker, Go and Python 3 available. Use the
base/candidate revisions in the retained result. Build the base image from a
clean worktree at the base revision, then build the candidate from its clean
worktree. The commands below assume the current checkout is the candidate.

```sh
git worktree add --detach /tmp/read-api-base <base-sha>
docker build --build-arg REVISION=<base-sha> -t loki-vl-proxy:read-api-base /tmp/read-api-base
docker build --build-arg REVISION=<candidate-sha> -t loki-vl-proxy:read-api-local .
docker compose -p read-api-proof -f bench/read-api/compose.json up -d --no-build
```

Wait until Loki `/ready` on port 34101 and both proxy `/ready` endpoints on
34100/34102 return 200. On a **new proof stack**, ingest once:

```sh
LOKI_URL=http://127.0.0.1:34101 PROXY_URL=http://127.0.0.1:34100 VL_URL=http://127.0.0.1:34428 \
  go test -tags=e2e ./test/e2e-compat -run '^TestSetup_IngestLogs$' -count=1 -v
```

Do not ingest the fixture repeatedly into retained volumes: the backends have
different duplicate-ingestion behavior. Then run the CI-wired core regression
and the read-only comparison:

```sh
LOKI_URL=http://127.0.0.1:34101 PROXY_URL=http://127.0.0.1:34100 VL_URL=http://127.0.0.1:34428 \
  go test -tags=e2e ./test/e2e-compat -run '^TestCompat_LabelsSingularAlias$' -count=1 -v
python3 bench/read-api/labels_alias_proof.py \
  --base-sha <base-sha> --pr-sha <candidate-sha> --out /tmp/read-api-result.json
```

The harness uses one fixed one-hour window and `{app="e2e-test"}`. Canonical GET
precedes its alias request; POST follows GET. The stack can already be warm from
the endpoint test, so this is not a controlled cold-cache measurement. Responses must be
nonempty and the candidate must match Loki. The retained counter rows, rather
than the script exit status alone, establish whether alias reuse avoided a
backend request. Absent counters are `null`, not zero. Counter resets abort the
proof. Do not run unrelated traffic against these proof proxies during capture.
The next POST uses a missing-app selector before its GET control. It must not
reuse the preceding app's labels. Loki omits `data` for this empty response;
the proxy returns `data: []`. Both mean no labels; the raw shapes are retained.

## Retained comparison

[Measured JSON](results/2026-10-31-labels-alias.json) records base `28a5a2f`
and candidate `c320190`, the exact committed runtime sources used for the images.
Subsequent proof/test-only commits do not change those measured identities.

| Signal | Main runtime | Candidate | Loki reference |
|---|---|---|---|
| Singular alias, GET and POST | 307 redirect | 200, same filtered labels as plural | 200 |
| Filtered canonical POST immediately after GET, VL selects | 4 | 0 | Not collected |
| Missing-app POST after nonempty app POST | Wrong previous labels | Empty, matches its GET control | Empty |
| Missing-app POST, VL selects | 0, wrong cache reuse | 3, required new answer | Not collected |
| Alias requests after canonical priming, VL selects | 0, redirect only | 0, correct labels | Not collected |

These are one sequence's process-counter deltas. They include the cost of
rejecting unsafe reuse: the candidate reads VL for the genuinely new selector.
CPU, RSS, physical disk traffic, per-tier occupancy/evictions, p50/p95 and fleet
scaling are not measured. Budgets and node count are unchanged; no capacity or
general performance improvement is claimed.

## Interpretation and limits

- Primary labels metadata entries are shared by the two routes and equivalent
  GET/POST forms. The optional outer GET compatibility cache is path-specific.
- Unit tests cover differing selectors/windows, body precedence, preserved raw
  forms, malformed partial forms, tenant/auth isolation and migration away from
  ambiguous old namespaces. HTTP migration tests seed old RAM, SSD and outer
  compatibility entries. The same versioned keys address peers and stale reads;
  this proof does not run a live peer-loss or mixed-version fleet experiment.
- Namespace fencing causes initial misses after an upgrade. Old records remain
  subject to existing expiry and eviction; this change does not implement a
  deletion-generation protocol or change configured cache capacity.
- Upstream select counters are logical proxy-to-VL requests. They are not SSD
  reads, IOPS, CPU, RSS, backend-server resource use or per-tier hit ratios.
- Shifted windows, dashboard refresh, disk restart, pressure, peer loss, p50/p95,
  throughput, allocation cost and process resource deltas are not measured here.
  Existing admission, completeness and freshness rules continue to apply.

This is a manually reproducible retained proof. It adds no CI measurement job;
the Go endpoint regression runs in the existing `TestCompat_.*` core shard.
