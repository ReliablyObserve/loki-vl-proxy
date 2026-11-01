# Repository working rules

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
