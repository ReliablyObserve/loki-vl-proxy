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
