# Benchmarks

Measured on Apple M3 Max (14 cores), Go 1.26.1, `-benchmem`.

## Per-Request Latency

| Operation | Latency | Allocs | Bytes/op | Notes |
|---|---|---|---|---|
| Labels (cache hit) | 2.0 us | 25 | 6.6 KB | Serve from in-memory cache |
| QueryRange (cache hit) | 118 us | 600 | 142 KB | Query translation + cache lookup |
| wrapAsLokiResponse | 2.8 us | 58 | 2.6 KB | JSON re-envelope |
| VL NDJSON to Loki streams (100 lines) | 170 us | 3118 | 70 KB | Parse + group + convert (pooled) |
| LogQL translation | ~5 us | ~20 | ~2 KB | String manipulation (no AST) |

## Throughput

| Scenario | Requests | Concurrency | Throughput | Cache Hit % | Memory Growth |
|---|---|---|---|---|---|
| Labels (cache hit) | 100,000 | 100 | 175,726 req/s | 98.2% | 0.5 MB |
| QueryRange (cache miss, 1ms backend) | 5,000 | 50 | 12,976 req/s | 0% | - |

## Scaling Profile (No Cache — Raw Proxy Overhead)

| Profile | Requests | Concurrency | Throughput | Avg Latency | Total Alloc | Live Heap | Errors |
|---|---|---|---|---|---|---|---|
| low (100 rps) | 1,000 | 10 | 8,062 req/s | 124 us | 136 MB | 0.9 MB | 0 |
| medium (1K rps) | 5,000 | 50 | 12,465 req/s | 80 us | 572 MB | 1.3 MB | 0 |
| high (10K rps) | 20,000 | 200 | 39,057 req/s | 26 us | 1,331 MB | 8.7 MB | 0 |

Key observations:
- **Live heap stays &lt;10 MB** even at 20K requests — GC keeps up
- **Total alloc is high** (~70 KB/request) due to JSON parse/serialize — this is GC pressure, not leak
- **No errors** at 200 concurrent connections (after connection pool tuning)

## Scaling Profile (With Cache)

| Profile | Requests | Concurrency | Throughput | Avg Latency | Live Heap |
|---|---|---|---|---|---|
| low (100 rps) | 1,000 | 10 | 8,207 req/s | 122 us | 1.1 MB |
| medium (1K rps) | 5,000 | 50 | 12,821 req/s | 78 us | 1.1 MB |

Cache provides marginal throughput improvement but dramatically reduces backend load (98%+ hit rate).

## Resource Usage at Scale

Measured from load tests (proxy overhead only, excludes network I/O):

| Load (req/s) | CPU (single core) | Memory (steady state) | Notes |
|---|---|---|---|
| 100 | &lt;1% | ~10 MB | Idle, mostly cache hits |
| 1,000 | ~8% | ~20 MB | Mix of cache hits/misses |
| 10,000 | ~30% | ~50 MB | Significant cache miss rate, backend-bound |
| 40,000+ | ~100% | ~100 MB | CPU-bound, needs horizontal scaling |

The proxy is CPU-bound at high load. Memory usage is stable — the cache has a fixed maximum size (configurable via `-cache-max`). Scaling strategy:

- **< 1,000 req/s**: Single replica, 100m CPU, 128Mi memory
- **1,000-10,000 req/s**: 2-3 replicas with HPA on CPU
- **> 10,000 req/s**: HPA with 5+ replicas, tune `cache-max` for hit rate

## Connection Pool Tuning

The proxy's HTTP transport is tuned for high-concurrency single-backend proxying:

```go
transport.MaxIdleConns = 256         // total idle connections
transport.MaxIdleConnsPerHost = 256  // all slots for VL (single backend)
transport.MaxConnsPerHost = 0        // unlimited concurrent connections
transport.IdleConnTimeout = 90s     // reuse connections
```

Go's defaults (`MaxIdleConnsPerHost=2`) cause ephemeral port exhaustion at >50 concurrent requests. Our tuning eliminates this — tested clean at 200 concurrency, 33K req/s.

## Known Hot Paths

1. **VL NDJSON to Loki streams** (3118 allocs/100 lines, down from 3417): Optimized with byte scanning (no `strings.Split`), `sync.Pool` for JSON entry maps, pre-allocated slice estimates. **49% memory reduction** from original. Remaining allocs are from `json.Unmarshal` internals — further gains need a custom tokenizer.

2. **QueryRange cache hit** (600 allocs/request): Even on cache hit, response bytes are re-parsed and re-serialized. Serving raw cached bytes would eliminate this overhead.

## Running Benchmarks

```bash
# All proxy benchmarks
go test ./internal/proxy/ -bench . -benchmem -run "^$" -count=3

# Translator benchmarks
go test ./internal/translator/ -bench . -benchmem -run "^$" -count=3

# Cache benchmarks
go test ./internal/cache/ -bench . -benchmem -run "^$" -count=3

# Load tests (requires no -short flag)
go test ./internal/proxy/ -run "TestLoad" -v -timeout=60s

# Profile CPU
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Profile memory
go test ./internal/proxy/ -bench BenchmarkVLLogsToLokiStreams -memprofile=mem.prof
go tool pprof mem.prof
```
