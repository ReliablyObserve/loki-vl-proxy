# loki-vl-proxy Read Performance Benchmark

Generated: 2026-05-26T22:05:18+02:00

## compute — 10 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 625 req/s | 629 req/s | — | — | — | 3947 req/s | 1.01x | — |
| P50 | 2ms | 2ms | — | — | — | 1ms | 1.22x | — |
| P90 | 71ms | 17ms | — | — | — | 6ms | 0.25x | — |
| P99 | 164ms | 289ms | — | — | — | 15ms | 1.76x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 249.520 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 208 MB | — | — |

## compute — 50 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 343 req/s | 539 req/s | — | — | — | 4338 req/s | 1.57x | — |
| P50 | 5ms | 38ms | — | — | — | 7ms | 6.39x | — |
| P90 | 27ms | 145ms | — | — | — | 25ms | 5.39x | — |
| P99 | 3286ms | 1194ms | — | — | — | 65ms | 0.36x | — |
| Error Rate | 90.55% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 302.450 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 444 MB | — | — |

## compute — 100 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 410 req/s | 468 req/s | — | — | — | 4473 req/s | 1.14x | — |
| P50 | 10ms | 144ms | — | — | — | 13ms | 14.01x | — |
| P90 | 26ms | 388ms | — | — | — | 51ms | 14.77x | — |
| P99 | 7287ms | 1552ms | — | — | — | 120ms | 0.21x | — |
| Error Rate | 97.49% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 287.930 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 261 MB | — | — |

## heavy — 10 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 146 req/s | 483 req/s | — | — | — | 2532 req/s | 3.30x | — |
| P50 | 13ms | 2ms | — | — | — | 1ms | 0.20x | — |
| P90 | 208ms | 19ms | — | — | — | 7ms | 0.09x | — |
| P99 | 681ms | 359ms | — | — | — | 34ms | 0.53x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 271.000 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 219 MB | — | — |

## heavy — 50 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 247 req/s | 582 req/s | — | — | — | 2000 req/s | 2.35x | — |
| P50 | 21ms | 22ms | — | — | — | 13ms | 1.04x | — |
| P90 | 628ms | 99ms | — | — | — | 46ms | 0.16x | — |
| P99 | 1280ms | 1193ms | — | — | — | 150ms | 0.93x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 277.570 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 528 MB | — | — |

## heavy — 100 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 279 req/s | 568 req/s | — | — | — | 1952 req/s | 2.04x | — |
| P50 | 31ms | 96ms | — | — | — | 30ms | 3.07x | — |
| P90 | 1151ms | 297ms | — | — | — | 89ms | 0.26x | — |
| P99 | 2381ms | 1429ms | — | — | — | 319ms | 0.60x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 261.440 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 637 MB | — | — |

## long_range — 10 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 239 req/s | 92 req/s | — | — | — | 443 req/s | 0.38x | — |
| P50 | 7ms | 1ms | — | — | — | 979µs | 0.26x | — |
| P90 | 155ms | 399ms | — | — | — | 16ms | 2.56x | — |
| P99 | 397ms | 1298ms | — | — | — | 726ms | 3.27x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 238.140 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 255 MB | — | — |

## long_range — 50 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 321 req/s | 84 req/s | — | — | — | 589 req/s | 0.26x | — |
| P50 | 14ms | 243ms | — | — | — | 35ms | 16.45x | — |
| P90 | 518ms | 2056ms | — | — | — | 70ms | 3.96x | — |
| P99 | 1362ms | 3721ms | — | — | — | 2076ms | 2.73x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 294.420 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 499 MB | — | — |

## long_range — 100 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 327 req/s | 111 req/s | — | — | — | 677 req/s | 0.34x | — |
| P50 | 15ms | 622ms | — | — | — | 91ms | 39.59x | — |
| P90 | 933ms | 2147ms | — | — | — | 166ms | 2.30x | — |
| P99 | 2768ms | 4978ms | — | — | — | 2168ms | 1.80x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 344.410 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 617 MB | — | — |

## small — 10 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 919 req/s | 2785 req/s | — | — | — | 3756 req/s | 3.03x | — |
| P50 | 5ms | 1ms | — | — | — | 1ms | 0.22x | — |
| P90 | 27ms | 6ms | — | — | — | 5ms | 0.22x | — |
| P99 | 77ms | 55ms | — | — | — | 26ms | 0.72x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 209.720 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 161 MB | — | — |

## small — 50 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 726 req/s | 2099 req/s | — | — | — | 3793 req/s | 2.89x | — |
| P50 | 57ms | 5ms | — | — | — | 6ms | 0.09x | — |
| P90 | 131ms | 30ms | — | — | — | 28ms | 0.23x | — |
| P99 | 286ms | 491ms | — | — | — | 106ms | 1.72x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 260.300 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 261 MB | — | — |

## small — 100 clients

| Metric | Loki (direct) | VL+Proxy (warm) | VL+Proxy (cold) | VL+Proxy (coalescer) | VL+Proxy (partial) | VL (native) | Δ warm vs Loki | Δ cold vs Loki |
|--------|:-------------:|:---------------:|:---------------:|:--------------------:|:------------------:|:-----------:|:--------------:|:--------------:|
| Throughput | 578 req/s | 1822 req/s | — | — | — | 3538 req/s | 3.15x | — |
| P50 | 156ms | 22ms | — | — | — | 13ms | 0.14x | — |
| P90 | 308ms | 94ms | — | — | — | 55ms | 0.31x | — |
| P99 | 602ms | 811ms | — | — | — | 300ms | 1.35x | — |
| Error Rate | 0.00% | 0.00% | — | — | — | 0.00% | — | — |
| CPU consumed | 0.000 s | 0.000 s | — | — | — | 286.380 s | — | — |
| RSS Memory | 0 MB | 0 MB | — | — | — | 416 MB | — | — |

