| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |
|---|---|---|---|---|---|
| dd-fields | 1h | 18 | 18/18 | 14/18 identical, 4 explained, 1 history-dependent | yes |
| dd-landing | 1h | 35 | 35/35 | 32/35 identical, 3 explained | yes |
| dd-service-logs | 1h | 7 | 7/7 | 3/7 identical, 3 explained, 1 history-dependent | yes |
| explore-logs-plain | 1h | 4 | 4/4 | 3/4 identical, 1 explained | yes |
| explore-metric-plain | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-sum-by-over-binary | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-topk-by-per-step | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unwrap-sum | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| lvl-explore-rate-errors-by-service | 1h | 3 | 3/3 | 1/3 identical, 1 explained, 1 unexplained | yes |

### dd-fields 1h
- PR vs Loki (explained): query ('sum by (service_name_extracted) (count_over_time({service_name="otel-collector"} | servi: request only on one side (right) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-10-30T20%3A08%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-10-30T20%3A08%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (pkg/ingester/instance.go LabelsWithValues), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (history-dependent, not counted): resource /api/datasources/uid/*/resources/patterns?end=2026-10-30T20%3A08%3A00.000Z&query=%7Bservic: body: content differs (rows 4 vs 0)

### dd-landing 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-10-30T20%3A08%3A00.000Z&query=&s: body: content differs (rows 15 vs 15) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (pkg/ingester/instance.go LabelsWithValues), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/volume?end=2026-10-30T20%3A08%3A00.000Z&limit=5000&: body: content differs (rows 0 vs 0) -- explained: volume bytes: VictoriaLogs sums stored line lengths, Loki its chunks' ingested bytes with structured metadata (registry loki_api_v1_index_volume, vl_cannot); the series match

### dd-service-logs 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-10-30T20%3A08%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-10-30T20%3A08%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (pkg/ingester/instance.go LabelsWithValues), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (history-dependent, not counted): resource /api/datasources/uid/*/resources/patterns?end=2026-10-30T20%3A08%3A00.000Z&query=%7Bservic: body: content differs (rows 4 vs 0)

### explore-logs-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1793: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match

### explore-metric-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1793: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match

### explore-sum-by-over-binary 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bservice_name%3D%22api-gateway%22%7D&: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match

### explore-topk-by-per-step 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1793: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match

### explore-unwrap-sum 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bservice_name%3D%22api-gateway%22%7D&: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match

### lvl-explore-rate-errors-by-service 1h
- PR vs Loki: query ('sum by (service_name) (rate({env="production"} | detected_level="error" [$__auto]))', 'r: series sets differ: 1 only left ['(\'A\', \'\', \'{"service_name": "worker-service"}\')'], 0 only right []
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1793: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
