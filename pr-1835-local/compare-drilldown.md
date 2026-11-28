| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |
|---|---|---|---|---|---|
| dd-fields | 15m | 18 | 18/18 | 15/18 identical, 4 explained | yes |
| dd-fields | 1h | 18 | 18/18 | 15/18 identical, 4 explained | yes |
| dd-landing | 15m | 43 | 43/43 | 40/43 identical, 3 explained | yes |
| dd-landing | 1h | 43 | 43/43 | 40/43 identical, 3 explained | yes |
| dd-service-logs | 15m | 7 | 7/7 | 4/7 identical, 3 explained | yes |
| dd-service-logs | 1h | 7 | 7/7 | 4/7 identical, 3 explained | yes |
| explore-unpack-sum-count | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-count | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-rate | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-rate | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |

### dd-fields 15m
- PR vs Loki (explained): query ('sum by (service_name_extracted) (count_over_time({service_name="otel-collector"} | servi: request only on one side (right) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-fields 1h
- PR vs Loki (explained): query ('sum by (service_name_extracted) (count_over_time({service_name="otel-collector"} | servi: request only on one side (right) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-landing 15m
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=&s: body: content differs (rows 17 vs 17) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/volume?end=2026-11-27T22%3A51%3A00.000Z&limit=5000&: body: content differs (rows 0 vs 0) -- explained: volume bytes: VictoriaLogs sums stored line lengths, Loki its chunks' ingested bytes with structured metadata (registry loki_api_v1_index_volume, vl_cannot); the series match

### dd-landing 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=&s: body: content differs (rows 17 vs 17) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/volume?end=2026-11-27T22%3A51%3A00.000Z&limit=5000&: body: content differs (rows 0 vs 0) -- explained: volume bytes: VictoriaLogs sums stored line lengths, Loki its chunks' ingested bytes with structured metadata (registry loki_api_v1_index_volume, vl_cannot); the series match

### dd-service-logs 15m
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-service-logs 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-27T22%3A51%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### explore-unpack-sum-count 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-count 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-rate 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-rate 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776
