| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |
|---|---|---|---|---|---|
| dd-fields | 1h | 18 | 18/18 | 15/18 identical, 4 explained | yes |
| dd-fields | 6h | 18 | 18/18 | n/a (Loki holds 1.5h) | yes |
| dd-labels | 1h | 16 | 16/16 | 13/16 identical, 3 explained | yes |
| dd-labels | 6h | 16 | 16/16 | n/a (Loki holds 1.5h) | yes |
| dd-landing | 1h | 43 | 43/43 | 40/43 identical, 3 explained | yes |
| dd-landing | 6h | 43 | 43/43 | n/a (Loki holds 1.5h) | yes |
| dd-patterns | 1h | 6 | 6/6 | 1/6 identical, 3 explained, 2 unexplained | yes |
| dd-patterns | 6h | 6 | 6/6 | n/a (Loki holds 1.5h) | yes |
| dd-service-logs | 1h | 7 | 7/7 | 4/7 identical, 3 explained | yes |
| dd-service-logs | 6h | 7 | 7/7 | n/a (Loki holds 1.5h) | yes |
| explore-logs-plain | 1h | 4 | 4/4 | 3/4 identical, 1 explained | yes |
| explore-logs-plain | 6h | 4 | 4/4 | n/a (Loki holds 1.5h) | yes |
| explore-metric-plain | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-metric-plain | 6h | 3 | 3/3 | n/a (Loki holds 1.5h) | yes |
| explore-unpack-sort-keep | 15m | 3 | 2/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sort-keep | 1h | 3 | 2/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-absent | 15m | 3 | 3/3 | 3/3 identical | yes |
| explore-unpack-sum-absent | 1h | 3 | 3/3 | 3/3 identical | yes |
| explore-unpack-sum-by-empty | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-by-empty | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-count | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-count | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-label-replace | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-label-replace | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-rate | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-rate | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-times-two | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-times-two | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |

### dd-fields 1h
- PR vs Loki (explained): query ('sum by (service_name_extracted) (count_over_time({service_name="otel-collector"} | servi: request only on one side (right) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-labels 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-landing 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-28T02%3A01%3A00.000Z&query=&s: body: content differs (rows 17 vs 17) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/volume?end=2026-11-28T02%3A01%3A00.000Z&limit=5000&: body: content differs (rows 0 vs 0) -- explained: volume bytes: VictoriaLogs sums stored line lengths, Loki its chunks' ingested bytes with structured metadata (registry loki_api_v1_index_volume, vl_cannot); the series match

### dd-patterns 1h
- PR vs Loki: resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 7 vs 8)
- PR vs Loki: resource /api/datasources/uid/*/resources/patterns?end=2026-11-28T02%3A01%3A00.000Z&query=%7Bservic: body: content differs (rows 8 vs 11)
- PR vs Loki (explained): query ('{service_name="cache-redis"}     | json  | logfmt | drop __error__, __error_details__  ': ('logsPanelQuery', '', '{}'): content differs (rows 1000 vs 1000) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision)
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 10 vs 10) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### dd-service-logs 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_fields?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 13 vs 15) -- explained: Loki's _extracted suffix for a key named like a stream label, where the build does not return it (detected_fields: open, semantics/detected-fields-extracted-suffix; service/service.name: owner decision); the proxy also returns data, status; Loki lists fields in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/detected_labels?end=2026-11-28T02%3A01%3A00.000Z&query=%7: body: content differs (rows 9 vs 9) -- explained: sampled cardinality: Loki counts every stream its ingesters hold, not the requested window (registry drilldown/detected-labels-sampled-cardinality), the proxy the window's last 5 minutes (metadataMaxFieldNamesWindow); the label names match; the proxy also returns data, limit, status; Loki lists labels in no fixed order
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/drilldown-limits: body: content differs (rows 0 vs 0) -- explained: configuration probe: each backend publishes its own limits, version and settings (the proxy's pattern persistence)

### explore-logs-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 13493, chunks 13493, entries 288314, bytes 28831400 vs streams 13493, chunks 13493, entries 288314, bytes 61828096

### explore-metric-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 13493, chunks 13493, entries 288314, bytes 28831400 vs streams 13493, chunks 13493, entries 288314, bytes 61828096

### explore-unpack-sort-keep 15m
- main vs PR: query ('sort(sum(rate({app="visual-unpack"} | unpack | keep agent [$__auto])))', 'range', '17958: series sets differ: 1 only left ["('status', '400')"], 1 only right ["('A', '', '{}')"]
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sort-keep 1h
- main vs PR: query ('sort(sum(rate({app="visual-unpack"} | unpack | keep agent [$__auto])))', 'range', '17958: series sets differ: 1 only left ["('status', '400')"], 1 only right ["('A', '', '{}')"]
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-by-empty 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-by-empty 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-count 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-count 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-label-replace 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-label-replace 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-rate 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-rate 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-times-two 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-times-two 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776
