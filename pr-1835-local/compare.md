| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |
|---|---|---|---|---|---|
| explore-logs-plain | 15m | 4 | 4/4 | 3/4 identical, 1 unexplained | yes |
| explore-logs-plain | 1h | 4 | 4/4 | 3/4 identical, 1 explained | yes |
| explore-metric-plain | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-metric-plain | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-topk-by-per-step | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-topk-by-per-step | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-absent | 15m | 3 | 3/3 | 3/3 identical | yes |
| explore-unpack-sum-absent | 1h | 3 | 3/3 | 3/3 identical | yes |
| explore-unpack-sum-count | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-count | 1h | 3 | 3/3 | 2/3 identical, 1 explained | yes |
| explore-unpack-sum-rate | 15m | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |
| explore-unpack-sum-rate | 1h | 3 | 3/3 | 2/3 identical, 1 unexplained | yes |

### explore-logs-plain 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 3405, chunks 3405, entries 74564, bytes 7456400 vs streams 3405, chunks 3405, entries 74563, bytes 15997952

### explore-logs-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 13532, chunks 13532, entries 289030, bytes 28903000 vs streams 13532, chunks 13532, entries 289030, bytes 61921280

### explore-metric-plain 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 3405, chunks 3405, entries 74564, bytes 7456400 vs streams 3405, chunks 3405, entries 74563, bytes 15997952

### explore-metric-plain 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 13532, chunks 13532, entries 289030, bytes 28903000 vs streams 13532, chunks 13532, entries 289030, bytes 61921280

### explore-topk-by-per-step 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 3405, chunks 3405, entries 74564, bytes 7456400 vs streams 3405, chunks 3405, entries 74563, bytes 15997952

### explore-topk-by-per-step 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Benv%3D%22production%22%7D&start=1795: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 13532, chunks 13532, entries 289030, bytes 28903000 vs streams 13532, chunks 13532, entries 289030, bytes 61921280

### explore-unpack-sum-count 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 1, entries 28, bytes 17408

### explore-unpack-sum-count 1h
- PR vs Loki (explained): resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0) -- explained: bytes and chunks are Loki's chunk accounting, which VictoriaLogs has no equivalent of; streams and entries match
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 1, entries 119, bytes 75776

### explore-unpack-sum-rate 15m
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 29, bytes 2900 vs streams 1, chunks 2, entries 56, bytes 34816

### explore-unpack-sum-rate 1h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D%22visual-unpack%22%7D&start=1: body: content differs (rows 0 vs 0)
- PR vs Loki index/stats: streams 1, chunks 1, entries 119, bytes 11900 vs streams 1, chunks 2, entries 238, bytes 151552
