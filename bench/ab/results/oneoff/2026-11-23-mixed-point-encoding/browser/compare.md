| page | range | backend requests | main = PR (identical) | PR vs Loki (identical) | settled |
|---|---|---|---|---|---|
| explore-mixed-general-count-control | 24h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-general-count-control | 6h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-explicit-count | 1h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-explicit-count | 24h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-explicit-count | 6h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-reverse-count | 1h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-reverse-count | 24h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |
| explore-mixed-parser-reverse-count | 6h | 3 | 3/3 | 1/3 identical, 2 unexplained | yes |

### explore-mixed-general-count-control 24h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795292340000000000&end=1795378740000000000: body: content differs (rows 20 vs 30)
- PR vs Loki index/stats: streams 605, chunks 605, entries 14662, bytes 1466200 vs streams 735, chunks 1120, entries 14555, bytes 11942912

### explore-mixed-general-count-control 6h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795357140000000000&end=1795378740000000000: body: content differs (rows 20 vs 28)
- PR vs Loki index/stats: streams 136, chunks 136, entries 7720, bytes 772000 vs streams 597, chunks 831, entries 3190, bytes 2617979

### explore-mixed-parser-explicit-count 1h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795375140000000000&end=1795378740000000000: body: content differs (rows 20 vs 28)
- PR vs Loki index/stats: streams 84, chunks 84, entries 1179, bytes 117900 vs streams 82, chunks 91, entries 1209, bytes 984064

### explore-mixed-parser-explicit-count 24h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795292340000000000&end=1795378740000000000: body: content differs (rows 20 vs 30)
- PR vs Loki index/stats: streams 605, chunks 605, entries 14662, bytes 1466200 vs streams 735, chunks 1120, entries 14555, bytes 11942912

### explore-mixed-parser-explicit-count 6h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795357140000000000&end=1795378740000000000: body: content differs (rows 20 vs 28)
- PR vs Loki index/stats: streams 136, chunks 136, entries 7720, bytes 772000 vs streams 214, chunks 443, entries 7816, bytes 6394880

### explore-mixed-parser-reverse-count 1h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795375140000000000&end=1795378740000000000: body: content differs (rows 20 vs 28)
- PR vs Loki index/stats: streams 84, chunks 84, entries 1179, bytes 117900 vs streams 597, chunks 831, entries 531, bytes 436329

### explore-mixed-parser-reverse-count 24h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795292340000000000&end=1795378740000000000: body: content differs (rows 20 vs 30)
- PR vs Loki index/stats: streams 605, chunks 605, entries 14662, bytes 1466200 vs streams 735, chunks 1120, entries 14555, bytes 11942912

### explore-mixed-parser-reverse-count 6h
- PR vs Loki: resource /api/datasources/uid/*/resources/index/stats?query=%7Bapp%3D~%22api-gateway-(storefront%7C: body: content differs (rows 0 vs 0)
- PR vs Loki: resource /api/datasources/uid/*/resources/labels?start=1795357140000000000&end=1795378740000000000: body: content differs (rows 20 vs 28)
- PR vs Loki index/stats: streams 136, chunks 136, entries 7720, bytes 772000 vs streams 597, chunks 831, entries 3190, bytes 2617979
