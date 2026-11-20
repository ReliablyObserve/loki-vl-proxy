# Fields discovery and downstream memory

Measured 2026-11-20 on one running Compose project with Grafana 13.3.1,
Loki 3.7.10 and VictoriaLogs 1.54.0. The base was main at `59d95f036`.
The proxy and Loki used a one-million-series limit for the untruncated tests.

## Three-hour field discovery

Five settled windows use the exact selector and pipeline captured from the
Grafana Fields tab. The proxy builds run against the same VictoriaLogs
data. The first request uses each fixed window, the repeat is identical
and the refresh moves both bounds by five minutes. Times are medians from
the five requests per mode. VictoriaLogs calls and decoded upstream bytes
are proxy counter deltas summed across those requests.

| Signal | Main | Candidate | Direct Loki |
|---|---:|---:|---:|
| Median fields returned | 54 | 49 | 51 |
| First request | 40.86 ms | 35.64 ms | 106.72 ms |
| Exact repeat | 0.94 ms | 0.98 ms | 95.95 ms |
| Shifted refresh | 44.54 ms | 38.22 ms | 110.84 ms |
| VictoriaLogs calls on first requests | 14 | 8 | — |
| VictoriaLogs calls on repeats | 0 | 0 | — |
| VictoriaLogs calls on refresh | 10 | 5 | — |
| Decoded bytes on first requests | 467,613 | 455,750 | — |
| Decoded bytes on refresh | 455,392 | 444,022 | — |

On the incident window, one of the five fields added by the base index
scan was `ip`. Its separately replayed breakdown returned 189,032 series,
259,513 points and 12,571,800 bytes of JSON. The candidate does not
advertise that unsampled card. The existing differences in discovery of
names ending in `_extracted` account for the two remaining missing fields
in the typical sample.

## Browser and API checks

| Workload | Outcome |
|---|---|
| Fields over 3h, 3 fixture services, 8 fields | Full frames matched Loki on first view, repeat and shifted refresh. Seven fields had 1,050 series each. No warning or new OOM. |
| Fresh `production` data over 15m | Main 50 cards in 4.8 s, candidate 49 in 5.3 s, Loki 50 in 8.2 s. All requests returned 200 without partial-result warnings or new OOM. These are single browser runs, with the normal datasource variants. |
| Older `production` incident over 3h | The candidate displayed 49 cards and four large requests returned 500. The Grafana plugin was killed and the cgroup OOM counter increased by one. |
| Isolated older `trace_id` breakdown through Grafana | The test cancelled the single request after cgroup memory reached 6.79 GiB. The OOM counter stayed unchanged. |

The affected older data was ingested with JSON keys promoted to stored
VictoriaLogs fields. For the same pre-parser query on that window, the
proxy returned 449,155 trace ID series (37.5 MB JSON) where Loki returned
189,318 (16.5 MB). The generator now preserves the line and structured
metadata as separate inputs. The provenance gap for historical data
remains open under `semantics/label-filter-before-parser-reads-ingest-json-key`.
Reducing the Fields fan-out addresses one source of work. A single large
answer can still exceed the datasource's memory budget after decoding.

Visual smoke exercises both changes on a small dual-written fixture.
`dd-fields-recent-sample` removes the extra historical field and matches
Loki. `dd-fields-json-sample-filter` restores two rendered charts and
matches Loki's field sample. The pre-existing `detected_labels` response
difference on that page is present on both proxy builds.

## Long-range overview follow-up

At both 24h and 48h, the Fields grid issued 53 distinct targets and the
selected trace field issued three. The trace target had the same LogQL,
time bounds, interval, maxDataPoints and source in both views. A
[wire replay](wire-context.json) through the real Grafana datasource
confirmed identical backend requests for the two 24h views. The grid
and exact detail therefore need an explicit request distinction.

On VictoriaLogs 1.54.0, a restricted JSON unpack followed by four
per-field counts in a single stats query matched each separate count.
The single run queried the batch first and the separate counts next,
using 35 hours of identically ingested fixture data.

| Requested range | Step | Separate VL calls | Batched VL calls | Separate elapsed | Batched elapsed |
|---|---:|---:|---:|---:|---:|
| 24h | 5 min | 4 | 1 | 115 ms | 38 ms |
| 48h | 10 min | 4 | 1 | 111 ms | 29 ms |

The populated buckets of these volume counts matched Loki's. The older
batch-ETL data differed from Loki in two buckets, despite identical
batched and separate VictoriaLogs counts. The measured primitive
covers aggregate volumes. It has yet to pass the grid's visual and
detail interactions as a product feature. The
[per-field fixture counts](overview-vl-fixture.json) retain each
bucket comparison and the unaggregated call totals.
