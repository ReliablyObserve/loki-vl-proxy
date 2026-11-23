# Correctness and CI proof follow-up

The product Go source is still the measured `fb2b09872464` source. This follow-up
repairs proof collection and extends the existing catalogs; it does not establish
a new product latency or memory improvement. Base image: `895c23078f`.

| Evidence | Result | Scope |
|---|---|---|
| Central compatibility matrix | 244/244 pass | Actual candidate, Loki and VL; fresh isolated numeric tenant and fixture marker |
| Two new rate contracts on main | 0/2 pass, expected RED | Both lack Loki's `user_id="usr_01"` series; complete failures retained |
| Same rate contracts on candidate | 2/2 pass | Included in the 244-case run; both parser orders |
| Previous routing executions | 27,120 unchanged; 20 added | No removed or changed execution blocks; ordinary replay passes |
| Existing semantic recordings | Four archives byte-identical | Canonical spelling, rejected queries, log and metric differential recordings; no rewritten oracle answers |
| Class-boundary controls | Actual oracle passes | `k` and `user_id`, VL 1.49/1.54; five added 128/129-character, nested, array and malformed-input controls |
| Native class versus Loki | 32 actual comparisons pass | Both parser orders, count/rate, instant/range, on the admitted controls |
| Browser capture mechanics | Exact 2 MiB recovered | One API POST and one CDP read; deliberately injected Playwright failure; complete identity/hash check |
| Existing visual unit suite | 263 pass | Final capture source; initial sandbox-blocked loopback run retained separately |
| Existing A/B harness unit suite | 611 pass | Complete suite after the final cache-evidence correction |
| Existing parity harness unit suite | 78 pass | Catalog census refreshed to 2,116 entries / 4,232 requests |
| Flushed-request collector mechanics | Three real requests pass | Two existing playground peers and one computed Loki reference; complete point/label comparison and both CPU/RSS boundaries |

The complete passing and expected-failing matrix events are retained in
`candidate-244.jsonl.gz` and `main-rate-red.jsonl.gz`. The latter is an expected
failure of main, not a passing test run. The existing peer was restored to its
original image, command and user after each check.

`route-preservation.json` audits the old execution blocks. `archive-preservation.json`
audits every pre-existing recording; only additive plan counters were regenerated.
Strict registry, ownership, wiring and generated-report checks pass using the
unfiltered old and new Go evidence. The historical failed full local run remains
a failed run; its exact package rerun and the published head's successful full CI
are separate evidence.

## What the collectors now prove

Cold remeasurement retains the actual per-request costs, original observation,
source-history bounds, backend-call headers and full candidate/reference
comparisons. The median's timing and resource costs use the same observations.
Missing boundaries, an unproved or incorrect original candidate answer, an
incorrect additional answer, partial counters or inconsistent provenance cannot
turn into a complete runtime proof. A differing baseline may document the fix;
it does not excuse an incorrect candidate.

Extra correctness reads preserve the query and append only 128 ASCII spaces/tabs
to obtain a separate Loki cache identity. Their original and actual spellings,
complete response hashes and cache counters are retained. These extra reads are
not the measured default-profile Loki requests. The standard cache policy and
latency thresholds remain unchanged. Cache blocks and index statistics are kept
separate from cached query answers.

The actual collector receipt in `collector-live.json` is a mechanics check on
two instances of the same existing playground image. It is not main-versus-PR
performance evidence. Its source hashes identify the collector that ran. Process
memory is sampled at the request boundaries; it is not a continuous peak.

The browser fallback uses a separate bounded CDP session: 128 MiB total,
32 MiB per response, bounded identity metadata and at most two simultaneous body
reads. It requires one completed request matching method, full URL, POST data
and response status. Ambiguous, redirected, cancelled, unavailable and oversized
responses remain incomplete. It performs no API refetch. `capture-integrity.json`
binds the final source and tests to the actual helper run; sampled Node/Chrome
process-tree memory was 676 MiB and both processes closed. This is not the
additional memory cost of a Grafana page.

## Acceptance still open

The published `2475951da` run has real cold-query regressions, incomplete runtime
references and a failed ingestion shard. Native Grafana positive rate-fix scenes
and actual Fields 6 h eviction recovery need fresh CI captures. Complete
Fields/Labels/48 h UX, current-source disk restart and peer-loss acceptance remain
open. No performance, data-completeness or visual threshold was relaxed.

The failed standing-data 1/6/24 h exploratory rate comparison is not used as a
passing proof: 1 h was empty and wider replies did not match. The isolated fixture
checks above are independent, positive-input checks. Neither substitutes for the
missing native browser or workload acceptance.
