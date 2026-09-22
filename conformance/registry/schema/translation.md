# Translation item schema

One file per feature, end to end across the three layers.

```yaml
id: <feature-id>
title: <what a user does>
covers:                      # registry items this feature spans
  endpoints: [loki_api_v1_query_range]
  behaviours: [severity-detected-level-derivation]
  logql: [range_function-count-over-time]
consumers: [explore, drilldown, api]

loki_layer: |                # what Loki does and returns, the contract clients rely on
victorialogs_layer: |        # what VictoriaLogs can give, and in what shape
execution: native_vl | hybrid | proxy_side
native_reuse: |              # which VictoriaLogs constructs carry the work, from which version

deltas:                      # every difference that would change the answer if ignored
  - id: <delta-id>
    difference: |            # Loki says X, VictoriaLogs gives Y
    impact: |                # what the user would see if the proxy did nothing
    proxy_layer: |           # what the proxy inserts, and at which stage
    where: internal/proxy/<file>.go:<line>
    cost: |                  # extra backend work or proxy work this costs
    cases: [<case ids>]      # proof that the delta is closed
    state: proven | partial | gap | waived

performance:
  verdict: native | acceptable | needs_work
  evidence: |                # measured: proxy vs Loki latency, VictoriaLogs rows scanned
```

`execution` says where the answer is computed. `deltas` is the part that matters:
each one is a concrete way the output would differ from Loki, the layer the proxy
adds to close it, the code that does it, what it costs, and the cases that prove it.
A feature is only `proven` when every delta is proven.
