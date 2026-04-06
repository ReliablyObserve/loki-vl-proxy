# Rules And Alerts Migration

This project treats alerting and ruler compatibility as a read path:

- `vmalert` executes alerting and recording rules against VictoriaLogs
- Loki-compatible read endpoints on the proxy expose those rules and alerts back to Grafana
- the proxy does not implement Loki ruler write APIs

If you already have Loki rule files, use the migration tool to convert them into `vmalert` rule files.

## Migration Model

Target shape:

1. Keep Grafana dashboards and Explore on the Loki datasource that points to the proxy.
2. Run `vmalert` for rules and alerts.
3. Point `vmalert` at VictoriaLogs.
4. Point the proxy `-ruler-backend` and `-alerts-backend` at `vmalert`.
5. Let Grafana read rules and alerts through Loki-compatible endpoints on the proxy.

This gives you:

- Loki-compatible rule and alert visibility in Grafana
- VictoriaLogs-native rule execution
- no write-side botched Loki ruler emulation

## Tooling

Convert a Loki-style rule file:

```bash
go run ./cmd/rules-migrate -in loki-rules.yaml -out vmalert-rules.yaml
```

Read from stdin and write to stdout:

```bash
cat loki-rules.yaml | go run ./cmd/rules-migrate > vmalert-rules.yaml
```

Supported inputs:

- Prometheus/Loki-style `groups:` rule files
- legacy Loki YAML maps such as the classic `/loki/api/v1/rules` shape

Output:

- `groups:` YAML
- every group forced to `type: vlogs`
- every rule `expr` translated from LogQL into LogsQL

## Example

Input:

```yaml
groups:
  - name: api
    interval: 1m
    rules:
      - alert: HighErrorRate
        expr: sum by (app) (rate({app="api-gateway"} |= "error" [5m]))
        for: 2m
        labels:
          severity: page
```

Output:

```yaml
groups:
  - name: api
    interval: 1m
    type: vlogs
    rules:
      - alert: HighErrorRate
        expr: '{app="api-gateway"} ~"error" | stats rate() as value by (app)'
        for: 2m
        labels:
          severity: page
```

The exact generated expression depends on the translator, but the important part is that the output is now valid `vmalert` + VictoriaLogs rule YAML.

## Deploying vmalert

Minimal example:

```yaml
services:
  vmalert:
    image: victoriametrics/vmalert:v1.139.0
    command:
      - -datasource.url=http://victorialogs:9428
      - -rule=/rules/vmalert-rules.yaml
      - -rule.defaultRuleType=vlogs
      - -notifier.url=http://alertmanager:9093
```

For local validation without a real Alertmanager:

```yaml
services:
  vmalert:
    image: victoriametrics/vmalert:v1.139.0
    command:
      - -datasource.url=http://victorialogs:9428
      - -rule=/rules/vmalert-rules.yaml
      - -rule.defaultRuleType=vlogs
      - -notifier.blackhole
```

Point the proxy at `vmalert`:

```bash
./loki-vl-proxy \
  -backend=http://victorialogs:9428 \
  -ruler-backend=http://vmalert:8880 \
  -alerts-backend=http://vmalert:8880
```

## Grafana Behavior

After `vmalert` is configured and the proxy points at it:

- `/loki/api/v1/rules` returns classic Loki-compatible YAML
- `/api/prom/rules` returns the same classic YAML alias
- `/prometheus/api/v1/rules` returns Prometheus-style JSON
- `/loki/api/v1/alerts`, `/api/prom/alerts`, and `/prometheus/api/v1/alerts` return JSON alert state

So Grafana can keep reading alert/rule state from the Loki-facing datasource path.

## What Converts Cleanly

These rule patterns generally migrate well:

- stream selectors
- line filters
- `| json`
- `| logfmt`
- basic metric functions like `rate`, `count_over_time`, `sum_over_time`
- normal `sum by (...)`, `topk(...)`, and similar aggregation patterns already covered by the translator

## What Needs Review

The migration tool now defaults to a hardened mode:

- safe rules are converted automatically
- risky rules fail conversion with a specific manual-review error
- you can generate a review report with `-report`
- you can override the safety guard with `-allow-risky` if you intentionally want translated output plus warnings

Example:

```bash
go run ./cmd/rules-migrate \
  -in loki-rules.yaml \
  -out vmalert-rules.yaml \
  -report migration-review.txt
```

If you intentionally want translated output even for risky rules:

```bash
go run ./cmd/rules-migrate \
  -in loki-rules.yaml \
  -out vmalert-rules.yaml \
  -report migration-review.txt \
  -allow-risky
```

The migration tool uses the translator, not the proxy runtime.

That means proxy-only runtime emulation does not automatically exist in `vmalert` rules. Review converted rules if they rely on:

- `without()`
- `on()` / `ignoring()`
- `group_left()` / `group_right()`
- subquery `[range:step]`
- `histogram()` recording rules
- other behaviors that the proxy currently emulates above VictoriaLogs rather than translating directly into a backend-native equivalent

For those cases:

1. convert the file
2. review the translated `expr`
3. validate it directly against VictoriaLogs or `vmalert`
4. simplify or rewrite the rule if needed

## Validation Workflow

Recommended migration flow:

1. Convert the Loki rule file with `cmd/rules-migrate`.
2. Start `vmalert` with the generated file.
3. Call `GET /api/v1/rules?datasource_type=vlogs` on `vmalert`.
4. Point the proxy at `vmalert`.
5. Verify:
   - `GET /prometheus/api/v1/rules`
   - `GET /prometheus/api/v1/alerts`
   - `GET /loki/api/v1/rules`
6. Confirm Grafana sees the rules/alerts through the Loki datasource path.

## Real-Life Test Coverage

The compose-backed compatibility stack already validates this model with a live backend:

- VictoriaLogs
- `vmalert`
- Loki-VL-proxy
- Grafana

See:

- [Testing](testing.md)
- [API Reference](api-reference.md)
- [Known Issues](KNOWN_ISSUES.md)
