# Drilldown range memory proof

Python 3.11+ and Docker Compose v2 are required. The harness compares six
concurrent Drilldown queries with Loki 3.7.9 over the same VictoriaLogs 1.52.0
fixture, recording response bodies, exact queries, configuration, binary hash,
container state/logs and sampled Go/process memory. The proxy binary must be
Linux and match the runtime image's architecture. Build the runtime image using
the repository Dockerfile, or supply an existing compatible image with
`--runtime-image`. Both binary paths are mounted read-only.

Use a dedicated, initially empty output directory and Docker project. All Docker
commands explicitly select `--context` (default `desktop-linux`); they never
switch the active context. Ports bind only to loopback and default to
63101/63428/63310. Override them with `--loki-port`, `--vl-port`, `--proxy-port`.
Keep context, project, output and port options identical for every operation.

```sh
PROOF=bench/drilldown-memory/proof.py
OUT=/tmp/drilldown-memory-example
PROJECT=drilldown-memory-example
python3 "$PROOF" setup --output "$OUT" --project "$PROJECT"
python3 "$PROOF" seed --output "$OUT" --project "$PROJECT"
python3 "$PROOF" reference --output "$OUT" --project "$PROJECT" --hours 12
python3 "$PROOF" reference --output "$OUT" --project "$PROJECT" --hours 48

# Replace these with Linux binary paths and a locally available runtime image.
for SOURCE in baseline candidate; do
  for HOURS in 12 48; do
    python3 "$PROOF" start --output "$OUT" --project "$PROJECT" \
      --source "$SOURCE" --baseline-binary /path/to/baseline \
      --candidate-binary /path/to/candidate --runtime-image loki-vl-proxy:e2e-local
    python3 "$PROOF" measure --output "$OUT" --project "$PROJECT" \
      --source "$SOURCE" --hours "$HOURS" \
      --baseline-binary /path/to/baseline --candidate-binary /path/to/candidate \
      --allow-baseline-oom
  done
done
python3 "$PROOF" cleanup --output "$OUT" --project "$PROJECT"
```

Each measurement starts six requests together: `trace_id`, `span_id`,
`request_id`, `user_id`, `version`, and `total`. The selector is
`{app="range-memory",env="production"}`; the exact step is `60000ms`. The default
360,000-row, 48-hour fixture puts 80% of rows in its last three hours and 20% in
its preceding 45 hours. Every row has unique trace/span metadata; alternate JSON
rows have unique request/total fields. Both backends receive the same logical
rows; VictoriaLogs' ingestion envelope preserves the original JSON in `_msg`.

Seeding is allowed once, including a failed attempt. To recover from a partial
seed, clean up the owned stack and use a new empty output directory. Reference
generation flushes Loki and VictoriaLogs, then retries until the independent
fixture oracle confirms all expected series **and event totals** for each
field. A full 48-hour reference must contain 360,000 trace/span series and
180,000 request/total series. The 12-hour oracle includes the first evaluation's
one-minute lookback. Nonempty partial results and Loki warnings fail validation.

Measurements revalidate reference completeness and compare complete canonical
matrices, including every label, timestamp and numeric value. `summary.json`
records `passed`; exit status is nonzero for candidate readiness/parity/container
failures. `--allow-baseline-oom` accepts only a baseline failure with Docker's
`OOMKilled` state, recorded separately from a pass. Sampling defaults to 4 Hz;
`--sample-interval 0.04` requests 25 Hz. Peaks are sampled observations, not a
continuous high-water mark. Loki's result cache is disabled; the proxy uses the
recorded five-second cache TTL, with a fresh proxy container per measurement.

For an existing compatible stack, `--existing-stack` permits **only** `reference`
and `measure` against its existing `compose.json`, `fixture.json`, `loki.yaml`
and `proxy-{source}` binaries (or explicit binary paths). It cannot seed, start
or clean up an unowned project. Explicitly set its context, project and ports.
The selected running binary bind mount is verified before measurement. Cleanup
requires the ownership marker created by setup and removes only that Compose
project's declared services/volumes; evidence files remain.

```sh
python3 bench/drilldown-memory/proof.py --help
python3 -m unittest discover -s bench/drilldown-memory -p 'test_*.py'
```
