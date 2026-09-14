---
sidebar_label: Performance Testing Guide
description: Running the Playwright Explore UI coverage and performance baseline suites, with pointers to the loki-bench read-path benchmark.
---

# Performance Testing Guide

This guide explains how to run and interpret the Playwright suites that cover Grafana Explore UI behavior and browser-side timings against the e2e compose stack.

For read-path throughput, latency and resource comparisons between Loki, VictoriaLogs through the proxy, and VictoriaLogs native, use the `loki-bench` harness described in `bench/README.md`; published results and methodology are in [Benchmarks](benchmarks.md).

## Overview

Two Playwright suites live in `test/e2e-ui/tests/`:

1. **Comprehensive UI Tests** (`@comprehensive-ui`) - validates Explore UI flows without browser or datasource errors; runs in CI
2. **Performance Baseline** (`@performance`) - measures browser-side timings against fixed thresholds; local only

## Prerequisites

Start the e2e stack with the log generator profile, as CI does, then install the UI test dependencies:

```bash
cd test/e2e-compat
docker compose --profile ui up -d --build
../../scripts/ci/wait_e2e_stack.sh 180

cd ../e2e-ui
npm ci
npx playwright install chromium
```

`GRAFANA_URL` defaults to `http://127.0.0.1:3002`.

## Test Suites

### Comprehensive UI Coverage

**File**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts`

**Purpose**: Exercise the main Explore flows against the proxy datasource. Every test fails on Grafana error toasts, unexpected `console.error` messages, or failed datasource requests. Timings are recorded as `timing-ms` test annotations and printed to the console; they are not asserted.

**Test Categories** (`test.describe` groups):

| Category | Tests | What it validates |
|----------|-------|-------------------|
| Page Load Performance | 3 | Explore page loads, query editor visible, Explore toolbar actions visible |
| Query Editor UI | 3 | LogQL editor renders, a query can be entered, a query executes and shows results |
| Query Execution | 3 | `sum(rate(...))` metric query, `\| json` parsed logs, results shown in the matching panel |
| UI Interactions | 3 | page loads quickly, results appear after execution, empty results handled |
| Time Range & Filters | 2 | label filter stage (`\| level="error"`), `avg_over_time ... \| unwrap` over a 5-minute range |
| Performance Summary | 1 | prints averages of the timings collected in the run |

**Total**: 15 tests

**CI**: shard `explore-comprehensive` in `.github/workflows/ci.yaml` runs `npx playwright test --grep @comprehensive-ui`.

#### Running Comprehensive UI Tests

```bash
cd test/e2e-ui

# Run all comprehensive UI tests
npx playwright test tests/explore-comprehensive-ui.spec.ts

# Run one category (matches the describe title)
npx playwright test tests/explore-comprehensive-ui.spec.ts --grep "Page Load Performance"
npx playwright test tests/explore-comprehensive-ui.spec.ts --grep "Query Editor UI"
npx playwright test tests/explore-comprehensive-ui.spec.ts --grep "Time Range & Filters"

# One line per test
npx playwright test tests/explore-comprehensive-ui.spec.ts --reporter=list

# Record a trace for every test
npx playwright test tests/explore-comprehensive-ui.spec.ts --trace=on
```

---

### Performance Baseline Tests

**File**: `test/e2e-ui/tests/performance-baseline.spec.ts`

**Purpose**: Measure browser-side timings against the thresholds in the spec's `THRESHOLDS` constant.

**Metrics Tracked**:

| Test | Threshold | Asserted |
|--------|--------|---------------|
| Explore page load time | &lt;3000 ms | yes |
| Simple metric query response | &lt;5000 ms | yes |
| JSON parsed logs query response | &lt;5000 ms | yes |
| Log entry expansion time | &lt;500 ms | only when the expand button is visible |
| Label selector load time | &lt;1000 ms | only when the label selector is visible |
| Concurrent filter changes response | 5000 ms | recorded only |
| Performance Report | - | prints every recorded result and fails if any recorded result exceeded its threshold |

**Total**: 7 tests

**CI**: not run. No `e2e-ui` shard matches `@performance`, so treat results as local measurements.

#### Running Performance Baseline

```bash
cd test/e2e-ui

# Run performance baseline tests
npx playwright test tests/performance-baseline.spec.ts

# Run with HTML report, then open it
npx playwright test tests/performance-baseline.spec.ts --reporter=html
npm run report

# Run specific test (matches the test title)
npx playwright test tests/performance-baseline.spec.ts --grep "page load"

# One line per test
npx playwright test tests/performance-baseline.spec.ts --reporter=list
```

The `Performance Report` test prints a table with one `PASS`/`FAIL` row per recorded measurement (duration, threshold, and percentage of the threshold) followed by a `SUMMARY: <passed>/<total> tests passed` line. Absolute numbers depend on the host, Docker resources and the data the log generator has written, so compare runs from the same machine only.

---

## Tracking Performance Over Time

### Establish Baseline

When you first add these tests or after major infrastructure changes:

```bash
cd test/e2e-ui
npx playwright test tests/performance-baseline.spec.ts --reporter=list > /tmp/baseline-$(date +%Y-%m-%d).txt
cat /tmp/baseline-*.txt
```

Save this output as your reference.

### Compare Against Baseline

```bash
cd test/e2e-ui
npx playwright test tests/performance-baseline.spec.ts --reporter=list > /tmp/current-$(date +%Y-%m-%d).txt
diff -u /tmp/baseline-*.txt /tmp/current-*.txt
```

Look for:
- Any measurement moving from PASS to FAIL
- Measurements approaching their thresholds (over 80% of the target)
- A consistent upward trend across several runs

---

## Advanced Usage

### Debugging Slow Tests

When a test fails or is slower than expected:

```bash
# Run with full tracing
npx playwright test tests/performance-baseline.spec.ts --trace=on

# Open a recorded trace (one trace.zip per test under test-results/)
npx playwright show-trace test-results/<test-directory>/trace.zip
```

`playwright.config.ts` sets `trace: "on-first-retry"` and `screenshot: "only-on-failure"`, so without `--trace=on` traces are recorded only when a test is retried (`retries` is 1 when `CI` is set, 0 otherwise).

The trace shows:
- Network timeline
- DOM snapshots
- Console output
- Screenshots at each step

### Profiling Specific Operations

Modify a test temporarily to add detailed timing:

```typescript
test("custom profile: query expansion", async ({ page }) => {
  await openExplore(page, PROXY_DS);

  console.time("query-execution");
  await runQuery(page, '{app="api-gateway"} | json');
  console.timeEnd("query-execution");

  console.time("log-expand");
  const row = page.locator("[data-testid='log-row']").first();
  await row.click();
  console.timeEnd("log-expand");
});
```

`console.time` / `console.timeEnd` print the elapsed milliseconds for each label to the test output.

### Browsers

`playwright.config.ts` defines a single `chromium` project. To use an installed Chrome/Chromium instead of the Playwright download, set `PLAYWRIGHT_EXECUTABLE_PATH`; set `HEADED=1` to run headed.

---

## Troubleshooting

### Tests timing out

**Symptom**: Tests hang or time out after 60s (the per-test `timeout` in `playwright.config.ts`)

**Cause**: Stack not ready, network latency, or browser crash

**Fix**:
```bash
# Ensure stack is running
docker ps | grep e2e-grafana

# Check stack health
curl -s http://127.0.0.1:3002/api/health | jq .

# Reinstall the Playwright browser
npx playwright install chromium
```

### Inconsistent timings

**Symptom**: Same test takes 1.5s one time, 3.5s another

**Cause**: System load, browser garbage collection, log generator volume, network variance

**Fix**:
- Run tests serially: `npx playwright test tests/performance-baseline.spec.ts --workers=1` (locally `workers` defaults to 4, or `WORKERS`)
- Close other heavy processes
- Run multiple times and compare several samples

### Tests pass locally but fail in CI

**Symptom**: `@comprehensive-ui` is green locally, red in CI

**Cause**: CI runs one worker with one retry on shared GitHub-hosted runners, and uses the runner's Chrome/Chromium when present

**Fix**:
1. Reproduce with `CI=1 npx playwright test --grep @comprehensive-ui`
2. Download the `playwright-report-explore-comprehensive` artifact from the failed run
3. Check the guard failures (console errors, failed requests, error toasts) before suspecting timings; this suite does not assert timings

---

## Best Practices

**Do**:
- Collect baselines on known-good hardware
- Compare runs from the same machine and stack state
- Document any threshold changes in the PR that changes them
- Review performance PRs with extra scrutiny

**Don't**:
- Use CI results as absolute performance truth (variance is normal)
- Chase sub-100ms differences (noise at that scale)
- Loosen thresholds just to make tests pass
- Run performance tests with other heavy processes running

---

## References

- **Test files**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts` and `test/e2e-ui/tests/performance-baseline.spec.ts`
- **UI test suite**: `test/e2e-ui/README.md` and [Testing](testing.md)
- **Read-path benchmarks**: `bench/README.md` and [Benchmarks](benchmarks.md)
- **Playwright docs**: https://playwright.dev/docs/intro
