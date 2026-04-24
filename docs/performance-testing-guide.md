# Performance Testing Guide

This guide explains how to run, interpret, and use the Loki Explorer performance tests added in PR #246.

## Overview

Two complementary test suites measure and track Loki Explorer UI performance:

1. **Comprehensive UI Tests** - Validates all UI interactions work correctly
2. **Performance Baseline** - Measures response times against targets

## Test Suites

### Comprehensive UI Coverage

**File**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts`

**Purpose**: Ensure all Loki Explorer and Logs Drilldown UI elements are clickable and functional.

**Test Categories**:

| Category | Tests | What it validates |
|----------|-------|-------------------|
| Page Load | 3 | Explorer page loads, datasource selector visible and clickable |
| Query Editor | 5 | Input works, syntax hints appear, history navigation functional |
| Query Execution | 4 | Queries run, results display, entries expandable, errors handled |
| Field Explorer | 3 | Field list visible, values clickable, field-value filtering works |
| Filters & Labels | 3 | Label selector present, labels available, multiple filters addable |
| Time Range | 3 | Picker visible, time range changes apply, custom ranges supported |
| Drilldown | 2 | Drill buttons functional, pattern analysis works |
| Edge Cases | 5 | Large results, special chars, empty results, rapid changes, debouncing |
| Performance Summary | 1 | Reports all collected metrics |

**Total**: 30+ test cases

#### Running Comprehensive UI Tests

```bash
cd test/e2e-ui

# Run all comprehensive UI tests
npx playwright test explore-comprehensive-ui.spec.ts

# Run specific test category
npx playwright test explore-comprehensive-ui.spec.ts --grep "Page Load"
npx playwright test explore-comprehensive-ui.spec.ts --grep "Query Editor"
npx playwright test explore-comprehensive-ui.spec.ts --grep "Edge Cases"

# Run with verbose output
npx playwright test explore-comprehensive-ui.spec.ts --reporter=verbose

# Run with tracing for debugging failures
npx playwright test explore-comprehensive-ui.spec.ts --trace=on
```

#### Output Example

```
✅ Page Load Performance
  ✓ should load Explore page within acceptable time
  ✓ should display datasource selector
  ✓ should show all datasource options

✅ Query Editor UI
  ✓ should render LogQL query editor
  ✓ should allow typing query in editor
  ✓ should show query syntax hints on focus
  ✓ should support query history navigation

...

📊 PERFORMANCE METRICS SUMMARY:
================================
Page Load Time (avg): 1250ms
  Range: 1150ms - 1350ms
Query Response Time (avg): 2850ms
  Range: 2500ms - 3200ms
UI Interaction Time (avg): 180ms
  Range: 120ms - 250ms
================================
```

---

### Performance Baseline Tests

**File**: `test/e2e-ui/tests/performance-baseline.spec.ts`

**Purpose**: Measure and validate response times against target thresholds.

**Metrics Tracked**:

| Metric | Target | Why Important |
|--------|--------|---------------|
| Page load time | &lt;3000ms | Affects user perception of responsiveness |
| Simple query response | &lt;5000ms | Basic metric queries should be fast |
| JSON parsed logs query | &lt;5000ms | Parsed logs are common queries |
| Log entry expansion | &lt;500ms | UI responsiveness for interactions |
| Label selector load | &lt;1000ms | Label filtering must be snappy |
| Rapid filter changes | &lt;5000ms | Debouncing should handle quick changes |

#### Running Performance Baseline

```bash
cd test/e2e-ui

# Run performance baseline tests
npx playwright test performance-baseline.spec.ts

# Run with HTML report
npx playwright test performance-baseline.spec.ts --reporter=html
open playwright-report/index.html

# Run specific test
npx playwright test performance-baseline.spec.ts -k "page load"

# Run with verbose timing
npx playwright test performance-baseline.spec.ts --reporter=verbose
```

#### Output Example

```
═══════════════════════════════════════════════════════════
        LOKI EXPLORER - PERFORMANCE BASELINE REPORT        
═══════════════════════════════════════════════════════════
✅ PASS | Explore page load                         |  1245ms / 3000ms (41%)
✅ PASS | Simple metric query                       |  2850ms / 5000ms (57%)
✅ PASS | JSON parsed logs query                    |  3120ms / 5000ms (62%)
✅ PASS | Log entry expansion                       |   185ms / 500ms (37%)
✅ PASS | Label selector open                       |   750ms / 1000ms (75%)
✅ PASS | Rapid filter application                  |  4250ms / 5000ms (85%)
═══════════════════════════════════════════════════════════
SUMMARY: 6/6 tests passed
═══════════════════════════════════════════════════════════
```

---

## Tracking Performance Over Time

### Establish Baseline

When you first add these tests or after major infrastructure changes:

```bash
cd test/e2e-ui
npm test -- performance-baseline.spec.ts > /tmp/baseline-$(date +%Y-%m-%d).txt
cat /tmp/baseline-*.txt
```

Save this output as your reference.

### Compare Against Baseline

Run monthly (or when you notice slowdowns):

```bash
cd test/e2e-ui
npm test -- performance-baseline.spec.ts > /tmp/current-$(date +%Y-%m-%d).txt
diff -u /tmp/baseline-*.txt /tmp/current-*.txt
```

Look for:
- ❌ Any test moving from PASS to FAIL
- ⚠️ Metrics approaching thresholds (>80% of target)
- 📈 Consistent upward trend (regression)

### Example: Detecting Regression

**Good** (stable or improving):
```
✅ Query response: 2850ms → 2600ms (improvement)
✅ Page load: 1245ms → 1300ms (minor variance, <5%)
```

**Bad** (regression):
```
❌ Query response: 2850ms → 4200ms (47% slower - INVESTIGATE)
❌ Page load: 1245ms → 2500ms (100% slower - CRITICAL)
```

---

## CI Integration

### Running in CI Pipeline

The tests run automatically on all PRs:

```bash
# Same commands as local
npx playwright test explore-comprehensive-ui.spec.ts
npx playwright test performance-baseline.spec.ts
```

**Expected behavior**:
- ✅ All tests PASS on baseline hardware
- ⚠️ Tests may be ~10-20% slower on CI hardware (acceptable variance)
- ❌ Tests FAIL if metrics exceed thresholds by >5%

### Interpreting CI Failures

If CI shows a performance test failure:

1. **Check baseline**: Did something change in the proxy/stack?
   ```bash
   git log --oneline -20 | grep -E "perf|cache|query|api"
   ```

2. **Run locally**: Reproduce on your machine
   ```bash
   cd test/e2e-ui
   npx playwright test performance-baseline.spec.ts --reporter=html
   ```

3. **Investigate root cause**:
   - Database performance? (Check docker compose)
   - Proxy changes? (Run locally against same data)
   - Browser state? (Clear cache: `rm -rf .playwright`)

4. **Document findings**: Add comment to PR with findings

---

## Advanced Usage

### Debugging Slow Tests

When a test fails or is slower than expected:

```bash
# Run with full tracing
npx playwright test performance-baseline.spec.ts --trace=on

# Open trace in Playwright Inspector
npx playwright show-trace trace/actual-test.spec.webm
```

The trace shows:
- Network timeline
- DOM changes
- JavaScript execution
- Screenshots at each step

### Profiling Specific Operations

Modify test temporarily to add detailed timing:

```typescript
test("custom profile: query expansion", async ({ page }) => {
  await openExplore(page, PROXY_DS);
  
  // Detailed timing
  console.time("query-execution");
  await runQuery(page, '{job="api"} | json');
  console.timeEnd("query-execution");
  
  console.time("log-expand");
  const row = page.locator("[data-testid='log-row']").first();
  await row.click();
  console.timeEnd("log-expand");
  
  console.time("field-load");
  const fields = page.locator("[data-testid='field-item']");
  await expect(fields.first()).toBeVisible();
  console.timeEnd("field-load");
});
```

Output:
```
query-execution: 2845.42ms
log-expand: 182.15ms
field-load: 456.78ms
```

### Cross-Browser Testing

Run same tests in Firefox/Safari:

```bash
# Test all browsers
npx playwright test performance-baseline.spec.ts --project=chromium
npx playwright test performance-baseline.spec.ts --project=firefox
npx playwright test performance-baseline.spec.ts --project=webkit

# Compare results
echo "=== CHROMIUM ===" && npm test -- --project=chromium | grep -A 10 "SUMMARY"
echo "=== FIREFOX ===" && npm test -- --project=firefox | grep -A 10 "SUMMARY"
```

---

## Troubleshooting

### Tests timing out

**Symptom**: Tests hang or timeout after 30s

**Cause**: Stack not ready, network latency, or browser crash

**Fix**:
```bash
# Ensure stack is running
docker ps | grep grafana

# Check stack health
curl -s http://localhost:3002/api/health | jq .

# Clear browser cache
rm -rf .playwright
npx playwright install chromium
```

### Inconsistent timings

**Symptom**: Same test takes 1.5s one time, 3.5s another

**Cause**: System load, browser garbage collection, network variance

**Fix**:
- Run tests in isolation: `npx playwright test -j 1`
- Disable other processes: Close browser, IDEs, etc.
- Run multiple times and average: Collect 3-5 baseline samples

### Tests pass locally but fail in CI

**Symptom**: Green locally, red in CI

**Cause**: CI hardware slower, different Docker base image, network latency

**Fix**:
1. Run locally with `--project=chromium --retries=2`
2. Check CI hardware specs in `.github/workflows/ci.yaml`
3. Adjust thresholds if CI is consistently 20%+ slower
4. Use percentile thresholds instead of absolute times

---

## Best Practices

✅ **Do**:
- Run comprehensive UI tests before performance tests
- Collect baselines on known-good hardware
- Track trends over time (monthly samples)
- Document any threshold changes in git
- Review performance PRs with extra scrutiny

❌ **Don't**:
- Use CI results as absolute performance truth (variance is normal)
- Chase sub-100ms improvements (noise at that scale)
- Loosen thresholds just to make tests pass
- Run performance tests with other processes running
- Skip comprehensive UI tests and go straight to performance

---

## References

- **Test files**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts` and `performance-baseline.spec.ts`
- **Documentation**: `docs/browser-automation-alternatives.md`
- **Playwright docs**: https://playwright.dev/docs/intro
- **Performance optimization**: See `docs/api-reference.md` for API-level performance metrics
