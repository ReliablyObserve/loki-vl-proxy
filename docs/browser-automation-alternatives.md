# Browser Automation Alternatives

This document evaluates alternative browser automation tools for the Loki-VL-proxy e2e test suite.

## Current Setup: Playwright

**Status**: Production ✅

### Overview
- **Language**: TypeScript with @playwright/test
- **Engines**: Chromium, Firefox, WebKit
- **CDP Support**: Yes (Chrome DevTools Protocol)
- **Performance**: 
  - Page load: ~500-1000ms
  - Query execution: 1-3 seconds
  - UI interactions: 50-200ms

### Strengths
- Mature and stable
- Excellent TypeScript support
- Great debugging (Inspector, trace viewer)
- Network interception & mocking
- Cross-browser testing (Chromium, Firefox, WebKit)
- Large community and ecosystem

### Weaknesses
- Heavy memory footprint (~200-300MB per instance)
- Slow startup (~500-1000ms)
- Browser binaries are large (~200-300MB each)
- Not optimized for headless-only scenarios

---

## Alternative: Obscura

**Status**: Research / Evaluation 🔬

**Repository**: [h4ckf0r0day/obscura](https://github.com/h4ckf0r0day/obscura)

### Overview
Obscura is a Rust-based headless browser engine designed specifically for web automation and scraping at scale.

- **Language**: Rust with Puppeteer/Playwright compatibility layer
- **Engine**: Chromium-based (V8 for JavaScript)
- **CDP Support**: Yes (full Chrome DevTools Protocol)
- **Memory**: ~30-50MB per instance (6-10x lighter)
- **Binary size**: ~70MB (vs 300MB for Chrome)
- **Page load**: ~85ms (6x faster than Chrome)
- **Anti-detect**: Built-in fingerprint spoofing

### Claimed Performance Improvements

| Metric | Obscura | Playwright/Chrome |
|--------|---------|-------------------|
| Memory per instance | 30 MB | 200-300 MB |
| Binary size | 70 MB | 300+ MB |
| Page load time | 85 ms | 500-1000 ms |
| Startup time | ~50 ms | ~500 ms |
| Concurrent instances (1GB RAM) | 33+ | 5-10 |

### Strengths
- Extremely lightweight (memory and disk)
- Blazing fast startup and page loads
- Built for headless-only automation
- Anti-detect features (fingerprint spoofing)
- Rust performance characteristics
- Drop-in replacement for Playwright/Puppeteer

### Weaknesses
- Early stage (v0.1.0) - potential stability issues
- Smaller community than Playwright
- Limited debugging tools compared to Playwright Inspector
- Rust-based (would require additional tooling in TypeScript)
- Not yet battle-tested at scale
- May have CDP protocol gaps vs full Chrome

### Integration Path

If adopting Obscura, the path would be:

1. **Phase 1: Prototype** - Create a wrapper layer compatible with our helpers
2. **Phase 2: Parallel testing** - Run same tests with both Playwright and Obscura
3. **Phase 3: Benchmark** - Measure performance on our actual test suite
4. **Phase 4: Evaluate** - Assess stability, feature parity, and real-world performance
5. **Phase 5: Migration** - Conditionally use Obscura for specific test scenarios

### Risk Assessment

**High Risk Items**:
- Early stage (v0.1.0) may have stability issues
- Debugging experience is unknown
- Feature parity with Chrome CDP unknown
- No production track record

**Mitigation**:
- Keep Playwright as primary until Obscura reaches v1.0+
- Use Obscura for selected tests initially (performance benchmarks, simple queries)
- Monitor for issues in Obscura repo
- Maintain fallback to Playwright

---

## Recommendation: Hybrid Approach

### For Now (Q2 2026)
**Stay with Playwright** as the primary tool because:
- ✅ Proven stability and reliability
- ✅ Excellent tooling and debugging
- ✅ Already integrated and working well
- ✅ Cross-browser testing capability
- ❌ Obscura is too early-stage for production

### Future (Q4 2026+)
**Evaluate Obscura for**:
- Performance-critical tests (benchmarking)
- Large-scale concurrent test scenarios
- Resource-constrained CI environments
- Potentially use as **alternative engine** alongside Playwright

### Specific Test Scenarios for Obscura Eval

If and when Obscura reaches v1.0+:

1. **Performance Benchmarks** - Use Obscura's speed advantage for timing tests
2. **Stress Testing** - Concurrent instance creation (Obscura's memory advantage)
3. **Scraping-Heavy Tests** - Pattern extraction from large result sets
4. **Anti-Bot Testing** - Verify Obscura's fingerprint spoofing works

---

## Current Test Coverage

### Comprehensive UI Testing
- **File**: `test/e2e-ui/tests/explore-comprehensive-ui.spec.ts`
- **Coverage**: 
  - Page load performance
  - Query editor UI interactions
  - Query execution
  - Field explorer
  - Filters & label selection
  - Time range picker
  - Logs drilldown integration
  - Edge cases (large result sets, special characters, empty results)
  - Performance metrics collection

### Performance Baseline
- **File**: `test/e2e-ui/tests/performance-baseline.spec.ts`
- **Metrics Tracked**:
  - Explore page load time (target: &lt;3s)
  - Simple query response (target: &lt;5s)
  - JSON parsed logs query (target: &lt;5s)
  - Log entry expansion (target: &lt;500ms)
  - Label selector load (target: &lt;1s)
  - Rapid filter changes (target: &lt;5s)

---

## Running Performance Tests

### Run all comprehensive UI tests
```bash
cd test/e2e-ui
npx playwright test explore-comprehensive-ui.spec.ts
```

### Run performance baseline only
```bash
cd test/e2e-ui
npx playwright test performance-baseline.spec.ts
```

### Run with specific browser
```bash
# Chromium only
npx playwright test --project chromium explore-comprehensive-ui.spec.ts

# Firefox only
npx playwright test --project firefox performance-baseline.spec.ts
```

### Generate performance report
```bash
npx playwright test performance-baseline.spec.ts --reporter=html
# Open playwright-report/index.html
```

---

## Monitoring Performance Over Time

To track performance regression:

1. **Establish baseline** (current PR):
   ```bash
   npm run test:e2e:ui:performance > baseline-$(date +%Y-%m-%d).txt
   ```

2. **Run monthly** to detect regressions:
   ```bash
   npm run test:e2e:ui:performance > current-$(date +%Y-%m-%d).txt
   diff baseline-*.txt current-*.txt
   ```

3. **CI Integration**:
   - Performance tests run on every commit
   - Warnings if any metric exceeds threshold
   - Trends tracked in metrics dashboard

---

## References

- [Obscura GitHub](https://github.com/h4ckf0r0day/obscura)
- [Playwright Documentation](https://playwright.dev)
- [Chrome DevTools Protocol](https://chromedevtools.github.io/devtools-protocol)
- [Puppeteer Documentation](https://github.com/puppeteer/puppeteer)
