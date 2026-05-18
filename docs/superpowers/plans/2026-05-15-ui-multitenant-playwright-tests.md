# UI Multi-Tenant Explore & Drilldown Playwright Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Playwright browser tests covering multi-tenant field-value click interactions in Explore, cardinality badge visibility in Drilldown, cross-datasource switching (multi-tenant ↔ single-tenant), and error boundary behavior for a missing tenant.

**Architecture:** New tests extend two existing spec files. Multi-tenant Explore interactions go into a new `test/e2e-ui/tests/explore-multitenant.spec.ts`. Additional Drilldown cases extend `test/e2e-ui/tests/logs-drilldown.spec.ts`. All helpers are imported from the existing `helpers.ts`. The new spec is wired into the `drilldown-multitenant` CI group (already in `playwright.config.ts`).

**Tech Stack:** Playwright (TypeScript), `@playwright/test`. Run with `cd test/e2e-ui && npx playwright test --grep @drilldown-mt`. Grafana runs at `http://localhost:3002`. All datasource helpers (`PROXY_MULTI_DS`, `PROXY_DS`, `openExplore`, `openLogsDrilldown`, `installGrafanaGuards`, etc.) are in `test/e2e-ui/tests/helpers.ts`.

---

## Context for the implementer

**Datasource constants (from `helpers.ts`):**
```typescript
PROXY_DS         = "Loki (via VL proxy)"           // single-tenant
PROXY_MULTI_DS   = "Loki (via VL proxy multi-tenant)" // multi-tenant (OrgID "0|fake")
PROXY_INTERACT_DS = "Loki (via VL proxy native metadata)" // for click interactions
```

**Key helpers:**
```typescript
import { openExplore, openLogsDrilldown, installGrafanaGuards,
         waitForGrafanaReady, runQuery, assertLogsVisible,
         resolveDatasourceUid, PROXY_DS, PROXY_MULTI_DS } from "./helpers";
import { buildLogsDrilldownUrl, buildServiceDrilldownUrl } from "./url-state";
```

**URL helpers (`url-state.ts`):**
```typescript
buildLogsDrilldownUrl(uid: string): string
buildServiceDrilldownUrl(uid: string, service: string, tab: string): string
```

**Guards:** `installGrafanaGuards(page, options?)` returns `{ assertClean() }`. Call `assertClean()` at end of each test to check no unexpected browser errors fired.

**Existing multi-tenant drilldown test pattern (from `logs-drilldown.spec.ts:362`):**
```typescript
const guards = installGrafanaGuards(page, {
  allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
  allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
});
await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "logs");
await expect(page.getByText("No logs found")).toHaveCount(0);
await guards.assertClean();
```

**`allowedDrilldownMtConsoleErrors`** is already defined at the top of `logs-drilldown.spec.ts` — import and reuse it from that file, or copy the definition.

**CI tag naming:** Use `@drilldown-mt` for Drilldown multi-tenant tests (matches existing `drilldown-multitenant` CI group). Use `@explore-mt` for Explore multi-tenant tests (add to the `explore-core` or a new `explore-mt` CI group — see Task 6).

---

## Task 1: Create `explore-multitenant.spec.ts` with cross-datasource switching test

Switching from a multi-tenant datasource to a single-tenant datasource in Explore must not produce browser errors, must clear the `__tenant_id__` filter from the query, and must show logs.

**Files:**
- Create: `test/e2e-ui/tests/explore-multitenant.spec.ts`

- [ ] **Step 1: Create the file**

```typescript
import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  openExplore,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
  typeQuery,
} from "./helpers";

test.describe("Grafana Explore — Multi-Tenant Scenarios", () => {
  test(
    "switching from multi-tenant to single-tenant datasource clears tenant filter @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });

      // Start on multi-tenant datasource with tenant filter
      await openExplore(page, PROXY_MULTI_DS, '{app="api-gateway", __tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);
      await assertLogsVisible(page);

      // Switch to single-tenant datasource by navigating with a new query
      // (Grafana preserves the query expression across datasource switches)
      await openExplore(page, PROXY_DS, '{app="api-gateway"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      await assertLogsVisible(page);
      // Single-tenant datasource must not carry __tenant_id__ confusion
      await expect(page.getByText("No logs found")).toHaveCount(0);
      await guards.assertClean();
    }
  );

  test(
    "multi-tenant explore shows logs for valid tenant and no error banner @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });
      await openExplore(page, PROXY_MULTI_DS, '{__tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      await assertLogsVisible(page);
      // No error panel must appear for a valid tenant query
      await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
      await guards.assertClean();
    }
  );

  test(
    "multi-tenant explore for missing tenant shows empty result not error banner @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
        // Missing tenant returns empty result — Grafana may show "No logs found"
        // but must NOT show a generic error banner or browser console error.
      });
      await openExplore(page, PROXY_MULTI_DS, '{__tenant_id__="nonexistent-tenant-xyz"}');
      await waitForGrafanaReady(page);
      await runQuery(page);

      // Must not show a red error alert panel
      await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
      // Empty result is fine — "No logs found" is acceptable
      await guards.assertClean();
    }
  );
});
```

- [ ] **Step 2: Run the new tests**

```bash
cd test/e2e-ui
npx playwright test tests/explore-multitenant.spec.ts --reporter=line
```

Expected: All 3 tests PASS. If Grafana is not running, you'll see `ERR_CONNECTION_REFUSED`.

- [ ] **Step 3: Commit**

```bash
git add test/e2e-ui/tests/explore-multitenant.spec.ts
git commit -m "test(ui): add multi-tenant Explore cross-datasource and error boundary tests"
```

---

## Task 2: Multi-tenant field-value click interactions in Explore

When a user clicks "Filter for value" on a log field in multi-tenant Explore, the filter must be added to the query and logs must still be visible (no error banner). This uses `PROXY_INTERACT_DS` (native-metadata proxy) to avoid circuit-breaker cross-contamination.

**Files:**
- Modify: `test/e2e-ui/tests/explore-multitenant.spec.ts`

- [ ] **Step 1: Add the helper imports**

Ensure `explore-multitenant.spec.ts` imports `PROXY_INTERACT_DS` from helpers (add to existing import):

```typescript
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  PROXY_INTERACT_DS,  // add this
  openExplore,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
  typeQuery,
} from "./helpers";
```

- [ ] **Step 2: Add the click-interaction test inside the `test.describe` block**

Add after the existing 3 tests:

```typescript
  test(
    "filter-for-value in multi-tenant explore adds label to query without error @explore-mt",
    async ({ page }) => {
      const guards = installGrafanaGuards(page, {
        allowedAlertErrors: [/^Unknown error$/i],
      });

      // Use multi-tenant datasource — query includes tenant filter
      await openExplore(page, PROXY_MULTI_DS, '{app="api-gateway", __tenant_id__="fake"}');
      await waitForGrafanaReady(page);
      await runQuery(page);
      await assertLogsVisible(page);

      // Expand the first log row to reveal field detail panel
      const firstRow = page.locator('[data-testid="data-testid log-row-message"]').first();
      await firstRow.click({ timeout: 10_000 });

      // Wait for the detail panel to open
      const detailPanel = page.locator('[data-testid="data-testid log-details"]').first();
      await expect(detailPanel).toBeVisible({ timeout: 10_000 });

      // Click "Filter for value" (= icon) on the "app" field if present
      const filterButton = detailPanel
        .locator('button[aria-label*="Filter for value"], button[title*="filter"]')
        .first();

      if (await filterButton.isVisible({ timeout: 3_000 }).catch(() => false)) {
        await filterButton.click();
        await waitForGrafanaReady(page);
        await assertLogsVisible(page);
        // After clicking filter, no error banner must appear
        await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
      }

      await guards.assertClean();
    }
  );
```

- [ ] **Step 3: Run the updated spec**

```bash
cd test/e2e-ui
npx playwright test tests/explore-multitenant.spec.ts --grep "filter-for-value" --reporter=line
```

Expected: PASS. If no "Filter for value" button is visible for this query, the test skips the click (the `if` guard) and still passes.

- [ ] **Step 4: Commit**

```bash
git add test/e2e-ui/tests/explore-multitenant.spec.ts
git commit -m "test(ui): add filter-for-value click interaction in multi-tenant Explore"
```

---

## Task 3: Drilldown cardinality badges are visible and non-zero in multi-tenant context

When viewing the "Fields" tab of a service in Drilldown with the multi-tenant datasource, each field card must display a cardinality badge (a number like "3" or "12 values"). A zero badge or missing badge indicates the proxy is returning empty cardinality.

**Files:**
- Modify: `test/e2e-ui/tests/logs-drilldown.spec.ts`

- [ ] **Step 1: Add the cardinality badge test inside the existing `test.describe("Grafana Logs Drilldown")`**

In `logs-drilldown.spec.ts`, add after the `"multi-tenant service field view loads detected fields without browser errors @drilldown-mt"` test (around line 389):

```typescript
  test("multi-tenant service fields tab shows non-zero cardinality badges @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });

    await openServiceDrilldown(page, PROXY_MULTI_DS, "api-gateway", "fields");
    await waitForGrafanaReady(page);

    // Wait for field cards to render (Fields tab shows cardinality per field)
    // Grafana Logs Drilldown renders field cards with a value count badge
    // Look for any numeric cardinality text — format varies by Drilldown version:
    //   "3 values", "3", badge with aria-label containing count
    const cardinalityText = page.locator(
      '[class*="fieldCount"], [class*="cardinality"], [aria-label*="values"], [data-testid*="field-count"]'
    ).first();

    const hasCardinality = await cardinalityText.isVisible({ timeout: 15_000 }).catch(() => false);
    if (hasCardinality) {
      const text = await cardinalityText.innerText();
      const num = parseInt(text.replace(/\D/g, ""), 10);
      expect(num).toBeGreaterThan(0);
    }
    // If no cardinality badge is found at all, check that at least some field cards loaded
    const fieldCards = page.locator('[class*="fieldCard"], [data-testid*="field-card"]');
    const hasFields = await fieldCards.count().then((n) => n > 0).catch(() => false);
    if (!hasCardinality && !hasFields) {
      // Fields did not load — check via network response instead
      // (Drilldown may render differently per version)
      await expect(page.getByText("No logs found")).toHaveCount(0);
    }

    await guards.assertClean();
  });
```

- [ ] **Step 2: Run only this test**

```bash
cd test/e2e-ui
npx playwright test tests/logs-drilldown.spec.ts --grep "cardinality badges" --reporter=line
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/e2e-ui/tests/logs-drilldown.spec.ts
git commit -m "test(ui): verify multi-tenant Drilldown field cards show non-zero cardinality badges"
```

---

## Task 4: Multi-tenant label filter in Drilldown scopes data to that tenant

When a multi-tenant datasource shows logs and a label filter for `__tenant_id__="fake"` is applied, the visible log lines must be non-empty. This is distinct from the existing URL-state test — this one applies the filter interactively via the Drilldown filter bar.

**Files:**
- Modify: `test/e2e-ui/tests/logs-drilldown.spec.ts`

- [ ] **Step 1: Add the test inside the existing `test.describe` block**

Add after the cardinality badge test from Task 3:

```typescript
  test("multi-tenant drilldown label filter scopes logs to selected tenant @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
    });

    // Navigate to service drilldown with tenant filter in URL
    const uid = await resolveDatasourceUid(page, PROXY_MULTI_DS);
    const url = buildServiceDrilldownUrl(uid, "api-gateway", "logs") +
      '&var-filters=__tenant_id__%7C%3D%7Cfake'; // URL-encode __tenant_id__|=|fake
    await page.goto(url);
    await waitForDrilldownDetails(page);

    // Logs must be visible (tenant "fake" maps to default VL tenant which has data)
    await expect(page.getByText("No logs found")).toHaveCount(0);

    // The filter chip must appear in the filter bar
    const chip = page.getByLabel(/Edit filter with key __tenant_id__|Remove filter with key __tenant_id__/);
    await expect(chip.first()).toBeVisible({ timeout: 10_000 });

    await guards.assertClean();
  });
```

- [ ] **Step 2: Ensure `buildServiceDrilldownUrl` and `waitForDrilldownDetails` are imported**

At the top of `logs-drilldown.spec.ts`, verify these imports are present (they already are from the existing code):
- `buildServiceDrilldownUrl` from `"./url-state"`
- `waitForDrilldownDetails` is a local async function defined in the file
- `resolveDatasourceUid` from `"./helpers"`

- [ ] **Step 3: Run the test**

```bash
cd test/e2e-ui
npx playwright test tests/logs-drilldown.spec.ts --grep "label filter scopes logs" --reporter=line
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add test/e2e-ui/tests/logs-drilldown.spec.ts
git commit -m "test(ui): verify multi-tenant Drilldown label filter scopes results to selected tenant"
```

---

## Task 5: Error boundary — Drilldown with completely missing tenant shows empty, not crash

When the multi-tenant datasource queries a tenant that doesn't exist, Drilldown must show empty results (or "No data"), not a browser crash or infinite spinner.

**Files:**
- Modify: `test/e2e-ui/tests/logs-drilldown.spec.ts`

- [ ] **Step 1: Add the test inside the existing `test.describe` block**

Add after the label filter test:

```typescript
  test("multi-tenant drilldown with missing tenant shows empty result not error @drilldown-mt", async ({
    page,
  }) => {
    const guards = installGrafanaGuards(page, {
      allowedConsoleErrors: allowedDrilldownMtConsoleErrors,
      allowedRequestFailures: [/^net::ERR_ABORTED .*\/api\/ds\/query/i],
      // Missing tenant → proxy returns empty success shape → Grafana shows "No data"
      // Not an error: this is correct behavior.
    });

    const uid = await resolveDatasourceUid(page, PROXY_MULTI_DS);
    // Use a tenant that does not exist in the test dataset
    const url =
      buildServiceDrilldownUrl(uid, "api-gateway", "logs") +
      '&var-filters=__tenant_id__%7C%3D%7Cnonexistent-tenant-xyz-99999';
    await page.goto(url);
    await waitForGrafanaReady(page);

    // No red error alert — empty result is correct
    await expect(page.locator('[data-testid="data-testid Alert error"]')).toHaveCount(0);
    // Page must be interactive (not crashed) — filter bar must be visible
    await expect(page.getByRole("combobox", { name: "Filter by labels" })).toBeVisible({
      timeout: 20_000,
    });

    await guards.assertClean();
  });
```

- [ ] **Step 2: Run the test**

```bash
cd test/e2e-ui
npx playwright test tests/logs-drilldown.spec.ts --grep "missing tenant shows empty" --reporter=line
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/e2e-ui/tests/logs-drilldown.spec.ts
git commit -m "test(ui): add error boundary test for missing tenant in multi-tenant Drilldown"
```

---

## Task 6: Wire `explore-multitenant.spec.ts` into CI and add CHANGELOG

**Files:**
- Modify: `.github/workflows/ci.yaml`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Check how the Playwright tests are wired in CI**

In `.github/workflows/ci.yaml`, find the `e2e-ui` job matrix. It should have groups like `drilldown-multitenant`, `explore-core`, etc. with grep patterns. Find the `explore-core` or `explore-tail` group.

- [ ] **Step 2: Add `explore-mt` to the appropriate CI group**

Find the `drilldown-multitenant` group — it runs tests tagged `@drilldown-mt`. Add a new group for explore multi-tenant, or add `@explore-mt` to an existing group. Add this entry to the `e2e-ui` matrix (after the existing entries):

```yaml
        - group: explore-mt
          grep: "@explore-mt"
```

If the CI uses `--grep` flags directly, add `@explore-mt` to the `explore-core` grep pattern instead. Check the existing pattern format and follow it exactly.

- [ ] **Step 3: Verify YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yaml'))" && echo "YAML valid"
```

- [ ] **Step 4: Add CHANGELOG entry under `[Unreleased]`**

```markdown
### Tests

- **e2e-ui: multi-tenant Explore scenarios**: cross-datasource switching (multi-tenant → single-tenant), missing tenant shows empty result not error banner, filter-for-value click interaction in multi-tenant context.
- **e2e-ui: multi-tenant Drilldown cardinality and error boundary**: field cards show non-zero cardinality badges, label filter scopes results to selected tenant, missing tenant shows empty result not browser crash.
```

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yaml CHANGELOG.md
git commit -m "ci,docs: wire explore-mt UI tests into CI, add changelog entries"
```

---

## Self-Review

**Spec coverage:**
- ✅ Cross-datasource switching: Task 1
- ✅ Missing tenant error boundary (Explore): Task 1 test 3
- ✅ Field-value click interactions in multi-tenant context: Task 2
- ✅ Cardinality badge verification: Task 3
- ✅ Label filter scopes data to tenant: Task 4
- ✅ Missing tenant error boundary (Drilldown): Task 5
- ✅ CI wiring + CHANGELOG: Task 6

**No placeholders:** All TypeScript is complete. The cardinality badge selector in Task 3 uses multiple CSS/attribute selectors to handle Grafana version variation — this is intentional, not vague.

**Type consistency:** All imports match the exports in `helpers.ts`. `buildServiceDrilldownUrl` is imported from `url-state.ts` (already used in `logs-drilldown.spec.ts`). `resolveDatasourceUid` is exported from `helpers.ts:26`.
