import { test, expect } from "@playwright/test";
import { PROXY_DS, PROXY_INTERACT_DS, LOKI_DS, resolveDatasourceUid, runQuery, waitForGrafanaReady, installGrafanaGuards } from "./helpers";
import { buildExploreUrl } from "./url-state";

// Separate markers in one old cache bucket: a successful empty page or a stale
// first response must never count as a successful compatibility test.
for (const datasource of [PROXY_DS, PROXY_INTERACT_DS, LOKI_DS]) {
  test(`exact windows and formatted rows stay visible: ${datasource} @explore-core`, async ({ page }) => {
    const service = `security-ui-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const stamp = Math.floor((Date.now() - 180_000) / 300_000) * 300_000 + 1_000;
    const markers = ["first-window-marker", "second-window-marker"];
    const labels = { service_name: service, level: "info" };
    const vl = process.env.VL_URL || "http://127.0.0.1:19428";
    const loki = process.env.LOKI_URL || "http://127.0.0.1:13101";
    for (const [i, marker] of markers.entries()) {
      const ms = stamp + i * 2_000;
      const vlResponse = await page.request.post(`${vl}/insert/jsonline?_stream_fields=service_name,level`, {
        headers: { "Content-Type": "application/stream+json" },
        data: JSON.stringify({ ...labels, _time: new Date(ms).toISOString(), _msg: marker }) + "\n",
      });
      expect(vlResponse.ok()).toBeTruthy();
      const lokiResponse = await page.request.post(`${loki}/loki/api/v1/push`, {
        data: { streams: [{ stream: labels, values: [[String(BigInt(ms) * 1_000_000n), marker]] }] },
      });
      expect(lokiResponse.status()).toBe(204);
    }
    expect((await page.request.post(`${vl}/internal/force_flush`)).ok()).toBeTruthy();
    const uid = await resolveDatasourceUid(page, datasource);
    const guards = installGrafanaGuards(page);
    for (const formatted of [false, true]) {
      for (const i of [0, 1, 0]) {
        const query = `{service_name="${service}"}` + (formatted ? ' | line_format `{{printf "%s" .service_name}}`' : "");
        const target = new URL(buildExploreUrl(uid, query), "http://grafana.invalid");
        const panes = JSON.parse(target.searchParams.get("panes")!);
        panes.A.range = { from: new Date(stamp + i * 2_000).toISOString(), to: new Date(stamp + i * 2_000 + 1_000).toISOString() };
        target.searchParams.set("panes", JSON.stringify(panes));
        await page.goto(target.pathname + target.search);
        await waitForGrafanaReady(page);
        await expect(page.getByRole("button", { name: /run query/i }).first()).toBeVisible();
        await runQuery(page);
        const rows = page.locator('[data-testid="logRows"]').first();
        await expect(rows).toBeVisible();
        await expect(rows).toContainText(formatted ? service : markers[i]);
        await expect(rows).not.toContainText(markers[1 - i]);
        await expect(rows).not.toContainText("{{printf");
      }
    }
    await guards.assertClean();
  });
}
