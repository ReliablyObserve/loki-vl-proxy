import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  PROXY_TAIL_DS,
  PROXY_TAIL_INGRESS_DS,
  PROXY_TAIL_NATIVE_DS,
  openExplore,
  typeQuery,
  runQuery,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

test.describe("Grafana Explore — Proxy Datasource", () => {
  test("basic log query returns results without errors @explore-core", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_DS);
    await waitForGrafanaReady(page);

    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("multi-tenant query respects __tenant_id__ filter in Explore @explore-tail", async ({ page }) => {
    const guards = installGrafanaGuards(page);
    await openExplore(page, PROXY_MULTI_DS);
    await waitForGrafanaReady(page);

    await typeQuery(page, '{app="api-gateway", __tenant_id__="fake"}');
    await runQuery(page);

    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("live tail works through the browser-allowed synthetic datasource @explore-tail", async ({
    page,
  }) => {
    const app = `ui-tail-${Date.now()}`;
    const msg = `ui tail frame ${app}`;
    const guards = installGrafanaGuards(page);
    const websockets: string[] = [];

    page.on("websocket", (ws) => {
      websockets.push(ws.url());
    });

    await openExplore(page, PROXY_TAIL_DS);
    await waitForGrafanaReady(page);
    await typeQuery(page, `{app="${app}"}`);

    const recoveryLiveButton = page.getByRole("button", { name: /live/i }).first();
    await expect(recoveryLiveButton).toBeVisible({ timeout: 15_000 });
    await recoveryLiveButton.click();

    const payload = JSON.stringify({
      _time: new Date().toISOString(),
      _msg: msg,
      app,
      env: "test",
      level: "info",
    });
    const pushResp = await page.request.post(
      "http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,env,level",
      {
        headers: { "Content-Type": "application/stream+json" },
        data: `${payload}\n`,
      }
    );
    expect(pushResp.ok()).toBeTruthy();

    await expect(page.getByText(msg, { exact: false })).toBeVisible({
      timeout: 15_000,
    });
    await guards.assertClean();
    expect(websockets.some((u) => u.includes("/tail") || u.includes("/api/live/ws"))).toBeTruthy();
  });

  test("native-tail failure can recover through ingress live tail @explore-tail", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedRequestFailures: [/\/tail\b/i, /\/api\/live\/ws/i],
      allowedResponseErrors: [/\/tail\b/i, /\/api\/live\/ws/i],
    });

    await openExplore(page, PROXY_TAIL_NATIVE_DS);
    await waitForGrafanaReady(page);
    await typeQuery(page, '{app="api-gateway"}');

    const liveButton = page.getByRole("button", { name: /live/i }).first();
    await expect(liveButton).toBeVisible({ timeout: 15_000 });
    await liveButton.click();
    await expect(page.getByText(/error|failed|unable/i).first()).toBeVisible({
      timeout: 15_000,
    });

    const ingressApp = `ui-tail-ingress-recovery-${Date.now()}`;
    const ingressMsg = `ui ingress recovery frame ${ingressApp}`;

    await openExplore(page, PROXY_TAIL_INGRESS_DS);
    await waitForGrafanaReady(page);
    await typeQuery(page, `{app="${ingressApp}"}`);

    const ingressLiveButton = page.getByRole("button", { name: /live/i }).first();
    await expect(ingressLiveButton).toBeVisible({ timeout: 15_000 });
    await ingressLiveButton.click();

    const pushResp = await page.request.post(
      "http://127.0.0.1:9428/insert/jsonline?_stream_fields=app,env,level",
      {
        headers: { "Content-Type": "application/stream+json" },
        data: `${JSON.stringify({
          _time: new Date().toISOString(),
          _msg: ingressMsg,
          app: ingressApp,
          env: "test",
          level: "info",
        })}\n`,
      }
    );
    expect(pushResp.ok()).toBeTruthy();

    await expect(page.getByText(ingressMsg, { exact: false })).toBeVisible({
      timeout: 15_000,
    });
    await guards.assertClean();
  });
});
