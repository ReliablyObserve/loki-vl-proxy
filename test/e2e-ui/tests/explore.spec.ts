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
  assertNoErrors,
  assertLogsVisible,
  waitForGrafanaReady,
  collectLokiErrors,
} from "./helpers";

test.describe("Grafana Explore — Proxy Datasource", () => {
  test.beforeEach(async ({ page }) => {
    await openExplore(page, PROXY_DS);
    await waitForGrafanaReady(page);
  });

  test("basic log query returns results without errors @explore-core", async ({ page }) => {
    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway"}');
    await runQuery(page);

    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
  });

  test("multi-tenant query respects __tenant_id__ filter in Explore @explore-tail", async ({ page }) => {
    await openExplore(page, PROXY_MULTI_DS);
    await waitForGrafanaReady(page);

    const errors = collectLokiErrors(page);
    await typeQuery(page, '{app="api-gateway", __tenant_id__="fake"}');
    await runQuery(page);

    await assertNoErrors(page);
    await assertLogsVisible(page);
    expect(errors).toHaveLength(0);
  });

  test("live tail works through the browser-allowed synthetic datasource @explore-tail", async ({
    page,
  }) => {
    const app = `ui-tail-${Date.now()}`;
    const msg = `ui tail frame ${app}`;
    const errors = collectLokiErrors(page);
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
    await assertNoErrors(page);
    expect(errors).toHaveLength(0);
    expect(websockets.some((u) => u.includes("/tail") || u.includes("/api/live/ws"))).toBeTruthy();
  });

  test("native-tail failure can recover through ingress live tail @explore-tail", async ({ page }) => {
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
    await assertNoErrors(page);
  });
});
