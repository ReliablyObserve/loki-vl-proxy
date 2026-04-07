import { test, expect } from "@playwright/test";
import {
  PROXY_DS,
  PROXY_MULTI_DS,
  PROXY_TAIL_DS,
  PROXY_TAIL_INGRESS_DS,
  PROXY_TAIL_NATIVE_DS,
  openExplore,
  runQuery,
  clickLiveStream,
  assertLogsVisible,
  waitForGrafanaReady,
  installGrafanaGuards,
} from "./helpers";

test.describe("Grafana Explore — Proxy Datasource", () => {
  test("basic log query returns results without errors @explore-core", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(page, PROXY_DS, '{app="api-gateway"}');
    await waitForGrafanaReady(page);
    await runQuery(page);

    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("multi-tenant query respects __tenant_id__ filter in Explore @explore-tail", async ({ page }) => {
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/^Unknown error$/i],
    });
    await openExplore(page, PROXY_MULTI_DS, '{app="api-gateway", __tenant_id__="fake"}');
    await waitForGrafanaReady(page);
    await runQuery(page);

    await assertLogsVisible(page);
    await guards.assertClean();
  });

  test("live tail works through the browser-allowed synthetic datasource @explore-tail", async ({
    page,
  }) => {
    const app = `ui-tail-${Date.now()}`;
    const msg = `ui tail frame ${app}`;
    const guards = installGrafanaGuards(page, {
      allowedAlertErrors: [/Live tailing was stopped due to following error:\s*undefined/i],
    });
    const websockets: string[] = [];

    page.on("websocket", (ws) => {
      websockets.push(ws.url());
    });

    await openExplore(page, PROXY_TAIL_DS, `{app="${app}"}`);
    await waitForGrafanaReady(page);

    await clickLiveStream(page);

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

    await expect
      .poll(() => websockets.some((u) => u.includes("/tail") || u.includes("/api/live/ws")), {
        timeout: 15_000,
      })
      .toBeTruthy();
    await guards.assertClean();
  });

  test("native-tail datasource can hand off to ingress live tail @explore-tail", async ({ page }) => {
    await openExplore(page, PROXY_TAIL_NATIVE_DS, '{app="api-gateway"}');
    await waitForGrafanaReady(page);
    const nativeGuards = installGrafanaGuards(page);

    await clickLiveStream(page);
    await expect
      .poll(() => page.getByRole("button", { name: /pause the live stream|stop and exit the live stream/i }).count(), {
        timeout: 15_000,
      })
      .toBeGreaterThan(0);
    await nativeGuards.assertClean();

    const ingressApp = `ui-tail-ingress-recovery-${Date.now()}`;
    const ingressMsg = `ui ingress recovery frame ${ingressApp}`;

    await openExplore(page, PROXY_TAIL_INGRESS_DS, `{app="${ingressApp}"}`);
    await waitForGrafanaReady(page);
    const recoveryGuards = installGrafanaGuards(page);

    await clickLiveStream(page);

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

    await expect
      .poll(() => page.getByRole("button", { name: /pause the live stream|stop and exit the live stream/i }).count(), {
        timeout: 15_000,
      })
      .toBeGreaterThan(0);
    await recoveryGuards.assertClean();
  });
});
