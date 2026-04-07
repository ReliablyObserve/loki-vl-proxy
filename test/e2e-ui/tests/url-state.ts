export const EXPLORE_URL = "/explore";
export const DRILLDOWN_URL = "/a/grafana-lokiexplore-app/explore";

type DrilldownView = "logs" | "fields";

function baseDrilldownState(datasourceUid: string): Record<string, string> {
  return {
    patterns: "[]",
    from: "now-2h",
    to: "now",
    timezone: "browser",
    "var-lineFormat": "",
    "var-ds": datasourceUid,
    "var-filters": "",
    "var-fields": "",
    "var-levels": "",
    "var-metadata": "",
    "var-jsonFields": "",
    "var-all-fields": "",
    "var-patterns": "",
    "var-lineFilterV2": "",
    "var-lineFilters": "",
  };
}

export function buildExploreUrl(datasourceUid: string): string {
  const paneState = {
    A: {
      datasource: datasourceUid,
      queries: [
        {
          refId: "A",
          expr: "",
          queryType: "range",
          datasource: {
            type: "loki",
            uid: datasourceUid,
          },
          editorMode: "builder",
          direction: "backward",
        },
      ],
      range: {
        from: "now-1h",
        to: "now",
      },
      compact: false,
    },
  };

  const params = new URLSearchParams({
    schemaVersion: "1",
    panes: JSON.stringify(paneState),
    orgId: "1",
  });
  return `${EXPLORE_URL}?${params.toString()}`;
}

export function buildLogsDrilldownUrl(
  datasourceUid: string,
  overrides: Record<string, string> = {}
): string {
  const params = new URLSearchParams({
    ...baseDrilldownState(datasourceUid),
    "var-primary_label": "service_name|=~|.+",
    ...overrides,
  });
  return `${DRILLDOWN_URL}?${params.toString()}`;
}

export function buildServiceDrilldownUrl(
  datasourceUid: string,
  serviceName: string,
  view: DrilldownView = "logs",
  overrides: Record<string, string> = {}
): string {
  const params = new URLSearchParams({
    ...baseDrilldownState(datasourceUid),
    "var-filters": `service_name|=|${serviceName}`,
    displayedFields: "[]",
    urlColumns: "[]",
    ...overrides,
  });

  return `${DRILLDOWN_URL}/service/${encodeURIComponent(serviceName)}/${view}?${params.toString()}`;
}
