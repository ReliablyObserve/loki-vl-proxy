"""Keep shipped cache panels aligned with the metric labels the proxy emits."""

import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
DASHBOARDS = (
    ROOT / "dashboard/loki-vl-proxy.json",
    ROOT / "charts/loki-vl-proxy/dashboards/loki-vl-proxy.json",
)
METRICS_SOURCE = ROOT / "internal/metrics/metrics.go"


class DashboardCacheTierTests(unittest.TestCase):
    def test_cache_panels_use_exported_tier_labels(self):
        source = METRICS_SOURCE.read_text()
        emitted = {}
        for kind in ("hits", "misses"):
            emitted[kind] = set(
                re.findall(
                    rf'fmt\.Fprintf\(sb, "loki_vl_proxy_cache_tier_{kind}_total'
                    rf'\{{tier=%q\}} %d\\n", "([^"]+)"',
                    source,
                )
            )
            self.assertEqual(emitted[kind], {"l0", "l1_memory", "l2_disk", "l3_peer"})

        evictions = set(
            re.findall(
                r'fmt\.Fprintf\(sb, "loki_vl_proxy_cache_tier_evictions_total'
                r'\{tier=%q\} %d\\n", "([^"]+)"',
                source,
            )
        )
        self.assertEqual(evictions, {"l1_memory", "l2_disk"})

        stored = {}
        for kind in ("objects", "bytes"):
            stored[kind] = set(
                re.findall(
                    rf'fmt\.Fprintf\(sb, "loki_vl_proxy_cache_{kind}'
                    rf'\{{tier=%q\}} %d\\n", "([^"]+)"',
                    source,
                )
            )
            self.assertEqual(stored[kind], {"l0", "l1_memory", "l2_disk"} if kind == "objects" else {"l1_memory", "l2_disk"})

        for path in DASHBOARDS:
            with self.subTest(path=path):
                dashboard = json.loads(path.read_text())
                panels = {panel["id"]: panel for panel in dashboard["panels"]}
                for panel_id, kind in ((45, "hits"), (46, "misses")):
                    panel = panels[panel_id]
                    expressions = [target["expr"] for target in panel["targets"]]
                    selected = {
                        match.group(1)
                        for expression in expressions
                        if (match := re.search(
                            rf'loki_vl_proxy_cache_tier_{kind}_total\{{tier="([^"]+)"\}}',
                            expression,
                        ))
                    }
                    self.assertEqual(selected, emitted[kind])
                    if panel_id == 45:
                        self.assertEqual(len(expressions), len(emitted[kind]))
                        self.assertEqual({target["refId"] for target in panel["targets"]}, {"A", "B", "C", "D"})
                    else:
                        eviction_exprs = [target["expr"] for target in panel["targets"] if "cache_tier_evictions_total" in target["expr"]]
                        selected_evictions = {
                            match.group(1)
                            for expression in eviction_exprs
                            if (match := re.search(r'loki_vl_proxy_cache_tier_evictions_total\{tier="([^"]+)"\}', expression))
                        }
                        self.assertEqual(selected_evictions, evictions)
                        self.assertEqual(len(expressions), len(emitted[kind]) + len(evictions))
                        self.assertEqual({target["refId"] for target in panel["targets"]}, {"A", "B", "C", "D", "E", "F"})
                    self.assertIn("hot-key", panel["description"])

                size_panel = panels[47]
                for metric, tiers in stored.items():
                    expressions = [
                        target["expr"]
                        for target in size_panel["targets"]
                        if f"loki_vl_proxy_cache_{metric}" in target["expr"]
                    ]
                    selected = {
                        match.group(1)
                        for expression in expressions
                        if (match := re.search(
                            rf'loki_vl_proxy_cache_{metric}\{{tier="([^"]+)"\}}',
                            expression,
                        ))
                    }
                    self.assertEqual(selected, tiers)
                    self.assertEqual(len(expressions), len(tiers))

                coalescing_panel = panels[79]
                self.assertIn("loki_vl_proxy_coalesced_wait_nanoseconds_total", coalescing_panel["targets"][0]["expr"])
                self.assertIn("loki_vl_proxy_coalesced_wait_count_total", coalescing_panel["targets"][0]["expr"])

        self.assertEqual(DASHBOARDS[0].read_bytes(), DASHBOARDS[1].read_bytes())


if __name__ == "__main__":
    unittest.main()
