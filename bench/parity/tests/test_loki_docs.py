"""The public-documentation sources of the Loki facts: corpus.py's documentation examples and sync_loki.py's route
listing and recorded-answer methods. No Loki source is read by either."""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "conformance", "scripts"))
import corpus  # noqa: E402
import sync_loki  # noqa: E402


class DocsExamplesTest(unittest.TestCase):
    def test_code_blocks_give_whole_queries(self):
        page = ('<p>count_over_time({job="x"}[5m]) in prose</p>'
                '<pre><code>count_over_time({job="mysql"}[5m])\n'
                'sum by (host) (rate({job="mysql"}\n  |= "error" [1m]))\n'
                'not a query\n'
                '{app="a"} | json # a comment\n</code></pre>'
                '<pre>&lt;aggr-op&gt;([parameter,] &lt;unwrapped-range&gt;)</pre>')
        self.assertEqual(corpus.docs_examples(page), [
            'count_over_time({job="mysql"}[5m])',
            'sum by (host) (rate({job="mysql"} |= "error" [1m]))',
            '{app="a"} | json',
        ])


class RoutesTest(unittest.TestCase):
    def test_reference_routes(self):
        page = ('<li><code>GET /loki/api/v1/label/&lt;name&gt;/values</code></li>'
                '<li><code>POST /loki/api/v1/push</code></li><p>GET /ready</p>'
                '<pre>GET /loki/api/v1/series\nPOST /loki/api/v1/series</pre>')
        self.assertEqual(sync_loki.documented_routes(page), {
            "/loki/api/v1/label/{name}/values": {"GET"}, "/loki/api/v1/push": {"POST"},
            "/loki/api/v1/series": {"GET", "POST"},
        })

    def test_unconfirmed_methods_are_kept_until_a_live_probe(self):
        page = "<code>GET /loki/api/v1/patterns</code>"
        previous = [{"path": "/loki/api/v1/patterns", "methods": ["GET", "POST"]},
                    {"path": "/loki/api/v1/delete", "methods": ["PUT"]}]
        sync_loki.fetch = lambda url: page
        doc = sync_loki.build("v3.7.7", "v3.7.x", "", "/nonexistent", previous, {})
        patterns = next(e for e in doc["endpoints"] if e["path"] == "/loki/api/v1/patterns")
        self.assertEqual(patterns["sources"], {"GET": ["docs"], "POST": ["unconfirmed"]})
        self.assertEqual(doc["unconfirmed"], ["POST /loki/api/v1/patterns", "PUT /loki/api/v1/delete"])
        live = {"/loki/api/v1/patterns": {"GET": 400, "POST": 400, "PUT": 405, "DELETE": 405},
                "/loki/api/v1/delete": {"GET": 404, "POST": 404, "PUT": 404, "DELETE": 404}}
        doc = sync_loki.build("v3.7.7", "v3.7.x", "", "/nonexistent", previous, live)
        paths = {e["path"]: e for e in doc["endpoints"]}
        self.assertEqual(paths["/loki/api/v1/patterns"]["sources"], {"GET": ["docs", "live:400"], "POST": ["live:400"]})
        self.assertNotIn("/loki/api/v1/delete", paths)
        self.assertEqual(doc["unconfirmed"], [])


if __name__ == "__main__":
    unittest.main()
