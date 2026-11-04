"""Unit tests for conformance/scripts/sync_vl_surface.py (what the proxy emits) and link_ast.py (the map)."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import link_ast  # noqa: E402
import sync_vl_surface as surface  # noqa: E402

AST = '''package logsql

func (p PipeUnion) String() string { return "| union (" + p.Inner + ")" }

func (c CountUniq) String() string { return "count_uniq(" + c.Field + ")" }

func (Count) String() string { return "count()" }
'''


def tree(files):
    root = tempfile.mkdtemp()
    for path, text in {"internal/logsql/ast.go": AST, **files}.items():
        target = Path(root, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
    return root


class GoLiteralsTest(unittest.TestCase):
    def test_comments_runes_and_raw_strings_do_not_shift_the_pairing(self):
        text = 'x := \'"\' // a "quoted" comment\ny := "| sort by (v)" + `raw "q"` /* "c" */ + "z"\n'
        self.assertEqual(surface.go_literals(text), ["| sort by (v)", 'raw "q"', "z"])

    def test_escaped_quotes_stay_inside_the_literal(self):
        self.assertEqual(surface.go_literals(r'a := "say \"hi\" | union"'), [r'say \"hi\" | union'])


class ProxyUsesTest(unittest.TestCase):
    def test_ast_definitions_alone_are_not_emission(self):
        root = tree({"internal/proxy/a.go": 'package proxy\nvar s = "| stats count()"\n'})
        self.assertEqual(surface.proxy_uses("pipe", "union", root), [])
        self.assertEqual(surface.proxy_uses("stats", "count_uniq", root), [])

    def test_building_an_ast_type_is_emission(self):
        root = tree({"internal/translator/t.go": "package translator\nvar p = logsql.PipeUnion{Inner: q}\n"})
        self.assertEqual(surface.proxy_uses("pipe", "union", root), ["internal/translator/t.go"])

    def test_a_translator_name_table_value_is_emission(self):
        root = tree({"internal/translator/t.go": 'package translator\nvar m = map[string]string{"avg_over_time": "avg"}\n'
                                                 'func f() string { row := "x"; row = "row_min"; return "stddev" }\n'})
        for name in ("avg", "row_min", "stddev"):
            self.assertEqual(surface.proxy_uses("stats", name, root), ["internal/translator/t.go"], name)

    def test_a_map_key_a_comparison_or_a_proxy_name_is_not(self):
        root = tree({"internal/translator/t.go": 'package translator\nvar m = map[string]string{"rate": "count()"}\n'
                                                 'func f(x string) bool { return x == "median" }\n',
                     "internal/proxy/p.go": 'package proxy\nfunc g() { fn := "rate"; _ = fn }\n'})
        self.assertEqual(surface.proxy_uses("stats", "rate", root), [])
        self.assertEqual(surface.proxy_uses("stats", "median", root), [])

    def test_logql_text_and_regular_expressions_are_not_emission(self):
        root = tree({"internal/proxy/p.go": 'package proxy\nvar e = "use rate({app=\\"x\\"}[5m])"\n'
                                            'var r = regexp.MustCompile(`\\|\\s*(json|unpack)\\b`)\n'
                                            'var prefixes = []string{"rate("}\n'})
        self.assertEqual(surface.proxy_uses("stats", "rate", root), [])
        self.assertEqual(surface.proxy_uses("pipe", "unpack", root), [])

    def test_a_call_built_from_its_name_is_emission(self):
        root = tree({"internal/translator/t.go": 'package translator\nfunc q(phi, f string) string { return "quantile(" + phi + ", " + f + ")" }\n'})
        self.assertEqual(surface.proxy_uses("stats", "quantile", root), ["internal/translator/t.go"])


class MapTest(unittest.TestCase):
    def test_series_count_and_ranking_map_to_real_constructs(self):
        self.assertEqual(link_ast.MAP["count"], ["stats-count"])
        self.assertEqual(link_ast.MAP["topk"], ["pipe-sort"])
        self.assertEqual(link_ast.MAP["bottomk"], ["pipe-sort"])
        self.assertEqual(link_ast.MAP["unpack"], ["pipe-unpack-json"])
        # Implementation files of VictoriaLogs, not LogsQL constructs.
        self.assertFalse({"pipe-sort-topk", "pipe-unpack", "pipe-top"} & {c for v in link_ast.MAP.values() for c in v})

    def test_partial_building_blocks_say_what_they_cover_and_syntax_is_not_a_gap(self):
        for op in ("+", "/", "==", ">", "or", "and"):
            self.assertIn(op, link_ast.MAP)
            self.assertTrue(link_ast.PARTIAL[op])
        self.assertEqual(set(link_ast.NOT_APPLICABLE), {"|", "offset"})
        self.assertEqual(link_ast.version_key("v1.49.0") > link_ast.version_key("v1.40.0"), True)


if __name__ == "__main__":
    unittest.main()
