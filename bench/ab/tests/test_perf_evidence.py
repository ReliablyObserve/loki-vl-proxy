"""Unit tests for conformance/scripts/perf_evidence.py (measurement flags) and gaps.py (perf section).

They live here because CI discovers bench/ab/tests; the scripts are imported from conformance/scripts.
"""
import copy
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "conformance", "scripts"))
import gaps  # noqa: E402
import perf_evidence as pe  # noqa: E402


def summary(timing="warm", isolated_windows=True):
    s = {"label": "t", "date": "2026-10-19", "baseline": "main", "candidate": "branch", "reference": "loki",
         "timing": timing, "valid": True}
    if isolated_windows:
        s["isolated_windows"] = True  # pr_smoke marks the sets it ran on their own windows
    return s



def row(branch_cold=1.0, loki_cold=0.1, branch_warm=0.5, loki_warm=0.05, **extra):
    r = {"shape": "s", "range": "1h", "verdict": "same", "parity_vs_reference": "same",
         "cold": {"main": 1.0, "branch": branch_cold, "loki": loki_cold},
         "p50": {"main": 0.5, "branch": branch_warm, "loki": loki_warm},
         "status": {"main": "200", "branch": "200", "loki": "200"}}
    r.update(extra)
    return r


class MeasurementTest(unittest.TestCase):
    def test_old_row_keeps_its_cold_flag_and_has_no_warm_or_refresh_flag(self):
        m = pe.measurement(summary(isolated_windows=False), row())
        self.assertTrue(m["slower_than_loki_cold"])
        self.assertFalse(m["loki_cache_served"])
        for key in ("slower_than_loki_warm", "slower_than_loki_refresh", "loki_cold_cache",
                    "refresh_p50", "loki_refresh_p50"):
            self.assertNotIn(key, m)  # unknown, never false
        self.assertEqual((m["warm_p50"], m["loki_warm_p50"]), (0.5, 0.05))  # recorded numbers still show

    def test_warm_flag_for_a_run_with_per_set_windows(self):
        m = pe.measurement(summary(), row())
        self.assertTrue(m["slower_than_loki_warm"])
        self.assertNotIn("loki_cold_nocache", m)

    def test_shared_windows_run_has_no_warm_or_refresh_flag(self):
        # daily runs' sets share windows: an earlier set may have filled Loki's cache
        m = pe.measurement(summary(isolated_windows=False),
                           row(refresh={"branch": 0.4, "loki": 0.05}, refresh_status={"branch": "200", "loki": "200"}))
        self.assertNotIn("slower_than_loki_warm", m)
        self.assertNotIn("slower_than_loki_refresh", m)
        self.assertTrue(m["slower_than_loki_cold"])

    def test_same_noise_rules_for_warm_as_for_cold(self):
        def warm(**kw):
            return pe.measurement(summary(), row(**kw))["slower_than_loki_warm"]
        self.assertFalse(warm(branch_warm=0.056, loki_warm=0.05))
        self.assertFalse(warm(branch_warm=0.06, loki_warm=0.02))  # < 50 ms
        self.assertFalse(warm(branch_warm=0.5, loki_warm=0.45))  # < 25%
        self.assertTrue(warm(branch_warm=0.5, loki_warm=0.2))

    def test_stale_heuristic_still_marks_the_row_and_excludes_both_flags(self):
        m = pe.measurement(summary(), row(stale=["loki"]))
        self.assertTrue(m["loki_cache_served"])
        self.assertFalse(m["slower_than_loki_cold"])
        self.assertFalse(m["slower_than_loki_warm"])

    def test_lokis_own_counters_mark_the_row_and_exclude_both_flags(self):
        m = pe.measurement(summary(), row(cold_cache_served=["loki"],
                                          cache={"loki": {"cold": {"result": {"entriesFound": 1}}, "warm": None}}))
        self.assertTrue(m["loki_cache_served"])
        self.assertFalse(m["slower_than_loki_cold"] or m["slower_than_loki_warm"])
        self.assertEqual(m["loki_cold_cache"], {"result": {"entriesFound": 1}})

    def test_a_reused_loki_cold_is_carried_and_the_row_stays_comparable(self):
        origin = {"set": "json-filter-pushdown", "shape": "D volume, no filters"}
        m = pe.measurement(summary(), row(loki_cold=0.1, loki_cold_reused_from=origin))
        self.assertEqual(m["loki_cold_reused_from"], origin)
        self.assertFalse(m["loki_cache_served"])
        self.assertTrue(m["slower_than_loki_cold"])
        self.assertNotIn("loki_cold_reused_from", pe.measurement(summary(), row()))

    def test_computed_cold_with_counters_recorded(self):
        m = pe.measurement(summary(), row(cache={"loki": {"cold": {"result": {"entriesFound": 0}}}}))
        self.assertFalse(m["loki_cache_served"])
        self.assertTrue(m["slower_than_loki_cold"])

    def test_cold_only_summary_has_no_warm_timing(self):
        self.assertFalse(pe.measurement(summary("cold"), row())["slower_than_loki_warm"])

    def test_refresh_is_compared_when_present_in_an_isolated_run(self):
        slow = pe.measurement(summary(), row(refresh={"branch": 0.4, "loki": 0.05},
                                             refresh_status={"branch": "200", "loki": "200"}))
        self.assertTrue(slow["slower_than_loki_refresh"])
        self.assertEqual((slow["refresh_p50"], slow["loki_refresh_p50"]), (0.4, 0.05))
        fine = pe.measurement(summary(), row(refresh={"branch": 0.06, "loki": 0.05},
                                             refresh_status={"branch": "200", "loki": "200"}))
        self.assertIs(fine["slower_than_loki_refresh"], False)
        failed = pe.measurement(summary(), row(refresh={"branch": 0.4, "loki": 0.05},
                                               refresh_status={"branch": "502", "loki": "200"}))
        self.assertIs(failed["slower_than_loki_refresh"], False)

    def test_error_status_is_not_a_slow_answer(self):
        m = pe.measurement(summary(), row(status={"main": "200", "branch": "502", "loki": "200"}))
        self.assertFalse(m["slower_than_loki_cold"] or m["slower_than_loki_warm"])


def evidence(isolated=False, **flags):
    base = pe.measurement(summary(isolated_windows=isolated), row())
    return {"parser-json": [dict(base, set="x", shape="s", range="1h", **flags)],
            "loki_api_v1_query_range": [dict(base, set="x", shape="s", range="1h")]}


class GapsTest(unittest.TestCase):
    def test_cache_served_rows_are_left_out(self):
        ev = evidence(loki_cache_served=True)
        self.assertEqual(gaps.slower_rows(ev, "slower_than_loki_cold"), {})
        self.assertIn("None measured.", "\n".join(gaps.perf_section(ev)))

    def test_generic_endpoint_items_are_skipped(self):
        self.assertEqual(list(gaps.slower_rows(evidence(), "slower_than_loki_cold")), ["parser-json"])

    def test_only_columns_are_added_to_the_first_load_section(self):
        lines = gaps.perf_section(evidence())
        self.assertEqual([l for l in lines if l.startswith("## ")], ["## Slower than Loki on first load"])
        self.assertIn("| registry item | slower shape×range | worst proxy cold | Loki cold there | proxy warm there "
                      "| Loki warm there | also slower warm |", lines)

    def test_old_rows_show_warm_next_to_cold_and_unknown_flags(self):
        text = "\n".join(gaps.perf_section(evidence()))
        self.assertIn("| `parser-json` | 1 | 1.00s (s, 1h) | 0.10s | 0.50s | 0.05s | — |", text)

    def test_isolated_rows_count_warm_gaps(self):
        text = "\n".join(gaps.perf_section(evidence(isolated=True)))
        self.assertIn("| `parser-json` | 1 | 1.00s (s, 1h) | 0.10s | 0.50s | 0.05s | 1 |", text)

    def test_does_not_mutate_the_evidence(self):
        ev = evidence()
        before = copy.deepcopy(ev)
        gaps.perf_section(ev)
        self.assertEqual(ev, before)


if __name__ == "__main__":
    unittest.main()
