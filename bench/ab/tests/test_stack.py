"""Unit tests for stack.py: the seed's count query."""
import os
import re
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import stack  # noqa: E402


class CountQueryTest(unittest.TestCase):
    def test_every_poll_sends_a_new_query_string(self):
        # Loki caches query_range answers by query string and ignores no-cache: a repeated
        # string would return the first poll's partial counts forever.
        self.assertNotEqual(stack.count_query(600), stack.count_query(600))

    def test_the_filter_keeps_every_line_of_the_selector(self):
        q = stack.count_query(600)
        self.assertRegex(q, r'^sum\(count_over_time\(\{env="production"\} != "seed-count-\d+" \[600s\]\)\)$')
        self.assertTrue(q.startswith("sum(count_over_time(" + stack.SELECTOR + " "))
        self.assertIsNotNone(re.search(r"\[600s\]", q))


if __name__ == "__main__":
    unittest.main()


def _loki_command(path):
    """The `command:` list of the loki service in a compose file, as written."""
    lines, out, in_loki, in_cmd = open(path).read().splitlines(), [], False, False
    for line in lines:
        if re.match(r"^  \S", line):
            in_loki = line.strip() == "loki:"
            in_cmd = False
            continue
        if in_loki and re.match(r"^    command:", line):
            in_cmd = True
            continue
        if in_cmd:
            m = re.match(r"^      - (\S.*)$", line)
            if m:
                out.append(m.group(1))
            elif re.match(r"^    \S", line):
                in_cmd = False
    return out


class LokiUncachedOverlayTest(unittest.TestCase):
    CACHES = ["-querier.cache-results=false", "-querier.cache-index-stats-results=false",
              "-querier.cache-volume-results=false", "-querier.cache-instant-metric-results=false",
              "-querier.cache-series-results=false", "-querier.cache-label-results=false"]

    def test_overlay_is_the_base_command_with_every_results_cache_off(self):
        base = _loki_command(os.path.join(stack.ROOT, stack.BASE_COMPOSE))
        uncached = _loki_command(os.path.join(stack.ROOT, stack.LOKI_UNCACHED_OVERLAY))
        self.assertTrue(base, "the base stack's loki command was not found")
        self.assertEqual(uncached, base + self.CACHES)

    def test_flag_selects_the_overlay(self):
        import argparse
        ap = argparse.ArgumentParser()
        stack.stack_args(ap)
        self.assertEqual(stack.stack_from(ap.parse_args(["--loki-uncached"])).overlays, [stack.LOKI_UNCACHED_OVERLAY])
        self.assertEqual(stack.stack_from(ap.parse_args([])).overlays, [])
