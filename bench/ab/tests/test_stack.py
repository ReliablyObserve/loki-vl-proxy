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
