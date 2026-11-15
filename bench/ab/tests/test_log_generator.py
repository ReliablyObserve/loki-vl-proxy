"""Unit tests for test/e2e-compat/log-generator.py: timestamps of the lines of one push."""
import importlib.util
import os
import unittest
from collections import Counter
from unittest import mock

GENERATOR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "test", "e2e-compat", "log-generator.py")


def load():
    """A fresh copy of the generator module (its switches are read at import)."""
    spec = importlib.util.spec_from_file_location("log_generator_under_test", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stream_timestamps(batch):
    """stream index -> the timestamps (ns, as ints) of its lines."""
    return [[int(v[0]) for v in s["values"]] for s in batch]


class LiveTimestampsTest(unittest.TestCase):
    def test_distinct_by_default_within_and_across_streams_and_pushes(self):
        with mock.patch.dict(os.environ, clear=False):
            os.environ.pop("LOG_DISTINCT_TIMESTAMPS", None)
            gen = load()
        first = gen.services_batch(8)
        second = gen.services_batch(8)
        every = [ts for stamps in stream_timestamps(first + second) for ts in stamps]
        self.assertGreater(len(every), 100)
        self.assertEqual(max(Counter(every).values()), 1, "two lines share a timestamp")
        for stamps in stream_timestamps(first + second):
            self.assertEqual(stamps, sorted(stamps))
            self.assertTrue(all(b - a >= 1_000 for a, b in zip(stamps, stamps[1:])), "lines closer than 1 us")

    def test_distinct_when_the_clock_does_not_move(self):
        gen = load()
        with mock.patch.object(gen.time, "time_ns", return_value=1_800_000_000_000_000_000):
            batch = gen.stream({"app": "x"}, ["a", "b", "c", "d"])
            more = gen.stream_with_metadata({"app": "x"}, [("a", {"k": "v"}), ("b", {"k": "v"})])
        stamps = [int(v[0]) for v in batch["values"]] + [int(v[0]) for v in more["values"]]
        self.assertEqual(len(set(stamps)), len(stamps))
        self.assertEqual(stamps, sorted(stamps))
        self.assertEqual(stamps[1] - stamps[0], 1_000)

    def test_line_content_and_labels_do_not_change(self):
        gen = load()
        labels = {"app": "x", "level": "info"}
        batch = gen.stream(labels, ["a", "b"])
        self.assertEqual(batch["stream"], labels)
        self.assertEqual([v[1] for v in batch["values"]], ["a", "b"])

    def test_the_switch_off_ties_the_lines_of_a_stream(self):
        with mock.patch.dict(os.environ, {"LOG_DISTINCT_TIMESTAMPS": "0"}):
            gen = load()
        batch = gen.services_batch(8)
        self.assertTrue(any(len(stamps) > 1 for stamps in stream_timestamps(batch)))
        for stamps in stream_timestamps(batch):
            self.assertEqual(len(set(stamps)), 1, "the lines of one stream must tie when the switch is off")
        meta = gen.stream_with_metadata({"app": "x"}, [("a", {}), ("b", {})])
        self.assertEqual(len({v[0] for v in meta["values"]}), 1)


class BackfillTimestampsTest(unittest.TestCase):
    def test_backfill_steps_one_microsecond_whatever_the_switch(self):
        for switch in ("1", "0"):
            with mock.patch.dict(os.environ, {"LOG_DISTINCT_TIMESTAMPS": switch}):
                gen = load()
            gen._virtual_ns = 1_800_000_000_500_000_000
            try:
                batch = gen.stream({"app": "x"}, ["a", "b", "c"])
            finally:
                gen._virtual_ns = None
            stamps = [int(v[0]) for v in batch["values"]]
            self.assertEqual(stamps, [1_800_000_000_500_001_000, 1_800_000_000_500_002_000, 1_800_000_000_500_003_000])


if __name__ == "__main__":
    unittest.main()
