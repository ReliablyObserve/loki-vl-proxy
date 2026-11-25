"""Unit tests for test/e2e-compat/log-generator.py: timestamps of the lines of one push."""
import importlib.util
import copy
import contextlib
import io
import json
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


class LokiPushIdentityTest(unittest.TestCase):
    def payload(self):
        return {"streams": [{"stream": {"app": "identity"}, "values": [
            ["1800000000000000000", '{"trace_id":"body","_msg":"literal"}'],
            ["1800000000000001000", '{"trace_id":"body"}', {"trace_id": "metadata"}],
            ["1800000000000002000", "level=info msg=done"],
            ["1800000000000003000", "plain <text> café"],
            ["1800000000000004000", '{"malformed":'],
        ]}]}

    def test_live_push_preserves_lines_labels_and_metadata(self):
        gen = load()
        payload = self.payload()
        before = copy.deepcopy(payload)
        with mock.patch.object(gen.urllib.request, "urlopen") as send:
            self.assertTrue(gen.push_vl(payload))
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, gen.VL_URL + "/insert/loki/api/v1/push?disable_message_parsing=1")
        self.assertEqual(json.loads(request.data), before)
        self.assertEqual(payload, before)

    def test_backfill_sends_identical_original_payload_to_both_backends(self):
        gen = load()
        gen.BACKFILL_END = 1_800_000_000
        payload = self.payload()
        before = copy.deepcopy(payload)
        with mock.patch.object(gen, "services_batch", return_value=payload["streams"]), \
             mock.patch.object(gen, "post_strict") as send:
            gen.backfill(1, 1)
        self.assertEqual(len(send.call_args_list), 2)
        self.assertEqual(send.call_args_list[0].args, (gen.LOKI_URL + "/loki/api/v1/push", before))
        self.assertEqual(send.call_args_list[1].args, (gen.VL_URL + "/insert/loki/api/v1/push?disable_message_parsing=1", before))
        self.assertEqual(payload, before)


class BackfillFlushTest(unittest.TestCase):
    def run_backfill(self, budget, seconds=250, batches=None, fail=None):
        env = {"LOG_BACKFILL_END": "1800000000", "LOG_BACKFILL_SEED": "checkpoint-proof"}
        if budget is not None:
            env["LOG_BACKFILL_FLUSH_STREAMS"] = str(budget)
        calls = []

        def send(request, timeout):
            calls.append((request.full_url, request.data, timeout))
            if fail and request.full_url.endswith(fail):
                raise OSError("ambiguous request failure")
            return mock.MagicMock()

        with mock.patch.dict(os.environ, env), contextlib.redirect_stdout(io.StringIO()):
            if budget is None:
                os.environ.pop("LOG_BACKFILL_FLUSH_STREAMS", None)
            gen = load()
            with contextlib.ExitStack() as patches:
                patches.enter_context(mock.patch.object(gen.urllib.request, "urlopen", side_effect=send))
                if batches is not None:
                    patches.enter_context(mock.patch.object(gen, "services_batch", side_effect=batches))
                try:
                    gen.backfill(seconds, 1 if batches is not None else 10)
                except Exception as error:
                    return calls, error
        return calls, None

    @staticmethod
    def batches(*sizes):
        return [[{"stream": {"app": "checkpoint", "part": str(i)},
                  "values": [["1800000000000000000", "original line"]]} for i in range(size)] for size in sizes]

    def test_checkpoint_preserves_every_payload_timestamp_and_label_without_consuming_rng(self):
        original, error = self.run_backfill(None)
        self.assertIsNone(error)
        enabled, error = self.run_backfill(1)
        self.assertIsNone(error)
        writes = [call for call in enabled if not call[0].endswith("/flush")]
        self.assertEqual(writes, original)
        self.assertGreater(len(writes), 2)
        self.assertEqual(sum(url.endswith("/flush") for url, _, _ in enabled), len(writes) // 2)
        for i in range(0, len(enabled), 3):
            loki, vl, flush = enabled[i:i + 3]
            self.assertTrue(loki[0].endswith("/loki/api/v1/push"))
            self.assertIn("disable_message_parsing=1", vl[0])
            self.assertEqual(loki[1], vl[1])
            self.assertTrue(flush[0].endswith("/flush"))
            self.assertIsNone(flush[1])
            self.assertEqual(flush[2], 60)

    def test_budget_counts_successful_stream_objects_and_resets_after_flush(self):
        calls, error = self.run_backfill(600, 5, self.batches(200, 200, 200, 200, 200))
        self.assertIsNone(error)
        self.assertEqual(["flush" if url.endswith("/flush") else len(json.loads(body)["streams"])
                          for url, body, _ in calls], [400, 400, 400, 400, "flush", 200, 200])

    def test_final_partial_envelope_can_cross_the_budget(self):
        calls, error = self.run_backfill(500, 3, self.batches(210, 210, 100))
        self.assertIsNone(error)
        self.assertEqual(["flush" if url.endswith("/flush") else len(json.loads(body)["streams"])
                          for url, body, _ in calls], [420, 420, 100, 100, "flush"])

    def test_ambiguous_push_or_flush_failure_aborts_without_replay(self):
        for failure, expected in (("/loki/api/v1/push", 1),
                                  ("/insert/loki/api/v1/push?disable_message_parsing=1", 2), ("/flush", 3)):
            with self.subTest(failure=failure):
                calls, error = self.run_backfill(1, 3, self.batches(210, 210, 100), fail=failure)
                self.assertIsInstance(error, OSError)
                self.assertEqual(len(calls), expected)

    def test_invalid_budget_fails_before_any_write(self):
        for budget in (-1, "not-an-integer"):
            with self.subTest(budget=budget):
                calls, error = self.run_backfill(budget, 1, self.batches(1))
                self.assertIsInstance(error, ValueError)
                self.assertEqual(calls, [])

    def test_zero_disables_checkpoints_and_live_push_ignores_backfill_setting(self):
        zero, error = self.run_backfill(0)
        self.assertIsNone(error)
        original, _ = self.run_backfill(None)
        self.assertEqual(zero, original)
        with mock.patch.dict(os.environ, {"LOG_BACKFILL_FLUSH_STREAMS": "-1"}):
            gen = load()
        with mock.patch.object(gen.urllib.request, "urlopen") as send:
            gen.dual_push([{"stream": {"app": "live"}, "values": [["1", "unchanged"]]}])
        self.assertEqual(len(send.call_args_list), 2)
        self.assertTrue(all("/flush" not in call.args[0].full_url for call in send.call_args_list))


if __name__ == "__main__":
    unittest.main()
