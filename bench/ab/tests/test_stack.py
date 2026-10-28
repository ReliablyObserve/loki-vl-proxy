"""Unit tests for stack.py: the seed's count query."""
import os
import re
import stat
import sys
import tempfile
import unittest
from unittest import mock

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


CONFIG = """auth_enabled: false
bloom_build:
  align_queries_with_step: false
query_range:
  align_queries_with_step: true
  cache_results: true
limits_config:
  max_cache_freshness_per_query: 0s
  split_queries_by_interval: 1h
"""


class LokiDefaultsOverlayTest(unittest.TestCase):
    FLAGS = ["-querier.align-querier-with-step=false", "-frontend.max-cache-freshness=10m"]

    def test_overlay_is_the_base_command_with_loki_s_two_defaults_restored(self):
        base = _loki_command(os.path.join(stack.ROOT, stack.BASE_COMPOSE))
        overlay = _loki_command(os.path.join(stack.ROOT, stack.LOKI_DEFAULTS_OVERLAY))
        self.assertTrue(base, "the base stack's loki command was not found")
        self.assertEqual(overlay, base + self.FLAGS)

    def test_the_shared_loki_config_is_the_aligned_profile(self):
        # The profiles are defined against this file; if it changes, the overlay and PROFILE_SETTINGS go with it.
        with open(os.path.join(stack.ROOT, "test/e2e-compat/loki-local-config.yaml")) as f:
            self.assertEqual(stack.profile_settings(f.read()), stack.PROFILE_SETTINGS["aligned"])

    def test_profile_selects_the_overlay(self):
        self.assertEqual(stack.Stack("p", profile="aligned").overlays, [])
        self.assertEqual(stack.Stack("p", profile="loki-defaults").overlays, [stack.LOKI_DEFAULTS_OVERLAY])
        self.assertEqual(stack.Stack("p", overlays=[stack.LOKI_UNCACHED_OVERLAY]).overlays,
                         [stack.LOKI_UNCACHED_OVERLAY])
        with self.assertRaises(ValueError):
            stack.Stack("p", profile="nope")

    def test_uncached_cannot_be_combined_with_the_defaults_profile(self):
        # Both overlays replace Loki's whole command: the later one would silently drop the other's flags.
        with self.assertRaises(ValueError):
            stack.Stack("p", overlays=[stack.LOKI_UNCACHED_OVERLAY], profile="loki-defaults")
        st = stack.Stack("p", overlays=[stack.LOKI_UNCACHED_OVERLAY])
        with self.assertRaises(RuntimeError):
            st.set_profile("loki-defaults")

    def test_profile_flag_is_a_stack_option_only_where_asked(self):
        import argparse
        ap = argparse.ArgumentParser()
        stack.stack_args(ap)
        self.assertEqual(stack.stack_from(ap.parse_args(["--profile", "loki-defaults"])).profile, "loki-defaults")
        ap = argparse.ArgumentParser()
        stack.stack_args(ap, profile=False)
        with self.assertRaises(SystemExit), mock.patch("sys.stderr"):
            ap.parse_args(["--profile", "aligned"])
        self.assertEqual(stack.stack_from(ap.parse_args([])).profile, "aligned")


class ProfileSettingsTest(unittest.TestCase):
    def test_reads_each_setting_from_its_own_section(self):
        # align_queries_with_step appears under another section first, as in Loki's real /config
        self.assertEqual(stack.profile_settings(CONFIG),
                         {"align_queries_with_step": True, "max_cache_freshness_per_query": "0s"})

    def test_a_setting_that_is_missing_is_none(self):
        self.assertEqual(stack.profile_settings("query_range:\n  cache_results: true\n"),
                         {"align_queries_with_step": None, "max_cache_freshness_per_query": None})

    def test_set_profile_recreates_loki_and_checks_it_reads_back_what_the_profile_says(self):
        st = stack.Stack("p")
        seen = []
        with mock.patch.object(st, "compose", side_effect=lambda *a, **kw: seen.append((a, st.overlays))), \
                mock.patch.object(stack, "wait_http"), mock.patch.object(stack, "log"), \
                mock.patch.object(st, "wait_equal_counts") as counts, \
                mock.patch.object(st, "loki_settings", return_value=stack.PROFILE_SETTINGS["loki-defaults"]):
            self.assertEqual(st.set_profile("loki-defaults", (100, 200), 5), stack.PROFILE_SETTINGS["loki-defaults"])
            self.assertEqual(seen, [(("up", "-d", "--no-deps", "--force-recreate", "loki"), [stack.LOKI_DEFAULTS_OVERLAY])])
            counts.assert_called_once_with(100, 200, 5)  # the seeded data is still there
            seen.clear()
            st.set_profile("loki-defaults", (100, 200), 5)  # already there: nothing recreated
            self.assertEqual(seen, [])
        st = stack.Stack("p")
        with mock.patch.object(st, "compose"), mock.patch.object(stack, "wait_http"), \
                mock.patch.object(st, "loki_settings", return_value=stack.PROFILE_SETTINGS["aligned"]):
            with self.assertRaises(RuntimeError):
                st.set_profile("loki-defaults")  # the container came up with the wrong setting


def fake_binary(directory, body):
    path = os.path.join(directory, "proxy")
    with open(path, "w") as f:
        f.write("#!/bin/sh\n" + body)
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)
    return path


class AlignFlagTest(unittest.TestCase):
    def test_probe_tells_a_build_that_knows_the_flag_from_one_that_does_not(self):
        with tempfile.TemporaryDirectory() as d:
            knows = fake_binary(d, 'echo "flag: help requested" >&2; exit 1\n')
            self.assertTrue(stack.probe_align_support(knows))
            old = fake_binary(d, 'echo "flag provided but not defined: -align-queries-with-step" >&2; exit 1\n')
            self.assertFalse(stack.probe_align_support(old))
            # Anything else (a crash, other output) is not support: the rows are marked, never guessed
            self.assertFalse(stack.probe_align_support(fake_binary(d, "echo panic >&2; exit 2\n")))
            self.assertFalse(stack.probe_align_support(os.path.join(d, "missing")))

    def test_probe_passes_the_flag_before_help_and_never_starts_a_server(self):
        with tempfile.TemporaryDirectory() as d:
            record = os.path.join(d, "argv")
            binary = fake_binary(d, f'echo "$@" > {record}; echo "flag: help requested" >&2; exit 1\n')
            stack.probe_align_support(binary)
            with open(record) as f:
                self.assertEqual(f.read().split(), ["-align-queries-with-step=true", "-help"])

    def test_aligned_profile_adds_the_flag_only_to_a_build_that_has_it(self):
        base = ["-backend=http://vl", "-x=1"]
        self.assertEqual(stack.align_args(base, "aligned", True), (base + ["-align-queries-with-step=true"], "on"))
        # an unknown flag exits the process: a build without it gets nothing and is marked
        self.assertEqual(stack.align_args(base, "aligned", False), (base, "unsupported"))

    def test_the_builds_own_stack_flag_never_leaks_into_another_profile(self):
        # Once the flag is in the compose file every proxy service carries it; the loki-defaults profile runs
        # the proxy on its default, so it is removed, in every spelling.
        for have in ("-align-queries-with-step=true", "-align-queries-with-step=false", "-align-queries-with-step"):
            args = ["-backend=http://vl", have, "-x=1"]
            self.assertEqual(stack.align_args(args, "loki-defaults", True), (["-backend=http://vl", "-x=1"], "off"))
            self.assertEqual(stack.align_args(args, "loki-defaults", False), (["-backend=http://vl", "-x=1"], "off"))
            self.assertEqual(stack.align_args(args, "aligned", True)[0].count("-align-queries-with-step=true"), 1)
            # a build without the flag does not run it even if its compose file somehow had it: nothing is added
            self.assertNotIn("-align-queries-with-step=true", stack.align_args(["-x=1"], "aligned", False)[0])

    def test_like_for_like(self):
        self.assertTrue(stack.like_for_like("aligned", "on"))
        self.assertFalse(stack.like_for_like("aligned", "unsupported"))
        self.assertFalse(stack.like_for_like("aligned", "off"))
        self.assertTrue(stack.like_for_like("loki-defaults", "off"))
        self.assertTrue(stack.like_for_like("loki-defaults", "unsupported"))

    def test_start_proxies_records_each_build_s_state_and_gives_each_profile_its_own_files(self):
        started = []

        class P(stack.Proxy):
            def start(self):
                started.append(self)

        specs = [("base", "/bin/base", "/t1", 1000), ("pr", "/bin/pr", "/t2", 1002)]
        proxies = []
        with tempfile.TemporaryDirectory() as out, \
                mock.patch.object(stack, "proxy_command", return_value=(["-backend=x", "-align-queries-with-step=true"], {})), \
                mock.patch.object(stack, "Proxy", P):
            stack.start_proxies(specs, out, mock.Mock(vl="http://vl"), "svc", proxies, profile="aligned",
                                supported={"base": False, "pr": True})
            stack.start_proxies(specs, out, mock.Mock(vl="http://vl"), "svc", proxies, profile="loki-defaults",
                                supported={"base": False, "pr": True})
        self.assertEqual([(p.name, p.profile, p.align) for p in proxies],
                         [("base", "aligned", "unsupported"), ("pr", "aligned", "on"),
                          ("base", "loki-defaults", "off"), ("pr", "loki-defaults", "off")])
        by = {(p.name, p.profile): p.args for p in proxies}
        self.assertNotIn("-align-queries-with-step=true", by[("base", "aligned")])
        self.assertIn("-align-queries-with-step=true", by[("pr", "aligned")])
        self.assertFalse(any(a.startswith("-align-queries-with-step") for a in by[("pr", "loki-defaults")]))
        self.assertEqual(len({p.log_path for p in proxies}), 4)


if __name__ == "__main__":
    unittest.main()
