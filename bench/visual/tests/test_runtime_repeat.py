"""ci_run.py's cold repeats of a first-pass runtime regression: a real cold regression still fails, noise is cleared.

The rule (ci_run.RUNTIME_REPEATS): a capture whose only finding is a base-vs-PR `runtime regression:` is loaded again in
two cold pairs (both proxies' caches flushed, PR first, then base first); per build, the median of the first pass and
the two repeats is judged by compare.runtime_regressions, the thresholds of one capture. Anything that is not a clean
cold sample leaves the first-pass finding standing.
"""
import copy
import http.server
import inspect
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import unittest.mock as mock
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import ci_run  # noqa: E402
import compare  # noqa: E402
import merge  # noqa: E402
import shards  # noqa: E402
import stack  # noqa: E402
import visual_comment as vc  # noqa: E402
from vio import dump_json, load_json  # noqa: E402

MIB = 1 << 20


def runtime(cpu=1.0, heap=100 * MIB, wire=1 * MIB, requests=3):
    return {"browser_thread_seconds": cpu, "browser_task_seconds": cpu + 0.2, "js_heap_max_sampled_bytes": heap,
            "metric_samples": 20, "first_api_data_ms": 1500, "api_requests": requests, "api_encoded_bytes": wire,
            "page_encoded_bytes": 2 * MIB}


def cap(settle_ms, attempts=1, settled=True, **kw):
    """A capture file as capture.spec.ts writes it, complete enough for the runtime proof."""
    return {"settled": settled, "settle_ms": settle_ms, "last_api_completion_ms": settle_ms - 3400, "attempts": attempts,
            "records": [], "ui": {"noData": 0, "banners": [], "panelErrors": 0}, "errors": [], "runtime": runtime(**kw)}


def first_pass_row(main, pr, loki=None, page="explore-logs-json", rng="15m"):
    """A compare.py row of a page whose only finding is the runtime regression the captures show."""
    row = {"page": page, "range": rng, "requests": 3, "series": 1, "main_vs_pr": "3/3", "pr_vs_loki": "3/3 identical", "settled": True, "settled_pr": True, "main_pr_diffs": [],
           "loki_diffs": [], "loki_new": [], "loki_compared": True, "points_main": 4, "points_pr": 4, "points_loki": 4,
           "ui_main": {"noData": 0}, "ui_pr": {"noData": 0}, "ui_loki": {"noData": 0}, "errors_pr": [], "errors_main": [],
           "settle_s": {"main": main["settle_ms"] / 1000, "pr": pr["settle_ms"] / 1000},
           "runtime_main": main["runtime"], "runtime_pr": pr["runtime"], "runtime_loki": (loki or {}).get("runtime")}
    if loki:
        row["settle_s"]["loki"] = loki["settle_ms"] / 1000
    row["runtime_problems"] = compare.runtime_problems(main, pr, required=True)
    return row


def pair(order, main, pr):
    """A cold pair checked against a first pass of three API requests per build (first_pass_row's default)."""
    return ci_run.repeat_observation(order, {"main": main, "pr": pr}, {"runtime_main": runtime(), "runtime_pr": runtime()})


def judged(row, pairs):
    return ci_run.judge_runtime_repeat(copy.deepcopy(row), {"pairs": pairs})


def gate(row):
    fails, warns, _ = vc.assess(row, 0.0)
    return fails, warns


class RuleTest(unittest.TestCase):
    """judge_runtime_repeat: the median of the first pass and the repeats, judged by the thresholds of one capture."""

    def test_a_real_cold_regression_that_reproduces_in_every_pair_still_fails(self):
        row = first_pass_row(cap(5000), cap(7100))
        self.assertTrue(row["runtime_problems"])
        got = judged(row, [pair(("pr", "main"), cap(5050), cap(7000)), pair(("main", "pr"), cap(4950), cap(7150))])
        self.assertEqual(got["runtime_repeat"]["verdict"], "reproduced")
        self.assertEqual(got["runtime_problems"],
                         ["runtime regression: Grafana settlement 5000 -> 7100 ms (median of 3 cold observations per build)"])
        self.assertNotIn("runtime_unconfirmed", got)
        fails, _ = gate(got)
        self.assertTrue(any(f.startswith("runtime regression: Grafana settlement") for f in fails))

    def test_a_cold_cpu_regression_fails_on_its_median_too(self):
        row = first_pass_row(cap(5000, cpu=1.0), cap(5000, cpu=1.6))
        got = judged(row, [pair(("pr", "main"), cap(5000, cpu=1.1), cap(5000, cpu=1.5)),
                           pair(("main", "pr"), cap(5000, cpu=0.9), cap(5000, cpu=1.7))])
        self.assertEqual(got["runtime_repeat"]["verdict"], "reproduced")
        self.assertEqual(got["runtime_problems"], ["runtime regression: browser thread CPU 1 -> 1.6 (median of 3 cold observations per build)"])

    def test_a_first_pass_outlier_that_does_not_reproduce_cold_is_cleared_and_shown(self):
        # explore-logs-json 15m on #1715: the same requests and first data on both builds, settlement 1.5 s apart once.
        row = first_pass_row(cap(5040), cap(6550))
        first_finding = list(row["runtime_problems"])
        got = judged(row, [pair(("pr", "main"), cap(5100), cap(4980)), pair(("main", "pr"), cap(5020), cap(5200))])
        self.assertEqual(got["runtime_repeat"]["verdict"], "not reproduced")
        self.assertEqual(got["runtime_problems"], [])
        self.assertEqual(got["runtime_unconfirmed"], first_finding)
        # Every observation is kept: the first pass, both pairs and the medians.
        record = got["runtime_repeat"]
        self.assertEqual(record["first_pass_findings"], first_finding)
        self.assertEqual([p["order"] for p in record["pairs"]], [["pr", "main"], ["main", "pr"]])
        self.assertEqual(record["first_pass"]["settle_s"], {"main": 5.04, "pr": 6.55})
        self.assertEqual(record["median"]["settle_s"], {"main": 5.04, "pr": 5.2})
        fails, warns = gate(got)
        self.assertEqual(fails, [])
        self.assertTrue(any("not reproduced cold" in w and "5040 -> 6550" in w for w in warns))

    def test_one_clean_pair_does_not_clear_a_regression_the_other_pair_reproduces(self):
        # PR slow in the first pass and the first repeat, not in the second: two of three observations are slow, the
        # median is slow, the finding stands. (Neither "any clean pair clears" nor a median of the repeats alone may
        # clear it: the median of 7500 and 5000 is 6250, exactly 1.25 x 5000.)
        row = first_pass_row(cap(5000), cap(7500))
        got = judged(row, [pair(("pr", "main"), cap(5000), cap(7500)), pair(("main", "pr"), cap(5000), cap(5000))])
        self.assertEqual(got["runtime_repeat"]["verdict"], "reproduced")
        self.assertTrue(gate(got)[0])

    def test_the_median_at_the_threshold_passes_as_one_capture_at_the_threshold_does(self):
        # 25% over the base is not "more than 25%", and the absolute guard is strict as well.
        self.assertEqual(compare.runtime_regressions(cap(2000), cap(2500)), [])
        self.assertEqual(compare.runtime_regressions(cap(4000), cap(5001)),
                         ["runtime regression: Grafana settlement 4000 -> 5001 ms"])
        row = first_pass_row(cap(4000), cap(5600))
        got = judged(row, [pair(("pr", "main"), cap(4000), cap(5000)), pair(("main", "pr"), cap(4000), cap(5000))])
        self.assertEqual(got["runtime_repeat"]["median"]["settle_s"], {"main": 4.0, "pr": 5.0})
        self.assertEqual(got["runtime_repeat"]["verdict"], "not reproduced")
        got = judged(row, [pair(("pr", "main"), cap(4000), cap(5001)), pair(("main", "pr"), cap(4000), cap(5001))])
        self.assertEqual(got["runtime_repeat"]["verdict"], "reproduced")

    def test_the_thresholds_are_the_ones_of_one_capture(self):
        # The repeat path reads the same guards compare.runtime_problems applies to a single capture.
        self.assertEqual([(k, m) for k, m, _ in compare.RUNTIME_GUARDS],
                         [("browser_thread_seconds", 0.25), ("js_heap_max_sampled_bytes", 32 << 20), ("api_encoded_bytes", 64 << 10)])
        self.assertEqual(compare.SETTLE_GUARD_MS, 500)
        self.assertEqual(compare.runtime_regressions(cap(1000, cpu=1.0), cap(1000, cpu=1.25)), [])
        self.assertEqual(compare.runtime_regressions(cap(1000, cpu=2.0), cap(1000, cpu=2.51)),
                         ["runtime regression: browser thread CPU 2 -> 2.51"])

    def test_a_sample_that_is_not_clean_and_cold_leaves_the_first_pass_finding_standing(self):
        row = first_pass_row(cap(5040), cap(6550))
        clean = pair(("main", "pr"), cap(5020), cap(5000))
        bad = {
            "retried": pair(("pr", "main"), cap(5000), cap(5000, attempts=2)),
            "unsettled": pair(("pr", "main"), cap(5000, settled=False), cap(5000)),
            "a request wave missed": pair(("pr", "main"), cap(5000), cap(5000, requests=2)),
            "a side missing": ci_run.repeat_observation(("pr", "main"), {"main": cap(5000)}, ci_run.first_pass_observation(row)),
            "flush refused": {"order": ["pr", "main"], "why": "cache flush refused: HTTP 403"},
            "out of time": {"order": ["pr", "main"], "why": "the time budget of the cold repeats ran out during the pair"},
        }
        for name, sample in bad.items():
            with self.subTest(name):
                self.assertTrue(sample.get("why"))
                got = judged(row, [sample, clean])
                self.assertEqual(got["runtime_repeat"]["verdict"], "stands")
                self.assertEqual(got["runtime_problems"], row["runtime_problems"])
                self.assertNotIn("median", got["runtime_repeat"])
                fails, warns = gate(got)
                self.assertEqual(fails, row["runtime_problems"])
                self.assertTrue(any("not measured again cold" in w for w in warns))
        with self.subTest("fewer pairs than the rule asks for"):
            got = judged(row, [clean])
            self.assertEqual(got["runtime_repeat"]["verdict"], "stands")
            self.assertEqual(got["runtime_problems"], row["runtime_problems"])
        with self.subTest("not measured again (cap, budget)"):
            got = ci_run.judge_runtime_repeat(copy.deepcopy(row), {"why": "over the cap of 4 cold-repeated captures per shard"})
            self.assertEqual(got["runtime_problems"], row["runtime_problems"])
            self.assertEqual(got["runtime_repeat"]["why"], "over the cap of 4 cold-repeated captures per shard")

    def test_only_a_row_whose_sole_finding_is_a_first_pass_regression_is_a_candidate(self):
        row = first_pass_row(cap(5040), cap(6550))
        self.assertTrue(ci_run.runtime_repeat_candidate(row))
        not_candidates = {
            "clean": {**row, "runtime_problems": []},
            "recaptured": row,
            "live": {**row, "range": "live"},
            "data differs": {**row, "main_pr_diffs": ["query x: value differs"]},
            "unsettled": {**row, "settled": False},
            "runtime proof missing": {**row, "runtime_problems": [*row["runtime_problems"], "runtime proof: PR did not settle"]},
            "fix page, judged vs Loki": {**row, "runtime_problems": ["runtime regression vs Loki: Grafana settlement 1 -> 9 ms (over 2x Loki)"]},
            "No data only on the PR": {**row, "ui_pr": {"noData": 1}},
            "error banner only on the PR": {**row, "ui_pr": {"noData": 0, "banners": ["Plugin unavailable"]}},
            "error answer": {**row, "errors_pr": ["status 500"]},
        }
        for name, r in not_candidates.items():
            with self.subTest(name):
                self.assertFalse(ci_run.runtime_repeat_candidate(r, ["explore-logs-json 15m"] if name == "recaptured" else ()))

    def test_a_final_row_that_is_no_longer_a_candidate_keeps_its_findings(self):
        row = first_pass_row(cap(5040), cap(6550))
        changed = {**row, "main_pr_diffs": ["query x: value differs"]}
        clean = [pair(("pr", "main"), cap(5000), cap(5000)), pair(("main", "pr"), cap(5000), cap(5000))]
        got, = ci_run.apply_runtime_repeats([copy.deepcopy(changed)], {"explore-logs-json 15m": {"pairs": clean}})
        self.assertEqual(got["runtime_problems"], row["runtime_problems"])
        self.assertNotIn("runtime_repeat", got)
        got, = ci_run.apply_runtime_repeats([copy.deepcopy(row)], {"explore-logs-json 15m": {"pairs": clean}})
        self.assertEqual(got["runtime_problems"], [])


class FlushServer(http.server.BaseHTTPRequestHandler):
    status = 200
    token = ""  # when set, the admin token every request must carry (the proxy's adminMiddleware)
    hits = []

    def do_POST(self):
        FlushServer.hits.append(self.path)
        refused = FlushServer.token and self.headers.get("X-Admin-Token") != FlushServer.token
        self.send_response(401 if refused else FlushServer.status)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *a):
        pass


class RunTest(unittest.TestCase):
    """repeat_runtime / repeat_pair: flushed, one page at a time, the first pass's settle rule, bounded."""

    def setUp(self):
        self.out = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.out, True)
        self.a = Namespace(workers=2, runtime_repeats=2, max_runtime_repeat=4, runtime_repeat_budget=300)
        self.state = {"end": 1000, "ports": {"main": 1, "pr": 3, "grafana": 5}}
        self.plan = {"entries": {}}

    def write_rows(self, rows):
        for r in rows:
            self.plan["entries"].setdefault(r["page"], {"kind": "explore", "ranges": []})["ranges"].append(r["range"])
        dump_json(os.path.join(self.out, "compare.json"), rows)

    def fake_playwright(self, sides, calls):
        def run(cmd, cwd, env, log, timeout):
            calls.append(("capture", env, timeout))
            entries = load_json(env["VP_PLAN"])["entries"]
            (page, entry), = entries.items()
            d = Path(env["VP_OUT"], "data", page, entry["ranges"][0])
            d.mkdir(parents=True)
            for side in env["VP_DATASOURCES"].split(","):
                (d / f"{side}.json").write_text(json.dumps(sides(page, side)))
            return 0
        return run

    def test_each_candidate_is_measured_in_two_flushed_pairs_pr_first_then_base_first(self):
        noisy = first_pass_row(cap(5040), cap(6550), page="explore-logs-json")
        real = first_pass_row(cap(5000), cap(7100), page="explore-extracted-filter-metric", rng="1h")
        clean = {**first_pass_row(cap(5000), cap(5000), page="dd-fields"), "runtime_problems": []}
        recaptured = first_pass_row(cap(5000), cap(7000), page="dd-labels")
        self.write_rows([noisy, real, clean, recaptured])
        calls = []
        slow = {"explore-extracted-filter-metric": 7100}
        sides = lambda page, side: cap(slow.get(page, 5000) if side == "pr" else 5000)
        with mock.patch.object(ci_run, "flush_proxies", lambda state: calls.append(("flush", state["out"])) or True), \
                mock.patch.object(ci_run, "bounded", self.fake_playwright(sides, calls)):
            meta = {"recaptured": ["dd-labels 15m"]}
            repeats = ci_run.repeat_runtime(self.a, self.out, self.plan, self.state, meta)
        self.assertEqual(sorted(repeats), ["explore-extracted-filter-metric 1h", "explore-logs-json 15m"])
        self.assertEqual(meta["runtime_repeat"]["candidates"], sorted(repeats))
        self.assertIn("runtime_repeat", meta.get("phases_s", {}))
        self.assertEqual(load_json(os.path.join(self.out, "runtime-repeat.json")), repeats)
        # Before every pair both proxies were flushed; each pair is one Playwright run of one page, two builds in order.
        self.assertEqual([c[0] for c in calls], ["flush", "capture"] * 4)
        captures = [c[1] for c in calls if c[0] == "capture"]
        self.assertEqual([e["VP_DATASOURCES"] for e in captures], ["pr,main", "main,pr"] * 2)
        for env in captures:
            self.assertEqual(env["WORKERS"], "1")
            self.assertEqual(env.get("VP_QUIET_MS"), os.environ.get("VP_QUIET_MS"))  # the first pass's settle rule
            self.assertEqual(env.get("VP_SETTLE_MS"), os.environ.get("VP_SETTLE_MS"))
            self.assertNotEqual(env["VP_OUT"], self.out)
            self.assertEqual(env["VP_END"], "1000")
        for timeout in (c[2] for c in calls if c[0] == "capture"):
            self.assertLessEqual(timeout, 300)
        # The final comparison is judged by the rule: the real one fails, the noise is cleared.
        rows = ci_run.apply_runtime_repeats(load_json(os.path.join(self.out, "compare.json")), repeats)
        by = {r["page"]: r for r in rows}
        self.assertEqual(by["explore-extracted-filter-metric"]["runtime_repeat"]["verdict"], "reproduced")
        self.assertTrue(by["explore-extracted-filter-metric"]["runtime_problems"])
        self.assertEqual(by["explore-logs-json"]["runtime_repeat"]["verdict"], "not reproduced")
        self.assertEqual(by["explore-logs-json"]["runtime_problems"], [])
        self.assertNotIn("runtime_repeat", by["dd-labels"])

    def test_rows_past_the_cap_or_the_budget_keep_their_first_pass_finding(self):
        rows = [first_pass_row(cap(5040), cap(6550), page=f"explore-{i}") for i in range(3)]
        self.write_rows(rows)
        calls = []
        self.a.max_runtime_repeat = 1
        with mock.patch.object(ci_run, "flush_proxies", lambda state: True), \
                mock.patch.object(ci_run, "bounded", self.fake_playwright(lambda p, s: cap(5000), calls)):
            repeats = ci_run.repeat_runtime(self.a, self.out, self.plan, self.state, {})
        self.assertEqual(len(calls), 2)  # two pairs of the one row within the cap
        self.assertIn("over the cap of 1", repeats["explore-1 15m"]["why"])
        final = {r["page"]: r for r in ci_run.apply_runtime_repeats(copy.deepcopy(rows), repeats)}
        self.assertEqual(final["explore-0"]["runtime_problems"], [])
        for page in ("explore-1", "explore-2"):
            self.assertEqual(final[page]["runtime_problems"], rows[0]["runtime_problems"])
            self.assertTrue(gate(final[page])[0])
        # Past the budget nothing more starts, and a pair cut by it does not count.
        self.a.max_runtime_repeat, self.a.runtime_repeat_budget = 4, 0
        calls.clear()
        with mock.patch.object(ci_run, "flush_proxies", lambda state: True), \
                mock.patch.object(ci_run, "bounded", self.fake_playwright(lambda p, s: cap(5000), calls)):
            repeats = ci_run.repeat_runtime(self.a, self.out, self.plan, self.state, {})
        self.assertEqual(calls, [])
        self.assertTrue(all("budget" in r["why"] for r in repeats.values()))
        with mock.patch.object(ci_run, "flush_proxies", lambda state: True), \
                mock.patch.object(ci_run, "bounded", lambda *a, **kw: None):
            obs = ci_run.repeat_pair(self.a, self.out, "explore-0", "15m", "explore", ("pr", "main"), 1000, 5,
                                     dict(self.state, out=self.out), ci_run.first_pass_observation(rows[0]), time.time() + 60)
        self.assertIn("time budget", obs["why"])

    def test_a_refused_flush_is_no_cold_sample(self):
        FlushServer.status, FlushServer.hits = 200, []
        srv = http.server.HTTPServer(("127.0.0.1", 0), FlushServer)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        target = {"url": f"http://127.0.0.1:{srv.server_address[1]}/admin/cache/flush", "token": ""}
        state = {"out": self.out, "ports": {"main": 1, "pr": 3}, "flush": {"main": target, "pr": target}}
        with self.assertRaisesRegex(OSError, "no cache flush target recorded for the main proxy"):
            ci_run.flush_proxies({**state, "flush": {}})
        Path(self.out, "proxy-main-aligned.log").write_text("x")
        with self.assertRaises(OSError):  # no log of the PR build to wait on: no cold sample
            ci_run.flush_proxies(state)
        Path(self.out, "proxy-pr-aligned.log").write_text("x")
        FlushServer.hits = []
        self.assertTrue(ci_run.flush_proxies(state))
        self.assertEqual(FlushServer.hits, ["/admin/cache/flush"] * 2)
        FlushServer.status = 403
        with self.assertRaises(OSError):
            ci_run.flush_proxies(state)
        row = first_pass_row(cap(5040), cap(6550))
        with mock.patch.object(ci_run, "bounded", lambda *a, **kw: self.fail("no capture after a refused flush")):
            obs = ci_run.repeat_pair(self.a, self.out, "explore-logs-json", "15m", "explore", ("pr", "main"), 1000, 5,
                                     dict(state, end=1000), ci_run.first_pass_observation(row), time.time() + 60)
        self.assertIn("cache flush refused", obs["why"])

    def test_the_flush_reaches_the_proxies_the_stack_starts(self):
        """The visual stack's service has an admin token, so its admin routes ride the main listener and no admin
        listener is started: the flush must go to the proxy's own port, with the token, as stack.up records it from
        the command the stack file gives the build. A flush sent to port + 1 is refused and nothing is repeated."""
        compose = Path(stack.ab.ROOT, stack.ab.BASE_COMPOSE).read_text()
        block = re.search(rf"^  {re.escape(stack.SERVICE)}:\n(.*?)(?=^  [\w-]+:\n|\Z)", compose, re.M | re.S).group(1)
        token = re.search(r'^\s*- "-server\.admin-auth-token=([^"]+)"', block, re.M).group(1)
        FlushServer.status, FlushServer.token, FlushServer.hits = 200, token, []
        self.addCleanup(setattr, FlushServer, "token", "")
        servers = []
        for _ in ("main", "pr"):
            srv = http.server.HTTPServer(("127.0.0.1", 0), FlushServer)
            threading.Thread(target=srv.serve_forever, daemon=True).start()
            self.addCleanup(srv.server_close)
            self.addCleanup(srv.shutdown)
            servers.append(srv)
        ports = {name: srv.server_address[1] for name, srv in zip(("main", "pr"), servers)}

        def start(specs, out, st, procs):
            for name, binary, _, port in specs:
                procs.append(stack.ab.Proxy(name, binary, [f"-server.admin-auth-token={token}"], {}, port, out))
                Path(procs[-1].log_path).write_text("x")

        args = Namespace(out=self.out, project="t", overlay=[], no_level_label=False, skip_seed=True, end=1000,
                         hours=1, loki_hours=1, main_ref="origin/main")
        with mock.patch.dict(stack.PORTS, ports), mock.patch.object(stack.ab, "build", lambda ref, out: stack.ab.ROOT), \
                mock.patch.object(stack.ab, "wait_http", lambda *a: None), \
                mock.patch.object(stack, "start_grafana", lambda *a: ("http://grafana", [])), \
                mock.patch.object(stack, "wait_plugins", lambda *a: (0, [])), \
                mock.patch.object(stack, "start_proxies", start):
            stack.up(args)
        state = load_json(os.path.join(self.out, "state.json"))
        self.assertEqual(state["flush"], {name: {"url": f"http://127.0.0.1:{port}/admin/cache/flush", "token": token}
                                          for name, port in ports.items()})
        self.assertTrue(ci_run.flush_proxies(dict(state, out=self.out)))
        self.assertEqual(FlushServer.hits, ["/admin/cache/flush"] * 2)
        wrong = {name: dict(target, token="") for name, target in state["flush"].items()}
        with self.assertRaisesRegex(OSError, "HTTP 401"):
            ci_run.flush_proxies(dict(state, out=self.out, flush=wrong))

    def test_a_pair_past_its_deadline_is_stopped_with_its_children(self):
        t0 = time.time()
        code = ci_run.bounded([sys.executable, "-c", ("import subprocess, sys; subprocess.run([sys.executable, '-c', "
                                      + "'import time; time.sleep(30)']); ")], self.out, dict(os.environ),
                              os.path.join(self.out, "log"), 1)
        self.assertIsNone(code)
        self.assertLess(time.time() - t0, 10)
        self.assertEqual(ci_run.bounded([sys.executable, "-c", "pass"], self.out, dict(os.environ), os.path.join(self.out, "log"), 30), 0)

    def test_the_repeats_run_before_the_live_tail_and_are_judged_on_the_final_comparison(self):
        body = inspect.getsource(ci_run.main)
        self.assertLess(body.index("recapture(a, out"), body.index("repeat_runtime("))
        self.assertLess(body.index("repeat_runtime("), body.index("capture_live("))
        self.assertLess(body.index("keep_runtime_observations("), body.index("apply_runtime_repeats("))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required")
    def test_capture_spec_loads_only_the_named_datasources_in_their_order(self):
        source = Path(ci_run.HERE, "capture.spec.ts").read_text()
        snippet = "const dss: Ds[] =" + source.split("const dss: Ds[] =", 1)[1].split("\nconst vars", 1)[0]
        script = r'''const strip = require("node:module").stripTypeScriptTypes;
if (!strip) process.exit(77);
const snippet = require("node:fs").readFileSync(0, "utf8");
const spec = {datasources: [{id: "main", uid: "a"}, {id: "pr", uid: "b"}, {id: "loki", uid: "c"}]};
const run = (env) => new Function("spec", "process", strip("type Ds = {id: string; uid: string};" + snippet) + "; return dss;")(spec, {env});
const ids = (env) => run(env).map((d) => d.id).join(",");
if (ids({}) !== "main,pr,loki") throw new Error("default: " + ids({}));
if (ids({VP_DATASOURCES: "pr,main"}) !== "pr,main") throw new Error("ordered: " + ids({VP_DATASOURCES: "pr,main"}));
let threw = false; try { run({VP_DATASOURCES: "pr,nope"}); } catch (e) { threw = true; }
if (!threw) throw new Error("an unknown datasource must fail the run");'''
        proc = subprocess.run(["node", "-e", script], input=snippet, text=True, capture_output=True)
        if proc.returncode == 77:
            self.skipTest("Node 22.13+ supplies the TypeScript loader")
        self.assertEqual(proc.returncode, 0, proc.stderr)


class ReportTest(unittest.TestCase):
    """Every observation reaches the comment, and the repeat fields survive the shard merge."""

    def rows(self):
        noisy = judged(first_pass_row(cap(5040), cap(6550), loki=cap(5100)),
                       [pair(("pr", "main"), cap(5100), cap(4980)), pair(("main", "pr"), cap(5020), cap(5200))])
        real = judged(first_pass_row(cap(5000), cap(7100), page="explore-extracted-filter-metric", rng="1h"),
                      [pair(("pr", "main"), cap(5050), cap(7000)), pair(("main", "pr"), cap(4950), cap(7150))])
        return [noisy, real]

    def test_the_runtime_table_lists_the_first_pass_every_cold_pair_and_the_median(self):
        lines, summary = vc.runtime_section(self.rows())
        text = "\n".join(lines)
        self.assertEqual(summary["regressions"], 1)
        self.assertEqual(summary["not_reproduced_cold"], 1)
        for label in ("pr (first pass)", "loki (first pass)", "main (cold repeat 1, pr first)", "pr (cold repeat 2, main first)",
                      "pr (median, not reproduced)", "pr (median, reproduced)"):
            self.assertIn(label, text)
        self.assertIn("| explore-logs-json / 15m | pr (first pass) | 6.550 |", text)
        self.assertIn("| explore-logs-json / 15m | pr (cold repeat 2, main first) | 5.200 |", text)
        self.assertNotIn("loki (cold repeat", text)
        self.assertIn("1 first-pass finding(s) not reproduced cold", text)

    def test_the_repeat_fields_survive_the_shard_merge_also_over_the_recapture_cap(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        noisy, real = self.rows()
        entries = {r["page"]: {"ranges": [r["range"]], "why": ["f.go"], "core": False, "kind": "explore"} for r in (noisy, real)}
        plan = {"run": True, "core_range": "1h", "entries": entries, "captures": shards.capture_count(entries), "trimmed": [],
                "dropped": [], "fix_cases": {}, "fix_trimmed": []}
        part, = shards.split(plan, 1)
        for over in (False, True):
            with self.subTest(over_cap=over):
                d = os.path.join(tmp, f"in-{over}", f"{merge.PREFIX}0")
                os.makedirs(os.path.join(d, "montage"))
                dump_json(os.path.join(d, "plan.json"), part)
                dump_json(os.path.join(d, "compare.json"), sorted([noisy, real], key=merge.row_order))
                meta = {"base": "b" * 40, "head": "h" * 40, "recaptured": [], "phases_s": {"runtime_repeat": 120}}
                if over:  # another page was reloaded and the run was over the cap: its first pass is restored
                    meta.update(recaptured=["other 1h"], recapture_candidates=9, max_recapture=6)
                    os.makedirs(os.path.join(d, "first-pass"))
                    dump_json(os.path.join(d, "first-pass", "compare.json"), [{"page": "other", "range": "1h"}])
                dump_json(os.path.join(d, "meta.json"), meta)
                out = os.path.join(tmp, f"out-{over}")
                merge.merge(plan, 1, os.path.dirname(d), out)
                got = {r["page"]: r for r in load_json(os.path.join(out, "compare.json"))}
                self.assertEqual(got["explore-logs-json"]["runtime_repeat"], noisy["runtime_repeat"])
                self.assertEqual(got["explore-logs-json"]["runtime_problems"], [])
                self.assertEqual(got["explore-extracted-filter-metric"]["runtime_problems"], real["runtime_problems"])
                self.assertEqual(load_json(os.path.join(out, "meta.json"))["shards"][0]["phases_s"], {"runtime_repeat": 120})


if __name__ == "__main__":
    unittest.main()
