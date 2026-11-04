#!/usr/bin/env python3
"""One differential refresh: stack, captures, corpus, run, record, prune, regenerate, summarise.

The CI workflow (.github/workflows/parity-refresh.yaml) runs exactly this; run it locally to reproduce:

  python3 bench/parity/refresh.py --out /tmp/parity --mode full
  python3 bench/parity/refresh.py --out /tmp/parity --mode validate --project parity --port-offset 1500

Steps (bench/visual/stack.py does the stack, bench/parity/README.md describes the pieces):
  1. a fresh Loki + VictoriaLogs stack under its own compose project and ports (own proxies, Grafana);
  2. Grafana's own requests: the bench/visual pages, captured once (a warm-up first);
  3. the corpus (corpus.py) from Loki's test suites (a shallow, sparse checkout of the Loki tag the stack runs),
     the repository's cases, what Loki reports for the seeded window, and the captures;
  4. run.py, after Loki's range-metric path left its fresh-stack blank window;
  5. publish.py records the run in the registry, parity_gaps.py --prune drops the cluster ids the run no longer
     shows, every generator of the conformance gate rewrites its report, the gate runs once for its verdict;
  6. the stack is torn down, whatever happened.

--mode full is the whole corpus and may prune. --mode validate (pull requests that edit the pipeline) sends an evenly
spaced slice of every corpus source, records the run as partial and shows that pruning against it is refused: the
results are for the job summary and the artifact, never for a commit.

Outputs in --out: state.json and the stack logs, corpus.json, run/ (results, clusters.json, report.md, health.json),
prune.md, conformance.txt, result.json and summary.md (the job summary and the pull request body).

Exit: 0 the run is recorded; 2 the run could not prove both sides healthy (run.py); 3 health changed during the run;
4 the run cannot speak for the corpus (a source sent nothing, no cluster): nothing was pruned or published;
1 any other failure.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "bench", "visual"))
sys.path.insert(0, os.path.join(ROOT, "conformance", "scripts"))
sys.path.insert(0, os.path.join(ROOT, "scripts", "ci"))
from ci_run import run, wait_loki_metric  # noqa: E402  (bench/visual: the step runner and the Loki blank-window wait)
import check_conformance  # noqa: E402  (REPORTS: the generators, in the order the gate runs them)
import parity_gaps  # noqa: E402
import perf_evidence  # noqa: E402  (pinned_loki: the Loki release the stack runs)
from registry_io import dump_json, load_json, read_text, write_text  # noqa: E402

PY = sys.executable
LOKI_REPO = "https://github.com/grafana/loki"
DISCOVERY = parity_gaps.DISCOVERY
# Pages of the validation slice: one explore page and one drilldown page, so both capture paths run.
VALIDATE_PAGES = ("explore-logs-plain", "lvl-dd-logs-pill-json")
RANGES = "15m,1h"
UNCLAIMED_SHOWN = 40
# Loki answers range metrics for some services a little after others on a fresh stack: run.py's health proof
# (exit 2) is tried again while that is the only thing missing.
METRIC_RETRIES = 20
METRIC_RETRY_S = 15


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


def loki_tag():
    return "v" + perf_evidence.pinned_loki(read_text(os.path.join(ROOT, perf_evidence.COMPOSE)))


def fetch_loki_source(dest, tag, log):
    """pkg/logql of the Loki tag the stack runs: shallow and sparse, so a few MB instead of the repository."""
    if os.path.isdir(os.path.join(dest, "pkg", "logql")):
        return dest
    run(["git", "clone", "--quiet", "--depth", "1", "--branch", tag, "--filter=blob:none", "--sparse",
         LOKI_REPO, dest], log=log)
    run(["git", "-C", dest, "sparse-checkout", "set", "pkg/logql"], log=log)
    return dest


def stack_cmd(a, out):
    return [PY, os.path.join(ROOT, "bench", "visual", "stack.py"), "--out", out, "--project", a.project,
            "--port-offset", str(a.port_offset)]


def stack_up(a, out, log):
    for attempt in (1, 2):
        try:
            run([*stack_cmd(a, out), "up", "--hours", str(a.hours), "--loki-hours", str(a.loki_hours),
                 "--overlay", os.path.join("bench", "parity", "docker-compose.parity.yml"),
                 *(["--end", str(a.end)] if a.end else [])], cwd=ROOT, log=log)
            return load_json(os.path.join(out, "state.json"))
        except subprocess.CalledProcessError:
            if attempt == 2:
                raise
            # A seed the backend refused (Loki answers 500 while it warms up) or a slow start: begin again clean.
            run([*stack_cmd(a, out), "down"], cwd=ROOT, log=log, check=False)


def spec_pages(only=()):
    spec = load_json(os.path.join(ROOT, "bench", "visual", "spec.json"))
    return [p["id"] for p in spec["pages"] if p["kind"] != "tail" and (not only or p["id"] in only)]


def capture(a, out, state, log):
    """Grafana's own requests on the bench/visual pages (a warm-up load first: the first drilldown load is slow).

    A failed Playwright run leaves captures missing; the corpus then holds fewer Grafana requests, which the full
    run's source guard (parity_gaps.prune_refusal) refuses to prune against."""
    visual = os.path.join(ROOT, "bench", "visual")
    if not os.path.isdir(os.path.join(visual, "node_modules")):
        run(["npm", "ci", "--no-audit", "--no-fund"], cwd=visual, log=log)
    pages = spec_pages(VALIDATE_PAGES if a.mode == "validate" else ())
    env = dict(os.environ, GRAFANA_URL=state["grafana"], VP_OUT=os.path.join(out, "cap"), VP_END=str(state["end"]),
               VP_PAGES=",".join(pages), VP_RANGES=RANGES, WORKERS=str(a.workers))
    run(["npx", "playwright", "test"], cwd=visual, env=dict(env, VP_WARMUP="1"), log=log, check=False)
    result = run(["npx", "playwright", "test"], cwd=visual, env=env, log=log, check=False)
    return {"pages": len(pages), "exit": result.returncode}


def only_metric_path_warming(run_dir):
    """run.py refused for one reason only: Loki's range-metric path misses services its label values list."""
    try:
        before = load_json(os.path.join(run_dir, "health.json"))["before"]
    except (OSError, ValueError, KeyError):
        return False
    return bool(before.get("loki_ready") == before.get("proxy_ready") == before.get("vl_ready") == 200
                and before.get("lines_loki") == before.get("lines_vl") and not before.get("slices_differing")
                and (before.get("services_in_loki_metric") or 0) < (before.get("services_listed") or 0))


def slice_corpus(path, per_source):
    """The corpus with `per_source` evenly spaced entries of each source (all of a smaller source)."""
    doc = load_json(path)
    by_source = {}
    for entry in doc["entries"]:
        by_source.setdefault(entry["source"], []).append(entry)
    kept = []
    for entries in by_source.values():
        step = max(1, len(entries) // per_source)
        kept += entries[::step][:per_source]
    doc["entries"] = kept
    doc["slice"] = {"per_source": per_source, "of": sum(len(v) for v in by_source.values())}
    sliced = path[: -len(".json")] + "-slice.json"
    dump_json(sliced, doc)
    return sliced, len(kept), doc["slice"]["of"]


def regenerate():
    """Every report the conformance gate compares, rewritten from the registry as it now stands (the gate's own order).

    The gate compares each report with what its generator writes; running the generators here is what
    leaves the files it would find fresh."""
    for script, target, docs_flag in check_conformance.REPORTS:
        args = (docs_flag, target) if docs_flag else ()
        done = subprocess.run([PY, os.path.join(ROOT, "conformance", "scripts", script), *args], cwd=ROOT,
                              capture_output=True, text=True)
        if done.returncode:
            raise RuntimeError(f"{script} {' '.join(args)} failed: {(done.stdout + done.stderr).strip()[-400:]}")


def gate(out):
    """The conformance gate once, after the generators: what it still reports is real (an unclaimed cluster)."""
    done = subprocess.run([PY, os.path.join(ROOT, "scripts", "ci", "check_conformance.py")], cwd=ROOT,
                          capture_output=True, text=True)
    write_text(os.path.join(out, "conformance.txt"), done.stdout + done.stderr)
    return done.returncode == 0


def run_table(discovery):
    lines = ["| corpus source | requests | same | differ | both empty | blocked |", "|---|---:|---:|---:|---:|---:|"]
    for source, s in sorted((discovery.get("summary") or {}).items()):
        lines.append(f"| {source} | {s['requests']} | {s['same']} | {s['diff']} | {s['vacuous']} | {s['blocked']} |")
    return lines


def summary_md(a, result, discovery, prune_text, problems):
    health = discovery.get("health") or {}
    lines = [f"## Differential parity run ({result['mode']})", "",
             f"- proxy built from `{result['ref']}`, Loki `{result['loki']}`, window {discovery.get('window')}",
             f"- both sides proven healthy: {health.get('ok')} before, {health.get('ok_after')} after; lines Loki "
             f"{health.get('lines_loki')} = VictoriaLogs {health.get('lines_vl')}; restarts Loki "
             f"{health.get('loki_restarts')}->{health.get('loki_restarts_after')}, VictoriaLogs "
             f"{health.get('vl_restarts')}->{health.get('vl_restarts_after')}",
             f"- {result['clusters']} clusters, {result['requests']} requests, {result['not_reproducible']} difference(s) "
             "did not reproduce (blocked)",
             f"- elapsed {result['elapsed_s']} s ({', '.join(f'{k} {v} s' for k, v in result['phases_s'].items())})", ""]
    lines += run_table(discovery) + [""]
    if result.get("slice"):
        lines += [f"This is a slice ({result['slice']['per_source']} per source of "
                  f"{result['slice']['of']} corpus entries): the run is recorded as partial, `parity_gaps.py --prune` "
                  f"refused it (`{result.get('prune_refused')}`) and nothing here is for a commit.", ""]
    else:
        lines += [prune_text, ""]
    if result["conformance_ok"]:
        lines += ["The conformance gate passes against this run.", ""]
    else:
        shown = problems[:UNCLAIMED_SHOWN]
        lines += [f"The conformance gate reports {len(problems)} problem(s) in `parity_gaps.py --check` "
                  "(an unclaimed cluster needs a registry case with a `gap:` block that claims it):", ""]
        lines += [f"- {p}" for p in shown]
        if len(problems) > len(shown):
            lines.append(f"- ... {len(problems) - len(shown)} more in `conformance.txt` of the artifact")
        lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--mode", choices=["full", "validate"], default="full")
    ap.add_argument("--slice", type=int, default=25, help="validate: entries per corpus source")
    ap.add_argument("--project", default="parity")
    ap.add_argument("--port-offset", type=int, default=1500)
    ap.add_argument("--hours", type=float, default=1.5, help="VictoriaLogs history; the run's window is the last hour")
    ap.add_argument("--loki-hours", type=float, default=1.5)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--end", type=int, default=0, help="seed end (unix seconds, a whole minute)")
    ap.add_argument("--loki-src", default="", help="a checkout of grafana/loki (pkg/logql) instead of fetching the tag")
    ap.add_argument("--keep-stack", action="store_true")
    ap.add_argument("--no-prune", action="store_true", help="full mode: record the run but leave the cases alone")
    a = ap.parse_args()
    out = os.path.abspath(a.out)
    os.makedirs(out, exist_ok=True)
    stack_log = os.path.join(out, "stack.log")
    step_log = os.path.join(out, "steps.log")
    os.chdir(ROOT)
    result = {"mode": a.mode, "ref": git("rev-parse", "--short=8", "HEAD"), "loki": loki_tag(), "phases_s": {}}
    t_all = time.time()
    code, stack_started = 1, False

    def phase(name, t0):
        result["phases_s"][name] = round(time.time() - t0)

    try:
        t0 = time.time()
        stack_started = True
        state = stack_up(a, out, stack_log)
        phase("stack_up", t0)

        t0 = time.time()
        src = a.loki_src or fetch_loki_source(os.path.join(out, "loki-src"), result["loki"], step_log)
        phase("loki_source", t0)

        t0 = time.time()
        result["capture"] = capture(a, out, state, step_log)
        phase("capture", t0)

        t0 = time.time()
        corpus = os.path.join(out, "corpus.json")
        run([PY, os.path.join(HERE, "corpus.py"), "--out", corpus, "--loki-src", src,
             "--seeded", f"http://127.0.0.1:{state['ports']['loki']}", "--end", str(state["end"]),
             "--captures", os.path.join(out, "cap", "data")], log=step_log)
        if a.mode == "validate":
            corpus, kept, total = slice_corpus(corpus, a.slice)
            result["slice"] = {"per_source": a.slice, "of": total, "sent": kept}
        phase("corpus", t0)

        t0 = time.time()
        result["loki_metric_wait_s"] = wait_loki_metric(state["ports"]["loki"], state["end"])
        phase("loki_metric_wait", t0)

        t0 = time.time()
        run_dir = os.path.join(out, "run")
        for attempt in range(1, METRIC_RETRIES + 1):
            ran = run([PY, os.path.join(HERE, "run.py"), "--state", os.path.join(out, "state.json"), "--corpus", corpus,
                       "--out", run_dir, "--workers", str(a.workers)], log=os.path.join(out, "run.log"), check=False)
            if ran.returncode != 2 or attempt == METRIC_RETRIES or not only_metric_path_warming(run_dir):
                break
            print(f"Loki's range-metric path still lists fewer services than its label values ({attempt}); "
                  f"trying again in {METRIC_RETRY_S} s", flush=True)
            time.sleep(METRIC_RETRY_S)
        phase("run", t0)
        if ran.returncode:
            print(f"run.py exited {ran.returncode}: nothing is recorded (see {out}/run.log and run/health.json)", flush=True)
            code = ran.returncode
            return code

        run([PY, os.path.join(HERE, "publish.py"), run_dir, "--ref", result["ref"],
             *(["--partial"] if a.mode == "validate" else [])], log=step_log)
        discovery = load_json(DISCOVERY)
        result.update(clusters=len(discovery["clusters"]), not_reproducible=discovery["health"].get("not_reproducible"),
                      requests=sum(s["requests"] for s in discovery["summary"].values()))

        prune_text = ""
        refusal = parity_gaps.prune_refusal(discovery)
        if a.mode == "validate":
            # A slice must be refused: that is what keeps a partial run from stripping the registry's claims.
            result["prune_refused"] = refusal
            if not refusal:
                print("a partial run was not refused by prune", file=sys.stderr)
                return code
        elif refusal:
            print(f"prune refused: {refusal}; the recorded run is not published", flush=True)
            code = 4
            return code
        elif not a.no_prune:
            changes, _ = parity_gaps.prune()
            prune_text = parity_gaps.prune_report(changes)
            write_text(os.path.join(out, "prune.md"), prune_text)
            result["pruned"] = {"cases": len(changes), "ids": sum(len(c["dropped"]) for c in changes),
                                "undecided": [c["id"] for c in changes if not c["left"] and c["status"] != "fixed"]}

        t0 = time.time()
        regenerate()
        result["conformance_ok"] = gate(out)
        phase("regenerate_and_gate", t0)
        problems = parity_gaps.analyse()[2]
        result["unclaimed"] = sum(1 for p in problems if "no registry case" in p)
        result["problems"] = len(problems)
        result["elapsed_s"] = round(time.time() - t_all)
        write_text(os.path.join(out, "summary.md"), summary_md(a, result, discovery, prune_text, problems))
        code = 0
    except Exception as e:  # noqa: BLE001 - any failure must still leave a result and tear down
        traceback.print_exc()
        result["error"] = f"{type(e).__name__}: {e}"[:400]
    finally:
        if stack_started and not a.keep_stack:
            t0 = time.time()
            run([*stack_cmd(a, out), "down"], cwd=ROOT, log=stack_log, check=False)
            phase("stack_down", t0)
        result["exit"] = code
        result["elapsed_s"] = round(time.time() - t_all)
        dump_json(os.path.join(out, "result.json"), result)
    return code


if __name__ == "__main__":
    sys.exit(main())
