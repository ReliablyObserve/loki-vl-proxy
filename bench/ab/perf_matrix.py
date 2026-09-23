#!/usr/bin/env python3
"""A/B latency and parity matrix: two or more proxy builds against Loki direct.

Runs every shape of a set (bench/ab/shapes.json) over each range against every
target, interleaved per (shape, range, run) so data growth and cache state do
not favour one target. All targets share the same pinned, step-aligned window
per run; each run shifts the window by one minute so repeated runs are not
response-cache hits. VictoriaLogs CPU is sampled with `docker stats` and the
container's RestartCount is recorded before and after: a run that spans a
restart is marked invalid and stops.

Writes the raw per-request rows to --out. Reduce them with report.py.

  perf_matrix.py --set control --runs 5 \\
      --target main=http://127.0.0.1:13190 \\
      --target branch=http://127.0.0.1:13192 \\
      --target loki=http://127.0.0.1:13101 \\
      --out /tmp/control.raw.json

Run each proxy build on a port you have verified you own and with a unique
-admin-listen; see bench/ab/README.md.
"""
import argparse
import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


class CPUSampler(threading.Thread):
    def __init__(self, container):
        super().__init__(daemon=True)
        self.container, self.samples, self.stop = container, [], False
        self.failed = 0

    def run(self):
        while not self.stop:
            try:
                out = subprocess.check_output(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", self.container],
                                              text=True, timeout=20).strip()
                self.samples.append((time.time(), float(out.rstrip("%"))))
            except (subprocess.SubprocessError, ValueError, OSError):
                # A missed sample only narrows the CPU figures; it is counted
                # and reported with the run instead of failing it.
                self.failed += 1
            time.sleep(0.5)

    def between(self, t0, t1):
        vals = [c for t, c in self.samples if t0 - 1.5 <= t <= t1 + 1.5]
        return (max(vals), sum(vals) / len(vals)) if vals else (None, None)


def request(url, headers, tenant, timeout):
    req = urllib.request.Request(url, headers={"X-Scope-OrgID": tenant, **headers})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body, status = r.read(), r.status
    except urllib.error.HTTPError as e:
        body, status = e.read(), e.code
    except (urllib.error.URLError, OSError) as e:
        return 0, str(e).encode()[:200], time.time() - t0, t0
    return status, body, time.time() - t0, t0


def signature(status, body, logs):
    """A result fingerprint: equal signatures mean equal answers."""
    if status != 200:
        try:
            return "ERR " + json.loads(body).get("error", "")[:90]
        except (ValueError, AttributeError):
            return "ERR " + body[:90].decode(errors="replace")
    try:
        data = json.loads(body)["data"]
    except (ValueError, KeyError, TypeError):
        return "unparseable"
    result = data.get("result", [])
    if logs or data.get("resultType") == "streams":
        return f"streams={len(result)} lines={sum(len(s.get('values', [])) for s in result)}"
    total, points = 0.0, 0
    for series in result:
        values = series.get("values") or ([series["value"]] if "value" in series else [])
        for _, v in values:
            total += float(v)
            points += 1
    return f"series={len(result)} points={points} sum={total:.6g}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", required=True, help="shape set name in shapes.json")
    ap.add_argument("--target", action="append", required=True, metavar="NAME=URL",
                    help="repeatable; name the Loki reference 'loki'")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--ranges", default="", help="comma-separated override of the set's ranges")
    ap.add_argument("--shapes", default="", help="comma-separated shape name prefixes (default all)")
    ap.add_argument("--shapes-file", default=os.path.join(HERE, "shapes.json"))
    ap.add_argument("--tenant", default="0")
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--container", default="e2e-victorialogs")
    ap.add_argument("--vl-health", default="http://127.0.0.1:19428/health")
    ap.add_argument("--long-runs", default="", metavar="NAME=N",
                    help="cap runs of ranges >= 24h for a target, e.g. main=1 when an old build scans raw rows "
                         "(repeated long raw scans have killed the shared backend)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    with open(args.shapes_file) as f:
        spec = json.load(f)
    shape_set = spec["sets"][args.set]
    range_defs = spec["ranges"]
    ranges = [r for r in (args.ranges.split(",") if args.ranges else shape_set["ranges"]) if r]
    targets = [t.split("=", 1) for t in args.target]
    long_runs = dict(x.split("=", 1) for x in args.long_runs.split(",") if x)
    wanted = [s for s in args.shapes.split(",") if s]

    def restarts():
        return subprocess.check_output(["docker", "inspect", args.container, "--format", "{{.RestartCount}}"], text=True).strip()

    def healthy_and_unrestarted(ref):
        last_error = None
        for _ in range(120):
            try:
                with urllib.request.urlopen(args.vl_health, timeout=5) as r:
                    if r.status == 200:
                        break
            except (urllib.error.URLError, OSError) as e:
                last_error = e  # still starting up; retry until the deadline
            time.sleep(1)
        else:
            print(f"VictoriaLogs did not become healthy: {last_error}", flush=True)
        return restarts() == ref

    end = (int(time.time()) - 60) // 60 * 60  # step-aligned for every range's step
    sampler = CPUSampler(args.container)
    sampler.start()
    restart0, rows, aborted = restarts(), [], False
    for shape in shape_set["shapes"]:
        if wanted and not any(shape["name"].startswith(w) for w in wanted):
            continue
        shape_ranges = ["instant"] if shape.get("instant") else ranges
        for rname in shape_ranges:
            secs, step = range_defs.get(rname, (0, 0))
            query = shape["query"].replace("$__auto", f"{step}s")
            for run in range(args.runs):
                run_end = end - 60 * run
                for tname, base in targets:
                    if secs >= 86400 and tname in long_runs and run >= int(long_runs[tname]):
                        continue
                    if shape.get("instant"):
                        url = f"{base}/loki/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": run_end})
                    else:
                        params = {"query": query, "start": run_end - secs, "end": run_end, "step": step,
                                  "limit": 1000, "direction": "backward"}
                        url = f"{base}/loki/api/v1/query_range?" + urllib.parse.urlencode(params)
                    status, body, dt, t0 = request(url, shape.get("headers", {}), args.tenant, args.timeout)
                    time.sleep(0.3)
                    cmax, cavg = sampler.between(t0, t0 + dt)
                    row = {"shape": shape["name"], "range": rname, "run": run, "end": run_end, "target": tname, "status": status,
                           "seconds": round(dt, 3), "signature": signature(status, body, shape.get("logs", False)),
                           "vl_cpu_max": cmax, "vl_cpu_avg": cavg}
                    rows.append(row)
                    print(f"{shape['name'][:44]:44} {rname:>7} run{run} {tname:8} {status} {dt:7.2f}s {row['signature'][:70]}", flush=True)
                    if status in (0, 502) and not healthy_and_unrestarted(restart0):
                        print(f"ABORT: {args.container} RestartCount changed; this run is invalid", flush=True)
                        aborted = True
                        break
                if aborted:
                    break
            if aborted:
                break
        if aborted:
            break
    sampler.stop = True
    restart1 = restarts()
    result = {"set": args.set, "description": shape_set.get("description", ""), "end": end, "runs": args.runs,
               "targets": [t for t, _ in targets], "restart_before": restart0, "restart_after": restart1,
              "valid": not aborted and restart0 == restart1, "cpu_samples_missed": sampler.failed, "rows": rows}
    with open(args.out, "w") as f:
        json.dump(result, f, indent=1)
    print(f"\nwrote {args.out}; VictoriaLogs RestartCount before={restart0} after={restart1}")


if __name__ == "__main__":
    main()
