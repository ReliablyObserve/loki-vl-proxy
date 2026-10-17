#!/usr/bin/env python3
"""The first view of a window, builds side by side: wall time and VictoriaLogs CPU of the very first
query of a shape on a freshly started proxy, and what each VictoriaLogs request of that view costs.

A proxy remembers what it learned about a window (which level fields and keys it holds, that no line
needs the slower path), so the first view is not what a warm run measures. This starts every build
fresh before each request, on a stack seeded with rows that have no stored level (the level only in
the line, the data the detected_level pipes read) and VictoriaLogs capped at 2 CPUs, and records
every sample:

  python3 bench/ab/cold_first_view.py cold --out /tmp/cold \\
      --build before=054d1706 --build main=d12a062c --build head=HEAD --runs 1h=7,24h=5

writes /tmp/cold/coldrestart.json: per build, shape and range the wall time of the query and the
CPU time VictoriaLogs' cgroup spent during it (Docker's container stats), one entry per sample, and
the medians. Builds are interleaved round by round.

  python3 bench/ab/cold_first_view.py probes --out /tmp/cold --build main=d12a062c --build head=HEAD

writes /tmp/cold/probes.json: the requests each build sent to VictoriaLogs for one first view of the
1h window (a recording forwarder sits between proxy and VictoriaLogs), each replayed --replays times
on windows shifted by a minute each, with wall time and CPU per replay. Both commands start their own
compose project (--project, --vl-port) and remove it; they need Docker, git and go.
"""
import argparse
import concurrent.futures
import datetime
import http.client
import http.server
import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stack  # noqa: E402

SERVICE = "loki-vl-proxy-underscore"
SELECTOR = '{env="production"}'
SHAPES = {
    "D": f"sum by (level, detected_level) (count_over_time({SELECTOR} | json | drop __error__ [1m]))",
    "DLV5": f"sum by (detected_level) (count_over_time({SELECTOR} | json | drop __error__ [1m]))",
}
RANGES = {"1h": 3600, "24h": 86400}
STEP = 15
WARM = f"{SELECTOR}"  # a trivial query: warms the label caches only


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost")
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.path)


def docker_socket():
    host = os.environ.get("DOCKER_HOST") or subprocess.check_output(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"], text=True).strip()
    if not host.startswith("unix://"):
        raise SystemExit(f"cpu sampling reads Docker's stats over a unix socket; the endpoint is {host}")
    return host[len("unix://"):]


class CPU:
    """Cumulative CPU time of a container (cgroup usage, as Docker reports it), in milliseconds."""

    def __init__(self, container):
        self.sock = docker_socket()
        self.id = subprocess.check_output(["docker", "inspect", container, "--format", "{{.Id}}"], text=True).strip()

    def ms(self):
        c = UnixConnection(self.sock)
        c.request("GET", f"/containers/{self.id}/stats?stream=false&one-shot=true")
        data = json.loads(c.getresponse().read())
        c.close()
        return data["cpu_stats"]["cpu_usage"]["total_usage"] / 1e6


class Tap(http.server.ThreadingHTTPServer):
    """Forwards to VictoriaLogs and records every request (path, form fields, start, duration)."""

    daemon_threads = True

    def __init__(self, vl):
        tap = self
        self.vl, self.log, self.lock = vl, [], threading.Lock()

        class H(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                t0 = time.time()
                req = urllib.request.Request(tap.vl + self.path, data=body, method="POST",
                                             headers={k: v for k, v in self.headers.items() if k.lower() not in ("host", "accept-encoding")})
                try:
                    r = urllib.request.urlopen(req, timeout=300)
                    status, data, ctype = r.status, r.read(), r.headers.get("Content-Type")
                except urllib.error.HTTPError as e:
                    status, data, ctype = e.code, e.read(), e.headers.get("Content-Type")
                form = {k: v[0] for k, v in urllib.parse.parse_qs(body.decode()).items()}
                with tap.lock:
                    tap.log.append({"path": self.path.split("?")[0], "form": form, "t": t0, "ms": (time.time() - t0) * 1000})
                self.send_response(status)
                if ctype:
                    self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST

            def log_message(self, *a):
                pass

        super().__init__(("127.0.0.1", 0), H)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server_address[1]}"


class Sink(http.server.BaseHTTPRequestHandler):
    """Stands in for Loki while the generator seeds VictoriaLogs only."""

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ready")

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *a):
        pass


def seed_vl_only(st, seconds, end):
    sink = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Sink)
    threading.Thread(target=sink.serve_forever, daemon=True).start()
    env = dict(os.environ, LOKI_URL=f"http://127.0.0.1:{sink.server_address[1]}", VL_URL=st.vl, LOG_BATCH=str(stack.BACKFILL_BATCH),
               LOG_BACKFILL_SECONDS=str(seconds), LOG_BACKFILL_INTERVAL=str(stack.BACKFILL_INTERVAL),
               LOG_BACKFILL_END=str(end), LOG_BACKFILL_ONLY="1", LOG_BACKFILL_SEED=str(end), LOG_NO_LEVEL_LABEL="1")
    subprocess.run([sys.executable, stack.GENERATOR], cwd=stack.ROOT, env=env, check=True, stdout=sys.stderr)
    time.sleep(5)
    sink.shutdown()


def parse_builds(specs):
    out = []
    for s in specs:
        name, _, ref = s.partition("=")
        if not name or not ref:
            raise SystemExit(f"--build wants name=ref, got {s!r}")
        out.append((name, ref))
    return out


def query(proxy_url, q, end, rng):
    params = {"query": q, "start": end - rng, "end": end, "step": STEP}
    req = urllib.request.Request(proxy_url + "/loki/api/v1/query_range?" + urllib.parse.urlencode(params), headers={"X-Scope-OrgID": "0"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            status = r.status
            r.read()
    except urllib.error.HTTPError as e:
        status = e.code
    return status, (time.time() - t0) * 1000


class Run:
    def __init__(self, args):
        self.args = args
        self.out = os.path.abspath(args.out)
        os.makedirs(self.out, exist_ok=True)
        self.st = stack.Stack(args.project, args.loki_port, args.vl_port, args.loki_mem or None, args.vl_mem or None, no_level_label=True)
        self.builds = parse_builds(args.build)
        self.trees, self.proxies, self.tap = [], {}, None

    def setup(self):
        a = self.args
        self.st.up()
        subprocess.run(["docker", "update", "--cpus", str(a.vl_cpus), self.st.vl_container], check=True, stdout=subprocess.DEVNULL)
        # data ends well in the past: the part of a window younger than five minutes is probed again
        # by every request and would blur a first view
        self.end = int(time.time()) // 60 * 60 - 600
        seconds = int(a.hours * 3600)
        jobs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.builds) + 1) as pool:
            seeding = pool.submit(seed_vl_only, self.st, seconds, self.end)
            for name, ref in self.builds:
                binary = os.path.join(self.out, f"proxy-{name}")
                jobs.append((name, pool.submit(stack.build, ref if ref != "WORKTREE" else None, binary), binary))
            for name, fut, binary in jobs:
                self.trees.append((name, fut.result(), binary))
            seeding.result()
        stack.log(f"seeded {seconds}s ending {self.end}")
        self.sha = {name: subprocess.check_output(["git", "rev-parse", ref], cwd=stack.ROOT, text=True).strip() for name, ref in self.builds}

    def start(self, name, backend):
        tree, binary = next((t, b) for n, t, b in self.trees if n == name)
        self.stop(name)
        work = os.path.join(self.out, f"work-{name}")
        os.makedirs(work, exist_ok=True)
        cmd, env = stack.proxy_command(tree, SERVICE, self.args.proxy_port, backend, work)
        p = stack.Proxy(name, binary, cmd, env, self.args.proxy_port, self.out)
        p.start()
        self.proxies[name] = p
        return p

    def stop(self, name):
        p = self.proxies.pop(name, None)
        if p:
            p.stop()

    def teardown(self):
        for name in list(self.proxies):
            self.stop(name)
        for _, tree, _ in self.trees:
            stack.remove_tree(tree)
        try:
            self.st.down()
        except Exception as e:  # noqa: BLE001
            stack.log(f"down failed: {e}")

    def cold(self):
        a = self.args
        runs = {k: int(v) for k, v in (x.split("=") for x in a.runs.split(","))}
        cpu = CPU(self.st.vl_container)
        samples = []
        restarts0 = self.st.restart_count()
        for rng_name, n in runs.items():
            for shape, q in SHAPES.items():
                for rnd in range(n):
                    for name, _ in self.builds:
                        p = self.start(name, self.st.vl)
                        query(p.url, WARM, self.end, 60)
                        c0 = cpu.ms()
                        status, wall = query(p.url, q, self.end, RANGES[rng_name])
                        c1 = cpu.ms()
                        samples.append({"build": name, "shape": shape, "range": rng_name, "round": rnd, "status": status,
                                        "wall_ms": round(wall, 1), "cpu_ms": round(c1 - c0, 1)})
                        stack.log(f"{name} {shape} {rng_name} #{rnd}: {wall:.0f} ms, {c1 - c0:.0f} CPU-ms")
                        self.stop(name)
        summary = []
        for rng_name in runs:
            for shape in SHAPES:
                for name, _ in self.builds:
                    s = [x for x in samples if (x["build"], x["shape"], x["range"]) == (name, shape, rng_name)]
                    w, c = [x["wall_ms"] for x in s], [x["cpu_ms"] for x in s]
                    summary.append({"build": name, "shape": shape, "range": rng_name, "n": len(s),
                                    "wall_ms": {"median": round(statistics.median(w), 1), "min": min(w), "max": max(w)},
                                    "cpu_ms": {"median": round(statistics.median(c), 1), "min": min(c), "max": max(c)}})
        self.write("coldrestart.json", {"samples": samples, "summary": summary, "vl_restarts": [restarts0, self.st.restart_count()]})

    def classify(self, form):
        q = form.get("query", "")
        if " as p0, " in q:
            return "presence"
        if q.endswith('filter _stream:{detected_level=~".+"} | limit 1'):
            return "stream guard"
        if q.endswith("limit 1"):
            if "__lr_" in q:
                return "unpack probe"
            return "partial-parse probe" if "unpack_json fields" in q else "keys probe"
        if "copy detected_level as __dl_d" in q:
            return "stored-level query"
        return "chain" if "__dl" in q else "other"

    def shifted(self, form, minutes):
        out = dict(form)
        for k in ("start", "end"):
            if k in out:
                t = datetime.datetime.fromisoformat(out[k].replace("Z", "+00:00")) - datetime.timedelta(minutes=minutes)
                out[k] = t.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"
        return out

    def probes(self):
        a = self.args
        cpu = CPU(self.st.vl_container)
        self.tap = Tap(self.st.vl)
        result = {}
        for name, _ in self.builds:
            self.tap.log.clear()
            p = self.start(name, self.tap.url)
            query(p.url, WARM, self.end, 60)
            self.tap.log.clear()
            query(p.url, SHAPES["D"], self.end, RANGES["1h"])
            seen = {}
            for rec in sorted(self.tap.log, key=lambda r: r["t"]):
                if rec["path"].endswith(("stream_field_names", "stream_field_values", "field_names")):
                    continue
                kind = self.classify(rec["form"])
                seen.setdefault(kind, {"requests": 0, "first": rec})["requests"] += 1
            self.stop(name)
            kinds = {}
            for kind, info in seen.items():
                rec = info["first"]
                reps = []
                for phase in ("warm", "measured"):
                    for i in range(a.replays):
                        form = self.shifted(rec["form"], i)
                        data = urllib.parse.urlencode(form).encode()
                        c0, t0 = cpu.ms(), time.time()
                        urllib.request.urlopen(urllib.request.Request(self.st.vl + rec["path"], data=data), timeout=300).read()
                        if phase == "measured":
                            reps.append({"wall_ms": round((time.time() - t0) * 1000, 1), "cpu_ms": round(cpu.ms() - c0, 1)})
                kinds[kind] = {"requests_in_a_first_view": info["requests"], "query_bytes": len(rec["form"]["query"]),
                               "wall_ms_median": round(statistics.median(r["wall_ms"] for r in reps), 1),
                               "cpu_ms_median": round(statistics.median(r["cpu_ms"] for r in reps), 1), "replays": reps}
            result[name] = kinds
        self.write("probes.json", {"shape": "D", "range": "1h", "builds": result})

    def write(self, name, body):
        argv = sys.argv[1:]
        argv = [("<dir>" if argv[i - 1] == "--out" else a) for i, a in enumerate(argv)]
        doc = {"command": ["bench/ab/" + os.path.basename(sys.argv[0])] + argv, "date": time.strftime("%Y-%m-%d"),
               "vl_cpus": self.args.vl_cpus, "data": {"hours": self.args.hours, "end": self.end, "rows_without_stored_level": True},
               "builds": self.sha, **body}
        with open(os.path.join(self.out, name), "w") as f:
            json.dump(doc, f, indent=1)
            f.write("\n")
        stack.log(f"wrote {os.path.join(self.out, name)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["cold", "probes"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--build", action="append", required=True, help="name=git ref (HEAD for the checked-out commit); repeatable")
    ap.add_argument("--runs", default="1h=7,24h=5", help="cold: samples per range")
    ap.add_argument("--replays", type=int, default=10, help="probes: replays per request")
    ap.add_argument("--hours", type=float, default=25, help="hours of data to seed")
    ap.add_argument("--vl-cpus", type=float, default=2)
    ap.add_argument("--proxy-port", type=int, default=27190)
    stack.stack_args(ap)
    args = ap.parse_args()
    run = Run(args)
    try:
        run.setup()
        run.cold() if args.cmd == "cold" else run.probes()
    finally:
        run.teardown()


if __name__ == "__main__":
    main()
