#!/usr/bin/env python3
"""Local CPU and memory comparison: the same read workload sent to Loki and to the proxy plus VictoriaLogs.

The e2e-compat stack (test/e2e-compat/docker-compose.yml) runs Loki, VictoriaLogs and the proxy variants on one
machine, all fed by the same ingest. For each side, one after the other (never both at once):

  CPU      user + system time of the side's containers during the workload, from /proc/1/stat read through a
           busybox sidecar in the container's PID namespace (works for scratch images that have no shell), minus
           the container's idle rate measured for --idle seconds just before, since ingest keeps running;
  memory   the resident set (VmRSS of PID 1), sampled every 0.5 s during the workload, peak per container.

Every request is a cold miss: each carries its own step or limit, so no cache answers it. The workload is
the same list of queries over the same ranges, sent by --workers concurrent workers like a few dashboards
refreshing; each side runs it twice (--rounds), the second time under the name "<side>-again".

  python3 bench/resources/compare.py --docker-context colima-lvp          # the default stack, writes results/latest.json
  python3 conformance/scripts/resource_comparison.py --readme README.md --docs docs/benchmarks.md   # then render

This is a local, single-machine test. The pages state that, and point to the production cost case for the
figures at scale. The renderer refuses to print numbers without the date, commit, images and host this file records.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
OUT = os.path.join(ROOT, "bench", "resources", "results", "latest.json")
SERVICE = '{service_name="api-gateway"}'
QUERIES = (
    ("range", f"sum by (level) (count_over_time({SERVICE} [5m]))"),
    ("range", 'sum by (service_name) (count_over_time({service_name=~".+"} [5m]))'),
    ("range", f"sum(rate({SERVICE} [5m]))"),
    ("range", f"sum(bytes_over_time({SERVICE} [5m]))"),
    ("range", 'topk(5, sum by (service_name) (rate({service_name=~".+"} [5m])))'),
    ("range", f"sum by (level) (count_over_time({SERVICE} | json [5m]))"),
    ("range", f"sum by (service_name) (avg_over_time({SERVICE} | json | unwrap duration_ms [5m]))"),
    ("log", f"{SERVICE} | json"),
    ("log", '{service_name="nginx-ingress"} | logfmt'),
)
RANGES = (900, 1800, 3600)
WORKLOAD = (f"{len(QUERIES)} queries (count, rate, bytes, topk and unwrap metrics, plain and json/logfmt log queries) "
            f"over {', '.join(str(r // 60) + ' min' for r in RANGES)}, three times each, 8 concurrent workers, "
            "every request a cold miss")


class Docker:
    def __init__(self, context):
        self.base = ["docker"] + (["--context", context] if context else [])
        self.sidecars = {}

    def run(self, *args):
        return subprocess.run(self.base + list(args), capture_output=True, text=True).stdout.strip()

    def sidecar(self, container):
        """A busybox sidecar in the container's PID namespace; it reads /proc/1 of the container."""
        if container not in self.sidecars:
            name = "res-" + container
            self.run("rm", "-f", name)
            self.run("run", "-d", "--name", name, "--pid", "container:" + container, "busybox:1.36", "sleep", "7200")
            self.sidecars[container] = name
        return self.sidecars[container]

    def cpu_usec(self, container):
        out = self.run("exec", self.sidecar(container), "cat", "/proc/1/stat")
        try:
            fields = out.rsplit(")", 1)[1].split()
            return (int(fields[11]) + int(fields[12])) * 10000  # utime + stime ticks (100 Hz) to microseconds
        except (IndexError, ValueError):
            return None

    def rss_bytes(self, container):
        out = self.run("exec", self.sidecar(container), "grep", "VmRSS", "/proc/1/status")
        try:
            return int(out.split()[1]) * 1024
        except (IndexError, ValueError):
            return None

    def cleanup(self):
        for name in self.sidecars.values():
            self.run("rm", "-f", name)

    def image(self, container):
        tag, _, image_id = self.run("inspect", "--format", "{{.Config.Image}} {{.Image}}", container).partition(" ")
        return f"{tag} {image_id.replace('sha256:', '')[:12]}".strip()

    def host(self):
        info = self.run("info", "--format", "{{.OperatingSystem}}|{{.NCPU}}|{{.MemTotal}}").split("|")
        try:
            return {"cpu": f"{int(info[1])} CPUs", "ram": f"{int(info[2]) / 2**30:.1f} GiB", "vm": info[0]}
        except (IndexError, ValueError):
            return {"vm": "|".join(info)}


def run_workload(base, headers, workers):
    now = int(time.time())
    codes, lock = {}, threading.Lock()
    jobs = []
    for _ in range(3):
        for kind, query in QUERIES:
            for rng in RANGES:
                jobs.append((len(jobs) + 1, kind, query, rng))

    def one(i, kind, query, rng):
        params = {"query": query, "start": now - rng, "end": now - 60}
        if kind == "range":
            params["step"] = max(15, rng // 1000) + i  # a distinct step keeps every request a cold miss
        else:
            params["limit"] = 1000 + i
        request = urllib.request.Request(base + "/loki/api/v1/query_range?" + urllib.parse.urlencode(params),
                                         headers=headers)
        try:
            code = urllib.request.urlopen(request, timeout=300).status
        except urllib.error.HTTPError as e:
            code = e.code
        except Exception as e:  # noqa: BLE001
            code = str(e)[:30]
        with lock:
            codes[code] = codes.get(code, 0) + 1

    queue = list(reversed(jobs))

    def worker():
        while True:
            with lock:
                if not queue:
                    return
                job = queue.pop()
            one(*job)

    started = time.time()
    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.time() - started, codes, len(jobs)


def measure(docker, name, base, containers, headers, idle_s, workers):
    c0 = {c: docker.cpu_usec(c) for c in containers}
    time.sleep(idle_s)
    c1 = {c: docker.cpu_usec(c) for c in containers}
    idle = {c: (c1[c] - c0[c]) / (idle_s * 1e6) if None not in (c0[c], c1[c]) else None for c in containers}
    before = {c: docker.rss_bytes(c) for c in containers}
    peak = dict(before)
    stop = threading.Event()

    def sampler():
        while not stop.is_set():
            for c in containers:
                m = docker.rss_bytes(c)
                if m is not None and (peak[c] is None or m > peak[c]):
                    peak[c] = m
            stop.wait(0.5)

    sampling = threading.Thread(target=sampler)
    sampling.start()
    a = {c: docker.cpu_usec(c) for c in containers}
    seconds, codes, requests = run_workload(base, headers, workers)
    b = {c: docker.cpu_usec(c) for c in containers}
    stop.set()
    sampling.join()
    result = {"side": name, "requests": requests, "seconds": round(seconds, 1),
              "statuses": {str(k): v for k, v in codes.items()}, "containers": {}}
    for c in containers:
        used = (b[c] - a[c]) / 1e6 if None not in (a[c], b[c]) else None
        net = used - idle[c] * seconds if used is not None and idle[c] is not None else None
        result["containers"][c] = {
            "cpu_s_total": round(used, 2) if used is not None else None,
            "cpu_s_workload": round(net, 2) if net is not None else None,
            "idle_cores": round(idle[c], 3) if idle[c] is not None else None,
            "mem_before_mib": round(before[c] / 2**20) if before[c] else None,
            "mem_peak_mib": round(peak[c] / 2**20) if peak[c] else None}
    return result


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True).stdout.strip()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docker-context", default=os.environ.get("DOCKER_CONTEXT", ""), help="docker context of the stack")
    ap.add_argument("--loki-url", default="http://127.0.0.1:13101")
    ap.add_argument("--loki-container", default="e2e-loki")
    ap.add_argument("--proxy-url", default="http://127.0.0.1:13102")
    ap.add_argument("--proxy-container", default="e2e-proxy-underscore")
    ap.add_argument("--vl-container", default="e2e-victorialogs")
    ap.add_argument("--tenant", default="0", help="X-Scope-OrgID of the requests")
    ap.add_argument("--idle", type=int, default=30, help="seconds the idle CPU rate of a container is measured for")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--rounds", type=int, default=2, help="how many times each side runs the workload")
    ap.add_argument("--commit", default="", help="proxy commit the image was built from (default: HEAD)")
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args()
    docker = Docker(a.docker_context)
    headers = {"X-Scope-OrgID": a.tenant}
    sides = (("loki", a.loki_url, [a.loki_container]),
             ("proxy+victorialogs", a.proxy_url, [a.proxy_container, a.vl_container]))
    runs = []
    try:
        for round_ in range(a.rounds):
            for name, base, containers in sides:
                result = measure(docker, name + ("-again" if round_ else ""), base, containers, headers, a.idle, a.workers)
                print(json.dumps(result), flush=True)
                runs.append(result)
        images = {"loki": docker.image(a.loki_container), "proxy": docker.image(a.proxy_container),
                  "victorialogs": docker.image(a.vl_container)}
        host = docker.host()
    finally:
        docker.cleanup()
    results = {"date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
               "proxy_commit": a.commit or git("rev-parse", "--short=10", "HEAD"), "host": host, "images": images,
               "method": {"workload": WORKLOAD, "idle_s": a.idle, "workers": a.workers}, "sides": runs}
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"{a.out}: {len(runs)} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
