#!/usr/bin/env python3
"""A fresh, isolated A/B stack: Loki and VictoriaLogs in Docker, proxy builds as host processes.

Used by pr_smoke.py (every pull request) and daily.py (the scheduled run from
main); run either locally to reproduce CI. The stack is a separate compose
project (docker-compose.ab.yml gives it its own container names and ports), so
it can run next to the e2e stack without touching it.

  stack.py up      --project ab-smoke            start Loki + VictoriaLogs
  stack.py seed    --project ab-smoke --seconds 3900 --end <unix>
  stack.py down    --project ab-smoke            remove containers and volumes

The steps, in order:
  1. `up` starts only Loki and VictoriaLogs from test/e2e-compat/docker-compose.yml.
  2. `seed` runs test/e2e-compat/log-generator.py in backfill mode: the live
     generator's data profile, written for a fixed window ending at --end, to
     both backends. It then flushes Loki and waits until Loki and
     VictoriaLogs count the same lines in every 10-minute slice of the window
     (Loki reads data older than query_ingesters_within from its store only,
     so an unflushed backfill would look missing on Loki).
  3. Each proxy build runs as a host process with the command line and
     environment of the stack's proxy service (--service) as that build's own
     compose file defines it, rewritten to host ports; flags pointing at
     services this stack does not start are dropped.
"""
import argparse
import concurrent.futures
import contextlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
BASE_COMPOSE = "test/e2e-compat/docker-compose.yml"
OVERLAY = "bench/ab/docker-compose.ab.yml"
GENERATOR = "test/e2e-compat/log-generator.py"
# Data profile of the backfill. Fixed so every run measures the same density:
# about 75 lines/s across the generator's services (the live generator's rate
# on the e2e stack), one batch -- a new set of pod streams -- per 10 s.
BACKFILL_BATCH = 60
BACKFILL_INTERVAL = 10
SELECTOR = '{env="production"}'


def log(msg):
    print(f"[stack] {msg}", file=sys.stderr, flush=True)


def http_get(url, timeout=10, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def wait_http(url, deadline_s, what):
    deadline, last = time.time() + deadline_s, None
    while time.time() < deadline:
        try:
            status, _ = http_get(url, timeout=3)
            if status == 200:
                return
        except (urllib.error.URLError, OSError) as e:
            last = e
        time.sleep(1)
    raise RuntimeError(f"{what} not ready at {url}: {last}")


def port_free(port):
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


class Stack:
    def __init__(self, project, loki_port=23101, vl_port=29428, loki_mem=None, vl_mem=None, root=ROOT):
        self.project, self.loki_port, self.vl_port, self.root = project, loki_port, vl_port, root
        self.env = dict(os.environ, AB_PROJECT=project, AB_LOKI_PORT=str(loki_port), AB_VL_PORT=str(vl_port))
        if loki_mem:
            # GOMEMLIMIT at ~80% of the cgroup limit, as the base stack sets it.
            gib = float(loki_mem.rstrip("gG"))
            self.env.update(AB_LOKI_MEM=loki_mem, AB_LOKI_GOMEMLIMIT=f"{int(gib * 0.8 * 1024)}MiB")
        if vl_mem:
            self.env["AB_VL_MEM"] = vl_mem
        self.loki = f"http://127.0.0.1:{loki_port}"
        self.vl = f"http://127.0.0.1:{vl_port}"
        self.vl_container = f"{project}-victorialogs"
        self.loki_container = f"{project}-loki"

    def compose(self, *args, capture=False):
        cmd = ["docker", "compose", "-p", self.project, "-f", BASE_COMPOSE, "-f", OVERLAY, *args]
        if capture:
            return subprocess.run(cmd, cwd=self.root, env=self.env, check=True, capture_output=True, text=True).stdout
        subprocess.run(cmd, cwd=self.root, env=self.env, check=True)
        return None

    def up(self):
        for port in (self.loki_port, self.vl_port):
            if not port_free(port):
                raise RuntimeError(f"port {port} is already in use; pick another with --loki-port/--vl-port")
        self.compose("up", "-d", "--no-deps", "loki", "victorialogs")
        wait_http(f"{self.vl}/health", 180, "VictoriaLogs")
        wait_http(f"{self.loki}/ready", 180, "Loki")
        log(f"up: Loki {self.loki}, VictoriaLogs {self.vl} (project {self.project})")

    def down(self):
        self.compose("down", "-v", "--remove-orphans")

    def restart_count(self):
        out = subprocess.check_output(["docker", "inspect", self.vl_container, "--format", "{{.RestartCount}}"], text=True)
        return int(out.strip())

    def seed(self, seconds, end, timeout=900, batch=BACKFILL_BATCH, interval=BACKFILL_INTERVAL):
        env = dict(os.environ, LOKI_URL=self.loki, VL_URL=self.vl, LOG_BATCH=str(batch),
                   LOG_BACKFILL_SECONDS=str(seconds), LOG_BACKFILL_INTERVAL=str(interval),
                   LOG_BACKFILL_END=str(end), LOG_BACKFILL_ONLY="1", LOG_BACKFILL_SEED=str(end))
        t0 = time.time()
        subprocess.run([sys.executable, GENERATOR], cwd=self.root, env=env, check=True, stdout=sys.stderr)
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{self.loki}/flush", method="POST"), timeout=60):
                pass  # the response body is empty; only the status matters
        except (urllib.error.URLError, OSError) as e:
            log(f"Loki /flush failed ({e}); waiting for the counts anyway")
        self.wait_equal_counts(end - seconds, end, timeout)
        log(f"seeded {seconds}s ending {end} in {time.time() - t0:.0f}s")

    def counts(self, start, end, slice_s):
        """Lines per slice (T-slice, T] on each backend, for every step-aligned T in (start, end].

        Loki aligns query_range to the step, so the slices sit on multiples of
        slice_s; the part of the window outside them is covered by the
        per-shape result parity check of the run itself.
        """
        first, last = (start // slice_s + 1) * slice_s, end // slice_s * slice_s
        q = f"sum(count_over_time({SELECTOR}[{slice_s}s]))"
        params = urllib.parse.urlencode({"query": q, "start": first, "end": last, "step": slice_s})
        _, body = http_get(f"{self.loki}/loki/api/v1/query_range?{params}", timeout=120,
                           headers={"X-Scope-OrgID": "0", "Cache-Control": "no-cache"})
        loki = {}
        for series in json.loads(body)["data"]["result"]:
            for ts, v in series["values"]:
                loki[int(float(ts))] = int(float(v))
        vl = {}
        for t in range(first, last + 1, slice_s):
            # Nanosecond digits: LogsQL rounds a time range end up to the
            # precision it is written in, so "07:00:00Z" would include 07:00:00.5.
            lo, hi = (time.strftime("%Y-%m-%dT%H:%M:%S.000000000Z", time.gmtime(x)) for x in (t - slice_s, t))
            query = f'{SELECTOR} _time:({lo}, {hi}] | stats count() c'
            _, body = http_get(f"{self.vl}/select/logsql/query?" + urllib.parse.urlencode({"query": query}), timeout=120)
            line = body.decode().strip().splitlines()
            vl[t] = int(json.loads(line[0])["c"]) if line else 0
        return loki, vl

    def wait_equal_counts(self, start, end, timeout, slice_s=600):
        deadline, last = time.time() + timeout, None
        while time.time() < deadline:
            try:
                loki, vl = self.counts(start, end, slice_s)
            except (urllib.error.URLError, OSError, ValueError, KeyError) as e:
                last = f"count query failed: {e}"
            else:
                diff = {t: (loki.get(t, 0), n) for t, n in vl.items() if loki.get(t, 0) != n}
                if not diff and sum(vl.values()) > 0:
                    log(f"Loki and VictoriaLogs agree: {sum(vl.values())} lines in {len(vl)} slices")
                    return
                last = f"{len(diff)} of {len(vl)} slices differ (loki, vl): " + ", ".join(
                    f"{t}={a}/{b}" for t, (a, b) in sorted(diff.items())[:4])
            time.sleep(10)
        raise RuntimeError(f"Loki and VictoriaLogs did not converge in {timeout}s: {last}")


def proxy_command(compose_root, service, listen_port, backend, work_dir):
    """The command line and environment of `service` in compose_root's stack, rewritten for a host process."""
    out = subprocess.run(["docker", "compose", "-f", BASE_COMPOSE, "config", "--format", "json"], cwd=compose_root,
                         check=True, capture_output=True, text=True).stdout
    services = json.loads(out)["services"]
    svc = services[service]
    others = [name for name in services if name != "victorialogs"]
    args = []
    for arg in svc.get("command") or []:
        name, _, value = arg.partition("=")
        if name == "-listen":
            continue
        if name == "-backend":
            value = backend
        elif any(f"//{other}:" in value or value.startswith(f"{other}:") for other in others):
            continue  # vmalert, peers: services this stack does not start
        elif value.startswith("/cache/"):
            value = os.path.join(work_dir, os.path.basename(value))
        args.append(f"{name}={value}" if value or "=" in arg else name)
    args += [f"-listen=127.0.0.1:{listen_port}", f"-admin-listen=127.0.0.1:{listen_port + 1}"]
    env = {k: str(v) for k, v in (svc.get("environment") or {}).items()}
    return args, env


def build(ref, out, root=ROOT):
    """Build ./cmd/proxy at a git ref (or the working tree when ref is None) into `out`.

    Returns the tree it built from; a worktree it created is removed again
    when the build fails.
    """
    if ref is None:
        subprocess.run(["go", "build", "-o", out, "./cmd/proxy"], cwd=root, check=True)
        return root
    tree = tempfile.mkdtemp(prefix="ab-tree-")
    try:
        subprocess.run(["git", "worktree", "add", "--detach", "--force", tree, ref], cwd=root, check=True,
                       stdout=subprocess.DEVNULL)
        subprocess.run(["go", "build", "-o", out, "./cmd/proxy"], cwd=tree, check=True)
    except BaseException:
        remove_tree(tree, root)
        raise
    return tree


def remove_tree(tree, root=ROOT):
    if tree and tree != root:
        subprocess.run(["git", "worktree", "remove", "--force", tree], cwd=root, check=False)
        shutil.rmtree(tree, ignore_errors=True)


def build_and_seed(st, builds, seed_args):
    """Build every (name, ref, out) while the stack seeds; returns {name: tree}.

    A failed build stops the run at once instead of waiting for the seed (the
    seed thread ends on its own when the stack goes down). Trees of the builds
    that succeeded are returned through the exception's `trees` attribute so
    the caller can remove them.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(builds) + 1)
    seeding = pool.submit(st.seed, *seed_args)
    futures = {pool.submit(build, ref, out): name for name, ref, out in builds}
    trees = {}
    try:
        for future in concurrent.futures.as_completed(futures):
            trees[futures[future]] = future.result()
        seeding.result()
    except BaseException as e:
        for future, name in futures.items():
            if future.done() and future.exception() is None:
                trees[name] = future.result()
        e.trees = trees
        raise
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return trees


def start_proxies(specs, out, st, service, proxies):
    """Start (name, binary, tree, port) builds with their tree's stack flags; appends to `proxies` first."""
    for name, binary, tree, port in specs:
        work = os.path.join(out, f"work-{name}")
        os.makedirs(work, exist_ok=True)
        cmd, env = proxy_command(tree, service, port, st.vl, work)
        proxy = Proxy(name, binary, cmd, env, port, out)
        proxies.append(proxy)  # before start(): a start that fails is still stopped
        proxy.start()


def teardown(proxies, trees, st, keep_stack=False):
    """Stop everything; one failing step never skips the others."""
    steps = [(p.stop, ()) for p in proxies] + [(remove_tree, (t,)) for t in trees]
    if not keep_stack:
        steps.append((st.down, ()))
    for fn, args in steps:
        try:
            fn(*args)
        except Exception as e:  # noqa: BLE001 - cleanup must go on
            log(f"cleanup step {getattr(fn, '__name__', fn)} failed: {e}")


class Proxy:
    """One proxy build as a host process on a port proven to be its own."""

    def __init__(self, name, binary, args, env, port, log_dir):
        self.name, self.binary, self.args, self.env, self.port = name, binary, args, env, port
        self.log_path = os.path.join(log_dir, f"proxy-{name}.log")
        self.proc = None
        self.url = f"http://127.0.0.1:{port}"

    def start(self):
        for p in (self.port, self.port + 1):
            if not port_free(p):
                raise RuntimeError(f"port {p} for '{self.name}' is taken: a stale process would answer for this build")
        with open(self.log_path, "w") as logf:
            self.proc = subprocess.Popen([self.binary, *self.args], env=dict(os.environ, **self.env),
                                         stdout=logf, stderr=subprocess.STDOUT, start_new_session=True)
        deadline, last_error = time.time() + 90, None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"proxy '{self.name}' exited with {self.proc.returncode}; see {self.log_path}")
            try:
                status, _ = http_get(f"{self.url}/ready", timeout=3)
                if status == 200:
                    log(f"proxy {self.name} ready on {self.url} (pid {self.proc.pid})")
                    return
                last_error = f"/ready answered {status}"
            except (urllib.error.URLError, OSError) as e:
                last_error = e  # still starting (label index warm-up, backend probe); retry until the deadline
            time.sleep(1)
        self.stop()
        raise RuntimeError(f"proxy '{self.name}' not ready in 90s (last: {last_error}); see {self.log_path}")

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def stop(self):
        if not self.alive():
            return
        # The process can exit between the alive() check and a signal; that
        # ProcessLookupError means it is already stopped, which is the goal.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                log(f"proxy {self.name} ignored SIGTERM for 20s; killing it")
                os.killpg(self.proc.pid, signal.SIGKILL)


def stack_args(ap):
    ap.add_argument("--project", default="ab-smoke", help="compose project and container-name prefix")
    ap.add_argument("--loki-port", type=int, default=23101)
    ap.add_argument("--vl-port", type=int, default=29428)
    ap.add_argument("--loki-mem", default="", help="Loki memory limit, e.g. 4g (default: the base stack's)")
    ap.add_argument("--vl-mem", default="", help="VictoriaLogs memory limit (default: the base stack's)")


def stack_from(args):
    return Stack(args.project, args.loki_port, args.vl_port, args.loki_mem or None, args.vl_mem or None)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("up", "down", "seed"):
        stack_args(sub.add_parser(name))
    seed = sub.choices["seed"]
    seed.add_argument("--seconds", type=int, required=True)
    seed.add_argument("--end", type=int, default=0, help="unix seconds; default now, floored to the minute")
    seed.add_argument("--timeout", type=int, default=900)
    args = ap.parse_args()
    stack = stack_from(args)
    if args.cmd == "up":
        stack.up()
    elif args.cmd == "down":
        stack.down()
    else:
        stack.seed(args.seconds, args.end or int(time.time()) // 60 * 60, args.timeout)


if __name__ == "__main__":
    main()
