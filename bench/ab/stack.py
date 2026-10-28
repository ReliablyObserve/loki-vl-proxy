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
import re
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
# --loki-uncached: Loki with every query results cache off, a cold reference.
LOKI_UNCACHED_OVERLAY = "bench/ab/docker-compose.ab-loki-uncached.yml"
# The two Loki profiles a proxy build is compared with (README, "Like for like"):
#   aligned        the stack's Loki as test/e2e-compat/loki-local-config.yaml configures it
#                  (align_queries_with_step on, max_cache_freshness_per_query 0s); every proxy build
#                  that supports -align-queries-with-step runs with it on
#   loki-defaults  Loki's own defaults (align off, freshness 10m), through an overlay that changes
#                  neither the shared config nor the e2e stack; proxy builds run with their defaults
PROFILES = ("aligned", "loki-defaults")
LOKI_DEFAULTS_OVERLAY = "bench/ab/docker-compose.ab-loki-defaults.yml"
PROFILE_OVERLAYS = {"aligned": [], "loki-defaults": [LOKI_DEFAULTS_OVERLAY]}
# The proxy flag that moves a metric query_range request onto the step grid, as Loki's frontend does.
ALIGN_FLAG = "-align-queries-with-step"
ALIGN_FLAG_ENABLED = f"{ALIGN_FLAG}=true"
GENERATOR = "test/e2e-compat/log-generator.py"
# Data profile of the backfill. Fixed so every run measures the same density:
# about 75 lines/s across the generator's services (the live generator's rate
# on the e2e stack), one batch -- a new set of pod streams -- per 10 s.
BACKFILL_BATCH = 60
BACKFILL_INTERVAL = 10
SELECTOR = '{env="production"}'


def count_query(slice_s):
    """The seed's Loki line count, unique per call.

    Loki's results cache keys on the query string and ignores Cache-Control:
    no-cache on query_range (Loki 3.7.7), so polling one fixed query keeps
    returning the first, partial answer for slices Loki could not see yet. A line
    filter on a string no line contains changes the cache key per poll and not
    the count.
    """
    return f'sum(count_over_time({SELECTOR} != "seed-count-{time.time_ns()}" [{slice_s}s]))'


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


# What Loki's /config shows for each profile.
PROFILE_SETTINGS = {"aligned": {"align_queries_with_step": True, "max_cache_freshness_per_query": "0s"},
                    "loki-defaults": {"align_queries_with_step": False, "max_cache_freshness_per_query": "10m"}}


def profile_check(profile):
    if profile not in PROFILES:
        raise ValueError(f"unknown Loki profile {profile!r}; one of {', '.join(PROFILES)}")
    return profile


# (top-level section of Loki's /config, key) of each setting the profiles differ in. align_queries_with_step
# also appears under another section, so the section has to be named.
PROFILE_KEYS = {"align_queries_with_step": ("query_range", "align_queries_with_step"),
                "max_cache_freshness_per_query": ("limits_config", "max_cache_freshness_per_query")}


def profile_settings(config_yaml):
    """The settings the profiles differ in, from Loki's /config (YAML text); a missing one is None."""
    sections, current = {}, None
    for line in config_yaml.splitlines():
        if line and not line[0].isspace() and line.rstrip().endswith(":") and not line.startswith("#"):
            current = line.rstrip()[:-1]
        elif current and (m := re.match(r"^  ([A-Za-z0-9_]+):\s+(\S+)\s*$", line)):
            sections.setdefault(current, {}).setdefault(m.group(1), m.group(2))
    out = {}
    for name, (section, key) in PROFILE_KEYS.items():
        value = sections.get(section, {}).get(key)
        out[name] = {"true": True, "false": False}.get(value, value)
    return out


class Stack:
    def __init__(self, project, loki_port=23101, vl_port=29428, loki_mem=None, vl_mem=None, root=ROOT, overlays=(),
                 no_level_label=False, profile="aligned"):
        self.project, self.loki_port, self.vl_port, self.root = project, loki_port, vl_port, root
        self.no_level_label = no_level_label  # seed rows whose level lives only in the line
        self.extra_overlays = list(overlays)  # further compose files after OVERLAY (relative to root)
        self.profile = profile_check(profile)
        if self.extra_overlays and self.profile != "aligned":
            raise ValueError("--loki-uncached replaces Loki's whole command; it cannot be combined with the "
                             "loki-defaults profile")
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

    @property
    def overlays(self):
        """Compose files after OVERLAY: the profile's, then the extra ones."""
        return [*PROFILE_OVERLAYS[self.profile], *self.extra_overlays]

    def compose(self, *args, capture=False):
        files = [x for f in (BASE_COMPOSE, OVERLAY, *self.overlays) for x in ("-f", f)]
        cmd = ["docker", "compose", "-p", self.project, *files, *args]
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

    def set_profile(self, profile, window=None, timeout=900):
        """Move the stack's Loki to `profile`; the seeded data and VictoriaLogs stay.

        Loki is recreated with the profile's overlay (its results cache is in memory, so the new Loki starts
        with every cache empty) on the same named volume, so the data written by seed() is still there.
        Returns the Loki config values the profile is read back from /config (profile_settings), and with
        window=(start, end) waits until Loki and VictoriaLogs count the same lines again.
        """
        profile = profile_check(profile)
        if profile == self.profile:
            return self.loki_settings()
        if self.extra_overlays:
            raise RuntimeError("--loki-uncached replaces Loki's whole command; it cannot be combined with moving "
                               "the stack between Loki profiles")
        self.profile = profile
        self.compose("up", "-d", "--no-deps", "--force-recreate", "loki")
        wait_http(f"{self.loki}/ready", 180, "Loki")
        settings = self.loki_settings()
        if settings != PROFILE_SETTINGS[profile]:
            raise RuntimeError(f"Loki profile {profile}: /config says {settings}, expected {PROFILE_SETTINGS[profile]}")
        if window:
            self.wait_equal_counts(window[0], window[1], timeout)
        log(f"Loki profile {profile}: {settings}")
        return settings

    def loki_settings(self):
        """The two settings that differ between the profiles, as Loki reports them on /config."""
        _, body = http_get(f"{self.loki}/config", timeout=10)
        return profile_settings(body.decode())

    def restart_count(self):
        out = subprocess.check_output(["docker", "inspect", self.vl_container, "--format", "{{.RestartCount}}"], text=True)
        return int(out.strip())

    def seed(self, seconds, end, timeout=900, batch=BACKFILL_BATCH, interval=BACKFILL_INTERVAL):
        env = dict(os.environ, LOKI_URL=self.loki, VL_URL=self.vl, LOG_BATCH=str(batch),
                   LOG_BACKFILL_SECONDS=str(seconds), LOG_BACKFILL_INTERVAL=str(interval),
                   LOG_BACKFILL_END=str(end), LOG_BACKFILL_ONLY="1", LOG_BACKFILL_SEED=str(end))
        if self.no_level_label:
            env["LOG_NO_LEVEL_LABEL"] = "1"
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
        q = count_query(slice_s)
        params = urllib.parse.urlencode({"query": q, "start": first, "end": last, "step": slice_s})
        _, body = http_get(f"{self.loki}/loki/api/v1/query_range?{params}", timeout=120,
                           headers={"X-Scope-OrgID": "0"})
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


def probe_align_support(binary, timeout=20):
    """Whether a proxy build knows -align-queries-with-step.

    An unknown flag makes the process exit, so a build is asked before it is given the flag: with the flag and
    -help, a build that knows it stops at -help ("flag: help requested") and one that does not stops at the flag
    ("flag provided but not defined"). Only the first answer counts as support; anything else (a timeout, other
    output) is "unsupported", which marks the build's aligned-profile rows not like-for-like instead of
    guessing. The exit status is not read: -help exits 1.
    """
    try:
        out = subprocess.run([binary, ALIGN_FLAG_ENABLED, "-help"], capture_output=True, text=True, timeout=timeout,
                             stdin=subprocess.DEVNULL)
    except (subprocess.SubprocessError, OSError):
        return False
    text = out.stdout + out.stderr
    return "help requested" in text and "provided but not defined" not in text


def align_args(args, profile, supported):
    """(args, state): a proxy's command line with -align-queries-with-step set for the Loki profile.

    The build's own stack file may already carry the flag (the e2e stack's proxies run with it), so any
    occurrence is removed first and the profile decides:
      aligned        "on"           the flag is added   (the stack's Loki aligns queries with the step)
                     "unsupported"  the build has no such flag; nothing is added. The build does not do what
                                    the stack's Loki does, so its aligned rows are not like-for-like
      loki-defaults  "off"          no flag, the build's default (Loki's own default is off too)
    """
    profile_check(profile)
    rest = [a for a in args if a.partition("=")[0] != ALIGN_FLAG]
    if profile == "loki-defaults":
        return rest, "off"
    return (rest + [ALIGN_FLAG_ENABLED], "on") if supported else (rest, "unsupported")


def like_for_like(profile, align_state):
    """A proxy build is compared with the Loki profile only when it ran the matching setting."""
    return profile_check(profile) != "aligned" or align_state == "on"


def start_proxies(specs, out, st, service, proxies, profile="aligned", supported=None):
    """Start (name, binary, tree, port) builds with their tree's stack flags for `profile`; appends to `proxies` first.

    supported maps a build's name to whether its binary knows -align-queries-with-step (probe_align_support);
    each started Proxy records its `align` state (align_args). Each profile has its own work directory and log.
    """
    supported = supported if supported is not None else {name: probe_align_support(binary) for name, binary, _, _ in specs}
    for name, binary, tree, port in specs:
        work = os.path.join(out, f"work-{name}-{profile}")
        os.makedirs(work, exist_ok=True)
        cmd, env = proxy_command(tree, service, port, st.vl, work)
        cmd, state = align_args(cmd, profile, supported[name])
        proxy = Proxy(name, binary, cmd, env, port, out, profile, state)
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

    def __init__(self, name, binary, args, env, port, log_dir, profile="aligned", align="off"):
        self.name, self.binary, self.args, self.env, self.port = name, binary, args, env, port
        self.profile, self.align = profile, align  # the Loki profile it runs for, and align_args' state
        self.log_path = os.path.join(log_dir, f"proxy-{name}-{profile}.log")
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


def stack_args(ap, profile=True):
    ap.add_argument("--project", default="ab-smoke", help="compose project and container-name prefix")
    ap.add_argument("--loki-port", type=int, default=23101)
    ap.add_argument("--vl-port", type=int, default=29428)
    ap.add_argument("--loki-mem", default="", help="Loki memory limit, e.g. 4g (default: the base stack's)")
    ap.add_argument("--vl-mem", default="", help="VictoriaLogs memory limit (default: the base stack's)")
    ap.add_argument("--loki-uncached", action="store_true",
                    help="start Loki with every query results cache off (docker-compose.ab-loki-uncached.yml), so its "
                         "first request of a window is computed, as a cold reference for the proxies' run 0")
    ap.add_argument("--no-level-label", action="store_true",
                    help="seed rows with no stored level (the level only in the line), for the detected_level routes")
    if profile:  # pr_smoke.py and daily.py choose their profiles with --profiles and move the stack between them
        ap.add_argument("--profile", choices=PROFILES, default="aligned",
                        help="Loki profile the stack starts in: aligned = the stack's Loki as configured, "
                             "loki-defaults = Loki's own defaults for align_queries_with_step and "
                             "max_cache_freshness_per_query (docker-compose.ab-loki-defaults.yml)")


def stack_from(args):
    return Stack(args.project, args.loki_port, args.vl_port, args.loki_mem or None, args.vl_mem or None,
                 overlays=[LOKI_UNCACHED_OVERLAY] if getattr(args, "loki_uncached", False) else [],
                 no_level_label=args.no_level_label, profile=getattr(args, "profile", "aligned"))


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
