#!/usr/bin/env python3
"""Isolated visual-proof stack: Loki + VictoriaLogs (Docker), two proxy builds
(host processes) and a Grafana (Docker) with three datasources.

  stack.py up   --out OUT [--main-ref origin/main] [--pr-ref WORKTREE] [--hours 24]
  stack.py down --out OUT
  stack.py live-start|live-stop --out OUT   mirrored live generator (Live tail)

Datasources provisioned in Grafana (fixed uids, so specs never resolve names):
  vp-main  "Loki (via VL proxy main)"  proxy built from --main-ref
  vp-pr    "Loki (via VL proxy)"       proxy built from the working tree (the PR)
  vp-loki  "Loki (direct)"             Loki, the reference
The last two carry the names of the e2e stack's datasources, so the e2e-ui
specs also run against this stack.

Data is seeded once with a fixed end (state.json: end) and no live generator
runs, so every datasource answers the same, static window. VictoriaLogs gets
--hours of history; Loki gets only --loki-hours (it stalls on long backfills).
Stack helpers come from bench/ab/stack.py; the proxies run with the flags of
the e2e `loki-vl-proxy-patterns-autodetect` service (Loki-compatible labels, patterns on).
"""
import argparse
import http.server
import threading
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "ab"))
import stack as ab  # noqa: E402
from vio import dump_json, load_json, write_text  # noqa: E402

# The profile of the datasource Logs Drilldown opens by default in the e2e stack.
SERVICE = "loki-vl-proxy-patterns-autodetect"
PORTS = dict(loki=33101, vl=33428, main=33200, pr=33202, grafana=33002)


def grafana_host_network():
    """Linux Docker (CI) reaches the host-only listeners through the host network; Docker Desktop
    publishes a port and reaches the host as host.docker.internal."""
    return sys.platform.startswith("linux")


def sh(*a, **kw):
    return subprocess.run(a, check=kw.pop("check", True), **kw)


def plugins_and_image(tree):
    out = subprocess.run(["docker", "compose", "-f", ab.BASE_COMPOSE, "config", "--format", "json"], cwd=tree,
                         check=True, capture_output=True, text=True).stdout
    g = json.loads(out)["services"]["grafana"]
    env = g["environment"]
    env = env if isinstance(env, dict) else dict(e.split("=", 1) for e in env)
    return g["image"], env["GF_PLUGINS_PREINSTALL"], env.get("GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS", "")


def datasources(host=None):
    host = host or ("127.0.0.1" if grafana_host_network() else "host.docker.internal")
    def ds(name, uid, port):
        return (f"  - name: {name}\n    uid: {uid}\n    type: loki\n    access: proxy\n    url: http://{host}:{port}\n"
                "    jsonData:\n      httpHeaderName1: X-Scope-OrgID\n      maxLines: 1000\n      timeout: 300\n"
                "    secureJsonData:\n      httpHeaderValue1: \"0\"\n")
    return ("apiVersion: 1\ndatasources:\n" + ds("Loki (via VL proxy main)", "vp-main", PORTS["main"])
            + ds("Loki (via VL proxy)", "vp-pr", PORTS["pr"]) + ds("Loki (direct)", "vp-loki", PORTS["loki"]))


class Sink(http.server.BaseHTTPRequestHandler):
    """Stands in for Loki: 200 for everything, so the generator's Loki pushes go nowhere."""

    def _ok(self):
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._ok()

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self._ok()

    def log_message(self, *a):
        pass


def seed_vl_only(st, start, end, out):
    """History before the Loki window: VictoriaLogs only (Loki pushes go to a sink)."""
    sink = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Sink)
    threading.Thread(target=sink.serve_forever, daemon=True).start()
    env = dict(os.environ, LOKI_URL=f"http://127.0.0.1:{sink.server_address[1]}", VL_URL=st.vl, LOG_BATCH=str(ab.BACKFILL_BATCH),
               LOG_BACKFILL_SECONDS=str(end - start), LOG_BACKFILL_INTERVAL=str(ab.BACKFILL_INTERVAL),
               LOG_BACKFILL_END=str(end), LOG_BACKFILL_ONLY="1", LOG_BACKFILL_SEED=f"{end}-old")
    with open(os.path.join(out, "seed-vl-only.log"), "w") as f:
        sh(sys.executable, ab.GENERATOR, cwd=ab.ROOT, env=env, stdout=f, stderr=subprocess.STDOUT)
    sink.shutdown()


def up(a):
    os.makedirs(a.out, exist_ok=True)
    st = ab.Stack(a.project, PORTS["loki"], PORTS["vl"])
    state = dict(project=a.project, ports=PORTS, pids={})
    sp = os.path.join(a.out, "state.json")
    old = load_json(sp) if a.skip_seed and os.path.exists(sp) else {}
    stop_proxies(old)  # a re-run replaces the proxies the earlier one started

    def save():
        dump_json(sp, state)

    save()
    try:  # a stack that is already up (a retry after a slow Loki start) is reused
        ab.wait_http(f"{st.vl}/health", 3, "VictoriaLogs")
        ab.wait_http(f"{st.loki}/ready", 3, "Loki")
    except RuntimeError:
        st.up()
    end = old.get("end") or int(time.time()) // 60 * 60
    state["end"] = end
    state["hours"], state["loki_hours"] = a.hours, a.loki_hours
    builds = [("main", a.main_ref, os.path.join(a.out, "proxy-main.bin")), ("pr", None, os.path.join(a.out, "proxy-pr.bin"))]
    ab.log("building both proxies")
    trees = {n: ab.build(ref, out) for n, ref, out in builds}
    state["trees"] = {k: v for k, v in trees.items() if v != ab.ROOT}
    save()
    # The Loki window starts on a 10-minute boundary: the count check compares 10-minute slices.
    loki_start = -(-(end - int(a.loki_hours * 3600)) // 600) * 600
    if a.skip_seed:
        ab.log("reusing the data already seeded")
    else:
        if a.hours > a.loki_hours:
            ab.log(f"seeding VictoriaLogs only: {a.hours}h .. {a.loki_hours}h before the end")
            seed_vl_only(st, end - int(a.hours * 3600), loki_start, a.out)
        ab.log(f"seeding Loki + VictoriaLogs: last {a.loki_hours}h")
        st.seed(end - loki_start, end)
    specs, procs = [], []
    for name, port in (("main", PORTS["main"]), ("pr", PORTS["pr"])):
        specs.append((name, os.path.join(a.out, f"proxy-{name}.bin"), trees[name], port))
    ab.start_proxies(specs, a.out, st, SERVICE, procs)
    state["pids"] = {p.name: p.proc.pid for p in procs}
    save()
    # Grafana
    image, plugins, unsigned = plugins_and_image(trees["pr"])
    gdir = os.path.join(os.path.abspath(a.out), "grafana")
    os.makedirs(gdir, exist_ok=True)
    write_text(os.path.join(gdir, "datasources.yaml"), datasources())
    name = f"{a.project}-grafana"
    sh("docker", "rm", "-f", name, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    net = (["--network", "host", "-e", f"GF_SERVER_HTTP_PORT={PORTS['grafana']}"] if grafana_host_network()
           else ["-p", f"127.0.0.1:{PORTS['grafana']}:3000"])
    sh("docker", "run", "-d", "--name", name, *net,
       "-e", "GF_AUTH_ANONYMOUS_ENABLED=true", "-e", "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
       "-e", "GF_AUTH_DISABLE_LOGIN_FORM=true", "-e", f"GF_PLUGINS_PREINSTALL={plugins}",
       "-e", f"GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS={unsigned}",
       "-v", f"{gdir}/datasources.yaml:/etc/grafana/provisioning/datasources/datasources.yaml:ro",
       image, stdout=subprocess.DEVNULL)
    ab.wait_http(f"http://127.0.0.1:{PORTS['grafana']}/api/health", 240, "Grafana")
    state["grafana"] = f"http://127.0.0.1:{PORTS['grafana']}"
    save()
    ab.log(f"up. Grafana {state['grafana']}  end={end}  state={sp}")


def stop_proxies(state):
    for name, pid in state.get("pids", {}).items():
        port = PORTS[name]
        owner = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"], capture_output=True, text=True).stdout.split()
        if str(pid) in owner:  # only a process this stack started
            try:
                os.killpg(pid, 15)
            except ProcessLookupError:
                pass


def live(a):
    """start/stop the log generator in live mode: every line goes to Loki and VictoriaLogs (mirrored), for Live tail."""
    sp = os.path.join(a.out, "state.json")
    state = load_json(sp)
    if a.cmd == "live-stop":
        pid = state.pop("live_pid", None)
        if pid:
            try:
                os.killpg(pid, 15)
            except ProcessLookupError:
                pass
    else:
        env = dict(os.environ, LOKI_URL=f"http://127.0.0.1:{PORTS['loki']}", VL_URL=f"http://127.0.0.1:{PORTS['vl']}",
                   LOG_INTERVAL="1", LOG_BATCH="30")
        with open(os.path.join(a.out, "live-generator.log"), "w") as f:
            proc = subprocess.Popen([sys.executable, ab.GENERATOR], cwd=ab.ROOT, env=env, stdout=f,
                                    stderr=subprocess.STDOUT, start_new_session=True)
        state["live_pid"] = proc.pid
    dump_json(sp, state)


def down(a):
    sp = os.path.join(a.out, "state.json")
    state = load_json(sp) if os.path.exists(sp) else {}
    project = state.get("project", a.project)
    stop_proxies(state)
    if state.get("live_pid"):
        a.cmd = "live-stop"
        live(a)
    sh("docker", "rm", "-f", f"{project}-grafana", check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ab.Stack(project, PORTS["loki"], PORTS["vl"]).down()
    for tree in state.get("trees", {}).values():
        ab.remove_tree(tree)
    ab.log("down")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["up", "down", "live-start", "live-stop"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--project", default="vp")
    ap.add_argument("--port-offset", type=int, default=0, help="shift every port (a second stack next to another)")
    ap.add_argument("--main-ref", default="origin/main")
    ap.add_argument("--hours", type=float, default=24, help="VictoriaLogs history (hours)")
    ap.add_argument("--skip-seed", action="store_true", help="reuse the data and end of an earlier up (state.json)")
    ap.add_argument("--loki-hours", type=float, default=1.5, help="Loki history (hours); longer backfills stall Loki")
    a = ap.parse_args()
    for k in PORTS:
        PORTS[k] += a.port_offset
    {"up": up, "down": down, "live-start": live, "live-stop": live}[a.cmd](a)


if __name__ == "__main__":
    main()
