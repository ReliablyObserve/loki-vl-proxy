#!/usr/bin/env python3
"""Detect Grafana child-process OOMs that container readiness can hide."""

import argparse
import datetime
import json
from pathlib import Path
import subprocess


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT, timeout=30)


def snapshot():
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    info = json.loads(docker("inspect", "e2e-grafana"))[0]
    events = docker("exec", "e2e-grafana", "cat", "/sys/fs/cgroup/memory.events")
    return {
        "time": started,
        "container": info["Id"],
        "started_at": info["State"]["StartedAt"],
        "restarts": info["RestartCount"],
        "running": info["State"]["Running"],
        "events": {k: int(v) for k, v in (line.split() for line in events.splitlines())},
        "memory_current": int(docker("exec", "e2e-grafana", "cat", "/sys/fs/cgroup/memory.current")),
        "memory_peak": int(docker("exec", "e2e-grafana", "cat", "/sys/fs/cgroup/memory.peak")),
    }


def failures(before, after, logs):
    problems = []
    if (before["container"] != after["container"] or before["restarts"] != after["restarts"]
            or before["started_at"] != after["started_at"] or not after["running"]):
        problems.append("Grafana stopped, restarted or was replaced during UI checks")
    delta = after["events"]["oom_kill"] - before["events"]["oom_kill"]
    if delta != 0:
        problems.append(f"Grafana cgroup OOM kills changed by {delta}")
    for line in logs.splitlines():
        if "plugin process exited" in line and "signal: killed" in line:
            problems.append("A Grafana datasource plugin was killed during UI checks")
            break
    return problems


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("before", "after"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=True)
    current = snapshot()
    (args.directory / f"{args.action}.json").write_text(json.dumps(current, indent=2) + "\n")
    if args.action == "after":
        before = json.loads((args.directory / "before.json").read_text())
        logs = docker("logs", "--since", before["time"], "e2e-grafana")
        problems = failures(before, current, logs)
        result = {"passed": not problems, "problems": problems,
                  "oom_kill_delta": current["events"]["oom_kill"] - before["events"]["oom_kill"],
                  "memory_peak_bytes": current["memory_peak"],
                  "peak_scope": "container lifetime high-water, not per-test delta"}
        (args.directory / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result))
        if problems:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
