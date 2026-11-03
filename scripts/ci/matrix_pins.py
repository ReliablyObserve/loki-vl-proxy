#!/usr/bin/env python3
"""Every image and plugin pin in the repository must equal the compatibility matrix.

test/e2e-compat/compatibility-matrix.json is the one place that says which Loki, VictoriaLogs, Grafana and
Grafana-plugin versions the stack is pinned to. The workflows read it (the *-matrix-source jobs); everything
else is a copy that this check compares with it:

  * every docker-compose file in the repository (root, review, e2e-compat, fleet, pipeline, examples/*, bench):
    each `repo:tag` of a matrix image, `${VAR:-repo:tag}` defaults included, and each plugin `name@version`;
  * the benchmark pins that name the stack by hand: bench/read-api/compose.json, bench/drilldown-memory/proof.py,
    bench/cache-placement/real_api_test.go and the fleet test fallback;
  * the workflows under .github/workflows: no image or plugin literal at all (they read the matrix outputs);
  * .github/dependabot.yml: its docker-compose ignore list must cover every matrix-owned image repository, so a
    bot never bumps a pin on its own;
  * the matrix itself: the Drilldown plugin pin of stack.grafana.plugins equals logs_drilldown_contract.pinned_version.

Usage: python3 scripts/ci/matrix_pins.py [--root DIR]    (exit 1 and the problems on stderr when a pin drifted)
"""
import argparse
import fnmatch
import json
import os
import re
import sys

MATRIX = "test/e2e-compat/compatibility-matrix.json"
BENCH_PINS = (
    "bench/read-api/compose.json",
    "bench/drilldown-memory/proof.py",
    "bench/cache-placement/real_api_test.go",
    "test/e2e-fleet/peer_cache_proof_test.go",
)
# The pins each of these files must carry (not only agree with the matrix when present): a file that quietly
# fell back to `latest` or lost a service is as wrong as a stale tag. Image repos by matrix component name.
MUST_PIN = {
    "docker-compose.yml": ("loki", "victorialogs", "grafana"),
    "test/e2e-compat/docker-compose.yml": ("loki", "victorialogs", "grafana", "plugins"),
    "test/e2e-compat/docker-compose.review.yml": ("loki", "grafana", "plugins"),
    "test/e2e-fleet/docker-compose.yml": ("loki", "victorialogs"),
    "test/e2e-pipeline/docker-compose.yml": ("victorialogs",),
    "examples/docker-compose.yml": ("victorialogs", "grafana"),
    "examples/peers/consul-catalog/docker-compose.yml": ("victorialogs",),
    "examples/peers/haproxy-dns/docker-compose.yml": ("victorialogs",),
    "examples/peers/prometheus-sd/docker-compose.yml": ("victorialogs",),
    "examples/peers/static-list/docker-compose.yml": ("victorialogs",),
    "bench/read-api/compose.json": ("loki", "victorialogs"),
    "bench/drilldown-memory/proof.py": ("loki", "victorialogs"),
    "bench/cache-placement/real_api_test.go": ("loki", "victorialogs"),
    "test/e2e-fleet/peer_cache_proof_test.go": ("loki", "victorialogs"),
}
DEPENDABOT = ".github/dependabot.yml"
WORKFLOWS = ".github/workflows"
SKIP_DIRS = {".git", "node_modules", ".worktrees", ".claude", "vendor", "results", "history"}
COMPOSE_NAME = re.compile(r"^(docker-)?compose[^/]*\.ya?ml$")
COMPONENTS = ("loki", "victorialogs", "grafana")


def load_pins(root):
    """image repo -> pinned tag, plugin name -> pinned version, component -> image repo, from the matrix; problems
    of the matrix itself."""
    with open(os.path.join(root, MATRIX), encoding="utf-8") as handle:
        stack = json.load(handle)["stack"]
    images, plugins, repos, problems = {}, {}, {}, []
    for component in COMPONENTS:
        meta = stack.get(component, {})
        if not meta.get("image_repo") or not meta.get("pinned_version"):
            problems.append(f"{MATRIX}: stack.{component} needs image_repo and pinned_version")
            continue
        images[meta["image_repo"]] = meta["pinned_version"]
        repos[component] = meta["image_repo"]
    for name, meta in (stack.get("grafana", {}).get("plugins") or {}).items():
        if not meta.get("pinned_version"):
            problems.append(f"{MATRIX}: stack.grafana.plugins.{name} needs pinned_version")
        else:
            plugins[name] = meta["pinned_version"]
    if not plugins:
        problems.append(f"{MATRIX}: stack.grafana.plugins is missing")
    drilldown = stack.get("logs_drilldown_contract", {}).get("pinned_version")
    if plugins.get("grafana-lokiexplore-app") not in (None, drilldown):
        problems.append(f"{MATRIX}: stack.grafana.plugins.grafana-lokiexplore-app is "
                        f"{plugins['grafana-lokiexplore-app']} but logs_drilldown_contract.pinned_version is {drilldown}")
    return images, plugins, repos, problems


def compose_files(root):
    found = []
    for current, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        found += [os.path.relpath(os.path.join(current, f), root) for f in files if COMPOSE_NAME.match(f)]
    return sorted(found)


def read(root, path):
    try:
        with open(os.path.join(root, path), encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def line_of(text, offset):
    return text.count("\n", 0, offset) + 1


def check_text(path, text, images, plugins):
    """Every pin the text names must equal the matrix; returns problems with file:line."""
    problems = []
    for repo, pin in images.items():
        for match in re.finditer(re.escape(repo) + r":([A-Za-z0-9][A-Za-z0-9._-]*)", text):
            if match.group(1) != pin:
                problems.append(f"{path}:{line_of(text, match.start())}: {repo}:{match.group(1)} but the matrix pins {repo}:{pin}")
    for name, pin in plugins.items():
        for match in re.finditer(re.escape(name) + r"@([A-Za-z0-9][A-Za-z0-9._-]*)", text):
            if match.group(1) != pin:
                problems.append(f"{path}:{line_of(text, match.start())}: {name}@{match.group(1)} but the matrix pins {name}@{pin}")
    return problems


def check_required(path, text, images, plugins, repos, must_pin):
    """The component pins the file must name, each at the matrix version. A pin present at another version is
    already reported by check_text, so only a missing one is reported here."""
    found = []
    for need in must_pin.get(path, ()):
        if need == "plugins":
            wanted = [(f"{name}@", f"{name}@{pin}") for name, pin in plugins.items()]
        elif need in repos:
            wanted = [(f"{repos[need]}:", f"{repos[need]}:{images[repos[need]]}")]
        else:
            continue
        found += [f"{path}: must pin {pin} (the matrix pin) but does not name it"
                  for prefix, pin in wanted if not re.search(re.escape(prefix) + r"[A-Za-z0-9]", text)]
    return found


def check_workflows(root, images, plugins):
    """A workflow never carries an image or plugin version: it reads the matrix outputs."""
    problems = []
    base = os.path.join(root, WORKFLOWS)
    for name in sorted(os.listdir(base)) if os.path.isdir(base) else []:
        if not name.endswith((".yaml", ".yml")):
            continue
        path = f"{WORKFLOWS}/{name}"
        text = read(root, path)
        patterns = [re.escape(repo) + r":[A-Za-z0-9]" for repo in images] + \
                   [re.escape(plug) + r"@[A-Za-z0-9]" for plug in plugins]
        for pattern in patterns:
            for match in re.finditer(pattern, text):
                problems.append(f"{path}:{line_of(text, match.start())}: a pin literal ({match.group(0)}...); "
                                "read the matrix-source job outputs instead")
    return problems


def check_dependabot(root, images):
    text = read(root, DEPENDABOT)
    if text is None:
        return [f"{DEPENDABOT} is missing"]
    section = re.search(r"- package-ecosystem: docker-compose\n(.*?)(?=\n  - package-ecosystem:|\Z)", text, re.S)
    if not section:
        return [f"{DEPENDABOT}: no docker-compose ecosystem block"]
    ignored = re.findall(r"dependency-name:\s*[\"']?([^\"'\s]+)[\"']?", section.group(1))
    return [f"{DEPENDABOT}: the docker-compose ignore list does not cover {repo}, so a bot could bump its pin"
            for repo in sorted(images) if not any(fnmatch.fnmatch(repo, pattern) for pattern in ignored)]


def problems(root=".", must_pin=None, bench=None):
    """must_pin and bench default to the repository's lists; a test passes smaller ones for a fixture tree."""
    must_pin = MUST_PIN if must_pin is None else must_pin
    bench = BENCH_PINS if bench is None else bench
    images, plugins, repos, found = load_pins(root)
    if len(images) < len(COMPONENTS):
        return found
    scanned = set(compose_files(root)) | set(bench)
    for path in sorted(scanned | set(must_pin)):
        text = read(root, path)
        if text is None:
            found.append(f"{path}: expected to hold a stack pin, but the file is missing")
            continue
        found += check_text(path, text, images, plugins) + check_required(path, text, images, plugins, repos, must_pin)
    return found + check_workflows(root, images, plugins) + check_dependabot(root, images)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    found = problems(parser.parse_args().root)
    for line in found:
        print(line, file=sys.stderr)
    if found:
        print(f"matrix pins: {len(found)} drifted pin(s)", file=sys.stderr)
        return 1
    print("matrix pins: every compose file, bench pin, workflow and the dependabot ignore list match the matrix")
    return 0


if __name__ == "__main__":
    sys.exit(main())
