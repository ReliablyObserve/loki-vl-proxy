#!/usr/bin/env python3
"""Summarize existing CI artifacts; never execute or extract PR artifact contents."""
import argparse
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import zipfile

MAX_ARCHIVE = 32 * 1024 * 1024
MAX_JSON = 2 * 1024 * 1024
WORKFLOWS = {"quality": "pr-quality-report.yaml", "perf": "perf-ab.yaml", "visual": "visual-smoke.yaml", "ci": "ci.yaml"}
WORKFLOWS.update(loki="compat-loki.yaml", drilldown="compat-drilldown.yaml", vl="compat-vl.yaml", security="security-pr.yaml")
ARTIFACTS = {
    "quality": {"pr-quality-report": {"pr-base-quality.json", "pr-head-quality.json", "quality-meta.json"}},
    "perf": {"perf-ab-smoke": {"meta.json", "verdict.json", "summary-*.json"}},
    "visual": {"visual-quality-evidence": {"meta.json", "plan.json", "compare.json", "verdict.json"}},
    "ci": {"unit-results": {"unit-results.json", "quality-meta.json"}, "coverage": {"coverage.out", "coverage-cache.out"},
           "fleet-peer-cache-proof": {"fleet-peer-cache-proof-rf1.json", "fleet-peer-cache-proof-rf2.json", "fleet-meta.json"}},
}


def strict_json(raw):
    return json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))


def read_artifact(raw, allowed):
    if len(raw) > MAX_ARCHIVE:
        raise ValueError("archive exceeds limit")
    result = {}
    expanded = 0
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        if len(archive.infolist()) > 1000:
            raise ValueError("too many archive entries")
        for item in archive.infolist():
            path = PurePosixPath(item.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in item.filename:
                continue
            name = path.name
            if not any(name == pattern or pattern == "summary-*.json" and name.startswith("summary-") and name.endswith(".json") for pattern in allowed):
                continue
            expanded += item.file_size
            if expanded > 48 * 1024 * 1024:
                raise ValueError("expanded artifact exceeds limit")
            limit = MAX_JSON if name.endswith(".json") and name not in {"unit-results.json", "pr-base-quality.json", "pr-head-quality.json"} else 12 * 1024 * 1024
            if item.file_size > limit or name in result:
                raise ValueError("oversized or duplicate artifact member")
            value = archive.read(item).decode("utf-8")
            if name in {"unit-results.json", "coverage.out", "coverage-cache.out"}:
                result[name] = value
            else:
                value = strict_json(value)
                if not isinstance(value, dict):
                    raise ValueError("expected JSON object")
                result[name] = value
    return result


def current_meta(meta, context, kind):
    heads = {context.get("head")} if kind == "visual" else {context.get("head"), context.get("merge")}
    heads.discard(None)
    return bool(meta.get("base") and meta.get("base") == context.get("base") and meta.get("head") in heads)


def latest_run(runs, head):
    return max((r for r in runs if r.get("head_sha") == head), key=lambda r: (r["id"], r.get("run_attempt", 1)), default=None)


def coverage_blocks(text):
    blocks = {}
    for line in text.splitlines()[1:]:
        location, statements, count = line.rsplit(" ", 2)
        statements, count = int(statements), int(count)
        if statements < 0 or count < 0:
            raise ValueError("negative coverage count")
        old = blocks.get(location, (statements, 0))
        if old[0] != statements:
            raise ValueError("inconsistent coverage block")
        blocks[location] = (statements, max(count, old[1]))
    return blocks


def coverage_pct(text):
    blocks = coverage_blocks(text)
    total = sum(n for n, _ in blocks.values())
    return 100 * sum(n for n, hit in blocks.values() if hit) / total if total else None


def package_coverage(text):
    packages = {}
    for location, (statements, hit) in coverage_blocks(text).items():
        package = location.rsplit("/", 1)[0] if "/" in location else "."
        entry = packages.setdefault(package, {"covered": 0, "total": 0})
        entry["total"] += statements
        if hit:
            entry["covered"] += statements
    return {package: {**entry, "pct": 100 * entry["covered"] / entry["total"]} for package, entry in packages.items() if entry["total"]}


def unit_summary(text):
    events = {}
    for line in text.splitlines():
        event = strict_json(line)
        if not isinstance(event, dict):
            raise ValueError("expected test event object")
        if event.get("Test") and event.get("Action") in {"pass", "fail", "skip"}:
            key = (event["Package"], event["Test"])
            if events.get(key) != "fail":
                events[key] = event["Action"]
    return {"scope": "internal packages (existing CI artifact)",
            **{label: sum(action == event for action in events.values()) for label, event in (("passed", "pass"), ("failed", "fail"), ("skipped", "skip"))},
            "top_level_passed": sum(action == "pass" and "/" not in name for (_, name), action in events.items()),
            "subtests_passed": sum(action == "pass" and "/" in name for (_, name), action in events.items()),
            "packages": len({pkg for pkg, _ in events}),
            "failed_names": [f"{pkg}:{name}" for (pkg, name), action in events.items() if action == "fail"][:30],
            "outcomes": {f"{pkg}\t{name}": action for (pkg, name), action in events.items()}}


def log_scope(text, context):
    """Read existing verbose job output; this never starts a test process."""
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    lines = [re.sub(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z\s?", "", line) for line in text.splitlines()]
    checkout = None
    for index, line in enumerate(lines[:-1]):
        if "[command]" in line and "git log -1 --format=%H" in line:
            match = re.fullmatch(r"[a-f0-9]{40}", lines[index + 1].strip())
            if match:
                checkout = match.group()
                break
    if not checkout:
        return {"state": "unavailable", "note": "Checkout identity missing from retained log."}
    if checkout not in {context.get("head"), context.get("merge")}:
        return {"state": "stale", "note": "Log belongs to a different tested revision."}
    events, versions, playwright, runs = [], {}, {}, []
    for line in lines:
        match = re.match(r"\s*--- (PASS|FAIL|SKIP): (\S+) \(", line)
        if match:
            events.append({"Action": match[1].lower(), "Package": "job", "Test": match[2]})
        version = re.fullmatch(r"\s*(LOKI_IMAGE|VICTORIALOGS_IMAGE|GRAFANA_IMAGE|PROXY_IMAGE):\s*(\S+)\s*", line)
        if version:
            values = versions.setdefault(version[1], [])
            if version[2] not in values and len(values) < 5:
                values.append(version[2])
        if re.match(r"\s*Running \d+ tests? using \d+ workers?", line) and playwright:
            runs.append(playwright)
            playwright = {}
        result = re.match(r"\s*(\d+) (passed|failed|skipped|flaky|interrupted|did not run)(?:\s|$)", line)
        if result:
            playwright["not_run" if result[2] == "did not run" else result[2]] = int(result[1])
    if events:
        summary = unit_summary("\n".join(json.dumps(event) for event in events))
        summary.pop("outcomes")
        summary.update(state="available", kind="Go", scope="Go cases in one retained CI job", source=checkout, versions=versions)
        return summary
    if playwright:
        runs.append(playwright)
    if runs:
        return {"state": "available", "kind": "Playwright", "source": checkout, "versions": versions,
                "runs": [{key: run.get(key, 0) for key in ("passed", "failed", "skipped", "flaky", "interrupted", "not_run")} for run in runs]}
    return {"state": "unavailable", "note": "No recognized named-test outcomes in retained log.", "source": checkout, "versions": versions}


def api_test_job(kind, name):
    return (kind == "ci" and (name.startswith("e2e-compat (") or name.startswith("e2e-ui (") or name == "e2e-fleet")
            or kind == "loki" and name == "loki-pinned"
            or kind == "vl" and name == "vl-pinned"
            or kind == "drilldown" and (name == "drilldown-pinned-runtime" or name.startswith("drilldown-grafana-pr-matrix (")))


class GitHub:
    def __init__(self, repository, token):
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
            raise ValueError("invalid repository")
        self.repository, self.token = repository, token

    def request(self, path, body=None, method=None, binary=False):
        url = "https://api.github.com/repos/" + self.repository + "/" + path
        request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
                                         method=method, headers={"Authorization": "Bearer " + self.token,
                                         "Accept": "application/vnd.github+json", "Content-Type": "application/json"})
        # urllib strips Authorization on cross-origin redirects via our handler.
        opener = urllib.request.build_opener(SafeRedirect())
        with opener.open(request, timeout=30) as response:
            raw = response.read(MAX_ARCHIVE + 1)
        if len(raw) > MAX_ARCHIVE:
            raise ValueError("API response exceeds limit")
        return raw if binary else strict_json(raw)

    def pages(self, path, field=None, max_pages=10):
        result = []
        for page in range(1, max_pages + 1):
            data = self.request(path + ("&" if "?" in path else "?") + f"per_page=100&page={page}")
            rows = data[field] if field else data
            result.extend(rows)
            if len(rows) < 100:
                return result
        raise ValueError("API pagination limit reached")


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme != "https" or not (parsed.hostname == "api.github.com" or parsed.hostname and parsed.hostname.endswith(".blob.core.windows.net")):
            raise ValueError("unexpected artifact redirect")
        target = super().redirect_request(request, fp, code, msg, headers, newurl)
        if parsed.hostname != "api.github.com":
            target.remove_header("Authorization")
        return target


def collect(api, pr):
    context = {"repository": api.repository, "number": pr["number"], "base": pr["base"]["sha"], "head": pr["head"]["sha"], "merge": pr.get("merge_commit_sha")}
    result = {"context": context, "sources": {}}
    for kind, workflow in WORKFLOWS.items():
        source = {"state": "unavailable", "artifacts": {}}
        result["sources"][kind] = source
        try:
            runs = api.pages(f"actions/workflows/{workflow}/runs?event=pull_request&head_sha={context['head']}", "workflow_runs", 2)
            run = latest_run(runs, context["head"])
            if not run:
                continue
            run_id, attempt = int(run["id"]), int(run.get("run_attempt", 1))
            if attempt > 1:
                # A run's artifact list spans attempts. Obtain this attempt's
                # actual start, rather than guessing from the original run.
                attempt_info = api.request(f"actions/runs/{run_id}/attempts/{attempt}")
                if not attempt_info.get("run_started_at"):
                    raise ValueError("missing attempt timestamp")
                run = {**run, "run_started_at": attempt_info["run_started_at"]}
            source.update(state="completed" if run["status"] == "completed" else "pending", conclusion=run.get("conclusion"),
                          url=f"https://github.com/{api.repository}/actions/runs/{run_id}/attempts/{attempt}")
            jobs = api.pages(f"actions/runs/{run_id}/attempts/{attempt}/jobs", "jobs")
            source["jobs"] = [{"name": j["name"], "status": j["status"], "conclusion": j.get("conclusion"),
                               "url": f"https://github.com/{api.repository}/actions/runs/{run_id}/job/{int(j['id'])}"} for j in jobs]
            primary = {"quality": "report", "perf": "perf-smoke", "visual": "visual-smoke"}.get(kind)
            if primary and any(j["name"] == primary and j.get("conclusion") == "skipped" for j in jobs):
                source["conclusion"] = "skipped"
            if source["state"] != "completed":
                continue
            artifacts = api.pages(f"actions/runs/{run_id}/artifacts", "artifacts")
            for name, allowed in ARTIFACTS.get(kind, {}).items():
                candidates = [a for a in artifacts if a["name"] == name and not a.get("expired")]
                if not candidates:
                    continue
                # Attempts share a run_id: old-attempt artifacts must never be reused.
                candidates = [a for a in candidates if a.get("created_at", "") >= run.get("run_started_at", "")]
                if not candidates:
                    continue
                artifact = max(candidates, key=lambda a: a["id"])
                if artifact.get("size_in_bytes", 0) > MAX_ARCHIVE:
                    raise ValueError("artifact exceeds download limit")
                files = read_artifact(api.request(f"actions/artifacts/{int(artifact['id'])}/zip", binary=True), allowed)
                meta_name = "fleet-meta.json" if name == "fleet-peer-cache-proof" else "quality-meta.json" if kind in {"ci", "quality"} else "meta.json"
                if name != "coverage" and not current_meta(files.get(meta_name, {}), context, kind):
                    source["note"] = "Some artifacts have missing or stale source identity; measurements omitted."
                    continue
                source["artifacts"].update(files)
            data = source["artifacts"]
            if kind == "ci":
                tests = data.pop("unit-results.json", None)
                cov, cache = data.pop("coverage.out", None), data.pop("coverage-cache.out", None)
                if tests is not None and current_meta(data.get("quality-meta.json", {}), context, "ci"):
                    data["ci-regression-summary.json"] = {"source": data["quality-meta.json"]["head"], "tests": unit_summary(tests),
                        "coverage": {"overall_pct": coverage_pct(cov) if cov else None, "cache_pct": coverage_pct(cache) if cache else None}}
            for job in jobs:
                if not api_test_job(kind, job["name"]) or job.get("status") != "completed" or job.get("conclusion") == "skipped":
                    continue
                try:
                    raw = api.request(f"actions/jobs/{int(job['id'])}/logs", binary=True)
                    scope = log_scope(raw.decode("utf-8", errors="replace"), context)
                except (ValueError, TypeError, OSError, AttributeError, RecursionError):
                    scope = {"state": "unavailable", "note": "Detailed job log unavailable; job conclusion remains authoritative."}
                scope.update(name=job["name"])
                source.setdefault("test_scopes", []).append(scope)
        except (ValueError, KeyError, TypeError, OSError, zipfile.BadZipFile, RuntimeError, UnicodeError, AttributeError, OverflowError, RecursionError):
            source.update(state="error", note="Evidence could not be read safely; see the original CI run.", artifacts={})
    return result


def provenance(output):
    def git(ref):
        return subprocess.check_output(["git", "rev-parse", ref], text=True).strip()
    head = git("HEAD")
    raw = subprocess.check_output(["git", "cat-file", "-p", head], text=True)
    parents = [line.split()[1] for line in raw.split("\n\n", 1)[0].splitlines() if line.startswith("parent ")]
    Path(output).write_text(json.dumps({"base": parents[0] if parents else None, "head": head}) + "\n")


def publish(api, pr, evidence, output, dry_run=False):
    from pr_quality_report import render
    # Artifacts are untrusted nested data. A malformed source loses its
    # measurements, not the other workflows' failures or the whole comment.
    for kind, source in evidence["sources"].items():
        if source.get("state") != "completed":
            continue
        artifacts = source.get("artifacts", {})
        try:
            if kind == "quality":
                for name in ("pr-base-quality.json", "pr-head-quality.json"):
                    sample = artifacts.get(name, {})
                    if not isinstance(sample, dict) or any(not isinstance(sample.get(key, {}), dict) for key in ("tests", "compatibility", "performance")):
                        raise ValueError("malformed quality snapshot")
            render(artifacts.get("pr-base-quality.json", {}) if kind == "quality" else {},
                   artifacts.get("pr-head-quality.json", {}) if kind == "quality" else {},
                   {"sources": {kind: source}})
        except (AttributeError, TypeError, ValueError, KeyError, OverflowError, RecursionError):
            source.update(state="error", artifacts={}, note="Malformed evidence omitted; inspect the original CI run.")
    data = evidence["sources"]["quality"].get("artifacts", {})
    body = render(data.get("pr-base-quality.json", {}), data.get("pr-head-quality.json", {}), evidence)
    Path(output).write_text(body)
    # Recheck identity immediately before writing: an old completion must not
    # replace a newer commit or a comparison against a newer base.
    current = api.request(f"pulls/{pr['number']}")
    if current["state"] != "open" or any(current[k]["sha"] != pr[k]["sha"] for k in ("base", "head")) or current.get("merge_commit_sha") != pr.get("merge_commit_sha"):
        return False
    if dry_run:
        return True
    comments = api.pages(f"issues/{pr['number']}/comments")
    existing = next((c for c in comments if c.get("user", {}).get("login") == "github-actions[bot]" and "<!-- pr-quality-report -->" in c.get("body", "")), None)
    if existing:
        api.request(f"issues/comments/{int(existing['id'])}", {"body": body}, "PATCH")
    else:
        api.request(f"issues/{pr['number']}/comments", {"body": body}, "POST")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provenance")
    parser.add_argument("--summarize-tests")
    parser.add_argument("--coverage-profile")
    parser.add_argument("--coverage-percent", type=float)
    parser.add_argument("--test-exit-code", type=int, default=0)
    parser.add_argument("--pr", type=int)
    parser.add_argument("--out", default="pr-quality-report.md")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.provenance:
        provenance(args.provenance)
        return
    if args.summarize_tests:
        raw = Path(args.summarize_tests).read_text()
        summary = unit_summary(raw)
        events = raw.splitlines()
        count = sum(1 for line in events if (event := strict_json(line)).get("Action") == "pass" and event.get("Test"))
        snapshot = {"count": count if not args.test_exit_code else 0, "coverage_pct": args.coverage_percent if not args.test_exit_code else 0,
                    "outcomes": summary["outcomes"], "execution_state": "complete" if not args.test_exit_code else "failed"}
        if args.test_exit_code:
            snapshot["test_exit_code"] = args.test_exit_code
        else:
            snapshot["package_coverage"] = package_coverage(Path(args.coverage_profile).read_text())
        print(json.dumps(snapshot))
        return
    api = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"])
    if args.pr:
        numbers = [args.pr]
    else:
        event = strict_json(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
        run = event["workflow_run"]
        if run.get("event") != "pull_request":
            return
        numbers = [int(p["number"]) for p in run.get("pull_requests", [])][:10]
        if not numbers:
            numbers = [p["number"] for p in api.pages("pulls?state=open") if p["head"]["sha"] == run["head_sha"]][:10]
    for number in numbers:
        pr = api.request(f"pulls/{number}")
        if pr["state"] != "open":
            continue
        evidence = collect(api, pr)
        Path(args.out + ".json").write_text(json.dumps(evidence))
        publish(api, pr, evidence, args.out, args.dry_run)


if __name__ == "__main__":
    main()
