#!/usr/bin/env python3
"""Read-only alias proof against an already seeded base/PR/Loki stack."""
import argparse
import datetime
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def request(base, path, method="GET", params=None):
    data = urllib.parse.urlencode(params or {}).encode()
    target = base.rstrip("/") + path
    headers = {}
    if method == "GET":
        target += "?" + data.decode() if data else ""
        data = None
    else:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = urllib.request.Request(target, data=data, headers=headers, method=method)
    try:
        response = urllib.request.build_opener(NoRedirect).open(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read(2 * 1024 * 1024 + 1)
        if len(body) > 2 * 1024 * 1024:
            raise ValueError("proof response exceeds bound")
        return response.code, body.decode()


def backend_reads(base):
    code, metrics = request(base, "/metrics")
    if code != 200:
        raise ValueError("proxy metrics unavailable")
    values = []
    for line in metrics.splitlines():
        if (line.startswith("loki_vl_proxy_requests_total{") and 'system="vl"' in line
                and 'direction="upstream"' in line and 'route="/select/logsql/' in line):
            values.append(float(line.rsplit(" ", 1)[1]))
    # These series are created lazily. Absence cannot supply a cold delta;
    # after the canonical request its exported counters can prove alias reuse.
    return sum(values) if values else None


def delta(before, after):
    if before is None or after is None:
        return None
    if after < before:
        raise ValueError("upstream counter reset during proof")
    return after - before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:34102")
    parser.add_argument("--pr", default="http://127.0.0.1:34100")
    parser.add_argument("--loki", default="http://127.0.0.1:34101")
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--pr-sha", required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    for sha in (args.base_sha, args.pr_sha):
        if not re.fullmatch(r"[a-f0-9]{40}", sha):
            parser.error("proof revisions must be full commit SHAs")
    end = time.time_ns()
    params = {"start": str(end - 3600 * 10**9), "end": str(end), "query": '{app="e2e-test"}'}
    rows = []
    references = {}
    for name, base in (("base", args.base), ("pr", args.pr), ("loki", args.loki)):
        get_reference = None
        for method in ("GET", "POST"):
            before = backend_reads(base) if name != "loki" else None
            plural_code, plural_body = request(base, "/loki/api/v1/labels", method, params)
            after_plural = backend_reads(base) if name != "loki" else None
            alias_code, alias_body = request(base, "/loki/api/v1/label", method, params)
            after_alias = backend_reads(base) if name != "loki" else None
            plural = json.loads(plural_body)
            if plural_code != 200 or plural.get("status") != "success" or "app" not in plural.get("data", []):
                raise ValueError(f"{name} {method}: canonical fixture labels unavailable")
            if method == "GET":
                get_reference = plural
                references[name] = plural
            canonical_matches_get = plural == get_reference
            same = json.loads(alias_body) == plural if alias_code == 200 else False
            if name in {"pr", "loki"} and (not same or not canonical_matches_get):
                raise ValueError(f"{name} {method}: alias differs from canonical labels")
            rows.append({"case": "singular_alias", "source": name, "method": method, "plural_status": plural_code,
                         "alias_status": alias_code, "alias_equals_plural": same,
                         "canonical_matches_get": canonical_matches_get,
                         "labels": plural["data"],
                         "plural_upstream_selects": delta(before, after_plural),
                         "alias_upstream_selects": delta(after_plural, after_alias)})
        missing = dict(params, query='{app="read-api-proof-missing-fixture"}')
        before = backend_reads(base) if name != "loki" else None
        post_code, post_body = request(base, "/loki/api/v1/labels", "POST", missing)
        after_post = backend_reads(base) if name != "loki" else None
        get_code, get_body = request(base, "/loki/api/v1/labels", "GET", missing)
        post, get = json.loads(post_body), json.loads(get_body)
        if post_code != 200 or get_code != 200 or get.get("status") != "success" or get.get("data", []) != []:
            raise ValueError(f"{name}: empty-selector fixture check failed")
        same = post == get
        if name in {"pr", "loki"} and not same:
            raise ValueError(f"{name}: POST reused another selector's labels")
        rows.append({"case": "post_selector_isolation", "source": name,
                     "query": missing["query"], "post_status": post_code, "get_status": get_code,
                     "post_matches_get": same, "post_labels": post.get("data"), "get_labels": get.get("data"),
                     "post_upstream_selects": delta(before, after_post)})
    if references["pr"] != references["loki"]:
        raise ValueError("candidate canonical labels differ from Loki")
    result = {"base": args.base_sha, "candidate": args.pr_sha,
              "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "scope": "dedicated compose, same seeded Loki/VL data; peers disabled; GET then POST; no redirect following",
              "params": params, "rows": rows,
              "filtered_nonempty_response_matches_loki": True,
              "limits": ["Request counters are proxy upstream selects, not physical storage operations.",
                         "No CPU, RSS, SSD IOPS, throughput or fleet-scaling comparison measured.",
                         "The optional outer GET compatibility cache remains path-specific."]}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(rows))


if __name__ == "__main__":
    main()
