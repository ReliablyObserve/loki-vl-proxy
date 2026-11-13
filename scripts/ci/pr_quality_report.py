#!/usr/bin/env python3
"""Render bounded CI evidence, without starting tests or estimating absent metrics."""
import html
import json
import math
import re
import sys
from pathlib import Path
from quality_comparison import analyze


def mapping(value):
    return value if isinstance(value, dict) else {}


def records(value):
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def cell(value):
    text = re.sub(r"[\x00-\x1f\x7f]", " ", str(value))[:180]
    text = html.escape(text, quote=True).replace("@", "&#64;")
    return re.sub(r"([\\`*\[\]{}()|_~])", r"\\\1", text)


def numeric(v):
    try:
        return isinstance(v, (float, int)) and not isinstance(v, bool) and math.isfinite(v)
    except OverflowError:
        return False


def fmt(v, unit=""):
    return f"{v:g}{unit}" if numeric(v) else "unavailable"


def ms(v):
    return fmt(v * 1000, " ms") if numeric(v) else "—"


def delta(b, h, better="lower", pct=25, absolute=750, minimum=500):
    if not numeric(b) or not numeric(h) or b == 0:
        return "n/a"
    change = (h - b) / b * 100
    significant = b >= minimum and abs(change) >= pct and abs(h - b) >= absolute
    state = ("improved" if (change < 0) == (better == "lower") else "regressed") if significant else "stable"
    return f"{change:+.1f}% ({state})" if change else "0.0% (stable)"


def fold(title, lines):
    return [f"<details><summary>{title}</summary>", "", *lines, "", "</details>", ""]


# Cell texts that carry no value. A table row whose data cells (all but the first) are all of
# these says nothing; a table without such a row is not rendered.
NO_VALUE = {"", "unavailable", "n/a", "—", "-", "not recorded"}
UNESCAPED_PIPE = re.compile(r"(?<!\\)\|")
SHOWING_ROWS = re.compile(r"^Showing (\d+)/(\d+) rows\.$")


def table_cells(line):
    return [part.strip() for part in UNESCAPED_PIPE.split(line.strip())[1:-1]]


def empty_cell(text):
    """True for a cell that is only no-value words, alone or joined as "a / b" or "a → b"."""
    return all(part.strip() in NO_VALUE for part in re.split(r" / | → ", text))


def has_value(row):
    return any(not empty_cell(c) for c in table_cells(row)[1:])


def prune(lines):
    """Drop the table rows that hold no value and the tables left without a row.

    Returns (lines, rows kept): the rows of every table that still has a row. A "Showing N/N rows."
    line follows its table; it is corrected to the rows kept, or dropped with the table.
    """
    out, kept, i = [], 0, 0
    while i < len(lines):
        if not lines[i].startswith("|"):
            out.append(lines[i])
            i += 1
            continue
        j = i
        while j < len(lines) and lines[j].startswith("|"):
            j += 1
        header, body = lines[i:i + 2], lines[i + 2:j]
        rows = [row for row in body if has_value(row)]
        note = j + 1 if j + 1 < len(lines) and lines[j] == "" else j
        showing = SHOWING_ROWS.match(lines[note]) if note < len(lines) else None
        if rows:
            out += header + rows
            kept += len(rows)
            if showing and len(rows) != len(body):
                lines = lines[:note] + [f"Showing {len(rows)}/{len(rows)} rows with a value."] + lines[note + 1:]
        elif showing:
            j = note + 1  # the count belongs to the dropped table
        i = j
    cleaned = []
    for line in out:
        if line == "" and (not cleaned or cleaned[-1] == ""):
            continue
        cleaned.append(line)
    while cleaned and cleaned[-1] == "":
        cleaned.pop()
    return cleaned, kept


def data(sources, name):
    source = sources.get(name, {})
    if source.get("state") != "completed" or source.get("conclusion") == "skipped":
        return {}
    return mapping(source.get("artifacts"))


def link(source):
    url = source.get("url", "")
    return f"[run]({url})" if re.fullmatch(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+(?:/attempts/[0-9]+)?", url) else "—"


def table(columns, rows):
    return ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns),
            *("| " + " | ".join(row) + " |" for row in rows)]


def execution_scopes(sources):
    scopes = []
    for name, source in sources.items():
        if source.get("state") != "completed":
            continue
        for scope in records(source.get("test_scopes")):
            runs = records(scope.get("runs"))
            if scope.get("kind") == "Playwright" and runs:
                scopes += [(name, {**scope, **run, "name": str(scope.get("name")) + f" / invocation {index}"}) for index, run in enumerate(runs, 1)]
            else:
                scopes.append((name, scope))
    return scopes


def test_details(base, head, ci):
    b, h = base.get("tests", {}), head.get("tests", {})
    valid = lambda t: numeric(t.get("count")) and t["count"] > 0 and numeric(t.get("coverage_pct"))
    bv, hv = valid(b), valid(h)
    lines = table(["Signal", "Base", "PR", "Delta"], [
        ["Test count", fmt(b.get("count")) if bv else "unavailable", fmt(h.get("count")) if hv else "unavailable", fmt(h['count'] - b['count']) if bv and hv else "n/a"],
        ["Coverage", fmt(b.get("coverage_pct"), "%") if bv else "unavailable", fmt(h.get("coverage_pct"), "%") if hv else "unavailable", delta(b.get("coverage_pct"), h.get("coverage_pct"), "higher", .1, .1, 1) if bv and hv else "n/a"]])
    lines += ["", "Test count includes passing Go test/subtest events, not independent scenarios."]
    if not hv:
        lines += ["Test count and coverage could not be collected for this PR; the test check remains authoritative."]
    if not bv:
        lines += ["Base collection unavailable: no test-count or coverage improvement is inferred."]
    summary = mapping(ci.get("ci-regression-summary.json"))
    counts = mapping(summary.get("tests"))
    if counts:
        lines += ["", "Current CI: " + ", ".join(f"{fmt(counts[k])} {k.replace('_', ' ')}" for k in ("passed", "failed", "skipped", "top_level_passed", "subtests_passed", "packages") if k in counts) + ".",
                  "Result scope: " + cell(counts.get("scope", "declared artifact scope")) + "."]
    coverage = mapping(summary.get("coverage"))
    if not hv and numeric(coverage.get("overall_pct")):
        lines += [f"Existing CI overall coverage: {fmt(coverage['overall_pct'], '%')} (candidate only; separate from the unavailable quality snapshot)."]
    if numeric(coverage.get("cache_pct")):
        lines += [f"Cache-package coverage: {fmt(coverage['cache_pct'], '%')} (candidate only)."]
    if counts.get("failed_names"):
        lines += ["Failing tests: " + ", ".join(cell(x) for x in counts["failed_names"][:8]) + "."]
    return lines


def conformance_details(summary):
    parents, subtests = mapping(summary.get("parents")), mapping(summary.get("subtests"))
    rows = [[label, fmt(parents.get(outcome)), fmt(subtests.get(outcome)) if outcome != "incomplete" else "parent completion only"]
            for outcome, label in (("pass", "Passed"), ("fail", "Failed"), ("skip", "Skipped"), ("incomplete", "Incomplete"))]
    lines = ["Existing conformance collector output: declaring unit tests plus the already-running non-race exceptions. " +
             "This artifact does not add live Loki, VictoriaLogs or Grafana runs. Candidate only; not full Loki compatibility.", "",
             *table(["Observed outcome", "Declaring parent tests", "Subtests"], rows), "",
             "Declared but not run: " + fmt(parents.get("not-run")) + " parent tests; these are not observed passes or skips.",
             "Counts deduplicate package + full test path across registry links. Parents and children are separate populations; do not add them as independent scenarios.",
             "Recorded revision: " + cell(summary.get("source", "unavailable")) + ".", ""]
    cases = [row for row in records(summary.get("cases")) if row.get("outcome") != "not-run"]
    priority = {"fail": 0, "incomplete": 1, "skip": 2, "pass": 3}
    cases.sort(key=lambda row: (priority.get(row.get("outcome"), 4), str(row.get("package")), str(row.get("test"))))
    lines += table(["Package", "Full test path", "Kind", "Outcome", "Linked registry items"], [])
    url = summary.get("artifact_url", "")
    artifact = (f"[conformance-evidence artifact]({url})" if isinstance(url, str) and len(url) <= 400 and
                re.fullmatch(r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[0-9]+/artifacts/[0-9]+", url)
                else "conformance-evidence artifact of the CI source above")
    # HTML and Markdown escaping can expand even bounded input cells. Reserve
    # space for the footer and cap the rendered section, not just row count.
    used, shown = len("\n".join(lines)), 0
    for row in cases[:40]:
        rendered = "| " + " | ".join([
            cell(row.get("package")), cell(row.get("test")), cell(row.get("kind")), cell(row.get("outcome")),
            ", ".join(cell(item) for item in row.get("items", [])[:4]) if isinstance(row.get("items"), list) else "unavailable"]) + " |"
        if used + len(rendered) + 1 > 11000:
            break
        lines.append(rendered)
        used += len(rendered) + 1
        shown += 1
    lines += ["", f"Showing {shown}/{len(cases)} observed identities; table cells are bounded, while exact full paths and all registry links remain in the {artifact}. Failures, incomplete parents and skips appear first."]
    return lines


def query_semantics_details(base, head):
    before = mapping(mapping(base.get("quality_inventory")).get("query_semantics"))
    after = mapping(mapping(head.get("quality_inventory")).get("query_semantics"))
    keys = (("cases", "Registered cases"), ("operations", "Tracked operations"),
            ("depth_paths", "Parser × depth paths"),
            ("exact_metric_points", "Exact metric-point contracts"),
            ("ranked_step_values", "Tie-tolerant ranked-step contracts"),
            ("exact_log_entries", "Exact log-entry contracts"),
            ("client_errors", "Expected client errors"), ("server_errors", "Expected server errors"),
            ("performance_cases", "Performance scenario definitions"),
            ("performance_shared", "Performance scenarios reusing quality cases"),
            ("performance_unshared", "Performance-only scenarios"))
    rows = []
    for key, label in keys:
        old, new = before.get(key), after.get(key)
        rows.append([label, fmt(old), fmt(new),
                     f"{new - old:+d}" if numeric(old) and numeric(new) else "n/a"])
    return ["Declared query cases and parser paths in the base and PR source. These counts do not claim execution or parity; the Loki compatibility check supplies live proof.", "",
            *table(["Signal", "Base", "PR", "Delta"], rows), "",
            "The matrix is the query catalog for quality and later performance scenarios. Visual evidence remains separate."]


# The corpus counts the report shows, in order (internal/querycorpus Summary).
QUERY_CORPUS_COUNTS = (
    ("client_rows", "Client queries (tests and routes)"),
    ("duplicated_client_queries", "Client queries defined in more than one test"),
    ("internal_parse_call_sites", "Internal parse call sites"),
    ("logql_rows", "Valid LogQL client queries"),
    ("untyped_logql_rows", "LogQL queries with an untyped node"),
    ("golden_rows", "Golden LogsQL translations"),
    ("golden_read", "Goldens our LogsQL parser reads"),
    ("golden_differ_by_version", "Goldens that differ between VictoriaLogs versions"),
    ("sent_rows", "LogsQL sent to VictoriaLogs"),
    ("sent_read", "Sent LogsQL our LogsQL parser reads"),
)


def error_compat_details(base, head):
    """Errors and status codes held to the standard of answers: the share of recorded Loki error answers the proxy
    reproduces and the share of route x error kind cells checked end to end, base versus PR (the registry's
    conformance/registry/generated/error-coverage.json, from conformance/scripts/error_registry.py)."""
    before = mapping(mapping(base.get("quality_inventory")).get("error_compat"))
    after = mapping(mapping(head.get("quality_inventory")).get("error_compat"))
    rows = []
    for key, label in (("matching_rows", "Recorded Loki answers matched"), ("comparable_rows", "Recorded Loki answers compared"),
                       ("covered_cells", "Matrix cells covered"), ("cells", "Matrix cells"),
                       ("messages_recorded", "Messages with a recorded answer"), ("messages", "Loki error messages")):
        old, new = before.get(key), after.get(key)
        rows.append([label, fmt(old) if before.get("state") == "available" else "no base", fmt(new),
                     f"{new - old:+d}" if numeric(old) and numeric(new) else "n/a"])
    for key, label in (("match_ratio", "Errors that match Loki"), ("coverage_ratio", "Error matrix coverage")):
        old, new = before.get(key), after.get(key)
        rows.append([label, f"{old * 100:.1f}%" if numeric(old) else "no base", f"{new * 100:.1f}%" if numeric(new) else "n/a",
                     f"{(new - old) * 100:+.1f} pp" if numeric(old) and numeric(new) else "n/a"])
    return ["The answer of every Loki error message, route by route and error kind by error kind, against the recorded answers of a real Loki "
            "(`docs/error-compatibility-coverage.md`). A drop in either percentage fails the quality gate.", "",
            *table(["Signal", "Base", "PR", "Delta"], rows)]


# The categories of TestQueryTextSites, in order.
QUERY_TEXT_SITES = (
    ("parse_calls", "LogQL/LogsQL parser calls outside the AST packages"),
    ("string_reads", "Query text read with strings/regexp"),
    ("text_built", "Query text built from literals"),
    ("deferred_raw", "Raw text in the LogsQL AST (DeferredExpr)"),
    ("translator_string_ops", "strings/regexp calls in the string translator"),
)


def query_text_sites_details(base, head):
    """Where production code handles query text as strings, base versus PR. The counts may only fall
    (TestQueryTextSites); the AST work takes each to 0."""
    before = mapping(mapping(mapping(base.get("quality_inventory")).get("query_text_sites")).get("totals"))
    after = mapping(mapping(mapping(head.get("quality_inventory")).get("query_text_sites")).get("totals"))
    rows = []
    for key, label in QUERY_TEXT_SITES:
        old, new = before.get(key), after.get(key)
        rows.append([label, fmt(old) if before else "no base", fmt(new),
                     f"{new - old:+d}" if numeric(old) and numeric(new) else "n/a"])
    note = ("Counted from the production source by `TestQueryTextSites` and held in `conformance/registry/query-text-sites.json`: "
            + "a count may only fall, and the AST work takes each to 0.")
    return [note, "", *table(["Site", "Base", "PR", "Delta"], rows)]


def query_corpus_details(corpus):
    """The corpus the PR's tests send in: counts against the base corpus and the committed targets, the goldens that
    changed per VictoriaLogs version, and the queries that got worse (the gate)."""
    summary, base = mapping(corpus.get("summary")), mapping(corpus.get("base"))
    limits = {c.get("name"): c for c in corpus.get("targets") or []}
    rows = []
    for key, label in QUERY_CORPUS_COUNTS:
        new, old = summary.get(key), base.get(key)
        limit = limits.get(key)
        rows.append([label, fmt(old) if base else "no base", fmt(new),
                     f"{new - old:+d}" if numeric(old) and numeric(new) else "n/a",
                     f"{limit['rule']} {fmt(limit['want'])}: {limit['state']}" if limit else "—"])
    lines = ["Every query the PR's tests send in, the LogsQL the proxy sends to VictoriaLogs, and the translator's LogsQL for each "
             f"LogQL query on VictoriaLogs {', '.join(cell(v) for v in summary.get('golden_versions') or []) or 'n/a'} (the goldens). "
             "Targets are in `conformance/registry/query-corpus-targets.json`; counts grow with new tests, so they inform and do not gate. "
             "The gate is per query: a query on both sides that turned untyped, or whose golden our parser no longer reads, fails it.", "",
             *table(["Count", "Base", "PR", "Delta", "Target"], rows)]
    reg = corpus.get("regressions")
    if isinstance(reg, dict):
        untyped, unread = reg.get("untyped") or [], reg.get("golden_unread") or []
        lines += ["", f"Regressions (queries on both sides): {len(untyped)} turned untyped"
                      + (" (" + ", ".join(cell(x) for x in untyped[:10]) + ")" if untyped else "")
                      + f"; {len(unread)} goldens no longer read" + (" (" + ", ".join(cell(x) for x in unread[:10]) + ")" if unread else "") + "."]
    diffs = corpus.get("golden_diffs") or []
    if diffs:
        lines += ["", *table(["VictoriaLogs", "Goldens compared", "Changed", "Added", "Removed", "Changed query ids (first 20)"],
                            [[cell(d.get("version")), fmt(d.get("compared")), fmt(d.get("changed")), fmt(d.get("added")), fmt(d.get("removed")),
                              ", ".join(cell(x) for x in d.get("examples") or []) or "none"] for d in diffs]),
                  "", "A changed golden is the translation of an existing query changing: expected for a translation change, a finding otherwise. "
                      "The rows are in the `pr-quality-report` artifact's corpus (`pr-head-query-corpus.jsonl.gz`)."]
    elif not base:
        lines += ["", "No base corpus: main's cached corpus for the base commit is missing or older than goldens, so nothing is compared or gated."]
    lines += oracle_details(corpus)
    lines += loki_details(corpus)
    return lines


# The counts of the Loki parse oracle (internal/querycorpus LokiCounts), in order.
LOKI_COUNTS = (
    ("queries", "Queries"),
    ("both_accept", "Both accept"),
    ("same_meaning", "… with the same meaning to Loki"),
    ("both_reject", "Both reject"),
    ("same_error", "… with Loki's error text"),
    ("ours_only", "The proxy accepts, Loki rejects"),
    ("loki_only", "Loki accepts, the proxy rejects"),
)


def loki_details(corpus):
    """Loki's own parser against the proxy's validation, on the corpus's client queries and on Loki's syntax test
    queries, base versus PR; the queries it now judges worse; and the proxy files that share code with Loki's."""
    loki = mapping(corpus.get("loki"))
    lines = []
    if loki:
        base = mapping(corpus.get("loki_base"))
        lines += ["", f"Loki {cell(loki.get('version'))}'s own parser (`test/oracle/loki`, test-only) against the proxy's query validation:", ""]
        for kind, title in (("client", "Client queries of the corpus"), ("case", "Loki's syntax test queries")):
            now, before = mapping(loki.get(kind)), mapping(base.get(kind))
            rows = [[label, fmt(before.get(k)) if before else "no base", fmt(now.get(k)),
                     f"{now[k] - before[k]:+d}" if numeric(before.get(k)) and numeric(now.get(k)) else "n/a"] for k, label in LOKI_COUNTS]
            lines += [f"{title}:", "", *table(["Count", "Base", "PR", "Delta"], rows), ""]
        reg = corpus.get("loki_regressions")
        if isinstance(reg, dict):
            lines.append("Regressions against Loki: " + "; ".join(f"{len(reg.get(k) or [])} {what}" for k, what in (
                ("disagree", "no longer agree on accepting"), ("error_lost", "error text no longer Loki's"),
                ("meaning_lost", "printed form no longer means the same"))) + ".")
    similar = corpus.get("similar")
    if isinstance(similar, list):
        lines += ["", "Repository files sharing runs of 50 tokens with Loki's sources (`test/oracle/loki/similarity-allowlist.json` lists the allowed ones, with why):", "",
                  *(table(["File", "Shared", "Allowed", "Loki file", "Note"],
                          [[cell(x.get("file")), fmt(x.get("shared")), fmt(x.get("allowed")), cell(x.get("best")),
                            "copy pending removal" if x.get("pending_removal") else ""] for x in similar])
                    if similar else ["none"])]
    return lines


# The counts of the VictoriaLogs parse oracle (internal/querycorpus OracleSummary), in order.
ORACLE_COUNTS = (
    ("goldens", "Goldens"),
    ("goldens_rejected", "Goldens VictoriaLogs rejects"),
    ("goldens_read", "Goldens our parser reads"),
    ("goldens_same_meaning", "… with the same meaning to VictoriaLogs"),
    ("sent", "Sent LogsQL (newest version only)"),
    ("sent_rejected", "Sent LogsQL VictoriaLogs rejects"),
    ("cases_accepted", "VictoriaLogs test cases it accepts"),
    ("cases_read", "… our parser reads with the same meaning"),
    ("cases_rejected", "VictoriaLogs test cases it rejects"),
    ("cases_wrongly_accepted", "… our parser accepts"),
    ("typed_selectors", "Stream selectors built as typed filters"),
    ("typed_selectors_rejected", "… whose string-translator filter VictoriaLogs rejects"),
    ("typed_selectors_same_meaning", "… VictoriaLogs reads the same as the string translator's"),
)


def oracle_details(corpus):
    """VictoriaLogs' own parser on the corpus and on its test cases, per version: base versus PR, and the queries it
    now judges worse (the gate)."""
    oracle = corpus.get("oracle") or []
    if not oracle:
        return []
    base = {o.get("version"): o for o in corpus.get("oracle_base") or []}
    lines = ["", "VictoriaLogs' own parser (`test/oracle/vl`, pinned per version) on the goldens, the LogsQL the tests sent and its own parser test cases:", ""]
    for o in oracle:
        v, b = o.get("version"), base.get(o.get("version"), {})
        rows = [[label, fmt(b.get(k)) if b else "no base", fmt(o.get(k)),
                 f"{o[k] - b[k]:+d}" if numeric(b.get(k)) and numeric(o.get(k)) else "n/a"] for k, label in ORACLE_COUNTS]
        lines += [f"VictoriaLogs {cell(v)}:", "", *table(["Count", "Base", "PR", "Delta"], rows), ""]
    for c in corpus.get("constructs") or []:
        missing = c.get("missing") or []
        lines.append(f"Constructs of VictoriaLogs {cell(c.get('version'))} not covered by `internal/logsql`: "
                     + (", ".join(cell(x) for x in missing[:20]) if missing else "none") + ".")
    for r in corpus.get("oracle_regressions") or []:
        parts = [f"{len(r.get(k) or [])} {what}" for k, what in (("goldens_rejected", "goldens now rejected"),
                                                                  ("goldens_meaning_lost", "goldens whose meaning changed"),
                                                                  ("cases_lost", "test cases no longer read the same"),
                                                                  ("typed_selectors_differ", "typed selector filters that differ"))]
        lines.append(f"Regressions on VictoriaLogs {cell(r.get('version'))}: " + "; ".join(parts) + ".")
    return lines


def query_semantics_runtime(sources):
    source = mapping(sources.get("loki"))
    if source.get("state") != "completed":
        return {}
    for scope in records(source.get("test_scopes")):
        matrix = mapping(scope.get("query_semantics_matrix"))
        if scope.get("state") == "available" and matrix:
            return matrix
    return {}


def paired_benchmarks(base, head):
    if mapping(head.get("performance")).get("mode") == "skipped":
        return []
    b, h = (mapping(mapping(s.get("performance")).get("benchmarks")) for s in (base, head))
    rows = []
    for prefix, label in (("query_range_cache_hit", "QueryRange cache-hit"), ("query_range_cache_bypass", "QueryRange cache-bypass"), ("labels_cache_hit", "Labels cache-hit"), ("labels_cache_bypass", "Labels cache-bypass")):
        for axis, unit, noun, pct, absolute, minimum in (("ns_per_op", " ns/op", "elapsed", 25, 750, 500), ("bytes_per_op", " B/op", "allocated bytes", 15, 32, 64), ("allocs_per_op", " allocs/op", "allocations", 15, 1, 1)):
            key = f"{prefix}_{axis}"
            if not numeric(b.get(key)) and not numeric(h.get(key)):
                continue
            if prefix.startswith("query_range"):
                absolute = {"ns_per_op": 1500, "bytes_per_op": 600, "allocs_per_op": 18}[axis]
            elif prefix == "labels_cache_bypass" and axis == "allocs_per_op":
                absolute = 12
            before, after = b.get(key), h.get(key)
            comparable = numeric(before) and numeric(after) and before > 0 and after >= 0
            change = delta(before, after, pct=pct, absolute=absolute, minimum=minimum) if comparable else "n/a"
            state = "regressed" if "regressed" in change else "improved" if "improved" in change else "stable" if comparable else "unavailable"
            rows.append({"case": prefix, "label": f"{label} {noun}", "base": before, "head": after, "unit": unit, "delta": change, "state": state})
    return rows


def candidate_benchmarks(ci):
    sample = mapping(ci.get("ci-handler-benchmarks.json"))
    if sample.get("state") != "available":
        return []
    return [row for row in records(sample.get("cases"))[:128]
            if isinstance(row.get("name"), str) and numeric(row.get("samples")) and 0 < row["samples"] <= 32
            and all(numeric(row.get(axis)) and row[axis] >= 0 for axis in ("ns_per_op", "bytes_per_op", "allocs_per_op"))]


def bench_summary(base, head, ci):
    rows = paired_benchmarks(base, head)
    comparable = [row for row in rows if row["state"] != "unavailable"]
    signals = []
    if comparable:
        counts = {state: sum(row["state"] == state for row in comparable) for state in ("regressed", "improved", "stable")}
        cases = len({row['case'] for row in comparable})
        text = f"Synthetic handlers: {cases} paired {'case' if cases == 1 else 'cases'}; " + ", ".join(f"{count} {state}" for state, count in counts.items()) + " metric axes."
        examples = sorted((row for row in comparable if row["state"] != "stable"), key=lambda row: (row["state"] != "regressed", row["label"]))[:3]
        if examples:
            text += " " + "; ".join(cell(row["label"]) + " " + row["delta"] for row in examples) + "."
        signals.append((bool(counts["regressed"]), text, "quality"))
    else:
        note = "skipped (no performance-sensitive paths changed)" if mapping(head.get("performance")).get("mode") == "skipped" else "no comparable paired metrics"
        signals.append((False, "Synthetic handlers: " + note + "; no improvement inferred.", "quality"))
    extra = candidate_benchmarks(ci)
    if extra:
        signals.append((False, f"{len(extra)} cases from the broader CI handler run (candidate only; no paired base or regression delta). See the synthetic microbenchmark details.", "ci"))
    return signals


def bench_details(base, head, ci):
    rows = paired_benchmarks(base, head)
    order = {"regressed": 0, "improved": 1, "unavailable": 2, "stable": 3}
    rows.sort(key=lambda row: (order[row["state"]], row["label"]))
    lines = ["Synthetic handler elapsed time and allocations; not process CPU, RSS or physical disk I/O. Repeated-sample medians.", "",
             *table(["Signal", "Base", "PR", "Delta"], [[cell(row["label"]), fmt(row["base"], row["unit"]), fmt(row["head"], row["unit"]), row["delta"]] for row in rows]), "",
             "Labels follow the existing gate percentage, absolute and low-baseline guards, including pooled-path allowances."]
    if not rows:
        lines += ["Skipped: no performance-sensitive paths changed." if mapping(head.get("performance")).get("mode") == "skipped" else "Paired base/PR measurements unavailable."]
    extra = candidate_benchmarks(ci)
    if extra:
        def elapsed(ns):
            return fmt(ns / 1e6, " ms") if ns >= 1e6 else fmt(ns / 1000, " µs") if ns >= 1000 else fmt(ns, " ns")
        # Preserve all observed cases in a bounded table; never rank unrelated
        # fixtures as though one being slower means a regression.
        lines += ["", "Additional existing CI handler benchmarks (candidate only). No paired base in this artifact; no speedup or regression is inferred. Separate runner/CPU settings from the paired quality samples above. Samples are observed repeats, not statistical confidence.", "",
                  *table(["Handler / fixture", "CPU setting", "Samples", "Elapsed/op", "Allocated B/op", "Allocs/op"],
                         [[cell(row["name"].removeprefix("BenchmarkProxy_")), fmt(row.get("cpu")), fmt(row["samples"]), elapsed(row["ns_per_op"]), fmt(row["bytes_per_op"]), fmt(row["allocs_per_op"])] for row in extra[:40]]), "",
                  f"Showing {min(40, len(extra))}/{len(extra)} candidate cases; all raw samples remain in the linked CI benchmarks artifact."]
        if mapping(ci.get("ci-handler-benchmarks.json")).get("job_conclusion") != "success":
            lines += ["The producing benchmark job did not succeed; these are partial observed samples, not a completed performance check."]
    elif ci.get("ci-handler-benchmarks.json"):
        lines += ["", "Additional CI handler samples unavailable: source/format verification did not produce current usable evidence."]
    return lines


def perf_rows(artifacts):
    rows = [(summary, row) for name, summary in sorted(artifacts.items())
            if name.startswith("summary-") and isinstance(summary, dict)
            for row in records(summary.get("rows"))]
    def priority(item):
        row = item[1]
        verdict = row.get("verdict")
        new_difference = row.get("parity_vs_reference") == "differs" and row.get("baseline_parity_vs_reference") != "differs"
        return (0 if verdict == "broken" else 1 if verdict == "slower" else 2 if new_difference else
                3 if verdict == "fixed" else 4 if verdict == "faster" else 5)
    return sorted(rows, key=priority)


def perf_examples(artifacts):
    examples = []
    for summary, row in perf_rows(artifacts):
        if row.get("verdict") not in ("slower", "broken", "faster", "fixed") or summary.get("valid") is False:
            continue
        b, p = summary.get("baseline", "base"), summary.get("candidate", "pr")
        if row.get("verdict") in ("faster", "slower") and any(t in row.get("stale", []) for t in (b, p)):
            continue  # A stale answer is not a latency reference.
        cold = summary.get("timing") == "cold"
        times = mapping(row.get("cold" if cold else "p50"))
        values = []
        for target in (b, p):
            status = mapping(row.get("status")).get(target)
            value = ms(times.get(target))
            if status is not None and str(status) != "200":
                value += " (HTTP " + cell(status) + ")"
            values.append(value)
        identity = " / ".join(cell(x) for x in (summary.get("profile") or "unspecified", summary.get("set") or "unspecified set", row.get("shape"), row.get("range")))
        notes = []
        if any(t in summary.get("not_like_for_like", []) for t in (b, p)):
            notes.append("not like-for-like")
        if row.get("note"):
            notes.append(cell(row["note"]))
        examples.append(cell(row["verdict"]) + ": " + identity + "; base " + values[0] + " → PR " + values[1] +
                        (" (cold)" if cold else " (warm p50)") + ("; " + "; ".join(notes) if notes else "") + ".")
        if len(examples) == 3:
            break
    return examples


def perf_details(artifacts):
    rows = []
    cpu_rows = []
    for summary, row in perf_rows(artifacts):
        targets = [summary.get(k, fallback) for k, fallback in (("baseline", "base"), ("candidate", "pr"), ("reference", "loki"))]
        cold, warm, tail = (mapping(row.get(k)) for k in ("cold", "p50", "p95"))
        timings = [f"{ms(cold.get(t))} / {ms(warm.get(t))}" for t in targets]
        timings[1] += " / " + ms(tail.get(targets[1]))
        for i, target in enumerate(targets):
            status = mapping(row.get("status")).get(target)
            if status is not None and str(status) != "200":
                timings[i] += " (HTTP " + cell(status) + ")"
        if targets[2] in row.get("cold_cache_served", []):
            timings[2] += " †"
        unlike = summary.get("not_like_for_like", [])
        note = "not like-for-like" if any(t in unlike for t in targets[:2]) else row.get("parity_vs_reference", "unavailable")
        if row.get("verdict"):
            note += "; " + str(row["verdict"])
        if row.get("note"):
            note += "; " + str(row["note"])
        if row.get("stale"):
            note += "; stale answer: " + ", ".join(str(t) for t in row["stale"]) + "; not a latency reference"
        identity = " / ".join(cell(x) for x in (summary.get("profile") or "unspecified", row.get("shape"), row.get("range")))
        rows.append([identity, *timings, cell(note)])
        cpu = mapping(row.get("vl_cpu_max"))
        if cpu:
            cpu_rows.append([identity, *(fmt(cpu.get(t), "%") for t in targets)])
    lines = ["Selected shape × range × Loki profile rows; not the complete workload baseline.", "", *table(["Profile / shape / range", "Base cold / warm p50", "PR cold / warm p50 / p95", "Loki cold / warm p50", "Result vs Loki"], rows[:20]), "", f"Showing {min(20, len(rows))}/{len(rows)} rows; full details remain in the linked A/B artifact/comment. † Loki's cache answered cold; not a computed cold comparison. Not-like-for-like rows are not speedup proof."]
    if cpu_rows:
        lines += ["", *table(["Profile / shape / range", "VL sampled max CPU, base", "VL sampled max CPU, PR", "VL sampled max CPU, Loki pass"], cpu_rows[:12]), "", "Docker CPU percentages sampled within ±1.5s of each request, maximum across that row's runs. Overlapping work and background activity can contribute; these are not query-attributable CPU seconds. This summary's zero may also mean missing samples."]
    return lines


def fleet_details(artifacts):
    reports = [v for k, v in sorted(artifacts.items()) if k.startswith("fleet-peer-cache-proof-rf") and isinstance(v, dict)]
    if not reports:
        return []
    rows, resources, containers, reads = [], [], [], []
    for report in reports:
        rf = fmt(report.get("replication_factor"))
        for row in records(report.get("summary")):
            identity = rf + " / " + " / ".join(cell(row.get(k)) for k in ("phase", "layer", "replica"))
            timings = [("no samples" if mapping(row.get(k)).get("count") == 0 else
                        " / ".join(fmt(mapping(row.get(k)).get(q)) for q in ("p50_ms", "p95_ms"))) for k in ("first", "repeat")]
            rows.append([identity, *timings, fmt(row.get("vl_calls")), fmt(row.get("peer_hits")) + " / " + fmt(row.get("peer_errors"))])
        groups = {}
        for request in records(report.get("requests")):
            key = tuple(str(request.get(k, "unavailable")) for k in ("phase", "layer", "replica"))
            groups.setdefault(key, []).append(request)
        for key, requests in groups.items():
            def counter(name, optional=False):
                values = [r.get(name, 0 if optional else None) for r in requests]
                return fmt(sum(values)) if all(numeric(v) for v in values) else "unavailable"
            routes = {}
            for request in requests:
                for route, value in mapping(request.get("fleet_vl_routes")).items():
                    if numeric(value):
                        routes[route] = routes.get(route, 0) + value
            route_text = ", ".join(cell(k) + "=" + fmt(v) for k, v in sorted(routes.items())[:4]) or "—"
            priority = 0 if any(word in key[0] for word in ("disk", "owner", "all_lost")) else 1
            reads.append((priority, [rf + " / " + " / ".join(cell(v) for v in key), str(len(requests)),
                counter("disk_hits") + " / " + counter("window_hits"), counter("peer_misses") + " / " + counter("backup_hits"),
                counter("fleet_vl_calls") + " / " + counter("vl_background_calls", optional=True), route_text]))
        for phase in records(report.get("phases")):
            for name, sample in mapping(phase.get("replicas")).items():
                sample = mapping(sample)
                measured = sample.get("samples") != 0
                rss = sample.get("rss_peak_bytes") if measured else None
                resources.append([rf + " / " + cell(phase.get("phase")) + " / " + cell(name), fmt(phase.get("seconds")) + " / " + fmt(sample.get("samples")), fmt(sample.get("cpu_seconds") if measured else None), fmt(rss / 1048576 if numeric(rss) else None), (fmt(sample.get("memory_cache_bytes_end")) + " / " + fmt(sample.get("disk_cache_bytes_end"))) if measured else "no samples"])
            for name, sample in mapping(phase.get("containers")).items():
                sample = mapping(sample)
                measured = sample.get("samples") != 0
                memory = sample.get("mem_peak_bytes") if measured else None
                containers.append([rf + " / " + cell(phase.get("phase")) + " / " + cell(name), fmt(phase.get("seconds")) + " / " + fmt(sample.get("samples")), fmt(sample.get("cpu_percent_avg") if measured else None, "%") + " / " + fmt(sample.get("cpu_percent_max") if measured else None, "%"), fmt(memory / 1048576 if numeric(memory) else None)])
    lines = ["Current PR candidate only. RF1/RF2 are different configurations, not main/PR comparison. First/repeat populations follow each fleet phase; first is not always cold VL.", "", *table(["RF / phase / layer / replica", "First p50 / p95 ms", "Repeat p50 / p95 ms", "VL calls", "Peer hits / errors"], rows[:16]), "", f"Showing {min(16, len(rows))}/{len(rows)} layer rows; full report in CI artifact.", "", *table(["RF / phase / replica", "Phase seconds / samples", "CPU seconds", "Peak RSS MiB", "End Loki-layer logical RAM / SSD bytes"], resources[:12]), "", "CPU seconds are sampled process-counter deltas within a phase, including background activity. RSS peaks belong to each sampled replica, not a simultaneous fleet-wide peak. Occupancy uses Loki-facing l1_memory/l2_disk counters; VL-facing cache occupancy is unmeasured here. Logical cache bytes are not physical disk I/O or process memory. Resources have no main-versus-PR delta."]
    if reads:
        reads.sort(key=lambda item: (item[0], item[1][0]))
        lines += ["", *table(["RF / phase / layer / replica", "Requests", "Disk / window hits", "Peer misses / backup hits", "Fleet VL / background VL calls", "Observed VL routes"], [row for _, row in reads[:16]]), "", f"Request-counter sums across each shown phase/layer/replica, including first and repeat requests. Showing {min(16, len(reads))}/{len(reads)} groups, disk-loss/restart and owner-loss phases first. Fleet VL calls exclude background calls; background is listed separately. These counters are not physical disk reads or throughput. Full routes and inventory-unit attribution remain in the artifact."]
    if containers:
        lines += ["", *table(["RF / phase / backend container", "Phase seconds / samples", "CPU avg / max", "Peak container memory MiB"], containers[:16]), "", "Existing docker stats samples from the candidate fleet's Loki/VL containers; CPU is Docker percentage, not CPU seconds or a main/PR delta. Memory is sampled container usage, not proxy RSS."]
    return lines


def render(base, head, evidence):
    base, head, evidence = mapping(base), mapping(head), mapping(evidence)
    # Malformed optional artifacts are absent evidence, never a reason to hide
    # the independently collected quality snapshot or the source check state.
    sources = {name: mapping(value) for name, value in mapping(evidence.get("sources")).items()}
    context = mapping(evidence.get("context"))
    for snapshot in (base, head):
        for key in ("tests", "compatibility", "performance"):
            snapshot[key] = mapping(snapshot.get(key))
    catalog = mapping(sources.get("catalog"))
    if catalog.get("state") == "completed" and catalog.get("conclusion") == "success":
        for snapshot, name in ((base, "base"), (head, "head")):
            inventory = mapping(snapshot.get("quality_inventory"))
            if mapping(inventory.get("query_semantics")).get("state") != "available":
                source = mapping(mapping(catalog.get("artifacts")).get(name))
                if source.get("state") == "available":
                    snapshot["quality_inventory"] = {**inventory, "query_semantics": source}
    lines = ["<!-- pr-quality-report -->", "## PR Quality Report", "", "Compared with the PR base commit; measurements below are scoped to their linked run.", ""]
    if context:
        lines += ["Base " + cell(context.get("base", "unavailable")) + " → PR " + cell(context.get("head", "unavailable")) + "; tested merge " + cell(context.get("merge", "unavailable")) + ".", ""]
    focus, urgent = [], []
    omitted = []

    def section(title, body, reason, prose=False):
        """A folded section, or nothing when it has no value: then its title and reason are listed once at the end."""
        if not prose:
            body, kept = prune(body)
            if not kept:
                omitted.append(f"{title} ({reason})")
                return []
        return fold(title, body)

    comparison = analyze(base, head)
    for signal in comparison["signals"]:
        target = focus if any(word in signal for word in (" new passing (", " recovered (", "inventory trend")) else urgent
        target.append(signal)
    inventory = mapping(head.get("quality_inventory"))
    if inventory.get("state") == "available":
        endpoints, gaps = mapping(inventory.get("endpoints")), mapping(inventory.get("gaps"))
        population = "read endpoints" if inventory.get("endpoint_scope") == "loki-read-api" else "endpoints"
        focus.append("Declared API inventory: " + fmt(endpoints.get("implemented")) + "/" + fmt(endpoints.get("total")) +
                     " " + population + " implemented, " + fmt(endpoints.get("differential")) + " linked to Loki comparison tests, " +
                     fmt(endpoints.get("declared_proven")) + " declared proven; " + fmt(gaps.get("open")) + " open compatibility gaps. These are registry counts, not a runtime pass percentage.")
    semantics = mapping(inventory.get("query_semantics"))
    semantics_run = query_semantics_runtime(sources)
    if semantics.get("state") == "available":
        strict_logs = (", " + fmt(semantics["exact_log_entries"]) + " exact log-entry contracts") if numeric(semantics.get("exact_log_entries")) else ""
        ranked = (", " + fmt(semantics["ranked_step_values"]) + " ranked-step contracts") if numeric(semantics.get("ranked_step_values")) and semantics["ranked_step_values"] else ""
        perf_only = (", " + fmt(semantics["performance_unshared"]) + " performance-only scenarios") if numeric(semantics.get("performance_unshared")) else ""
        focus.append("Query semantics catalog: " + fmt(semantics.get("cases")) + " cases, " +
                     fmt(semantics.get("depth_paths")) + " parser × depth paths, " +
                     fmt(semantics.get("exact_metric_points")) + " exact metric-point contracts" + ranked + strict_logs + perf_only + "; " +
                     "declared coverage, not a live pass count. " + link(sources.get("loki", {})))
    if semantics_run:
        (urgent if semantics_run.get("failed") else focus).append(
            "Pinned Loki query matrix: " + ", ".join(fmt(semantics_run.get(k)) + " " + k for k in ("passed", "failed", "skipped")) +
            " cases observed in CI. " + link(sources.get("loki", {})))
    for key in ("base", "head"):
        sample = (base if key == "base" else head).get("tests", {})
        if sample.get("execution_state") == "failed":
            urgent.append(f"{key.title()} Go test execution failed (exit {cell(sample.get('test_exit_code', 'unknown'))}); observed cases are retained and the population is incomplete.")
        elif not numeric(sample.get("count")) or sample["count"] <= 0:
            focus.append(f"{key.title()} test/coverage collection unavailable; no comparison inferred.")
    btests, htests = (s.get("tests", {}) for s in (base, head))
    if all(numeric(t.get("count")) and t["count"] > 0 and numeric(t.get("coverage_pct")) for t in (btests, htests)):
        change = htests["coverage_pct"] - btests["coverage_pct"]
        if abs(change) >= .1 - 1e-9:
            focus.append(f"Coverage {'increased' if change > 0 else 'decreased'} {abs(change):.2f} percentage points: {fmt(btests['coverage_pct'], '%')} → {fmt(htests['coverage_pct'], '%')}.")
        if htests["count"] < btests["count"]:
            focus.append(f"Passing test/subtest events decreased: {fmt(btests['count'])} → {fmt(htests['count'])}; inspect scope or removed coverage.")
    for key, label in (("loki", "Loki API"), ("drilldown", "Logs Drilldown"), ("vl", "VictoriaLogs")):
        btrack, htrack = (mapping(s.get("compatibility", {}).get(key)) for s in (base, head))
        if numeric(htrack.get("total")) and htrack["total"] > 0 and numeric(htrack.get("passed")):
            failed = htrack["total"] - htrack["passed"]
            if failed > 0:
                focus.append(f"{label} selected smoke contracts: {fmt(failed)} not passing ({fmt(htrack['passed'])}/{fmt(htrack['total'])}); inspect the quality run. " + link(sources.get("quality", {})))
            elif numeric(btrack.get("pct")) and numeric(htrack.get("pct")) and htrack["pct"] < btrack["pct"]:
                focus.append(f"{label} selected smoke score decreased: {fmt(btrack['pct'], '%')} → {fmt(htrack['pct'], '%')}. " + link(sources.get("quality", {})))
    perf, visual, ci = (data(sources, name) for name in ("perf", "visual", "ci"))
    conformance = mapping(ci.get("ci-conformance-summary.json"))
    if conformance:
        parents, children = mapping(conformance.get("parents")), mapping(conformance.get("subtests"))
        target = urgent if any(numeric(parents.get(outcome)) and parents[outcome] > 0 for outcome in ("fail", "incomplete")) else focus
        target.append("Recorded conformance: " + ", ".join(fmt(parents.get(outcome)) + " " + label for outcome, label in
                      (("pass", "passed"), ("fail", "failed"), ("skip", "skipped"), ("incomplete", "incomplete"))) +
                      " parent tests; " + ", ".join(fmt(children.get(outcome)) + " " + label for outcome, label in
                      (("pass", "passed"), ("fail", "failed"), ("skip", "skipped"))) +
                      " subtests. Linked execution only, not full Loki compatibility. " + link(sources.get("ci", {})))
    for regressed, signal, source in bench_summary(base, head, ci):
        (urgent if regressed else focus).append(signal + " " + link(sources.get(source, {})))
    verdict = mapping(perf.get("verdict.json"))
    if verdict:
        if verdict.get("state") in ("invalid", "error", "failed"):
            focus.append("A/B verdict: " + cell(verdict["state"]) + "; inspect the run before interpreting timings. " + link(sources.get("perf", {})))
        counts = mapping(verdict.get("counts"))
        focus += ["A/B selected workload: " + ", ".join(f"{fmt(counts[k])} {k}" for k in ("faster", "fixed", "same", "slower", "broken") if k in counts) + f"; {fmt(verdict.get('new_differences'))} new and {fmt(verdict.get('preexisting_differences'))} pre-existing Loki answer differences. " + link(sources.get("perf", {}))]
        over = mapping(verdict.get("over_2x"))
        if over:
            focus += ["Above 2× Loki: " + ", ".join(f"{fmt(over[k])} {k.replace('_', ' ')}" for k in ("new", "worsened", "known", "cleared", "locked", "lost_below") if k in over) + (" (warning only; green check does not clear gaps). " if over.get("mode") == "warn" else ". ") + link(sources.get("perf", {}))]
        if verdict.get("state") not in ("invalid", "error"):
            focus += [example + " " + link(sources.get("perf", {})) for example in perf_examples(perf)]
    vv = mapping(visual.get("verdict.json"))
    if vv:
        focus += [f"Visual proof: {fmt(vv.get('captures'))} captures, {len(vv.get('failures', []))} failures, {len(vv.get('missing', []))} missing, {fmt(vv.get('improved'))} improved, {fmt(vv.get('warnings'))} warnings. " + link(sources.get("visual", {}))]
    ci_summary = mapping(ci.get("ci-regression-summary.json"))
    ci_tests = mapping(ci_summary.get("tests"))
    if ci_tests:
        focus.append("Existing CI test evidence: " + ", ".join(f"{fmt(ci_tests[k])} {k}" for k in ("passed", "failed", "skipped") if k in ci_tests) +
                     "; scope: " + cell(ci_tests.get("scope", "declared artifact scope")) + ". " + link(sources.get("ci", {})))
    ci_coverage = mapping(ci_summary.get("coverage"))
    if (not numeric(htests.get("count")) or htests["count"] <= 0) and numeric(ci_coverage.get("overall_pct")):
        focus.append("Existing CI overall coverage: " + fmt(ci_coverage["overall_pct"], "%") + "; candidate only, base comparison unavailable. " + link(sources.get("ci", {})))
    jobs = [(name, job) for name, source in sources.items() if source.get("state") in ("completed", "pending")
            for job in records(source.get("jobs"))]
    def rank(item):
        job = item[1]
        return (0 if job.get("conclusion") not in (None, "success", "skipped", "neutral") else
                1 if job.get("status") != "completed" else 2, str(item[0]), str(job.get("name", "")))
    jobs.sort(key=rank)
    for name, job in jobs:
        if rank((name, job))[0] < 2:
            target = urgent if rank((name, job))[0] == 0 else focus
            target.append("Check " + cell(name) + " / " + cell(job.get("name")) + ": " +
                         cell(job.get("conclusion") or job.get("status") or "unavailable") + ". " + link(sources.get(name, {})))
    for name, source in sources.items():
        if source.get("note") or source.get("state") != "completed" or source.get("conclusion") not in ("success", "skipped"):
            target = urgent if source.get("note") or source.get("state") in ("stale", "error") or source.get("conclusion") in ("failure", "timed_out", "cancelled") else focus
            target += [cell(name) + ": " + cell(source.get("state", "unavailable")) + " / " + cell(source.get("conclusion") or "result unavailable") +
                      ("; " + cell(source["note"]) if source.get("note") else "") + ". " + link(source)]
    scopes = execution_scopes(sources)
    available_scopes = [scope for _, scope in scopes if scope.get("state") == "available"]
    if available_scopes:
        bad = sum(numeric(scope.get("failed")) and scope["failed"] > 0 for scope in available_scopes)
        skipped = sum(numeric(scope.get("skipped")) and scope["skipped"] > 0 for scope in available_scopes)
        unstable = sum(any(numeric(scope.get(key)) and scope[key] > 0 for key in ("flaky", "interrupted", "not_run")) for scope in available_scopes)
        target = urgent if bad or unstable else focus
        target.append(f"Broader API/Grafana execution: {len(available_scopes)} scopes with retained test outcomes; {bad} contain failing cases, {skipped} contain skips and {unstable} report flaky/incomplete cases. Candidate-only proof, not a full compatibility percentage.")
    errors = mapping(mapping(head.get("quality_inventory")).get("error_compat"))
    if errors.get("state") == "available":
        before = mapping(mapping(base.get("quality_inventory")).get("error_compat"))
        was = ""
        if before.get("state") == "available":
            for key in ("match_ratio", "coverage_ratio"):
                if numeric(before.get(key)) and numeric(errors.get(key)) and errors[key] + 1e-9 < before[key]:
                    urgent.append(f"Error compatibility fell ({key.replace('_', ' ')}: {before[key] * 100:.1f}% → {errors[key] * 100:.1f}%).")
            was = f" (base {before['match_ratio'] * 100:.1f}%)" if numeric(before.get("match_ratio")) else ""
        focus.append(f"Errors match Loki: {errors['match_ratio'] * 100:.1f}%{was} of {fmt(errors.get('comparable_rows'))} recorded answers; error matrix coverage "
                     f"{errors['coverage_ratio'] * 100:.1f}% of {fmt(errors.get('cells'))} route × error kind cells.")
    focus = urgent + focus
    if focus:
        lines += ["Focus signals:", "", *("- " + x for x in focus[:12]), ""]
        if len(focus) > 12:
            lines += ["Additional affected checks are listed below.", ""]
    lines += ["Main-versus-PR whole-process CPU, peak RSS, physical SSD throughput/IOPS and per-layer evictions are not measured here. Synthetic allocations and fleet cache occupancy do not substitute for them.", ""]
    if sources:
        names = [key for key in ("quality", "ci", "perf", "visual") if key in sources]
        names += sorted(set(sources) - set(names))
        lines += section("Evidence sources and revision status", table(["Source", "State", "Result", "Link", "Notes"], [[cell(key), cell(sources[key].get("state", "unavailable")), cell(sources[key].get("conclusion") or "pending / unavailable"), link(sources[key]), cell(sources[key].get("note", ""))] for key in names]) + ["", "Pending, stale, unavailable and skipped sources contribute no measurements."], "no source reported")
    lines += section("Coverage and tests", test_details(base, head, ci), "test count and coverage were not collected")
    if semantics.get("state") == "available":
        details = query_semantics_details(base, head)
        if semantics_run:
            details += ["", *table(["Live pinned Loki result", "Passed", "Failed", "Skipped"],
                                    [["Matrix cases", *(fmt(semantics_run.get(k)) for k in ("passed", "failed", "skipped"))]]),
                        "", "Failing case IDs: " + (", ".join(cell(x) for x in semantics_run.get("failed_cases", [])[:8]) or "none") + "."]
        lines += section("Query semantics catalog: base versus PR", details, "no catalog counts")
    if mapping(mapping(head.get("quality_inventory")).get("query_text_sites")).get("state") == "available":
        lines += section("Query text handled as strings: base versus PR", query_text_sites_details(base, head), "no site counts")
    if errors.get("state") == "available":
        lines += section("Error compatibility: base versus PR", error_compat_details(base, head), "no error counts")
    corpus = mapping(head.get("query_corpus"))
    if corpus:
        lines += section("Query corpus: base versus PR", query_corpus_details(corpus), "no corpus counts")
    if conformance:
        lines += section("Recorded conformance contracts and subtests", conformance_details(conformance), "no conformance outcomes")
    lines += section("API contract coverage and regression changes", comparison["details"], "no test outcomes, coverage or declared inventory in either snapshot")
    if scopes:
        scope_rows = []
        for name, scope in sorted(scopes, key=lambda item: (item[1].get("failed", 0) == 0, item[1].get("state") == "available", item[0], str(item[1].get("name")))):
            measured = scope.get("state") == "available"
            versions = ", ".join(cell(key) + "=" + cell(value) for key, values in mapping(scope.get("versions")).items() if isinstance(values, list) for value in values[:5])
            scope_rows.append([cell(name) + " / " + cell(scope.get("name")), cell(scope.get("state")),
                               fmt(scope.get("passed")) if measured else "unavailable", fmt(scope.get("failed")) if measured else "unavailable",
                               fmt(scope.get("skipped")) if measured else "unavailable",
                               fmt(scope.get("top_level_passed")) + " / " + fmt(scope.get("subtests_passed")) if scope.get("kind") == "Go" else "—",
                               " / ".join(fmt(scope.get(key)) for key in ("flaky", "interrupted", "not_run")) if scope.get("kind") == "Playwright" else "not recorded",
                               versions or cell(scope.get("note", ""))])
        lines += section("Broader API and Grafana execution scopes", ["Existing CI job logs, verified against the tested revision. Candidate only: these scopes have no paired base run here. Go counts include top-level cases and subtests; repeated identities count once and a failed repeat stays failed. Playwright invocations remain separate. Do not sum profiles as independent API coverage. Failed jobs can contain only partial observed populations.", "",
                      *table(["CI source / scope", "Evidence", "Passing", "Failing", "Skipped", "Go pass parent / child", "Flaky / interrupted / not run", "Logged stack / note"], scope_rows[:20]), "", f"Showing {min(20, len(scopes))}/{len(scopes)} scopes; source links are above."], "no scope reported outcomes")
    if verdict:
        lines += section("Real workload A/B: base, PR and Loki", perf_details(perf), "no A/B rows")
    if vv:
        lines += section("Visual proofs and regressions", ["Failures: " + (", ".join(cell(x) for x in vv.get("failures", [])[:8]) or "none"), "Missing captures: " + (", ".join(cell(x) for x in vv.get("missing", [])[:8]) or "none"), "See the linked Visual smoke source for main / PR / Loki montages, targeted fixes and general controls. Pixel changes alone are warnings, not compatibility proof."], "", prose=True)
    fleet = fleet_details(ci)
    if fleet:
        lines += section("Distributed cache: candidate only", fleet, "no cache measurements")
    lines += section("Synthetic handler microbenchmarks", bench_details(base, head, ci), "skipped: no performance-sensitive paths changed" if mapping(head.get("performance")).get("mode") == "skipped" else "no paired base/PR measurements")
    if jobs:
        lines += section("CI regression checks", table(["Source / check", "State", "Result"], [[cell(name) + " / " + cell(j.get("name")), cell(j.get("status")), cell(j.get("conclusion") or "unavailable")] for name, j in jobs[:60]]) + ["", f"Showing {min(60, len(jobs))}/{len(jobs)} checks; failed and pending checks first. Full job lists remain in the linked runs."], "no checks reported")
    if omitted:
        lines += ["No data (sections omitted): " + "; ".join(omitted) + ".", ""]
    lines += ["Required checks decide merge safety. This report summarizes selected evidence; rendering starts no tests or heavy workloads.", ""]
    return "\n".join(lines)


def main():
    if len(sys.argv) not in (4, 5):
        raise SystemExit("usage: pr_quality_report.py BASE.json HEAD.json OUT.md [EVIDENCE.json]")
    base, head = (json.loads(Path(p).read_text()) for p in sys.argv[1:3])
    evidence = json.loads(Path(sys.argv[4]).read_text()) if len(sys.argv) == 5 else {}
    Path(sys.argv[3]).write_text(render(base, head, evidence))


if __name__ == "__main__":
    main()
