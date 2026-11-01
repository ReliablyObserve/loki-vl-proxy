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
            values.append("HTTP " + cell(status) if status is not None and str(status) != "200" else ms(times.get(target)))
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
                timings[i] = "HTTP " + cell(status)
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
    lines = ["<!-- pr-quality-report -->", "## PR Quality Report", "", "Compared with the PR base commit; measurements below are scoped to their linked run.", ""]
    if context:
        lines += ["Base " + cell(context.get("base", "unavailable")) + " → PR " + cell(context.get("head", "unavailable")) + "; tested merge " + cell(context.get("merge", "unavailable")) + ".", ""]
    focus, urgent = [], []
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
    focus = urgent + focus
    if focus:
        lines += ["Focus signals:", "", *("- " + x for x in focus[:12]), ""]
        if len(focus) > 12:
            lines += ["Additional affected checks are listed below.", ""]
    lines += ["Main-versus-PR whole-process CPU, peak RSS, physical SSD throughput/IOPS and per-layer evictions are not measured here. Synthetic allocations and fleet cache occupancy do not substitute for them.", ""]
    if sources:
        names = [key for key in ("quality", "ci", "perf", "visual") if key in sources]
        names += sorted(set(sources) - set(names))
        lines += fold("Evidence sources and revision status", table(["Source", "State", "Result", "Link", "Notes"], [[cell(key), cell(sources[key].get("state", "unavailable")), cell(sources[key].get("conclusion") or "pending / unavailable"), link(sources[key]), cell(sources[key].get("note", ""))] for key in names]) + ["", "Pending, stale, unavailable and skipped sources contribute no measurements."])
    lines += fold("Coverage and tests", test_details(base, head, ci))
    lines += fold("API contract coverage and regression changes", comparison["details"])
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
        lines += fold("Broader API and Grafana execution scopes", ["Existing CI job logs, verified against the tested revision. Candidate only: these scopes have no paired base run here. Go counts include top-level cases and subtests; repeated identities count once and a failed repeat stays failed. Playwright invocations remain separate. Do not sum profiles as independent API coverage. Failed jobs can contain only partial observed populations.", "",
                      *table(["CI source / scope", "Evidence", "Passing", "Failing", "Skipped", "Go pass parent / child", "Flaky / interrupted / not run", "Logged stack / note"], scope_rows[:20]), "", f"Showing {min(20, len(scopes))}/{len(scopes)} scopes; source links are above."])
    if verdict:
        lines += fold("Real workload A/B: base, PR and Loki", perf_details(perf))
    if vv:
        lines += fold("Visual proofs and regressions", ["Failures: " + (", ".join(cell(x) for x in vv.get("failures", [])[:8]) or "none"), "Missing captures: " + (", ".join(cell(x) for x in vv.get("missing", [])[:8]) or "none"), "See the linked Visual smoke source for main / PR / Loki montages, targeted fixes and general controls. Pixel changes alone are warnings, not compatibility proof."])
    fleet = fleet_details(ci)
    if fleet:
        lines += fold("Distributed cache: candidate only", fleet)
    lines += fold("Synthetic handler microbenchmarks", bench_details(base, head, ci))
    if jobs:
        lines += fold("CI regression checks", table(["Source / check", "State", "Result"], [[cell(name) + " / " + cell(j.get("name")), cell(j.get("status")), cell(j.get("conclusion") or "unavailable")] for name, j in jobs[:60]]) + ["", f"Showing {min(60, len(jobs))}/{len(jobs)} checks; failed and pending checks first. Full job lists remain in the linked runs."])
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
