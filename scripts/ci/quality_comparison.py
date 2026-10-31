"""Compare already-collected test outcomes and declared conformance inventory."""
import html
import math
import re

LIMIT = 20
OUTCOMES = {"pass", "fail", "skip"}


def mapping(value):
    return value if isinstance(value, dict) else {}


def cell(value):
    text = re.sub(r"[\x00-\x1f\x7f]", " ", str(value))[:180]
    text = html.escape(text, quote=True).replace("@", "&#64;")
    return re.sub(r"([\\`*\[\]{}()|_~])", r"\\\1", text)


def number(value):
    try:
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and abs(value) <= 10**12
    except OverflowError:
        return False


def count(value):
    return number(value) and value >= 0 and int(value) == value


def fmt(value):
    return f"{value:g}" if number(value) else "unavailable"


def table(columns, rows):
    return ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns),
            *("| " + " | ".join(row) + " |" for row in rows[:LIMIT]),
            "", f"Showing {min(LIMIT, len(rows))}/{len(rows)} rows."]


def outcomes(snapshot):
    value = mapping(snapshot.get("tests")).get("outcomes")
    if not isinstance(value, dict):
        return None
    # An empty mapping is a measured empty population, while malformed data is
    # unavailable: filtering bad events would invent removed/passing cases.
    if any(not isinstance(key, str) or "\t" not in key or not all(key.split("\t", 1))
           or not isinstance(state, str) or state not in OUTCOMES for key, state in value.items()):
        return None
    return value


def test_kind(identity):
    return "subtests" if "/" in identity.partition("\t")[2] else "top-level"


def execution_complete(snapshot):
    return mapping(snapshot.get("tests")).get("execution_state", "complete") == "complete"


def compare_outcomes(base, head, signals, details):
    before, after = outcomes(base), outcomes(head)
    base_complete, head_complete = execution_complete(base), execution_complete(head)
    details += ["Exact Go case outcomes from the existing quality test run. Top-level tests and subtests are separate populations; package + full test name defines identity.", ""]
    rows = []
    for kind in ("top-level", "subtests"):
        for state in ("pass", "fail", "skip"):
            values = [str(sum(test_kind(key) == kind and value == state for key, value in sample.items()))
                      if sample is not None else "unavailable" for sample in (before, after)]
            rows.append([kind + " " + state, *values])
    details += table(["Observed case outcomes", "Base", "PR"], rows) + [""]
    incomplete = []
    for label, snapshot, complete in (("base", base, base_complete), ("candidate", head, head_complete)):
        if not complete:
            tests = mapping(snapshot.get("tests"))
            exit_code = tests.get("test_exit_code")
            incomplete.append(label + " execution " + cell(tests.get("execution_state", "unavailable")) +
                              (f" (exit {exit_code:g})" if count(exit_code) else ""))
    if incomplete:
        details += ["Incomplete observed population: " + ", ".join(incomplete) +
                    ". Counts cover observed outcomes only. Missing identities do not prove removals or new passing cases; shared observed identities remain comparable.", ""]
    if before is None or after is None:
        details += ["Outcome comparison unavailable: " + ", ".join(label + " unavailable" for label, sample in (("base", before), ("candidate", after)) if sample is None) + "; no additions, removals or fixes inferred.", ""]
    changes = []
    if after is not None:
        failures = [key for key, state in after.items() if state == "fail"]
        if failures:
            signals.append(f"Existing quality outcomes: {len(failures)} current failing cases; " + ", ".join(cell(key) for key in sorted(failures)[:3]) + ".")
    if before is not None and after is not None:
        groups = {"newly failing": [], "newly skipped": [], "removed": [], "recovered": [], "new passing": []}
        for key in sorted(set(before) | set(after)):
            b, h = before.get(key), after.get(key)
            complete = base_complete and head_complete
            label = ("removed" if h is None and complete else "newly failing" if h == "fail" and b != h and (b is not None or base_complete) else
                     "newly skipped" if h == "skip" and b != h and (b is not None or complete) else
                     "new passing" if h == "pass" and b is None and complete else
                     "recovered" if h == "pass" and b in ("fail", "skip") else None)
            if label:
                groups[label].append(key)
                changes.append((list(groups).index(label), key, label, b or "not present", h or "not present"))
        for label, keys in groups.items():
            if keys:
                kinds = ", ".join(f"{sum(test_kind(key) == kind for key in keys)} {kind}" for kind in ("top-level", "subtests"))
                signals.append(f"Exact case comparison: {len(keys)} {label} ({kinds}); " + ", ".join(cell(key) for key in keys[:3]) + ".")
        # Keep unchanged failures visible alongside changed outcomes.
        changes += [(-1, key, "still failing", "fail", "fail") for key in after if after[key] == "fail" and before.get(key) == "fail"]
        changes += [(-1, key, "current failing", "not observed", "fail") for key in after
                    if after[key] == "fail" and key not in before and not base_complete]
    elif after is not None:
        changes = [(0, key, "current failing", "unavailable", "fail") for key in after if after[key] == "fail"]
    if changes:
        rows = []
        for _, key, label, b, h in sorted(changes):
            package, _, name = key.partition("\t")
            rows.append([cell(label), cell(package), cell(name), test_kind(key), cell(b), cell(h)])
        details += table(["Change", "Package", "Full test name", "Population", "Base", "PR"], rows) + [""]


def package_coverage(snapshot):
    raw = mapping(snapshot.get("tests")).get("package_coverage")
    if not isinstance(raw, dict):
        return None
    result = {}
    for package, sample in raw.items():
        if not isinstance(package, str):
            continue
        sample = mapping(sample)
        covered, total = sample.get("covered"), sample.get("total")
        if count(covered) and count(total) and total > 0 and covered <= total:
            result[package] = (covered, total, covered / total * 100)
        else:
            result[package] = None
    return result


def compare_coverage(base, head, signals, details):
    before, after = package_coverage(base), package_coverage(head)
    if before is None and after is None:
        details += ["Per-package statement coverage unavailable in both snapshots.", ""]
        return
    rows, drops = [], []
    for package in sorted(set(before or {}) | set(after or {})):
        b, h = (sample.get(package) if sample is not None else None for sample in (before, after))
        change = h[2] - b[2] if b and h else None
        if change is not None and change < -1e-9:
            drops.append((change, package))
        text = lambda value: f"{value[0]:g}/{value[1]:g} ({value[2]:.2f}%)" if value else "unavailable"
        rows.append((change if change is not None else 0, package, [cell(package), text(b), text(h), f"{change:+.2f} pp" if change is not None else "unavailable"]))
    for change, package in sorted(drops)[:3]:
        signals.append(f"Package statement coverage decreased {abs(change):.2f} percentage points: {cell(package)}.")
    details += ["Per-package covered/total instrumented statements; missing packages or snapshots have no coverage delta. Coverage changes also reflect changes in the statement denominator.", "",
                *table(["Package", "Base covered/total", "PR covered/total", "Change"], [row for _, _, row in sorted(rows)]), ""]


def available_inventory(snapshot):
    value = mapping(snapshot.get("quality_inventory"))
    return value if value.get("state") == "available" else None


def inventory_state(item):
    state = mapping(item).get("state")
    return state if isinstance(state, str) else None


def degraded(before, after):
    # Different registries have different state vocabularies. Do not order
    # owner-kept/documented/upstream decisions as degrees of correctness.
    ranks = {"proven": 3, "partial": 2, "gap": 1, "unknown": 0}
    return ((before in ranks and after in ranks and ranks[after] < ranks[before]) or
            before == "fixed" and after == "open")


def compare_inventory(base, head, signals, details):
    before, after = available_inventory(base), available_inventory(head)
    details += ["Declared conformance inventory, not full runtime compatibility. Registry states describe the checked-in contract and proof links; these counts do not certify that every endpoint, version or workload ran successfully.", ""]
    if before is None or after is None:
        details += ["Declared inventory comparison: " + ", ".join(label + " unavailable" for label, sample in (("base", before), ("candidate", after)) if sample is None) + "; no count or state improvements inferred.", ""]
    details += ["Loki surface version: " + " → ".join(cell(sample.get("surface_version", "unavailable")) if sample else "unavailable" for sample in (before, after)) + ".", ""]
    versions = [mapping(sample).get("surface_version") for sample in (before, after)]
    changed_surface = all(isinstance(version, str) and version for version in versions) and versions[0] != versions[1]
    scope_note = ("; Loki surface changed " + cell(versions[0]) + " → " + cell(versions[1]) + "; review inventory scope" if changed_surface else "")
    if changed_surface:
        details += ["The Loki surface changed: counts describe different declared inventories. Review added or removed surface definitions before interpreting count differences.", ""]
    rows = []
    def metric(label, path):
        values = []
        for sample in (before, after):
            value = sample
            for part in path:
                value = mapping(value).get(part)
            values.append(value if count(value) else None)
        rows.append([label, *(fmt(value) for value in values), f"{values[1] - values[0]:+g}" if all(value is not None for value in values) else "unavailable"])
        return values
    def trend(label, values, positive=False):
        if not all(value is not None for value in values) or values[0] == values[1]:
            return
        if values[1] < values[0]:
            signals.append(label + " decreased: " + fmt(values[0]) + " → " + fmt(values[1]) + "; inspect declared coverage" + scope_note + ".")
        elif positive:
            signals.append(label + " increased: " + fmt(values[0]) + " → " + fmt(values[1]) + "; declared inventory trend, not runtime proof" + scope_note + ".")
    for key, label in (("total", "Endpoints: declared total"), ("implemented", "Endpoints: declared implemented"),
                       ("differential", "Endpoints: linked differential tests"), ("declared_proven", "Endpoints: declared proven")):
        values = metric(label, ("endpoints", key))
        if key != "total":
            noun = "proven" if key == "declared_proven" else key
            trend("Declared " + noun + " endpoints", values, positive=key == "declared_proven")
    details += table(["Declared endpoint inventory", "Base", "PR", "Change"], rows) + [""]
    rows = []
    for track in ("behaviours", "translations"):
        states = set()
        for sample in (before, after):
            states.update(key for key in mapping(mapping(mapping(sample).get("tracks")).get(track)) if isinstance(key, str))
        for state in sorted(states):
            values = metric(cell(track) + ": " + cell(state), ("tracks", track, state))
            if state == "proven":
                trend("Declared " + cell(track) + " proven count", values, positive=True)
    if rows:
        details += table(["Declared track states", "Base", "PR", "Change"], rows) + [""]
    rows = []
    for state in ("open", "fixed", "documented", "owner-kept", "upstream"):
        values = metric("Declared gaps: " + cell(state), ("gaps", state))
        if state == "open" and all(value is not None for value in values):
            if values[1] > values[0]:
                signals.append(f"Open declared gaps increased: {fmt(values[0])} → {fmt(values[1])}; inspect changed registry cases" + scope_note + ".")
            elif values[1] < values[0]:
                signals.append(f"Open declared gaps decreased: {fmt(values[0])} → {fmt(values[1])}; declared inventory trend, not runtime proof" + scope_note + ".")
    for key in ("cells", "links"):
        values = metric("Known error-layer leak " + key, ("error_layer", key))
        if key == "cells" and all(value is not None for value in values) and values[1] > values[0]:
            signals.append(f"Known error-layer leak cells increased: {fmt(values[0])} → {fmt(values[1])}; inspect declared leaks" + scope_note + ".")
    details += table(["Declared gaps / error layer", "Base", "PR", "Change"], rows) + [""]
    if before is None or after is None:
        return
    bitems, hitems = (mapping(sample.get("items")) for sample in (before, after))
    changes = []
    for key in sorted(key for key in set(bitems) | set(hitems) if isinstance(key, str)):
        b, h = inventory_state(bitems.get(key)), inventory_state(hitems.get(key))
        if b == h or b is None and key in bitems or h is None and key in hitems:
            continue
        label = "degraded" if degraded(b, h) else "added" if key not in bitems else "removed" if key not in hitems else "changed"
        changes.append((0 if label == "degraded" else 1 if label == "removed" else 2, key, label, b, h))
    regressions = [item for item in changes if item[2] == "degraded"]
    if regressions:
        signals.append(f"Declared conformance states degraded: {len(regressions)}; " + ", ".join(cell(key) + " " + cell(b) + " → " + cell(h) for _, key, _, b, h in regressions[:3]) + scope_note + ".")
    if changes:
        details += table(["Declared item", "Transition", "Base state", "PR state"],
                         [[cell(key), label, cell(b or "not present"), cell(h or "not present")] for _, key, label, b, h in sorted(changes)]) + [""]


def compare_smoke(base, head, details):
    rows = []
    for track in ("loki", "drilldown", "vl"):
        components = mapping(mapping(mapping(head.get("compatibility")).get(track)).get("components"))
        for key in sorted(key for key in components if isinstance(key, str)):
            sample = mapping(components[key])
            passed, total = sample.get("passed"), sample.get("total")
            valid = count(passed) and count(total) and total > 0 and passed <= total
            score = passed / total * 100 if valid else None
            value = f"{passed:g}/{total:g} ({score:.2f}%)" if valid else "unavailable"
            priority = 0 if score is not None and score < 100 else 1
            rows.append((priority, track, key, [cell(track), cell(key), value]))
    if rows:
        details += ["Selected smoke component checks against the candidate backend only. Both quality snapshot invocations share that backend image, so these checks provide no runtime comparison between revisions. Use the real workload A/B evidence for separately built revisions. This is a small contract sample, not the full declared inventory or version matrix.", "",
                    *table(["Track", "Component", "PR passing"], [row for _, _, _, row in sorted(rows)]), ""]


def analyze(base, head):
    """Return escaped focus signals and bounded Markdown detail tables."""
    base, head = mapping(base), mapping(head)
    signals, details = [], []
    compare_outcomes(base, head, signals, details)
    compare_coverage(base, head, signals, details)
    compare_inventory(base, head, signals, details)
    compare_smoke(base, head, details)
    return {"signals": signals, "details": details}
