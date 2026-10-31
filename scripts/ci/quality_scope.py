#!/usr/bin/env python3
"""Read declared registry inventory; never import or execute the measured tree."""
import argparse
import json
import re
from collections import Counter
from pathlib import Path

STATES = ("proven", "partial", "gap", "waived", "not-applicable", "unknown")
GAPS = ("open", "fixed", "documented", "owner-kept", "upstream")


def scalar(text, name, indent="", optional=False):
    matches = re.findall(rf"^{indent}{re.escape(name)}:[ \t]*(.*)$", text, re.M)
    if len(matches) != 1:
        if optional and not matches:
            return None
        raise ValueError(f"missing or duplicate {name}")
    value = matches[0].strip()
    if value.startswith('"'):
        value = json.loads(value)
        if not isinstance(value, str):
            raise ValueError(f"invalid {name}")
    elif value.startswith("'"):
        if not value.endswith("'") or len(value) < 2:
            raise ValueError(f"invalid {name}")
        value = value[1:-1].replace("''", "'")
    else:
        value = value.split(" #", 1)[0].strip()
    if not value:
        raise ValueError(f"empty {name}")
    return value


def methods(text, name, optional=False):
    value = scalar(text, name, optional=optional)
    if value is None:
        return []
    if not re.fullmatch(r"\[\s*(?:[A-Z]+(?:\s*,\s*[A-Z]+)*)?\s*\]", value):
        raise ValueError(f"invalid {name} list")
    return [m.strip() for m in value[1:-1].split(",") if m.strip()]


def registry_snapshot(root):
    """Return source inventory, not proof that any test ran or an item works.

    Missing or inconsistent inputs yield only ``state`` and ``note``; absent
    data must not become an apparent zero in base-versus-PR comparisons.
    """
    try:
        tree = Path(root).resolve()
        registry = tree / "conformance/registry"
        if not registry.resolve().is_relative_to(tree):
            raise ValueError("registry escapes source tree")

        def read(relative):
            path = registry / relative
            if not path.resolve().is_relative_to(registry.resolve()) or path.stat().st_size > 1024 * 1024:
                raise ValueError("registry input outside tree or too large")
            return path.read_text(encoding="utf-8")

        def json_file(relative):
            return json.loads(read(relative), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))

        def files(relative):
            path = registry / relative
            if not path.is_dir() or not path.resolve().is_relative_to(registry.resolve()):
                raise ValueError(f"missing {relative} directory")
            return sorted(path.rglob("*.yaml"))

        def text(path):
            return read(path.relative_to(registry))

        summary = json_file("generated/summary.json")
        keys = {"total": "endpoints", "implemented": "implemented", "differential": "with_differential_tests", "declared_proven": "proven"}
        if not isinstance(summary, dict):
            raise ValueError("invalid summary")
        endpoints = {key: summary.get(source) for key, source in keys.items()}
        if any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in endpoints.values()):
            raise ValueError("missing or invalid endpoint counts")
        if any(value > endpoints["total"] for value in endpoints.values()):
            raise ValueError("endpoint counts exceed total")
        version = summary.get("loki_version")
        if not isinstance(version, str) or not re.fullmatch(r"v?\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version):
            raise ValueError("missing or invalid registry surface version")

        items = {}
        state_files = files("state/loki")
        definitions = {str(p.relative_to(registry / "loki/endpoints"))[:-5] for p in files("loki/endpoints")}
        known = set()
        excluded_write_endpoints = 0
        for path in state_files:
            identifier = str(path.relative_to(registry / "state/loki"))[:-5]
            state_text = text(path)
            if scalar(state_text, "id") != identifier:
                raise ValueError("endpoint state id differs from path")
            state = scalar(state_text, "state")
            if state not in STATES:
                raise ValueError("unknown endpoint state")
            definition = read(f"loki/endpoints/{identifier}.yaml")
            if scalar(definition, "id") != identifier:
                raise ValueError("endpoint definition id differs from path")
            allowed = methods(definition, "methods")
            removed = methods(state_text, "out_of_scope_methods", optional=True)
            if not set(removed) <= set(allowed):
                raise ValueError("unknown excluded endpoint method")
            remaining = [m for m in allowed if m not in removed]
            known.add(identifier)
            if state == "not-applicable" or (allowed and not remaining):
                if allowed and "GET" not in allowed and "HEAD" not in allowed:
                    excluded_write_endpoints += 1
                continue
            items["endpoint:" + identifier] = {"id": identifier, "kind": "endpoint", "state": state,
                                              "path": scalar(definition, "path"), "methods": remaining}
        if known != definitions:
            raise ValueError("endpoint definitions and states differ")
        endpoint_items = [v for v in items.values() if v["kind"] == "endpoint"]
        if len(endpoint_items) != endpoints["total"] or sum(v["state"] == "proven" for v in endpoint_items) != endpoints["declared_proven"]:
            raise ValueError("endpoint summary differs from declared states")

        # The registry also tracks optional deletion administration. Keep that
        # visible separately instead of charging it to read API coverage. POST
        # query transports are reads and remain in the read endpoint population.
        deletion = {key: 0 for key in keys}
        for item in endpoint_items:
            if item["path"] != "/loki/api/v1/delete":
                continue
            identifier = item["id"]
            def entry(relative):
                document = json_file(relative)
                entries = document.get("endpoints") if isinstance(document, dict) else None
                if not isinstance(entries, list):
                    raise ValueError("missing deletion inventory")
                matches = [e for e in entries if isinstance(e, dict) and e.get("id") == identifier]
                if len(matches) != 1:
                    raise ValueError("missing or duplicate deletion entry")
                return matches[0]
            implemented = entry("generated/proxy/implementation.json").get("implemented")
            differential = entry("generated/proxy/coverage.json").get("differential_tests")
            if not isinstance(implemented, bool) or not isinstance(differential, dict) or set(differential) != {"unit", "e2e", "ui"} or any(
                    not isinstance(v, list) or any(not isinstance(test, str) for test in v) for v in differential.values()):
                raise ValueError("invalid deletion inventory")
            counts = {"total": 1, "implemented": int(implemented),
                      "differential": int(any(differential.values())), "declared_proven": int(item["state"] == "proven")}
            for key, value in counts.items():
                deletion[key] += value
                endpoints[key] -= value
            item["kind"] = "deletion-management"
        if any(value < 0 or value > endpoints["total"] for value in endpoints.values()):
            raise ValueError("read endpoint counts inconsistent")

        tracks = {}
        for track, kind in (("behaviours", "behaviour"), ("translations", "translation")):
            counts = Counter()
            for path in files("state/" + track):
                identifier = str(path.relative_to(registry / "state" / track))[:-5]
                content = text(path)
                if scalar(content, "id") != identifier:
                    raise ValueError(f"{kind} id differs from path")
                state = scalar(content, "state")
                if state not in STATES:
                    raise ValueError(f"unknown {kind} state")
                counts[state] += 1
                items[kind + ":" + identifier] = {"id": identifier, "kind": kind, "state": state}
            tracks[track] = {state: counts[state] for state in STATES}

        gaps = Counter()
        for path in files("cases"):
            content = text(path)
            identifier = str(path.relative_to(registry / "cases"))[:-5]
            if scalar(content, "id") != identifier:
                raise ValueError("case id differs from path")
            headers = re.findall(r"^gap:[ \t]*$", content, re.M)
            if not headers:
                if re.search(r"^gap:", content, re.M):
                    raise ValueError("invalid case gap value")
                continue
            block = re.search(r"^gap:[ \t]*\n((?:[ \t]+.*\n|\n)+)", content + "\n", re.M)
            if len(headers) != 1 or not block:
                raise ValueError("invalid case gap block")
            state = scalar(block[1], "status", indent="  ")
            if state not in GAPS:
                raise ValueError("unknown case gap status")
            gaps[state] += 1
            item = {"id": identifier, "kind": "case", "state": state}
            impact = scalar(block[1], "impact", indent="  ", optional=True)
            if impact:
                item["impact"] = impact
            items["case:" + identifier] = item

        errors = json_file("error-layer-known-leaks.json")
        leaks = errors.get("leaks") if isinstance(errors, dict) else None
        if not isinstance(leaks, dict) or any(not isinstance(k, str) or not k or not isinstance(v, list) or
                any(not isinstance(i, str) or not i for i in v) or len(set(v)) != len(v) for k, v in leaks.items()):
            raise ValueError("invalid error-layer exceptions")
        links = sum(len(v) for v in leaks.values())
        if not isinstance(errors.get("ceiling"), int) or isinstance(errors["ceiling"], bool) or errors["ceiling"] != links:
            raise ValueError("error-layer ceiling differs from case/cell links")
        return {"state": "available", "surface_version": version, "endpoints": endpoints, "tracks": tracks,
                "endpoint_scope": "loki-read-api", "deletion_management": deletion,
                "excluded_write_endpoints": excluded_write_endpoints,
                "gaps": {state: gaps[state] for state in GAPS}, "error_layer": {"cells": len(leaks), "links": links},
                "items": items}
    except (OSError, ValueError, TypeError, UnicodeError, RecursionError):
        return {"state": "unavailable", "note": "Registry inventory inputs are missing, malformed or inconsistent; no counts inferred."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(registry_snapshot(args.root), sort_keys=True))


if __name__ == "__main__":
    main()
