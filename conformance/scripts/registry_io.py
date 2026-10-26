"""Small file helpers shared by the conformance scripts.

Every read and write goes through a context manager so no handle is left open,
and the scripts stay readable.
"""
import json


def read_text(path, errors="strict"):
    with open(path, encoding="utf-8", errors=errors) as handle:
        return handle.read()


def load_json(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


def dump_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def state_field(identifier, field, track="loki"):
    """One scalar field of an item's state file, or None."""
    import os, re
    path = os.path.join("conformance/registry/state", track, identifier + ".yaml")
    if not os.path.exists(path):
        return None
    match = re.search(rf"^{field}:\s*(\S+)\s*$", read_text(path), re.M)
    return match.group(1) if match else None


def out_of_scope():
    """Endpoint ids whose state is not-applicable: the proxy serves Loki's read API, so
    write endpoints are left out of every total, ratio, gap list and ranking."""
    import os
    base = "conformance/registry/state/loki"
    names = sorted(os.listdir(base)) if os.path.isdir(base) else []
    return {n[:-5] for n in names if n.endswith(".yaml") and state_field(n[:-5], "state") == "not-applicable"}
