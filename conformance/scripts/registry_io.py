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


# The one explanation of what the proxy serves, reused by every generated report.
SCOPE_SENTENCE = ("The proxy serves Loki's read API; write endpoints such as push are not supported "
                  "(ingest goes to VictoriaLogs directly); delete may be supported, and the "
                  "alerting-rules API may come last; no other write endpoint is planned.")
WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
# The one endpoint whose write methods stay in scope (the owner may support delete).
WRITE_IN_SCOPE = {"loki_api_v1_delete"}
STATE_DIR = "conformance/registry/state/loki"
ENDPOINT_DIR = "conformance/registry/loki/endpoints"


def list_field(path, field):
    """A flow-list field (`field: [A, B]`) of a registry file as a list; [] when absent."""
    import os, re
    if not os.path.exists(path):
        return []
    match = re.search(rf"^{field}:\s*\[(.*)\]\s*$", read_text(path), re.M)
    return [x.strip() for x in match.group(1).split(",") if x.strip()] if match else []


def endpoint_methods(identifier):
    return list_field(f"{ENDPOINT_DIR}/{identifier}.yaml", "methods")


def out_of_scope_methods(identifier):
    """Write methods the owner left out of scope for an endpoint (state file)."""
    return list_field(f"{STATE_DIR}/{identifier}.yaml", "out_of_scope_methods")


def read_methods(identifier):
    """The methods an endpoint is judged on: its methods minus the out-of-scope write methods."""
    left = set(out_of_scope_methods(identifier))
    return [m for m in endpoint_methods(identifier) if m not in left]


def out_of_scope():
    """Endpoint ids the proxy does not serve at all: state not-applicable, or every method listed
    in out_of_scope_methods. They are left out of every total, ratio, gap list and ranking."""
    import os
    names = sorted(os.listdir(STATE_DIR)) if os.path.isdir(STATE_DIR) else []
    ids = {n[:-5] for n in names if n.endswith(".yaml")}
    return {i for i in ids if state_field(i, "state") == "not-applicable"
            or (endpoint_methods(i) and not read_methods(i))}


def scope_problems():
    """Strict consistency of the method-level scope; returns a list of messages."""
    import os
    problems = []
    names = sorted(os.listdir(ENDPOINT_DIR)) if os.path.isdir(ENDPOINT_DIR) else []
    for name in names:
        if not name.endswith(".yaml"):
            continue
        identifier = name[:-5]
        methods = set(endpoint_methods(identifier))
        listed = set(out_of_scope_methods(identifier))
        state = state_field(identifier, "state")
        for method in sorted(listed - methods):
            problems.append(f"{identifier}: out_of_scope_methods lists {method}, which the endpoint does not have")
        for method in sorted(listed - WRITE_METHODS):
            problems.append(f"{identifier}: out_of_scope_methods lists {method}, which is not a write method")
        if listed and identifier in WRITE_IN_SCOPE:
            problems.append(f"{identifier}: delete stays in scope, it cannot list out_of_scope_methods")
        if listed:
            path = f"{STATE_DIR}/{identifier}.yaml"
            text = read_text(path) if os.path.exists(path) else ""
            if "owner 2026-10-27" not in text:
                problems.append(f"{identifier}: out_of_scope_methods needs the owner decision (owner 2026-10-27) in owner_decision")
        unlisted = sorted((methods & WRITE_METHODS) - listed) if identifier not in WRITE_IN_SCOPE else []
        # POST on a read endpoint is a form-encoded read; only an endpoint with PUT, PATCH or
        # DELETE (or POST as its only method) carries a write that must be decided.
        decisive = [m for m in unlisted if m != "POST" or methods == {"POST"}
                    or methods & {"DELETE", "PUT", "PATCH"}]
        if decisive:
            problems.append(f"{identifier}: write methods {', '.join(decisive)} are neither in out_of_scope_methods nor allowed")
        everything = bool(methods) and methods <= listed
        if everything and state != "not-applicable":
            problems.append(f"{identifier}: every method is out of scope, so its state must be not-applicable (is {state})")
        if state == "not-applicable" and not everything:
            problems.append(f"{identifier}: state not-applicable requires out_of_scope_methods to list every method")
    return problems
