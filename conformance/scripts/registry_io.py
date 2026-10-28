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


# --- performance acceptance (the `perf:` block) ------------------------------------------------------------
# The owner's rule (2026-10-28): at most 2× Loki in the worst case, close to Loki in general and below Loki for
# most queries. Every registry item an A/B shape covers (bench/ab/shapes.json `covers`) carries the block in its
# own file (not its state file), in this fixed form so it is read without a YAML library:
#
#   perf:
#     max_ratio: 2.0          # the hard bound: a locked row above it is a regression (at most PERF_MAX)
#     target_ratio: 1.0       # the target: a row between it and max_ratio needs an effort_exception
#     modes: [cold, warm, refresh]
#     locked: true            # every row measured within max_ratio on the daily run; set by people only
#     locked_since: '2026-10-27'
#     effort_exception: null  # or the two keys below, one line each
#       reason: 'why going below Loki is high effort now (quote the evidence)'
#       follow_up: 'what would bring it below Loki'
PERF_MAX = 2.0
PERF_MODES = ("cold", "warm", "refresh")
PERF_DEFAULTS = {"max_ratio": PERF_MAX, "target_ratio": 1.0, "modes": list(PERF_MODES), "locked": False,
                 "locked_since": None, "effort_exception": None}
REGISTRY_ROOT = "conformance/registry"


def _scalar(text):
    """One value of the block: a number, true/false/null, a [flow, list] or a quoted or bare string; a
    trailing `# comment` after an unquoted value is dropped."""
    import re
    text = text.strip()
    if text[:1] not in ("'", '"'):
        text = text.split(" #")[0].strip()
    if text in ("true", "false"):
        return text == "true"
    if text in ("null", "~", ""):
        return None
    if text.startswith("[") and text.endswith("]"):
        return [_scalar(x) for x in text[1:-1].split(",") if x.strip()]
    if len(text) >= 2 and text[0] == text[-1] == "'":
        return text[1:-1].replace("''", "'")
    if len(text) >= 2 and text[0] == text[-1] == '"':
        return text[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if re.fullmatch(r"-?\d+(\.\d+)?", text):
        return float(text)
    return text


def parse_perf(text):
    """(block or None, problems) for one registry file's text; the block has every key, defaults filled."""
    import re
    starts = re.findall(r"(?m)^perf:", text)
    if not starts:
        return None, []
    if len(starts) > 1:
        return None, ["more than one perf: block"]
    match = re.search(r"(?m)^perf:[ \t]*\n((?:(?: {2}.*)?\n)*)", text + "\n")
    if not match or re.match(r"(?m)^perf:[ \t]*\S", text[text.index("perf:"):]):
        return None, ["perf: must be a block (perf: on its own line, keys indented two spaces)"]
    block, problems, nested = {}, [], None
    for line in match.group(1).splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        sub = re.fullmatch(r" {4}(\w+):(.*)", line)
        top = re.fullmatch(r" {2}(\w+):(.*)", line)
        if sub and nested is not None:
            nested[sub.group(1)] = _scalar(sub.group(2))
        elif top:
            key, value = top.group(1), top.group(2)
            if key in block:
                problems.append(f"perf.{key} is given twice")
            if key == "effort_exception" and not value.split(" #")[0].strip():
                nested = block[key] = {}
            else:
                nested, block[key] = None, _scalar(value)
        else:
            problems.append(f"perf: cannot read the line {line.strip()!r}")
    unknown = sorted(set(block) - set(PERF_DEFAULTS))
    if unknown:
        problems.append(f"perf: unknown key(s) {', '.join(unknown)}")
    return {**PERF_DEFAULTS, **{k: v for k, v in block.items() if k in PERF_DEFAULTS}}, problems


def perf_schema_problems(block):
    """What is wrong with a parsed block's values (schema only: never measured values)."""
    import datetime
    problems = []
    mx, tg = block["max_ratio"], block["target_ratio"]
    if not isinstance(mx, float) or not 1.0 <= mx <= PERF_MAX:
        problems.append(f"perf.max_ratio must be a number from 1.0 to {PERF_MAX} (the owner's hard max), is {mx!r}")
    if not isinstance(tg, float) or tg <= 0 or (isinstance(mx, float) and tg > mx):
        problems.append(f"perf.target_ratio must be a positive number no larger than max_ratio, is {tg!r}")
    modes = block["modes"]
    if not isinstance(modes, list) or not modes or any(m not in PERF_MODES for m in modes) or len(set(modes)) != len(modes):
        problems.append(f"perf.modes must be a non-empty list of {', '.join(PERF_MODES)}, is {modes!r}")
    if not isinstance(block["locked"], bool):
        problems.append(f"perf.locked must be true or false, is {block['locked']!r}")
    since = block["locked_since"]
    if block["locked"] is True:
        try:
            datetime.date.fromisoformat(str(since))
        except ValueError:
            problems.append(f"perf.locked_since must be the date the row was locked (YYYY-MM-DD), is {since!r}")
    elif since is not None:
        problems.append("perf.locked_since is set on a row that is not locked")
    exc = block["effort_exception"]
    if exc is not None:
        if not isinstance(exc, dict) or set(exc) != {"reason", "follow_up"} or not all(
                isinstance(exc[k], str) and exc[k].strip() for k in ("reason", "follow_up")):
            problems.append("perf.effort_exception must be null or carry a non-empty reason and follow_up, nothing else")
    return problems


def _quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def render_perf(block):
    """The block in the fixed form parse_perf reads."""
    lines = ["perf:", f"  max_ratio: {block['max_ratio']:.1f}", f"  target_ratio: {block['target_ratio']:.1f}",
             f"  modes: [{', '.join(block['modes'])}]", f"  locked: {'true' if block['locked'] else 'false'}",
             f"  locked_since: {_quote(block['locked_since']) if block['locked_since'] else 'null'}"]
    exc = block.get("effort_exception")
    if exc:
        lines += ["  effort_exception:", f"    reason: {_quote(exc['reason'])}", f"    follow_up: {_quote(exc['follow_up'])}"]
    else:
        lines.append("  effort_exception: null")
    return "\n".join(lines) + "\n"


def with_perf(text, block):
    """The file's text with its perf: block replaced by block (or appended when it has none)."""
    import re
    rendered = render_perf(block)
    match = re.search(r"(?m)^perf:[ \t]*\n(?:(?: {2}.*)?\n)*", text)
    if match:
        rest = text[match.end():]
        return text[:match.start()] + rendered + ("\n" if rest and match.group(0).endswith("\n\n") else "") + rest
    return text + ("" if text.endswith("\n") else "\n") + rendered


def item_files(root=REGISTRY_ROOT, texts=None):
    """id -> (path, text) of every registry item's own file (state, generated and schema files are not items).
    texts: path -> text to read instead of the working tree (a file listing at a git ref)."""
    import glob, re
    if texts is None:
        texts = {p: read_text(p) for p in glob.glob(f"{root}/**/*.yaml", recursive=True)}
    out = {}
    for path in sorted(texts):
        rel = path[len(root):] if path.startswith(root) else path
        if any(part in rel for part in ("/generated/", "/state/", "/schema/")):
            continue
        match = re.search(r"^id:\s*(\S+)", texts[path], re.M)
        if match:
            out.setdefault(match.group(1).strip("'\""), (path, texts[path]))
    return out


def perf_blocks(root=REGISTRY_ROOT, texts=None):
    """id -> (block, path, problems) for every item file that carries a perf: block."""
    out = {}
    for identifier, (path, text) in item_files(root, texts).items():
        block, problems = parse_perf(text)
        if block is not None or problems:
            out[identifier] = (block, path, problems + (perf_schema_problems(block) if block else []))
    return out


def perf_problems(covered, over_2x_rows=(), shape_covers=None, root=REGISTRY_ROOT):
    """The conformance gate's check of the perf: blocks: every item an A/B shape covers has a valid one, no other
    item has one, and no locked item covers a row on the above-2× list (a locked row is within its bound; the
    list only shrinks, so this holds on every daily run). covered: the ids bench/ab/shapes.json covers;
    shape_covers: (set, shape) -> ids, for the list check."""
    blocks = perf_blocks(root)
    problems = []
    for identifier in sorted(covered):
        if identifier not in blocks:
            problems.append(f"{identifier}: an A/B shape covers it (bench/ab/shapes.json), so its file needs a perf: block")
    for identifier, (block, path, found) in sorted(blocks.items()):
        problems += [f"{path}: {p}" for p in found]
        if identifier not in covered:
            problems.append(f"{path}: perf: block on an item no A/B shape covers (bench/ab/shapes.json)")
    for row in over_2x_rows:
        for identifier in (shape_covers or {}).get((row["set"], row["shape"]), []):
            block = blocks.get(identifier, (None,))[0]
            if block and block["locked"] is True and row["mode"] in block["modes"]:
                problems.append(f"{identifier}: locked, but covers {row['set']} / {row['shape']} / {row['range']} / "
                                f"{row['mode']}, which is on the above-2× list in the {row.get('profile', 'aligned')} profile "
                                "(bench/ab/history/over-2x-loki.json)")
    return problems
