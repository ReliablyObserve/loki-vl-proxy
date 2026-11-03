#!/usr/bin/env python3
"""Shard, check and run the CI fuzz smoke targets.

The targets live in scripts/ci/fuzz_smoke_targets.tsv. `check` proves every Fuzz
function in the tree is accounted for and assigned to exactly one shard. `run`
fuzzes one shard's targets.

On pull requests `run --allow-skip` may skip a target whose inputs are unchanged
since a green run. The decision is conservative: a target is skipped only when its
key (see target_key) is in the ledger of an earlier green run, and any failure to
compute the key means the target runs. The ledger is written only when every target
of the shard passed.
"""
import argparse
import hashlib
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = Path(__file__).with_name("fuzz_smoke_targets.tsv")
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yaml"
SHARD_JOB = "fuzz-smoke-shard"
# The unscheduled list may only shrink: lower this when a target moves to `smoke`.
MAX_UNSCHEDULED = 2
KEY_VERSION = "fuzz-smoke-key-v1"

FUZZ_FUNC = re.compile(r"^func (Fuzz\w*)\(\s*\w+\s+\*testing\.F\s*\)", re.M)
# Code that can read the repository outside its own package directory, or whose
# behaviour depends on the host. A package that shows any of these is keyed on the
# whole tracked tree instead of its dependency directories.
ESCAPES = re.compile(r'"\.\./|\.\./\.\./|runtime\.Caller|os\.Getwd|os\.Executable|exec\.Command')
SKIP_DIRS = {".git", "node_modules", "vendor"}


class Target:
    def __init__(self, status, package, name, fuzztime, timeout, description=""):
        self.status, self.package, self.name = status, package, name
        self.fuzztime, self.timeout, self.description = fuzztime, timeout, description

    @property
    def ident(self):
        return (self.package, self.name)

    def command(self):
        cmd = ["go", "test", f"./{self.package}", "-run", "^$", f"-fuzz=^{self.name}$", f"-fuzztime={self.fuzztime}"]
        if self.timeout != "-":
            cmd.append(f"-timeout={self.timeout}")
        return cmd


def load_registry(path=REGISTRY):
    targets = []
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        cols = line.split("\t")
        if len(cols) != 6 or cols[0] not in ("smoke", "unscheduled"):
            raise ValueError(f"{path}:{number}: want 6 tab-separated columns starting smoke|unscheduled")
        targets.append(Target(*cols))
    return targets


def discover(root=ROOT):
    """Return {(package, FuzzName)} for every Fuzz function in the tree's test files."""
    found = set()
    for base, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for name in files:
            if name.endswith("_test.go"):
                package = Path(base).relative_to(root).as_posix()
                for match in FUZZ_FUNC.finditer(Path(base, name).read_text(errors="replace")):
                    found.add((package, match.group(1)))
    return found


def shard_count(workflow=WORKFLOW):
    """The number of shards the CI matrix runs, read from the workflow."""
    text = Path(workflow).read_text()
    job = re.search(rf"(?m)^  {SHARD_JOB}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", text, re.S)
    matrix = re.search(r"shard:\s*\[([^\]]*)\]", job.group(1)) if job else None
    if not matrix:
        raise ValueError(f"{SHARD_JOB} has no `shard: [...]` matrix in {workflow}")
    shards = [s.strip() for s in matrix.group(1).split(",") if s.strip()]
    if shards != [str(i) for i in range(1, len(shards) + 1)]:
        raise ValueError(f"{SHARD_JOB} matrix must be 1..N, got {shards}")
    return len(shards)


def smoke_targets(targets):
    return [t for t in targets if t.status == "smoke"]


def assign(targets, shards):
    """Shard (1-based) of each smoke target: row order modulo the shard count."""
    return {t.ident: i % shards + 1 for i, t in enumerate(smoke_targets(targets))}


def shard_targets(targets, shard, shards):
    mine = assign(targets, shards)
    return [t for t in smoke_targets(targets) if mine[t.ident] == shard]


def check(targets, tree, shards):
    """Return every violation: dead rows, unaccounted functions, and sharding gaps."""
    problems = []
    seen = {}
    for t in targets:
        if t.ident in seen:
            problems.append(f"{t.package} {t.name}: listed twice")
        seen[t.ident] = t
        if t.ident not in tree:
            problems.append(f"{t.package} {t.name}: dead row, no such Fuzz function in the tree")
        if t.status == "smoke" and not re.fullmatch(r"\d+[smh]", t.fuzztime):
            problems.append(f"{t.package} {t.name}: smoke row needs a fuzztime like 12s, got {t.fuzztime!r}")
    for package, name in sorted(tree - set(seen)):
        problems.append(f"{package} {name}: in the tree but not in {REGISTRY.name}; add it as `smoke` (or, to leave it a seed-corpus test, `unscheduled`)")
    unscheduled = [t for t in targets if t.status == "unscheduled"]
    for t in unscheduled:
        if not t.description.strip() or t.description == "-":
            problems.append(f"{t.package} {t.name}: an unscheduled row needs the reason in its description")
    if len(unscheduled) > MAX_UNSCHEDULED:
        problems.append(f"{len(unscheduled)} unscheduled targets, the list may only shrink (cap {MAX_UNSCHEDULED}): make the new target `smoke`")
    if shards < 1:
        problems.append("shard count must be at least 1")
        return problems
    placed = [t.ident for k in range(1, shards + 1) for t in shard_targets(targets, k, shards)]
    expected = [t.ident for t in smoke_targets(targets)]
    if sorted(placed) != sorted(expected) or len(placed) != len(set(placed)):
        problems.append("every smoke target must be assigned to exactly one shard")
    return problems


# --- skip decision ------------------------------------------------------------------------


def _sha(*parts):
    h = hashlib.sha256()
    for part in parts:
        data = part if isinstance(part, bytes) else str(part).encode()
        h.update(len(data).to_bytes(8, "big"))
        h.update(data)
    return h.hexdigest()


def _tree_digest(directory, root):
    """Content digest of every file below `directory` (relative paths and bytes)."""
    h = hashlib.sha256()
    for base, dirs, files in os.walk(directory):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for name in sorted(files):
            path = Path(base, name)
            h.update(path.relative_to(root).as_posix().encode() + b"\0")
            h.update(path.read_bytes() if not path.is_symlink() else os.readlink(path).encode())
            h.update(b"\0")
    return h.hexdigest()


def _tracked_digest(root):
    files = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True).stdout.split(b"\0")
    h = hashlib.sha256()
    for raw in sorted(f for f in files if f):
        path = Path(root, raw.decode())
        h.update(raw + b"\0")
        h.update(path.read_bytes() if path.exists() else b"<missing>")
        h.update(b"\0")
    return h.hexdigest()


def _go(args, root):
    return subprocess.run(["go", *args], cwd=root, check=True, capture_output=True, text=True).stdout


def package_dirs(package, root):
    """In-module directories of the package, its test files and everything they import.

    Returns None (doubt) when a dependency lives outside both the module and the
    module cache, such as a local `replace`, which go.mod alone does not pin.
    """
    root = Path(root).resolve()
    modcache = Path(_go(["env", "GOMODCACHE"], root).strip()).resolve()
    listed = _go(["list", "-deps", "-test", "-f", "{{if not .Standard}}{{.Dir}}{{end}}", f"./{package}"], root)
    dirs = set()
    for line in listed.split():
        d = Path(line).resolve()
        if d == root or root in d.parents:
            dirs.add(d)
        elif modcache not in d.parents:
            return None
    return sorted(dirs)


def target_key(target, root=ROOT, dirs_for=package_dirs):
    """Content key of a fuzz target's inputs, or None when it cannot be computed.

    Covers the Go version and platform, go.mod and go.sum (so every external
    module), the content of every in-module directory the package's test binary is
    built from (sources, test files, embedded files, the fuzz corpus under
    testdata), this script, and the target's registry row. A package whose code can
    read outside those directories (ESCAPES) is keyed on the whole tracked tree.
    """
    root = Path(root).resolve()
    try:
        dirs = dirs_for(target.package, root)
        if dirs is None:
            return None
        own = (root / target.package).resolve()
        escapes = False
        for d in dirs:
            for path in sorted(d.glob("*.go")):
                if d != own and path.name.endswith("_test.go"):
                    continue  # another package's tests are not part of this binary
                if ESCAPES.search(path.read_text(errors="replace")):
                    escapes = True
        parts = [KEY_VERSION, _go(["env", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED"], root),
                 (root / "go.mod").read_bytes(), (root / "go.sum").read_bytes(),
                 Path(__file__).read_bytes(), target.package, target.name, target.fuzztime, target.timeout]
        parts.append(_tracked_digest(root) if escapes else "|".join(_tree_digest(d, root) for d in dirs))
        return _sha(*parts)
    except (OSError, subprocess.CalledProcessError, UnicodeError, ValueError):
        return None


def should_skip(key, ledger, allow_skip):
    """Skip only when allowed, the key is known, and a green run recorded it."""
    return bool(allow_skip and key and key in ledger)


# --- running ------------------------------------------------------------------------------


def read_ledger(path):
    try:
        return set(Path(path).read_text().split())
    except (OSError, ValueError):
        return set()


def run_shard(targets, shard, shards, allow_skip, ledger_in, ledger_out, root=ROOT, runner=subprocess.call, key_for=target_key):
    ledger = read_ledger(ledger_in) if ledger_in else set()
    mine = shard_targets(targets, shard, shards)
    green, failed, skipped = [], [], 0
    started = time.time()
    for t in mine:
        key = key_for(t, root)
        if should_skip(key, ledger, allow_skip):
            print(f"skip {t.package} {t.name}: inputs unchanged since a green run ({key[:12]})", flush=True)
            skipped += 1
            green.append(key)
            continue
        print(f"::group::Fuzz smoke {t.package} {t.name} ({t.description})", flush=True)
        code = runner(t.command(), cwd=root)
        print("::endgroup::", flush=True)
        if code:
            failed.append(t)
        elif key:
            green.append(key)
    print(f"shard {shard}/{shards}: {len(mine)} targets, {skipped} skipped, {len(failed)} failed, {time.time() - started:.0f}s")
    for t in failed:
        print(f"FAILED {t.package} {t.name}")
    if failed:
        return 1
    if ledger_out:
        Path(ledger_out).parent.mkdir(parents=True, exist_ok=True)
        Path(ledger_out).write_text("\n".join(sorted(set(green))) + "\n")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("check")
    run = sub.add_parser("run")
    run.add_argument("--shard", type=int, required=True)
    run.add_argument("--of", type=int, required=True)
    run.add_argument("--allow-skip", action="store_true")
    run.add_argument("--ledger-in")
    run.add_argument("--ledger-out")
    args = parser.parse_args(argv)
    targets = load_registry()
    if args.cmd == "check":
        problems = check(targets, discover(), shard_count())
        for p in problems:
            print(f"fuzz smoke: {p}", file=sys.stderr)
        if not problems:
            print(f"fuzz smoke: {len(smoke_targets(targets))} targets in {shard_count()} shards, "
                  f"{len(targets) - len(smoke_targets(targets))} unscheduled, every Fuzz function accounted for")
        return 1 if problems else 0
    if not 1 <= args.shard <= args.of:
        parser.error("--shard must be within 1..--of")
    return run_shard(targets, args.shard, args.of, args.allow_skip, args.ledger_in, args.ledger_out)


if __name__ == "__main__":
    sys.exit(main())
