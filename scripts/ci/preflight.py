#!/usr/bin/env python3
"""Pure helpers of scripts/ci/preflight.sh, the local pre-push CI preflight.

The shell script runs the steps; everything that has to read the repository, the
workflows or a diff lives here so it can be tested (scripts/ci/tests/test_preflight.py)
and so the commands CI runs are read from the workflows instead of copied.

Commands:

  plan --base REF [--dependents]   what the diff against REF touches, as key=value lines:
                                   all, packages, proxy, proxy_run, proxy_unmatched, bench,
                                   compose, cache
  snapshot DEST                    copy the working tree (tracked and untracked, not ignored) to DEST
  unittests                        the `python3 -m unittest ...` argument lines ci.yaml runs
  run-unittest [--parallel N] LINE run one such line as CI does (one interpreter); with --parallel,
                                   one process per module, N at a time (a failure may then hide an
                                   import-order problem that only the single process shows)
  golangci-version                 the golangci-lint version ci.yaml pins
  regression-run                   the -run pattern of ci.yaml's concurrent-regression race step
  fuzz-targets --packages P...     tab separated package, target, fuzztime, timeout of the smoke
                                   rows in those packages (no packages: every row)
  summarize-json FILE              the failing tests of a `go test -json` file, with their output
"""
import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
PROXY = "internal/proxy"
# Files that are not Go but are read by internal/querycorpus's tests (the query corpus, the plan and query-text
# ratchets): a change to one runs that package's tests, which a registry-only change would otherwise skip.
QUERYCORPUS = "internal/querycorpus"
QUERYCORPUS_INPUTS = ("conformance/registry/", "bench/ab/shapes.json", "test/e2e-compat/query-semantics-matrix.json")
CI = ROOT / ".github" / "workflows" / "ci.yaml"

TEST_FUNC = re.compile(r"^func ((?:Test|Fuzz)\w*)\(\s*\w+\s+\*testing\.[TF]\s*\)", re.M)
UNITTEST_LINE = re.compile(r"^\s*run:\s*python3?\s+-m\s+unittest\s+(.+?)\s*$", re.M)
SKIP_PARTS = {".git", "node_modules", "vendor"}


# --- the diff -------------------------------------------------------------------------------


def _git(*args, root=ROOT):
    return subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=True).stdout


def changed_files(base, root=ROOT):
    """Every path the pull request would carry plus the working tree's own changes (deleted ones included)."""
    paths = set(_git("diff", "--name-only", "--no-renames", f"{base}...HEAD", root=root).splitlines())
    paths |= set(_git("diff", "--name-only", "--no-renames", "HEAD", root=root).splitlines())
    paths |= set(_git("ls-files", "--others", "--exclude-standard", root=root).splitlines())
    return sorted(p for p in paths if p)


def _module_root(path, root):
    """The directory of the go.mod that owns `path` (root for the main module)."""
    d = (root / path).parent
    while d != root and root in d.parents:
        if (d / "go.mod").exists():
            return d
        d = d.parent
    return root


def package_dir(path, root):
    """The directory of the main-module package a changed path belongs to, or None.

    A Go file, a testdata file or an embedded file belongs to the nearest directory at or above it that
    holds Go files; a path in a nested module (bench/, test/oracle/*) or in no package belongs to none."""
    if _module_root(path, root) != root:
        return None
    d = (root / path).parent
    while root in d.parents:
        if d.is_dir() and any(d.glob("*.go")):
            return d.relative_to(root).as_posix()
        d = d.parent
    return None


def test_names(path, root):
    """The Test and Fuzz functions of a test file, in file order."""
    try:
        return TEST_FUNC.findall((root / path).read_text(errors="replace"))
    except OSError:
        return []


def proxy_selection(files, root):
    """(tests, unmatched): the internal/proxy tests that answer for the changed files.

    A changed test file contributes its own tests. A changed source file `foo.go` contributes the tests of
    `foo_test.go` and `foo_*_test.go`. A changed file in the package with no such test (or a testdata file)
    is returned in `unmatched`, because nothing says which tests cover it."""
    tests, unmatched = [], []
    for f in files:
        directory, _, name = f.rpartition("/")
        if directory != PROXY:
            if f.startswith(PROXY + "/") and package_dir(f, root) == PROXY:
                unmatched.append(f)  # testdata and the like
            continue
        if not name.endswith(".go"):
            unmatched.append(f)
        elif name.endswith("_test.go"):
            tests += test_names(f, root)
        else:
            stem = name[: -len(".go")]
            found = sorted((root / PROXY).glob(f"{stem}_test.go")) + sorted((root / PROXY).glob(f"{stem}_*_test.go"))
            if not found:
                unmatched.append(f)
            for test_file in found:
                tests += test_names(test_file.relative_to(root).as_posix(), root)
    return sorted(set(tests)), sorted(unmatched)


def run_pattern(names):
    return "^(" + "|".join(names) + ")$" if names else ""


def reverse_closure(imports, seeds):
    """seeds plus every package that imports one of them, directly or not. imports: {pkg: {imported pkgs}}."""
    wanted = set(seeds)
    grew = True
    while grew:
        grew = False
        for pkg, deps in imports.items():
            if pkg not in wanted and deps & wanted:
                wanted.add(pkg)
                grew = True
    return wanted


def go_imports(root):
    """{package dir: {imported package dirs}} of the main module, test imports included."""
    module = subprocess.run(["go", "list", "-m"], cwd=root, capture_output=True, text=True, check=True).stdout.strip()
    fmt = "{{.ImportPath}}|{{join .Imports \",\"}}|{{join .TestImports \",\"}}|{{join .XTestImports \",\"}}"
    out = subprocess.run(["go", "list", "-f", fmt, "./..."], cwd=root, capture_output=True, text=True, check=True).stdout
    imports = {}
    for line in out.splitlines():
        path, *lists = line.split("|")
        deps = {d for part in lists for d in part.split(",") if d.startswith(module)}
        imports[path.removeprefix(module).lstrip("/") or "."] = {d.removeprefix(module).lstrip("/") or "." for d in deps}
    return imports


def plan(base, dependents=False, root=ROOT, files=None):
    files = changed_files(base, root) if files is None else files
    result = {"all": "0", "bench": "0", "compose": "0", "cache": "0"}
    pkgs = set()
    for f in files:
        name = f.rsplit("/", 1)[-1]
        if f in ("go.mod", "go.sum"):
            result["all"] = "1"
        if f.startswith("bench/") and not f.endswith(".md"):
            result["bench"] = "1"
        if re.match(r"(docker-)?compose[^/]*\.ya?ml$", name) or name.startswith("docker-compose"):
            result["compose"] = "1"
        pkg = package_dir(f, root) if not f.startswith(".git/") else None
        if pkg:
            pkgs.add(pkg)
        if f.startswith(QUERYCORPUS_INPUTS[0]) or f in QUERYCORPUS_INPUTS[1:]:
            pkgs.add(QUERYCORPUS)
    if dependents and pkgs and result["all"] == "0":
        pkgs = reverse_closure(go_imports(root), pkgs)
    # A deleted package leaves nothing to test.
    pkgs = {p for p in pkgs if (root / p).is_dir()}
    tests, unmatched = proxy_selection(files, root)
    if "internal/cache" in pkgs:
        result["cache"] = "1"
    result["packages"] = " ".join("./" + p for p in sorted(pkgs))
    result["proxy"] = "1" if PROXY in pkgs else "0"
    result["proxy_run"] = run_pattern(tests)
    result["proxy_unmatched"] = " ".join(unmatched)
    return result


# --- the workflows --------------------------------------------------------------------------


def workflow_unittests(text):
    """The argument lines of every `python3 -m unittest ...` run step, in order, without repeats."""
    seen, out = set(), []
    for args in UNITTEST_LINE.findall(text):
        if args not in seen:
            seen.add(args)
            out.append(args)
    return out


def _step_block(text, name):
    """The lines of the step whose `name:` (or `uses:` for an unnamed one) mentions `name`."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if re.match(rf"^\s*- (name|uses):\s*.*{re.escape(name)}", line):
            indent = len(line) - len(line.lstrip())
            block = [line]
            for nxt in lines[i + 1:]:
                if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= indent:
                    break
                block.append(nxt)
            return "\n".join(block)
    return ""


def golangci_version(text):
    match = re.search(r"^\s*version:\s*\"?(v\d+\.\d+\.\d+)\"?", _step_block(text, "golangci-lint"), re.M)
    return match.group(1) if match else ""


def regression_run(text):
    """The -run pattern of the concurrent-regression step of the race job (`go test -race ... -run '<this>'`)."""
    match = re.search(r"-run\s+'([^']+)'", _step_block(text, "Concurrent regression tests"))
    return match.group(1) if match else ""


def unittest_commands(line, parallel=False):
    """The argv lists that run one ci.yaml `python3 -m unittest <line>`: the line itself, or with `parallel`
    one command per module of a plain module list (a `discover ...` line stays whole)."""
    args = shlex.split(line)
    if parallel and args and args[0] != "discover":
        return [[sys.executable, "-m", "unittest", module] for module in args]
    return [[sys.executable, "-m", "unittest", *args]]


def run_unittest(line, parallel=0, root=ROOT):
    """Run the commands of unittest_commands; returns 0 when every one passed. Output is printed per command,
    in order, once it finishes, so parallel runs do not interleave."""
    commands = unittest_commands(line, parallel > 0)
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")

    def one(argv):
        return subprocess.run(argv, cwd=root, env=env, capture_output=True, text=True)

    if parallel > 0 and len(commands) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            results = list(pool.map(one, commands))
    else:
        results = [one(argv) for argv in commands]
    status = 0
    for argv, result in zip(commands, results):
        print(f"$ {' '.join(argv[1:])}")
        sys.stdout.write(result.stdout + result.stderr)
        status = status or result.returncode
    return status


# --- fuzz targets ---------------------------------------------------------------------------


def fuzz_targets(packages):
    import fuzz_smoke
    rows = [t for t in fuzz_smoke.load_registry() if t.status == "smoke"]
    return [t for t in rows if not packages or t.package in packages]


# --- go test -json --------------------------------------------------------------------------


def failing_tests(lines):
    """{(package, test or ''): [output lines]} of the tests (and packages) that failed in `go test -json` events."""
    events, failed = [], set()
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            continue
        events.append(event)
        if event.get("Action") == "fail":
            failed.add((event.get("Package", ""), event.get("Test", "")))
    out = {key: [] for key in sorted(failed)}
    for event in events:
        key = (event.get("Package", ""), event.get("Test", ""))
        if event.get("Action") == "output" and key in out:
            out[key].append(event.get("Output", "").rstrip("\n"))
    return out


def snapshot(dest, root=ROOT):
    """Copy every tracked and untracked (not ignored) file that still exists, so a gate that rewrites its
    reports can run on the copy and leave the checkout alone."""
    listed = _git("ls-files", "-z", "--cached", "--others", "--exclude-standard", root=root).split("\0")
    dest = Path(dest)
    count = 0
    for rel in filter(None, sorted(set(listed))):
        src = root / rel
        if src.is_symlink():
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.is_symlink():
                os.symlink(os.readlink(src), target)
        elif src.is_file():
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
        else:
            continue
        count += 1
    return count


# --- command line ---------------------------------------------------------------------------


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--base", required=True)
    p.add_argument("--dependents", action="store_true")
    sub.add_parser("snapshot").add_argument("dest")
    sub.add_parser("unittests")
    u = sub.add_parser("run-unittest")
    u.add_argument("--parallel", type=int, default=0)
    u.add_argument("line")
    sub.add_parser("golangci-version")
    sub.add_parser("regression-run")
    f = sub.add_parser("fuzz-targets")
    f.add_argument("--packages", nargs="*", default=[])
    sub.add_parser("summarize-json").add_argument("file")
    args = parser.parse_args(argv)

    if args.cmd == "plan":
        for key, value in plan(args.base, args.dependents).items():
            print(f"{key}={value}")
    elif args.cmd == "snapshot":
        print(f"preflight: copied {snapshot(args.dest)} files to {args.dest}")
    elif args.cmd == "unittests":
        print("\n".join(workflow_unittests(CI.read_text())))
    elif args.cmd == "run-unittest":
        sys.exit(run_unittest(args.line, args.parallel))
    elif args.cmd == "golangci-version":
        version = golangci_version(CI.read_text())
        if not version:
            sys.exit("preflight: no golangci-lint version found in .github/workflows/ci.yaml")
        print(version)
    elif args.cmd == "regression-run":
        print(regression_run(CI.read_text()))
    elif args.cmd == "fuzz-targets":
        for t in fuzz_targets(set(args.packages)):
            print(f"{t.package}\t{t.name}\t{t.fuzztime}\t{t.timeout}")
    elif args.cmd == "summarize-json":
        failed = failing_tests(Path(args.file).read_text(errors="replace").splitlines())
        for (package, test), output in failed.items():
            print(f"--- FAIL {package} {test or '(package)'}")
            print("\n".join(output[-60:]))
        sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
