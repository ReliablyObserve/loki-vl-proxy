"""Replay of 2026-11-03: the pushes to main, which release runs survived, and
the release metadata pull requests, through the release logic.

The pull requests, their labels, sizes and changed paths are the real ones; each
entry is the one that pull request added to [Unreleased]. The repository is
built here commit by commit, so every release is computed from the state main
was in when its run started.
"""

import unittest
from pathlib import Path

from scripts.ci import changelog_release as cr
from scripts.ci import release_plan
from scripts.ci.tests.release_repo import Repo

SHIP = "internal/proxy/{}.go"
LABELS = {
    1016: (("bugfix", "size/S", "scope/proxy"), 4, 43),
    1014: (("maintenance", "size/XL", "scope/ci"), 10, 1094),
    1017: (("bugfix", "size/L", "scope/proxy"), 15, 251),
    1021: (("bugfix", "size/M", "scope/proxy"), 8, 78),
    1022: (("bugfix", "size/M", "scope/proxy"), 4, 88),
    995: (("bugfix", "size/XL", "scope/translator", "ci-full"), 24, 3512),
    996: (("performance", "size/XL", "scope/proxy"), 18, 1477),
    1027: (("maintenance", "size/XL", "scope/ci", "ci-full"), 13, 1169),
}

# (id, pull request, files, entry); entry None = ships nothing / no entry.
EVENTS = [
    ("ecfbe3f9", 1016, {SHIP.format("step_align"): "1"}, "- E1016."),
    ("f4be79c3", 1014, {".github/workflows/ci.yaml": "1", "scripts/ci/shards.py": "1"}, None),
    ("15d91682", None, {"docs/architecture.md": "1"}, None),
    ("a652b3f0", 1017, {SHIP.format("range_metric"): "1"}, "- E1017."),
    ("da58f093", None, {"internal/proxy/fuzz_test.go": "1"}, None),
    ("09a2e591", 1021, {SHIP.format("label_metadata"): "1"}, "- E1021."),
    ("76316a07", 1022, {SHIP.format("step_align"): "2"}, "- E1022."),
    ("295ce528", "metadata", None, None),
    ("34e0c151", 995, {SHIP.format("label_filter"): "1"}, "- E995."),
    ("5a3bf5cb", 996, {SHIP.format("detected_level"): "1"}, "- E996."),
    ("54ba26ae", "metadata", None, None),
    ("39297de2", "metadata", None, None),
    ("17596d4b", 1027, {".github/workflows/visual-smoke.yaml": "1", "bench/visual/shards.py": "1"}, None),
    ("8509ee7f", "metadata", None, None),
]


class Replay:
    def __init__(self, runs):
        self.repo = Repo()
        self.runs = set(runs)
        self.prs = {n: {"labels": [{"name": l} for l in labels], "changed_files": f, "additions": c, "deletions": 0}
                    for n, (labels, f, c) in LABELS.items()}
        self.pending = []
        self.sha = {}
        self.released = []  # (commit id, version)

    def play(self):
        for ident, pr, files, entry in EVENTS:
            if pr == "metadata":
                if not self.pending:
                    continue
                # The pull request for the newest tag files every section main lacks.
                self.repo.merge_metadata(self.pending[-1])
                self.pending = []
            else:
                subject = {1016: "fix", 1017: "fix", 1021: "fix", 1022: "fix", 995: "fix", 996: "perf"}.get(pr, "ci")
                tail = f" (#{pr})" if pr else " (#1019)"
                self.repo.commit(f"{subject}: change{tail}", files=files, entry=entry)
            self.sha[ident] = self.repo.head()
            if ident in self.runs:
                result = release_plan.plan(self.repo.path, self.sha[ident], self.prs.get)
                if result["should_release"] == "true":
                    version = result["next_version"]
                    self.repo.tag(f"v{version}", self.sha[ident])
                    self.released.append((ident, version, result["bump"]))
                    self.pending.append(version)
        return self

    def sections(self):
        text = self.repo.read("CHANGELOG.md")
        lines = text.split("\n")
        return {n: [e.key[0] for e in cr.parse_entries(lines, h + 1, e)] for n, h, e in cr.split_sections(lines)}


class ReplayTests(unittest.TestCase):
    def tearDown(self):
        for replay in self.replays:
            replay.repo.close()

    def setUp(self):
        self.replays = []

    def replay(self, runs):
        replay = Replay(runs).play()
        self.replays.append(replay)
        return replay

    def check_nothing_lost_or_repeated(self, replay):
        sections = replay.sections()
        filed = [e for name, entries in sections.items() if name != "2.62.0" for e in entries]
        self.assertEqual(sorted(filed), sorted(f"- E{n}." for n in (1016, 1017, 1021, 1022, 995, 996)))
        self.assertEqual(sections["Unreleased"], [])
        order = [cr.version_tuple(n) for n in sections if n != "Unreleased"]
        self.assertEqual(order, sorted(order, reverse=True))

    def test_the_runs_that_cut_a_tag_that_day(self):
        # Only the runs that tagged, as on the day; the cancelled ones never ran.
        replay = self.replay({"15d91682", "a652b3f0", "34e0c151", "17596d4b"})
        self.assertEqual(replay.released, [
            ("15d91682", "2.62.1", "patch"),
            ("a652b3f0", "2.62.2", "patch"),
            ("34e0c151", "2.63.0", "minor"),
            # #996 is a performance pull request of size XL in the proxy: the size
            # rule has always promoted such a pull request to minor. On the day it
            # was released by the run for #1027, whose own labels were read instead.
            ("17596d4b", "2.64.0", "minor"),
        ])
        sections = replay.sections()
        self.assertEqual(sections["2.62.1"], ["- E1016."])
        self.assertEqual(sections["2.62.2"], ["- E1017."])
        self.assertEqual(sections["2.63.0"], ["- E1021.", "- E1022.", "- E995."])
        self.assertEqual(sections["2.64.0"], ["- E996."])
        self.check_nothing_lost_or_repeated(replay)

    def test_every_run_that_did_not_fail_or_get_cancelled_that_day(self):
        # The runs that completed, including the ones started by metadata merges
        # (which the old guard turned into no-ops).
        replay = self.replay({"15d91682", "a652b3f0", "295ce528", "34e0c151", "54ba26ae", "39297de2", "17596d4b", "8509ee7f"})
        self.assertEqual(replay.released, [
            ("15d91682", "2.62.1", "patch"),
            ("a652b3f0", "2.62.2", "patch"),
            ("295ce528", "2.62.3", "patch"),
            ("34e0c151", "2.63.0", "minor"),
            ("54ba26ae", "2.64.0", "minor"),
        ])
        sections = replay.sections()
        self.assertEqual(sections["2.62.3"], ["- E1021.", "- E1022."])
        self.assertEqual(sections["2.63.0"], ["- E995."])
        self.assertEqual(sections["2.64.0"], ["- E996."])
        self.check_nothing_lost_or_repeated(replay)

    def test_a_single_surviving_run_at_the_end_releases_everything_once(self):
        # Every run but the last was cancelled, and the last was started by a
        # docs push: it still releases all six pull requests, as one version.
        replay = self.replay(set())
        replay.repo.commit("docs: later (#1040)", files={"docs/x.md": "x"})
        result = release_plan.plan(replay.repo.path, "HEAD", replay.prs.get)
        self.assertEqual((result["should_release"], result["next_version"]), ("true", "2.63.0"))
        entries = cr.release_entries(replay.repo.path, "HEAD", "v2.62.0")
        self.assertEqual(sorted(e.key[0] for e in entries), sorted(f"- E{n}." for n in (1016, 1017, 1021, 1022, 995, 996)))


REAL = Path(__file__).resolve().parents[3] / "CHANGELOG.md"


class RealChangelogReplay(unittest.TestCase):
    """The same day with the entries the CHANGELOG really holds for 2.62.1 to 2.64.0.

    Each tag's section is read from the repository's CHANGELOG, handed to the
    pull request it belongs to, and the sections the release logic files must
    be the ones the tags say, line for line.
    """

    # real section -> head of its entry -> pull request (an entry is told by its first words)
    ENTRIES = [
        (1016, "2.62.1", "A metric `query_range` whose bounds fall outside"),
        (1017, "2.62.2", "A line exactly on an evaluation time"),
        (1021, "2.63.0", "The label cache merge dedupes"),
        (1022, "2.63.0", "Step alignment leaves a bound after 2262"),
        (995, "2.63.0", "Accept Loki's label filter expressions"),
        (996, "2.63.1", "Answer `sum by (detected_level)`"),
        (1032, "2.64.0", "List only Loki's fields"),
    ]

    def setUp(self):
        self.text = REAL.read_text(encoding="utf-8")
        self.repo = Repo()
        self.addCleanup(self.repo.close)
        self.labels = dict(LABELS)
        self.labels[1032] = (("bugfix", "size/M", "scope/proxy"), 4, 90)

    def real_entry(self, section, head):
        entries = [e for e in cr.section_entries(self.text, section) if e.lines[0].replace("**", "").startswith("- " + head)]
        self.assertEqual(len(entries), 1, (section, head))
        return entries[0]

    def test_the_sections_come_out_as_the_tags_say(self):
        prs = {n: {"labels": [{"name": l} for l in lab], "changed_files": f, "additions": c, "deletions": 0}
               for n, (lab, f, c) in self.labels.items()}
        repo, pending, released = self.repo, [], []
        runs_at = {1016, 1017, 995, 996, 1032}  # the others' runs were cancelled
        for number, section, head in self.ENTRIES:
            entry = self.real_entry(section, head)
            repo.commit(f"fix: change (#{number})", files={SHIP.format(f"pr{number}"): "x"},
                        entry="\n".join(entry.lines), category=entry.category)
            if number == 1022:  # the metadata pull request of the earlier tags merges here
                repo.merge_metadata(pending[-1])
                pending = []
            if number in runs_at:
                result = release_plan.plan(repo.path, "HEAD", prs.get)
                self.assertEqual(result["should_release"], "true", number)
                repo.tag(f"v{result['next_version']}")
                released.append((number, result["next_version"]))
                pending.append(result["next_version"])
        repo.merge_metadata(pending[-1])
        self.assertEqual(released, [(1016, "2.62.1"), (1017, "2.62.2"), (995, "2.63.0"), (996, "2.64.0"), (1032, "2.64.1")])

        mine = repo.read("CHANGELOG.md")
        lines = mine.split("\n")
        order = [n for n, _, _ in cr.split_sections(lines)]
        self.assertEqual(order, ["Unreleased", "2.64.1", "2.64.0", "2.63.0", "2.62.2", "2.62.1", "2.62.0"])
        self.assertEqual(cr.unreleased_entries(mine), [])
        # tag by tag, the same entries as the real section; the two that the
        # size rule numbers differently (#996, #1032) are compared under the real number
        for mine_version, real_version in (("2.62.1", "2.62.1"), ("2.62.2", "2.62.2"), ("2.63.0", "2.63.0"),
                                           ("2.64.0", "2.63.1"), ("2.64.1", "2.64.0")):
            got = sorted(e.key for e in cr.section_entries(mine, mine_version))
            want = sorted(e.key for e in cr.section_entries(self.text, real_version))
            self.assertEqual(got, want, mine_version)
        # and the one-entry sections byte for byte
        for mine_version, real_version in (("2.62.1", "2.62.1"), ("2.62.2", "2.62.2"), ("2.64.0", "2.63.1"), ("2.64.1", "2.64.0")):
            def body(text, version):
                ls = text.split("\n")
                h, e = next((h, e) for n, h, e in cr.split_sections(ls) if n == version)
                return "\n".join(ls[h + 1 : e]).rstrip("\n")
            self.assertEqual(body(mine, mine_version), body(self.text, real_version), mine_version)


if __name__ == "__main__":
    unittest.main()
