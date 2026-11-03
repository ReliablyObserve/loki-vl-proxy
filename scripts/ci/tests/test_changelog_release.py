import unittest
from pathlib import Path

from scripts.ci import changelog_release as cr
from scripts.ci.tests.release_repo import Repo

REPO = Path(__file__).resolve().parents[3]

CHANGELOG = """# Changelog

## [Unreleased]

### Added

- Added one.

### Fixed

- Fixed one, with a second
  line and a list:
  - nested a
  - nested b

  and a paragraph after a blank line.

- Fixed two.

## [1.2.0] - 2026-01-02

### Fixed

- Old.

## [1.0.0] - 2026-01-01

- First.
"""


def keys(entries):
    return [e.key[0] for e in entries]


class ParseTests(unittest.TestCase):
    def test_entries_keep_their_lines_category_and_blank_line_continuations(self):
        entries = cr.unreleased_entries(CHANGELOG)
        self.assertEqual([e.category for e in entries], ["Added", "Fixed", "Fixed"])
        self.assertEqual(len(entries[1].lines), 6)
        self.assertEqual(entries[1].lines[-1], "  and a paragraph after a blank line.")
        self.assertEqual(entries[2].lines, ("- Fixed two.",))

    def test_released_sections_are_not_unreleased(self):
        self.assertEqual(keys(cr.section_entries(CHANGELOG, "1.2.0")), ["- Old."])
        self.assertEqual(cr.unreleased_entries("# Changelog\n"), [])

    def test_the_real_changelog_renders_back_byte_for_byte(self):
        # Parsing a released section and rendering it again must give the same
        # bytes, or moving entries would rewrite history. Releases before 2.0
        # have repeated headings and loose layout and are left out.
        lines = (REPO / "CHANGELOG.md").read_text(encoding="utf-8").split("\n")
        checked = 0
        for name, head, end in cr.split_sections(lines):
            version = cr.version_tuple(name)
            if version is None or version[0] < 2:
                continue
            original = "\n".join(lines[head:end]).rstrip("\n") + "\n\n"
            date = lines[head].split(" - ", 1)[1]
            rendered = cr.render_section(name, date, cr.parse_entries(lines, head + 1, end))
            self.assertEqual(rendered, original, name)
            checked += 1
        self.assertGreater(checked, 50)


class NewEntriesTests(unittest.TestCase):
    def test_the_previous_release_entries_are_not_repeated(self):
        prev = "## [Unreleased]\n\n### Fixed\n\n- A.\n\n- B.\n"
        cur = "## [Unreleased]\n\n### Fixed\n\n- A.\n\n- B.\n\n- C.\n"
        self.assertEqual(keys(cr.new_entries(prev, cur)), ["- C."])

    def test_entries_already_filed_under_a_release_are_not_new(self):
        cur = "## [Unreleased]\n\n### Fixed\n\n- A.\n\n- B.\n\n## [1.0.0] - x\n\n### Fixed\n\n- A.\n"
        self.assertEqual(keys(cr.new_entries("", cur)), ["- B."])

    def test_a_metadata_merge_between_releases_changes_nothing(self):
        prev = "## [Unreleased]\n\n### Fixed\n\n- A.\n"
        cur = "## [Unreleased]\n\n### Fixed\n\n- B.\n\n## [1.0.0] - x\n\n### Fixed\n\n- A.\n"
        self.assertEqual(keys(cr.new_entries(prev, cur)), ["- B."])

    def test_an_identical_entry_written_again_after_the_previous_release_is_new(self):
        # The previous release carried "- Same."; its metadata was merged (the
        # entry sits under 1.0.1) and the same text was written again.
        prev = "## [Unreleased]\n\n- Same.\n\n## [1.0.0] - x\n\n- Same.\n"
        cur = "## [Unreleased]\n\n- Same.\n\n## [1.0.1] - x\n\n- Same.\n\n## [1.0.0] - x\n\n- Same.\n"
        self.assertEqual(keys(cr.new_entries(prev, cur)), ["- Same."])
        # not merged yet: the old copy is still in [Unreleased] and is not new
        cur = "## [Unreleased]\n\n- Same.\n\n## [1.0.0] - x\n\n- Same.\n"
        self.assertEqual(cr.new_entries(prev, cur), [])

    def test_text_equal_to_an_older_released_entry_is_new_when_written_again(self):
        prev = "## [Unreleased]\n\n## [1.0.0] - x\n\n- Same.\n"
        cur = "## [Unreleased]\n\n- Same.\n\n## [1.0.0] - x\n\n- Same.\n"
        self.assertEqual(keys(cr.new_entries(prev, cur)), ["- Same."])

    def test_an_entry_listed_twice_counts_twice(self):
        prev = "## [Unreleased]\n\n- Same.\n"
        cur = "## [Unreleased]\n\n- Same.\n\n- Same.\n"
        self.assertEqual(keys(cr.new_entries(prev, cur)), ["- Same."])


class ApplyReleaseTests(unittest.TestCase):
    def entries(self, *wanted):
        return [e for e in cr.unreleased_entries(CHANGELOG) if e.key[0] in wanted]

    def test_moves_exactly_the_given_entries_and_leaves_the_rest(self):
        result = cr.apply_release(CHANGELOG, "1.3.0", "2026-02-01", self.entries("- Added one.", "- Fixed two."))
        self.assertEqual(keys(cr.unreleased_entries(result)), ["- Fixed one, with a second"])
        self.assertEqual(keys(cr.section_entries(result, "1.3.0")), ["- Added one.", "- Fixed two."])
        self.assertIn("## [1.3.0] - 2026-02-01\n\n### Added\n\n- Added one.\n\n### Fixed\n\n- Fixed two.\n\n## [1.2.0]", result)
        # the heading left without entries went with them
        self.assertEqual(result.split("## [1.3.0]")[0].count("### Added"), 0)

    def test_released_sections_stay_byte_identical(self):
        result = cr.apply_release(CHANGELOG, "1.3.0", "2026-02-01", self.entries("- Added one."))
        self.assertTrue(result.endswith(CHANGELOG[CHANGELOG.index("## [1.2.0]") :]))

    def test_the_section_goes_in_version_order(self):
        text = CHANGELOG.replace("## [1.2.0]", "## [1.4.0] - 2026-03-01\n\n- Four.\n\n## [1.2.0]")
        result = cr.apply_release(text, "1.3.0", "2026-02-01", self.entries("- Added one."))
        order = [n for n, _, _ in cr.split_sections(result.split("\n"))]
        self.assertEqual(order, ["Unreleased", "1.4.0", "1.3.0", "1.2.0", "1.0.0"])
        older = cr.apply_release(CHANGELOG, "0.9.0", "2025-12-01", self.entries("- Added one."))
        self.assertEqual([n for n, _, _ in cr.split_sections(older.split("\n"))][-1], "0.9.0")
        self.assertTrue(older.endswith("\n") and not older.endswith("\n\n\n"))

    def test_an_existing_section_or_no_entries_changes_nothing(self):
        self.assertEqual(cr.apply_release(CHANGELOG, "1.2.0", "x", self.entries("- Added one.")), CHANGELOG)
        self.assertEqual(cr.apply_release(CHANGELOG, "1.3.0", "x", []), CHANGELOG)

    def test_entries_no_longer_in_unreleased_are_still_filed(self):
        gone = cr.parse_entries(["- Elsewhere."], 0, 1)
        result = cr.apply_release(CHANGELOG, "1.3.0", "x", gone)
        self.assertEqual(keys(cr.section_entries(result, "1.3.0")), ["- Elsewhere."])
        self.assertEqual(len(cr.unreleased_entries(result)), 3)

    def test_an_entry_a_release_already_holds_is_not_filed_twice(self):
        # filed under a section added since the previous release (a sweep)
        swept = CHANGELOG.replace("## [1.2.0]", "## [1.2.5] - x\n\n- Swept.\n\n## [1.2.0]")
        entry = cr.parse_entries(["- Swept."], 0, 1)
        self.assertEqual(cr.apply_release(swept, "1.3.0", "x", entry, since=(1, 2, 0)), swept)

    def test_text_equal_to_an_entry_of_an_older_release_is_still_filed(self):
        old = cr.parse_entries(["- Old."], 0, 1)
        text = CHANGELOG.replace("- Fixed two.", "- Old.")
        result = cr.apply_release(text, "1.3.0", "x", old, since=(1, 2, 0))
        self.assertEqual(keys(cr.section_entries(result, "1.3.0")), ["- Old."])
        self.assertEqual(len(cr.unreleased_entries(result)), 2)

    def test_a_reworded_entry_moves_once_with_its_current_text_and_warns(self):
        tagged = cr.parse_entries(["- Fixed two, as the tag had it."], 0, 1)
        text = CHANGELOG.replace("- Fixed two.", "- Fixed two, as the tag has it now.")
        warnings = []
        result = cr.apply_release(text, "1.3.0", "x", tagged, warnings=warnings)
        self.assertEqual(keys(cr.section_entries(result, "1.3.0")), ["- Fixed two, as the tag has it now."])
        self.assertNotIn("Fixed two", "\n".join(e.key[0] for e in cr.unreleased_entries(result)))
        self.assertEqual(len(warnings), 1)
        self.assertIn("reworded or extended", warnings[0])

    def test_a_sub_bullet_added_after_the_tag_does_not_file_the_entry_twice(self):
        tagged = cr.parse_entries(["- Fixed two."], 0, 1)
        text = CHANGELOG.replace("- Fixed two.", "- Fixed two.\n  - and a detail added later")
        warnings = []
        result = cr.apply_release(text, "1.3.0", "x", tagged, warnings=warnings)
        self.assertEqual(cr.section_entries(result, "1.3.0")[0].lines, ("- Fixed two.", "  - and a detail added later"))
        self.assertEqual(sum(1 for e in cr.unreleased_entries(result) if e.key[0] == "- Fixed two."), 0)
        self.assertEqual(len(warnings), 1)

    def test_an_entry_found_nowhere_is_filed_from_the_tag_text_and_reported(self):
        gone = cr.parse_entries(["- Something entirely different and long enough to share nothing."], 0, 1)
        warnings = []
        result = cr.apply_release(CHANGELOG, "1.3.0", "x", gone, warnings=warnings)
        self.assertEqual(len(cr.unreleased_entries(result)), 3)
        self.assertIn("not in [Unreleased] any more", warnings[0])

    def test_a_different_new_entry_is_not_taken_for_the_tagged_one(self):
        tagged = cr.parse_entries(["- Added one."], 0, 1)
        result = cr.apply_release(CHANGELOG, "1.3.0", "x", tagged)
        self.assertEqual(sorted(e.key[0] for e in cr.unreleased_entries(result)), ["- Fixed one, with a second", "- Fixed two."])

    def test_emptying_unreleased_leaves_the_usual_empty_section(self):
        result = cr.apply_release(CHANGELOG, "1.3.0", "x", cr.unreleased_entries(CHANGELOG))
        self.assertIn("## [Unreleased]\n\n## [1.3.0] - x\n", result)


class GitTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repo()
        self.addCleanup(self.repo.close)

    def test_a_tag_files_only_what_it_shipped_and_every_missing_tag_is_filed_in_order(self):
        r = self.repo
        r.commit("fix: a (#1)", files={"internal/proxy/a.go": "a"}, entry="- A.")
        r.tag("v2.62.1")
        r.commit("fix: b (#2)", files={"internal/proxy/b.go": "b"}, entry="- B.")
        r.tag("v2.62.2")
        r.commit("fix: c (#3)", files={"internal/proxy/c.go": "c"}, entry="- C.")
        text = cr.apply_missing(r.read("CHANGELOG.md"), r.path, "2.62.2")
        self.assertEqual([n for n, _, _ in cr.split_sections(text.split("\n"))], ["Unreleased", "2.62.2", "2.62.1", "2.62.0"])
        self.assertEqual(keys(cr.section_entries(text, "2.62.1")), ["- A."])
        self.assertEqual(keys(cr.section_entries(text, "2.62.2")), ["- B."])
        self.assertEqual(keys(cr.unreleased_entries(text)), ["- C."])
        self.assertIn("## [2.62.1] - 2026-11-03", text)

    def test_the_move_is_the_same_when_applied_twice(self):
        r = self.repo
        r.commit("fix: a (#1)", files={"internal/proxy/a.go": "a"}, entry="- A.")
        r.tag("v2.62.1")
        once = cr.apply_missing(r.read("CHANGELOG.md"), r.path, "2.62.1")
        self.assertEqual(cr.apply_missing(once, r.path, "2.62.1"), once)

    def test_a_tag_older_than_the_newest_section_is_not_filed_again(self):
        r = self.repo
        r.commit("fix: a (#1)", files={"internal/proxy/a.go": "a"}, entry="- A.")
        r.tag("v2.62.1")
        r.commit("fix: b (#2)", files={"internal/proxy/b.go": "b"}, entry="- B.")
        r.tag("v2.62.2")
        text = cr.apply_missing(r.read("CHANGELOG.md"), r.path, "2.62.2")
        # a branch made for the older tag, applied on top, finds nothing to do
        self.assertEqual(cr.apply_missing(text, r.path, "2.62.1"), text)

    def test_notes_read_the_commit_not_the_branch_tip(self):
        r = self.repo
        a = r.commit("fix: a (#1)", files={"internal/proxy/a.go": "a"}, entry="- A.")
        r.commit("fix: b (#2)", files={"internal/proxy/b.go": "b"}, entry="- B.")
        self.assertEqual(keys(cr.release_entries(r.path, a, "v2.62.0")), ["- A."])
        self.assertEqual(keys(cr.release_entries(r.path, "HEAD", "v2.62.0")), ["- A.", "- B."])


if __name__ == "__main__":
    unittest.main()
