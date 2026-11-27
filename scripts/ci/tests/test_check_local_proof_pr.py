import contextlib
import io
import os
import unittest
from unittest import mock

from scripts.ci import check_local_proof_pr as gate

TEMPLATE = gate.TEMPLATE.read_text(encoding="utf-8")


def body(proof: str) -> str:
    return f"## Summary\n\n- a change\n\n## Local proof (Required)\n\n{proof}\n\n## Docs (Required)\n\n- none needed\n"


class ProblemTest(unittest.TestCase):
    def test_template_has_the_section(self):
        self.assertTrue(gate.content_lines(gate.section(TEMPLATE) or ""))

    def test_untouched_template_fails(self):
        self.assertIn("template text", gate.problem(TEMPLATE, TEMPLATE))

    def test_untouched_template_with_windows_line_ends_fails(self):
        self.assertIsNotNone(gate.problem(TEMPLATE.replace("\n", "\r\n"), TEMPLATE))

    def test_missing_section_fails(self):
        self.assertIn("no", gate.problem("## Summary\n\n- a change\n", TEMPLATE))
        self.assertIsNotNone(gate.problem("", TEMPLATE))

    def test_empty_values_fail(self):
        for proof in ("", "- ", "TODO", "- n/a", "- [ ] TBD", "@@VISUAL@@", "<!-- later -->",
                      "<!-- never closed", "### Metrics", "```\n```"):
            with self.subTest(proof=proof):
                self.assertIsNotNone(gate.problem(body(proof), TEMPLATE))

    def test_other_sections_do_not_count(self):
        text = "## Local proof\n\n## Validation\n\n- 12 tests pass\n"
        self.assertIsNotNone(gate.problem(text, TEMPLATE))

    def test_heading_inside_a_fence_is_not_the_section(self):
        text = "## Summary\n\n```\n## Local proof\nrows\n```\n"
        self.assertIsNotNone(gate.problem(text, TEMPLATE))

    def test_written_evidence_passes(self):
        for proof in (
            "- 2026-11-27, head abc1234: 85 cells, main = PR in 85",
            "| cell | main | PR |\n|---|---|---|\n| 1h | 200 | 200 |",
            "![panel](https://example.invalid/panel.png)",
            "```\n## output heading\nok\n```",
            "### Metrics\n\n1 series before, 2 after",
        ):
            with self.subTest(proof=proof):
                self.assertIsNone(gate.problem(body(proof), TEMPLATE))

    def test_evidence_added_under_the_template_text_passes(self):
        filled = gate.section(TEMPLATE) + "\n- head abc1234: gate ok\n"
        self.assertIsNone(gate.problem(body(filled), TEMPLATE))

    def test_plain_heading_and_subsections_pass(self):
        text = "## Local proof\n\n### Load\n\n240 x 200\n"
        self.assertIsNone(gate.problem(text, TEMPLATE))


class MainTest(unittest.TestCase):
    def run_gate(self, **env):
        with mock.patch.dict(os.environ, env, clear=True), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return gate.main()

    def test_ready_pull_request_without_proof_fails(self):
        self.assertEqual(1, self.run_gate(PR_BODY="## Summary\n", PR_AUTHOR="someone", PR_DRAFT="false"))
        self.assertEqual(1, self.run_gate())

    def test_filled_pull_request_passes(self):
        self.assertEqual(0, self.run_gate(PR_BODY=body("- gate ok"), PR_AUTHOR="someone", PR_DRAFT="false"))

    def test_draft_passes(self):
        self.assertEqual(0, self.run_gate(PR_BODY="", PR_AUTHOR="someone", PR_DRAFT="true"))

    def test_automation_passes(self):
        for author in sorted(gate.AUTOMATION_AUTHORS):
            with self.subTest(author=author):
                self.assertEqual(0, self.run_gate(PR_BODY="", PR_AUTHOR=author, PR_DRAFT="false"))


if __name__ == "__main__":
    unittest.main()
