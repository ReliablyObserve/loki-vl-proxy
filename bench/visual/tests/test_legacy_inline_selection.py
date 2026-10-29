"""Exercise the write-token workflow's legacy base-renderer image allowlist."""

import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/visual-smoke.yaml"
PREFIX = "https://raw.githubusercontent.com/ReliablyObserve/loki-vl-proxy/pr-visuals/pr-737/"


def selector_script():
    workflow = WORKFLOW.read_text(encoding="utf-8")
    after = workflow.split("python3 - <<'PY'\n", 1)[1]
    return textwrap.dedent(after.split("\n          PY", 1)[0])


class LegacyInlineSelectionTests(unittest.TestCase):
    def run_selector(self, markdown, files):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            montage = root / "montage"
            montage.mkdir()
            for name, value in files.items():
                path = montage / name
                if isinstance(value, tuple):
                    path.symlink_to(value[1])
                else:
                    path.write_bytes(value)
            preview = root / "preview.md"
            preview.write_text(markdown, encoding="utf-8")
            staged = root / "staged"
            env = {**os.environ, "REPO": "ReliablyObserve/loki-vl-proxy", "PR": "737",
                   "VISUAL_PREVIEW": str(preview), "VISUAL_MONTAGE_DIR": str(montage),
                   "VISUAL_STAGED_DIR": str(staged)}
            result = subprocess.run([sys.executable, "-c", selector_script()], env=env,
                                    capture_output=True, text=True)
            names = sorted(path.name for path in staged.iterdir()) if staged.exists() else []
            return result, names

    def test_only_trusted_rendered_inline_images_are_staged(self):
        md = (f"![one]({PREFIX}explore-absent-over-time-1h.png?v=1234abcd)\n"
              f"![two]({PREFIX}explore-absent-over-time-6h.png?v=1234abcd)\n")
        result, names = self.run_selector(md, {"explore-absent-over-time-1h.png": b"PNG",
                                               "explore-absent-over-time-6h.png": b"PNG",
                                               "unreferenced-1h.png": b"PNG"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(names, ["explore-absent-over-time-1h.png", "explore-absent-over-time-6h.png"])

    def test_foreign_urls_and_traversal_are_not_selected(self):
        md = (f"![bad]({PREFIX}../other.png?v=1234abcd)\n"
              "![foreign](https://raw.githubusercontent.com/evil/repo/pr-visuals/pr-737/a.png?v=1234abcd)\n")
        result, names = self.run_selector(md, {"a.png": b"PNG"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(names, [])

    def test_symlink_and_oversized_images_are_rejected(self):
        md = f"![one]({PREFIX}case-1h.png?v=1234abcd)\n"
        for value in (("link", "/etc/hosts"), b"x" * 300_001):
            with self.subTest(value=type(value)):
                result, names = self.run_selector(md, {"case-1h.png": value})
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(names, [])


if __name__ == "__main__":
    unittest.main()
