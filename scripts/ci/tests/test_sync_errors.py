"""Unit tests for conformance/scripts/sync_errors.py: every error writer is found and classified."""
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "conformance" / "scripts"))
import sync_errors  # noqa: E402

SOURCE = '''package proxy

const lokiErrInternal = "internal server error"

func (p *Proxy) writeError(w http.ResponseWriter, code int, msg string) {
	p.writeErrorCause(w, code, msg, nil)
}

func (p *Proxy) writeErr(w http.ResponseWriter, ctx context.Context, err error) int {
	return 0
}

func (p *Proxy) handle(w http.ResponseWriter, r *http.Request, err error) {
	p.writeError(w, http.StatusBadRequest, "invalid query")
	p.writeError(w, http.StatusInternalServerError, lokiErrInternal)
	p.writeError(w, http.StatusBadRequest, "parse error: "+err.Error())
	p.writeErrorShaped(w, status, fmt.Sprintf("limit %d", n), err, writeLokiHTTPError)
	p.writeErr(w, r.Context(), err)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	writeBodylessError(w, http.StatusMethodNotAllowed)
}
'''


def run(source):
    root = tempfile.mkdtemp()
    os.makedirs(os.path.join(root, "internal/proxy"))
    Path(root, "internal/proxy/a.go").write_text(source)
    Path(root, "internal/proxy/a_test.go").write_text('package proxy\nfunc t() { p.writeError(w, 400, "x") }\n')
    cwd = os.getcwd()
    try:
        os.chdir(root)
        with redirect_stdout(io.StringIO()):
            sync_errors.main()
        return json.loads(Path(root, "conformance/registry/generated/proxy/errors.json").read_text())
    finally:
        os.chdir(cwd)


class SyncErrorsTest(unittest.TestCase):
    def test_every_writer_is_found_and_classified(self):
        doc = run(SOURCE)
        kinds = [(e["writer"], e["status"], e["kind"], e["error_text"]) for e in doc["entries"]]
        self.assertEqual(kinds, [
            ("writeError", 400, "literal", False),
            ("writeError", 500, "literal", False),  # a string constant is text the proxy wrote
            ("writeError", 400, "dynamic", True),
            ("writeErrorShaped", None, "dynamic", False),
            ("writeErr", None, "layer", False),
            ("http.Error", 405, "literal", False),
            ("writeBodylessError", 405, "no_body", False),
        ])
        self.assertEqual(doc["entries"][1]["message"], "internal server error")

    def test_a_writer_passing_its_parameters_on_and_test_files_are_not_sites(self):
        doc = run(SOURCE)
        self.assertFalse(any(e["writer"] == "writeErrorCause" for e in doc["entries"]))
        self.assertFalse(any("a_test.go" in e["where"] for e in doc["entries"]))

    def test_the_summary_counts_each_kind(self):
        summary = run(SOURCE)["summary"]["by_status"]
        self.assertEqual((summary["400"]["literal_sites"], summary["400"]["dynamic_sites"],
                          summary["400"]["error_text_sites"]), (1, 1, 1))
        self.assertEqual((summary["derived"]["layer_sites"], summary["derived"]["dynamic_sites"]), (1, 1))
        self.assertEqual(summary["405"]["no_body_sites"], 1)


if __name__ == "__main__":
    unittest.main()
