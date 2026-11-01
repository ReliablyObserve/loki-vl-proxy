"""Every top-level compatibility test needs an executable CI selector or reason.

This checks reachability, not a passing test run. It follows the existing main
shards, pinned compatibility workflows, their runtime manifest and the security
helper. A copied selector in a comment is not evidence of a runnable test.
"""

import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TEST_FUNCTION = re.compile(r"^func (Test\w+)\(\s*\w+\s+\*testing\.T\s*\)", re.MULTILINE)
QUOTED_RUN = re.compile(r"-run\s+(['\"])(\^\(?Test[^'\"\n]+)\1")

# Exact names only: adding a new test with one of these prefixes must still
# choose a shard. Existing local helper checks are classified separately;
# their exemption is not evidence that their assertions ran in CI.
EXEMPTIONS = {
    "TestErrorLayerE2EClasses_TransportInJSONEnvelope": "local error-classifier helper",
    "TestErrorLayerE2EClasses_LeakInMessageField": "local error-classifier helper",
    "TestComposeProxiesKeepTheLokiDefaultMetadataLookback": "local compose manifest guard",
    "TestDenseLinearTimestamp_MonotonicAndBounded": "manual dense-pattern fixture helper",
    "TestDenseExpectedBuckets_Boundaries": "manual dense-pattern fixture helper",
    "TestRecorderReadTransportRetriesTruncatedSuccessfulPOST": "local recorder transport helper",
    "TestRecorderReadTransportDoesNotRetryBackendError": "local recorder transport helper",
    "TestRecorderReadTransportDoesNotRetryTruncatedBackendError": "local recorder transport helper",
    "TestE2ELock_ProxyHeapBoundedUnderDrilldownLoad": "manual high-cardinality process-memory load lock",
    "TestPatternsDenseRepro_FullRangeAndRefreshStability": "explicit E2E_PATTERNS_DENSE_REPRO opt-in",
    "TestPerf_Labels": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_LabelValues": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_QueryRange_Simple": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_QueryRange_WithFilter": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_QueryRange_JSONParse": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_Series": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_Ready": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_BuildInfo": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_CacheEffect": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestPerf_Summary": "manual latency harness; performance is measured by perf-smoke/A-B",
    "TestParityLatency": "manual A-B benchmark; uses separate output and timing budgets",
}


def executable_text(text):
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def selectors(root=ROOT):
    workflows = root / ".github/workflows"
    ci = executable_text((workflows / "ci.yaml").read_text())
    # Restrict pattern fields to the actual compatibility matrix job.
    group = ci.split("  e2e-compat-group:", 1)[1].split("    name: e2e-compat", 1)[0]
    patterns = re.findall(r"^\s+pattern: '([^']+)'", group, re.MULTILINE)
    for name in ("compat-loki.yaml", "compat-vl.yaml", "compat-drilldown.yaml"):
        text = executable_text((workflows / name).read_text())
        patterns.extend(match[1] for match in QUOTED_RUN.findall(text))
    security = executable_text((workflows / "security-pr.yaml").read_text())
    if "run: ./scripts/ci/run_security_regressions.sh" in security:
        helper = executable_text((root / "scripts/ci/run_security_regressions.sh").read_text())
        for command in helper.replace("\\\n", " ").splitlines():
            if "test/e2e-compat" in command and "-tags=e2e" in command:
                patterns.extend(match[1] for match in QUOTED_RUN.findall(command))
    drilldown = executable_text((workflows / "compat-drilldown.yaml").read_text())
    if '-run "$TEST_REGEX"' in drilldown and "compatibility-matrix.json" in drilldown:
        manifest = json.loads((root / "test/e2e-compat/compatibility-matrix.json").read_text())

        def visit(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key == "test_regex":
                        patterns.append(item)
                    else:
                        visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        visit(manifest)
    return [re.compile(pattern) for pattern in patterns]


def top_level_tests(root=ROOT):
    tests = {}
    for path in sorted((root / "test/e2e-compat").glob("*_test.go")):
        for name in TEST_FUNCTION.findall(path.read_text()):
            if name in tests:
                raise AssertionError(f"duplicate test {name}")
            tests[name] = str(path.relative_to(root))
    return tests


def unclassified(tests, patterns, exemptions):
    return sorted(name for name in tests if name not in exemptions and not any(p.search(name) for p in patterns))


class E2ESelectionTests(unittest.TestCase):
    def test_every_test_is_selected_or_explicitly_classified(self):
        tests = top_level_tests()
        patterns = selectors()
        self.assertGreater(len(tests), 400)
        self.assertGreater(len(patterns), 8)
        self.assertFalse(set(EXEMPTIONS) - set(tests), "remove stale exemptions")
        self.assertEqual([], unclassified(tests, patterns, EXEMPTIONS))

    def test_new_tests_cannot_inherit_manual_prefix_exemption(self):
        self.assertEqual(["TestPerf_NewCorrectnessCase"], unclassified(
            {"TestPerf_NewCorrectnessCase": "new.go"}, [], EXEMPTIONS))

    def test_discovery_does_not_require_the_parameter_to_be_named_t(self):
        self.assertEqual(["TestNewCase"], TEST_FUNCTION.findall(
            "func TestNewCase(test *testing.T) {\n}"))

    def test_security_runtime_and_pinned_contracts_are_followed(self):
        patterns = selectors()
        for name in ("TestHardeningLive_BinaryMatchingValues", "TestLabelCache_FilteredValuesTenantIsolation"):
            self.assertTrue(any(pattern.search(name) for pattern in patterns), name)


if __name__ == "__main__":
    unittest.main()
