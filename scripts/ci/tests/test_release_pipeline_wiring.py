import re
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOWS = REPO / ".github" / "workflows"


def text(name):
    return (WORKFLOWS / name).read_text(encoding="utf-8")


class AutoReleaseWiring(unittest.TestCase):
    def test_every_push_to_main_runs_the_release_check(self):
        # A docs-only or CHANGELOG-only push must still release earlier shipped
        # changes whose own run was cancelled.
        self.assertNotIn("paths-ignore", text("auto-release.yaml"))

    def test_the_trigger_commit_message_no_longer_decides_anything(self):
        auto = text("auto-release.yaml")
        self.assertNotIn("HEAD_COMMIT_MSG", auto)
        self.assertNotIn("sync release metadata for v[0-9]", auto)

    def test_the_decision_notes_and_metadata_come_from_the_scripts_with_tests(self):
        auto = text("auto-release.yaml")
        self.assertIn("scripts/ci/release_plan.py plan", auto)
        self.assertEqual(auto.count("scripts/ci/changelog_release.py notes"), 2)  # prepare and publish
        self.assertIn("scripts/ci/release_metadata.py prepare", auto)
        self.assertIn("scripts/ci/release_metadata.py refresh", auto)
        self.assertNotIn("materialize_unreleased", auto)
        self.assertNotIn("sync_release_metadata.sh", auto)

    def test_the_release_is_still_validated_tagged_and_published_from_one_commit(self):
        auto = text("auto-release.yaml")
        self.assertEqual(auto.count("ref: ${{ needs.prepare-release.outputs.release_sha }}"), 2)
        self.assertIn("git checkout --detach origin/main", auto)
        for step in ("cosign sign", "attest-build-provenance", "Create release tag", "Scan the pushed image with Trivy"):
            self.assertIn(step, auto)

    def test_the_metadata_branch_is_refreshed_on_every_push(self):
        auto = text("auto-release.yaml")
        job = auto[auto.index("  refresh-metadata-pr:") :]
        self.assertIn("release_metadata.py refresh", job)
        self.assertIn("contents: read", job)

    def test_the_manual_release_uses_the_same_metadata_script(self):
        release = text("release.yaml")
        self.assertIn("scripts/ci/release_metadata.py prepare", release)
        self.assertNotIn("sync_release_metadata.sh", release)

    def test_scripts_the_workflows_call_exist(self):
        for name in ("auto-release.yaml", "release.yaml"):
            for script in set(re.findall(r"scripts/ci/[\w.]+", text(name))):
                self.assertTrue((REPO / script).exists(), f"{name} calls {script}")


class BotBranchWiring(unittest.TestCase):
    def test_the_daily_report_branch_is_only_ever_moved_through_the_api(self):
        # git compares a push over an old tip with that tip: when main edited a
        # workflow file in between, a token without the workflow scope is refused.
        # The commit goes to a new temporary ref by push, and the report branch is
        # then moved once through the API, never pushed and never set to main's tip.
        perf = text("perf-daily.yaml")
        step = perf[perf.index("name: Open or update the daily report PR") :]
        step = step[: step.index("name: Arm auto-merge")]
        self.assertIn('push origin "${TMP_BRANCH}"', step)
        self.assertNotIn('push origin "$BRANCH"', step)
        self.assertNotRegex(step, r"push [^\n]*(--force|-f )")
        self.assertIn("-X PATCH", step)
        self.assertIn("-X DELETE", step)  # a branch that cannot be moved is created again
        self.assertNotIn("BASE=", step)
        self.assertLess(step.index('push origin "${TMP_BRANCH}"'), step.index("-X PATCH"))

    def test_the_metadata_refresh_waits_for_the_release_job_of_its_run(self):
        auto = text("auto-release.yaml")
        job = auto[auto.index("  refresh-metadata-pr:") :]
        self.assertIn("needs: [prepare-release, publish-release]", job)
        self.assertIn("!cancelled()", job)

    def test_release_workflows_never_force_push(self):
        for name in ("auto-release.yaml", "release.yaml", "perf-daily.yaml", "parity-refresh.yaml"):
            self.assertNotRegex(text(name), r"git[^\n]* push [^\n]*--force")

    def test_the_new_tests_run_in_ci(self):
        ci = text("ci.yaml")
        for module in ("test_changelog_release", "test_release_plan", "test_release_metadata",
                       "test_release_replay", "test_release_pipeline_wiring"):
            self.assertIn(f"scripts.ci.tests.{module}", ci)



class BranchExistenceChecks(unittest.TestCase):
    # GitHub answers GET git/refs/heads/<name> with every ref that starts with
    # <name> when none matches exactly (bench/daily-perf "existed" while only
    # bench/daily-perf-next did, and moving it failed with HTTP 422). An
    # existence check reads git/ref/heads/<name>, which is exact.
    def test_no_workflow_checks_a_branch_with_the_prefix_matching_endpoint(self):
        for path in sorted(WORKFLOWS.glob("*.y*ml")):
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "gh api" not in line or " -X " in line:
                    continue
                if re.search(r"git/refs/heads/|\$\{REF\}/heads/", line):
                    self.fail(f"{path.name}:{number}: read one branch with git/ref/heads/, not git/refs/heads/: {line.strip()}")


class ParityRefreshWiring(unittest.TestCase):
    # The weekly differential run (bench/parity) is delivered the way perf-daily's report is: a bot branch moved
    # through the API, a pull request, never main. Its schedule keeps off perf-daily's runner window.
    def job(self, name):
        parity = text("parity-refresh.yaml")
        start = parity.index(f"\n  {name}:\n")
        following = [parity.find(f"\n  {other}:\n", start + 1) for other in ("validate", "refresh")]
        ends = [i for i in following if i > start]
        return parity[start : min(ends)] if ends else parity[start:]

    def test_the_schedule_is_weekly_and_clear_of_the_perf_window(self):
        parity, perf = text("parity-refresh.yaml"), text("perf-daily.yaml")
        parity_cron = re.search(r'cron: "(\d+) (\d+) \* \* (\d)"', parity)
        self.assertIsNotNone(parity_cron, "parity-refresh runs weekly (one weekday)")
        perf_cron = re.search(r'cron: "(\d+) (\d+) \* \* (\d)"', perf)
        self.assertIsNotNone(perf_cron, "the perf run is weekly (one weekday)")
        perf_minutes = int(re.search(r"timeout-minutes: (\d+)\n    permissions:\n      contents: write", perf).group(1))
        parity_minutes = int(re.search(r"timeout-minutes: (\d+)", self.job("refresh")).group(1))
        week = 7 * 24 * 60

        def start(cron):
            minute, hour, weekday = (int(cron.group(i)) for i in (1, 2, 3))
            return weekday * 24 * 60 + hour * 60 + minute

        # On a weekly clock the two runs must not overlap: each starts after the other's longest run ended.
        p, q = start(perf_cron), start(parity_cron)
        self.assertTrue((q - p) % week >= perf_minutes, "parity-refresh starts after the perf run's longest run ended")
        self.assertTrue((p - q) % week >= parity_minutes, "the perf run starts after parity-refresh's longest run ended")

    def test_the_entrypoint_is_the_one_the_tests_and_the_docs_describe(self):
        self.assertIn('python3 bench/parity/refresh.py --out "$OUT" --mode full', self.job("refresh"))
        self.assertIn('python3 bench/parity/refresh.py --out "$OUT" --mode validate', self.job("validate"))
        self.assertTrue((REPO / "bench" / "parity" / "refresh.py").exists())

    def test_a_pull_request_only_validates_with_a_read_only_token(self):
        parity = text("parity-refresh.yaml")
        self.assertIn("if: github.event_name == 'pull_request'", self.job("validate"))
        self.assertIn("if: github.event_name != 'pull_request'", self.job("refresh"))
        validate = self.job("validate")
        self.assertNotIn("contents: write", validate)
        self.assertNotIn("RELEASE_PR_TOKEN", validate)
        self.assertNotIn("gh pr create", validate)
        self.assertIn("pull_request:", parity)

    def test_the_job_holding_the_bot_token_restores_no_cache_and_checks_out_main(self):
        refresh = self.job("refresh")
        self.assertIn("ref: main", refresh)
        self.assertIn("cache: false", refresh)
        self.assertNotIn("cache: npm", refresh)
        self.assertNotIn("actions/cache", refresh)
        self.assertIn("persist-credentials: false", refresh)
        self.assertIn("timeout-minutes: 240", refresh)

    def test_the_report_branch_is_only_ever_moved_through_the_api(self):
        refresh = self.job("refresh")
        step = refresh[refresh.index("name: Open or update the parity report PR") :]
        step = step[: step.index("name: Arm auto-merge")]
        self.assertIn('push origin "${TMP_BRANCH}"', step)
        self.assertNotIn('push origin "$BRANCH"', step)
        self.assertNotRegex(step, r"push [^\n]*(--force|-f |main)")
        self.assertIn("-X PATCH", step)
        self.assertIn("-X DELETE", step)
        self.assertIn("git/ref/heads/${BRANCH}", step)  # the exact ref, not git/refs/heads/<prefix>
        self.assertLess(step.index('push origin "${TMP_BRANCH}"'), step.index("-X PATCH"))
        self.assertIn("BRANCH: bench/parity-refresh", refresh)

    def test_what_a_refresh_commits_is_a_list_of_the_files_it_may_change(self):
        refresh = self.job("refresh")
        self.assertIn("conformance/registry/cases", refresh)
        self.assertIn("conformance/registry/generated", refresh)
        self.assertNotRegex(refresh, r"git add (-A|\.|--all)")

    def test_the_pull_request_is_opened_even_when_the_gate_needs_a_case_and_the_job_then_ends_red(self):
        refresh = self.job("refresh")
        self.assertLess(refresh.index("name: Open or update the parity report PR"),
                        refresh.index("name: Fail when the registry needs a case"))
        self.assertIn("conformance_ok", refresh)


# Steps that publish something a user or the world can see: a registry login or push, a tag, an attestation,
# a release, the chart's visibility, the metadata branch and its pull request.
PUBLISHING_USES = ("actions/attest", "softprops/action-gh-release", "docker/login-action")
PUBLISHING_COMMANDS = ("helm push", "helm registry login", "imagetools create", "gh api -X PATCH",
                       "release_metadata.py prepare", "gh pr create", "gh pr merge", "git push")
NOT_DRY = "!inputs.dry_run"


def job_steps(workflow_text):
    """The steps of a workflow's jobs as (name, if, uses, text) from the block layout: a step starts at
    `      - ` and its keys sit at eight spaces."""
    out = []
    for block in re.split(r"(?m)^      - ", workflow_text)[1:]:
        lines = ("  " + block).splitlines()
        first = lines[0].strip()
        name = re.search(r"(?m)^\s*name: (.+)$", "\n".join(lines[:3]))
        cond = re.search(r"(?m)^\s{0,8}if: (.+)$", block)
        uses = re.search(r"(?m)^\s*(?:- )?uses: (\S+)", first + "\n" + block)
        out.append((name.group(1).strip() if name else first, cond.group(1).strip() if cond else "",
                    uses.group(1) if uses else "", block))
    return out


def publishes(uses, text):
    if uses.startswith(PUBLISHING_USES):
        return True
    if uses.startswith("docker/build-push-action") and "push=true" in text:
        return True
    run = text.split("run: |", 1)[1] if "run: |" in text else ""
    return any(c in run for c in PUBLISHING_COMMANDS)


def unguarded_publishing(workflow_text):
    return [name for name, cond, uses, text in job_steps(workflow_text) if publishes(uses, text) and NOT_DRY not in cond]


def keyless_in_dry_run(workflow_text):
    """Steps that run in a dry run and could sign keylessly (a Rekor upload is a publication): every cosign
    sign of such a step must sit after a dry-run branch that signs with the throwaway key and exits."""
    bad = []
    signs = re.compile(r"cosign sign(?:-blob)? ")
    for name, cond, _, text in job_steps(workflow_text):
        if not signs.search(text) or NOT_DRY in cond:
            continue
        branch = text.find('if [ "${DRY_RUN}" = "true" ]; then')
        end = text.find("\n          fi", branch)
        dry = text[branch:end] if branch >= 0 and end > branch else ""
        keyless_before = branch < 0 or bool(signs.search(text[:branch]))
        if keyless_before or "--key" not in dry or "--tlog-upload=false" not in dry or "exit 0" not in dry:
            bad.append(name)
    return bad


class ReleaseDryRunWiring(unittest.TestCase):
    # release.yaml's dry_run runs every step and publishes nothing. A new publishing step without the guard
    # fails here, whatever it is called.
    def test_the_input_is_a_boolean_that_defaults_to_off(self):
        release = text("release.yaml")
        block = release[release.index("      dry_run:"):release.index("permissions:")]
        self.assertIn("type: boolean", block)
        self.assertIn("default: false", block)
        self.assertIn("DRY_RUN: ${{ inputs.dry_run }}", release)

    def test_every_publishing_step_is_skipped_in_a_dry_run(self):
        release = text("release.yaml")
        self.assertEqual(unguarded_publishing(release), [])
        found = [name for name, _, uses, body in job_steps(release) if publishes(uses, body)]
        # The registry logins, the push, the tags, the chart pushes and visibility, every attestation, the
        # release and the metadata branch and pull request.
        self.assertGreaterEqual(len(found), 19, found)

    def test_no_step_of_a_dry_run_signs_keylessly(self):
        self.assertEqual(keyless_in_dry_run(text("release.yaml")), [])

    def test_the_dry_run_builds_scans_and_collects_locally(self):
        steps = {name: (cond, body) for name, cond, _, body in job_steps(text("release.yaml"))}
        cond, body = steps["Build the Docker image (dry run)"]
        self.assertEqual(cond, "${{ inputs.dry_run }}")
        self.assertIn("type=oci,dest=", body)
        self.assertNotIn("push=true", body)
        self.assertIn("sbom: true", body)
        self.assertIn("provenance: mode=max", body)
        self.assertIn("IMAGE_ARCHIVE", steps["Scan the pushed image with Trivy"][1])
        self.assertIn("oci-archive:", steps["Collect provenance bundles, image SBOMs and digests"][1])
        cond, body = steps["Check the release assets (dry run)"]
        self.assertEqual(cond, "${{ inputs.dry_run }}")
        self.assertIn("cosign verify-blob --key", body)
        self.assertIn('rm -f "${DRY_RUN_DIR}/throwaway.key"', body)
        cond, body = steps["Keep the dry run's outputs"]
        self.assertIn("inputs.dry_run", cond)
        self.assertIn("!/tmp/release-dry-run/*.key", body)
        self.assertIn("a dry run replaces nothing and goes on", steps["Refuse to republish an immutable release"][1])
        # A release still refuses every other ref; only a dry run, which publishes nothing, goes on from a branch.
        refs = steps["Refuse to publish from other refs"][1]
        self.assertIn("releases are published only from main or a v* tag", refs)
        self.assertLess(refs.index('if [ "${DRY_RUN}" = "true" ]'), refs.index("exit 1"))

    def test_the_detection_catches_a_new_publishing_step(self):
        sample = "\n".join([
            "    steps:",
            "      - name: Push somewhere new",
            "        run: |",
            "          helm push dist/x.tgz oci://example",
            "      - name: Guarded push",
            "        if: ${{ !inputs.dry_run }}",
            "        run: |",
            "          git push origin x",
            "      - name: A new attestation",
            "        uses: actions/attest-sbom@0123456789abcdef0123456789abcdef01234567 # v1",
            "      - name: Keyless in a dry run",
            "        run: |",
            "          cosign sign --yes ghcr.io/x@sha256:y",
            "      - name: Key in a dry run",
            "        run: |",
            '          if [ "${DRY_RUN}" = "true" ]; then',
            "            cosign sign-blob --key k --tlog-upload=false f",
            "            exit 0",
            "          fi",
            "          cosign sign-blob --yes f",
            "",
        ])
        self.assertEqual(unguarded_publishing(sample), ["Push somewhere new", "A new attestation"])
        self.assertEqual(keyless_in_dry_run(sample), ["Keyless in a dry run"])


if __name__ == "__main__":
    unittest.main()
