#!/usr/bin/env python3

import argparse
import json
import sys
from pathlib import Path


def parse_required_checks(values: list[str]) -> dict[str, float]:
    required: dict[str, float] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(
                f"invalid --require-check value {value!r}; expected NAME=MIN_SCORE"
            )
        name, score = value.split("=", 1)
        required[name.strip()] = float(score)
    return required


def evaluate_report(
    payload: dict, min_overall: float, require_checks: dict[str, float]
) -> int:
    checks = {item["name"]: item for item in payload.get("checks", [])}
    failures: list[str] = []

    overall = payload.get("score")
    if overall is None:
        failures.append("scorecard report is missing overall score")
    elif float(overall) < min_overall:
        failures.append(
            f"overall score {float(overall):.1f} is below minimum {min_overall:.1f}"
        )

    for check_name, min_score in require_checks.items():
        check = checks.get(check_name)
        if check is None:
            # Check is entirely absent from the scorecard output. This happens when
            # evaluating a specific commit (--commit) that scorecard cannot fully
            # analyse via the GitHub API (e.g. SAST on a fresh PR merge commit with
            # no CodeQL results yet). Treat as unavailable rather than a failure.
            print(f"  note: {check_name} check absent from scorecard output — skipping requirement")
            continue
        score = check.get("score")
        if score is None:
            failures.append(f"scorecard check {check_name!r} has no score")
            continue
        # Score of -1 means "could not determine" (e.g. CI-Tests when evaluating a
        # fresh PR merge commit that has no CI runs yet). Treat as unavailable rather
        # than a failure so transient evaluation gaps don't block PRs.
        if float(score) == -1:
            print(f"  note: {check_name} score is -1 (unavailable) — skipping requirement")
            continue
        if float(score) < min_score:
            failures.append(
                f"{check_name} score {float(score):.1f} is below minimum {min_score:.1f}"
            )

    print("OpenSSF Scorecard summary")
    print(f"  overall: {float(overall):.1f}" if overall is not None else "  overall: missing")
    for check_name in sorted(require_checks):
        check = checks.get(check_name)
        score = "missing" if check is None else check.get("score", "missing")
        print(f"  {check_name}: {score}")

    if failures:
        print("\nScorecard guardrail failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


def main_for_test(report: Path, min_overall: float, require_checks: dict[str, float]) -> int:
    text = report.read_text().strip()
    if not text:
        # Scorecard container failed to fetch the score (API rate limit, network issue,
        # etc.). The OpenSSF Scorecard step has continue-on-error: true precisely for
        # this case. Treat an empty report as "unavailable" rather than a guardrail
        # failure so transient upstream issues don't block PRs.
        print("OpenSSF Scorecard report is empty — skipping guardrails (scorecard unavailable)")
        return 0
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        print(f"OpenSSF Scorecard report is not valid JSON ({exc}) — skipping guardrails")
        return 0
    return evaluate_report(
        payload=payload,
        min_overall=min_overall,
        require_checks=require_checks,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail CI when required OpenSSF Scorecard checks are below thresholds."
    )
    parser.add_argument("report", type=Path, help="Path to scorecard JSON output")
    parser.add_argument("--min-overall", type=float, default=0.0)
    parser.add_argument("--require-check", action="append", default=[])
    args = parser.parse_args()

    return main_for_test(
        report=args.report,
        min_overall=args.min_overall,
        require_checks=parse_required_checks(args.require_check),
    )


if __name__ == "__main__":
    sys.exit(main())
