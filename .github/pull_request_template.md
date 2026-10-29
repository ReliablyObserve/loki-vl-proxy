## Summary

- describe the user-visible and operator-visible change

## Changelog (Required)

- add at least one `- ...` bullet under `## [Unreleased]` in `CHANGELOG.md`
- this text is promoted into the release section and used for GitHub release notes

## Validation

- list checks/tests you ran locally or in CI

## Visual evidence (Product changes)

Release, CI, documentation, test-only, chart-only and harness-only PRs skip Visual
smoke and performance A/B. State the scope and list their relevant checks instead.

- [ ] Link the current-head **Visual smoke** CI comment, with the measured base and PR SHAs.
- [ ] Map every Grafana-visible behavior change to its targeted scene/query and inline `main | PR | Loki` screenshot. Generic regression screenshots alone do not prove the change.
- [ ] Confirm that all targeted proof scenes and the fixed general regression graphs/pages ran and that CI published their images directly in the comment.
- [ ] For product behavior without a Grafana-visible effect, explain why and link the exact API/test evidence; eligible product PRs retain the general regression controls.
- [ ] After the final push or rebase, verify the current images and data/error comparisons; mark older evidence as historical.
