## Summary

- describe the user-visible and operator-visible change

## Changelog (Required)

- add at least one `- ...` bullet under `## [Unreleased]` in `CHANGELOG.md`
- this text is promoted into the release section and used for GitHub release notes

## Validation

- list checks/tests you ran locally or in CI

## Cache efficiency evidence (Cache behavior changes)

- [ ] Add a **main versus PR** matrix with measured commit SHAs, workload/config,
  node/RF/Tier0 settings and links to reproducible results.
- [ ] Show parity/errors, p50/p95, scoped proxy CPU, allocation/retained RAM/RSS,
  disk occupancy/encoded traffic/evictions, VL requests/ranges/bytes and peers.
- [ ] Separate Loki-layer, VL-layer and reference Loki cache observations;
  give hits/lookups and bypasses rather than an unexplained hit percentage.
- [ ] Cover relevant cold/repeat/shift/refresh/restart/peer-loss/pressure states;
  name untested states, costs, regressions and unchanged cases explicitly.
- [ ] Distinguish logical bytes/file size from physical IOPS, and measured
  request work from background/priming. Mark unavailable measurements explicitly.
- [ ] Explain freshness/isolation/invalidation and resource-bound implications,
  or state why this section is not applicable. See AGENTS.md for the full rule.

## Visual evidence (Product changes)

Release, CI, documentation, test-only, chart-only and harness-only PRs skip Visual
smoke and performance A/B. State the scope and list their relevant checks instead.

- [ ] Link the current-head **Visual smoke** CI comment, with the measured base and PR SHAs.
- [ ] Map every Grafana-visible behavior change to its targeted scene/query and inline `main | PR | Loki` screenshot. Generic regression screenshots alone do not prove the change.
- [ ] Confirm that all targeted proof scenes and the fixed general regression graphs/pages ran and that CI published their images directly in the comment.
- [ ] For product behavior without a Grafana-visible effect, explain why and link the exact API/test evidence; eligible product PRs retain the general regression controls.
- [ ] After the final push or rebase, verify the current images and data/error comparisons; mark older evidence as historical.
