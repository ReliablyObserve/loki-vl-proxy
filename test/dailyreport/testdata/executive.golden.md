### Executive summary

**Red: 4 of 14 checks (areas) at target; 1175 of 1254 counted items ok (93.7%); 5 red reasons.**

#### Where we stand

| Area | Good | Not good | % good | Target | Still needed | Change since 2026-11-14 |
|---|---|---|---|---|---|---|
| Registry cases | 6 | 3 | 54.5% | 0 open | 3 open cases to close | good +1, total 10 to 11, +4.5 pt |
| Live verdicts, VictoriaLogs v1.49.0 | 4 | 0 | 100.0% | 100% of proven match | none | good +2, not good -1, total 3 to 4, +33.3 pt |
| Coverage, VictoriaLogs v1.49.0 | 5 | 1 | 83.3% | 100% proven live or by a test | 1 more case to prove | good +1, total 5 to 6, +3.3 pt |
| Live verdicts, VictoriaLogs v1.53.0 | 3 | 1 | 75.0% | 100% of proven match | 1 differing case to fix (1 of them regressions) | good +1, total 3 to 4, +8.3 pt |
| Coverage, VictoriaLogs v1.53.0 | 5 | 1 | 83.3% | 100% proven live or by a test | 1 more case to prove | good +1, total 5 to 6, +3.3 pt |
| Error answers | 19 | 1 | 95.0% | 100% match | 1 answer to align, 0 live change(s) to review | good +1, not good -1, +5.0 pt |
| Grammar sweep | 958 | 42 | 95.8% | 0 differences | 42 queries to bring in line | good +5, not good -5, +0.5 pt |
| Parity slice | 24 | 6 | 60.0% | 100% same, 0 unclaimed clusters | 6 differing requests to fix, 1 cluster to claim | good +4, not good -4, +10.0 pt |
| Daily perf smoke, cold | 18 | 2 | 90.0% | 95% green, 0 above 2x | 1 green row more; 2 above 2x to fix | good +1, not good -1, +5.0 pt |
| Daily perf smoke, warm | 35 | 0 | 100.0% | 95% green, 0 above 2x | none | no change |
| Weekly perf, 2026-11-09 | 90 | 10 | 90.0% | 95% faster or on par, 0 above 2x | 5 green rows more; 3 above 2x to fix | no change |
| Cache reuse, VictoriaLogs v1.49.0 | 5 | 0 | 100.0% | 100% reused, 0 VictoriaLogs calls on repeats | none | good +1, not good -1, +20.0 pt |
| Cache reuse, VictoriaLogs v1.53.0 | 5 | 0 | 100.0% | 100% reused, 0 VictoriaLogs calls on repeats | none | good +1, not good -1, +20.0 pt |
| Scheduled workflows | 8 | 2 | 80.0% | all ok | 2 workflows to fix: parity-refresh.yaml, perf-daily.yaml | good -1, not good +1, -10.0 pt |

A check is one area of this table at its target. Counted items are the units of a row: a registry case, a proven live verdict, a recorded error answer, a sweep query, a parity request, a perf row, a scheduled workflow. Coverage rows show how much of the case set is proven live, or by a named passing test, and are not counted again; a case not proven is a number to work down, red only when a proven case regressed. Not good counts differences only; accepted and upstream-blocked cases are shown apart.

- Registry cases: fixed 4 + no gap 2 are good; accepted 1 (documented 1, owner-kept 0) and upstream-blocked 1 are shown apart; good + accepted 7 of 11 (63.6%); open by impact: explore-visible 1, edge 2.
- Live verdicts, VictoriaLogs v1.49.0: proven = matches 4 + differs 0 = 4 of 6 cases; regressions 0; proposed closes 1; behaviour changed 0.
- Coverage, VictoriaLogs v1.49.0: 5 of 6 cases proven (live 4, by a named passing test 1); not proven 1 (empty 1); tests passing: ci.yaml run of 2026-11-15 on main, https://x/ci.
- Live verdicts, VictoriaLogs v1.53.0: proven = matches 3 + differs 1 = 4 of 6 cases; regressions 1; proposed closes 2; behaviour changed 0.
- Coverage, VictoriaLogs v1.53.0: 5 of 6 cases proven (live 4, by a named passing test 1); not proven 1 (empty 1); tests passing: ci.yaml run of 2026-11-15 on main, https://x/ci.
- Error answers: 1 known differences, 0 known leaks; live re-record: 0 of 20 rows changed; unit replay passes.
- Grammar sweep: 958 queries agree out of 1000 (every difference row counted); Loki accepts 700 and rejects 300.
- Parity slice: 40 requests (same 24, differ 6, both empty 8, blocked 2); 5 clusters, 1 claimed by no registry case.
- Daily perf smoke, cold: 20 rows, below Loki 9, close 9, 1-2x 0, above 2x 2 (green = below + close).
- Daily perf smoke, warm: 35 rows, below Loki 5, close 30, 1-2x 0, above 2x 0 (green = below + close).
- Weekly perf (measured 2026-11-09): 100 shape x range pairs, faster 60, on par 30, slower 10; 3 on the above-2x list.
- Cache reuse, VictoriaLogs v1.49.0: 5 repeats reused of 5 measured, 1 not measured; VictoriaLogs calls 9 cold, 0 on the repeats. Counted per request for 5 repeats and by process-wide metrics for 0; the proxy's background calls around the repeats (3) are not in the count.
- Cache reuse, VictoriaLogs v1.53.0: 5 repeats reused of 5 measured, 1 not measured; VictoriaLogs calls 9 cold, 0 on the repeats. Counted per request for 5 repeats and by process-wide metrics for 0; the proxy's background calls around the repeats (3) are not in the count. Perf smoke: 30 rows, VictoriaLogs calls 50 cold and 40 on the repeats, 0 rows lost reuse.
- Scheduled workflows: 1 pending their first scheduled run, not counted and not red: new-weekly.yaml.

#### Loki side vs VictoriaLogs side

**Loki (client-facing compatibility)**

- Open registry cases: 3 of 11 (27.3%).
- Live differs, VictoriaLogs v1.49.0: 0 of 4 proven (0.0%); proven is 4 of 6 cases.
- Live differs, VictoriaLogs v1.53.0: 1 of 4 proven (25.0%); proven is 4 of 6 cases.
- Error answers differing from Loki: 1 of 20 recorded (5.0%).
- Grammar sweep differences with Loki: 24 of 1000 queries (2.4%).
  - error text differs: 10 (1.0%)
  - proxy accepts, Loki rejects: 7 (0.7%)
  - proxy rejects, Loki accepts: 4 (0.4%)
  - tree fact differs (1 kind): 3 (0.3%)
- To reach 100%: close 3 open cases; fix 1 differing proven case; prove 2 more cases live; align 1 error answer; remove 24 sweep differences.

**VictoriaLogs (backend side)**

- Grammar sweep, VictoriaLogs rejects the translator text: 18 of 1000 queries (1.8%).
- Grammar sweep, accepted, translation fails: 0 of 1000 queries (0.0%).
- Upstream-blocked cases: 1 of 11 (9.1%).
- Cases whose live verdict differs between v1.49.0 and v1.53.0: 1 of 6 (16.7%).
- Daily perf smoke rows above 2x Loki: 2 of 55 (3.6%).
- Weekly perf pairs above 2x Loki: 3 on the list, of 100 pairs (3.0%).
- To reach 100%: fix 18 sweep queries whose text VictoriaLogs refuses or the translation fails; 1 case waits on a VictoriaLogs fix; 1 version-dependent verdict to explain; bring 5 perf rows (2 daily, 3 weekly) under 2x Loki.

#### What changed since 2026-11-14

Main `bbbbbbbbbbbb` (was `aaaaaaaaaaaa`).

| Number | Before | Now |
|---|---|---|
| Registry: cases | 10 | 11 |
| Registry: fixed | 3 | 4 |
| Registry: open, explore-visible | 2 | 1 |
| Registry: open, edge | 1 | 2 |
| Verdicts v1.49.0: matches | 2 | 4 |
| Verdicts v1.49.0: differs | 1 | 0 |
| Verdicts v1.49.0: regressions | 1 | 0 |
| Cache reuse v1.49.0: reused | 4 | 5 |
| Cache reuse v1.49.0: not reused | 1 | 0 |
| Cache reuse v1.49.0: not measured | 0 | 1 |
| Cache reuse v1.49.0: VictoriaLogs calls on repeats | 2 | 0 |
| Coverage v1.49.0: proven live | 3 | 4 |
| Verdicts v1.53.0: matches | 2 | 3 |
| Verdicts v1.53.0: regressions | 0 | 1 |
| Verdicts v1.53.0: proposed close | 1 | 2 |
| Cache reuse v1.53.0: reused | 4 | 5 |
| Cache reuse v1.53.0: not reused | 1 | 0 |
| Cache reuse v1.53.0: not measured | 0 | 1 |
| Cache reuse v1.53.0: VictoriaLogs calls on repeats | 2 | 0 |
| Coverage v1.53.0: proven live | 3 | 4 |
| Error answers: matching | 18 | 19 |
| Error answers: known differences | 2 | 1 |
| Error answers: live rows changed | 1 | 0 |
| Grammar sweep: VictoriaLogs rejects the translator text | 20 | 18 |
| Grammar sweep: accepted, translation fails | 5 | 0 |
| Grammar sweep: proxy rejects, Loki accepts | 0 | 4 |
| Grammar sweep: tree fact differs: binary operation | 2 | 0 |
| Parity slice: unclaimed clusters | 2 | 1 |
| Parity slice: same | 20 | 24 |
| Parity slice: differ | 10 | 6 |
| Daily perf smoke: above 2x | 1 | 2 |
| Daily perf smoke cold: below | 8 | 9 |
| Daily perf smoke cold: between | 2 | 0 |
| Daily perf smoke cold: above | 1 | 2 |
| Daily perf smoke cold: green | 17 | 18 |
| Scheduled workflows: problems | 1 | 2 |

**New red items** (2)

- scheduled workflow perf-daily.yaml: last scheduled run 2026-11-09 ended failure
- a/4 differs from Loki on VictoriaLogs v1.53.0, and the registry holds it fixed

**Red items resolved** (1)

- b/old differs from Loki on VictoriaLogs v1.49.0, and the registry holds it fixed

**Red items that changed** (3)

- scheduled workflow parity-refresh.yaml: last scheduled run 2026-11-14 ended failure (was: scheduled workflow parity-refresh.yaml: last scheduled run 2026-11-13 ended failure)
- parity slice: 1 difference cluster(s) are claimed by no registry case (was: parity slice: 2 difference cluster(s) are claimed by no registry case)
- perf smoke: 2 row(s) above 2x Loki (was: perf smoke: 1 row(s) above 2x Loki)

**Cases whose verdict changed** (2)

- a/2: differs to matches (v1.49.0, v1.53.0)
- a/4: matches to differs (v1.53.0)

**New regressions** (1)

- a/4 (v1.53.0)

**Regressions resolved** (1)

- b/old (v1.49.0)

**New proposed closes** (1)

- a/2 (v1.49.0, v1.53.0)

**No longer proposed for close** (1)

- a/3 (v1.49.0)

**Accepted behaviour newly changed** (1)

- a/9 (v1.53.0)

**Measured another way from today** (2)

- Cache reuse v1.49.0: VictoriaLogs calls on repeats are counted per request from today (before: process-wide metrics, background calls included); a drop is not a cache improvement
- Cache reuse v1.53.0: VictoriaLogs calls on repeats are counted per request from today (before: process-wide metrics, background calls included); a drop is not a cache improvement

**Registry status moves** (2)

- a/11: new case, open
- a/3: open to fixed

**Perf rows newly above 2x Loki** (1)

- s2 / Q7, 6h, cold, p: Loki 0.2 s, proxy 0.6 s, 3.0x

**New unclaimed parity clusters** (1)

- c9

**Parity clusters now claimed or gone** (1)

- c1

**Scheduled workflows newly failing** (1)

- perf-daily.yaml: last scheduled run 2026-11-09 ended failure

#### What is wrong now

| Item | Count | Which | What to do |
|---|---|---|---|
| Scheduled workflow not ok | 2 | parity-refresh.yaml; perf-daily.yaml | open the workflow's last scheduled run, fix the failure or the missing schedule, run it again |
| Parity cluster claimed by no case | 1 | c2; c9 | add a registry case with a gap block that claims each cluster |
| Perf smoke rows above 2x Loki | 2 | s1 / Q1, 1h, cold, p; s2 / Q7, 6h, cold, p | bring each row under 2x Loki (closest to Loki first) |
| Fixed case differs from Loki | 1 | a/4 | fix the regression, or reopen the case with the owner's decision |

