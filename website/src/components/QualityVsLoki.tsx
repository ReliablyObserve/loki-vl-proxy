import type {ReactNode} from 'react';
import Heading from '@theme/Heading';
import styles from '@site/src/components/marketing.module.css';
// Written by `conformance/scripts/perf_evidence.py --site-data` from the latest weekly A/B run; the conformance gate
// fails when it is stale, so the page never states figures the README does not.
import data from '@site/src/data/quality-vs-loki.json';

const README =
  'https://github.com/ReliablyObserve/Loki-VL-proxy#quality-and-performance-vs-loki-current';
const EVIDENCE =
  'https://github.com/ReliablyObserve/Loki-VL-proxy/blob/main/conformance/reports/performance.md#summary-versus-loki';
const GAPS =
  'https://github.com/ReliablyObserve/Loki-VL-proxy/blob/main/conformance/reports/parity-gaps.md';

function share(part: number, whole: number): string {
  return whole ? `${part} (${((100 * part) / whole).toFixed(1)}%)` : String(part);
}

function sec(value: number | null): string {
  return value === null ? '—' : `${value.toFixed(2)} s`;
}

// The verdict of two medians by the rule of the generated tables: beyond the share and the seconds.
function verdict(proxy: number | null, loki: number | null): string {
  if (proxy === null || loki === null) {
    return '⚪';
  }
  const {share: noise, seconds} = data.noise;
  const beyond = (a: number, b: number) => a - b > seconds && a > b * (1 + noise);
  if (beyond(proxy, loki)) {
    return '🔴';
  }
  return beyond(loki, proxy) ? '🟢' : '🟡';
}

type Spread = {p50: number; p90: number; max: number} | null;

function spread(s: Spread): string {
  return s ? `${s.p50.toFixed(2)} · ${s.p90.toFixed(2)} · ${s.max.toFixed(2)} s` : '—';
}

const ICON: Record<string, string> = {faster: '🟢', par: '🟡', slower: '🔴'};

type Pair = {
  shape: string;
  range: string;
  workload: string;
  verdict: string;
  proxy: number;
  loki: number;
  maybe_cached: boolean;
};

function multiple(row: Pair): string {
  const icon = ICON[row.verdict] ?? '⚪';
  if (row.verdict === 'par' || !row.proxy || !row.loki) {
    return `${icon} on par`;
  }
  return row.verdict === 'faster'
    ? `${icon} ${(row.loki / row.proxy).toFixed(1)}× faster`
    : `${icon} ${(row.proxy / row.loki).toFixed(1)}× slower`;
}

function PairTable({rows}: {rows: Pair[]}): ReactNode {
  return (
    <div className={styles.tableWrap}>
      <table className={styles.comparisonTable}>
        <thead>
          <tr>
            <th>Query shape</th>
            <th>Range</th>
            <th>Workload</th>
            <th>Proxy</th>
            <th>Loki</th>
            <th>Proxy vs Loki</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={`${r.shape}-${r.range}`}>
              <td>
                <code>{r.shape}</code>
              </td>
              <td>{r.range}</td>
              <td>{r.workload}</td>
              <td>
                <strong>{sec(r.proxy)}</strong>
                {r.maybe_cached ? ' ‡' : ''}
              </td>
              <td>{sec(r.loki)}</td>
              <td>{multiple(r)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function QualityVsLoki(): ReactNode {
  const impact = data.differences.open_by_impact as Record<string, number>;
  const visible =
    (impact['explore-visible'] ?? 0) + (impact['drilldown-visible'] ?? 0) + (impact['grafana-datasource'] ?? 0);
  const answered = data.answers.same + data.answers.differs + data.answers['not compared'];
  return (
    <section className={styles.section}>
      <div className={styles.sectionHeader}>
        <Heading as="h2" className={styles.sectionTitle}>
          Measured against Loki v{data.loki}
        </Heading>
        <p className={styles.sectionLead}>
          The latest weekly A/B run, {data.date} at commit <code>{data.commits.join(', ')}</code>: the same data and
          hardware for both, {data.sets} query-shape sets over {data.ranges[0]} to {data.ranges[1]}. A row is one
          query shape over one time range; faster or slower means beyond {Math.round(data.noise.share * 100)}% and{' '}
          {Math.round(data.noise.seconds * 1000)} ms, and a first request Loki&apos;s results cache answered is not
          judged. The same figures are generated into the <a href={README}>README</a>.
        </p>
      </div>
      <div className={styles.tableWrap}>
        <table className={styles.comparisonTable}>
          <thead>
            <tr>
              <th>Loki profile</th>
              <th>Request</th>
              <th>Rows compared</th>
              <th>🟢 Below Loki</th>
              <th>🟡 Close to Loki</th>
              <th>🟠 1–2× Loki</th>
              <th>🔴 Above 2× Loki</th>
            </tr>
          </thead>
          <tbody>
            {data.distribution.map((row) => (
              <tr key={`${row.profile}-${row.request}`}>
                <td>
                  <code>{row.profile}</code>
                </td>
                <td>{row.request}</td>
                <td>{row.rows}</td>
                <td>{share(row.below, row.rows)}</td>
                <td>{share(row.close, row.rows)}</td>
                <td>{share(row.between, row.rows)}</td>
                <td>
                  <strong>{share(row.above, row.rows)}</strong>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className={styles.tableWrap}>
        <table className={styles.comparisonTable}>
          <thead>
            <tr>
              <th>Workload ({data.profile})</th>
              <th>Pairs</th>
              <th>🟢 / 🟡 / 🔴</th>
              <th>First request p50 · p90 · slowest: proxy</th>
              <th>Loki</th>
              <th>Repeat p50: proxy</th>
              <th>Loki</th>
            </tr>
          </thead>
          <tbody>
            {data.workloads.map((w) => (
              <tr key={w.name}>
                <td>{w.name}</td>
                <td>{w.compared}</td>
                <td>
                  {w.faster} / {w.par} / {w.slower}
                </td>
                <td>
                  <strong>{spread(w.first.proxy)}</strong>
                </td>
                <td>{spread(w.first.loki)}</td>
                <td>
                  {verdict(w.repeat.proxy, w.repeat.loki)} <strong>{sec(w.repeat.proxy)}</strong>
                </td>
                <td>{sec(w.repeat.loki)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className={styles.sectionLead}>
        First request by the time range a user picks, p50 · p90 · slowest pair:
      </p>
      <div className={styles.tableWrap}>
        <table className={styles.comparisonTable}>
          <thead>
            <tr>
              <th>Workload</th>
              <th>Range</th>
              <th>Pairs</th>
              <th>🟢 / 🟡 / 🔴</th>
              <th>Proxy</th>
              <th>Loki</th>
            </tr>
          </thead>
          <tbody>
            {data.by_range.map((r) => (
              <tr key={`${r.workload}-${r.range}`}>
                <td>{r.workload}</td>
                <td>{r.range}</td>
                <td>{r.pairs}</td>
                <td>
                  {r.faster} / {r.par} / {r.slower}
                </td>
                <td>
                  <strong>{spread(r.proxy)}</strong>
                </td>
                <td>{spread(r.loki)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className={styles.sectionLead}>The heaviest queries for Loki, the ones a user waits for:</p>
      <PairTable rows={data.heaviest} />
      {data.heaviest.some((r) => r.maybe_cached) ? (
        <p>
          ‡ The proxy answered a first request over 6h or more in under {Math.round(data.noise.seconds * 1000)} ms:
          probably from its own cache, warmed by an earlier set of the weekly run that sent the same query. The run
          records Loki&apos;s cache counters but not the proxy&apos;s, so this row is not a cold proxy.
        </p>
      ) : null}
      {data.losses.length ? (
        <>
          <p className={styles.sectionLead}>Where the proxy loses most:</p>
          <PairTable rows={data.losses} />
        </>
      ) : null}
      <p className={styles.sectionLead}>
        Readiness by client: the Loki endpoints each client calls, their evidence, and the open differences it shows.
      </p>
      <div className={styles.tableWrap}>
        <table className={styles.comparisonTable}>
          <thead>
            <tr>
              <th>Client</th>
              <th>Loki endpoints it calls</th>
              <th>Routed</th>
              <th>Compared with Loki in tests</th>
              <th>Proven</th>
              <th>Open differences it shows</th>
            </tr>
          </thead>
          <tbody>
            {data.clients.map((c) => (
              <tr key={c.client}>
                <td>{c.name}</td>
                <td>{c.endpoints}</td>
                <td>{c.routed}</td>
                <td>{c.compared_with_loki}</td>
                <td>{c.proven}</td>
                <td>{c.open_differences}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <ul className={styles.list}>
        <li>
          Answers: <strong>{data.answers.same} of {answered}</strong> pairs return the same result as Loki,{' '}
          {data.answers.differs} differ and {data.answers['not compared']} could not be compared.
        </li>
        <li>
          Loki read API: <strong>{data.read_api.routed} of {data.read_api.endpoints}</strong> endpoints routed,{' '}
          {data.read_api.compared_with_loki} with tests that compare the proxy against Loki, {data.read_api.proven}{' '}
          proven.
        </li>
        <li>
          Known differences from Loki: <strong>{data.differences.ranked}</strong> ranked, {data.differences.open} open;{' '}
          {visible} of the open ones are visible in Grafana ({impact['explore-visible'] ?? 0} Explore,{' '}
          {impact['drilldown-visible'] ?? 0} Logs Drilldown, {impact['grafana-datasource'] ?? 0} Grafana datasource).
          Each is listed in the <a href={GAPS}>ranked differences</a>.
        </li>
        <li>
          Every shape and range, the open gaps and the regression gate: <a href={EVIDENCE}>performance evidence</a>.
        </li>
      </ul>
    </section>
  );
}
