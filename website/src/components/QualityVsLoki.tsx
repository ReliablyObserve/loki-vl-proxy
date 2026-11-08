import type {ReactNode} from 'react';
import Heading from '@theme/Heading';
import styles from '@site/src/components/marketing.module.css';
// Written by `conformance/scripts/perf_evidence.py --site-data` from the latest daily A/B run; the conformance gate
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
          The latest daily A/B run, {data.date} at commit <code>{data.commits.join(', ')}</code>: the same data and
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
              <th>Pairs compared</th>
              <th>🟢 / 🟡 / 🔴</th>
              <th>First request: proxy</th>
              <th>Loki</th>
              <th>Repeat: proxy</th>
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
                  {verdict(w.first.proxy, w.first.loki)} <strong>{sec(w.first.proxy)}</strong>
                </td>
                <td>{sec(w.first.loki)}</td>
                <td>
                  {verdict(w.repeat.proxy, w.repeat.loki)} <strong>{sec(w.repeat.proxy)}</strong>
                </td>
                <td>{sec(w.repeat.loki)}</td>
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
